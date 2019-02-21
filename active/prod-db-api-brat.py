from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators import PythonOperator, BashOperator
from airflow.models import DAG

args = {
    'owner' : 'wchau',
    'depends_on_past' : False,
    'start_date' : datetime.utcnow(),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

dag = DAG(dag_id = 'prod_db_api_brat',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))

def generate_job_id(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id = "prod-airflow-nlp-pipeline")
    mssql_hook = MsSqlHook(mssql_conn_id = "prod-hidra-dz-db01")

    # get last update date from last successful run
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_runs WHERE job_status = 'successful' or job_status = 'running'"
    update_date_from_last_run =  pg_hook.get_first(tgt_select_stmt)[0]

    if update_date_from_last_run == None:
        # first run
        update_date_from_last_run = datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    tgt_select_stmt = "SELECT max(af_runs_id) FROM af_runs"
    last_run_id = pg_hook.get_first(tgt_select_stmt)[0]

    if last_run_id == None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1 

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT distinct HDCPUpdateDate FROM orca_ce_blob WHERE HDCPUpdateDate > %s"
    tgt_insert_stmt = "INSERT INTO af_runs (af_runs_id, source_last_update_date, job_start, job_status) VALUES (%s, %s, %s, 'running')"

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in mssql_hook.get_records(src_select_stmt, parameters=(update_date_from_last_run,)):
        hdcpupdatedates.append(row[0])
        pg_hook.run(tgt_insert_stmt, parameters=(new_run_id, row[0], job_start_date))
     
    return (new_run_id, hdcpupdatedates)


def get_source_record_id(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id = "prod-airflow-nlp-pipeline")
    mssql_hook = MsSqlHook(mssql_conn_id = "prod-hidra-dz-db01")

    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT TOP 10 hdcorcablobid FROM orca_ce_blob WHERE hdcpupdatedate = %s"
    tgt_insert_stmt = "INSERT INTO af_runs_details (af_runs_id, hdcpupdatedate, hdcorcablobid) VALUES (%s, %s, %s)"

    for hdcpupdatedate in hdcpupdatedates:
        for row in mssql_hook.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            pg_hook.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0]))
 

generate_job_id = \
    PythonOperator(task_id = 'generate_job_id',
                   provide_context = True,
                   python_callable=generate_job_id,
                   dag=dag)

populate_blobid_in_job_table = \
    PythonOperator(task_id = 'populate_blobid_in_job_table',
                   provide_context = True,
                   python_callable=get_source_record_id,
                   dag=dag)

generate_job_id >> populate_blobid_in_job_table
