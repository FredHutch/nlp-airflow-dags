from datetime import datetime, timedelta
import json
import subprocess
import base64
import paramiko

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators import PythonOperator, BashOperator
from airflow.models import DAG

args = {
    'owner': 'wchau',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='prod_db_api_brat',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def generate_job_id(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
    mssql_hook = MsSqlHook(mssql_conn_id="prod-hidra-dz-db01")

    # get last update date from last completed run
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_runs WHERE job_status = 'completed' or job_status = 'running'"
    update_date_from_last_run = pg_hook.get_first(tgt_select_stmt)[0]

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
    src_select_stmt = "SELECT HDCPUpdateDate, count(*) FROM orca_ce_blob WHERE HDCPUpdateDate > %s GROUP BY HDCPUpdateDate"
    tgt_insert_stmt = "INSERT INTO af_runs (af_runs_id, source_last_update_date, record_counts, job_start, job_status) VALUES (%s, %s, %s, %s, 'running')"

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in mssql_hook.get_records(src_select_stmt, parameters=(update_date_from_last_run,)):
        hdcpupdatedates.append(row[0])
        pg_hook.run(tgt_insert_stmt, parameters=(new_run_id, row[0], row[1], job_start_date))

    if len(hdcpupdatedates) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    return (new_run_id, hdcpupdatedates)


def populate_blobid_in_job_table(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
    mssql_hook = MsSqlHook(mssql_conn_id="prod-hidra-dz-db01")

    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT TOP 1 hdcorcablobid FROM orca_ce_blob WHERE hdcpupdatedate = %s"
    tgt_insert_stmt = "INSERT INTO af_runs_details (af_runs_id, hdcpupdatedate, hdcorcablobid) VALUES (%s, %s, %s)"

    for hdcpupdatedate in hdcpupdatedates:
        for row in mssql_hook.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            pg_hook.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0]))


def send_notes_to_brat(**kwargs):
    clinical_notes = kwargs['clinical_notes']
    datefolder = kwargs['datefolder']
    remoteNlpHomePath = "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"
    remoteNlpDataPath = "{}/{}".format(remoteNlpHomePath, datefolder)
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")

    record_processed = 0
    for notes_items in clinical_notes:
        hdcorcablobid = list(notes_items.keys())[0]
        notes = notes_items[hdcorcablobid]

        # create a subfolder for hdcpupdatedate
        if record_processed == 0:
            remote_command = "[ -d {} ] && echo 'found'".format(remoteNlpDataPath)
            is_datefolder_found = subprocess.getoutput(
                "ssh {}@{} {}".format(ssh_hook.username, ssh_hook.remote_host, remote_command))

            if is_datefolder_found != 'found':
                remote_command = "mkdir {}".format(remoteNlpDataPath)
                subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                                 "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])

        # send original notes to brat
        remote_command = """
                         umask 002;

                         if [[ -f {remotePath}/{filename}.txt ]]; then
                           rm {remotePath}/{filename}.txt
                         fi

                         echo "{data}" | base64 -d - > {remotePath}/{filename}.txt;
        """.format(
            data=str(base64.b64encode(notes['original_note']['extract_text'].encode('utf-8'))).replace("b'",
                                                                                                       "").replace("'",
                                                                                                                   ""),
            remotePath=remoteNlpDataPath,
            filename=hdcorcablobid[0]
        )

        subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                         "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])

        # send annotated notes to brat
        phiAnnoData = []
        line = 0
        for j in notes['annotated_note']:
            if j['Category'] == 'PROTECTED_HEALTH_INFORMATION':
                line += 1
                phiAnnoData.append(
                    "T{}\t{} {} {}\t{}".format(line, j['Type'], j['BeginOffset'], j['EndOffset'], j['Text']))

        if len(phiAnnoData) > 0:
            remote_command = """
                     umask 002;

                     echo '{data}' | base64 -d -  > {remotePath}/{filename}.ann;
            """.format(
                data=str(base64.b64encode("\r\n".join(phiAnnoData).encode('utf-8'))).replace("b'", "").replace("'", ""),
                remotePath=remoteNlpDataPath,
                filename=hdcorcablobid[0]
            )
        else:
            remote_command = "umask 002; touch {remotePath}/{filename}.ann;".format(remotePath=remoteNlpDataPath,
                                                                                    filename=hdcorcablobid[0])

        subprocess.call(["ssh", "-p {}".format(ssh_hook.port), "{}@{}".format(ssh_hook.username, ssh_hook.remote_host),
                         remote_command])
        '''
        insert update to table (to be made) for use to scan in next step
        '''
        update_brat_db_status(run_id, hdcorcablobid[0], "PENDING REVIEW")
        record_processed += 1

def update_brat_db_status(note_id, brat_status, job_date, directory_location):

    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
    tgt_insert_stmt = """
    INSERT INTO brat_review_status (source_last_update_date, directory_location, job_start, job_status)
     VALUES (%s, %s, %s, 'PENDING REVIEW')
     """

    job_start_date = datetime.now()
    pg_hook.run(tgt_insert_stmt,
                parameters=(run_id, job_start_date, directory_location, job_start_date))


def annotate_clinical_notes(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
    mssql_hook = MsSqlHook(mssql_conn_id="prod-hidra-dz-db01")
    api_hook = HttpHook(http_conn_id=' 	fh-nlp-api-deid', method='POST')

    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    tgt_select_stmt = "SELECT hdcorcablobid FROM af_runs_details WHERE af_runs_id = %s and hdcpupdatedate = %s and annotation_status is null"
    src_select_stmt = "SELECT blob_contents FROM orca_ce_blob WHERE hdcpupdatedate = %s and hdcorcablobid = %s"
    tgt_update_stmt = "UPDATE af_runs_details SET annotation_status = %s, annotation_date = %s WHERE af_runs_id = %s and hdcpupdatedate = %s and hdcorcablobid in (%s)"

    for hdcpupdatedate in hdcpupdatedates:

        datefolder = hdcpupdatedate.strftime('%Y-%m-%d')
        record_processed = 0

        for blobid in pg_hook.get_records(tgt_select_stmt, parameters=(run_id, hdcpupdatedate)):
            batch_records = []

            for row in mssql_hook.get_records(src_select_stmt, parameters=(hdcpupdatedate, blobid)):
                # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
                record = {}
                record[blobid] = {}

                record[blobid]['original_note'] = {"extract_text": "{}".format(row[0])}
                try:
                    resp = api_hook.run("/medlp/annotate/phi", data=json.dumps(record[blobid]['original_note']),
                                        headers={"Content-Type": "application/json"})
                    record[blobid]['annotated_note'] = json.loads(resp.content)
                    annotation_status = 'successful'

                    batch_records.append(record)
                except Exception as e:
                    annotation_status = 'failed'

                pg_hook.run(tgt_update_stmt,
                            parameters=(annotation_status, datetime.now(), run_id, hdcpupdatedate, blobid[0]))

                send_notes_to_brat(clinical_notes=batch_records, datefolder=datefolder)

                record_processed += 1

    tgt_update_stmt = "UPDATE af_runs SET job_end = %s, job_status = 'completed' WHERE af_runs_id = %s"
    pg_hook.run(tgt_update_stmt, parameters=(datetime.now(), run_id))


generate_job_id = \
    PythonOperator(task_id='generate_job_id',
                   provide_context=True,
                   python_callable=generate_job_id,
                   dag=dag)

populate_blobid_in_job_table = \
    PythonOperator(task_id='populate_blobid_in_job_table',
                   provide_context=True,
                   python_callable=populate_blobid_in_job_table,
                   dag=dag)

annotate_clinical_notes = \
    PythonOperator(task_id='annotate_clinical_notes',
                   provide_context=True,
                   python_callable=annotate_clinical_notes,
                   dag=dag)

generate_job_id >> populate_blobid_in_job_table >> annotate_clinical_notes
