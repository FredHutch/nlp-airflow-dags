from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from utilities.common import JOB_RUNNING, JOB_COMPLETE, JOB_FAILURE, REVIEW_BYPASSED_ANNOTATION_TYPE, BRAT_REVIEWED_ANNOTATION_TYPE
from airflow.models import DAG
import operators.trashman as trashman

DAG_NAME ='prod-cortex-track-stale-notes'

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id=DAG_NAME,
	      default_args=args,
	      dagrun_timeout=timedelta(seconds=30))

generate_job_id = PythonOperator(task_id='generate_job_id',
	                             provide_context=True,
	                             python_callable=trashman.generate_job_id,
	                             dag=dag)

check_brat_staleness = PythonOperator(task_id='check_brat_staleness',
                                      provide_context=True,
                                      python_callable=trashman.check_brat_staleness,
                                      op_args={'generate_job_id'},
                                      dag=dag)

report_stale_brat_jobs = PythonOperator(task_id='report_stale_brat_jobs',
                              provide_context=True,
                              python_callable=trashman.report_stale_brat_jobs,
                              op_args='check_brat_staleness',
                              dag=dag)

subject = "Airflow Trashman: Brat Stale Notes Report"
content = """
          {{ task_instance.xcom_pull(task_ids='report_stale_brat_jobs', key='email_body') }}
          """
send_stale_brat_email = trashman.generate_email_operator(dag, 'send_stale_brat_email', subject, content)

remove_complete_brat_jobs = PythonOperator(task_id='remove_complete_brat_jobs',
                              provide_context=True,
                              python_callable=trashman.remove_complete_brat_jobs,
                              op_args='check_brat_staleness',
                              dag=dag)

redrive_jobs = PythonOperator(task_id='redrive_jobs',
                              provide_context=True,
                              python_callable=trashman.redrive_jobs,
                              dag=dag)

generate_job_id >> check_brat_staleness >> report_stale_brat_jobs >> send_stale_brat_email