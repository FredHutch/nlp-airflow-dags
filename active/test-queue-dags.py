from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from utilities.job_states import JOB_RUNNING, JOB_COMPLETE, JOB_FAILURE, REVIEW_BYPASSED_ANNOTATION_TYPE, BRAT_REVIEWED_ANNOTATION_TYPE
from airflow.models import DAG
import operators.trashman as trashman

DAG_NAME ='queue-dags'

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

enqueue = PythonOperator(task_id='enqueue',
	                             provide_context=True,
	                             python_callable=enqueue_job,
	                             dag=dag)
dequeue = PythonOperator(task_id='dequeue',
                               provide_context=True,
                               python_callable=dequeue_job,
                               dag=dag)

dequeue >> enqueue

def enqueue_job():

  pass

def dequeue_job():
  pass