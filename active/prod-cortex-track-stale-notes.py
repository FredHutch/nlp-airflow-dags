from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from active.utilities.common import JOB_RUNNING, JOB_COMPLETE, JOB_FAILURE, REVIEW_BYPASSED_ANNOTATION_TYPE, BRAT_REVIEWED_ANNOTATION_TYPE
from airflow.models import DAG
import active.operators.trashman as trashman

DAG_NAME ='prod-cortex-track-stale-notes'

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
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
                                      dag=dag)

generate_job_id >> check_brat_staleness#>> redrive_stale_notes