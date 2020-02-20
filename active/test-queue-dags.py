from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
import utilities.common as common
from airflow.models import DAG

DAG_NAME ='test-queue-dags'

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

def enqueue(upstream_task, **kwargs):
  upstream_task = kwargs['upstream_task']
  (run_id, date_stamp) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
  run_id = run_id + '88888' # so i can see this sifting through the queue
  exec_stmt = ("EXEC dbo.sp_requeue_note_id 'dbo.clinical_notes_process_queue', %s, %s")
  common.ANNOTATIONS_DB.run(exec_stmt, parameters=(run_id, date_stamp), autocommit=True)

def dequeue(**kwargs):
  exec_stmt = ("EXEC dbo.sp_dequeue_note_id 'dbo.clinical_notes_process_queue'")
  results = common.ANNOTATIONS_DB.get_first(exec_stmt, autocommit=True)
  print(results)
  return (results[0], results[1])

enqueue = PythonOperator(task_id='enqueue',
	                             provide_context=True,
	                             python_callable=enqueue,
	                             dag=dag)

dequeue = PythonOperator(task_id='dequeue',
                               provide_context=True,
                               python_callable=dequeue,
                               dag=dag)

dequeue >> enqueue

