from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from operators.ner import generate_job_id, populate_blobid_in_job_table, run_ner_task

DAG_NAME = 'af5-clinical-nlp-basics'
CHILD_DAG_NAME = 'populate_blobid_in_job_table'

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

generate_job_id = \
    PythonOperator(task_id='generate_job_id',
                   provide_context=True,
                   python_callable=generate_job_id,
                   dag=dag)



populate_blobid_in_job_table_operator = \
    PythonOperator(task_id='populate_blobid_in_job_table',
                   provide_context=True,
                   python_callable=populate_blobid_in_job_table,
                   dag=dag)

run_ner_tasks_and_save_to_source = \
    PythonOperator(task_id='run_ner_tasks_and_save_to_source',
                   provide_context=True,
                   python_callable=run_ner_task,
                   dag=dag)


generate_job_id >> populate_blobid_in_job_table_operator >> run_ner_tasks_and_save_to_source



