from datetime import datetime, timedelta

from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
import active.operators.ner as ner

DAG_NAME = 'prod-cortex-ner-tasks-on-resynthed-notes'
CHILD_DAG_NAME = 'populate_blobid_in_job_table'

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

generate_job_id = \
    PythonOperator(task_id='generate_job_id',
                   provide_context=True,
                   python_callable=ner.generate_job_id.generate_job_id,
                   dag=dag)

call_flask_blob_nlp = \
    PythonOperator(task_id='call_flask_blob_nlp',
                   provide_context=True,
                   python_callable=ner.ner._call_flask_blob_nlp,
                   dag=dag)

populate_blobid_in_job_table_operator = ner.populate_blobid_in_job_table.populate_blobid_in_job_table(dag=dag, default_args=args)

run_ner_tasks_and_save_to_source = \
    SubDagOperator(task_id=CHILD_DAG_NAME,
                   provide_context=True,
                   subdag=ner.run_ner_tasks_and_save_to_source_operator(main_dag=dag,
                                                                        parent_dag_name=dag.dag_id,
                                                                        child_dag_name=CHILD_DAG_NAME),
                   dag=dag)



generate_job_id >> call_flask_blob_nlp >> populate_blobid_in_job_table_operator >> run_ner_tasks_and_save_to_source



