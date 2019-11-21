from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator


def run_ner_tasks_and_save_to_source(parent_dag_name, child_dag_name, start_date, schedule_interval, kwargs):
    (run_id, createddate) = kwargs['ti'].xcom_pull(task_ids='populate_blobid_in_job_table')
    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    for x in range(value):
        bash_cmd = "echo B{}".format(x)
        task_operator = PythonOperator(
            task_id='ner_task*{}'.format(x),
            provide_context=True,
            python_callable=__run_ner_task_dag,
            op_kwargs={'blobid': run_id, 'hdcpupdatedate': hdcpupdatedate},
            dag=dag,
        )
    return dag

def __run_ner_task_dag(**kwargs):

    return

def run_ner_tasks_and_save_to_source_operator(main_dag, **kwargs):
    child_dag_name = kwargs.get('child_dag_name')
    sub_dag = SubDagOperator(
        subdag=run_ner_tasks_and_save_to_source(parent_dag_name=main_dag.task_id,
                                  child_dag_name=child_dag_name,
                                  start_date=main_dag.start_date,
                                  schedule_interval=main_dag.schedule_interval),
        task_id=child_dag_name,
        dag=main_dag,
    )
    return sub_dag