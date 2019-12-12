from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

import active.utilities.common as common
from datetime import datetime

def run_ner_tasks_and_save_to_source(parent_dag_name, child_dag_name, start_date, schedule_interval,  xcom_source, **kwargs):
    (run_id, createddate) = kwargs['ti'].xcom_pull(task_ids=xcom_source)
    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    for x in range(run_id):
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


def _update_ner_run_details(run_id, blobid, date, state):
    tgt_update_stmt = "UPDATE af_ner_runs_details" \
                      "SET ner_status = %s, ner_date = %s" \
                      "WHERE af_ner_runs_id = %s" \
                      "AND hdcpupdatedate = %s and hdcorcablobid in (%s)"

    print("updating blob {} to {}".format(blobid, state))

    return common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                     parameters=(state, datetime.now(), run_id, date, blobid))


def run_ner_tasks_and_save_to_source_operator(main_dag, **kwargs):
    child_dag_name = kwargs.get('child_dag_name')
    xcom_source = kwargs.get('upstream_operator', 'populate_blobid_in_job_table')
    sub_dag = SubDagOperator(
        subdag=run_ner_tasks_and_save_to_source(parent_dag_name=main_dag.task_id,
                                                child_dag_name=child_dag_name,
                                                start_date=main_dag.start_date,
                                                schedule_interval=main_dag.schedule_interval,
                                                xcom_source=xcom_source),
        task_id=child_dag_name,
        dag=main_dag,
    )
    return sub_dag