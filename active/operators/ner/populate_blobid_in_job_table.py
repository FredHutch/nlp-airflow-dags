import active.utilities.common as common
import active.utilities.job_states as job_states

from airflow.operators.python_operator import PythonOperator


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, datecreated) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate FROM annotations WHERE date_created = %s"
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, ner_date from af_ner_runs_details  " \
                           "WHERE ner_status = %s"
    complete_job_rows = common.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(job_states.JOB_COMPLETE,))
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = "INSERT INTO af_ner_runs_details " \
                      "(af_ner_runs_id, hdcpupdatedate, hdcorcablobid, annotation_creation_date, ner_status) " \
                      "VALUES (%s, %s, %s, %s, %s) "

    for creation_date in datecreated:
        for row in common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(creation_date,)):
            if (row[0], row[1]) in complete_jobs:
                print("Job for note {},{}  originally created on {} has already been completed on {} ."
                      " Skipping.".format(row[0], row[1], creation_date, complete_jobs[(row[0], row[1])]))
                continue

            print("Inserting new note job for blobid {}:{}".format(row[0], row[1]))
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, row[1], row[0], creation_date, job_states.JOB_RUNNING))


def populate_blobid_in_job_table_operator(dag, default_args):
    populate_blobid_in_job_table_operator = \
        PythonOperator(task_id='{}_{}'.format(dag.task_id, 'populate_blobid_in_job_table'),
                       provide_context=True,
                       python_callable=populate_blobid_in_job_table,
                       dag=dag)

    return populate_blobid_in_job_table_operator