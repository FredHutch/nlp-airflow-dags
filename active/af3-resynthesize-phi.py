from datetime import datetime, timedelta
import json

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymssql import OperationalError

from operators.resynthesis import get_resynth_ready_annotations_since_date, resynthesize_notes_marked_as_deid

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='af3-resynthesize-phi',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def _insert_resynth_run_job(run_id, update_date, record_count, job_start_date):
    tgt_insert_stmt = ("INSERT INTO {table} "
                       "({run_id}, annotation_creation_date, record_counts, job_start, job_status) "
                       "VALUES (%s, %s, %s, %s, %s)".format(table=common_variables.AF3_RUNS,
                                                            run_id=common_variables.AF3_RUNS_ID))
    common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                                    parameters=(
                                        run_id, update_date, record_count, job_start_date,
                                        common_variables.JOB_RUNNING))

    return


def _get_last_resynth_run_id():
    tgt_select_stmt = "SELECT max({run_id}) FROM {table}".format(table=common_variables.AF3_RUNS,
                                                                 run_id=common_variables.AF3_RUNS_ID)
    last_run_id = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))
    return last_run_id[0]


def _get_last_resynth_update_date():
    tgt_select_stmt = "SELECT max(annotation_creation_date) " \
                      "FROM {table} WHERE job_status = %s ".format(table=common_variables.AF3_RUNS)
    update_date_from_last_run = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                                       parameters=(common_variables.JOB_COMPLETE,))
                                 or (None,))
    return update_date_from_last_run[0]


def generate_job_id(**kwargs):
    # get last update date from last completed run
    update_date_from_last_run = _get_last_resynth_update_date()

    if update_date_from_last_run is None:
        # first run
        update_date_from_last_run = common_variables.EPOCH

    last_run_id = (_get_last_resynth_run_id() or 0)
    new_run_id = last_run_id + 1

    print("starting batch run ID: {id} of annotations since {date}".format(id=new_run_id,
                                                                           date=update_date_from_last_run))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    jobs = []
    datecreated = []
    for row in get_resynth_ready_annotations_since_date(update_date_from_last_run):
        jobs.append((new_run_id,  row['date_created']))
        datecreated.append(row['date_created'])
        _insert_resynth_run_job(new_run_id, row['date_created'], row['count'], job_start_date)

    if len(jobs) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    print("{} new update batches found since last update date: {}".format(len(jobs), update_date_from_last_run))
    kwargs['ti'].xcom_push(key=common_variables.RESYNTH_JOB_ID, value=jobs)

    return new_run_id, datecreated


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, datecreated) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate " \
                      "FROM {table} " \
                      "WHERE date_created = %s".format(table=common_variables.ANNOTATION_TABLE)
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = ("SELECT hdcorcablobid, hdcpupdatedate, resynth_date from {table}  "
                            "WHERE resynth_status = %s".format(table=common_variables.AF3_RUNS_DETAILS))
    complete_job_rows = common_hooks.AIRFLOW_NLP_DB.get_records(screen_complete_stmt,
                                                                parameters=(common_variables.JOB_COMPLETE,))
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}
    tgt_insert_stmt = ("INSERT INTO {table} "

                      "(af3_runs_id, hdcpupdatedate, hdcorcablobid, annotation_creation_date, resynth_status) "
                      "VALUES (%s, %s, %s, %s, %s) ".format(table=common_variables.AF3_RUNS_DETAILS))
    annotations_db = common_hooks.get_annotations_db_hook()

    for creation_date in datecreated:
        try:
            for row in annotations_db.get_records(src_select_stmt, parameters=(creation_date,)):
                if (row[0], row[1]) in complete_jobs:
                    print("Job for note {},{}  originally created on {} has already been completed on {} ."
                          " Skipping.".format(row[0], row[1], creation_date, complete_jobs[(row[0], row[1])]))
                    continue

                print("Inserting new note job for blobid {}:{}".format(row[0], row[1]))
                common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(
                    run_id, row[1], row[0], creation_date, common_variables.JOB_RUNNING))
        except OperationalError as e:
            message = (
                "An OperationalError occured while trying to fetch potential annotations from the source data server"
                " for select statement {stmt} \n {e}".format(stmt=src_select_stmt, e=e))
            print(message)
            common_functions.log_error_and_failure_for_resynth_note_job(run_id, 0,
                                                                        datetime.strptime('1900-01-01', "%Y-%m-%d"),
                                                                        message,
                                                                        common_variables.JOB_FAILURE)


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

resynthesize_notes_marked_as_deid = \
    PythonOperator(task_id='resynthesize_notes_marked_as_deid',
                   provide_context=True,
                   python_callable=resynthesize_notes_marked_as_deid,
                   op_args={'generate_job_id'},
                   dag=dag)

generate_job_id >> populate_blobid_in_job_table >> resynthesize_notes_marked_as_deid
