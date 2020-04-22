from datetime import datetime

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
from airflow.operators.python_operator import PythonOperator

def generate_job_id(**kwargs):
    """
    return job id since last task (BORT) run
    """
    last_run_id = _get_last_bort_run_id()
    #TODO implement _get_last_bort_update_date()
    last_bort_update_date = _get_last_resynth_update_date()

    if last_run_id is None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    if last_bort_update_date is None:
        last_bort_update_date = common_variables.EPOCH
    print("starting batch run ID: {id} of blobs since {date}".format(id=new_run_id,
                                                                     date=last_bort_update_date))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    bortdates = []

    blob_job_queue = _get_blobs_since_date(date=last, job_state=common_variables.JOB_COMPLETE)

    #check the queue for any new jobs that need to be scheduled
    if blob_job_queue is None:
      exit()
    else:
      for row in blob_job_queue:
        bortdates.append(row[1])
    return new_run_id, bortdates


def _insert_bort_scheduled(run_id, update_date, job_start_date, **kwargs):
    """
    :param run_id: run id
    :param job_start_date: ner job start date
    :param update_date:  bortdate from the af_bort_runs_details table where bortstatus == complete
    """
    tgt_insert_stmt = "INSERT INTO {table}" \
                      "({run_id}, job_status, job_start, bortdate) " \
                      "VALUES (%s, %s, %s, %s)".format(table=common_variables.AF6_RUNS,
                                                       run_id=common_variables.AF6_RUNS_ID)
    common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, common_variables.JOB_RUNNING, job_start_date, update_date))

    return

def _get_blobs_since_date(date, job_state, **kwargs):
  """
  :param date:
  :param job_state:
  """

def _get_last_bort_run_id(**kwargs):
    tgt_select_stmt = "SELECT max({run_id}) FROM {table}".format(table=common_variables.AF6_RUNS,
                                                                 run_id=common_variables.AF6_RUNS_ID)
    last_run_id = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]


def _get_last_bort_update_date(**kwargs):
    tgt_select_stmt = "SELECT max(bortdate) " \
                      "FROM {table} " \
                      "WHERE job_status = %s".format(table=common_variables.AF6_RUNS)
    last_bort_update_date = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                            parameters=(common_variables.NLP_BORT_COMPLETE,)) or (None,))

    return last_bort_update_date[0]

def generate_job_id_operator(dag, default_args):
    generate_job_id_operator = \
        PythonOperator(task_id='{}_{}'.format(dag.task_id, 'generate_job_id'),
                       provide_context=True,
                       python_callable=generate_job_id,
                       )

    return generate_job_id_operator

