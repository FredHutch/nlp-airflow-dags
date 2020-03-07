from datetime import datetime

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
from airflow.operators.python_operator import PythonOperator


def generate_job_id(**kwargs):
    """
    return job id since last task (NER) run
    """
    last_run_id = _get_last_ner_run_id()
    last_ner_update_date = _get_last_ner_update_date()

    if last_run_id is None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    if last_ner_update_date is None:
        last_ner_update_date = common_variables.EPOCH
    print("starting batch run ID: {id} of blobs since {date}".format(id=new_run_id,
                                                                     date=last_ner_update_date))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    resynthdates = []

    blob_job_queue = _get_blobs_since_date(date=last_ner_update_date, state=common_variables.JOB_COMPLETE)

    if blob_job_queue is None:
        print("No new records found since last update date: {}".format(last_ner_update_date))
        exit()
    else:
        for row in blob_job_queue:
            resynthdates.append(row[1])
            _insert_ner_scheduled(new_run_id, row[1], job_start_date)

        print("{} new update batches found since last update date: {}".format(len(resynthdates), last_ner_update_date))

    return new_run_id, resynthdates


def _insert_ner_scheduled(run_id, update_date, job_start_date, **kwargs):
    """
    :param run_id: run id
    :param job_start_date: ner job start date
    :param update_date:  resynth_date from the af_resynthesis_runs_details table where resynth_status == complete
    """
    tgt_insert_stmt = "INSERT INTO {table}" \
                      "({run_id}, job_status, job_start, resynth_date) " \
                      "VALUES (%s, %s, %s, %s)".format(table=common_variables.AF5_RUNS,
                                                       run_id=common_variables.AF5_RUNS_ID)
    common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, common_variables.JOB_RUNNING, job_start_date, update_date))

    return


def _get_blobs_since_date(date, state, **kwargs):
    """
    :param date: last resynth complete date
    :param state: resynth job complete
    """
    tgt_update_stmt = "SELECT distinct hdcorcablobid, resynth_date " \
                      "FROM {table} " \
                      "WHERE resynth_date >= %s " \
                      "AND resynth_status = %s ".format(table=common_variables.AF3_RUNS_DETAILS)
    return common_hooks.AIRFLOW_NLP_DB.get_records(tgt_update_stmt, parameters=(date, state))


def _get_last_ner_run_id(**kwargs):
    tgt_select_stmt = "SELECT max({run_id}) FROM {table}".format(table=common_variables.AF5_RUNS,
                                                                 run_id=common_variables.AF5_RUNS_ID)
    last_run_id = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]


def _get_last_ner_update_date(**kwargs):
    tgt_select_stmt = "SELECT max(resynth_date) " \
                      "FROM {table} " \
                      "WHERE job_status = %s".format(table=common_variables.AF5_RUNS)
    last_ner_update_date = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                            parameters=(common_variables.NLP_NER_COMPLETE,)) or (None,))

    return last_ner_update_date[0]

def generate_job_id_operator(dag, default_args):
    generate_job_id_operator = \
        PythonOperator(task_id='{}_{}'.format(dag.task_id, 'generate_job_id'),
                       provide_context=True,
                       python_callable=generate_job_id,
                       )

    return generate_job_id_operator
