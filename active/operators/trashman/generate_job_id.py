import active.utilities.job_states as job_states
import active.utilities.common as common
from datetime import datetime
from . import REDRIVE_RUN_ID, REDRIVE_RUNS_TABLE


def generate_job_id(**kwargs):
    """
    return job id since last task (NER) run
    """
    last_run_id = _get_last_redrive_run_id() or 0
    last_run_id += 1

    job_start_date = datetime.now().strftime(common.DT_FORMAT)[:-3]

    _insert_redrive_scheduled(last_run_id, job_start_date)

    return last_run_id


def _get_last_redrive_run_id():
    return common.get_last_run_id(REDRIVE_RUNS_TABLE, REDRIVE_RUN_ID)


def _insert_redrive_scheduled(run_id, job_start_date, **kwargs):
    """
    :param job_start_date: ner job start date
    """
    tgt_insert_stmt = ("INSERT INTO {run_table} " 
                      "({run_id}, job_status, job_start) " 
                      "VALUES (%s, %s, %s)".format(run_table=REDRIVE_RUNS_TABLE, run_id=REDRIVE_RUN_ID))
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, job_states.JOB_RUNNING, job_start_date))

    return