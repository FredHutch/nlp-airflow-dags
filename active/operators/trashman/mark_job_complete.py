import json
from datetime import datetime, timedelta
import utilities.job_states as job_states
import utilities.common as common
from operators.trashman.common_vars import REDRIVE_RUNS_TABLE, REDRIVE_RUN_ID


def mark_job_complete(upstream_task, **kwargs):
    """
    :param job_start_date: ner job start date
    """

    run_id = kwargs['ti'].xcom_pull(task_ids=upstream_task, key='completed_job_id')
    job_end_date = common.generate_timestamp()
    tgt_update_stmt = ("UPDATE {run_table} "
                       "SET  job_status =%s, job_end = %s "
                       "WHERE {run_id} = %s".format(run_table=REDRIVE_RUNS_TABLE, run_id=REDRIVE_RUN_ID))

    common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                              parameters=(job_states.JOB_COMPLETE, job_end_date, run_id,))

