import json
from datetime import datetime, timedelta
import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions


def mark_job_complete(upstream_task, **kwargs):
    """
    :param job_start_date: ner job start date
    """

    run_id = kwargs['ti'].xcom_pull(key='completed_job_id')
    job_end_date = common_functions.generate_timestamp()
    print("{run_id} finished. Marking Run Complete.".format(run_id=run_id))
    tgt_update_stmt = ("UPDATE {run_table} "
                       "SET  job_status =%s, job_end = %s "
                       "WHERE {run_id} = %s".format(run_table=common_variables.AF4_RUNS_TABLE, run_id=common_variables.AF4_RUN_ID))

    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                              parameters=(common_variables.JOB_COMPLETE, job_end_date, run_id,), autocommit=True)

