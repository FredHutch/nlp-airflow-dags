import json
from datetime import datetime, timedelta
import utilities.job_states as job_states
import utilities.common as common
from operators.trashman.common_vars import REDRIVE_TASK_FN

STALE_BRAT_EMAIL_PREAMBLE = \
"""
The following files are over {threshold} old, and are considered STALE!
Please check the following files in Brat:\n
""".format(threshold=common.STALE_THRESHOLD)


def report_stale_brat_jobs(upstream_task, **kwargs):
    (run_id, date_stamp, check_date, stale_brat_files) = kwargs['ti'].xcom_pull(task_ids=upstream_task)

    email_meat = "\n".join(["{fp}    -    {elapsed_time} old".format(fp=stale_file['File'], elapsed_time=stale_file['ElapsedTime']) for stale_file in stale_brat_files])
    email_body = "\n".join([STALE_BRAT_EMAIL_PREAMBLE, email_meat])
    kwargs['ti'].xcom_push(key='email_body', value=email_body)

    return email_body