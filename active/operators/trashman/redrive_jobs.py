import json
from datetime import datetime, timedelta
import utilities.job_states as job_states
import utilities.common as common
from operators.trashman.common_vars import REDRIVE_TASK_FN
"""
TODO: Implement redriving capabilities.
"""


def redrive_jobs(**kwargs):
    upstream_task = kwargs['upstream_task']
    (run_id, task_info) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    for task in task_info:
        redrive_fn = REDRIVE_TASK_FN[task['type']]
        results = redrive_fn(task)

    return


def _redrive_brat_stale_task(task):

    return


def _redrive_resynth_task(task):
    return True


def _redrive_deid_task(task):
    return True