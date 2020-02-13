import json
from datetime import datetime, timedelta
import utilities.job_states as job_states
import utilities.common as common


def remove_complete_brat_jobs(upstream_task, **kwargs):
    (run_id, task_info) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    for task in task_info:
        redrive_fn = REDRIVE_TASK_FN[task['type']]
        results = redrive_fn(task)

    return