import json
from datetime import datetime, timedelta
import utilities.job_states as job_states
import utilities.common as common
"""
TODO: Implement redriving capabilities.

"""
def check_resynth_tasks(**kwargs):
    (run_id, task_info) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    return None