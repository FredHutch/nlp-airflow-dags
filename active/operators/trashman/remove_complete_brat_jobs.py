import json
import subprocess
from datetime import datetime, timedelta
import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks

def remove_complete_brat_jobs(upstream_task, **kwargs):
    (run_id, completed_files_dict) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    if completed_files_dict is None:
        print("Error! Entered Removal FN without providing tasks to remove")
        exit(1)
    for remote_file in completed_files_dict:
        _remove_remote_file(remote_file['directory_location'])
        _remove_remote_file(remote_file['directory_location'].replace('.ann','.txt'))
    kwargs['ti'].xcom_push(key='completed_job_id', value=run_id)

    return

def _remove_remote_file(remote_file, **kwargs):
    print("attempting to remove file {}".format(remote_file))
    remote_command = r"rm {}".format(remote_file)

    output = subprocess.getoutput(
        'ssh -o StrictHostKeyChecking=no -p {} {}@{} "{}"'.format(common_hooks.BRAT_SSH_HOOK.port,
                                                                  common_hooks.BRAT_SSH_HOOK.username,
                                                                  common_hooks.BRAT_SSH_HOOK.remote_host,
                                                                  remote_command))

    return