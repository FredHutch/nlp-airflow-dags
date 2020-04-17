import utilities.common_variables as common_variables


def redrive_jobs(**kwargs):
    upstream_task = kwargs['upstream_task']
    (run_id, task_info) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    for task in task_info:
        redrive_fn = common_variables.REDRIVE_TASK_FN[task['type']]
        results = redrive_fn(task)

    return


def redrive_brat_stale_task(task):

    return


def redrive_resynth_jobs(upstream_task, **kwargs):
    job_tuples =  kwargs['ti'].xcom_pull(task_ids=upstream_task, key=common_variables.REDRIVEABLE_RESYNTH_ID)

    resynth_jobs = [(job['af3_runs_id'], job['annotation_creation_date']) for _, _, job in job_tuples]
    kwargs['ti'].xcom_push(key=common_variables.RESYNTH_JOB_ID, value=resynth_jobs)

    return True


def redrive_deid_task(task):
    return True