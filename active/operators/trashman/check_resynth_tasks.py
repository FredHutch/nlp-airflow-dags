import json
from datetime import datetime, timedelta

import utilities.common_variables as common_variables

from utilities.common_hooks import AIRFLOW_NLP_DB

'''
TODO: 
this clause DOES NOT take into account (hdcorcablobid,hdcpupdatedate) jobs which failed and then later succeeded.
Need to account for this somehow
'''
REDRIVEABLE_CLAUSE = ("(resynth_status = {scheduled_var} AND DATE_PART('DAY', NOW() - annotation_creation_date) >= {delta}) "
                     " OR resynth_status = {failure_var} ".format(scheduled_var=common_variables.JOB_RUNNING,
                                   delta=common_variables.RESYNTH_REDRIVE_THRESHOLD,
                                   failure_var=common_variables.JOB_FAILURE))

SQL_FETCH_AF3_REDRIVABLE_JOBS = ("SELECT hdcorcablobid"
                                 ", hdcpupdatedate"
                                 ", MAX(annotation_creation_date) "
                                 "FROM af3_runs_details "
                                 "WHERE {clause} "
                                 "GROUP BY hdcorcablobid, hdcpupdatedate".format(clause=REDRIVEABLE_CLAUSE))


def check_resynth_tasks(upstream_task, **kwargs):
    """
    Checks af3 db for redrivable jobs based on job status
    :param upstream_task: the string task_id of the task to pull XCOM's from
    """
    (run_id, date_stamp) = kwargs['ti'].xcom_pull(task_ids=upstream_task)


    redriveable_jobs = get_redriveable_records()
    if not redriveable_jobs:
        print("No redriveable resynthesis jobs found. Exiting.")
        exit(0)

    for job in redriveable_jobs:
        kwargs['ti'].xcom_push(key='redriveable_resynth_tasks', value=(run_id, date_stamp, job))

    return run_id, date_stamp, redriveable_jobs

def get_redriveable_records():
    records = AIRFLOW_NLP_DB.get_records(SQL_FETCH_AF3_REDRIVABLE_JOBS)
    if records is None:
        return None

    results = [{"hdcorcablobid": result[0],
                "hdcpupdatedate": result[1],
                "annotation_creation_date": result[2]}
                for result in records]

    return results

