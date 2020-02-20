import json
import subprocess
from datetime import datetime, timedelta

import utilities.job_states as job_states
import utilities.common as common
import utilities.job_states as job_states
from operators.trashman import trashman_utilities
from airflow.operators.email_operator import EmailOperator
from operators.trashman.common_vars import REDRIVE_RUN_ID, REDRIVE_STALE_BRAT_TABLE


def check_brat_staleness(upstream_task, **kwargs):
    """
    Checks contents in brat for based on modification times within brat file system.
    :param upstream_task: the string task_id of the task to pull XCOM's from
    """
    (run_id, date_stamp) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    #TODO: Generate a job_id and pair with staleness check from DB.
    check_date = trashman_utilities.safe_datetime_strp(date_stamp, '%Y-%m-%d')
    

    #list all the files ending with ann and find the modified dates
    remote_command = r"find {dir} -type f -name *.ann -printf '%TY-%Tm-%Td\t%p\n' 2>&1 | grep -v 'Permission denied'".format(dir=common.BRAT_NLP_FILEPATH)
    #output of check_output is in bytes
    output = subprocess.getoutput('ssh -o StrictHostKeyChecking=no -p {} {}@{} "{}"'.format(common.BRAT_SSH_HOOK.port,
                                                                                          common.BRAT_SSH_HOOK.username,
                                                                                    common.BRAT_SSH_HOOK.remote_host,
                                                                                    remote_command))
    #converte bytes to str/datetime datatypes
    parsed_output  = trashman_utilities.parse_remote_output(output, check_date)
    brat_files = trashman_utilities.compare_dates(parsed_output)
    stale_brat_files = [f for f in brat_files if  f['IsStale'] == 1]
    print("{} Brat Files Checked".format(len(brat_files)))
    print("{} Stale Brat Files Found".format(len(stale_brat_files)))
    write_run_details(run_id, check_date, brat_files)

    return run_id, date_stamp, check_date, stale_brat_files


def write_run_details(run_id, check_date, brat_files, stale_threshold=common.STALE_THRESHOLD):
    """
    Writes run statistics on stale v. nonstale files in brat. Used to track modification over time.
    param: brat_files: list of dicts containing File, ModifiedDate, ElapsedTime, and IsStale
    """
    tgt_insert_stmt = ("INSERT INTO {job_table} "
                       "({run_id}, stale_threshold_days, stale_check_date, directory_location, last_modified_date, job_status) "
                       "VALUES (%s, %s, %s, %s, %s, %s)".format(job_table=REDRIVE_STALE_BRAT_TABLE, run_id=REDRIVE_RUN_ID))
    brat_capacity = len(brat_files)
    #get count of stale v. nonstale in current run
    stale_count = sum(d.get('IsStale') for d in brat_files)
    print("{} Stale Brat Jobs Found".format(stale_count))
    non_stale_count = brat_capacity - stale_count
    #write job_id, count of stale vs nonstale to db, and threshold parameter
    for file in [f for f in brat_files if f['IsStale']]:
        common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                          parameters=(run_id,
                                      stale_threshold.days,
                                      check_date,
                                      file['File'],
                                      file['ModifiedDate'],
                                      job_states.JOB_RUNNING),
                                  autocommit=True)
