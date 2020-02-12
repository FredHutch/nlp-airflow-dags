import json
import subprocess
from datetime import datetime, timedelta

import utilities.job_states as job_states
import utilities.common as common
from airflow.operators.email_operator import EmailOperator
from operators.trashman.common_vars import REDRIVE_RUN_ID, REDRIVE_JOBS_TABLE

def check_brat_staleness(upstream_task, **kwargs):
    """
    Checks contents in brat for based on modification times within brat file system.
    """
    upstream_task = kwargs['upstream_task']
    (run_id, date_stamp) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    #TODO: Generate a job_id and pair with staleness check from DB.
    check_date = datetime.strptime(date_stamp, '%Y-%m-%d')
    

    #list all the files ending with ann and find the modified dates
    remote_command = """find {dir} -type f -name \*.ann -printf '%TY-%Tm-%Td:%TT\t%p\n'""".format(dir=common.BRAT_NLP_FILEPATH)
    #output of check_output is in bytes
    output = subprocess.check_output(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(common.BRAT_SSH_HOOK.port),
                 "{}@{}".format(common.BRAT_SSH_HOOK.username, common.BRAT_SSH_HOOK.remote_host), remote_command])
    #converte bytes to str/datetime datatypes
    parsed_output  = parse_remote_output(output, check_date)
    brat_files = compare_dates(parsed_output)
    stale_brat_files = [f for f in brat_files if  f['IsStale'] == 1]
    write_run_details(run_id, check_date, brat_files)

    return run_id, date_stamp, check_date, stale_brat_files


def parse_remote_output(remote_command_output, check_date):
    """
    Checks contents in brat for staleness.

    param: remote_command_output - UTF-8 encoded output from a subprocess call

    Returns:
        brat_files: list of dictionaries containing ModifiedDates and the absolute path +
        and number of days elapsed from today
    """

    #decode subprocess output from bytes to str
    brat_files = []
    decoded = remote_command_output.decode("utf-8")
    #parse into list of dictionaries
    for line in decoded.splitlines():
        split_string = line.split('\t')
        modified_date, path = datetime.strptime(split_string[0], '%Y-%m-%d'), split_string[1]
        files = {
                 'File': path, 
                 'ModifiedDate': modified_date, 
                 'ElapsedTime': check_date - modified_date
                }
        brat_files.append(files)
    return brat_files


def compare_dates(brat_files, stale_threshold=common.STALE_THRESHOLD):
    """
    Compares dates with predefined airflow threshold (days)
    """
    for file in brat_files:
        file.update({'IsStale': (1 if file.get('ElapsedTime') > stale_threshold else 0)})
    return brat_files


def write_run_details(run_id, check_date, brat_files, stale_threshold=common.STALE_THRESHOLD):
    """
    Writes run statistics on stale v. nonstale files in brat. Used to track modification over time.
    param: brat_files: list of dicts containing File, ModifiedDate, ElapsedTime, and IsStale
    """
    tgt_insert_stmt = ("INSERT INTO {job_table}"
                       "({run_id}}, brat_capacity, stale_count, non_stale_count, stale_threshold, stale_check_date)"
                       "VALUES (%s, %s, %s, %s, %s, %s)".format(job_table=REDRIVE_JOBS_TABLE, run_id=REDRIVE_RUN_ID))
    brat_capacity = len(brat_files)
    #get count of stale v. nonstale in current run
    stale_count = sum(d.get('IsStale') for d in brat_files)
    non_stale_count = brat_capacity - stale_count
    #write job_id, count of stale vs nonstale to db, and threshold parameter
    common.NLP_DB.run(tgt_insert_stmt,
                      parameters=(run_id, brat_capacity, stale_count, non_stale_count, stale_threshold, str(check_date)))


def notify_email(context, **kwargs):
    """
    Sends a notification email to the service account.
    TODO: Redo tests on email operator
    TODO: Move this to its own 'operators' module for reuse.
    """
    alert = EmailOperator(
        task_id=alertTaskID,
        #for testing purposes+
        to='rlong@fredhutch.org',
        subject='Stale Annotations Report',
        html_content='test',
        dag=dag
    )
    title = ("Trashman Report: Stale Files in Brat for {trashman_run}".format(trashman_run=str(datetime.now())))
    body = json.dumps(context)
    #execute email
