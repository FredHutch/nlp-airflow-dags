import json
import subprocess
from datetime import datetime, timedelta

import utilities.job_states as job_states
import utilities.common as common
from airflow.operators.email_operator import EmailOperator
from operators.trashman import trashman_utilities
from operators.trashman.common_vars import REDRIVE_RUN_ID, REDRIVE_COMPLETE_BRAT_TABLE, REDRIVE_SOURCE_BRAT_TABLE


def check_brat_completeness(upstream_task, **kwargs):
    """
    Checks contents in brat for based on modification times within brat file system.
    """
    (run_id, date_stamp) = kwargs['ti'].xcom_pull(task_ids=upstream_task)
    #TODO: Generate a job_id and pair with staleness check from DB.
    check_date = trashman_utilities.safe_datetime_strp(date_stamp, '%Y-%m-%d')
    complete_brat_files = _get_complete_brat_notes_from_db()
    if not complete_brat_files:
        print("No completed brat files found to be deleted.")
        return

    write_run_details(run_id, check_date, complete_brat_files)

def _get_complete_brat_notes_from_db():
    src_select_stmt = ("SELECT b.brat_id, b.last_update_date, b.directory_location, b.hdcorcablobid, b.hdcpupdatedate "
                        "FROM {brat_table} as b "
                        "LEFT JOIN {job_table} as j "
                        "  ON b.brat_id = j.brat_id "
                        "WHERE b.job_status = '{complete_status}' "
                        "AND j.brat_id is NULL ".format(brat_table=REDRIVE_SOURCE_BRAT_TABLE,
                                                        job_table=REDRIVE_COMPLETE_BRAT_TABLE,
                                                        complete_status=job_states.BRAT_READY_TO_EXTRACT))

    completed_notes = (common.AIRFLOW_NLP_DB.get_records(src_select_stmt) or [])
    dict_notes = [{'brat_id': n[0],
                       'last_update_date': n[1],
                       'directory_location':n[2],
                       'hdcorcablobid': n[3],
                       'hdcpupdatedate': n[4]} for n in completed_notes]

    return dict_notes


def write_run_details(run_id, check_date, brat_files, stale_threshold=common.STALE_THRESHOLD):
    """
    Writes run statistics on stale v. nonstale files in brat. Used to track modification over time.
    param: brat_files: list of dicts containing File, ModifiedDate, ElapsedTime, and IsStale
    """
    tgt_insert_stmt = ("INSERT INTO {job_table}"
                       "({run_id}, "
                       "stale_threshold_days,"
                       " stale_check_date,"
                       " directory_location,"
                       " last_modified_date,"
                       " job_status,"
                       " brat_id,"
                       " hdcorcablobid,"
                       " hdcpupdatedate) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(job_table=REDRIVE_COMPLETE_BRAT_TABLE, run_id=REDRIVE_RUN_ID))
    #write job_id, count of stale vs nonstale to db, and threshold parameter
    for file in brat_files:
        common.NLP_DB.run(tgt_insert_stmt,
                          parameters=(run_id,
                                      stale_threshold.days,
                                      check_date,
                                      file['directory_location'],
                                      file['last_modified_date'],
                                      job_states.JOB_RUNNING,
                                      file['brat_id'],
                                      file['hdcorcablobid'],
                                      file['hdcpupdatedate'],
                                      ))