from datetime import datetime

import utilities.common_hooks as common_hooks
import utilities.common_variables as common_variables

<<<<<<< HEAD
def populate_blobid_in_job_table(**kwargs):
    # get last update date
    #bortdates instead of resynth dates?
    (run_id, bortdates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    print('run_id: ', run_id)

    # get record id to be processed

    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, bort_date " \
                           "FROM {table}  " \
                           "WHERE bort_status = %s".format(table=common_variables.AF6_RUNS_DETAILS)
    complete_job_rows = common_hooks.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(common_variables.JOB_COMPLETE,))
    print('complete_job_rows, ', complete_job_rows)
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = "INSERT INTO {table} " \
                      "({run_id}, hdcpupdatedate, hdcorcablobid, bort_status, bort_date) " \
                      "VALUES (%s, %s, %s, %s, %s, %s) ".format(table=common_variables.AF6_RUNS_DETAILS,
                                                                run_id=common_variables.AF6_RUNS_ID)

    jobs_list = []

    return jobs_list
=======
>>>>>>> b971b26922305b16fba3df263b1a220425ce9d97
