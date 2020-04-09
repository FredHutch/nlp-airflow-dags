import json
from datetime import datetime

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions
#TODO: add prospective BERT endpoint in utilities?


def run_bort_task(**kwargs):
    # get last update date
    (jobs_list) = kwargs['ti'].xcom_pull(task_ids='populate_blobid_in_job_table')
    print("job tuple: {}".format(jobs_list))
    print("job_tuple indices: {}".format(len(jobs_list)))
    for run_id, blobid, hdcpupdatedate in jobs_list:
        # record number of NER tasks
        record_processed = 0
        #TODO: Code to run to bort from storage and to relation extraction stuff


def _update_bort_run_details(run_id, blobid, hdcpupdatedate, state):
    tgt_update_stmt = "UPDATE {table} " \
                      "SET bort_status = %s, bort_date = %s " \
                      "WHERE {run_id} = %s " \
                      "AND hdcpupdatedate = %s and hdcorcablobid in (%s) ".format(table=common_variables.AF6_RUNS_DETAILS,
                                                                                  run_id=common_variables.AF6_RUNS_ID)

    print("updating blob {} to {}".format(blobid, state))

    return common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                     parameters=(state, datetime.now(), run_id, hdcpupdatedate, blobid))


def _update_bort_runs(run_id, state):
    tgt_update_stmt = "UPDATE {table} SET job_end = %s, job_status = %s WHERE {run_id} = %s".format(table=common_variables.AF6_RUNS,
                                                                                                    run_id=common_variables.AF6_RUNS_ID)
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), state, run_id), autocommit=True)

def _update_job_id_as_failed(run_id):
    tgt_update_stmt = "UPDATE {table} SET job_end = %s, job_status = %s WHERE {run_id} = %s".format(table=common_variables.AF6_RUNS,
                                                                                                        run_id=common_variables.AF6_RUNS_ID)
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), common_variables.NLP_bort_FAILED, run_id), autocommit=True)
