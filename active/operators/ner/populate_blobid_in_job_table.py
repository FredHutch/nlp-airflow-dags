from datetime import datetime

import utilities.common_hooks as common_hooks
import utilities.common_variables as common_variables

def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, resynthdates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    print('run_id: ', run_id)
    print('datecreated: ', resynthdates)

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate " \
                      "FROM af_resynthesis_runs_details " \
                      "WHERE resynth_date = %s " \
                      "AND resynth_status = %s "
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, ner_date " \
                           "FROM af_ner_runs_details  " \
                           "WHERE ner_status = %s"
    complete_job_rows = common_hooks.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(common_variables.JOB_COMPLETE,))
    print('complete_job_rows, ', complete_job_rows)
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = "INSERT INTO af_ner_runs_details " \
                      "(af_ner_runs_id, hdcpupdatedate, hdcorcablobid, resynth_date, ner_status, ner_date) " \
                      "VALUES (%s, %s, %s, %s, %s, %s) "

    jobs_list = []
    for resynthdate in resynthdates:
        print('resynth_date: ', resynthdate)
        for row in common_hooks.AIRFLOW_NLP_DB.get_records(src_select_stmt, parameters=(resynthdate, common_variables.JOB_COMPLETE,)):
            print('resyn records: ', row)
            blob_id = row[0]
            hdcpupdatedate = row[1]
            if (blob_id, hdcpupdatedate) in complete_jobs:
                print("Job for note {},{}  originally created on {} has already been completed on {} ."
                      " Skipping.".format(blob_id, hdcpupdatedate, resynthdate, complete_jobs[(blob_id, row[1])]))
                continue

            print("Inserting new note job for blobid {}:{}".format(blob_id, hdcpupdatedate))
            common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, blob_id,
                                                                   resynthdate, common_variables.JOB_RUNNING,
                                                                   datetime.now().strftime(common_variables.DT_FORMAT)[:-3]))
            jobs_list.append((run_id, resynthdate, blob_id, hdcpupdatedate))

    return jobs_list