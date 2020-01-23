from datetime import datetime

import utilities.common as common
import utilities.job_states as job_states


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, datecreated) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    print('run_id: ', run_id)
    print('datecreated: ', datecreated)

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate " \
                      "FROM af_resynthesis_runs_details " \
                      "WHERE resynth_date = %s " \
                      "AND resynth_status = %s "
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, ner_date " \
                           "FROM af_ner_runs_details  " \
                           "WHERE ner_status = %s"
    complete_job_rows = common.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(job_states.JOB_COMPLETE,))
    print('complete_job_rows, ', complete_job_rows)
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = "INSERT INTO af_ner_runs_details " \
                      "(af_ner_runs_id, hdcpupdatedate, hdcorcablobid, annotation_creation_date, ner_status, ner_date) " \
                      "VALUES (%s, %s, %s, %s, %s, %s) "

    for creation_date in datecreated:
        print('creation_date: ', creation_date)
        for row in common.AIRFLOW_NLP_DB.get_records(src_select_stmt, parameters=(creation_date, job_states.JOB_COMPLETE,)):
            print('resyn records: ', row)
            if (row[0], row[1]) in complete_jobs:
                print("Job for note {},{}  originally created on {} has already been completed on {} ."
                      " Skipping.".format(row[0], row[1], creation_date, complete_jobs[(row[0], row[1])]))
                continue

            print("Inserting new note job for blobid {}:{}".format(row[0], row[1]))
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, row[1], row[0],
                                                                   creation_date, job_states.JOB_RUNNING,
                                                                   datetime.now().strftime(common.DT_FORMAT)[:-3]))