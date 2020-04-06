from datetime import datetime, timedelta
from collections import defaultdict
import json
import subprocess
import base64

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymssql import OperationalError

from operators.identify_phi import dequeue_batch_blobid_from_process_queue,\
                                   send_notes_to_brat, \
                                   requeue_blobid_to_process_queue, \
                                   save_deid_annotations, \
                                   save_unreviewed_annotations, \
                                   save_note_to_temp_storage, \
                                   save_person_info_to_temp_storage

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions

args = {
    'owner': 'wchau',
    'depends_on_past': False,
    'start_date': datetime(2019,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'schedule_interval':'* */1 * * *'
}

dag = DAG(dag_id='af1-dequeue-and-identify-phi',
          catchup=False,
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def generate_job_id(**kwargs):
    # get last update date from last completed run
    tgt_select_stmt = "SELECT max(HDCPUpdateDate) FROM {table} WHERE job_status = %s".format(table=common_variables.AF1_RUNS)
    update_date_from_last_run = common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt, parameters=(common_variables.JOB_COMPLETE,))[0]

    print("last updatedate was {}".format(update_date_from_last_run))
    if update_date_from_last_run is None:
        # first run
        update_date_from_last_run = common_variables.EPOCH
        print("no updatedate found. falling back on epoch of {}".format(update_date_from_last_run))
    tgt_select_stmt = "SELECT max({run_id}) FROM {table}".format(table=common_variables.AF1_RUNS, run_id=common_variables.AF1_RUNS_ID)
    last_run_id = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0] or 0)
    new_run_id = last_run_id + 1

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_tuples = dequeue_batch_blobid_from_process_queue(common_variables.MAX_BATCH_SIZE)
    blobid, hdcpupdatedate = job_tuples[0] #peak at first result
    if blobid is None:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    tgt_insert_stmt = ("INSERT INTO {table} "
                      "({run_id}, HDCPUpdateDate, record_counts, job_start, job_status) "
                      "VALUES (%s, %s, %s, %s, %s)".format(table=common_variables.AF1_RUNS,
                                                           run_id=common_variables.AF1_RUNS_ID)
                       )

    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]

    print("Job Batch: {}  contains {} notes."
          " {} total notes scheduled".format(new_run_id, len(job_tuples), len(job_tuples)))
    for i, (blobid, hdcpupdatedate) in enumerate(job_tuples):
        common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(new_run_id, hdcpupdatedate, 1, job_start_date, common_variables.JOB_RUNNING))


    return new_run_id, job_tuples


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, job_tuples) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')   # get completed jobs so that we do not repeat completed work

    tgt_insert_stmt = ("INSERT INTO {table} ({run_id}, HDCPUpdateDate, HDCOrcaBlobId, annotation_status) "
                      "VALUES (%s, %s, %s, %s)".format(table=common_variables.AF1_RUNS_DETAILS,
                                                       run_id=common_variables.AF1_RUNS_ID))

    for i, (blobid, hdcpupdatedate) in enumerate(job_tuples):
        common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                                        parameters=(run_id, hdcpupdatedate, blobid, common_variables.JOB_RUNNING))

    return run_id, job_tuples


def annotate_clinical_notes(**kwargs):
    # get last update date
    (run_id, job_tuple) = kwargs['ti'].xcom_pull(task_ids='populate_blobid_in_job_table')
    tgt_select_stmt = ("SELECT HDCOrcaBlobId "
                      "FROM {table} "
                      "WHERE {run_id} = %s "
                      "AND HDCPUpdateDate = %s "
                      "AND annotation_status = %s".format(table=common_variables.AF1_RUNS_DETAILS,
                                                          run_id=common_variables.AF1_RUNS_ID))
    tgt_update_stmt = ("UPDATE {table} "
                      "SET annotation_status = %s, annotation_date = %s "
                      "WHERE {run_id} = %s "
                      "AND HDCPUpdateDate = %s "
                      "AND HDCOrcaBlobId in (%s)".format(table=common_variables.AF1_RUNS_DETAILS,
                                                         run_id=common_variables.AF1_RUNS_ID))


    batch_records = {}
    for i, (blobid, hdcpupdatedate) in enumerate(job_tuple):
        try:
            note_metadata = common_functions.get_note_and_metadata_dict_from_source(blobid, hdcpupdatedate)
            patient_data = common_functions.get_patient_data_from_source(note_metadata["patient_id"])

        except OperationalError as e:
            message = ("A OperationalError occurred while trying to get person and note data from source for"
                               " for blobid: {blobid}".format(blobid=blobid))
            _log_failure_and_reqeue(message, "NOTE AND PERSON RETRIEVAL", run_id, blobid, hdcpupdatedate)
            continue

        if note_metadata is None or note_metadata["patient_id"] is None or patient_data is None:
            message = "Exception occurred: No PatientID found for BlobId, HDCPUpdateDate: {id},{date}".format(
                id=blobid, date=hdcpupdatedate)
            _log_failure_and_reqeue(message, "Flask DeID API", run_id, blobid, hdcpupdatedate)
            continue

        # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
        record = dict()
        batch_records[blobid] = dict()
        record['original_note'] = {"extract_text": "{}".format(note_metadata["blob_contents"]),
                                   "annotation_by_source": True}
        record['hdcpupdatedate'] = hdcpupdatedate
        try:
            resp = common_hooks.DEID_NLP_API_HOOK.run("/identifyphi", data=json.dumps(record['original_note']),
                                headers={"Content-Type": "application/json"})
            record['annotated_note'] = json.loads(resp.content)
            annotation_status = common_variables.JOB_COMPLETE
            batch_records[blobid] = record

        except Exception as e:
            message = ("An Exception occurred while trying to store temp note to source for"
                       " for blobid: {blobid} {error}".format(blobid=blobid, error=e))
            _log_failure_and_reqeue(message, "Flask DeID API", run_id, blobid, hdcpupdatedate)
            continue

        common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                  parameters=(annotation_status,
                                              datetime.now().strftime(common_variables.DT_FORMAT)[:-3],
                                              run_id,
                                              hdcpupdatedate,
                                              blobid))
        try:
            save_note_to_temp_storage(blobid, hdcpupdatedate, note_metadata)
        except OperationalError as e:
            message = ("A OperationalError occurred while trying to store temp note to source for"
                       " for blobid: {blobid} {error}".format(blobid=blobid, error=e))
            _log_failure_and_reqeue(message, "TEMP NOTE STORAGE", run_id, blobid, hdcpupdatedate)
            continue

        try:
            save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data)
        except OperationalError as e:
            message = ("A OperationalError occurred while trying to store person data to source for"
                       " for blobid: {blobid} {error}".format(blobid=blobid, error=e))
            _log_failure_and_reqeue(message, "TEMP PERSON STORAGE", run_id, blobid, hdcpupdatedate)
            continue

    to_review, skip_review = split_records_by_review_status(batch_records)

    #Notes for Review
    assignment = _divide_tasks(to_review, common_variables.BRAT_ASSIGNEE)
    print('ASSIGNMENTS: {}'.format(assignment))
    for assignee, to_review_by_assignee in assignment.items():
        send_notes_to_brat(clinical_notes=to_review_by_assignee,
                           datafolder='{assignee}'.format(assignee=assignee))

        for blobid, record in to_review_by_assignee.items():
            try:
                common_functions.save_deid_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))

            except OperationalError as e:
                message = ("A OperationalError occurred while trying to store deid annotations to source for"
                           " for blobid: {blobid} {error}".format(blobid=blobid, error=e))
                _log_failure_and_reqeue(message, "DEID ANNOTATION STORAGE", run_id, blobid, record['hdcpupdatedate'])
                continue

    # Notes Without Review
    for blobid, record in skip_review.items():
        try:
            common_functions.save_unreviewed_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))
        except OperationalError as e:
            message = ("A OperationalError occurred while trying to store unreviewed deid annotations to source for"
                       " for blobid: {blobid} {error}".format(blobid=blobid, error=e))
            _log_failure_and_reqeue(message, "DEID UNREVIEWED ANNOTATION STORAGE", run_id, blobid, record['hdcpupdatedate'])


    tgt_update_complete_stmt = ("UPDATE {table} " 
                                "SET job_end = %s, job_status = %s " 
                                "WHERE {run_id} = %s".format(table=common_variables.AF1_RUNS,
                                                   run_id=common_variables.AF1_RUNS_ID))
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_complete_stmt, parameters=(datetime.now().strftime(common_variables.DT_FORMAT)[:-3],
                                     common_variables.JOB_COMPLETE, run_id))


def split_records_by_review_status(records):
    records_to_review = dict()
    records_without_review = dict()

    for blobid, record in records.items():
        if _review_criterion(record):
            records_to_review[blobid] = record
        else:
            records_without_review[blobid] = record
    return records_to_review, records_without_review


def _log_failure_and_reqeue(message, error_type, run_id, blobid, hdcpupdatedate):
    print(message)
    common_functions.log_error_and_failure_for_deid_note_job(run_id,
                                                             blobid,
                                                             hdcpupdatedate,
                                                             message,
                                                             error_type)
    requeue_blobid_to_process_queue(blobid, hdcpupdatedate)


def _review_criterion(record):
    # TODO: this is where annotation-specific logic should go for determining true (needs review)
    # TODO: or False (no review required)
    if True:
        return True

    return False

def _divide_tasks(records, assignees):
    """
    split list into n parts of approximately equal length
    :return: defaultdict(dict, {assignee: blobs json})
    """

    if not assignees:
        print('No brat assignees were found, assigning to "ALL USERS" by default')
        return {common_variables.BRAT_ASSIGNEE: records}
    i=0
    split_dicts = defaultdict(dict)
    for k,v in records.items():
        split_dicts[assignees[i%len(assignees)]][k] = v
        i+=1

    return split_dicts

generate_job_id = \
    PythonOperator(task_id='generate_job_id',
                   provide_context=True,
                   python_callable=generate_job_id,
                   dag=dag)

populate_blobid_in_job_table = \
    PythonOperator(task_id='populate_blobid_in_job_table',
                   provide_context=True,
                   python_callable=populate_blobid_in_job_table,
                   dag=dag)

annotate_clinical_notes = \
    PythonOperator(task_id='annotate_clinical_notes',
                   provide_context=True,
                   python_callable=annotate_clinical_notes,
                   dag=dag)

generate_job_id >> populate_blobid_in_job_table >> annotate_clinical_notes
