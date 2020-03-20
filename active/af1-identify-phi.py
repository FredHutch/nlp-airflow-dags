from datetime import datetime, timedelta
from collections import defaultdict
import json
import subprocess
import base64

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymssql import OperationalError

from operators.identify_phi import  send_notes_to_brat
import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions

args = {
    'owner': 'wchau',
    'depends_on_past': False,
    'start_date': datetime(2019,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='af1-identify-phi',
          catchup=False,
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def generate_job_id(**kwargs):
    # get last update date from last completed run
    tgt_select_stmt = "SELECT max(HDCPUpdateDate) FROM {table} WHERE job_status = %s".\
                      format(table=common_variables.AF1_RUNS)
    update_date_from_last_run = common_hooks.AIRFLOW_NLP_DB.\
                      get_first(tgt_select_stmt, parameters=(common_variables.JOB_COMPLETE,))[0].\
                      strftime(common_variables.DT_FORMAT)[:-3]

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
    src_select_stmt = ("SELECT HDCPUpdateDate, count(*) "
                      "FROM vClinicalNoteDiscovery "
                      "WHERE HDCPUpdateDate >= %s "
                      "GROUP BY HDCPUpdateDate")
    tgt_insert_stmt = ("INSERT INTO {table} "
                      "({run_id}, HDCPUpdateDate, record_counts, job_start, job_status) "
                      "VALUES (%s, %s, %s, %s, %s)".format(table=common_variables.AF1_RUNS,
                                                           run_id=common_variables.AF1_RUNS_ID)
                       )

    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    hdcpupdatedates = []
    total_job_count = common_variables.MAX_BATCH_SIZE

    # temp fix: string formatting for the most recent update date
    # (note right now this has to be hard coded to earlier than 2020-01-09 ... or else it will hang)

    temp_date = common_variables.TEMP_DATE
    if update_date_from_last_run >= temp_date:
        update_date_from_last_run = temp_date

    for row in common_hooks.SOURCE_NOTE_DB.get_records(src_select_stmt, parameters=(update_date_from_last_run,)):
        hdcpupdatedates.append(row[0])
        common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(new_run_id, row[0], row[1], job_start_date, common_variables.JOB_RUNNING))
        print("Job Batch for hdcpupdatedate: {}  contains {} notes."
              " {} total notes scheduled".format(row[0], row[1], (common_variables.MAX_BATCH_SIZE - total_job_count)))
        total_job_count -= row[1]
        if total_job_count <= 0:
            print("Job Batch for hdcpupdatedate: {}  contains {} notes"
                  " and exceeds cumulative total per-run Job Size of {}."
                  " Breaking early.".format(row[0], row[1], common_variables.MAX_BATCH_SIZE))
            break

    if len(hdcpupdatedates) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    return new_run_id, hdcpupdatedates


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT top {} HDCOrcaBlobId FROM vClinicalNoteDiscovery WHERE HDCPUpdateDate = %s".\
                    format(common_variables.MAX_BATCH_SIZE)
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = ("SELECT HDCOrcaBlobId, HDCPUpdateDate, annotation_date from {table} "
                           "WHERE annotation_status = %s".format(table=common_variables.AF1_RUNS_DETAILS))
    complete_job_rows = common_hooks.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(common_variables.JOB_COMPLETE,))
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = ("INSERT INTO {table} ({run_id}, HDCPUpdateDate, HDCOrcaBlobId, annotation_status) "
                      "VALUES (%s, %s, %s, %s)".format(table=common_variables.AF1_RUNS_DETAILS,
                                                       run_id=common_variables.AF1_RUNS_ID))

    total_job_count = common_variables.MAX_BATCH_SIZE
    for hdcpupdatedate in hdcpupdatedates:
        for row in common_hooks.SOURCE_NOTE_DB.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            print("checking ID: {} for previous completion.".format(row[0]))
            if (row[0], hdcpupdatedate) in complete_jobs.keys():
                print("Job for note {},{} has already been completed on {} . "
                      "Skipping.".format(row[0], hdcpupdatedate, complete_jobs[(row[0], hdcpupdatedate)]))
                continue

            common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0], common_variables.JOB_RUNNING))
            total_job_count -= 1
            if total_job_count == 0:
                print("Job Batch cumulative total per-run Job Size of {batch_limit}."
                      " Breaking early without processing {blobid}"\
                      .format(batch_limit=common_variables.MAX_BATCH_SIZE, blobid=row[0]))
                break





def save_note_to_temp_storage(blobid, hdcpupdatedate, metadata_dict):

    insert_stmt = ("INSERT INTO TEMP_NOTES (HDCOrcaBlobID, HDCPUpdateDate,"
                   "CLINICAL_EVENT_ID, HDCPersonId, BLOB_CONTENTS,"
                   "SERVICE_DT_TM, INSTITUTION, EVENT_CD_DESCR) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    print("saving metadata to temp storage for {}, {}".format(blobid, hdcpupdatedate))

    common_hooks.ANNOTATIONS_DB.run(insert_stmt, parameters=(blobid,
                                                       hdcpupdatedate,
                                                       metadata_dict["clinical_event_id"],
                                                       metadata_dict["patient_id"],
                                                       metadata_dict["blob_contents"],
                                                       metadata_dict["servicedt"],
                                                       metadata_dict["instit"],
                                                       metadata_dict["cd_descr"]), autocommit=True)


def save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data):
    insert_stmt = ("INSERT INTO TEMP_PERSON "
                  "(HDCOrcaBlobId, HDCPUpdateDate, HDCPersonId, FirstName, MiddleName, LastName) "
                  "VALUES (%s, %s, %s, %s, %s, %s)")
    print("saving person info to temp storage for {}, {}: {}".format(blobid, hdcpupdatedate, patient_data[0]))
    common_hooks.ANNOTATIONS_DB.run(insert_stmt, parameters=(blobid,
                                                       hdcpupdatedate,
                                                       patient_data[0],
                                                       patient_data[1],
                                                       patient_data[2],
                                                       patient_data[3]), autocommit=True)


def annotate_clinical_notes(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
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

    for hdcpupdatedate in hdcpupdatedates:
        batch_records = {}
        hdcpupdatedate = hdcpupdatedate.strftime(common_variables.DT_FORMAT)[:-3]
        for id_row in common_hooks.AIRFLOW_NLP_DB.get_records(tgt_select_stmt,
                                                        parameters=(run_id, hdcpupdatedate, common_variables.JOB_RUNNING)):
            blobid = id_row[0]

            try:
                note_metadata = common_functions.get_note_and_metadata_dict_from_source(blobid, hdcpupdatedate)
            except OperationalError as e:
                message = ("A OperationalError occurred while trying to fetch note metadata from source for"
                           " for blobid: {blobid}".format(blobid=blobid))
                print(message)
                common_functions.log_error_and_failure_for_deid_note_job(run_id,
                                                                         blobid,
                                                                         hdcpupdatedate,
                                                                         message,
                                                                         "Flask ID PHI API")
                continue

            try:
                patient_data = common_functions.get_patient_data_from_source(note_metadata["patient_id"])
            except OperationalError as e:
                message = ("A OperationalError occurred while trying to fetch patient data"
                      " for blobid: {blobid} and patientid: {pid}".format(blobid=blobid, pid=note_metadata["patient_id"]))
                print(message)
                common_functions.log_error_and_failure_for_deid_note_job(run_id,
                                                                         blobid,
                                                                         hdcpupdatedate,
                                                                         message,
                                                                         "Flask ID PHI API")
                continue


            if note_metadata["patient_id"] is None or patient_data is None:
                message = "Exception occurred: No PatientID found for BlobId, HDCPUpdateDate: {id},{date}".format(
                    id=blobid, date=hdcpupdatedate)
                common_functions.log_error_and_failure_for_deid_note_job(run_id,
                                                               blobid,
                                                               hdcpupdatedate,
                                                               message,
                                                               "Flask ID PHI API")
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
                common_functions.log_error_and_failure_for_deid_note_job(run_id, blobid, hdcpupdatedate, e, "Flask ID PHI API")
                continue

            common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                      parameters=(annotation_status,
                                                  datetime.now().strftime(common_variables.DT_FORMAT)[:-3],
                                                  run_id,
                                                  hdcpupdatedate,
                                                  blobid))

            try:
                save_note_to_temp_storage(blobid, hdcpupdatedate, note_metadata)
                save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data)
            except OperationalError as e:
                message = ("A OperationalError occurred while trying to save patient data"
                           " for blobid: {blobid} ".format(blobid=blobid))
                print(message)
                common_functions.log_error_and_failure_for_deid_note_job(run_id,
                                                                         blobid,
                                                                         hdcpupdatedate,
                                                                         message,
                                                                         "Flask ID PHI API")

        to_review, skip_review = split_records_by_review_status(batch_records)

        assignment = _divide_tasks(to_review, common_variables.BRAT_ASSIGNEE)

        for assignee, to_review_by_assignee in assignment.items():
            send_notes_to_brat(clinical_notes=to_review_by_assignee,
                               datafolder='{assignee}'.format(assignee=assignee),
                               hdcpupdatedate=hdcpupdatedate[:10])
            save_deid_annotations(to_review_by_assignee)

        save_unreviewed_annotations(skip_review)

    tgt_update_stmt = "UPDATE {table} " \
                      "SET job_end = %s, job_status = %s " \
                      "WHERE {run_id} = %s".format(table=common_variables.AF1_RUNS,
                                                   run_id=common_variables.AF1_RUNS_ID)
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now().strftime(common_variables.DT_FORMAT)[:-3],
                                                                 common_variables.JOB_COMPLETE, run_id))


def save_deid_annotations(annotation_records):
    for blobid, record in annotation_records.items():
        common_functions.save_deid_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))


def save_unreviewed_annotations(annotation_records):
    for blobid, record in annotation_records.items():
        common_functions.save_unreviewed_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))


def split_records_by_review_status(records):
    records_to_review = dict()
    records_without_review = dict()

    for blobid, record in records.items():
        if _review_criterion(record):
            records_to_review[blobid] = record
        else:
            records_without_review[blobid] = record
    return records_to_review, records_without_review


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

    i=0
    split_dicts = defaultdict(dict)
    for k,v in records.items():
        split_dicts[assignees[i%len(assignees)]][k] = v
        i+=1

    print('{n} brat tasks are assigned to {assignee}'.format(n=len(records), assignee=assignees))

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