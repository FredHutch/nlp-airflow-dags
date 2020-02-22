from datetime import datetime, timedelta
from collections import defaultdict
import json
import subprocess
import base64

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import utilities.common_hooks as common_hooks
import utilities.common_variables as common_variables
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
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_runs WHERE job_status = %s"
    update_date_from_last_run = common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt, parameters=(common_variables.JOB_COMPLETE,))[0]
    print("last updatedate was {}".format(update_date_from_last_run))
    if update_date_from_last_run is None:
        # first run
        update_date_from_last_run = common_variables.EPOCH
        print("no updatedate found. falling back on epoch of {}".format(update_date_from_last_run))
    tgt_select_stmt = "SELECT max(af_runs_id) FROM af_runs"
    last_run_id = (common_hooks.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0] or 0)
    new_run_id = last_run_id + 1

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = ("SELECT HDCPUpdateDate, count(*) " 
                      "FROM vClinicalNoteDiscovery " 
                      "WHERE HDCPUpdateDate >= %s "
                      "GROUP BY HDCPUpdateDate")
    tgt_insert_stmt = ("INSERT INTO af_runs "
                      "(af_runs_id, source_last_update_date, record_counts, job_start, job_status) "
                      "VALUES (%s, %s, %s, %s, %s)")

    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    hdcpupdatedates = []
    total_job_count = common_variables.MAX_BATCH_SIZE
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
    src_select_stmt = "SELECT DISTINCT HDCOrcaBlobId FROM vClinicalNoteDiscovery WHERE HDCPUpdateDate = %s"
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = ("SELECT HDCOrcaBlobId, HDCPUpdateDate, annotation_date from af_runs_details  "
                           "WHERE annotation_status = %s")
    complete_job_rows = common_hooks.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(common_variables.JOB_COMPLETE,))
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = ("INSERT INTO af_runs_details (af_runs_id, HDCPUpdateDate, HDCOrcaBlobId, annotation_status) "
                      "VALUES (%s, %s, %s, %s)")

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
                print("Job Batch for hdcpupdatedate: {}  contains {} notes"
                      " and exceeds cumulative total per-run Job Size of {}."
                      " Breaking early.".format(row[0], row[1], common_variables.MAX_BATCH_SIZE))
                break



def send_notes_to_brat(**kwargs):
    clinical_notes = kwargs['clinical_notes']
    datafolder = kwargs['datafolder']
    remote_nlp_data_path = "{}/{}".format(common_hooks.BRAT_NLP_FILEPATH, datafolder)

    record_processed = 0
    for hdcorcablobid, notes in clinical_notes.items():
        # create a subfolder for hdcpupdatedate
        if record_processed == 0:
            remote_command = "[ -d {} ] && echo 'found'".format(remote_nlp_data_path)
            is_datefolder_found = subprocess.getoutput(
                "ssh {}@{} {}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host, remote_command))

            if is_datefolder_found != 'found':
                remote_command = "mkdir {}".format(remote_nlp_data_path)
                subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port),
                                 "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host), remote_command])

        # send original notes to brat
        remote_command = """
                         umask 002;

                         if [[ -f {remotePath}/{filename}.txt ]]; then
                           rm {remotePath}/{filename}.txt
                         fi

                         echo "{data}" | base64 -d - > {remotePath}/{filename}.txt;
        """.format(
            data=str(base64.b64encode(notes['original_note']['extract_text'].encode('utf-8'))).replace("b'",
                                                                                                       "").replace("'",
                                                                                                                   ""),
            remotePath=remote_nlp_data_path,
            filename=hdcorcablobid
        )

        subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port),
                         "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host), remote_command])

        # send annotated notes to brat
        phi_anno_data = []
        line = 0
        for j in notes['annotated_note']:
            if j['type'] is not 'O':
                line += 1
                phi_anno_data.append(
                    "T{}\t{} {} {}\t{}".format(line, j['type'], j['start'], j['end'], j['text']))

        full_file_name = "".join(map(str, [remote_nlp_data_path, "/", hdcorcablobid, ".ann"]))
        if len(phi_anno_data) > 0:
            remote_command = """
                     umask 002;

                     echo '{data}' | base64 -d -  > {remotePath}/{filename}.ann;
            """.format(
                data=str(base64.b64encode("\r\n".join(
                    phi_anno_data).encode('utf-8'))).replace("b'", "").replace("'", ""),
                remotePath=remote_nlp_data_path,
                filename=hdcorcablobid
            )
        else:
            remote_command = "umask 002; touch {remotePath}/{filename}.ann;".format(remotePath=remote_nlp_data_path,
                                                                                    filename=hdcorcablobid)
        subprocess.call(["ssh", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port), "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host),
                         remote_command])

        update_brat_db_status(hdcorcablobid, notes['hdcpupdatedate'], full_file_name)

    print("{num} annotations sent to brat for review.".format(num=len(clinical_notes.keys())))


def update_brat_db_status(note_id, hdcpupdatedate, directory_location):
    tgt_insert_stmt = """
         INSERT INTO brat_review_status 
         (HDCOrcaBlobId, last_update_date, directory_location, job_start, job_status, HDCPUpdateDate)
         VALUES (%s, %s, %s, %s, %s, %s)
         """

    job_start_date = datetime.now()
    common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(note_id, job_start_date, directory_location, job_start_date, common_variables.BRAT_PENDING,
                                          hdcpupdatedate))


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
                      "FROM af_runs_details "
                      "WHERE af_runs_id = %s and HDCPUpdateDate = %s and annotation_status = %s")
    tgt_update_stmt = ("UPDATE af_runs_details "
                      "SET annotation_status = %s, annotation_date = %s "
                      "WHERE af_runs_id = %s and HDCPUpdateDate = %s and HDCOrcaBlobId in (%s)")

    for hdcpupdatedate in hdcpupdatedates:
        batch_records = {}
        for id_row in common_hooks.AIRFLOW_NLP_DB.get_records(tgt_select_stmt,
                                                        parameters=(run_id, hdcpupdatedate, common_variables.JOB_RUNNING)):
            blobid = id_row[0]
            note_metadata = common_hooks.get_note_and_metadata_dict_from_source(blobid, hdcpupdatedate)
            patient_data = common_hooks.get_patient_data_from_source(note_metadata["patient_id"])

            if note_metadata["patient_id"] is None or patient_data is None:
                message = "Exception occurred: No PatientID found for BlobId, HDCPUpdateDate: {id},{date}".format(
                    id=blobid, date=hdcpupdatedate)
                common_hooks.log_error_and_failure_for_deid_note_job(run_id,
                                                               blobid,
                                                               hdcpupdatedate,
                                                               message,
                                                               "Flask DeID API")
                continue

            # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
            record = dict()
            batch_records[blobid] = dict()
            record['original_note'] = {"extract_text": "{}".format(note_metadata["blob_contents"]),
                                       "annotation_by_source": True}
            record['hdcpupdatedate'] = hdcpupdatedate
            try:
                resp = common_hooks.DEID_NLP_API_HOOK.run("/deid/annotate", data=json.dumps(record['original_note']),
                                    headers={"Content-Type": "application/json"})
                record['annotated_note'] = json.loads(resp.content)
                annotation_status = common_variables.JOB_COMPLETE
                batch_records[blobid] = record

            except Exception as e:
                common_functions.log_error_and_failure_for_deid_note_job(run_id, blobid, hdcpupdatedate, e, "Flask DeID API")
                continue

            common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                      parameters=(annotation_status,
                                                  datetime.now().strftime(common_variables.DT_FORMAT)[:-3],
                                                  run_id,
                                                  hdcpupdatedate,
                                                  blobid))

            save_note_to_temp_storage(blobid, hdcpupdatedate, note_metadata)
            save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data)

        to_review, skip_review = split_records_by_review_status(batch_records)

        assignment = _divide_tasks(to_review, common_variables.BRAT_DEFAULT_ASSIGNEE)

        for assignee, to_review_by_assignee in assignment.items():
            send_notes_to_brat(clinical_notes=to_review_by_assignee,
                               datafolder='{assignee}/{date}'.format(assignee=assignee, date=hdcpupdatedate.strftime('%Y-%m-%d')))
            save_deid_annotations(to_review_by_assignee)

        save_unreviewed_annotations(skip_review)

    tgt_update_stmt = "UPDATE af_runs SET job_end = %s, job_status = %s WHERE af_runs_id = %s"
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), common_variables.JOB_COMPLETE, run_id))


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

    if not assignees:
        print('No brat assignees were found, assigning to "ALL USERS" by default')
        return {common_variables.BRAT_DEFAULT_ASSIGNEE: records}
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
