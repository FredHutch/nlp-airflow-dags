from datetime import datetime, timedelta
import json
import subprocess
import base64

from airflow.hooks import HttpHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators import PythonOperator
from airflow.models import DAG
import utilities.common as common
from utilities.common import JOB_RUNNING, JOB_COMPLETE, JOB_FAILURE, REVIEW_BYPASSED_ANNOTATION_TYPE, BRAT_PENDING


args = {
    'owner': 'wchau',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='prod-cortex-deid-notes-and-put-to-brat',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def generate_job_id(**kwargs):
    # get last update date from last completed run
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_runs WHERE job_status = %s"
    update_date_from_last_run = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt, parameters=(JOB_COMPLETE,))[0]

    if update_date_from_last_run == None:
        # first run
        update_date_from_last_run = datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tgt_select_stmt = "SELECT max(af_runs_id) FROM af_runs"
    last_run_id = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0]

    if last_run_id == None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT HDCPUpdateDate, count(*) FROM orca_ce_blob WHERE HDCPUpdateDate >= %s GROUP BY HDCPUpdateDate"
    tgt_insert_stmt = "INSERT INTO af_runs (af_runs_id, source_last_update_date, record_counts, job_start, job_status)" \
                      " VALUES (%s, %s, %s, %s, %s)"

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in common.SOURCE_NOTE_DB.get_records(src_select_stmt, parameters=(update_date_from_last_run,)):
        hdcpupdatedates.append(row[0])
        common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(new_run_id, row[0], row[1], job_start_date, JOB_RUNNING))

    if len(hdcpupdatedates) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    return (new_run_id, hdcpupdatedates)


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid FROM orca_ce_blob WHERE hdcpupdatedate = %s"
    #get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, annotation_date from af_runs_details  " \
                           "WHERE annotation_status = %s"
    complete_job_rows = common.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(JOB_COMPLETE,))
    complete_jobs = {(row[0],row[1]):row[2] for row in complete_job_rows}


    tgt_insert_stmt = "INSERT INTO af_runs_details (af_runs_id, hdcpupdatedate, hdcorcablobid, annotation_status) " \
                      "VALUES (%s, %s, %s, %s)"

    for hdcpupdatedate in hdcpupdatedates:
        for row in common.SOURCE_NOTE_DB.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            print("checking ID: {}".format(row[0]))
            if (row[0], hdcpupdatedate) in complete_jobs.keys():
                print("Job for note {},{} has already been completed on {} . Skipping.".format(row[0], hdcpupdatedate, complete_jobs[(row[0], hdcpupdatedate)]))
                continue
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0], JOB_RUNNING))


def send_notes_to_brat(**kwargs):
    clinical_notes = kwargs['clinical_notes']
    datefolder = kwargs['datefolder']
    remoteNlpHomePath = "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"
    remoteNlpDataPath = "{}/{}".format(remoteNlpHomePath, datefolder)
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")

    record_processed = 0
    for hdcorcablobid, notes in clinical_notes.items():
        # create a subfolder for hdcpupdatedate
        if record_processed == 0:
            remote_command = "[ -d {} ] && echo 'found'".format(remoteNlpDataPath)
            is_datefolder_found = subprocess.getoutput(
                "ssh {}@{} {}".format(ssh_hook.username, ssh_hook.remote_host, remote_command))

            if is_datefolder_found != 'found':
                remote_command = "mkdir {}".format(remoteNlpDataPath)
                subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                                 "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])

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
            remotePath=remoteNlpDataPath,
            filename=hdcorcablobid
        )

        subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                         "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])

        # send annotated notes to brat
        phiAnnoData = []
        line = 0
        for j in notes['annotated_note']:
            if j['type'] is not 'O':
                line += 1
                phiAnnoData.append(
                    "T{}\t{} {} {}\t{}".format(line, j['type'], j['start'], j['end'], j['text']))

        full_file_name = "".join(map(str, [remoteNlpDataPath, "/", hdcorcablobid, ".ann"]))
        if len(phiAnnoData) > 0:
            remote_command = """
                     umask 002;

                     echo '{data}' | base64 -d -  > {remotePath}/{filename}.ann;
            """.format(
                data=str(base64.b64encode("\r\n".join(phiAnnoData).encode('utf-8'))).replace("b'", "").replace("'", ""),
                remotePath=remoteNlpDataPath,
                filename=hdcorcablobid
            )
        else:
            remote_command = "umask 002; touch {remotePath}/{filename}.ann;".format(remotePath=remoteNlpDataPath,
                                                                                    filename=hdcorcablobid)
        subprocess.call(["ssh", "-p {}".format(ssh_hook.port), "{}@{}".format(ssh_hook.username, ssh_hook.remote_host),
                         remote_command])

        update_brat_db_status(hdcorcablobid, notes['hdcpupdatedate'], full_file_name)

    print("{num} annotations sent to brat for review.".format(num=len(clinical_notes.keys())))


def update_brat_db_status(note_id, hdcpupdatedate, directory_location):
    tgt_insert_stmt = """
    INSERT INTO brat_review_status (hdcorcablobid, last_update_date, directory_location, job_start, job_status, hdcpupdatedate)
     VALUES (%s, %s, %s, %s, %s, %s)
     """

    job_start_date = datetime.now()
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                parameters=(note_id, job_start_date, directory_location, job_start_date, BRAT_PENDING, hdcpupdatedate))


def save_note_to_temp_storage(blobid, hdcpupdatedate, metadata_dict):
    insert_stmt = "INSERT INTO temp_notes " \
                  "(hdcorcablobid, hdcpupdatedate, clinical_event_id, person_id, " \
                  "blob_contents, service_dt_time, institution, event_class_cd_descr) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    print("saving metadata to temp storage for {}, {}: {}".format(blobid, hdcpupdatedate, metadata_dict))
    common.ANNOTATIONS_DB.run(insert_stmt, parameters=(blobid,
                                                       hdcpupdatedate,
                                                       metadata_dict["clinical_event_id"],
                                                       metadata_dict["patient_id"],
                                                       metadata_dict["blob_contents"],
                                                       metadata_dict["servicedt"],
                                                       metadata_dict["instit"],
                                                       metadata_dict["cd_descr"]), autocommit=True)


def save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data):
    insert_stmt = "INSERT INTO temp_person " \
                  "(hdcorcablobid, hdcpupdatedate, person_id, GivenName, MiddleName, FamilyName" \
                  "VALUES (%s, %s, %s, %s, %s, %s)"
    print("saving person info to temp storage for {}, {}: {}".format(blobid, hdcpupdatedate, patient_data[0]))
    common.ANNOTATIONS_DB.run(insert_stmt, parameters=(blobid,
                                                       hdcpupdatedate,
                                                       patient_data[0],
                                                       patient_data[1],
                                                       patient_data[2],
                                                       patient_data[3]), autocommit=True)

def annotate_clinical_notes(**kwargs):
    api_hook = HttpHook(http_conn_id='fh-nlp-api-deid', method='POST')
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    tgt_select_stmt = "SELECT hdcorcablobid FROM af_runs_details WHERE af_runs_id = %s and hdcpupdatedate = %s and annotation_status = %s"
    tgt_update_stmt = "UPDATE af_runs_details SET annotation_status = %s, annotation_date = %s WHERE af_runs_id = %s and hdcpupdatedate = %s and hdcorcablobid in (%s)"

    for hdcpupdatedate in hdcpupdatedates:
        batch_records = {}
        for id_row in common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, hdcpupdatedate, JOB_RUNNING)):
            blobid = id_row[0]
            note_metadata = common.get_note_and_metadata_dict_from_source(blobid, hdcpupdatedate)
            patient_data = common.get_patient_data_from_source(note_metadata["patient_id"])

            if note_metadata["patient_id"] is None or patient_data is None:
                message = "Exception occurred: No PatientID found for blobid, hdcpupdatedate: {id},{date}".format(
                    id=blobid, date=hdcpupdatedate)
                common.log_error_and_failure_for_deid_note_job(run_id, blobid, hdcpupdatedate, message, "Flask DeID API")
                continue

            # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
            record = {}
            batch_records[blobid] = {}
            record['original_note'] = {"extract_text": "{}".format(note_metadata["blob_contents"]),
                                               "annotation_by_source": True}
            record['hdcpupdatedate'] = hdcpupdatedate
            try:
                resp = api_hook.run("/deid/annotate", data=json.dumps(record['original_note']),
                                    headers={"Content-Type": "application/json"})
                print("API response: {}".format(resp.content))
                record['annotated_note'] = json.loads(resp.content)
                annotation_status = JOB_COMPLETE
                batch_records[blobid] = record
            except Exception as e:
                common.log_error_and_failure_for_deid_note_job(run_id, blobid, hdcpupdatedate, e, "Flask DeID API")
                continue

            common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                        parameters=(annotation_status,
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                                    run_id,
                                    hdcpupdatedate,
                                    blobid))

            save_note_to_temp_storage(blobid, hdcpupdatedate, note_metadata)
            save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data)

        datefolder = hdcpupdatedate.strftime('%Y-%m-%d')
        send_notes_to_brat(clinical_notes=batch_records, datefolder=datefolder)
        for blobid, record in batch_records.items():
            common.save_deid_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))

    tgt_update_stmt = "UPDATE af_runs SET job_end = %s, job_status = %s WHERE af_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), JOB_COMPLETE, run_id))


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
