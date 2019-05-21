from datetime import datetime, timedelta
import json
import subprocess
import base64
import paramiko

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import common

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='prod-cortex-resynthesis-deid-notes',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def _insert_resynth_run_job(run_id, update_date, record_count, job_start_date):
    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs (af_resynth_runs_id, source_last_update_date, record_counts, job_start, job_status) VALUES (%s, %s, %s, %s, 'running')"
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, update_date, record_count, job_start_date))

    return


def _get_annotations_since_date(update_date_from_last_run):
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT date_created, count(*) " \
                      "FROM annotations " \
                      "WHERE date_created > %s " \
                      "AND (category = 'BRAT REVIEWED ANNOTATION' " \
                      "     OR category = 'BRAT REVIEWED ANNOTATION')" \
                      "GROUP BY date_created "

    return common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=update_date_from_last_run)



def _get_last_resynth_run_id():
    tgt_select_stmt = "SELECT max(af_resynth_runs_id) FROM af_resynthesis_runs"
    last_run_id = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0]

    return last_run_id

def _get_last_resynth_update_date():
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_resynthesis_runs WHERE job_status = 'completed' or job_status = 'running'"
    update_date_from_last_run = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0]

    return update_date_from_last_run

def generate_job_id(**kwargs):
    # get last update date from last completed run
    update_date_from_last_run = _get_last_resynth_update_date()

    if update_date_from_last_run == None:
        # first run
        update_date_from_last_run = datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    last_run_id = _get_last_resynth_run_id()

    if last_run_id == None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT date_created, count(*) " \
                      "FROM annotations " \
                      "WHERE date_created > %s " \
                      "AND (category = 'BRAT REVIEWED ANNOTATION' " \
                      "     OR category = 'BRAT REVIEWED ANNOTATION')" \
                      "GROUP BY date_created "

    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs (af_resynth_runs_id, source_last_update_date, record_counts, job_start, job_status) VALUES (%s, %s, %s, %s, 'running')"

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in _get_annotations_since_date(update_date_from_last_run):
        hdcpupdatedates.append(row[0])
        _insert_resynth_run_job(new_run_id, row[0], row[1], job_start_date)

    if len(hdcpupdatedates) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    return (new_run_id, hdcpupdatedates)


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT TOP 1 hdcorcablobid FROM annotations WHERE date_created = %s"
    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs_details (af_resynth_runs_id, hdcpupdatedate, hdcorcablobid) VALUES (%s, %s, %s)"

    for hdcpupdatedate in hdcpupdatedates:
        for row in common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0]))


def _get_resynth_run_details_id_by_date(run_id, date):
    tgt_select_stmt = "SELECT hdcorcablobid FROM af_resynthesis_runs_details " \
                      "WHERE af_resynth_runs_id = %s and hdcpupdatedate = %s and resynth_status is null"

    return common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date))


def _get_annotations_by_id_and_created_date(id, date):
    src_select_stmt = "SELECT annotation FROM annotations " \
                      "WHERE date_created = %s and hdcorcablobid = %s " \
                      "AND (category = 'BRAT REVIEWED ANNOTATION' " \
                      "     OR category = 'BRAT REVIEWED ANNOTATION')"

    return common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(date, id))


def _call_resynthesis_api(blobid, deid_note, deid_annotations, deid_alias):
    api_hook = HttpHook(http_conn_id='fh-nlp-api-resynth', method='POST')
    results = None
    try:
        resp = api_hook.run("/resynthesize",
                            data=json.dumps({"text": deid_note, "annotations": deid_annotations,
                                             "alias": deid_alias}),
                            headers={"Content-Type": "application/json"})
        results = json.loads(resp.content)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        common.log_error_message(blobid=blobid, state="Flask Resynth API", time=time_of_error, error_message=str(e))

    return results


def _update_job_id_as_complete(run_id):
    tgt_update_stmt = "UPDATE af_runs SET job_end = %s, job_status = 'completed' WHERE af_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), run_id))

def cast_start_end_as_int(json_data):
    if isinstance(json_data, list):
        corrected_list = []
        for items in json_data:
            corrected_list.append(cast_start_end_as_int(items))
        return corrected_list

    corrected_dict = {}
    for key, value in json_data.items():
        if isinstance(value, list):
            value = [cast_start_end_as_int(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            value = cast_start_end_as_int(value)
        if key == 'start' or key == 'end':
            try:
                print("casting '{}' to {} for key '{}' json".format(value, int(value), key))
                value = int(value)
            except Exception as ex:
                pass
        corrected_dict[key] = value

    return corrected_dict


def _get_alias_data(patientId):
    al_select_stmt = ("SELECT FakeId, DateshiftDays, FirstName, MiddleName, LastName"
                      " FROM PatientMap"
                      " WHERE HdcPersonId = %s")
    return (common.SOURCE_NOTE_DB.get_first(al_select_stmt, parameters=(patientId,))
        or (None, None, None, None, None))


def _get_patient_data(patientId):
    pt_select_stmt = ("SELECT GivenName, MiddleName, FamilyName"
                      " FROM PersonCurrentIdentifiers JOIN Common_Person"
                      " ON PersonCurrentIdentifiers.HDCPersonID = Common_Person.HdcPersonID"
                      " WHERE PersonCurrentIdentifiers.OrcaPersonID = %s")
    return (common.SOURCE_NOTE_DB.get_first(pt_select_stmt, parameters=(patientId,))
        or (None, None, None))


def _get_note_metadata(blobId):
    note_select_stmt = ("SELECT SERVICE_DT_TM, INSTITUTION, EVENT_CLASS_CD_DESCR, PERSON_ID"
                        " FROM ORCA_CE_Blob JOIN ORCA_Clinical_Event"
                        " ON ORCA_CE_Blob.CLINICAL_EVENT_ID = ORCA_Clinical_Event.CLINICAL_EVENT_ID"
                        " WHERE ORCA_CE_Blob.HDCOrcaBlobID = %s")
    return (common.SOURCE_NOTE_DB.get_first(note_select_stmt, parameters=(blobId,))
        or (None, None, None, None))


def _build_patient_alias_map(patientId):
    alias_data = _get_alias_data(patientId)
    rl_names = _get_patient_data(patientId)
    alias_map = {'pt_names': {}}
    if alias_data[1]:
        alias_map['date_shift'] = alias_data[1]
    for idx, name in enumerate(rl_names):
        if name and alias_data[2:][idx]:
            alias_map['pt_names'][name] = alias_data[2:][idx]
    return alias_map, alias_data[0]


def resynthesize_notes_marked_as_deid(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    tgt_update_stmt = "UPDATE af_resynthesis_runs_details" \
                      " SET resynth_status = %s, resynth_date = %s" \
                      " WHERE af_resynth_runs_id = %s and hdcpupdatedate = %s and hdcorcablobid in (%s)"

    for hdcpupdatedate in hdcpupdatedates:
        record_processed = 0

        for id_record in _get_resynth_run_details_id_by_date(run_id, hdcpupdatedate):
            blobid = id_record[0]
            servicedt, instit, cd_descr, patient_id = _get_note_metadata(blobid)
            if not patient_id:
                err_msg = "No PatientID found for BlobID {}".format(blobid)
                print(err_msg)
                time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                common.log_error_message(blobid, state='Extract Blob/Patient Metadata',
                                         time=time_of_error, error_message=err_msg)
                continue
            alias_map, fake_id = _build_patient_alias_map(patient_id)
            batch_records = []
            for row in _get_annotations_by_id_and_created_date(blobid, hdcpupdatedate):
                # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
                record = {}
                deid_note = common.get_original_note_by_blobid(blobid)
                corrected_dict = cast_start_end_as_int(json.loads(row[0]))
                results = _call_resynthesis_api(blobid, deid_note, corrected_dict, alias_map)
                resynth_status = 'failed'
                if results is not None:
                    record[blobid] = results
                    batch_records.append(record)
                    resynth_status = 'successful'

                common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                            parameters=(resynth_status, datetime.now(), run_id, hdcpupdatedate, blobid[0]))
                for record in batch_records:
                    try:
                        # save json to db
                        common.save_json_annotation(blobid, str(record[blobid]), 'RESYNTHESIZED ANNOTATIONS')
                        file_to_s3 = json.dumps({'resynthesized_notes': record[blobid]['text'],
                                                 'patient_pubid': fake_id,
                                                 'service_date': servicedt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                                                 'institution': instit,
                                                 'note_type':cd_descr})
                        # save annotated notes to s3
                        common.write_to_s3(string_payload=file_to_s3,
                                           key='deid_test/annotated_note/{id}.json'.format(id=blobid))
                    except Exception as e:
                        print("Exception occurred: {}".format(e))
                        time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        common.log_error_message(blobid, state='Save JSON annotation', time=time_of_error, error_message=str(e))
                record_processed += 1

        _update_job_id_as_complete(run_id)
        print("{} records processed for update date: {}".format(record_processed, hdcpupdatedate))


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

resynthesize_notes_marked_as_deid = \
    PythonOperator(task_id='resynthesize_notes_marked_as_deid',
                   provide_context=True,
                   python_callable=resynthesize_notes_marked_as_deid,
                   dag=dag)

generate_job_id >> populate_blobid_in_job_table >> resynthesize_notes_marked_as_deid
