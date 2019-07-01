from datetime import datetime, timedelta
import json

from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

import utilities.common as common
from utilities.common import JOB_RUNNING, JOB_COMPLETE, JOB_FAILURE, REVIEW_BYPASSED_ANNOTATION_TYPE, BRAT_REVIEWED_ANNOTATION_TYPE


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
    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs " \
                      "(af_resynth_runs_id, source_last_update_date, record_counts, job_start, job_status) " \
                      "VALUES (%s, %s, %s, %s, %s)"
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, update_date, record_count, job_start_date, JOB_RUNNING))

    return


def _get_annotations_since_date(update_date_from_last_run):
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT date_created, count(*) " \
                      "FROM annotations " \
                      "WHERE date_created >= %s " \
                      "AND (category = %s " \
                      "     OR category = %s) " \
                      "GROUP BY date_created "

    return common.ANNOTATIONS_DB.get_records(src_select_stmt,
                                             parameters=(update_date_from_last_run,
                                                         BRAT_REVIEWED_ANNOTATION_TYPE,
                                                         REVIEW_BYPASSED_ANNOTATION_TYPE))


def _get_last_resynth_run_id():
    tgt_select_stmt = "SELECT max(af_resynth_runs_id) FROM af_resynthesis_runs"
    last_run_id = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]


def _get_last_resynth_update_date():
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_resynthesis_runs WHERE job_status = %s"
    update_date_from_last_run = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                                 parameters=(JOB_COMPLETE,)) or (None,))

    return update_date_from_last_run[0]


def generate_job_id(**kwargs):
    # get last update date from last completed run
    update_date_from_last_run = _get_last_resynth_update_date()

    if update_date_from_last_run is None:
        # first run
        update_date_from_last_run = datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    last_run_id = _get_last_resynth_run_id()

    if last_run_id is None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    print("starting batch run ID: {id} of annotations since {date}".format(id=new_run_id,
                                                                           date=update_date_from_last_run))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    datecreated = []
    for row in _get_annotations_since_date(update_date_from_last_run):
        datecreated.append(row[0])
        _insert_resynth_run_job(new_run_id, row[0], row[1], job_start_date)

    if len(datecreated) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    print("{} new update batches found since last update date: {}".format(len(datecreated), update_date_from_last_run))

    return new_run_id, datecreated


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, datecreated) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate FROM annotations WHERE date_created = %s"
    # get completed jobs so that we do not repeat completed work
    screen_complete_stmt = "SELECT hdcorcablobid, hdcpupdatedate, resynth_date from af_resynthesis_runs_details  " \
                           "WHERE resynth_status = %s"
    complete_job_rows = common.AIRFLOW_NLP_DB.get_records(screen_complete_stmt, parameters=(JOB_COMPLETE,))
    complete_jobs = {(row[0], row[1]): row[2] for row in complete_job_rows}

    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs_details " \
                      "(af_resynth_runs_id, hdcpupdatedate, hdcorcablobid, annotation_creation_date, resynth_status) " \
                      "VALUES (%s, %s, %s, %s, %s) "

    for creation_date in datecreated:
        for row in common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(creation_date,)):
            if (row[0], row[1]) in complete_jobs:
                print("Job for note {},{}  originally created on {} has already been completed on {} ."
                      " Skipping.".format(row[0], row[1], creation_date, complete_jobs[(row[0], row[1])]))
                continue

            print("Inserting new note job for blobid {}:{}".format(row[0], row[1]))
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, row[1], row[0], creation_date, JOB_RUNNING))


def _get_resynth_run_details_id_by_creation_date(run_id, date):
    tgt_select_stmt = "SELECT hdcorcablobid, hdcpupdatedate FROM af_resynthesis_runs_details " \
                      "WHERE af_resynth_runs_id = %s and annotation_creation_date = %s and resynth_status = %s"

    return common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date, JOB_RUNNING))


def _update_resynth_run_details_to_complete(run_id, blobid, date):
    print("updating blob {} to complete".format(blobid))
    return _update_resynth_run_details_by_id_and_date(run_id, blobid, date, JOB_COMPLETE)


def _update_resynth_run_details_to_failed(run_id, blobid, date):
    print("updating blob {} to failed".format(blobid))
    return _update_resynth_run_details_by_id_and_date(run_id, blobid, date, JOB_FAILURE)


def _update_resynth_run_details_by_id_and_date(run_id, blobid, date, state):
    tgt_update_stmt = "UPDATE af_resynthesis_runs_details " \
                      "SET resynth_status = %s, resynth_date = %s " \
                      "WHERE af_resynth_runs_id = %s and hdcpupdatedate = %s and hdcorcablobid in (%s)"

    common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                              parameters=(
                                  state, datetime.now(), run_id, date, blobid), autocommit=True)


def _get_annotations_by_id_and_created_date(blobid, date):
    src_select_stmt = "SELECT annotation FROM annotations " \
                      "WHERE date_created = %s and hdcorcablobid = %s " \
                      "AND (category = %s " \
                      "     OR category = %s)"

    return common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(
                                             date, blobid, REVIEW_BYPASSED_ANNOTATION_TYPE,
                                             BRAT_REVIEWED_ANNOTATION_TYPE))


def _call_resynthesis_api(blobid, hdcpupdatedate, deid_note, deid_annotations, deid_alias):
    api_hook = HttpHook(http_conn_id='fh-nlp-api-resynth', method='POST')
    results = None
    print("resynth post data for blob {}: {}".format(
        blobid, {"text": deid_note, "annotations": deid_annotations, "alias": deid_alias}))
    try:
        resp = api_hook.run("/resynthesize",
                            data=json.dumps({"text": deid_note, "annotations": deid_annotations,
                                             "alias": deid_alias}),
                            headers={"Content-Type": "application/json"})
        results = json.loads(resp.content)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        common.log_error_message(blobid=blobid, hdcpupdatedate=hdcpupdatedate, state="Flask Resynth API",
                                 time=time_of_error, error_message=str(e))

    return results


def _update_job_id_as_complete(run_id):
    tgt_update_stmt = "UPDATE af_resynthesis_runs SET job_end = %s, job_status = %s WHERE af_resynth_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), JOB_COMPLETE, run_id), autocommit=True)


def cast_start_end_as_int(json_data, blobid, hdcpupdatedate):
    if isinstance(json_data, list):
        corrected_list = []
        for items in json_data:
            corrected_list.append(cast_start_end_as_int(items, blobid, hdcpupdatedate))
        return corrected_list

    corrected_dict = {}
    for key, value in json_data.items():
        if isinstance(value, list):
            value = [cast_start_end_as_int(item, blobid, hdcpupdatedate)
                     if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            value = cast_start_end_as_int(value, blobid, hdcpupdatedate)
        if key == 'start' or key == 'end':
            try:
                # print("casting '{}' to {} for key '{}' json".format(value, int(value), key))
                value = int(value)
            except Exception as ex:
                print("Exception occurred: {}".format(ex))
                time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                common.log_error_message(blobid, hdcpupdatedate=hdcpupdatedate, state='Load JSON annotations',
                                         time=time_of_error, error_message=str(ex))
        corrected_dict[key] = value

    return corrected_dict


def _get_patient_data_from_temp(blobid, hdcpupdatedate, patientid):
    print(
        "Fetching Real Patient Name Data from Temp DB for blobID: {blobid}, hdcpupdatedate: {date}, patientId: {patientid}".format(
            blobid=blobid, date=hdcpupdatedate, patientid=patientid))
    pt_select_stmt = ("SELECT hdcorcablobid, hdcpupdatedate, person_id, GivenName, MiddleName, FamilyName"
                      " FROM temp_person"
                      " WHERE hdcorcablobid = %s AND hdcpupdatedate = %s AND person_id = %s")
    ret = (common.ANNOTATIONS_DB.get_first(pt_select_stmt, parameters=(blobid, hdcpupdatedate, patientid))
            or (None, None, None, None, None, None))
    if ret[0] is None:
        print(
            "No Real Patient Name Data found in Temp DB for blobID: "
            "{blobid}, hdcpupdatedate: {date}, patientId: {patientid}".format(
                blobid=blobid, date=hdcpupdatedate, patientid=patientid))
    return ret


def _get_alias_data(patientid):
    print("Fetching Alias Name Data from Source DB for patientId: {}".format(patientid))
    al_select_stmt = ("SELECT FakeId, DateshiftDays, FirstName, MiddleName, LastName"
                      " FROM PatientMap JOIN PersonCurrentIdentifiers"
                      " ON PersonCurrentIdentifiers.HDCPersonID = PatientMap.HdcPersonID"
                      " WHERE PersonCurrentIdentifiers.OrcaPersonID = %s")
    return (common.SOURCE_NOTE_DB.get_first(al_select_stmt, parameters=(patientid,))
            or (None, None, None, None, None))


def _build_patient_alias_map(blobid, hdcpupdatedate, patientid):
    alias_data = _get_alias_data(patientid)
    print("alias data for patient {}: {}".format(patientid, alias_data))
    rl_names = _get_patient_data_from_temp(blobid, hdcpupdatedate, patientid)
    alias_map = {'pt_names': {}}
    if alias_data[1]:
        alias_map['date_shift'] = alias_data[1]
    for idx, name in enumerate(rl_names[3:]):
        if name and alias_data[2:][idx]:
            alias_map['pt_names'][name] = alias_data[2:][idx]
    return alias_map, alias_data[0]


def resynthesize_notes_marked_as_deid(**kwargs):
    # get last update date
    (run_id, createddate) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    for creation_date in createddate:
        record_processed = 0
        hdcpupdatedate = creation_date
        for id_record in _get_resynth_run_details_id_by_creation_date(run_id, creation_date):
            record_processed = 0
            service_dts = {}
            blobid = id_record[0]
            hdcpupdatedate = id_record[1]
            note_metadata = common.get_note_from_temp(blobid, hdcpupdatedate)
            if not note_metadata["patient_id"]:
                message = "No PatientID found for BlobID {id}. Failing note and Continuing.".format(id=blobid)
                common.log_error_and_failure_for_resynth_note_job(run_id, blobid, hdcpupdatedate, message,
                                                                  "Extract Blob/Patient Metadata")
                continue

            alias_map, fake_id = _build_patient_alias_map(blobid, hdcpupdatedate, note_metadata["patient_id"])
            batch_records = []

            for row in _get_annotations_by_id_and_created_date(blobid, creation_date):
                # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
                record = {}
                service_dts = {}
                corrected_dict = cast_start_end_as_int(json.loads(row[0]), blobid, hdcpupdatedate)
                results = _call_resynthesis_api(blobid, hdcpupdatedate, note_metadata["blob_contents"], corrected_dict,
                                                alias_map)

                if results is None:
                    print("No resynthesis results returned for id: {id}. Failing note and Continuing".format(id=blobid))
                    _update_resynth_run_details_to_failed(run_id, blobid, hdcpupdatedate)
                    continue

                record[blobid] = results
                batch_records.append(record)
                service_dts[blobid] = note_metadata["servicedt"] + timedelta(days=record[blobid]['alias']['date_shift'])

            for record in batch_records:
                try:
                    # save json to db
                    common.save_resynthesis_annotation(blobid, hdcpupdatedate, str(record[blobid]))
                    file_to_s3 = json.dumps({'resynthesized_notes': record[blobid]['text'],
                                             'patient_pubid': fake_id,
                                             'service_date': service_dts[blobid].strftime("%Y-%m-%d %H:%M:%S.%f")[: -3],
                                             'institution': note_metadata["instit"],
                                             'note_type': note_metadata["cd_descr"]})
                    # save annotated notes to s3
                    common.write_to_s3(blobid,
                                       hdcpupdatedate,
                                       string_payload=file_to_s3,
                                       key='deid_test/annotated_note/{id}.json'.format(id=blobid))

                except common.OutOfDateAnnotationException as e:
                    print("OutOfDateAnnotationException occurred: {}".format(e))
                    time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    common.log_error_message(blobid, hdcpupdatedate=hdcpupdatedate, state='Save Resynth S3',
                                             time=time_of_error,
                                             error_message=str(e))
                except Exception as e:
                    message = "Exception occurred: {}".format(e)
                    common.log_error_and_failure_for_resynth_note_job(run_id, blobid, hdcpupdatedate, message,
                                                                      "Save JSON Resynthesis Annotation")
                    continue

                _update_resynth_run_details_to_complete(run_id, blobid, hdcpupdatedate)
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
