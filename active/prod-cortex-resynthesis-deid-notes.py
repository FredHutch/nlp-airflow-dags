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


def _get_notes(status, ids_only=False):
    first_status = status
    additional_statuses = ""
    if type(status) is list:
        first_status = status[0]
        rest = status[1:]
        sql_statements = ["OR LIKE '{}'".format(stat) for stat in status[1:]]
        additional_statuses = " ".join(sql_statements)

    # get all job records that are ready to check for review completion
    src_select_stmt = """
                      SELECT hdcorcablobid, brat_id, directory_location, job_status 
                      FROM brat_review_status 
                      WHERE job_status like '{first_status}'
                      {additional_statuses}
                      """.format(first_status=first_status, additional_statuses=additional_statuses)

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in common.AIRFLOW_NLP_DB.get_records(src_select_stmt):
        if ids_only:
            hdcpupdatedates.append(row[0])
        else:
            hdcpupdatedates.append(row)
    if len(hdcpupdatedates) == 0:
        print("No reviews with status: {status} found as of {date}".format(status=status, date=job_start_date))
        exit()
    print("{} notes to be checked for completion".format(len(hdcpupdatedates)))

    return (hdcpupdatedates)


def generate_job_id(**kwargs):
    # get last update date from last completed run
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_resynthesis_runs WHERE job_status = 'completed' or job_status = 'running'"
    update_date_from_last_run = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0]

    if update_date_from_last_run == None:
        # first run
        update_date_from_last_run = datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    tgt_select_stmt = "SELECT max(af_resynth_runs_id) FROM af_resynthesis_runs"
    last_run_id = common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt)[0]

    if last_run_id == None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT date_created, count(*) " \
                      "FROM nlp_annotation.dbo.annotations " \
                      "WHERE date_created > %s " \
                      "AND (category = 'BRAT REVIEWED ANNOTATION' " \
                      "     OR category = 'BRAT REVIEWED ANNOTATION')" \
                      "GROUP BY date_created "



    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs (af_resynth_runs_id, source_last_update_date, record_counts, job_start, job_status) VALUES (%s, %s, %s, %s, 'running')"

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=update_date_from_last_run):
        hdcpupdatedates.append(row[0])
        common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(new_run_id, row[0], row[1], job_start_date))

    if len(hdcpupdatedates) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    return (new_run_id, hdcpupdatedates)


def populate_blobid_in_job_table(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    # get record id to be processed
    src_select_stmt = "SELECT TOP 1 hdcorcablobid FROM nlp_annotation.dbo.annotations WHERE date_created = %s"
    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs_details (af_resynth_runs_id, hdcpupdatedate, hdcorcablobid) VALUES (%s, %s, %s)"

    for hdcpupdatedate in hdcpupdatedates:
        for row in common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(hdcpupdatedate,)):
            common.AIRFLOW_NLP_DB.run(tgt_insert_stmt, parameters=(run_id, hdcpupdatedate, row[0]))


def _get_resynth_run_details_id_by_date(run_id, date):
    tgt_select_stmt = "SELECT hdcorcablobid FROM af_resynthesis_runs_details " \
                      "WHERE af_resynth_runs_id = %s and hdcpupdatedate = %s and resynth_status is null"

    return common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date))


def _get_annotations_by_id_and_created_date(id, date):
    src_select_stmt = "SELECT annotation FROM nlp_annotation.dbo.annotations " \
                      "WHERE date_created = %s and hdcorcablobid = %s " \
                      "AND (category = 'BRAT REVIEWED ANNOTATION' " \
                      "     OR category = 'BRAT REVIEWED ANNOTATION')"

    return common.ANNOTATIONS_DB.get_records(src_select_stmt, parameters=(date, id))


def _call_resynthesis_api(blobid, deid_note, deid_annotations):
    api_hook = HttpHook(http_conn_id='fh-nlp-api-resynth', method='POST')
    results = None
    try:
        resp = api_hook.run("/resynthesize",
                            data=json.dumps({"text": deid_note, "annotations": deid_annotations}),
                            headers={"Content-Type": "application/json"})
        results = json.loads(resp.content)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        common.log_error_message(blobid=blobid, state="Flask Resynth API", time=time_of_error, error_message=str(e))

    return results

def _get_original_note_by_blobid(blobid):
    src_select_stmt = "SELECT blob_contents FROM orca_ce_blob WHERE  hdcorcablobid = %s"
    results = common.SOURCE_NOTE_DB.get_first(src_select_stmt, parameters=(blobid))

    #return blob_contents [0] from returned row
    return results[0]

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


def resynthesize_notes_marked_as_deid(**kwargs):
    # get last update date
    (run_id, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    tgt_update_stmt = "UPDATE af_resynthesis_runs_details" \
                      " SET resynth_status = %s, resynth_date = %s" \
                      " WHERE af_resynth_runs_id = %s and hdcpupdatedate = %s and hdcorcablobid in (%s)"

    for hdcpupdatedate in hdcpupdatedates:
        record_processed = 0

        for blobid in _get_resynth_run_details_id_by_date(run_id, hdcpupdatedate):
            blobid
            batch_records = []
            for row in _get_annotations_by_id_and_created_date(blobid, hdcpupdatedate):
                # record = { 'hdcorcablobid' : { 'original_note' : json, 'annotated_note' : json } }
                record = {}
                deid_note = _get_original_note_by_blobid(blobid)
                corrected_dict = cast_start_end_as_int(json.loads(row[0]))
                results = _call_resynthesis_api(blobid, deid_note, corrected_dict)
                resynth_status = 'failed'
                if results is not None:
                    record[blobid] = results
                    batch_records.append(record)
                    resynth_status = 'successful'

                common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                            parameters=(resynth_status, datetime.now(), run_id, hdcpupdatedate, blobid[0]))
                for record in batch_records:
                    common.save_json_annotation(blobid, str(record[blobid]), 'RESYNTHESIZED ANNOTATIONS')

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