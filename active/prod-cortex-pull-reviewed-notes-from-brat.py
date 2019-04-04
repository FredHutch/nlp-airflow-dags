from datetime import datetime, timedelta
import json
import subprocess
import base64
import paramiko
import re

from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators import PythonOperator, BashOperator
from airflow.models import DAG

REVIEW_NOTES_COL = {'BRAT_ID':0, 'DIR_LOCATION':1, 'JOB_STATUS':2}

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='prod-cortex-pull-reviewed-notes-from-brat',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def scan_and_update_notes_for_completion(**kwargs):
    remoteNlpHomePath = "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"

    ssh_hook = SSHHook(ssh_conn_id="prod-brat")

    #specifying it as a literal regex gets airflows ssh cmd recognize the wildcards in the filepath.
    remote_command = r'egrep -l "^T[0-9]+[[:space:]]+.*REVIEW_COMPLETE" {location}/*/*.ann'.format(location=remoteNlpHomePath)

    username = 'brat'
    remote_host = 'nlp-cortex-brat'
    complete_list = subprocess.getoutput(
        "ssh {}@{} {}".format(username, remote_host, remote_command))

    full_paths = []
    for completed_annotation in complete_list.splitlines():
        print("found path was: {}".format(completed_annotation))
        full_paths.append(completed_annotation.strip())

    _update_job_status_by_directory_loc(full_paths)

    return


def _update_job_status_by_directory_loc(directory_locations):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")

    print("{} notes to be updated for Extraction".format(len(directory_locations)))
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    sql_quote_escapes_locations = "'" + "','".join(directory_locations) + "'"
    tgt_update_stmt = """
                UPDATE brat_review_status 
                SET job_status = 'EXTRACTION READY', last_update_date = '{date}' 
                WHERE job_status like 'PENDING REVIEW'
                  AND directory_location in ({locations})
                """.format(date=update_time, locations=sql_quote_escapes_locations)

    pg_hook.run(tgt_update_stmt)

    return


def _get_notes(status, ids_only=False):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")

    # get all job records that are ready to check for review completion
    src_select_stmt = """
                      SELECT brat_id, directory_location, job_status 
                      FROM brat_review_status 
                      WHERE job_status like '{status}'
                      """.format(status=status)

    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    hdcpupdatedates = []
    for row in pg_hook.get_records(src_select_stmt):
        if ids_only:
            hdcpupdatedates.append(row[0])
        else:
            hdcpupdatedates.append(row)
    if len(hdcpupdatedates) == 0:
        print("No reviews with status: {status} found as of {date}".format(status=status, date=job_start_date))
        exit()
    print("{} notes to be checked for completion".format(len(hdcpupdatedates)))
    return (hdcpupdatedates)

def _get_note_by_brat_id(brat_id):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")

    # get all job records that are ready to check for review completion
    src_select_stmt = """
                          SELECT brat_id, directory_location, job_status 
                          FROM brat_review_status 
                          WHERE brat_id = {brat_id}
                          """.format(brat_id=brat_id)

    #make the assumption that this will always return a unique record
    return pg_hook.get_records(src_select_stmt)[0]


def _scan_note_for_completion(review_note):
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")

    dir_location = review_note[REVIEW_NOTES_COL['DIR_LOCATION']]
    cmd = """
          grep "^T[0-9]\+\s\+REVIEW_COMPLETE" {location}
          """.format(location=dir_location)
    remote_command = "{} && echo 'found'".format(cmd)
    is_complete = subprocess.getoutput(
        "ssh {}@{} {}".format(ssh_hook.username, ssh_hook.remote_host, remote_command))

    return is_complete != 'found'


def _update_note_status(brat_id, job_status):
    pg_hook = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tgt_update_stmt = """
            UPDATE brat_review_status 
            SET job_status = %s, last_update_date = %s 
            WHERE brat_id in (%s)
            """

    pg_hook.run(tgt_update_stmt, parameters=(job_status, update_time, brat_id))

    return


def save_and_mark_completed_note(**kwargs):
    extraction_notes = _get_notes("EXTRACTION READY")

    for extraction_note in extraction_notes:
        reviewed_notation = _get_note_from_brat(extraction_note[REVIEW_NOTES_COL['DIR_LOCATION']])
        _translate_and_save_note(reviewed_notation)
        _mark_review_completed(extraction_note[REVIEW_NOTES_COL['BRAT_ID']])

    return


def _translate_and_save_note(ann_annotation):
    json_annotation = _translate_ann_to_json(ann_annotation)
    _save_json_annotation(json_annotation)

    return json_annotation


def _translate_ann_to_json(ann_annotation):
    pattern = re.compile(r"T(\d+)\s+([A-Z]+)\s+(\d+)\s+(\d+)\s+([\S]+[\s\S]*)")
    dict_list = []
    for line in ann_annotation.splitlines():
        results = pattern.search(line)
        if results is not None:
            json_dict = {}
            json_dict['type'] = results.group(2)
            json_dict['start'] = results.group(3)
            json_dict['end'] = results.group(4)
            json_dict['text'] = results.group(5)
            dict_list.append(json_dict)

    json_annotation = json.dumps(dict_list)

    return json_annotation


def _save_json_annotation(json_annotation):

    return


def _get_note_from_brat(note_location):
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")
    remote_command = 'cat {annotation_location}'.format(annotation_location=note_location)
    username = 'brat'
    remote_host = 'nlp-cortex-brat'
    reviewed_annotation_output = subprocess.getoutput(
        "ssh {}@{} {}".format(username, remote_host, remote_command))

    return reviewed_annotation_output


def _mark_review_completed(brat_id):
    return _update_note_status(brat_id, "REVIEW COMPLETE")


scan_and_update_notes_for_completion = \
    PythonOperator(task_id='scan_and_update_notes_for_completion',
                   provide_context=True,
                   python_callable=scan_and_update_notes_for_completion,
                   dag=dag)



save_and_mark_completed_note = \
    PythonOperator(task_id='save_and_mark_completed_note',
                   provide_context=True,
                   python_callable=save_and_mark_completed_note,
                   dag=dag)

scan_and_update_notes_for_completion >> save_and_mark_completed_note
