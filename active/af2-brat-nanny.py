from datetime import datetime, timedelta
import json
import subprocess

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import utilities.common_hooks as common_hooks
import utilities.common_variables as common_variables
import utilities.common_functions as common_functions

REVIEW_NOTES_COL = {'BRAT_ID': 0, 'DIR_LOCATION': 1, 'JOB_STATUS': 2, 'HDCPUPDATEDATE': 3, 'HDCORCABLOBID': 4}

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='af2-brat-nanny',
          default_args=args,
          dagrun_timeout=timedelta(seconds=30))


def scan_and_update_notes_for_completion(**kwargs):

    # specifying it as a literal regex gets airflows ssh cmd recognize the wildcards in the filepath.
    remote_command = r'egrep -l "^T[0-9]+[[:space:]]+.*REVIEW_COMPLETE" {location}/*/*.ann'.format(
        location=common_hooks.BRAT_NLP_FILEPATH)

    complete_list = subprocess.getoutput(
        "ssh {}@{} {}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host, remote_command))

    full_paths = []
    for completed_annotation in complete_list.splitlines():
        print("A review-complete annotation found at: {}".format(completed_annotation))
        full_paths.append(completed_annotation.strip())

    _update_job_status_by_directory_loc(full_paths)

    return


def _update_job_status_by_directory_loc(directory_locations):
    print("{} notes to be updated for Extraction".format(len(directory_locations)))
    update_time = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    sql_quote_escapes_locations = "'" + "','".join(directory_locations) + "'"
    tgt_update_stmt = """
                      UPDATE {table}
                      SET job_status = '{extract_status}', brat_last_review_date = '{date}'
                      WHERE job_status like '{review_status}'
                      AND directory_location in ({locations})
                      """.format(table = common_variables.AF2_RUNS_DETAILS,
                                 extract_status=common_variables.BRAT_READY_TO_EXTRACT,
                                 date=update_time,
                                 review_status=common_variables.BRAT_PENDING,
                                 locations=sql_quote_escapes_locations)

    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt)

    return


def _get_notes(status, ids_only=False):
    # get all job records that are ready to check for review completion
    src_select_stmt = """
                      SELECT brat_id, directory_location, job_status, hdcpupdatedate, hdcorcablobid
                      FROM {table}
                      WHERE job_status like '{status}'
                      """.format(table = common_variables.AF2_RUNS_DETAILS, status=status)

    job_start_date = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    hdcpupdatedates = []
    for row in common_hooks.AIRFLOW_NLP_DB.get_records(src_select_stmt):
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
    # get all job records that are ready to check for review completion
    src_select_stmt = """
                          SELECT brat_id, directory_location, job_status
                          FROM {table}
                          WHERE brat_id = {brat_id}
                          """.format(table = common_variables.AF2_RUNS_DETAILS, brat_id=brat_id)

    # make the assumption that this will always return a unique record
    return common_hooks.AIRFLOW_NLP_DB.get_records(src_select_stmt)[0]


def _scan_note_for_completion(review_note):

    dir_location = review_note[REVIEW_NOTES_COL['DIR_LOCATION']]
    cmd = """
          grep "^T[0-9]\+\s\+REVIEW_COMPLETE" {location}
          """.format(location=dir_location)
    remote_command = "{} && echo 'found'".format(cmd)
    is_complete = subprocess.getoutput(
        "ssh {}@{} {}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host, remote_command))

    return is_complete != 'found'


def _update_note_status(brat_id, hdcpupdatedate, job_status):
    update_time = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
    tgt_update_stmt = """
            UPDATE {table}
            SET job_status = %s, brat_last_review_date = %s
            WHERE brat_id in (%s) AND hdcpupdatedate = %s
            """.format(table = common_variables.AF2_RUNS_DETAILS)

    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(job_status, update_time, brat_id, hdcpupdatedate))

    return


def save_and_mark_completed_note(**kwargs):
    extraction_notes = _get_notes(common_variables.BRAT_READY_TO_EXTRACT)

    for extraction_note in extraction_notes:
        reviewed_notation = _get_note_from_brat(extraction_note[REVIEW_NOTES_COL['DIR_LOCATION']])
        try:
            _translate_and_save_note(extraction_note[REVIEW_NOTES_COL['HDCORCABLOBID']],
                                     extraction_note[REVIEW_NOTES_COL['HDCPUPDATEDATE']],
                                     reviewed_notation)
            _mark_review_completed(extraction_note[REVIEW_NOTES_COL['BRAT_ID']],
                                   extraction_note[REVIEW_NOTES_COL['HDCPUPDATEDATE']])
        except Exception as e:
            time_of_error = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
            common_functions.log_error_message(blobid=extraction_note[REVIEW_NOTES_COL['HDCORCABLOBID']],
                                     hdcpupdatedate=extraction_note[REVIEW_NOTES_COL['HDCPUPDATEDATE']],
                                     state="Extract Review Complete Note",
                                     time=time_of_error,
                                     error_message="Exception ocurred for brat_id: {}: {}".format(
                                         REVIEW_NOTES_COL['BRAT_ID'], e))
    return


def _translate_and_save_note(note_uid, hdcpupdatedate, ann_annotation):
    try:
        json_annotation = _translate_ann_to_json(ann_annotation)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        raise e

    common_functions.save_brat_reviewed_annotation(note_uid, hdcpupdatedate, str(json_annotation))

    return json_annotation


def _translate_ann_to_json(ann_annotation):
    dict_list = []
    for line in ann_annotation.splitlines():
        if line and line.startswith('T'):
            ann_parts = line.split('\t')
            if ann_parts[1].split()[0] != 'REVIEW_COMPLETE':
                dict_list.append({
                    'type': ann_parts[1].split()[0].lower(),
                    'start': ann_parts[1].split()[1],
                    'end': ann_parts[1].split()[-1],
                    'text': ann_parts[2]
                })
    return json.dumps(dict_list)


def _get_note_from_brat(note_location):
    remote_command = 'cat {annotation_location}'.format(annotation_location=note_location)
    reviewed_annotation_output = subprocess.getoutput(
        "ssh {}@{} {}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host, remote_command))

    return reviewed_annotation_output


def _mark_review_completed(brat_id, hdcpupdatedate):
    return _update_note_status(brat_id, hdcpupdatedate, common_variables.BRAT_COMPLETE)


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
