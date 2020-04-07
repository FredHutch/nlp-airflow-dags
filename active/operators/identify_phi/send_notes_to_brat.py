from datetime import datetime, timedelta
from collections import defaultdict
import json
import subprocess
import base64

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymssql import OperationalError

import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions


def send_notes_to_brat(**kwargs):
    clinical_notes = kwargs['clinical_notes']
    datafolder = kwargs['datafolder']

    remote_nlp_data_path = "{}/{}".format(common_hooks.BRAT_NLP_FILEPATH, datafolder)
    find_or_create_remote_dir(remote_nlp_data_path)

    for hdcorcablobid, notes in clinical_notes.items():
        send_singleton_note_to_brat(remote_nlp_data_path, hdcorcablobid, notes)

    print("{num} annotations sent to brat for review.".format(num=len(clinical_notes.keys())))


def find_or_create_remote_dir(remote_path):
    remote_command = "[ -d {} ] && echo 'found'".format(remote_path)
    is_folder_found = subprocess.getoutput(
        "ssh {}@{} {}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host,
                              remote_command))

    if is_folder_found != 'found':
        remote_command = "mkdir -p {}".format(remote_path)
        subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port),
                         "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username,
                                        common_hooks.BRAT_SSH_HOOK.remote_host), remote_command])


def send_singleton_note_to_brat(remote_path, hdcorcablobid, note):
    # create a subfolder for hdcpupdatedate
    hdcpupdatedate = note['hdcpupdatedate'].strftime('%Y-%m-%d')
    record_processed = 0


    annotation_name = "_".join(map(str, [hdcorcablobid, hdcpupdatedate]))
    # send original notes to brat
    remote_command = """
                             umask 002;
                             if [[ -f {remotePath}/{filename}.txt ]]; then
                               rm {remotePath}/{filename}.txt
                             fi
                             echo "{data}" | base64 -d - > {remotePath}/{filename}.txt;
                     """.format(
        data=str(base64.b64encode(note['original_note']['extract_text'].encode('utf-8'))).replace("b'",
                                                                                                   "").replace("'",
                                                                                                               ""),
        remotePath=remote_path,
        filename=annotation_name
    )

    subprocess.call(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port),
                     "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host),
                     remote_command])

    # send annotated notes to brat
    phi_anno_data = []
    line = 0
    for j in note['annotated_note']:
        if j['type'] is not 'O':
            line += 1
            phi_anno_data.append(
                "T{}\t{} {} {}\t{}".format(line, j['type'], j['start'], j['end'], j['text']))

    full_file_name = "".join(map(str, [remote_path, "/", annotation_name, ".ann"]))
    if len(phi_anno_data) > 0:
        remote_command = """
                         umask 002;
                         echo '{data}' | base64 -d -  > {remotePath}/{filename}.ann;
                """.format(
            data=str(base64.b64encode("\r\n".join(
                phi_anno_data).encode('utf-8'))).replace("b'", "").replace("'", ""),
            remotePath=remote_path,
            filename=annotation_name
        )
    else:
        remote_command = "umask 002; touch {remotePath}/{filename}.ann;".format(remotePath=remote_path,
                                                                                filename=annotation_name)
    subprocess.call(["ssh", "-p {}".format(common_hooks.BRAT_SSH_HOOK.port),
                     "{}@{}".format(common_hooks.BRAT_SSH_HOOK.username, common_hooks.BRAT_SSH_HOOK.remote_host),
                     remote_command])

    update_brat_db_status(hdcorcablobid, note['hdcpupdatedate'], full_file_name)


def update_brat_db_status(note_id, hdcpupdatedate, directory_location):
    tgt_insert_stmt = """
         INSERT INTO af2_runs_details
         (HDCOrcaBlobId, brat_last_modified_date, directory_location, job_start, job_status, HDCPUpdateDate)
         VALUES (%s, %s, %s, %s, %s, %s)
         """

    job_start_date = datetime.now()
    common_hooks.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(note_id, job_start_date, directory_location, job_start_date, common_variables.BRAT_PENDING,
                                          hdcpupdatedate))
