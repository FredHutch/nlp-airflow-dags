import json
import os
from contextlib import closing
from datetime import datetime

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models import Variable
from sci.store import swift, s3
import utilities.job_states as job_states

__error_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                         "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                         }
__source_note_db_stage_dict = {"PROD": MsSqlHook(mssql_conn_id="prod_src_notes"),
                               "DEV": MsSqlHook(mssql_conn_id="dev_src_notes")
                               }
__airflow_nlp_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                               "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                               }
__annotations_db_stage_dict = {"PROD": MsSqlHook(mssql_conn_id="prod_nlp_annos"),
                               "DEV": MsSqlHook(mssql_conn_id="dev_nlp_annos")
                               }
__brat_ssh_stage_dict = {"PROD": SSHHook(ssh_conn_id="prod-brat"),
                         "DEV": SSHHook(ssh_conn_id="prod-brat")
                        }
__remote_nlp_home_path_dict = {"PROD": "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp",
                               "DEV": "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"
                        }

__storage_dict = {'SWIFT':
                    {"PROD":'Swift__HDC_project_uw-clinic-notes',
                     "DEV":'AUTH_Swift__HDC'},
                  'S3':
                    {"PROD": 'fh-nlp-deid-s3',
                     "DEV": 'fh-nlp-deid-s3'}
                 }
__bucket_dict = {'SWIFT':
                  {"PROD": "NLP",
                   "DEV": "NLP"},
                'S3':
                    {"PROD": 'fh-nlp-deid',
                     "DEV": 'fh-nlp-deid'}
                }

__annotations_prefix_dict = {
    'SWIFT':
        {"PROD": "fh-nlp-deid-swift/annotated_note",
         "DEV": "fh-nlp-deid-swift-dev/annotated_note"},
    'S3':
        {"PROD": 'fh-nlp-deid',
         "DEV": 'fh-nlp-deid'}
}

__blob_process_prefix_dict = {
    'SWIFT':
        {"PROD": "fh-nlp-deid-swift/NER_processed_blobs",
         "DEV": "fh-nlp-deid-swift-dev/NER_processed_blobs"},
    'S3':
        {"PROD": 'fh-nlp-deid',
         "DEV": 'fh-nlp-deid'}
}
__storage_writer = {'SWIFT':swift,
                    'S3':s3}

__deid_nlp_http_hook = {"PROD": HttpHook(http_conn_id='fh-nlp-api-deid', method='POST'),
                        "DEV": HttpHook(http_conn_id='fh-nlp-api-deid', method='POST')
                        }
__nlp_test_http_hook = {"PROD": HttpHook(http_conn_id='fh-nlp-api-test', method='POST'),
                        "DEV": HttpHook(http_conn_id='fh-nlp-api-test', method='POST')
                        }
__flask_resynth_http_hook = {"PROD": HttpHook(http_conn_id='fh-nlp-api-resynth', method='POST'),
                        "DEV": HttpHook(http_conn_id='fh-nlp-api-resynth', method='POST')
                        }
__flask_blob_nlp_http_hook = {"PROD": HttpHook(http_conn_id='fh-nlp-api-flask-blob-nlp', method='POST'),
                              "DEV": HttpHook(http_conn_id='fh-nlp-api-flask-blob-nlp', method='POST')
                              }

STAGE = Variable.get("NLP_ENVIRON")
DEFAULT_EMAIL_TGT = "nlp@fredhutch.org"
BRAT_ASSIGNEE = Variable.get("BRAT_CONFIG", deserialize_json=True)["BRAT_ASSIGNEE"]
BRAT_DEFAULT_ASSIGNEE = "ALL_USERS"
MAX_BATCH_SIZE = Variable.get("MAX_BATCH_SIZE", 3)
os.environ['OS_AUTH_URL'] =  Variable.get('OS_AUTH_URL')
os.environ['OS_PASSWORD'] = Variable.get('OS_PASSWORD')
os.environ['OS_TENANT_NAME'] = Variable.get('OS_TENANT_NAME')
os.environ['OS_USERNAME'] = Variable.get('OS_USERNAME')


# safety in hardcoding for now - TODO - should eventually be changed to an ENV VAR
STORAGE = 'SWIFT'
MYSTOR = __storage_writer[STORAGE](__bucket_dict[STORAGE][STAGE])

ERROR_DB = __error_db_stage_dict[STAGE]
SOURCE_NOTE_DB = __source_note_db_stage_dict[STAGE]
AIRFLOW_NLP_DB = __airflow_nlp_db_stage_dict[STAGE]
ANNOTATIONS_DB = __annotations_db_stage_dict[STAGE]
DEID_NLP_API_HOOK = __deid_nlp_http_hook[STAGE]
NLP_API_TEST_HOOK = __nlp_test_http_hook[STAGE]
FLASK_RESYNTH_NLP_API_HOOK = __flask_resynth_http_hook[STAGE]
FLASK_BLOB_NLP_API_HOOK = __flask_blob_nlp_http_hook[STAGE]
BRAT_SSH_HOOK = __brat_ssh_stage_dict[STAGE]
BRAT_NLP_FILEPATH = __remote_nlp_home_path_dict[STAGE]

OBJ_STORE = __storage_dict[STORAGE][STAGE]
BUCKET_NAME = __bucket_dict[STORAGE][STAGE]
ANNOTATION_PREFIX = __annotations_prefix_dict[STORAGE][STAGE]
BLOB_PROCESS_PREFIX = __blob_process_prefix_dict[STORAGE][STAGE]

#threshold for af4
STALE_THRESHOLD = Variable.get("STALE_THRESHOLD", 1)


#the default 'beginning time' for any date-based choosing strategies
DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
EPOCH = Variable.get("EPOCH", datetime(1970, 1, 1).strftime(DT_FORMAT)[:-3])

class OutOfDateAnnotationException(Exception):
    def __init__(self, message, blobid, blob_date, compared_date):
        additional_info = "Blob ID: {blobid}, " \
                          "Blob Date: {blob_date}, " \
                          "Compared Date: {compared_date}".format(blobid=blobid,
                                                                  blob_date=blob_date,
                                                                  compared_date=compared_date)
        full_message = ": ".join([message,additional_info])
        super(OutOfDateAnnotationException, self).__init__(full_message)
        self.blobid = blobid
        self.blob_date = blob_date
        self.compared_date = compared_date

def __set_autocommit_on_db_hook(hook):
    with closing(hook.get_conn()) as conn:
        hook.set_autocommit(conn, True)

#Set autocommit = TRUE for all of our writeable DB hooks,
# since we do not stage our commits
__set_autocommit_on_db_hook(ERROR_DB)
__set_autocommit_on_db_hook(AIRFLOW_NLP_DB)
__set_autocommit_on_db_hook(ANNOTATIONS_DB)


def log_error_message(blobid, hdcpupdatedate, state, time, error_message):
    tgt_insert_stmt = ("INSERT INTO af_errors (HDCOrcaBlobID, state, datetime, message, HDCPUpdateDate)"
                       "VALUES (%s, %s, %s, %s, %s)")
    #print error_message here since we're doing it on every call already
    if type(error_message) is not str:
        error_message = repr(error_message)
    print(error_message)
    ERROR_DB.run(tgt_insert_stmt,
                 parameters=(blobid, state, time, error_message, hdcpupdatedate),
                 autocommit=True)
    return

def save_brat_reviewed_annotation(note_uid, hdcpupdatedate, json_annotation):
    return save_json_annotation(note_uid, hdcpupdatedate, json_annotation, job_states.BRAT_REVIEWED_ANNOTATION_TYPE)

def save_deid_annotation(note_uid, hdcpupdatedate, json_annotation):
    return save_json_annotation(note_uid, hdcpupdatedate, json_annotation, job_states.DEID_ANNOTATION_TYPE)

def save_unreviewed_annotation(note_uid, hdcpupdatedate, json_annotation):
    return save_json_annotation(note_uid, hdcpupdatedate, json_annotation, job_states.REVIEW_BYPASSED_ANNOTATION_TYPE)

def save_resynthesis_annotation(note_uid, hdcpupdatedate, json_annotation):
    return save_json_annotation(note_uid, hdcpupdatedate, json_annotation, job_states.RESYNTH_ANNOTATION_TYPE)

def save_json_annotation(hdcorcablobid, hdcpupdatedate, json_annotation, annotation_type):
    tgt_insert_stmt = ("INSERT INTO annotations "
                      "(HDCOrcaBlobID, HDCPUpdateDate, category, date_created, date_modified, annotation) "
                      "VALUES (%s, %s, %s, %s, %s, %s)")
    job_start_date = datetime.now().strftime(DT_FORMAT)[:-3]
    print("new annotation added: BlobId {}, HDCPUpdateDate {}: annotation_type {}, job_start_date {}".format(
        hdcorcablobid, hdcpupdatedate, annotation_type, job_start_date))

    ANNOTATIONS_DB.run(tgt_insert_stmt,
                       parameters=(
                       hdcorcablobid, hdcpupdatedate, annotation_type, job_start_date, job_start_date, json_annotation),
                       autocommit=True)
    return


def log_error_and_failure_for_deid_note_job(run_id, blobid, hdcpupdatedate, message, state):
    tgt_update_stmt = ("UPDATE af_runs_details "
                      "SET annotation_status = %s, annotation_date = %s "
                      "WHERE af_runs_id = %s and HDCPUpdateDate = %s and HDCOrcaBlobID in (%s)")
    _log_failure(run_id, blobid, hdcpupdatedate, message, state, tgt_update_stmt)


def log_error_and_failure_for_resynth_note_job(run_id, blobid, hdcpupdatedate, message, state):
    tgt_update_stmt = ("UPDATE af_resynthesis_runs_details SET resynth_status = %s, resynth_date = %s "
                      "WHERE af_resynth_runs_id = %s and HDCPUpdateDate = %s and HDCOrcaBlobID in (%s)")
    _log_failure(run_id, blobid, hdcpupdatedate, message, state, tgt_update_stmt)

def log_error_and_failure_for_ner_job(run_id, blobid, hdcpupdatedate, message, state):
    tgt_update_stmt = ("UPDATE af_ner_runs_details SET ner_status = %s, ner_date = %s " 
                      "WHERE af_ner_runs_id = %s and HDCPUpdateDate = %s and HDCOrcaBlobID in (%s)")
    _log_failure(run_id, blobid, hdcpupdatedate, message, state, tgt_update_stmt)

def _log_failure(run_id, blobid, hdcpupdatedate, message, state, update_stmt):
    time_of_error = datetime.now().strftime(DT_FORMAT)[:-3]
    log_error_message(blobid=blobid, hdcpupdatedate=hdcpupdatedate, state=state,
                      time=time_of_error,
                      error_message=message)
    AIRFLOW_NLP_DB.run(update_stmt,
                       parameters=(job_states.JOB_FAILURE, time_of_error, run_id, hdcpupdatedate, blobid), autocommit=True)


def get_original_note_by_blobid(blobid):
    src_select_stmt = "SELECT BLOB_CONTENTS " \
                      "FROM vClinicalNoteDiscovery " \
                      "WHERE HDCOrcaBlobID = %s"
    results = SOURCE_NOTE_DB.get_first(src_select_stmt, parameters=(blobid,))

    #return blob_contents [0] from returned row
    return results[0]


def get_note_from_temp(blobid, hdcpupdatedate):
    print("Getting Note Metadata from Temp Note DB for BlobId: {id} and HDCPUpdateDate: {date}".format(id=blobid,
                                                                                                    date=hdcpupdatedate))
    src_select_stmt = ("SELECT CLINICAL_EVENT_ID, HDCOrcaBlobID, HDCPUpdateDate, HDCPersonID," 
                       "BLOB_CONTENTS, SERVICE_DT_TM, INSTITUTION, EVENT_CD_DESCR " 
                       "FROM TEMP_NOTES " 
                       "WHERE  HDCOrcaBlobID = %s AND HDCPUpdateDate = %s")
    result = (ANNOTATIONS_DB.get_first(src_select_stmt, parameters=(blobid, hdcpupdatedate))
               or (None, None, None, None, None, None, None, None))

    result_dict = {"clinical_event_id": result[0],
                   "blobid":result[1],
                   "hdcpupdatedate":result[2],
                   "patient_id": result[3],
                   "blob_contents": result[4],
                   "servicedt": result[5],
                   "instit": result[6],
                   "cd_descr": result[7]}

    return result_dict


def get_note_metadata_from_source(blobId):
    print("Getting Note Metadata  from Source DB for BlobId: {id}".format(id=blobId))
    note_select_stmt = ("SELECT DISTINCT CLINICAL_EVENT_ID, SERVICE_DT_TM,"
                        "INSTITUTION, EVENT_CD_DESCR, HDCPersonID"
                        " FROM vClinicalNoteDiscovery"
                        " WHERE HDCOrcaBlobID = %s")
    return (SOURCE_NOTE_DB.get_first(note_select_stmt, parameters=(blobId,))
        or (None, None, None, None, None))


def get_note_and_metadata_dict_from_source(blobId, hdcpupdatedate):
    print("Getting Note Metadata from Source DB for BlobId: {id} and HDCPUpdateDate: {date}".format(id=blobId,
                                                                                                    date=hdcpupdatedate))
    note_select_stmt = ("SELECT DISTINCT CLINICAL_EVENT_ID, BLOB_CONTENTS, SERVICE_DT_TM,"
                        "INSTITUTION, EVENT_CD_DESCR, HDCPersonID"
                        " FROM vClinicalNoteDiscovery"
                        " WHERE HDCOrcaBlobID = %s AND HDCPUpdateDate = %s")

    result = (SOURCE_NOTE_DB.get_first(note_select_stmt, parameters=(blobId, hdcpupdatedate))
        or (None, None, None, None, None, None))

    result_dict = {"clinical_event_id": result[0],
                   "blob_contents": result[1],
                   "servicedt": result[2],
                   "instit": result[3],
                   "cd_descr": result[4],
                   "patient_id": result[5]}

    return result_dict

def get_patient_data_from_source(patientId):
    print("Querying Source DB for patient date for PatientId: {id}".format(id=patientId))
    pt_select_stmt = ("SELECT HDCPersonID, GivenName, MiddleName, FamilyName"
                      " FROM Common_Person"
                      " WHERE HDCPersonID = %s")
    return (SOURCE_NOTE_DB.get_first(pt_select_stmt, parameters=(patientId,))
            or (None, None, None, None))


def _get_most_recent_successful_note_job_update_date(blobid, sourcetable, updatedate_type, job_state_type):
    select_stmt = ("SELECT MAX({updatedate_type}) " \
                   "FROM {sourcetable} " \
                   "WHERE HDCOrcaBlobID = %s " \
                   "AND {job_state_type} = %s".format(updatedate_type=updatedate_type,
                                                      sourcetable=sourcetable,
                                                      job_state_type=job_state_type))

    return AIRFLOW_NLP_DB.get_first(select_stmt, parameters=(blobid, job_states.JOB_COMPLETE))[0]

def _get_most_recent_ner_start_date(**kwargs):
    select_stmt = ("SELECT MAX(job_start) " \
                   "FROM af_ner_runs " \
                   "WHERE job_status = %s ")
    most_recent_ner_start_date = (AIRFLOW_NLP_DB.run(select_stmt, parameters=(job_states.JOB_RUNNING,)) or (None,))
    return most_recent_ner_start_date[0]

def _get_last_resynth_date_on_storage(**kwargs):
    """
    get all the blobs and update date from swift
    return {'blob filename': 'last modified date', ...}
    """
    select_stmt = ("SELECT MAX(resynth_date) " \
                   "FROM af_resynthesis_runs_details " \
                   "WHERE resynth_status = %s ")
    last_resynth_date_on_storage = (AIRFLOW_NLP_DB.get_first(select_stmt, parameters=(job_states.JOB_COMPLETE,)) or (None,))
    return last_resynth_date_on_storage[0]

def write_to_storage(blobid, sourcetable, job_state_type, updatedate_type, update_date, payload, key, connection):
    """
    create appropriate storage hook, upload json object to the object store (swift or s3)
    :param blobid: the hdcorcablobid for the note object
    :param update_date: the UpdateDate associated with the note object (normally hdcpupdatedate)
    :param string_payload: file to be dropped off
    :param key: file name shown on S3
    """
    job_date = _get_most_recent_successful_note_job_update_date(blobid, sourcetable, updatedate_type, job_state_type)
    print("Verifying storage status for blobId: {}, incoming update Date: {}, saved update date: {}".format(
          blobid, update_date, job_date))
    if job_date is None or job_date <= update_date:
        connection.object_put_json(key, json.dumps(payload))
        return
    if sourcetable == "af_resynthesis_runs_details":
        raise OutOfDateAnnotationException("OutOfDateAnnotationException: \
                                           A newer version of the annotation exists in Object Storage ",
                                           blobid,
                                           update_date,
                                           job_date)


def read_from_storage(blobid, connection, blob_prefix):
    """
    create appropriate storage hook, upload json object to the object store (swift or s3)
    :param key: file name shown on swift or s3, named by blobid
    """
    job_date = _get_most_recent_ner_start_date()
    update_date = _get_last_resynth_date_on_storage()

    print("blobId: {} was updated on {}, after last NER task ran on {}".format(
        blobid, update_date, job_date))

    if job_date is None or job_date <= update_date:
        return connection.object_get_json(get_default_keyname(blobid, prefix=blob_prefix))


def get_last_run_id(run_table_name, run_id_name):
    tgt_select_stmt = "SELECT max({run_id}) FROM {run_table}".format(run_table=run_table_name, run_id=run_id_name)
    last_run_id = (AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]

 
def get_default_keyname(blobid, prefix):
    return '{prefix}/{id}.json'.format(prefix=prefix, id=blobid)

def generate_timestamp():
    return datetime.now().strftime(DT_FORMAT)[:-3]

