from datetime import datetime
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


__error_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                         "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                         }
__source_note_db_stage_dict = {"PROD":MsSqlHook(mssql_conn_id="prod-hidra-dz-db01"),
                               "DEV": MsSqlHook(mssql_conn_id="dev-hidra-dz-db01")
                               }
__airflow_nlp_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                               "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                               }
__annotations_db_stage_dict = {"PROD": MsSqlHook(mssql_conn_id="nile"),
                               "DEV": MsSqlHook(mssql_conn_id="test_nile")
                               }
__s3_hook_stage_dict = {"PROD": S3Hook('fh-nlp-deid-s3'),
                        "DEV": S3Hook('fh-nlp-deid-s3')
                        }
__s3_bucket_stage_dict = {"PROD": "fh-nlp-deid",
                          "DEV": "fh-nlp-deid"
                          }

STAGE = Variable.get("NLP_ENVIRON")
ERROR_DB = __error_db_stage_dict[STAGE]
SOURCE_NOTE_DB = __source_note_db_stage_dict[STAGE]
AIRFLOW_NLP_DB = __airflow_nlp_db_stage_dict[STAGE]
ANNOTATIONS_DB = __annotations_db_stage_dict[STAGE]
S3 = __s3_hook_stage_dict[STAGE]
S3_BUCKET_NAME = __s3_bucket_stage_dict[STAGE]

JOB_RUNNING = 'scheduled'
JOB_COMPLETE = 'completed'
JOB_FAILURE = 'failed'

BRAT_PENDING = 'PENDING REVIEW'
BRAT_READY_TO_EXTRACT = "EXTRACTION READY"
BRAT_COMPLETE = "REVIEW COMPLETE"

BRAT_REVIEWED_ANNOTATION_TYPE = 'BRAT REVIEWED ANNOTATION'
REVIEW_BYPASSED_ANNOTATION_TYPE = 'REVIEW BYPASSED'
DEID_ANNOTATION_TYPE = 'DEID ANNOTATIONS'
RESYNTH_ANNOTATION_TYPE = 'RESYNTHESIZED ANNOTATIONS'


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
    conn = hook.get_conn()
    hook.set_autocommit(conn, True)

#Set autocommit = TRUE for all of our writeable DB hooks,
# since we do not stage our commits
__set_autocommit_on_db_hook(ERROR_DB)
__set_autocommit_on_db_hook(AIRFLOW_NLP_DB)
__set_autocommit_on_db_hook(ANNOTATIONS_DB)

def log_error_message(blobid, hdcpupdatedate, state, time, error_message):
    tgt_insert_stmt = "INSERT INTO af_errors (hdcorcablobid, state, datetime, message, hdcpupdatedate) VALUES (%s, %s, %s, %s, %s)"

    ERROR_DB.run(tgt_insert_stmt, parameters=(blobid, state, time, error_message, hdcpupdatedate))


def save_deid_annotation(note_uid, json_annotation):
    return save_json_annotation(note_uid, json_annotation, DEID_ANNOTATION_TYPE)

def save_resynthesis_annotation(note_uid, json_annotation):
    return save_json_annotation(note_uid, json_annotation, RESYNTH_ANNOTATION_TYPE)

def save_json_annotation(note_uid, json_annotation, annotation_type):
    tgt_insert_stmt = "INSERT INTO annotations (hdcorcablobid, category, date_created, date_modified, annotation) VALUES (%s, %s, %s, %s, %s)"
    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("{}, {}, {}".format(note_uid, annotation_type, job_start_date))
    ANNOTATIONS_DB.run(tgt_insert_stmt, parameters=(note_uid, annotation_type, job_start_date, job_start_date, json_annotation), autocommit=True)
    return


def get_original_note_by_blobid(blobid):
    src_select_stmt = "SELECT blob_contents FROM orca_ce_blob WHERE  hdcorcablobid = %s"
    results = SOURCE_NOTE_DB.get_first(src_select_stmt, parameters=(blobid))

    #return blob_contents [0] from returned row
    return results[0]

def _get_most_recent_successful_note_job_update_date(blobid):
    select_stmt = "SELECT MAX(hdcpupdatedate) FROM af_resynthesis_runs_details where hdcorcablobid = %s and resynth_status = %s"

    return AIRFLOW_NLP_DB.get_first(select_stmt, parameters=(blobid, JOB_COMPLETE))[0]


def write_to_s3(blobid, update_date, string_payload, key):
    """
    create S3 hook, upload json object to the bucket
    :param string_payload: file to be dropped off
    :param key: file name shown on S3
    """
    s3_job_date = _get_most_recent_successful_note_job_update_date(blobid)
    print("blobId: {}, incoming update Date: {}, saved update date: {}".format(blobid, update_date, s3_job_date))
    if s3_job_date is None or s3_job_date <= update_date:
        S3.load_string(string_payload,
                       key,
                       bucket_name=S3_BUCKET_NAME,
                       replace=True)
        return

    raise OutOfDateAnnotationException("OutOfDateAnnotationException: A newer version of the annotation exists in S3",
                                       blobid,
                                       update_date,
                                       s3_job_date)

