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

def __set_autocommit_on_db_hook(hook):
    conn = hook.get_conn()
    hook.set_autocommit(conn, True)

#Set autocommit = TRUE for all of our writeable DB hooks,
# since we do not stage our commits
__set_autocommit_on_db_hook(ERROR_DB)
__set_autocommit_on_db_hook(AIRFLOW_NLP_DB)
__set_autocommit_on_db_hook(ANNOTATIONS_DB)

def log_error_message(blobid, state, time, error_message):
    tgt_insert_stmt = "INSERT INTO af_errors (hdcorcablobid, state, datetime, message) VALUES (%s, %s, %s, %s)"

    ERROR_DB.run(tgt_insert_stmt, parameters=(blobid, state, time, error_message))


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


def write_to_s3(string_payload, key):
    """
    create S3 hook, upload json object to the bucket
    :param string_payload: file to be dropped off
    :param key: file name shown on S3
    """
    S3.load_string(string_payload,
                   key,
                   bucket_name=S3_BUCKET_NAME,
                   replace=False)

