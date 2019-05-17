import datetime

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

ERROR_DB = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
SOURCE_NOTE_DB = MsSqlHook(mssql_conn_id="prod-hidra-dz-db01")
AIRFLOW_NLP_DB = PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline")
ANNOTATIONS_DB = MsSqlHook(mssql_conn_id="nile")
S3 = S3Hook('fh-nlp-deid-s3') # airflow conn ID
S3_BUCKET_NAME = "fh-nlp-deid" # s3 bucket name

def log_error_message(blobid, state, time, error_message):
    tgt_insert_stmt = "INSERT INTO af_errors (hdcorcablobid, state, datetime, message) VALUES (%s, %s, %s, %s)"

    ERROR_DB.run(tgt_insert_stmt, parameters=(blobid, state, time, error_message))



def save_json_annotation(note_uid, json_annotation, annotation_type):
    tgt_insert_stmt = "INSERT INTO nlp_annotation.dbo.annotations (hdcorcablobid, category, date_created, date_modified, annotation) VALUES (%s, %s, %s, %s, %s)"
    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("{}, {}, {}".format(note_uid, annotation_type, job_start_date))
    ANNOTATIONS_DB.run(tgt_insert_stmt, parameters=(note_uid, annotation_type, job_start_date, job_start_date, json_annotation), autocommit=True)
    return

def write_to_s3(filename, key, bucket_name):
    """
    create S3 hook, upload file to the bucket
    :param filename: file to be dropped off
    :param key: file name shown on S3
    :param bucket_name: S3 bucket name
    """
    S3.load_file(filename, key, bucket_name)