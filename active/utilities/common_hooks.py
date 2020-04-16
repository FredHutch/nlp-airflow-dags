import utilities.common_variables as common_variables
from sci.store import swift, s3

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook

from airflow.models import Variable


__error_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                         "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                         }
__source_note_db_stage_dict = {"PROD": "prod_src_notes",
                               "DEV": "dev_src_notes"
                               }
__airflow_nlp_db_stage_dict = {"PROD": PostgresHook(postgres_conn_id="prod-airflow-nlp-pipeline"),
                               "DEV": PostgresHook(postgres_conn_id="dev-airflow-nlp-pipeline")
                               }
__annotations_db_stage_dict = {"PROD": "prod_nlp_annos",
                               "DEV": "dev_nlp_annos"
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
#HOOK SPECIFIC VARIABLES
STAGE = Variable.get("NLP_ENVIRON")
STORAGE = 'SWIFT'

#DB VARIABLES
MYSTOR = __storage_writer[STORAGE](__bucket_dict[STORAGE][STAGE])
ERROR_DB = __error_db_stage_dict[STAGE]
#SOURCE_NOTE_DB = __source_note_db_stage_dict[STAGE]
AIRFLOW_NLP_DB = __airflow_nlp_db_stage_dict[STAGE]
#ANNOTATIONS_DB = __annotations_db_stage_dict[STAGE]
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


def get_annotations_db_hook():
    return MsSqlHook(mssql_conn_id=__annotations_db_stage_dict[STAGE])

def get_source_notes_db_hook():
    return MsSqlHook(mssql_conn_id=__source_note_db_stage_dict[STAGE])