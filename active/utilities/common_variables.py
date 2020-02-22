from airflow.models import Variable
from datetime import datetime, timedelta

# COMMON VARIABLES
DEFAULT_EMAIL_TGT = "nlp@fredhutch.org"
BRAT_ASSIGNEE = Variable.get("BRAT_CONFIG", deserialize_json=True)["BRAT_ASSIGNEE"]
BRAT_DEFAULT_ASSIGNEE = "ALL_USERS"
MAX_BATCH_SIZE = Variable.get("MAX_BATCH_SIZE", 3)
os.environ['OS_AUTH_URL'] =  Variable.get('OS_AUTH_URL')
os.environ['OS_PASSWORD'] = Variable.get('OS_PASSWORD')
os.environ['OS_TENANT_NAME'] = Variable.get('OS_TENANT_NAME')
os.environ['OS_USERNAME'] = Variable.get('OS_USERNAME')

# safety in hardcoding for now - TODO - should eventually be changed to an ENV VAR
# threshold for af4
STALE_THRESHOLD = timedelta(days=Variable.get("STALE_THRESHOLD", 1))

#the default 'beginning time' for any date-based choosing strategies
DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
EPOCH = Variable.get("EPOCH", datetime(1970, 1, 1).strftime(DT_FORMAT)[:-3])

############
#JOB STATES#
############
JOB_RUNNING = 'scheduled'
JOB_COMPLETE = 'completed'
JOB_FAILURE = 'failed'

## Brat Specific
BRAT_PENDING = 'PENDING REVIEW'
BRAT_READY_TO_EXTRACT = "EXTRACTION READY"
BRAT_COMPLETE = "REVIEW COMPLETE"

#Annotation Codes
BRAT_REVIEWED_ANNOTATION_TYPE = 'BRAT REVIEWED ANNOTATION'
REVIEW_BYPASSED_ANNOTATION_TYPE = 'REVIEW BYPASSED'
DEID_ANNOTATION_TYPE = 'DEID ANNOTATIONS'
RESYNTH_ANNOTATION_TYPE = 'RESYNTHESIZED ANNOTATIONS'

## NLP task related
NLP_NER_PENDING = "NER Pending"
NLP_NER_COMPLETE = "NER Complete"
NLP_NER_FAILED = "NER Failed"


################
#Trashman Vars #
################
REDRIVE_RUNS_TABLE = "af_trashman_runs"
REDRIVE_JOBS_TABLE = "af_trashman_runs_details"
REDRIVE_STALE_BRAT_TABLE = "af_trashman_stale_brat_jobs"
REDRIVE_COMPLETE_BRAT_TABLE = "af_trashman_complete_brat_jobs"
REDRIVE_RUN_ID = "af_trashman_runs_id"
REDRIVE_SOURCE_BRAT_TABLE = "brat_review_status"

REDRIVE_TASK_FN = {
    'BRAT_STALE': True,
    'RESYNTH': True,
    'DEID': True
}