import os
from airflow.models import Variable
from datetime import datetime, timedelta

# COMMON VARIABLES
DEFAULT_EMAIL_TGT = "nlp@fredhutch.org"
BRAT_ASSIGNEE = Variable.get("BRAT_CONFIG", default_var = {"BRAT_ASSIGNEE": ["ALL_USERS"]}, deserialize_json=True)["BRAT_ASSIGNEE"]
MAX_BATCH_SIZE = int(Variable.get("MAX_BATCH_SIZE", 3))
os.environ['OS_AUTH_URL'] =  Variable.get('OS_AUTH_URL')
os.environ['OS_PASSWORD'] = Variable.get('OS_PASSWORD')
os.environ['OS_TENANT_NAME'] = Variable.get('OS_TENANT_NAME')
os.environ['OS_USERNAME'] = Variable.get('OS_USERNAME')

# safety in hardcoding for now - TODO - should eventually be changed to an ENV VAR
# threshold for af4
COMPLETE_STALE_THRESHOLD = timedelta(days=int(Variable.get("COMPLETE_STALE_THRESHOLD", 7)))
REVIEW_STALE_THRESHOLD = timedelta(days=int(Variable.get("REVIEW_STALE_THRESHOLD", 20)))
RESYNTH_REDRIVE_THRESHOLD = timedelta(days=int(Variable.get("RESYNTH_REDRIVE_THRESHOLD", 1)))
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

# NLP bort related
NLP_BORT_PENDING = "BORT Pending"
NLP_BORT_COMPLETE = "BORT Complete"
NLP_BORT_FAILED = "BORT Failed"


################
#Trashman Vars #
################

REDRIVE_TASK_FN = {
    'BRAT_STALE': True,
    'RESYNTH': True,
    'DEID': True
}

################
#AF Table Names#
################
# af1 table: identify phi
AF1_RUNS = 'af1_runs'
AF1_RUNS_DETAILS = 'af1_runs_details'
AF1_RUNS_ID = "af1_runs_id"
# af2 table: brat nanny (brat review status)
AF2_RUNS_DETAILS = 'af2_runs_details'
AF2_RUNS_ID = "af2_runs_id" # brat job id
# af3 table: resynthesize PHI
AF3_RUNS = 'af3_runs'
AF3_RUNS_DETAILS = 'af3_runs_details'
AF3_RUNS_ID = "af3_runs_id"
# af4 table: trashman
AF4_RUNS_TABLE = "af4_runs"
AF4_RUNS_TABLE_DETAILS = "af4_runs_details"
AF4_STALE_BRAT_TABLE = "af4_stale_brat_jobs"
AF4_COMPLETE_BRAT_TABLE = "af4_complete_brat_jobs"
AF4_RUN_ID = "af4_runs_id"
AF4_SOURCE_BRAT_TABLE = "af2_runs_details"
AF4_SOURCE_BRAT_ID = "af2_runs_id"
# af5 table: clinical nlp basics
AF5_RUNS = 'af5_runs'
AF5_RUNS_DETAILS = 'af5_runs_details'
AF5_RUNS_ID = "af5_runs_id"
# af6 table: bort
AF6_RUNS = 'af6_runs'
AF6_RUNS_DETAILS = 'af6_runs_details'
AF6_RUNS_ID = 'af6_runs_id'
# source table
ANNOTATION_TABLE = 'annotations'
TEMP_PERSON = 'TEMP_PERSON'
PatientMap = 'deid.PatientMap'
# Temp fix for airflow 1
TEMP_DATE = '2020-01-01 00:00:00.000'
