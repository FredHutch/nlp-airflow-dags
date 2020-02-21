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