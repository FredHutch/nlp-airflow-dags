import json
from datetime import datetime

import utilities.common as common
import utilities.job_states as job_states


def get_resynthesized_note(blobid):
    """
    get resynthesized notes from storage (swift, s3)
    """
    return common.read_from_storage(blobid, connection=common.MYSTOR)


def get_last_ner_update_date(blobid):
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_ner_runs WHERE job_status = %s"
    last_ner_update_date = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                            parameters=(job_states.JOB_COMPLETE,)) or (None,))