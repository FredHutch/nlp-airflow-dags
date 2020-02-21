import json
from datetime import datetime
from operators.ner.generate_job_id import _get_last_ner_update_date

import utilities.common_hooks as common_hooks
import utilities.common_variables as common_variables
import utilities.common_functions as common_functions


def call_flask_blob_nlp_preprocessing(blobid, hdcpupdatedate, note):
    return call_flask_blob_nlp_endpoint(blobid, hdcpupdatedate, note, "preprocess")


def call_flask_blob_nlp_sectionerex(blobid, hdcpupdatedate, note):
    return call_flask_blob_nlp_endpoint(blobid, hdcpupdatedate, note, "sectionerex")

def call_flask_blob_nlp_medlp(blobid, hdcpupdatedate, note):
    return call_flask_blob_nlp_endpoint(blobid, hdcpupdatedate, note, "medlp")


def call_flask_blob_nlp_endpoint(blobid, hdcpupdatedate, note, endpoint):
    """
    call flaskblobnlp to process the de-id notes
    """
    try:
        resp = common_hooks.FLASK_BLOB_NLP_API_HOOK.run("/{}/".format(endpoint),
                            data=json.dumps({"extract_text": note}),
                            headers={"Content-Type": "application/json"})
        result = json.loads(resp.content)

        return result

    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime(common_variables.DT_FORMAT)[:-3]
        common_functions.log_error_message(hdcpupdatedate=hdcpupdatedate, blobid=blobid, state="FlaskBlobNLP API",
                                 time=time_of_error, error_message=str(e))
        return None


