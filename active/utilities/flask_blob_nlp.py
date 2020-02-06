import json
from datetime import datetime
from operators.ner.generate_job_id import _get_last_ner_update_date

import utilities.common as common


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
        resp = common.FLASK_BLOB_NLP_API_HOOK.run("/{}".format(endpoint),
                            data=json.dumps({"extract_text": note}),
                            headers={"Content-Type": "application/json"})
        result = json.loads(resp.content)

        return result

    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime(common.DT_FORMAT)[:-3]
        common.log_error_message(hdcpupdatedate=hdcpupdatedate, blobid=blobid, state="FlaskBlobNLP API",
                                 time=time_of_error, error_message=str(e))
        return None


