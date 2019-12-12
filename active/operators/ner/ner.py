import json
from datetime import datetime
from active.operators.ner.generate_job_id import _get_last_ner_update_date
import active.utilities.job_states as job_states

import active.utilities.common as common


def _call_flask_blob_nlp(blobid):
    """
    call flaskblobnlp to process the de-id notes
    """

    last_ner_update_date = _get_last_ner_update_date()
    deid_note = _get_resynthesized_notes(blobid, last_ner_update_date)

    if common.flasknlobnlp_api_hook is not None:
       print("NER post data for blob {}: {}".format(blobid, deid_note))
    try:
        resp = common.flasknlobnlp_api_hook.run("/flaskml",
                            data=json.dumps({"text": deid_note}),
                            headers={"Content-Type": "application/json"})
        results = json.loads(resp.content)
        return results

    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        common.log_error_message(blobid=blobid, state="FlaskBlobNLP API",
                                 time=time_of_error, error_message=str(e))


def _get_resynthesized_notes(blobid, last_ner_update_date):

    job_start_date = datetime.now()
    print("Verifying storage status for blobId: {}, incoming update Date: {}, saved update date: {}".format(
        blobid, last_ner_update_date, job_start_date))
    if job_start_date is None or job_start_date <= last_ner_update_date:
        return common.read_from_storage('{}.json'.format(blobid), connection=common.MYSTOR)
