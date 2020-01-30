import json
from datetime import datetime

import utilities.common as common


def _call_flask_blob_nlp(blobid):
    """
    call flaskblobnlp to process the de-id notes
    """
    results = {}

    deid_note = common.read_from_storage(blobid, connection=common.MYSTOR)

    if common.flasknlobnlp_api_hook is not None:
       print("NER post data for blob {}: {}".format(blobid, deid_note['resynthesized_notes']))
    try:
        resp = common.flasknlobnlp_api_hook.run("/flaskml",
                            data=json.dumps({"text": deid_note['resynthesized_notes']}),
                            headers={"Content-Type": "application/json"})
        results['ner_processed_notes'] = json.loads(resp.content)
        results['patient_pubid'] = deid_note['patient_pubid']
        results['service_date'] = deid_note['service_date']
        results['institution'] = deid_note['institution']
        results['note_type'] = deid_note['note_type']

        return results

    except Exception as e:
        print("Exception occurred: {}".format(e))
        time_of_error = datetime.now().strftime(common.DT_FORMAT)[:-3]
        common.log_error_message(blobid=blobid, state="FlaskBlobNLP API",
                                 time=time_of_error, error_message=str(e))
        return None
