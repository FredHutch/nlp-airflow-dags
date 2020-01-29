import json
from datetime import datetime

import utilities.common as common
import utilities.job_states as job_states

import operators.utilities.flask_blob_nlp as flask_blob_nlp


def run_ner_task(**kwargs):
    # get last update date
    (run_id, resynthdates, blobids, hdcpupdatedates) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    job_tuple = (resynthdates, blobids, hdcpupdatedates)
    for resynth_date, blobid, hdcpupdatedate in job_tuple:
        # record number of NER tasks
        record_processed = 0

        note = common.read_from_storage(blobid, connection=common.MYSTOR)
        preprocessing_results = flask_blob_nlp.call_flask_blob_nlp_preprocessing(blobid, note)
        sectionerx_results = flask_blob_nlp.call_flask_blob_nlp_sectionerx(blobid, note)

        if preprocessing_results is None:
            print("No NER preprocessing results returned for id: {id}. Failing note and Continuing".format(id=blobid))
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=job_states.NLP_NER_FAILED)
            continue

        if sectionerx_results is None:
            print(
                "No NER sectionerx results returned for id: {id}. Failing note and Continuing".format(id=blobid))
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=job_states.NLP_NER_FAILED)
            continue

        results = {}
        results['patient_pubid'] = note['patient_pubid']
        results['service_date'] = note['service_date']
        results['institution'] = note['institution']
        results['note_type'] = note['note_type']
        results['preprocessed_note'] = preprocessing_results
        results['sectionerex_note'] = sectionerx_results

        try:
            # save json to db
            json_obj_to_store = json.dumps(results, indent=4, sort_keys=True)
            # save annotated notes to object store
            common.write_to_storage(blobid,
                                    hdcpupdatedate,
                                    payload=json_obj_to_store,
                                    key='deid_test/NER_blobs/{id}.json'.format(id=blobid),
                                    connection=common.MYSTOR)
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=job_states.NLP_NER_COMPLETE)
            _update_ner_runs(run_id, state=job_states.NLP_NER_COMPLETE)

        except Exception as e:
            message = "Exception occurred: {}".format(e)
            common.log_error_and_failure_for_ner_job(run_id, blobid, hdcpupdatedate, message,
                                                     "Save JSON NER Blobs")
            continue

        record_processed += 1

    print("{} records processed".format(record_processed))


def _update_ner_run_details(run_id, blobid, hdcpupdatedate, state):
    tgt_update_stmt = "UPDATE af_ner_runs_details " \
                      "SET ner_status = %s, ner_date = %s " \
                      "WHERE af_ner_runs_id = %s " \
                      "AND hdcpupdatedate = %s and hdcorcablobid in (%s) "

    print("updating blob {} to {}".format(blobid, state))

    return common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                     parameters=(state, datetime.now(), run_id, hdcpupdatedate, blobid))

def _get_ner_run_details_id_by_resynth_date(run_id, date):
    tgt_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate " \
                      "FROM af_ner_runs_details " \
                      "WHERE af_ner_runs_id = %s " \
                      "AND resynth_date = %s " \
                      "AND ner_status = %s"

    return common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date, job_states.JOB_RUNNING))

def _update_ner_runs(run_id, state):
    tgt_update_stmt = "UPDATE af_ner_runs SET job_end = %s, job_status = %s WHERE af_ner_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), state, run_id), autocommit=True)

def _update_job_id_as_failed(run_id):
    tgt_update_stmt = "UPDATE af_ner_runs SET job_end = %s, job_status = %s WHERE af_ner_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), job_states.NLP_NER_FAILED, run_id), autocommit=True)