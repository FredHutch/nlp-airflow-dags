import json
from datetime import datetime

import utilities.common_hooks as common_hooks
import utilities.common_functions as common_functions
import utilities.common_variables as common_variables

import utilities.flask_blob_nlp as flask_blob_nlp


def run_ner_task(**kwargs):
    # get last update date
    (jobs_list) = kwargs['ti'].xcom_pull(task_ids='populate_blobid_in_job_table')
    print("job tuple: {}".format(jobs_list))
    print("job_tuple indices: {}".format(len(jobs_list)))
    for run_id, resynth_date, blobid, hdcpupdatedate in jobs_list:
        # record number of NER tasks
        record_processed = 0

        note = common_functions.read_from_storage(blobid, connection=common_hooks.MYSTOR, blob_prefix=common_hooks.ANNOTATION_PREFIX)
        preprocessing_results = flask_blob_nlp.call_flask_blob_nlp_preprocessing(blobid, hdcpupdatedate, note['resynthesized_notes'])
        sectionerx_results = flask_blob_nlp.call_flask_blob_nlp_sectionerex(blobid, hdcpupdatedate, note['resynthesized_notes'])

        if preprocessing_results is None:
            print("No NER preprocessing results returned for id: {id}. Failing note and Continuing".format(id=blobid))
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=common_variables.NLP_NER_FAILED)
            continue

        if sectionerx_results is None:
            print(
                "No NER sectionerx results returned for id: {id}. Failing note and Continuing".format(id=blobid))
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=common_variables.NLP_NER_FAILED)
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
            common_functions.write_to_storage(blobid = blobid,
                                    sourcetable = 'af_ner_runs_details',
                                    job_state_type = 'ner_status',
                                    updatedate_type = 'resynth_date',
                                    update_date = resynth_date,
                                    payload=json_obj_to_store,
                                    key=common_functions.get_default_keyname(blobid, prefix=common_hooks.BLOB_PROCESS_PREFIX),
                                    connection=common_hooks.MYSTOR)
            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=common_variables.NLP_NER_COMPLETE)
            _update_ner_runs(run_id, state=common_variables.NLP_NER_COMPLETE)

        except Exception as e:
            message = "Exception occurred: {}".format(e)
            common_functions.log_error_and_failure_for_ner_job(run_id, blobid, hdcpupdatedate, message,
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

    return common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                     parameters=(state, datetime.now(), run_id, hdcpupdatedate, blobid))

def _get_ner_run_details_id_by_resynth_date(run_id, date):
    tgt_select_stmt = "SELECT DISTINCT hdcorcablobid, hdcpupdatedate " \
                      "FROM af_ner_runs_details " \
                      "WHERE af_ner_runs_id = %s " \
                      "AND resynth_date = %s " \
                      "AND ner_status = %s"

    return common_hooks.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date, common_variables.JOB_RUNNING))

def _update_ner_runs(run_id, state):
    tgt_update_stmt = "UPDATE af_ner_runs SET job_end = %s, job_status = %s WHERE af_ner_runs_id = %s"
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), state, run_id), autocommit=True)

def _update_job_id_as_failed(run_id):
    tgt_update_stmt = "UPDATE af_ner_runs SET job_end = %s, job_status = %s WHERE af_ner_runs_id = %s"
    common_hooks.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), common_variables.NLP_NER_FAILED, run_id), autocommit=True)