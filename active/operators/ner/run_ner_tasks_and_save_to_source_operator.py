import json
from datetime import datetime

import utilities.common as common
import utilities.job_states as job_states
from operators.ner import ner


def run_ner_task(**kwargs):
    # get last update date
    (run_id, resynthdate) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')

    for resynth_date in resynthdate:
        # record number of NER tasks
        record_processed = 0

        for id_record in _get_ner_run_details_id_by_resynth_date(run_id, resynth_date):

            batch_records = {}

            blobid = id_record[0]
            hdcpupdatedate = id_record[1]

            try:
                results = ner._call_flask_blob_nlp(blobid)
                batch_records[blobid] = results

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

            print("{} records processed for update date: {}".format(record_processed, hdcpupdatedate))


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