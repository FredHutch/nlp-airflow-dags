from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

import utilities.common as common
import utilities.job_states as job_states
import operators.ner as ner
import operators.ner.flask_blob_nlp as flask_blob_nlp
from operators.ner.get_resynthesized_notes import get_resynthesized_note

from datetime import datetime
import json

def run_ner_tasks_and_save_to_source(parent_dag_name, child_dag_name, start_date, schedule_interval,  xcom_source, **kwargs):
    (run_id, createddate) = kwargs['ti'].xcom_pull(task_ids=xcom_source)
    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    for x in range(run_id):
        bash_cmd = "echo B{}".format(x)
        task_operator = PythonOperator(
            task_id='ner_task*{}'.format(x),
            provide_context=True,
            python_callable=__run_ner_task_dag,
            op_kwargs={'blobid': run_id, 'hdcpupdatedate': hdcpupdatedate},
            dag=dag,
        )
    return dag

def __run_ner_task_dag(**kwargs):

    return


def run_ner_task(**kwargs):
    # get last update date
    (run_id, createddate) = kwargs['ti'].xcom_pull(task_ids='generate_job_id')
    for creation_date in createddate:
        # record number of NER tasks
        record_processed = 0
        hdcpupdatedate = creation_date

        for id_record in _get_ner_run_details_id_by_creation_date(run_id, creation_date):
            record_processed = 0
            batch_records = {}

            blobid = id_record[0]
            hdcpupdatedate = id_record[1]
            note = get_resynthesized_note(blobid)

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

            batch_records[blobid] = results

            try:
                # save json to db
                json_obj_to_store = json.dumps(results, indent=4, sort_keys=True)
                # save annotated notes to object store
                common.write_to_storage(blobid,
                                        hdcpupdatedate,
                                        payload=json_obj_to_store,
                                        key='deid_test/NER_blobs/{id}.json'.format(id=blobid),
                                        connection=common.MYSTOR)

            except Exception as e:
                message = "Exception occurred: {}".format(e)
                common.log_error_and_failure_for_ner_job(run_id, blobid, hdcpupdatedate, message,
                                                         "Save JSON NER Blobs")
                continue

            _update_ner_run_details(run_id, blobid, hdcpupdatedate, state=job_states.NLP_NER_COMPLETE)
            record_processed += 1

        _update_job_id_as_complete(run_id)
        print("{} records processed for update date: {}".format(record_processed, hdcpupdatedate))


def _update_ner_run_details(run_id, blobid, hdcpupdatedate, state):
    tgt_update_stmt = "UPDATE af_ner_runs_details" \
                      "SET ner_status = %s, ner_date = %s" \
                      "WHERE af_ner_runs_id = %s" \
                      "AND hdcpupdatedate = %s and hdcorcablobid in (%s)"

    print("updating blob {} to {}".format(blobid, state))

    return common.AIRFLOW_NLP_DB.run(tgt_update_stmt,
                                     parameters=(state, datetime.now(), run_id, hdcpupdatedate, blobid))

def _get_ner_run_details_id_by_creation_date(run_id, date):
    tgt_select_stmt = "SELECT hdcorcablobid, hdcpupdatedate FROM af_ner_runs_details " \
                      "WHERE af_ner_runs_id = %s and ner_date = %s and ner_status = %s"

    return common.AIRFLOW_NLP_DB.get_records(tgt_select_stmt, parameters=(run_id, date, common.JOB_RUNNING))

def _update_job_id_as_complete(run_id):
    tgt_update_stmt = "UPDATE af_ner_runs SET job_end = %s, job_status = %s WHERE af_ner_runs_id = %s"
    common.AIRFLOW_NLP_DB.run(tgt_update_stmt, parameters=(datetime.now(), job_states.NLP_NER_COMPLETE, run_id), autocommit=True)

def run_ner_tasks_and_save_to_source_operator(main_dag, **kwargs):
    child_dag_name = kwargs.get('child_dag_name')
    xcom_source = kwargs.get('upstream_operator', 'populate_blobid_in_job_table')
    sub_dag = SubDagOperator(
        subdag=run_ner_tasks_and_save_to_source(parent_dag_name=main_dag.task_id,
                                                child_dag_name=child_dag_name,
                                                start_date=main_dag.start_date,
                                                schedule_interval=main_dag.schedule_interval,
                                                xcom_source=xcom_source),
        task_id=child_dag_name,
        dag=main_dag,
    )
    return sub_dag