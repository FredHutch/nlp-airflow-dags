import active.utilities.job_states as job_states
import active.utilities.common as common

import swiftclient
from datetime import datetime

from airflow.operators.python_operator import PythonOperator


def generate_job_id(**kwargs):
    """
    return job id since last task (NER) run
    """
    last_run_id = _get_last_ner_run_id()
    last_ner_update_date = _get_last_ner_update_date()

    if last_run_id is None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    print("starting batch run ID: {id} of blobs since {date}".format(id=new_run_id,
                                                                     date=last_ner_update_date))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    datecreated = []
    blob_job_queue = _get_blobs_since_date(common.swift_conn, common.container_name, last_ner_update_date)

    if blob_job_queue is None:
        print("No new records found since last update date: {}".format(last_ner_update_date))
        exit()
    else:
        for day in blob_job_queue.values():
            datecreated.append(day)
            _insert_ner_scheduled(new_run_id, day, job_start_date)

        print("{} new update batches found since last update date: {}".format(len(datecreated), last_ner_update_date))

    return new_run_id, datecreated


def _insert_ner_scheduled(run_id, update_date, job_start_date):
    tgt_insert_stmt = "INSERT INTO af_ner_runs " \
                      "(af_ner_runs_id, source_last_update_date, job_start, job_status) " \
                      "VALUES (%s, %s, %s, %s, %s)"
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, update_date, job_start_date, job_states.JOB_RUNNING))

    return


def _get_blobs_since_date(store_connection, container, last_ner_update_date):
    """
    get all the blobs and update date from swift
    return {'blob filename': 'last modified date', ...}
    To-do: create a job status table to track run status.
    """
    if type(store_connection) == swiftclient.client.Connection:

        return {data['name']:data['last_modified']
                for data in store_connection.get_container(container)[1]\
                if 'swift_test' in data['name']\
                and data['last_modified'] >= last_ner_update_date}
    else:
        print('Input {} is not valid'.format(store_connection))
        return None



def _get_last_ner_run_id():
    tgt_select_stmt = "SELECT max(af_ner_runs_id) FROM af_ner_runs"
    last_run_id = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]



def _get_last_ner_update_date():
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_ner_runs WHERE job_status = %s"
    last_ner_update_date = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                            parameters=(job_states.JOB_COMPLETE,)) or (None,))

    return last_ner_update_date[0]

def generate_job_id_operator(dag, default_args):
    generate_job_id_operator = \
        PythonOperator(task_id='{}_{}'.format(dag.task_id, 'generate_job_id'),
                       provide_context=True,
                       python_callable=generate_job_id,
                       )

    return generate_job_id_operator