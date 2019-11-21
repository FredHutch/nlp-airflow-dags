import datetime
import active.utilities.job_states as job_states
import active.utilities.common as common

from airflow.operators.python_operator import PythonOperator


def generate_job_id(**kwargs):
    last_run_id = _get_last_resynth_run_id()

    if last_run_id is None:
        new_run_id = 1
    else:
        new_run_id = last_run_id + 1

    print("starting batch run ID: {id} of annotations since {date}".format(id=new_run_id,
                                                                           date=update_date_from_last_run))
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    job_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    datecreated = []
    for row in _get_annotations_since_date(update_date_from_last_run):
        datecreated.append(row[0])
        _insert_resynth_run_job(new_run_id, row[0], row[1], job_start_date)

    if len(datecreated) == 0:
        print("No new records found since last update date: {}".format(update_date_from_last_run))
        exit()

    print("{} new update batches found since last update date: {}".format(len(datecreated), update_date_from_last_run))

    return new_run_id, datecreated


def _insert_resynth_run_job(run_id, update_date, record_count, job_start_date):
    tgt_insert_stmt = "INSERT INTO af_resynthesis_runs " \
                      "(af_resynth_runs_id, source_last_update_date, record_counts, job_start, job_status) " \
                      "VALUES (%s, %s, %s, %s, %s)"
    common.AIRFLOW_NLP_DB.run(tgt_insert_stmt,
                              parameters=(run_id, update_date, record_count, job_start_date, job_states.JOB_RUNNING))

    return


def _get_annotations_since_date(update_date_from_last_run):
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = "SELECT date_created, count(*) " \
                      "FROM annotations " \
                      "WHERE date_created >= %s " \
                      "AND (category = %s " \
                      "     OR category = %s) " \
                      "GROUP BY date_created "

    return common.ANNOTATIONS_DB.get_records(src_select_stmt,
                                             parameters=(update_date_from_last_run,
                                                         BRAT_REVIEWED_ANNOTATION_TYPE,
                                                         REVIEW_BYPASSED_ANNOTATION_TYPE))



def _get_last_resynth_run_id():
    tgt_select_stmt = "SELECT max(af_resynth_runs_id) FROM af_resynthesis_runs"
    last_run_id = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt) or (None,))

    return last_run_id[0]


def _get_last_resynth_update_date():
    tgt_select_stmt = "SELECT max(source_last_update_date) FROM af_resynthesis_runs WHERE job_status = %s"
    update_date_from_last_run = (common.AIRFLOW_NLP_DB.get_first(tgt_select_stmt,
                                                                 parameters=(job_states.JOB_COMPLETE,)) or (None,))

    return update_date_from_last_run[0]

def generate_job_id_operator(dag, default_args):
    generate_job_id_operator = \
        PythonOperator(task_id='{}_{}'.format(dag.task_id, 'generate_job_id'),
                       provide_context=True,
                       python_callable=generate_job_id,
                       )

    return generate_job_id_operator