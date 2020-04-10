from datetime import datetime, timedelta
import json
import subprocess
import base64
from airflow.hooks import HttpHook, MsSqlHook, PostgresHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
import operators.trashman as trashman

DAG_NAME ='af4-trashman'

args = {
    'owner': 'whiteau',
    'depends_on_past': False,
    'start_date': datetime(2019,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id=DAG_NAME,
          catchup=False,
	      default_args=args,
	      dagrun_timeout=timedelta(seconds=30))

generate_job_id = PythonOperator(task_id='generate_job_id',
	                             provide_context=True,
	                             python_callable=trashman.generate_job_id,
	                             dag=dag)

check_brat_staleness = PythonOperator(task_id='check_brat_staleness',
                                      provide_context=True,
                                      python_callable=trashman.check_brat_staleness,
                                      op_args={'generate_job_id'},
                                      dag=dag)

check_brat_completeness = PythonOperator(task_id='check_brat_completeness',
                                      provide_context=True,
                                      python_callable=trashman.check_brat_completeness,
                                      op_args={'generate_job_id'},
                                      dag=dag)

report_stale_brat_jobs = PythonOperator(task_id='report_stale_brat_jobs',
                              provide_context=True,
                              python_callable=trashman.report_stale_brat_jobs,
                              op_args={'check_brat_staleness'},
                              dag=dag)

mark_job_complete = PythonOperator(task_id='mark_job_complete',
                                   provide_context=True,
                                   python_callable=trashman.mark_job_complete,
                                   op_args={'check_brat_staleness'},
                                   trigger_rule=TriggerRule.ALL_DONE,
                                   dag=dag
                                   )

subject = "Airflow Trashman: Brat Stale Notes Report"
content = """
          {{ task_instance.xcom_pull(task_ids='report_stale_brat_jobs', key='email_body') }}
          """
send_stale_brat_email = trashman.generate_email_operator(dag, 'send_stale_brat_email', subject, content)

remove_complete_brat_jobs = PythonOperator(task_id='remove_complete_brat_jobs',
                              provide_context=True,
                              python_callable=trashman.remove_complete_brat_jobs,
                              op_args={'check_brat_completeness'},
                              trigger_rule=TriggerRule.ALL_SUCCESS,
                              dag=dag)


check_resynth_tasks = PythonOperator(task_id='check_resynth_tasks',
                                      provide_context=True,
                                      python_callable=trashman.check_resynth_tasks,
                                      op_args={'generate_job_id'},
                                      dag=dag)

redrive_resynth_jobs = PythonOperator(task_id='redrive_resynth_jobs',
                                      provide_context=True,
                                      python_callable=trashman.redrive_resynth_jobs,
                                      op_args={'check_resynth_tasks'},
                                      dag=dag)

#Brat Checks
generate_job_id >> check_brat_staleness >> report_stale_brat_jobs >> send_stale_brat_email
generate_job_id >> check_brat_completeness >> remove_complete_brat_jobs


#Resynthesis Checks
generate_job_id >> check_resynth_tasks >> redrive_resynth_jobs


[report_stale_brat_jobs, remove_complete_brat_jobs, redrive_resynth_jobs] >> mark_job_complete