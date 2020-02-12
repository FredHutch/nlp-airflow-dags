import json
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
import utilities.job_states as job_states
import utilities.common as common

def generate_email_operator(dag, task_id, subject, content):
    email_operator = EmailOperator(to=common.DEFAULT_EMAIL_TGT,
                                   task_id=task_id,
                                   subject=subject,
                                   html_content=content,
                                   dag=dag)

    return email_operator

