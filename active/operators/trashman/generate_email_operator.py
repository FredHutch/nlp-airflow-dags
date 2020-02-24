import json
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
import utilities.common_variables as common_variables

def generate_email_operator(dag, task_id, subject, content):
    email_operator = EmailOperator(to=common_variables.DEFAULT_EMAIL_TGT,
                                   task_id=task_id,
                                   subject=subject,
                                   html_content=content,
                                   dag=dag)

    return email_operator

