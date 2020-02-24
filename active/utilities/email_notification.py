from airflow.operators.email_operator import EmailOperator


def send_notification(context, **kwargs):
    """
    # stub for common/shared email notification from airflow
    """
    alert = EmailOperator(
        task_id='email_notification',
        to=kwargs['recipient'],
        subject=kwargs['subject'],
        html_content=kwargs['message'],
        dag=dag
    )