import json
from datetime import datetime, timedelta
import active.utilities.job_states as job_states
import active.utilities.common as common
"""
TODO:

"""

alert = EmailOperator(
    task_id=alertTaskID,
    #for testing purposes
    to='ritche.long@fredhutch.org',
    subject='Stale Annotations Report',
    html_content='test',
    dag=dag
)



def check_brat_modification_date(**kwargs):
    """
    Checks contents in brat for staleness.
    """
    job_start_date = datetime.now()
    datefolder = kwargs['datefolder']
    remote_nlp_home_path = "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")
    #list all the files ending with ann and find the modified dates
    remote_command = """find {dir} -type f -name \*.ann -printf '%TY-%Tm-%Td:%TT\t%p\n'""".format(dir=remote_nlp_home_path) 
    #output of check_output is in bytes
    output = subprocess.check_output(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                 "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])
    
    #converte bytes to str/datetime datatypes
    parsed_output  = parse_remote_output(output)
    time_dict = compare_dates(parsed_find_output)
    write_run_details(time_dict)

    return new_run_id, job_start_date


def parse_remote_output(remote_command_output):
    """
    Checks contents in brat for staleness.

    param:


    Returns:

        brat_files: list of dictionaries containing ModifiedDates and the absolute path + filename
    """

    #decode subprocess output from bytes to str
    brat_files = []
    decoded = remote_command_output.decode("utf-8")
    current_date = datetime.now()
    #parse into list of dictionaries
    for line in decoded.splitlines():
        split_string = line.split('\t')
        modified_date, path = split_string[0], split_string[1]
        files = {'File': path, 
                     'ModifiedDate': datetime.strptime(modified_date, '%Y-%m-%d'), 
                     'ElapsedTime': current_date - datetime.strptime(modified_date, '%Y-%m-%d')}
        brat_files.append(files)
    return brat_files

def compare_dates(brat_files):
    """
    """
    for file in brat_files:
        if file['ElapsedTime'] < STALE_THRESHOLD:
            #flag as a stale
            file.update({'Stale': 1})
        else:
            file.update({'Stale': 0})

    return None

def write_run_details(brat_files):
    """
    Writes run statistics on stale v. nonstale files in brat. Used to track modification over time.
    Args:
    Returns:
    """
    tgt_insert_stmt = ("""INSERT INTO trashman_runs_details (af_trashman_runs_id, brat_capacity, stale_count, non_stale_count, stale_check_date) VALUES (%s, %s, %s, %s, %s)""")
    #get count of stale v. nonstale in current run

    #write job_id, count of stale vs nonstale to db, and threshold parameter
    pass


def notify_email(context, **kwargs):
    """
    Sends a notification email to the service account for bookkeeping.

    Args:
        context: a dict containing
    Returns:
    """
    if(task_instance.xcom_pull(task_ids=None, dag_id=dag_id, key=dag_id) == True):
        logging.info("Other failing task has been notified.")
    send_email = EmailOperator()

    title = ("Trashman Report: Stale Files in Brat for {trashman_run}".format(trashman_run=str(datetime.now()))
    body = json.dumps(context)
    send_email()
