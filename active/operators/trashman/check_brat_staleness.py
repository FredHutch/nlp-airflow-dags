import json
from datetime import datetime, timedelta
import active.utilities.job_states as job_states
import active.utilities.common as common

def check_brat_modification_date(**kwargs):
    """
    Checks contents in brat for staleness.

    """

    #TODO: Generate a job_id and pair with staleness check from DB.

    job_start_date = datetime.now() #this should go in the job generation instead maybe?
    datefolder = kwargs['datefolder']
    #new_run_id = generate_job_id()
    ssh_hook = SSHHook(ssh_conn_id="prod-brat")
    remote_nlp_home_path = "/mnt/encrypted/brat-v1.3_Crunchy_Frog/data/nlp"
    #list all the files ending with ann and find the modified dates
    remote_command = """find {dir} -type f -name \*.ann -printf '%TY-%Tm-%Td:%TT\t%p\n'""".format(dir=remote_nlp_home_path) 
    #output of check_output is in bytes
    output = subprocess.check_output(["ssh", "-o StrictHostKeyChecking=no", "-p {}".format(ssh_hook.port),
                 "{}@{}".format(ssh_hook.username, ssh_hook.remote_host), remote_command])
    check_date = datetime.now()
    #converte bytes to str/datetime datatypes
    parsed_output  = parse_remote_output(output)
    brat_files = compare_dates(parsed_find_output)
    write_run_details(new_run_id, check_date, brat_files)
    return new_run_id, job_start_date, check_date


def parse_remote_output(remote_command_output):
    """
    Checks contents in brat for staleness.

    param: remote_command_output - UTF-8 encoded output from a subprocess call

    Returns:
        brat_files: list of dictionaries containing ModifiedDates and the absolute path +
        and number of days elapsed from today
    """

    #decode subprocess output from bytes to str
    brat_files = []
    decoded = remote_command_output.decode("utf-8")
    current_date = datetime.now()
    #parse into list of dictionaries
    for line in decoded.splitlines():
        split_string = line.split('\t')
        modified_date, path = split_string[0], split_string[1]
        files = {
                 'File': path, 
                 'ModifiedDate': datetime.strptime(modified_date, '%Y-%m-%d'), 
                 'ElapsedTime': current_date - datetime.strptime(modified_date, '%Y-%m-%d')
                }
        brat_files.append(files)
    return brat_files

def compare_dates(brat_files):
    """
    Compares dates with predefined airflow threshold (days)
    """
    for file in brat_files:
        if file['ElapsedTime'] > STALE_THRESHOLD:
            #flag as a stale
            file.update({'IsStale': 1})
        else:
            file.update({'IsStale': 0})
    return brat_files

def get_stale_count(brat_files):
    """
    Calculates number of stale files
    """
    stale_count = 0
    for file in brat_files:
        count += file['IsStale']
    return stale_count

def write_run_details(run_id, check_date, brat_files, threshold=STALE_THRESHOLD):
    """
    Writes run statistics on stale v. nonstale files in brat. Used to track modification over time.
    param: brat_files: list of dicts containing File, ModifiedDate, ElapsedTime, and IsStale
    """
    tgt_insert_stmt = ("INSERT INTO af_trashman_runs_details"\
                       "(af_trashman_runs_id, brat_capacity, stale_count, non_stale_count, stale_threshold, stale_check_date)"\
                       "VALUES (%s, %s, %s, %s, %s, %s)")
    brat_capacity = len(brat_files)
    #get count of stale v. nonstale in current run
    stale_count = get_stale_count(brat_files)
    non_stale_count = stale_count - brat_capacity
    #write job_id, count of stale vs nonstale to db, and threshold parameter
    common.NLP_DB.run(tgt_insert_stmt, parameters=(run_id, brat_capacity, stale_count, non_stale_count, threshold, str(check_date)))

def notify_email(context, **kwargs):
    """
    Sends a notification email to the service account.
    TODO: Redo tests on email operator
    """
    alert = EmailOperator(
        task_id=alertTaskID,
        #for testing purposes
        to='ritche.long@fredhutch.org',
        subject='Stale Annotations Report',
        html_content='test',
        dag=dag
    )
    title = ("Trashman Report: Stale Files in Brat for {trashman_run}".format(trashman_run=str(datetime.now()))
    body = json.dumps(context)
