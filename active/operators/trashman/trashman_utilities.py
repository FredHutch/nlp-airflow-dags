import utilities.common_variables as common_variables
import utilities.common_functions as common_functions
from datetime import datetime, timedelta

def compare_dates(brat_files, stale_threshold=common_variables.STALE_THRESHOLD):
    """
    Compares dates with predefined airflow threshold (days)
    """
    for file in brat_files:
        file.update({'IsStale': (1 if file.get('ElapsedTime') > stale_threshold else 0)})
    return brat_files


def parse_remote_output(remote_command_output, check_date):
    """
    Checks contents in brat for staleness.

    param: remote_command_output - UTF-8 encoded output from a subprocess call

    Returns:
        brat_files: list of dictionaries containing ModifiedDates and the absolute path +
        and number of days elapsed from today
    """

    #decode subprocess output from bytes to str
    brat_files = []
    #decoded = remote_command_output.decode("utf-8")
    #parse into list of dictionaries
    for line in remote_command_output.splitlines():
        split_string = line.split('\t')
        print("split_string is {}".format(split_string))
        try:
            modified_date, path = datetime.strptime(split_string[0], '%Y-%m-%d'), split_string[1]
        except ValueError as e:
            common_functions.log_error_message(blobid="",
                                     hdcpupdatedate="",
                                     state="Brat DateTime Parsing",
                                     time=common_functions.generate_timestamp(),
                                     error_message=e)
            continue
        files = {
                 'File': path,
                 'ModifiedDate': modified_date,
                 'ElapsedTime': check_date - modified_date
                }
        brat_files.append(files)
    return brat_files

def safe_datetime_strp(time, fmt):
    try:
        end_date = datetime.strptime(time, fmt)
    except ValueError as v:
        ulr = len(v.args[0].partition('unconverted data remains: ')[2])
        if ulr:
            end_date = datetime.strptime(time[:-ulr], fmt)
        else:
            raise v

    return end_date
