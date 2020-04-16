import utilities.common_hooks as common_hooks
from pymssql import OperationalError

def requeue_blobid_to_process_queue(blobid, hdcpupdatedate, **kwargs):
  '''
  re-enqueue a blobid, hdcpupdatedate tuple to the queue.
  :param blobid: the blobid of the job
  :param hdcpupdatedate: the hdcpupdatedate of the job
  :param kwargs:
  :return:
  '''
  annotations_db = common_hooks.get_annotations_db_hook()
  try:
    exec_stmt = ("EXEC dbo.sp_requeue_note_id 'dbo.clinical_notes_process_queue', %s, %s")
    results = annotations_db.run(exec_stmt, parameters=(blobid, hdcpupdatedate))
  except OperationalError as e:
    message = ("A OperationalError occurred while trying to store person data to source for"
               " for blobid, hdcpupdatedate: ({blobid}, {updatedate})\n"
               "{error}".format(blobid=blobid, updatedate=hdcpupdatedate, error=e))
    print(message)

  return