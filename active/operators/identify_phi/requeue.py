import utilities.common_hooks as common_hooks


def requeue_blobid_to_process_queue(blobid, hdcpupdatedate, **kwargs):
  '''
  re-enqueue a blobid, hdcpupdatedate tuple to the queue.
  :param blobid: the blobid of the job
  :param hdcpupdatedate: the hdcpupdatedate of the job
  :param kwargs:
  :return:
  '''
  exec_stmt = ("EXEC dbo.sp_requeue_note_id 'dbo.clinical_notes_process_queue', %s, %s")
  results = common_hooks.ANNOTATIONS_DB.run(exec_stmt, parameters=(blobid, hdcpupdatedate))

  return