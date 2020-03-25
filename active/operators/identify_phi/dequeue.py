import utilities.common_hooks as common_hooks


def dequeue_blobid_from_process_queue(**kwargs):
  '''
  get a job tuple from the source database
  :param kwargs:
  :return: a list of tuples: (blobid, hdcpupdatedate)
  '''
  return dequeue_batch_blobid_from_process_queue(1, **kwargs)

def dequeue_batch_blobid_from_process_queue(batch_size, **kwargs):
  '''
  get a batch of job tuples from the source database
  :param batch_size: the number of job_tuples to dequeue
  :param kwargs:
  :return: a list of tuples: (blobid, hdcpupdatedate)
  '''
  exec_stmt = ("EXEC dbo.sp_dequeue_note_id 'dbo.clinical_notes_process_queue', {batch_size}".format(batch_size=batch_size))
  results = (common_hooks.ANNOTATIONS_DB.get_records(exec_stmt) or (None, None))
  print("Dequeued results: {}".format(results))

  records = []
  for row in results:
    records.append((row[0], row[1]))

  return records

