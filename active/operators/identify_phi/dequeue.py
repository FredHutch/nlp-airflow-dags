import utilities.common_hooks as common_hooks


def dequeue_blobid_from_process_queue(**kwargs):
  exec_stmt = ("EXEC dbo.sp_dequeue_note_id 'dbo.clinical_notes_process_queue'")
  results = (common_hooks.ANNOTATIONS_DB.get_first(exec_stmt) or (None, None))
  print("Dequeued results: {}".format(results))

  return (results[0], results[1])
