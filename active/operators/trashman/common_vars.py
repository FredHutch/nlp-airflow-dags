REDRIVE_RUNS_TABLE = "af_trashman_runs"
REDRIVE_JOBS_TABLE = "af_trashman_runs_details"
REDRIVE_RUN_ID = "af_trashman_runs_id"

REDRIVE_TASK_FN = {
    'BRAT_STALE': _redrive_brat_stale_task,
    'RESYNTH': run_ner_tasks_and_save_to_source_operator,
    'DEID': True
}