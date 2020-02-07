from .generate_job_id import generate_job_id
from .check_brat_staleness import check_brat_staleness
from .check_resynth_tasks import check_resynth_tasks
from .redrive_jobs import redrive_jobs, _redrive_brat_stale_task

from active.operators.ner import run_ner_tasks_and_save_to_source_operator

REDRIVE_RUNS_TABLE = "af_trashman_runs"
REDRIVE_RUN_ID = "af_trashman_runs_id"

REDRIVE_TASK_FN = {
    'BRAT_STALE': _redrive_brat_stale_task,
    'RESYNTH': run_ner_tasks_and_save_to_source_operator,
    'DEID': True,
}