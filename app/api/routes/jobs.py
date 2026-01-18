from fastapi import APIRouter, Depends

from ...deps import _require_session_user
from ...integrations.rq_queue import q
from ...jobs.run import job_sync_wrapper

router = APIRouter()


@router.post("/jobs/ingest")
def ingest_job(_=Depends(_require_session_user)):
    job = q.enqueue(job_sync_wrapper)
    return {"job_id": job.id}
