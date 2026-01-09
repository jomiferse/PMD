from fastapi import APIRouter, Depends

from ...integrations.rq_queue import q
from ...jobs.run import job_sync_wrapper
from ...rate_limit import rate_limit

router = APIRouter()


@router.post("/jobs/ingest")
def ingest_job(_=Depends(rate_limit)):
    job = q.enqueue(job_sync_wrapper)
    return {"job_id": job.id}
