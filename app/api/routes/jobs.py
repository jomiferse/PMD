from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from ...db import get_db
from ...deps import _require_session_user
from ...integrations.rq_queue import q
from ...jobs.run import job_sync_wrapper

router = APIRouter()

def _require_jobs_session(request: Request, db: Session = Depends(get_db)):
    return _require_session_user(request, db)


@router.post("/jobs/ingest")
def ingest_job(_=Depends(_require_jobs_session)):
    job = q.enqueue(job_sync_wrapper)
    return {"job_id": job.id}
