import asyncio
from sqlalchemy.orm import Session
from ..db import SessionLocal
from .tasks import run_ingest_and_alert, run_cleanup

def job_sync_wrapper():
    db: Session = SessionLocal()
    try:
        return asyncio.run(run_ingest_and_alert(db))
    finally:
        db.close()


def cleanup_sync_wrapper():
    db: Session = SessionLocal()
    try:
        return run_cleanup(db)
    finally:
        db.close()
