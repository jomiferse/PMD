from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from .api.router import api_router
from .db_utils import is_statement_timeout
from .logging_conf import configure_logging
from .rate_limit import RateLimitMiddleware
from .request_logging import RequestLoggingMiddleware
from .settings import settings

configure_logging()

DISCLAIMER = (
    "Read-only analytics. Manual execution only. Not financial advice. "
    "No guarantee of outcomes. No custody. No execution."
)

app = FastAPI(
    title="PMD - Polymarket Mispricing Detector",
    description=DISCLAIMER,
)


@app.exception_handler(OperationalError)
async def handle_db_errors(request: Request, exc: OperationalError):
    if is_statement_timeout(exc):
        return JSONResponse(status_code=503, content={"detail": "db_timeout"})
    return JSONResponse(status_code=500, content={"detail": "db_error"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(RequestLoggingMiddleware)

app.include_router(api_router)
