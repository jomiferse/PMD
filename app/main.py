from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.router import api_router
from .logging_conf import configure_logging
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
