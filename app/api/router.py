from fastapi import APIRouter

from .routes import admin, alerts, auth, billing, entitlements, health, jobs, settings, snapshots, webhooks

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(billing.router)
api_router.include_router(webhooks.router)
api_router.include_router(alerts.router)
api_router.include_router(snapshots.router)
api_router.include_router(settings.router)
api_router.include_router(entitlements.router)
api_router.include_router(admin.router)
api_router.include_router(jobs.router)
