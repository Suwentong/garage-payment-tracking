from fastapi import APIRouter

from app.api.v1.endpoints import report

api_router = APIRouter()

api_router.include_router(report.router, prefix="/report", tags=["Отчеты"])
