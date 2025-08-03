from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import logging

from app.api.v1.endpoints.report import router as report_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Garage Payment Tracking API",
    description="API for tracking garage rental payments and generating status reports",
    version="1.0.0"
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(report_router, prefix="/api/v1", tags=["reports"])

@app.get("/")
async def root():
    """Serve the main HTML page."""
    return FileResponse("app/static/index.html")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
