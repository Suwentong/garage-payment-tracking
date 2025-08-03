from fastapi import FastAPI
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

# Include routers
app.include_router(report_router, prefix="/api/v1", tags=["reports"])

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Garage Payment Tracking API",
        "version": "1.0.0",
        "endpoints": {
            "generate_report": "/api/v1/generate-report",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
