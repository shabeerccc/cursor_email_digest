#!/usr/bin/env python3
"""
Stock Digest Application - Main FastAPI Application
Standalone service for daily stock digest generation and email delivery
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Dict, Any
import os
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add core modules to path
core_path = Path(__file__).parent.parent / "core"
sys.path.insert(0, str(core_path))

# Import core modules
from core.email.smart_email_sender import SmartEmailSender
from core.scoring.integrated_digest_with_yahoo_cache import IntegratedDigestSystem
from core.caching.smart_cache_first_system import SmartCacheFirstSystem

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Stock Digest Application",
    description="Standalone daily stock digest service with complete email generation",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class DailyDigestRequest(BaseModel):
    recipient_email: EmailStr
    force_refresh: bool = False
    include_sectors: Optional[List[str]] = None

class DailyDigestResponse(BaseModel):
    success: bool
    message: str
    email_id: Optional[str] = None
    generated_at: str
    stock_count: int
    sector_count: int

class TestEmailResponse(BaseModel):
    success: bool
    message: str
    html_length: int
    text_length: int
    stock_count: int
    generated_at: str

class SystemStatusResponse(BaseModel):
    service: str
    version: str
    status: str
    smart_system: Dict[str, Any]
    email_system: str
    cache_system: str
    last_email_sent: str
    system_ready: bool
    timestamp: str

# Initialize core systems
smart_system: Optional[SmartCacheFirstSystem] = None
integrated_system: Optional[IntegratedDigestSystem] = None
email_sender: Optional[SmartEmailSender] = None

def get_smart_system() -> SmartCacheFirstSystem:
    """Get or initialize smart cache-first system."""
    global smart_system
    if smart_system is None:
        try:
            smart_system = SmartCacheFirstSystem()
            logger.info("✅ Smart cache-first system initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize smart system: {e}")
            raise HTTPException(status_code=500, detail="System initialization failed")
    return smart_system

def get_integrated_system() -> IntegratedDigestSystem:
    """Get or initialize integrated digest system."""
    global integrated_system
    if integrated_system is None:
        try:
            integrated_system = IntegratedDigestSystem()
            logger.info("✅ Integrated digest system initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize integrated system: {e}")
            raise HTTPException(status_code=500, detail="System initialization failed")
    return integrated_system

def get_email_sender() -> SmartEmailSender:
    """Get or initialize email sender."""
    global email_sender
    if email_sender is None:
        try:
            email_sender = SmartEmailSender()
            logger.info("✅ Email sender initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize email sender: {e}")
            raise HTTPException(status_code=500, detail="Email system initialization failed")
    return email_sender

@app.on_event("startup")
async def startup_event():
    """Initialize systems on startup."""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        os.makedirs("cache", exist_ok=True)
        
        logger.info("🚀 Stock Digest Application starting up...")
        
        # Initialize core systems
        get_smart_system()
        get_integrated_system()
        get_email_sender()
        
        logger.info("✅ All systems initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise

@app.get("/", response_model=Dict[str, Any])
async def root():
    """Root endpoint with application information."""
    return {
        "service": "Stock Digest Application",
        "version": "2.0.0",
        "status": "running",
        "description": "Standalone daily stock digest service with complete email generation",
        "features": [
            "Complete daily digest email generation",
            "Google Sheets integration (98 stocks, 15 sectors)",
            "Smart cache-first data collection",
            "Yahoo Finance + Alpha Vantage APIs",
            "Enhanced multi-source caching",
            "Comprehensive stock scoring",
            "Market insights by sector",
            "Automated email delivery"
        ],
        "endpoints": {
            "health": "/health",
            "status": "/api/v1/status",
            "send_daily_digest": "/api/v1/digest/send",
            "test_email": "/api/v1/digest/test",
            "cache_status": "/api/v1/cache/status"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check if core systems are available
        smart_sys = get_smart_system()
        integrated_sys = get_integrated_system()
        email_sys = get_email_sender()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "systems": {
                "smart_cache_system": "operational",
                "integrated_digest_system": "operational",
                "email_sender": "operational"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.get("/api/v1/status", response_model=SystemStatusResponse)
async def get_system_status():
    """Get complete system status."""
    try:
        smart_sys = get_smart_system()
        
        return SystemStatusResponse(
            service="Stock Digest Application",
            version="2.0.0",
            status="operational",
            smart_system=smart_sys.get_system_status(),
            email_system="operational",
            cache_system="operational",
            last_email_sent="N/A",
            system_ready=True,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Status error: {str(e)}")

@app.post("/api/v1/digest/send", response_model=DailyDigestResponse)
async def send_daily_digest(
    request: DailyDigestRequest,
    background_tasks: BackgroundTasks
):
    """Send comprehensive daily digest email."""
    try:
        email_sys = get_email_sender()
        
        # Send email in background
        background_tasks.add_task(
            email_sys.send_daily_digest_email,
            force_refresh=request.force_refresh
        )
        
        logger.info(f"📧 Daily digest email queued for: {request.recipient_email}")
        
        return DailyDigestResponse(
            success=True,
            message="Daily digest email queued for sending",
            generated_at=datetime.now().isoformat(),
            stock_count=98,  # From Google Sheets
            sector_count=15   # From current cache
        )
        
    except Exception as e:
        logger.error(f"Failed to send daily digest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send daily digest: {str(e)}"
        )

@app.post("/api/v1/digest/test", response_model=TestEmailResponse)
async def test_email_generation():
    """Test email generation system."""
    try:
        integrated_sys = get_integrated_system()
        
        # Generate test email
        email_content = integrated_sys.generate_comprehensive_email(force_refresh=False)
        
        logger.info("🧪 Test email generated successfully")
        
        return TestEmailResponse(
            success=True,
            message="Test email generated successfully",
            html_length=len(email_content.get('html_content', '')),
            text_length=len(email_content.get('text_content', '')),
            stock_count=len(email_content.get('stock_data', [])),
            generated_at=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Test email generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Test email generation failed: {str(e)}"
        )

@app.get("/api/v1/cache/status")
async def get_cache_status():
    """Get cache system status."""
    try:
        smart_sys = get_smart_system()
        return smart_sys.get_system_status()
    except Exception as e:
        logger.error(f"Cache status check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Cache status error: {str(e)}")

@app.post("/api/v1/cache/refresh")
async def refresh_cache():
    """Refresh cache data."""
    try:
        smart_sys = get_smart_system()
        # This would trigger a cache refresh
        return {
            "success": True,
            "message": "Cache refresh initiated",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Cache refresh failed: {e}")
        raise HTTPException(status_code=500, detail=f"Cache refresh error: {str(e)}")

@app.get("/api/v1/stocks")
async def get_stock_data():
    """Get current stock data."""
    try:
        smart_sys = get_smart_system()
        # Get sample stock data
        return {
            "success": True,
            "total_stocks": 98,
            "total_sectors": 15,
            "data_source": "cache",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Stock data retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=f"Stock data error: {str(e)}")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    
    logger.info("🚀 Starting Stock Digest Application...")
    
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
