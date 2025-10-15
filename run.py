#!/usr/bin/env python3
"""
Stock Digest Application Launcher
Simple script to run the standalone stock digest service
"""

import os
import sys
from pathlib import Path

def main():
    """Launch the stock digest application."""
    print("🚀 Stock Digest Application")
    print("=" * 40)
    
    # Check if we're in the right directory
    current_dir = Path.cwd()
    if not (current_dir / "core").exists():
        print("❌ Error: Please run this script from the Stock Digest directory")
        print(f"Current directory: {current_dir}")
        print("Expected: ~/Stock Digest")
        return False
    
    # Create necessary directories
    os.makedirs("logs", exist_ok=True)
    os.makedirs("cache", exist_ok=True)
    
    print("✅ Directories created")
    print("✅ Starting FastAPI application...")
    print()
    print("🌐 Service will be available at: http://localhost:8000")
    print("📚 API Documentation: http://localhost:8000/docs")
    print("🔍 Health Check: http://localhost:8000/health")
    print()
    print("Press Ctrl+C to stop the service")
    print()
    
    try:
        # Import and run the FastAPI app
        from api.main import app
        import uvicorn
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure all dependencies are installed:")
        print("   pip install -r requirements.txt")
        return False
        
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
