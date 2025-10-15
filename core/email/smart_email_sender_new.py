#!/usr/bin/env python3
"""
Smart Email Sender
Automatically sends daily digest emails using cache-first system - no user input required
"""

import os
import sys
import logging
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SmartEmailSender:
    """
    Smart email sender that automatically uses cache-first system.
    No user input required - completely automated.
    """
    
    def __init__(self, use_modern_template: bool = True):
        """
        Initialize SmartEmailSender.
        
        Args:
            use_modern_template: If True, uses new modern email templates (default: True)
                                If False, uses legacy email generation
        """
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.use_modern_template = use_modern_template
        
        # Load environment variables
        self._load_environment()
        
        # Initialize smart cache-first system
        try:
            from core.caching.smart_cache_first_system import SmartCacheFirstSystem
            self.smart_system = SmartCacheFirstSystem()
            logger.info("✅ Smart cache-first system initialized")
        except ImportError:
            logger.error("❌ Smart cache-first system not available")
            raise
        
        if self.use_modern_template:
            try:
                from core.email.templates.modern_email_template import ModernEmailTemplate
                self.modern_template = ModernEmailTemplate()
                logger.info("✅ Modern email templates enabled")
            except ImportError:
                logger.warning("⚠️ Modern templates not available, falling back to legacy")
                self.use_modern_template = False
    
    
    def _generate_email_content(self, scored_stocks: pd.DataFrame) -> dict:
        """
        Generate email content (HTML and text versions).
        
        Args:
            scored_stocks: DataFrame with scored stock data
            
        Returns:
            dict: Dictionary with 'html' and 'text' keys
        """
        generation_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        if self.use_modern_template:
            try:
                html_content = self.modern_template.generate_html(scored_stocks, generation_time)
                text_content = self.modern_template.generate_text(scored_stocks, generation_time)
                logger.info("✅ Generated email using modern templates")
            except Exception as e:
                logger.warning(f"⚠️ Modern template failed, using legacy: {e}")
                html_content = self._generate_legacy_html(scored_stocks, generation_time)
                text_content = self._generate_legacy_text(scored_stocks, generation_time)
        else:
            html_content = self._generate_legacy_html(scored_stocks, generation_time)
            text_content = self._generate_legacy_text(scored_stocks, generation_time)
            logger.info("✅ Generated email using legacy templates")
        
        return {
            'html': html_content,
            'text': text_content
        }
    
    def _generate_legacy_html(self, scored_stocks: pd.DataFrame, generation_time: str) -> str:
        """
        Generate legacy HTML email content (original implementation).
        Kept for backward compatibility.
        """
        # Your existing HTML generation code here
        # This preserves your original email format
        html = f"""
        <html>
        <body>
            <h1>Daily Stock Digest - {generation_time}</h1>
             Your existing HTML template 
        </body>
        </html>
        """
        return html
    
    def _generate_legacy_text(self, scored_stocks: pd.DataFrame, generation_time: str) -> str:
        """
        Generate legacy plain text email content (original implementation).
        Kept for backward compatibility.
        """
        # Your existing text generation code here
        text = f"Daily Stock Digest - {generation_time}\n\n"
        # ... rest of your existing text generation
        return text
