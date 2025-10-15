"""
Enhanced Email Sender with Beautiful Templates
Uses the new modern email templates for professional-looking digests
"""

import os
import sys
import logging
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd

from core.email.templates.email_template import EmailTemplate

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedEmailSender:
    """
    Enhanced email sender with beautiful HTML templates.
    Drop-in replacement for SmartEmailSender with improved styling.
    """
    
    def __init__(self):
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
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
    
    def _load_environment(self):
        """Load environment variables for email configuration."""
        try:
            from dotenv import load_dotenv
            env_path = os.path.expanduser("~/stock_digest_platform/.env")
            load_dotenv(env_path)
            
            # Email configuration
            self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
            self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
            self.smtp_user = os.getenv("SMTP_USER")
            self.smtp_password = os.getenv("SMTP_PASSWORD")
            self.recipient_email = os.getenv("RECIPIENT_EMAIL")
            
            if not all([self.smtp_user, self.smtp_password, self.recipient_email]):
                raise ValueError("Missing email configuration in environment variables")
            
            logger.info("✅ Email configuration loaded")
            
        except Exception as e:
            logger.error(f"❌ Error loading environment: {e}")
            raise
    
    def send_daily_digest_email(self, force_refresh: bool = False) -> bool:
        """
        Send daily digest email with beautiful HTML template.
        
        Args:
            force_refresh: Force refresh of data (default: False - use cache when possible)
        
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            logger.info("📧 Starting enhanced daily digest email generation...")
            
            # Step 1: Get tickers from Google Sheets
            tickers = self._get_tickers_from_sheets()
            if not tickers:
                logger.warning("⚠️ No tickers found, using sample tickers")
                tickers = self._get_sample_tickers()
            
            logger.info(f"✅ Found {len(tickers)} tickers")
            
            # Step 2: Get stock data using smart cache-first system
            logger.info("🧠 Getting stock data using smart cache-first system...")
            stock_data = self.smart_system.get_stock_data_smart(tickers, force_refresh)
            
            if stock_data.empty:
                logger.error("❌ No stock data retrieved")
                return False
            
            logger.info(f"✅ Stock data retrieved: {len(stock_data)} stocks")
            
            # Step 3: Calculate scores
            logger.info("📊 Calculating stock scores...")
            scored_stocks = self._calculate_stock_scores(stock_data)
            
            # Step 4: Generate beautiful email content
            logger.info("🎨 Generating beautiful email content...")
            metadata = {
                'generation_time': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
                'total_stocks': len(scored_stocks)
            }
            
            html_content = EmailTemplate.generate_daily_digest_html(scored_stocks, metadata)
            text_content = EmailTemplate.generate_plain_text(scored_stocks, metadata)
            
            # Step 5: Send email
            logger.info("📤 Sending email...")
            success = self._send_email(html_content, text_content)
            
            if success:
                logger.info("✅ Enhanced daily digest email sent successfully!")
                return True
            else:
                logger.error("❌ Failed to send email")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error in daily digest email: {e}")
            return False
    
    def _send_email(self, html_content: str, text_content: str) -> bool:
        """
        Send email with both HTML and plain text versions.
        
        Args:
            html_content: HTML version of email
            text_content: Plain text version of email
        
        Returns:
            bool: True if sent successfully
        """
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"📈 Daily Stock Digest - {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.smtp_user
            msg['To'] = self.recipient_email
            
            # Attach both plain text and HTML versions
            part1 = MIMEText(text_content, 'plain')
            part2 = MIMEText(html_content, 'html')
            
            msg.attach(part1)
            msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info("✅ Email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False
    
    def _get_tickers_from_sheets(self) -> list:
        """Get stock tickers from Google Sheets."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
            sheets_url = os.getenv("GOOGLE_SHEETS_URL")
            
            if not credentials_path or not os.path.exists(credentials_path):
                logger.warning("⚠️ Google Sheets credentials not found")
                return []
            
            credentials = Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            
            client = gspread.authorize(credentials)
            
            # Extract spreadsheet ID from URL
            spreadsheet_id = self._extract_spreadsheet_id(sheets_url)
            spreadsheet = client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet("Sheet6")
            
            # Get all values from first column (tickers)
            all_values = worksheet.get_all_values()
            tickers = []
            
            for row in all_values:
                if row and row[0].strip():
                    ticker = row[0].strip().upper()
                    if len(ticker) <= 5:  # Basic validation
                        tickers.append(ticker)
            
            logger.info(f"✅ Retrieved {len(tickers)} tickers from Google Sheets")
            return tickers
            
        except Exception as e:
            logger.warning(f"⚠️ Error reading from Google Sheets: {e}")
            return []
    
    def _extract_spreadsheet_id(self, url: str) -> str:
        """Extract spreadsheet ID from Google Sheets URL."""
        try:
            if '/d/' in url:
                return url.split('/d/')[1].split('/')[0]
            elif 'spreadsheets/d/' in url:
                return url.split('spreadsheets/d/')[1].split('/')[0]
            else:
                return url
        except Exception as e:
            logger.error(f"❌ Error extracting spreadsheet ID: {e}")
            raise
    
    def _get_sample_tickers(self) -> list:
        """Get sample tickers for testing."""
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
    
    def _calculate_stock_scores(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive scores for stocks.
        This is a placeholder - integrate with your existing scoring system.
        """
        # Add mock scores if not present (replace with your actual scoring logic)
        if 'total_score' not in stock_data.columns:
            import numpy as np
            stock_data['total_score'] = np.random.randint(40, 95, size=len(stock_data))
        
        return stock_data


if __name__ == "__main__":
    # Test the enhanced email sender
    sender = EnhancedEmailSender()
    success = sender.send_daily_digest_email(force_refresh=False)
    
    if success:
        print("✅ Test email sent successfully!")
    else:
        print("❌ Failed to send test email")
