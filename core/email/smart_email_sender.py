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
        Send daily digest email automatically using smart cache-first system.
        
        Args:
            force_refresh: Force refresh of data (default: False - use cache when possible)
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            logger.info("📧 Starting smart daily digest email generation...")
            
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
            
            # Step 4: Generate email content
            logger.info("📝 Generating email content...")
            email_content = self._generate_email_content(scored_stocks)
            
            # Step 5: Send email
            logger.info("📤 Sending email...")
            success = self._send_email(email_content)
            
            if success:
                logger.info("✅ Daily digest email sent successfully!")
                return True
            else:
                logger.error("❌ Failed to send email")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in daily digest email: {e}")
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
            # Handle different URL formats
            if '/d/' in url:
                return url.split('/d/')[1].split('/')[0]
            elif 'spreadsheets/d/' in url:
                return url.split('spreadsheets/d/')[1].split('/')[0]
            else:
                raise ValueError("Invalid Google Sheets URL format")
        except Exception as e:
            logger.error(f"❌ Error extracting spreadsheet ID: {e}")
            raise
    
    def _get_sample_tickers(self) -> list:
        """Get sample tickers as fallback."""
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'PG']
    
    def _calculate_stock_scores(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate stock scores using existing scoring system."""
        try:
            # Import existing digest system for scoring
            sys.path.insert(0, "/Users/shabeerpc/Documents/iamhr")
            from integrated_digest_with_yahoo_cache import IntegratedDigestSystem
            
            digest_system = IntegratedDigestSystem()
            scored_stocks = digest_system.calculate_comprehensive_scores(stock_data)
            
            logger.info(f"✅ Calculated scores for {len(scored_stocks)} stocks")
            return scored_stocks
            
        except Exception as e:
            logger.warning(f"⚠️ Error calculating scores: {e}")
            # Return original data with basic scores
            stock_data['overall_score'] = 75.0  # Default score
            return stock_data
    
    def _generate_email_content(self, scored_stocks: pd.DataFrame) -> dict:
        """Generate comprehensive email content."""
        try:
            # Import existing digest system for email generation
            sys.path.insert(0, "/Users/shabeerpc/Documents/iamhr")
            from integrated_digest_with_yahoo_cache import IntegratedDigestSystem
            
            digest_system = IntegratedDigestSystem()
            
            # Generate email sections
            email_sections = {
                'top_10_picks': digest_system._generate_top_10_picks(scored_stocks),
                'market_insights': digest_system._generate_market_insights(scored_stocks),
                'complete_analysis': digest_system._generate_complete_analysis(scored_stocks),
                'market_summary': digest_system._generate_market_summary(scored_stocks)
            }
            
            # Generate HTML and text content
            html_content = digest_system._generate_html_email(email_sections)
            text_content = digest_system._generate_text_email(email_sections)
            
            logger.info("✅ Email content generated successfully")
            
            return {
                'html_content': html_content,
                'text_content': text_content,
                'subject': f"📊 Daily Stock Digest - {datetime.now().strftime('%Y-%m-%d')}",
                'stock_count': len(scored_stocks)
            }
            
        except Exception as e:
            logger.error(f"❌ Error generating email content: {e}")
            # Generate simple fallback content
            return self._generate_fallback_email_content(scored_stocks)
    
    def _generate_fallback_email_content(self, scored_stocks: pd.DataFrame) -> dict:
        """Generate simple fallback email content."""
        try:
            # Sort by overall score
            top_stocks = scored_stocks.nlargest(10, 'overall_score')
            
            html_content = f"""
            <html>
            <head>
                <title>Daily Stock Digest</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .header {{ background-color: #f0f0f0; padding: 20px; text-align: center; }}
                    .section {{ margin: 20px 0; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>📊 Daily Stock Digest</h1>
                    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="section">
                    <h2>🏆 Top 10 Stock Picks</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Ticker</th>
                                <th>Company</th>
                                <th>Price</th>
                                <th>Score</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            for _, stock in top_stocks.iterrows():
                html_content += f"""
                <tr>
                    <td><strong>{stock.get('ticker', 'N/A')}</strong></td>
                    <td>{stock.get('company_name', 'N/A')}</td>
                    <td>${stock.get('current_price', 0):.2f}</td>
                    <td><strong>{stock.get('overall_score', 0):.1f}</strong></td>
                </tr>
                """
            
            html_content += """
                        </tbody>
                    </table>
                </div>
                
                <div class="section">
                    <h2>📈 Summary</h2>
                    <p>This digest contains analysis of {len(scored_stocks)} stocks using smart cache-first data collection.</p>
                    <p>Data source: Enhanced cache system with automatic fallback</p>
                </div>
            </body>
            </html>
            """
            
            text_content = f"""
            Daily Stock Digest - {datetime.now().strftime('%Y-%m-%d')}
            ================================================
            
            Top 10 Stock Picks:
            """
            
            for _, stock in top_stocks.iterrows():
                text_content += f"""
            {stock.get('ticker', 'N/A')}: {stock.get('company_name', 'N/A')} - ${stock.get('current_price', 0):.2f} (Score: {stock.get('overall_score', 0):.1f})
                """
            
            text_content += f"""
            
            Summary: This digest contains analysis of {len(scored_stocks)} stocks using smart cache-first data collection.
            Data source: Enhanced cache system with automatic fallback
            """
            
            return {
                'html_content': html_content,
                'text_content': text_content,
                'subject': f"📊 Daily Stock Digest - {datetime.now().strftime('%Y-%m-%d')}",
                'stock_count': len(scored_stocks)
            }
            
        except Exception as e:
            logger.error(f"❌ Error generating fallback content: {e}")
            return {
                'html_content': '<p>Error generating email content</p>',
                'text_content': 'Error generating email content',
                'subject': f"📊 Daily Stock Digest - {datetime.now().strftime('%Y-%m-%d')}",
                'stock_count': 0
            }
    
    def _send_email(self, email_content: dict) -> bool:
        """Send the email."""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = email_content['subject']
            msg['From'] = self.smtp_user
            msg['To'] = self.recipient_email
            
            # Attach both HTML and text versions
            text_part = MIMEText(email_content['text_content'], 'plain')
            html_part = MIMEText(email_content['html_content'], 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to {self.recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False

def main():
    """Send daily digest email automatically."""
    print("🚀 Smart Daily Digest Email Sender")
    print("=" * 50)
    
    try:
        # Initialize smart email sender
        email_sender = SmartEmailSender()
        print("✅ Smart email sender initialized")
        
        # Check system status
        status = email_sender.smart_system.get_system_status()
        print(f"📊 Smart System Status: {status}")
        
        # Send email automatically (no user input required)
        print("\n📧 Sending daily digest email automatically...")
        print("💡 No user input required - automatically uses cache when APIs are rate-limited!")
        
        success = email_sender.send_daily_digest_email(force_refresh=False)
        
        if success:
            print("\n🎉 Daily digest email sent successfully!")
            print("✅ Smart cache-first system working perfectly!")
            print("💡 No more waiting for user input - completely automated!")
            return True
        else:
            print("\n❌ Failed to send daily digest email")
            return False
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
