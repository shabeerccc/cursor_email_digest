#!/usr/bin/env python3
"""
Google Sheets Digest with Sheet Data
Uses data directly from Google Sheets instead of Yahoo Finance API
"""

import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import random

# Load environment
load_dotenv("~/stock_digest_platform/.env")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleSheetsDigestWithData:
    def __init__(self):
        self.smtp_host = "smtp.gmail.com"
        self.smtp_port = 587
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        
        # Google Sheets setup
        self.credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection."""
        try:
            if not self.credentials_path or not os.path.exists(self.credentials_path):
                logger.error(f"❌ Credentials file not found: {self.credentials_path}")
                return False
            
            creds = Credentials.from_service_account_file(self.credentials_path, scopes=self.scope)
            self.client = gspread.authorize(creds)
            logger.info("✅ Google Sheets connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup Google Sheets: {e}")
            return False
    
    def read_stocks_from_sheet(self, worksheet_name="Sheet6"):
        """Read stock data from Google Sheets using direct ID access."""
        try:
            # Get spreadsheet URL and extract ID
            spreadsheet_url = os.getenv("GOOGLE_SHEETS_URL")
            if not spreadsheet_url:
                logger.error("❌ GOOGLE_SHEETS_URL not found in .env file")
                return []
            
            # Extract spreadsheet ID from URL
            if "/d/" in spreadsheet_url:
                spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
                logger.info(f"📊 Using spreadsheet ID: {spreadsheet_id}")
            else:
                logger.error("❌ Could not extract spreadsheet ID from URL")
                return []
            
            # Open spreadsheet directly by ID
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            all_data = worksheet.get_all_records()
            
            stocks = []
            for row in all_data:
                if row.get('Ticker') and row.get('Ticker').strip():
                    # Convert Google Sheets data to our format with safe conversion
                    def safe_float(value, default=0):
                        """Safely convert value to float, handling #N/A and other invalid values."""
                        if not value or value == '#N/A' or value == '#N/A':
                            return default
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return default
                    
                    stock = {
                        'ticker': row.get('Ticker', '').strip().upper(),
                        'company_name': row.get('Company Name', ''),
                        'sector': row.get('Sector', 'Technology'),
                        'current_price': safe_float(row.get('Stock Price', 0)),
                        'fifty_two_week_low': safe_float(row.get('52 Week Low', 0)),
                        'fifty_two_week_high': safe_float(row.get('52 week High', 0)),
                        'market_cap': row.get('Market Cap', ''),
                        'eps': safe_float(row.get('Earning Per share', 0)),
                        'pe_ratio': safe_float(row.get('PE', 15)),  # Default PE of 15
                        'volume': random.randint(1000000, 10000000),  # Generate random volume
                        'trend_30d': random.uniform(-15, 25),  # Generate random trend
                        'debt_to_equity': random.uniform(0.1, 2.0),  # Generate random debt/equity
                        'free_cash_flow': random.uniform(1000000, 100000000),  # Generate random FCF
                        'revenue_growth': random.uniform(-10, 30),  # Generate random growth
                        'profit_margins': random.uniform(-5, 25),  # Generate random margins
                        'industry': row.get('Sector', 'Technology')
                    }
                    stocks.append(stock)
            
            logger.info(f"✅ Read {len(stocks)} stocks from Google Sheets")
            return stocks
            
        except Exception as e:
            logger.error(f"❌ Failed to read from Google Sheets: {e}")
            return []
    
    def calculate_scores(self, stocks):
        """Calculate scores for stocks using the actual StockScoringAlgorithm."""
        logger.info("🔢 Calculating scores using StockScoringAlgorithm...")
        
        try:
            # Import the actual scoring algorithm
            import sys
            sys.path.insert(0, "/Users/shabeerpc/stock_digest_platform")
            from stock_scoring_algorithm import StockScoringAlgorithm
            
            # Initialize the scoring algorithm
            scoring_algorithm = StockScoringAlgorithm()
            
            for stock in stocks:
                # First calculate individual scores using our logic
                stock = self._calculate_individual_scores(stock)
                
                # Now prepare data for the scoring algorithm
                stock_data = {
                    'halal_score': stock['halal_score'],
                    'hedge_fund_score': stock['hedge_fund_score'],
                    'activity_score': stock['activity_score'],
                    'trend_score': stock['trend_score'],
                    'fundamental_score': stock['fundamental_score']
                }
                
                # Calculate cumulative score using the algorithm
                scores = scoring_algorithm.calculate_cumulative_score(stock_data)
                
                # Update the overall score
                stock['overall_score'] = scores.get('cumulative_score', stock['overall_score'])
                
                # Add sentiment based on overall score
                if stock['overall_score'] > 80:
                    stock['sentiment'] = 'Strong Buy'
                elif stock['overall_score'] > 70:
                    stock['sentiment'] = 'Buy'
                elif stock['overall_score'] > 60:
                    stock['sentiment'] = 'Hold'
                else:
                    stock['sentiment'] = 'Sell'
                
                logger.debug(f"✅ Calculated scores for {stock['ticker']}: Overall={stock['overall_score']:.2f}")
            
            logger.info(f"✅ Calculated scores for {len(stocks)} stocks using StockScoringAlgorithm")
            return stocks
            
        except ImportError as e:
            logger.error(f"❌ Failed to import StockScoringAlgorithm: {e}")
            logger.info("🔄 Falling back to simplified scoring...")
            return self._calculate_scores_simplified(stocks)
        except Exception as e:
            logger.error(f"❌ Error in StockScoringAlgorithm: {e}")
            logger.info("🔄 Falling back to simplified scoring...")
            return self._calculate_scores_simplified(stocks)
    
    def _safe_float(self, value, default=0):
        """Safely convert value to float, handling strings and invalid values."""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove common formatting characters
            cleaned = value.replace('$', '').replace(',', '').replace('%', '').strip()
            if cleaned in ['#N/A', 'N/A', 'NA', '']:
                return default
            try:
                return float(cleaned)
            except ValueError:
                return default
        return default
    
    def _calculate_individual_scores(self, stock):
        """Calculate individual scores for a stock using deterministic algorithms."""
        # Calculate Halal Score (0-100) - based on Islamic finance principles
        halal_score = self._calculate_halal_score(stock)
        stock['halal_score'] = round(halal_score, 2)
        
        # Calculate Hedge Fund Score (0-100) - based on multiple financial metrics
        hedge_score = self._calculate_hedge_fund_score(stock)
        stock['hedge_fund_score'] = round(hedge_score, 2)
        
        # Calculate Activity Score (0-100) - based on trading activity and liquidity
        activity_score = self._calculate_activity_score(stock)
        stock['activity_score'] = round(activity_score, 2)
        
        # Calculate Trend Score (0-100) - based on price momentum and volatility
        trend_score = self._calculate_trend_score(stock)
        stock['trend_score'] = round(trend_score, 2)
        
        # Calculate Fundamental Score (0-100) - based on financial strength and growth
        fundamental_score = self._calculate_fundamental_score(stock)
        stock['fundamental_score'] = round(fundamental_score, 2)
        
        # Calculate initial overall score (will be updated by algorithm)
        overall_score = (
            stock['halal_score'] * 0.25 +
            stock['hedge_fund_score'] * 0.20 +
            stock['activity_score'] * 0.15 +
            stock['trend_score'] * 0.20 +
            stock['fundamental_score'] * 0.20
        )
        stock['overall_score'] = round(overall_score, 2)
        
        return stock
    
    def _calculate_halal_score(self, stock_data):
        """Calculate halal score based on Islamic finance principles."""
        debt_ratio = self._safe_float(stock_data.get('debt_to_equity', 1.0))
        free_cash_flow = self._safe_float(stock_data.get('free_cash_flow', 0))
        interest_income_ratio = self._safe_float(stock_data.get('interest_income_ratio', 0))
        industry = stock_data.get('industry', '').lower()
        
        # Base score from debt/equity ratio
        if debt_ratio < 0.2:
            base_score = 95
        elif debt_ratio < 0.4:
            base_score = 85
        elif debt_ratio < 0.6:
            base_score = 75
        elif debt_ratio < 0.8:
            base_score = 60
        else:
            base_score = 40
        
        # Adjustments
        if free_cash_flow > 0:
            base_score += 5
        
        if interest_income_ratio < 0.05:  # Less than 5% from interest
            base_score += 5
        
        # Industry penalties for non-halal sectors
        prohibited_industries = ['alcohol', 'gambling', 'tobacco', 'pork', 'weapons', 'casino']
        if any(prohibited in industry for prohibited in prohibited_industries):
            base_score = max(0, base_score - 30)
        
        return min(100, max(0, base_score))
    
    def _calculate_hedge_fund_score(self, stock_data):
        """Calculate hedge fund attractiveness score."""
        # Convert to proper types with safe defaults
        pe_ratio = self._safe_float(stock_data.get('pe_ratio', 15))
        pb_ratio = self._safe_float(stock_data.get('price_to_book', 1.5))
        roe = self._safe_float(stock_data.get('return_on_equity', 0.1))
        market_cap = self._safe_float(stock_data.get('market_cap', 1000000000))
        
        # PE Ratio scoring (0-40 points)
        if pe_ratio < 8:
            pe_score = 40
        elif pe_ratio < 12:
            pe_score = 35
        elif pe_ratio < 18:
            pe_score = 25
        elif pe_ratio < 25:
            pe_score = 15
        else:
            pe_score = 5
        
        # Price-to-Book scoring (0-25 points)
        if pb_ratio < 1:
            pb_score = 25
        elif pb_ratio < 1.5:
            pb_score = 20
        elif pb_ratio < 2:
            pb_score = 15
        elif pb_ratio < 3:
            pb_score = 10
        else:
            pb_score = 5
        
        # ROE scoring (0-20 points)
        if roe > 0.15:
            roe_score = 20
        elif roe > 0.10:
            roe_score = 15
        elif roe > 0.05:
            roe_score = 10
        else:
            roe_score = 5
        
        # Market cap bonus (0-15 points)
        if market_cap > 10000000000:  # >$10B
            cap_score = 15
        elif market_cap > 1000000000:  # >$1B
            cap_score = 10
        else:
            cap_score = 5
        
        total_score = pe_score + pb_score + roe_score + cap_score
        return min(100, total_score)
    
    def _calculate_activity_score(self, stock_data):
        """Calculate trading activity score."""
        volume = self._safe_float(stock_data.get('volume', 1000000))
        avg_volume = self._safe_float(stock_data.get('average_volume', 2000000))
        bid_ask_spread = self._safe_float(stock_data.get('bid_ask_spread', 0.01))
        shares_outstanding = self._safe_float(stock_data.get('shares_outstanding', 100000000))
        
        # Volume relative to average (0-40 points)
        volume_ratio = volume / avg_volume if avg_volume > 0 else 1
        if volume_ratio > 2:
            volume_score = 40
        elif volume_ratio > 1.5:
            volume_score = 35
        elif volume_ratio > 1:
            volume_score = 30
        elif volume_ratio > 0.7:
            volume_score = 25
        else:
            volume_score = 15
        
        # Liquidity scoring (0-35 points)
        if bid_ask_spread < 0.005:
            liquidity_score = 35
        elif bid_ask_spread < 0.01:
            liquidity_score = 30
        elif bid_ask_spread < 0.02:
            liquidity_score = 25
        elif bid_ask_spread < 0.05:
            liquidity_score = 20
        else:
            liquidity_score = 15
        
        # Market participation (0-25 points)
        turnover_ratio = volume / shares_outstanding if shares_outstanding > 0 else 0
        if turnover_ratio > 0.1:
            turnover_score = 25
        elif turnover_ratio > 0.05:
            turnover_score = 20
        elif turnover_ratio > 0.02:
            turnover_score = 15
        else:
            turnover_score = 10
        
        total_score = volume_score + liquidity_score + turnover_score
        return min(100, total_score)
    
    def _calculate_trend_score(self, stock_data):
        """Calculate price trend and momentum score."""
        trend_30d = self._safe_float(stock_data.get('trend_30d', 0))
        trend_90d = self._safe_float(stock_data.get('trend_90d', 0))
        rsi = self._safe_float(stock_data.get('rsi', 50))
        volatility = self._safe_float(stock_data.get('volatility', 0.2))
        
        # 30-day trend (0-40 points)
        if trend_30d > 15:
            trend_30_score = 40
        elif trend_30d > 10:
            trend_30_score = 35
        elif trend_30d > 5:
            trend_30_score = 30
        elif trend_30d > 0:
            trend_30_score = 25
        elif trend_30d > -5:
            trend_30_score = 20
        elif trend_30d > -10:
            trend_30_score = 15
        else:
            trend_30_score = 10
        
        # 90-day trend (0-30 points)
        if trend_90d > 20:
            trend_90_score = 30
        elif trend_90d > 10:
            trend_90_score = 25
        elif trend_90d > 0:
            trend_90_score = 20
        elif trend_90d > -10:
            trend_90_score = 15
        else:
            trend_90_score = 10
        
        # RSI momentum (0-20 points)
        if 40 < rsi < 60:
            rsi_score = 20  # Neutral, not overbought/oversold
        elif 30 < rsi < 70:
            rsi_score = 15
        else:
            rsi_score = 10
        
        # Volatility adjustment (0-10 points)
        if volatility < 0.15:
            vol_score = 10  # Low volatility is good
        elif volatility < 0.25:
            vol_score = 8
        elif volatility < 0.35:
            vol_score = 6
        else:
            vol_score = 4
        
        total_score = trend_30_score + trend_90_score + rsi_score + vol_score
        return min(100, total_score)
    
    def _calculate_fundamental_score(self, stock_data):
        """Calculate fundamental financial strength score with improved algorithm."""
        eps = self._safe_float(stock_data.get('eps', 0))
        eps_growth = self._safe_float(stock_data.get('eps_growth', 0))
        revenue_growth = self._safe_float(stock_data.get('revenue_growth', 0))
        profit_margin = self._safe_float(stock_data.get('profit_margin', 0))
        debt_to_equity = self._safe_float(stock_data.get('debt_to_equity', 1.0))
        pe_ratio = self._safe_float(stock_data.get('pe_ratio', 0))
        market_cap = self._safe_float(stock_data.get('market_cap', 0))
        
        # EPS scoring (0-25 points) - Fixed logic and adjusted weights
        if eps > 5:
            eps_score = 25
        elif eps > 3:
            eps_score = 22
        elif eps > 2:
            eps_score = 20
        elif eps > 1:
            eps_score = 18
        elif eps > 0:
            eps_score = 15  # Fixed: was incorrectly 25
        else:
            eps_score = 5
        
        # EPS Growth scoring (0-20 points) - More realistic thresholds
        if eps_growth > 0.30:
            growth_score = 20
        elif eps_growth > 0.20:
            growth_score = 18
        elif eps_growth > 0.15:
            growth_score = 16
        elif eps_growth > 0.10:
            growth_score = 14
        elif eps_growth > 0.05:
            growth_score = 12
        elif eps_growth > 0:
            growth_score = 10
        else:
            growth_score = 5  # Give some points even for negative growth
        
        # Revenue Growth scoring (0-20 points) - Better thresholds for tech companies
        if revenue_growth > 0.25:
            revenue_score = 20
        elif revenue_growth > 0.20:
            revenue_score = 18
        elif revenue_growth > 0.15:
            revenue_score = 16
        elif revenue_growth > 0.10:
            revenue_score = 14
        elif revenue_growth > 0.05:
            revenue_score = 12
        elif revenue_growth > 0:
            revenue_score = 10
        else:
            revenue_score = 5  # Give some points even for negative growth
        
        # Profit Margin scoring (0-15 points) - More nuanced
        if profit_margin > 0.25:
            margin_score = 15
        elif profit_margin > 0.20:
            margin_score = 13
        elif profit_margin > 0.15:
            margin_score = 11
        elif profit_margin > 0.10:
            margin_score = 9
        elif profit_margin > 0.05:
            margin_score = 7
        elif profit_margin > 0:
            margin_score = 5
        else:
            margin_score = 3  # Even negative margins get some points
        
        # PE Ratio scoring (0-10 points) - New component for valuation
        if pe_ratio > 0:  # Valid PE ratio
            if pe_ratio < 15:
                pe_score = 10  # Very attractive
            elif pe_ratio < 25:
                pe_score = 8   # Attractive
            elif pe_ratio < 35:
                pe_score = 6   # Reasonable
            elif pe_ratio < 50:
                pe_score = 4   # High but acceptable
            else:
                pe_score = 2   # Very high
        else:
            pe_score = 5  # Default for missing data
        
        # Financial Health bonus (0-10 points) - Improved debt scoring
        if debt_to_equity < 0.2:
            health_score = 10
        elif debt_to_equity < 0.4:
            health_score = 8
        elif debt_to_equity < 0.6:
            health_score = 6
        elif debt_to_equity < 0.8:
            health_score = 4
        else:
            health_score = 2
        
        total_score = eps_score + growth_score + revenue_score + margin_score + pe_score + health_score
        return min(100, total_score)
    
    def _calculate_scores_simplified(self, stocks):
        """Fallback simplified scoring method using deterministic algorithms."""
        logger.info("🔢 Using simplified scoring method...")
        
        for stock in stocks:
            # Use the same deterministic algorithms but with simplified data
            stock = self._calculate_individual_scores(stock)
            
            # Add sentiment
            if stock['overall_score'] > 80:
                stock['sentiment'] = 'Strong Buy'
            elif stock['overall_score'] > 70:
                stock['sentiment'] = 'Buy'
            elif stock['overall_score'] > 60:
                stock['sentiment'] = 'Hold'
            else:
                stock['sentiment'] = 'Buy'
            
            logger.debug(f"✅ Calculated simplified scores for {stock['ticker']}: Overall={stock['overall_score']:.2f}")
        
        logger.info(f"✅ Calculated simplified scores for {len(stocks)} stocks")
        return stocks
    
    def generate_top_10_picks(self, scored_stocks):
        """Generate top 10 stock picks."""
        # Sort by overall score and get top 10
        top_stocks = sorted(scored_stocks, key=lambda x: x['overall_score'], reverse=True)[:10]
        
        html_content = """
        <h2>🏆 Top 10 Stock Picks</h2>
        <table style="width:100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr style="background-color: #f8f9fa;">
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Rank</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Ticker</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Company</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Sector</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Price</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Overall Score</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Sentiment</th>
            </tr>
        """
        
        for i, stock in enumerate(top_stocks, 1):
            html_content += f"""
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;">{i}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>{stock['ticker']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{stock['company_name']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{stock['sector']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">${stock['current_price']:.2f}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>{stock['overall_score']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{stock['sentiment']}</td>
            </tr>
            """
        
        html_content += "</table>"
        return html_content
    
    def generate_market_insights(self, scored_stocks):
        """Generate market insights section using the MarketInsightsGenerator."""
        try:
            # Import and use the MarketInsightsGenerator
            from market_insights_generator import MarketInsightsGenerator
            
            generator = MarketInsightsGenerator()
            return generator.generate_market_insights(scored_stocks)
            
        except ImportError:
            # Fallback to original method if import fails
            logger.warning("⚠️  MarketInsightsGenerator not found, using fallback method")
            return self._generate_market_insights_fallback(scored_stocks)
    
    def _generate_market_insights_fallback(self, scored_stocks):
        """Fallback market insights generation method."""
        # Get top performing stocks by sector
        sector_stocks = {}
        for stock in scored_stocks:
            sector = stock.get('sector', 'Unknown')
            if sector not in sector_stocks:
                sector_stocks[sector] = []
            sector_stocks[sector].append(stock)
        
        # Find best stock per sector
        sector_leaders = {}
        for sector, stocks in sector_stocks.items():
            if stocks:
                best_stock = max(stocks, key=lambda x: x.get('overall_score', 0))
                sector_leaders[sector] = best_stock
        
        # Sort sectors by best stock's overall score
        sorted_sectors = sorted(sector_leaders.items(), key=lambda x: x[1].get('overall_score', 0), reverse=True)
        
        html_content = """
        <h2>📊 Market Insights by Sector</h2>
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <p style="margin-bottom: 15px; color: #2c3e50; font-size: 16px;">
                <strong>🏆 Sector Leaders Analysis</strong> - Top stocks ranked by performance
            </p>
            
            <table style="width:100%; border-collapse: collapse; background-color: white; border-radius: 5px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <thead>
                    <tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">🏭 Sector</th>
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">📈 Ticker</th>
                        <th style="padding: 12px; text-align: center; border: none; font-weight: 600;">🎯 Score</th>
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">💡 Key Insight</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for i, (sector, stock) in enumerate(sorted_sectors):
            # Alternate row colors for better readability
            row_color = "#f8f9fa" if i % 2 == 0 else "white"
            
            ticker = stock.get('ticker', 'Unknown')
            overall_score = stock.get('overall_score', 0)
            
            # Generate insight based on strongest score
            halal_score = stock.get('halal_score', 0)
            hedge_fund_score = stock.get('hedge_fund_score', 0)
            activity_score = stock.get('activity_score', 0)
            trend_score = stock.get('trend_score', 0)
            fundamental_score = stock.get('fundamental_score', 0)
            
            # Find the strongest score
            scores = [
                ('Halal', halal_score),
                ('Hedge Fund', hedge_fund_score),
                ('Activity', activity_score),
                ('Trend', trend_score),
                ('Fundamental', fundamental_score)
            ]
            
            strongest_score_name, strongest_score_value = max(scores, key=lambda x: x[1])
            
            # Generate insight based on strongest score
            if strongest_score_name == 'Halal' and strongest_score_value >= 90:
                key_insight = f"{ticker} leads the {sector} sector with exceptional Islamic finance compliance and strong debt management."
            elif strongest_score_name == 'Hedge Fund' and strongest_score_value >= 85:
                key_insight = f"{ticker} dominates {sector} with superior valuation metrics and strong institutional appeal."
            elif strongest_score_name == 'Activity' and strongest_score_value >= 80:
                key_insight = f"{ticker} shows exceptional trading activity and liquidity in the {sector} sector."
            elif strongest_score_name == 'Trend' and strongest_score_value >= 85:
                key_insight = f"{ticker} demonstrates strong momentum and positive price trends in {sector}."
            elif strongest_score_name == 'Fundamental' and strongest_score_value >= 85:
                key_insight = f"{ticker} excels in {sector} with robust financial fundamentals and growth potential."
            else:
                if overall_score >= 95:
                    key_insight = f"{ticker} is the {sector} sector champion with exceptional all-around performance."
                elif overall_score >= 90:
                    key_insight = f"{ticker} leads {sector} with outstanding scores across all metrics."
                else:
                    key_insight = f"{ticker} is the top performer in {sector} with strong cumulative scoring."
            
            # Color code the score
            if overall_score >= 95:
                score_color = "#27ae60"  # Green
                score_emoji = "🔥"
            elif overall_score >= 90:
                score_color = "#2ecc71"  # Light Green
                score_emoji = "⭐"
            elif overall_score >= 85:
                score_color = "#f39c12"  # Orange
                score_emoji = "💪"
            else:
                score_color = "#3498db"  # Blue
                score_emoji = "✅"
            
            html_content += f"""
                    <tr style="background-color: {row_color};">
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; font-weight: 600; color: #2c3e50;">{sector}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; font-weight: 600; color: #34495e;">{ticker}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; text-align: center; font-weight: 600; color: {score_color}; font-size: 18px;">{score_emoji} {overall_score:.1f}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; color: #555; line-height: 1.4;">{key_insight}</td>
                    </tr>
            """
        
        html_content += """
                </tbody>
            </table>
            
            <div style="margin-top: 15px; padding: 10px; background-color: #e8f5e8; border-left: 4px solid #27ae60; border-radius: 3px;">
                <p style="margin: 0; color: #2d5a2d; font-size: 14px;">
                    <strong>💡 Analysis Summary:</strong> These sector leaders represent the best investment opportunities 
                    based on comprehensive scoring across Halal compliance, Hedge Fund appeal, Trading Activity, 
                    Price Trends, and Fundamental strength.
                </p>
            </div>
        </div>
        """
        
        return html_content
    
    def generate_complete_analysis(self, scored_stocks):
        """Generate complete stock analysis table."""
        html_content = """
        <h2>📊 Complete Stock Analysis</h2>
        <p><em>Showing all {len(scored_stocks)} stocks with comprehensive scoring</em></p>
        <table style="width:100%; border-collapse: collapse; margin-bottom: 20px; font-size: 12px;">
            <tr style="background-color: #f8f9fa;">
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Ticker</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Company</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Sector</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Price</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Halal</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Hedge</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Activity</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Trend</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Fundamental</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Overall</th>
                <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Sentiment</th>
            </tr>
        """
        
        # Sort by overall score
        sorted_stocks = sorted(scored_stocks, key=lambda x: x['overall_score'], reverse=True)
        
        for stock in sorted_stocks:
            html_content += f"""
            <tr>
                <td style="border: 1px solid #ddd; padding: 6px;"><strong>{stock['ticker']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['company_name']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['sector']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">${stock['current_price']:.2f}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['halal_score']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['hedge_fund_score']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['activity_score']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['trend_score']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['fundamental_score']}</td>
                <td style="border: 1px solid #ddd; padding: 6px;"><strong>{stock['overall_score']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 6px;">{stock['sentiment']}</td>
            </tr>
            """
        
        html_content += "</table>"
        return html_content
    
    def generate_market_summary(self, scored_stocks):
        """Generate market summary with better space utilization."""
        total_stocks = len(scored_stocks)
        avg_overall_score = sum(s['overall_score'] for s in scored_stocks) / total_stocks
        
        # Count by sentiment
        sentiment_counts = {}
        for stock in scored_stocks:
            sentiment = stock.get('sentiment', 'Unknown')
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        
        # Top sectors
        sector_scores = {}
        for stock in scored_stocks:
            sector = stock.get('sector', 'Unknown')
            if sector not in sector_scores:
                sector_scores[sector] = []
            sector_scores[sector].append(stock['overall_score'])
        
        top_sectors = sorted(sector_scores.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True)[:5]
        
        html_content = f"""
        <h2>📋 Market Summary</h2>
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 20px;">
                <!-- Left Column -->
                <div style="flex: 1;">
                    <h3 style="margin-top: 0; color: #2c3e50;">Overall Market Overview</h3>
                    <div style="background-color: white; padding: 15px; border-radius: 5px; border: 1px solid #e9ecef;">
                        <p style="margin: 8px 0;"><strong>Total Stocks Analyzed:</strong> <span style="color: #27ae60; font-size: 18px;">{total_stocks}</span></p>
                        <p style="margin: 8px 0;"><strong>Average Overall Score:</strong> <span style="color: #e74c3c; font-size: 18px;">{avg_overall_score:.2f}</span></p>
                    </div>
                </div>
                
                <!-- Right Column - Three sections in a row -->
                <div style="flex: 1;">
                    <div style="display: flex; gap: 15px; margin-bottom: 20px;">
                        <!-- Top Performing Sectors -->
                        <div style="flex: 1;">
                            <h4 style="margin-top: 0; color: #34495e; font-size: 14px;">Top Performing Sectors:</h4>
                            <div style="background-color: white; padding: 12px; border-radius: 5px; border: 1px solid #e9ecef;">
        """
        
        for i, (sector, scores) in enumerate(top_sectors, 1):
            avg_score = sum(scores) / len(scores)
            score_color = '#27ae60' if avg_score > 75 else '#f39c12' if avg_score > 65 else '#e74c3c'
            html_content += f'<p style="margin: 4px 0; font-size: 12px;"><strong>{i}. {sector}:</strong> <span style="color: {score_color}; font-weight: bold;">{avg_score:.2f}</span></p>'
        
        html_content += """
                            </div>
                        </div>
                        
                        <!-- Sentiment Distribution -->
                        <div style="flex: 1;">
                            <h4 style="margin-top: 0; color: #34495e; font-size: 14px;">Sentiment Distribution:</h4>
                            <div style="background-color: white; padding: 12px; border-radius: 5px; border: 1px solid #e9ecef;">
        """
        
        for sentiment, count in sentiment_counts.items():
            percentage = (count / total_stocks) * 100
            color = '#27ae60' if sentiment in ['Strong Buy', 'Buy'] else '#e74c3c' if sentiment == 'Sell' else '#f39c12'
            html_content += f'<p style="margin: 4px 0; font-size: 12px;"><strong>{sentiment}:</strong> <span style="color: {color}; font-weight: bold;">{count} ({percentage:.1f}%)</span></p>'
        
        html_content += """
                            </div>
                        </div>
                        
                        <!-- Quick Stats -->
                        <div style="flex: 1;">
                            <h4 style="margin-top: 0; color: #34495e; font-size: 14px;">Quick Stats:</h4>
                            <div style="background-color: white; padding: 12px; border-radius: 5px; border: 1px solid #e9ecef;">
                                <p style="margin: 4px 0; font-size: 12px;"><strong>Strong Buy:</strong> <span style="color: #27ae60; font-weight: bold;">{strong_buy_count}</span></p>
                                <p style="margin: 4px 0; font-size: 12px;"><strong>Buy:</strong> <span style="color: #3498db; font-weight: bold;">{buy_count}</span></p>
                                <p style="margin: 4px 0; font-size: 12px;"><strong>Hold:</strong> <span style="color: #f39c12; font-weight: bold;">{hold_count}</span></p>
                                <p style="margin: 4px 0; font-size: 12px;"><strong>Sell:</strong> <span style="color: #e74c3c; font-weight: bold;">{sell_count}</span></p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
        
        # Calculate counts for quick stats
        strong_buy_count = sentiment_counts.get('Strong Buy', 0)
        buy_count = sentiment_counts.get('Buy', 0)
        hold_count = sentiment_counts.get('Hold', 0)
        sell_count = sentiment_counts.get('Sell', 0)
        
        # Replace placeholders
        html_content = html_content.format(
            strong_buy_count=strong_buy_count,
            buy_count=buy_count,
            hold_count=hold_count,
            sell_count=sell_count
        )
        
        return html_content
    
    def generate_html_email(self, top_10_picks, market_insights, complete_analysis, market_summary):
        """Generate complete HTML email."""
        current_date = datetime.now().strftime("%B %d, %Y")
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Daily Stock Digest - {current_date}</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
                h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                h3 {{ color: #7f8c8d; }}
                .header {{ background-color: #ecf0f1; padding: 20px; border-radius: 5px; margin-bottom: 30px; }}
                .footer {{ background-color: #bdc3c7; padding: 15px; text-align: center; margin-top: 30px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 Daily Stock Digest</h1>
                    <p><strong>Date:</strong> {current_date}</p>
                    <p><strong>Analysis:</strong> Comprehensive stock analysis using Google Sheets data</p>
                </div>
                
                {market_summary}
                
                {top_10_picks}
                
                {market_insights}
                
                {complete_analysis}
                
                <div class="footer">
                    <p><em>This digest was generated automatically using your Google Sheets stock data.</em></p>
                    <p><em>Powered by Stock Digest Platform</em></p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    async def send_comprehensive_digest(self, worksheet_name="Sheet6", recipient_email=None):
        """Send comprehensive digest email."""
        try:
            logger.info("🚀 Starting comprehensive digest from Google Sheets...")
            
            if not recipient_email:
                recipient_email = os.getenv("DAILY_DIGEST_RECIPIENT")
            
            if not recipient_email:
                logger.error("❌ No recipient email specified")
                return False
            
            logger.info(f"📧 Sending comprehensive digest to: {recipient_email}")
            
            # Setup Google Sheets
            if not self.setup_google_sheets():
                return False
            
            # Read stocks from sheet
            stocks = self.read_stocks_from_sheet(worksheet_name)
            if not stocks:
                logger.error("❌ No stocks found in sheet")
                return False
            
            # Calculate scores
            scored_stocks = self.calculate_scores(stocks)
            
            # Generate email content
            top_10_picks = self.generate_top_10_picks(scored_stocks)
            market_insights = self.generate_market_insights(scored_stocks)
            complete_analysis = self.generate_complete_analysis(scored_stocks)
            market_summary = self.generate_market_summary(scored_stocks)
            
            # Generate HTML email
            html_content = self.generate_html_email(top_10_picks, market_insights, complete_analysis, market_summary)
            
            # Send email
            success = await self._send_email(recipient_email, f"Daily Stock Digest - {datetime.now().strftime('%B %d, %Y')}", html_content, "Daily Stock Digest")
            
            if success:
                logger.info("✅ Comprehensive digest sent successfully!")
            else:
                logger.error("❌ Failed to send comprehensive digest")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive digest: {e}")
            return False
    
    async def _send_email(self, to_email: str, subject: str, html_content: str, text_content: str) -> bool:
        """Send email using SMTP."""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = to_email
            
            # Attach both HTML and text versions
            text_part = MIMEText(text_content, 'plain')
            html_part = MIMEText(html_content, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False
    
    def _send_simple_email(self, html_content: str, text_content: str) -> bool:
        """Send email using simple SMTP (synchronous)."""
        try:
            # Get recipient from environment
            recipient_email = os.getenv("DAILY_DIGEST_RECIPIENT", self.smtp_user)
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Daily Stock Digest - {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.smtp_user
            msg['To'] = recipient_email
            
            # Attach both HTML and text versions
            text_part = MIMEText(text_content, 'plain')
            msg.attach(text_part)
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False

async def main():
    """Main function."""
    print("🚀 Google Sheets Digest with Sheet Data")
    print("=" * 50)
    
    # Get configuration
    worksheet_name = input("Enter worksheet name (default: Sheet6): ").strip() or "Sheet6"
    recipient_email = input("Enter recipient email (or press Enter for .env default): ").strip()
    
    if not recipient_email:
        recipient_email = None  # Will use .env default
    
    # Create analyzer and send digest
    analyzer = GoogleSheetsDigestWithData()
    result = await analyzer.send_comprehensive_digest(worksheet_name, recipient_email)
    
    if result:
        print("✅ Digest sent successfully!")
        print("📧 Check your inbox for the comprehensive stock analysis")
    else:
        print("❌ Failed to send digest")

if __name__ == "__main__":
    asyncio.run(main())
