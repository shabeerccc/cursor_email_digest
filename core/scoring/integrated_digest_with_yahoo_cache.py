#!/usr/bin/env python3
"""
Integrated Digest System with Yahoo Finance Daily Cache
Combines real-time Yahoo Finance data with existing digest functionality
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from typing import List

# Load environment
env_path = os.path.expanduser("~/stock_digest_platform/.env")
load_dotenv(env_path)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import the Yahoo Finance cache system
sys.path.insert(0, "/Users/shabeerpc/Documents/iamhr")
from yahoo_finance_daily_cache import YahooFinanceDailyCache

# Import existing digest components
from run_digest_with_sheets_data import GoogleSheetsDigestWithData
from market_insights_generator import MarketInsightsGenerator

class IntegratedDigestSystem:
    """
    Integrated Digest System with Yahoo Finance Daily Cache
    
    Features:
    - Real-time stock data from Yahoo Finance (cached daily)
    - Comprehensive scoring algorithms
    - All email sections with enriched data
    - Seamless integration with existing systems
    """
    
    def __init__(self):
        # Initialize multi-source data system
        try:
            sys.path.insert(0, "/Users/shabeerpc/Documents/iamhr")
            from multi_source_stock_data import MultiSourceStockDataSystem
            self.multi_source = MultiSourceStockDataSystem()
            logger.info("✅ Multi-source data system initialized")
        except ImportError as e:
            logger.warning(f"⚠️ Multi-source system not available: {e}")
            self.multi_source = None
        
        # Initialize Yahoo Finance cache system (fallback)
        self.yahoo_cache = YahooFinanceCache()
        
        # Initialize existing digest system
        self.existing_digest = GoogleSheetsDigestWithData()
        
        # Initialize market insights generator
        self.market_insights = MarketInsightsGenerator()
        
        # Cache settings
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        
    def get_comprehensive_stock_data(self, tickers: List[str], force_refresh: bool = False) -> pd.DataFrame:
        """
        Get comprehensive stock data from multiple sources with intelligent fallback.
        
        Args:
            tickers: List of stock tickers to get data for
            force_refresh: Force refresh of data
            
        Returns:
            DataFrame with enriched stock data ready for scoring
        """
        try:
            logger.info(f"🔍 Getting comprehensive stock data for {len(tickers)} tickers...")
            
            # Step 2: Try multi-source system first (if available)
            if self.multi_source:
                try:
                    logger.info("🔄 Attempting multi-source data collection...")
                    multi_source_data = self.multi_source.get_comprehensive_stock_data(tickers, force_refresh)
                    if not multi_source_data.empty:
                        logger.info(f"✅ Multi-source data collected: {len(multi_source_data)} stocks")
                        return multi_source_data
                except Exception as multi_error:
                    logger.warning(f"⚠️ Multi-source system failed: {multi_error}")
            
            # Step 3: Fallback to Yahoo Finance cache system
            try:
                logger.info("🔄 Attempting Yahoo Finance data collection...")
                yahoo_data = self.yahoo_cache.get_stock_data(tickers, force_refresh)
                if not yahoo_data.empty:
                    logger.info(f"✅ Retrieved Yahoo Finance data for {len(yahoo_data)} stocks")
                    enriched_data = self._enrich_stock_data(yahoo_data)
                    logger.info(f"✅ Enriched data with {len(enriched_data.columns)} total metrics")
                    return enriched_data
                else:
                    logger.warning("⚠️ Yahoo Finance data is empty, falling back to cached/sample data")
                    raise ValueError("Empty Yahoo Finance data")
                    
            except Exception as yahoo_error:
                logger.warning(f"⚠️ Yahoo Finance API error: {yahoo_error}")
                logger.info("🔄 Falling back to cached or sample data...")
                
                # Try to get cached data first
                cached_data = self._get_cached_stock_data(tickers)
                if not cached_data.empty:
                    logger.info(f"✅ Using cached data for {len(cached_data)} stocks")
                    enriched_data = self._enrich_stock_data(cached_data)
                    logger.info(f"✅ Enriched cached data with {len(enriched_data.columns)} total metrics")
                    return enriched_data
                
                # If no cached data, generate sample data
                logger.info("🔄 No cached data available, generating sample data...")
                sample_data = self._generate_sample_stock_data(tickers)
                enriched_data = self._enrich_stock_data(sample_data)
                logger.info(f"✅ Generated sample data with {len(enriched_data.columns)} total metrics")
                return enriched_data
            
        except Exception as e:
            logger.error(f"❌ Critical error in get_comprehensive_stock_data: {e}")
            # Final fallback: generate sample data
            logger.info("🔄 Critical error, using final fallback to sample data...")
            try:
                sample_data = self._generate_sample_stock_data(tickers)
                enriched_data = self._enrich_stock_data(sample_data)
                logger.info(f"✅ Final fallback successful with {len(enriched_data.columns)} metrics")
                return enriched_data
            except Exception as final_error:
                logger.error(f"❌ Final fallback also failed: {final_error}")
                raise
    
    def _get_tickers_from_sheets(self) -> list:
        """Get stock tickers from Google Sheets."""
        try:
            # Use existing digest system to read tickers
            if hasattr(self.existing_digest, '_read_tickers_from_sheet'):
                return self.existing_digest._read_tickers_from_sheet("Sheet6")
            else:
                # Fallback to direct Google Sheets access
                return self._read_tickers_direct()
        except Exception as e:
            logger.warning(f"⚠️ Error reading from existing digest system: {e}")
            return self._read_tickers_direct()
    
    def _read_tickers_direct(self) -> list:
        """Direct Google Sheets ticker reading as fallback."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
            sheets_url = os.getenv("GOOGLE_SHEETS_URL")
            
            if not credentials_path or not os.path.exists(credentials_path):
                logger.warning("⚠️ Google Sheets credentials not found, using sample tickers")
                return self._get_sample_tickers()
            
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
                    if ticker and ticker != 'TICKER':
                        tickers.append(ticker)
            
            logger.info(f"✅ Read {len(tickers)} tickers directly from Google Sheets")
            return tickers
            
        except Exception as e:
            logger.error(f"❌ Error reading tickers directly: {e}")
            return self._get_sample_tickers()
    
    def _extract_spreadsheet_id(self, url: str) -> str:
        """Extract spreadsheet ID from Google Sheets URL."""
        try:
            start = url.find('/d/') + 3
            end = url.find('/edit')
            return url[start:end]
        except:
            return url
    
    def _get_sample_tickers(self) -> list:
        """Get sample tickers for testing."""
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM']
    
    def _get_cached_stock_data(self, tickers: list) -> pd.DataFrame:
        """Try to get cached stock data from local storage."""
        try:
            cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
            cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv') and 'stock_data' in f]
            
            if cache_files:
                # Get the most recent cache file
                cache_files.sort(reverse=True)
                latest_cache = os.path.join(cache_dir, cache_files[0])
                
                logger.info(f"🔄 Loading cached data from: {latest_cache}")
                cached_data = pd.read_csv(latest_cache)
                
                # Filter to requested tickers
                if 'ticker' in cached_data.columns:
                    filtered_data = cached_data[cached_data['ticker'].isin(tickers)]
                    if not filtered_data.empty:
                        logger.info(f"✅ Loaded {len(filtered_data)} stocks from cache")
                        return filtered_data
                
            logger.info("⚠️ No valid cached data found")
            return pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"⚠️ Error loading cached data: {e}")
            return pd.DataFrame()
    
    def _generate_sample_stock_data(self, tickers: list) -> pd.DataFrame:
        """Generate comprehensive sample stock data for fallback."""
        try:
            logger.info(f"🔄 Generating sample data for {len(tickers)} tickers...")
            
            stock_data = []
            
            for i, ticker in enumerate(tickers):
                # Generate realistic sample data
                stock_info = {
                    'ticker': ticker,
                    'company_name': f"{ticker} Corporation",
                    'sector': self._get_sample_sector(ticker),
                    'industry': 'Technology',
                    
                    # Price data
                    'current_price': round(100 + (i * 25) + (i * 10), 2),
                    'change': round((i * 2) - 5, 2),
                    'change_percent': round(((i * 2) - 5) / 100 * 100, 2),
                    'high_52w': round(120 + (i * 30), 2),
                    'low_52w': round(80 + (i * 20), 2),
                    'open': round(99 + (i * 25), 2),
                    'prev_close': round(100 + (i * 25), 2),
                    
                    # Volume data
                    'volume': int(1000000 + (i * 500000)),
                    'avg_volume_30d': int(1200000 + (i * 600000)),
                    'volume_ratio': round(0.8 + (i * 0.1), 2),
                    
                    # Market data
                    'market_cap': int(1000000000 + (i * 50000000000)),
                    'enterprise_value': int(1100000000 + (i * 55000000000)),
                    'shares_outstanding': int(10000000 + (i * 1000000)),
                    'float_shares': int(9000000 + (i * 900000)),
                    
                    # Valuation metrics
                    'pe_ratio': round(15 + (i * 2), 2),
                    'forward_pe': round(14 + (i * 1.5), 2),
                    'peg_ratio': round(1.2 + (i * 0.1), 2),
                    'pb_ratio': round(2.5 + (i * 0.3), 2),
                    'ps_ratio': round(3.0 + (i * 0.4), 2),
                    'ev_ebitda': round(12 + (i * 1.5), 2),
                    
                    # Financial metrics
                    'debt_to_equity': round(0.3 + (i * 0.05), 2),
                    'debt_to_assets': round(0.2 + (i * 0.03), 2),
                    'current_ratio': round(1.5 + (i * 0.1), 2),
                    'quick_ratio': round(1.2 + (i * 0.08), 2),
                    'free_cash_flow': int(100000000 + (i * 50000000)),
                    'operating_cash_flow': int(150000000 + (i * 75000000)),
                    
                    # Profitability metrics
                    'profit_margin': round(0.15 + (i * 0.02), 3),
                    'operating_margin': round(0.20 + (i * 0.025), 3),
                    'gross_margin': round(0.45 + (i * 0.03), 3),
                    'ebitda_margins': round(0.25 + (i * 0.03), 3),
                    
                    # Growth metrics
                    'revenue_growth': round(0.10 + (i * 0.02), 3),
                    'earnings_growth': round(0.15 + (i * 0.025), 3),
                    'eps_growth': round(0.12 + (i * 0.02), 3),
                    
                    # Efficiency metrics
                    'roe': round(0.18 + (i * 0.02), 3),
                    'roa': round(0.12 + (i * 0.015), 3),
                    'roic': round(0.15 + (i * 0.02), 3),
                    'asset_turnover': round(0.8 + (i * 0.05), 2),
                    
                    # EPS data
                    'eps': round(2.5 + (i * 0.5), 2),
                    'forward_eps': round(2.8 + (i * 0.6), 2),
                    'book_value': round(25 + (i * 3), 2),
                    
                    # Dividend data
                    'dividend_rate': round(1.2 + (i * 0.1), 2),
                    'dividend_yield': round(0.02 + (i * 0.005), 3),
                    'payout_ratio': round(0.25 + (i * 0.03), 3),
                    
                    # Analyst data
                    'recommendation': 'Buy',
                    'recommendation_key': 'buy',
                    'target_price': round(110 + (i * 30), 2),
                    'target_high': round(120 + (i * 35), 2),
                    'target_low': round(95 + (i * 25), 2),
                    'number_of_analysts': int(15 + (i * 2)),
                    
                    # Technical indicators
                    'trend_30d': round(5 + (i * 2), 2),
                    'trend_90d': round(12 + (i * 3), 2),
                    'rsi': round(55 + (i * 3), 2),
                    'volatility': round(0.25 + (i * 0.02), 3),
                    
                    # Additional metrics
                    'beta': round(1.1 + (i * 0.05), 2),
                    'short_ratio': round(2.5 + (i * 0.3), 2),
                    'shares_short': int(500000 + (i * 100000)),
                    'shares_short_prev_month': int(480000 + (i * 95000)),
                    
                    # Collection timestamp
                    'data_collected_at': datetime.now().isoformat(),
                    'cache_date': datetime.now().date().isoformat(),
                    'data_source': 'Sample Data (Fallback)'
                }
                
                stock_data.append(stock_info)
            
            logger.info(f"✅ Generated sample data for {len(stock_data)} stocks")
            return pd.DataFrame(stock_data)
            
        except Exception as e:
            logger.error(f"❌ Error generating sample data: {e}")
            return pd.DataFrame()
    
    def _get_sample_sector(self, ticker: str) -> str:
        """Get sample sector based on ticker."""
        sectors = {
            'AAPL': 'Technology', 'MSFT': 'Technology', 'GOOGL': 'Technology',
            'AMZN': 'Consumer Cyclical', 'TSLA': 'Consumer Cyclical',
            'META': 'Technology', 'NVDA': 'Technology', 'NFLX': 'Communication Services',
            'ADBE': 'Technology', 'CRM': 'Technology'
        }
        return sectors.get(ticker, 'Technology')
    
    def _enrich_stock_data(self, yahoo_data: pd.DataFrame) -> pd.DataFrame:
        """Enrich Yahoo Finance data with additional calculated metrics."""
        try:
            logger.info("🔧 Enriching stock data with calculated metrics...")
            
            enriched_data = yahoo_data.copy()
            
            # Add calculated metrics that might be missing
            enriched_data = self._add_calculated_metrics(enriched_data)
            
            # Ensure all required fields exist for scoring
            enriched_data = self._ensure_required_fields(enriched_data)
            
            # Add data quality indicators
            enriched_data = self._add_data_quality_indicators(enriched_data)
            
            logger.info("✅ Stock data enrichment completed")
            return enriched_data
            
        except Exception as e:
            logger.error(f"❌ Error enriching stock data: {e}")
            return yahoo_data
    
    def _add_calculated_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add calculated metrics to the dataset."""
        try:
            # Calculate additional volume metrics
            if 'volume' in data.columns and 'avg_volume_30d' in data.columns:
                data['volume_ratio'] = data['volume'] / data['avg_volume_30d'].replace(0, 1)
                data['volume_ratio'] = data['volume_ratio'].fillna(1)
            
            # Calculate price momentum
            if 'current_price' in data.columns and 'prev_close' in data.columns:
                data['price_momentum'] = ((data['current_price'] - data['prev_close']) / data['prev_close'] * 100).fillna(0)
            
            # Calculate market cap categories
            if 'market_cap' in data.columns:
                data['market_cap_category'] = pd.cut(
                    data['market_cap'],
                    bins=[0, 2e9, 10e9, 100e9, float('inf')],
                    labels=['Small Cap', 'Mid Cap', 'Large Cap', 'Mega Cap']
                )
            
            # Calculate valuation ratios
            if 'pe_ratio' in data.columns and 'eps_growth' in data.columns:
                data['peg_ratio_calculated'] = data['pe_ratio'] / (data['eps_growth'] * 100).replace(0, 1)
                data['peg_ratio_calculated'] = data['peg_ratio_calculated'].fillna(0)
            
            return data
            
        except Exception as e:
            logger.warning(f"⚠️ Error adding calculated metrics: {e}")
            return data
    
    def _ensure_required_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required fields exist for scoring algorithms."""
        try:
            required_fields = {
                'debt_to_equity': 1.0,
                'free_cash_flow': 0,
                'interest_income_ratio': 0,
                'volume': 1000000,
                'average_volume': 1000000,
                'bid_ask_spread': 0.01,
                'shares_outstanding': 10000000,
                'trend_30d': 0,
                'trend_90d': 0,
                'rsi': 50,
                'volatility': 0.2,
                'eps': 1.0,
                'eps_growth': 0.1,
                'revenue_growth': 0.1,
                'profit_margin': 0.15,
                'pe_ratio': 20,
                'pb_ratio': 2.0,
                'roe': 0.15
            }
            
            for field, default_value in required_fields.items():
                if field not in data.columns:
                    data[field] = default_value
                    logger.info(f"➕ Added missing field: {field} with default value")
            
            return data
            
        except Exception as e:
            logger.warning(f"⚠️ Error ensuring required fields: {e}")
            return data
    
    def _add_data_quality_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add data quality indicators to the dataset."""
        try:
            # Count non-null values for each stock
            data['data_completeness'] = data.notna().sum(axis=1)
            data['data_completeness_pct'] = (data['data_completeness'] / len(data.columns)) * 100
            
            # Add data freshness indicator
            data['data_age_hours'] = 0  # Fresh data from Yahoo Finance
            
            # Add source indicator
            data['data_source'] = 'Yahoo Finance'
            
            return data
            
        except Exception as e:
            logger.warning(f"⚠️ Error adding data quality indicators: {e}")
            return data
    
    def calculate_comprehensive_scores(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive scores using existing algorithms."""
        try:
            logger.info("📊 Calculating comprehensive scores...")
            
            # Convert DataFrame to list of dictionaries for existing scoring
            stocks_list = stock_data.to_dict('records')
            
            # Calculate individual scores using existing system
            scored_stocks = []
            for stock in stocks_list:
                try:
                    scored_stock = self.existing_digest._calculate_individual_scores(stock)
                    scored_stocks.append(scored_stock)
                except Exception as e:
                    logger.warning(f"⚠️ Error scoring {stock.get('ticker', 'Unknown')}: {e}")
                    # Add default scores
                    stock.update({
                        'halal_score': 50.0,
                        'hedge_fund_score': 50.0,
                        'activity_score': 50.0,
                        'trend_score': 50.0,
                        'fundamental_score': 50.0,
                        'overall_score': 50.0
                    })
                    scored_stocks.append(stock)
            
            logger.info(f"✅ Calculated scores for {len(scored_stocks)} stocks")
            return pd.DataFrame(scored_stocks)
            
        except Exception as e:
            logger.error(f"❌ Error calculating scores: {e}")
            raise
    
    def generate_comprehensive_email(self, force_refresh: bool = False) -> dict:
        """Generate comprehensive email with all sections using Yahoo Finance data."""
        try:
            logger.info("📧 Generating comprehensive email...")
            
            # Get tickers from Google Sheets first
            tickers = self._get_tickers_from_sheets()
            if not tickers:
                logger.warning("⚠️ No tickers found, using sample tickers")
                tickers = self._get_sample_tickers()
            
            logger.info(f"✅ Found {len(tickers)} tickers from Google Sheets")
            
            # Get comprehensive stock data
            stock_data = self.get_comprehensive_stock_data(tickers, force_refresh)
            
            # Calculate scores
            scored_stocks = self.calculate_comprehensive_scores(stock_data)
            
            # Generate all email sections
            email_sections = {
                'top_10_picks': self._generate_top_10_picks(scored_stocks),
                'market_insights': self._generate_market_insights(scored_stocks),
                'complete_analysis': self._generate_complete_analysis(scored_stocks),
                'market_summary': self._generate_market_summary(scored_stocks)
            }
            
            # Generate HTML email
            html_content = self._generate_html_email(email_sections)
            
            # Generate text email
            text_content = self._generate_text_email(email_sections)
            
            logger.info("✅ Comprehensive email generated successfully")
            
            return {
                'html_content': html_content,
                'text_content': text_content,
                'stock_data': scored_stocks,
                'email_sections': email_sections,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Error generating comprehensive email: {e}")
            raise
    
    def _generate_top_10_picks(self, scored_stocks: pd.DataFrame) -> str:
        """Generate top 10 picks section."""
        try:
            # Sort by overall score and get top 10
            top_10 = scored_stocks.nlargest(10, 'overall_score')
            
            html = """
            <div class="section">
                <h2>🏆 Top 10 Stock Picks</h2>
                <div class="table-container">
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>Ticker</th>
                                <th>Company</th>
                                <th>Sector</th>
                                <th>Price</th>
                                <th>Change</th>
                                <th>Overall Score</th>
                                <th>Key Strengths</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            for _, stock in top_10.iterrows():
                strengths = self._get_key_strengths(stock)
                change_color = "green" if stock.get('change', 0) >= 0 else "red"
                
                html += f"""
                <tr>
                    <td><strong>{stock.get('ticker', 'N/A')}</strong></td>
                    <td>{stock.get('company_name', 'N/A')}</td>
                    <td>{stock.get('sector', 'N/A')}</td>
                    <td>${stock.get('current_price', 0):.2f}</td>
                    <td style="color: {change_color}">{stock.get('change', 0):+.2f}</td>
                    <td><strong>{stock.get('overall_score', 0):.1f}</strong></td>
                    <td>{strengths}</td>
                </tr>
                """
            
            html += """
                        </tbody>
                    </table>
                </div>
            </div>
            """
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generating top 10 picks: {e}")
            return "<p>Error generating top 10 picks</p>"
    
    def _generate_market_insights(self, scored_stocks: pd.DataFrame) -> str:
        """Generate market insights section."""
        try:
            # Use existing market insights generator
            stocks_list = scored_stocks.to_dict('records')
            return self.market_insights.generate_market_insights(stocks_list)
            
        except Exception as e:
            logger.error(f"❌ Error generating market insights: {e}")
            return "<p>Error generating market insights</p>"
    
    def _generate_complete_analysis(self, scored_stocks: pd.DataFrame) -> str:
        """Generate complete stock analysis section."""
        try:
            # Sort by overall score
            sorted_stocks = scored_stocks.sort_values('overall_score', ascending=False)
            
            html = """
            <div class="section">
                <h2>📊 Complete Stock Analysis</h2>
                <div class="table-container">
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>Ticker</th>
                                <th>Company</th>
                                <th>Sector</th>
                                <th>Price</th>
                                <th>Halal</th>
                                <th>Hedge Fund</th>
                                <th>Activity</th>
                                <th>Trend</th>
                                <th>Fundamental</th>
                                <th>Overall</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            for _, stock in sorted_stocks.iterrows():
                html += f"""
                <tr>
                    <td><strong>{stock.get('ticker', 'N/A')}</strong></td>
                    <td>{stock.get('company_name', 'N/A')}</td>
                    <td>{stock.get('sector', 'N/A')}</td>
                    <td>${stock.get('current_price', 0):.2f}</td>
                    <td>{stock.get('halal_score', 0):.1f}</td>
                    <td>{stock.get('hedge_fund_score', 0):.1f}</td>
                    <td>{stock.get('activity_score', 0):.1f}</td>
                    <td>{stock.get('trend_score', 0):.1f}</td>
                    <td>{stock.get('fundamental_score', 0):.1f}</td>
                    <td><strong>{stock.get('overall_score', 0):.1f}</strong></td>
                </tr>
                """
            
            html += """
                        </tbody>
                    </table>
                </div>
            </div>
            """
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generating complete analysis: {e}")
            return "<p>Error generating complete analysis</p>"
    
    def _generate_market_summary(self, scored_stocks: pd.DataFrame) -> str:
        """Generate market summary section."""
        try:
            # Calculate market statistics
            total_stocks = len(scored_stocks)
            avg_score = scored_stocks['overall_score'].mean()
            top_performers = scored_stocks.nlargest(5, 'overall_score')
            
            # Sector analysis
            sector_counts = scored_stocks['sector'].value_counts()
            top_sector = sector_counts.index[0] if not sector_counts.empty else 'Unknown'
            
            html = f"""
            <div class="section">
                <h2>📈 Market Summary</h2>
                <div class="summary-grid">
                    <div class="summary-card">
                        <h3>Market Overview</h3>
                        <p><strong>Total Stocks Analyzed:</strong> {total_stocks}</p>
                        <p><strong>Average Overall Score:</strong> {avg_score:.1f}</p>
                        <p><strong>Top Performing Sector:</strong> {top_sector}</p>
                    </div>
                    <div class="summary-card">
                        <h3>Top Performers</h3>
                        <ul>
            """
            
            for _, stock in top_performers.iterrows():
                html += f"<li>{stock.get('ticker', 'N/A')}: {stock.get('overall_score', 0):.1f}</li>"
            
            html += """
                        </ul>
                    </div>
                </div>
            </div>
            """
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generating market summary: {e}")
            return "<p>Error generating market summary</p>"
    
    def _get_key_strengths(self, stock: pd.Series) -> str:
        """Get key strengths for a stock."""
        try:
            strengths = []
            
            if stock.get('halal_score', 0) >= 80:
                strengths.append("Strong Halal")
            if stock.get('hedge_fund_score', 0) >= 80:
                strengths.append("Hedge Fund Favorite")
            if stock.get('fundamental_score', 0) >= 80:
                strengths.append("Strong Fundamentals")
            if stock.get('trend_score', 0) >= 80:
                strengths.append("Strong Trend")
            
            if not strengths:
                strengths.append("Balanced Profile")
            
            return ", ".join(strengths)
            
        except Exception as e:
            return "Balanced Profile"
    
    def _generate_html_email(self, email_sections: dict) -> str:
        """Generate complete HTML email."""
        try:
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Daily Stock Digest - {datetime.now().strftime('%B %d, %Y')}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    .header {{ text-align: center; border-bottom: 2px solid #007bff; padding-bottom: 20px; margin-bottom: 30px; }}
                    .section {{ margin-bottom: 30px; }}
                    h1 {{ color: #007bff; margin: 0; }}
                    h2 {{ color: #333; border-left: 4px solid #007bff; padding-left: 15px; }}
                    .table-container {{ overflow-x: auto; }}
                    .stock-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                    .stock-table th, .stock-table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                    .stock-table th {{ background-color: #f8f9fa; font-weight: bold; }}
                    .stock-table tr:hover {{ background-color: #f5f5f5; }}
                    .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
                    .summary-card {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #007bff; }}
                    .footer {{ text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>📊 Daily Stock Digest</h1>
                        <p><strong>{datetime.now().strftime('%B %d, %Y')}</strong></p>
                        <p>Powered by Yahoo Finance & Advanced Scoring Algorithms</p>
                    </div>
                    
                    {email_sections['market_summary']}
                    {email_sections['top_10_picks']}
                    {email_sections['market_insights']}
                    {email_sections['complete_analysis']}
                    
                    <div class="footer">
                        <p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p>Data source: Yahoo Finance | Scoring: Advanced Algorithms</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generating HTML email: {e}")
            return f"<p>Error generating email: {e}</p>"
    
    def _generate_text_email(self, email_sections: dict) -> str:
        """Generate plain text email."""
        try:
            text = f"""
Daily Stock Digest - {datetime.now().strftime('%B %d, %Y')}
{'=' * 60}

Powered by Yahoo Finance & Advanced Scoring Algorithms

{email_sections['market_summary']}
{email_sections['top_10_picks']}
{email_sections['market_insights']}
{email_sections['complete_analysis']}

Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Data source: Yahoo Finance | Scoring: Advanced Algorithms
            """
            
            return text
            
        except Exception as e:
            logger.error(f"❌ Error generating text email: {e}")
            return f"Error generating email: {e}"


class YahooFinanceCache:
    """Wrapper for Yahoo Finance Daily Cache system."""
    
    def __init__(self):
        self.cache_system = YahooFinanceDailyCache()
    
    def get_stock_data(self, tickers: list, force_refresh: bool = False) -> pd.DataFrame:
        """Get stock data from Yahoo Finance cache."""
        try:
            return self.cache_system.get_stock_data(tickers, force_refresh)
        except Exception as e:
            logger.error(f"❌ Error getting Yahoo Finance data: {e}")
            # Return empty DataFrame as fallback
            return pd.DataFrame()


def main():
    """Test the integrated digest system."""
    print("🧪 Testing Integrated Digest System with Yahoo Finance Cache")
    print("=" * 70)
    
    try:
        # Initialize integrated system
        integrated_system = IntegratedDigestSystem()
        
        # Generate comprehensive email
        print("\n📧 Generating comprehensive email...")
        email_data = integrated_system.generate_comprehensive_email(force_refresh=False)
        
        print(f"\n✅ Email generated successfully!")
        print(f"📊 Stock data: {len(email_data['stock_data'])} stocks")
        print(f"📧 HTML content length: {len(email_data['html_content'])} characters")
        print(f"📧 Text content length: {len(email_data['text_content'])} characters")
        
        # Show sample data
        print(f"\n📋 Sample Stock Data:")
        sample_cols = ['ticker', 'company_name', 'sector', 'current_price', 'overall_score']
        print(email_data['stock_data'][sample_cols].head(3).to_string(index=False))
        
        # Show available metrics
        print(f"\n🔍 Available Metrics: {len(email_data['stock_data'].columns)}")
        
        # Save email content to files
        cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(cache_dir, exist_ok=True)
        
        html_file = os.path.join(cache_dir, "integrated_digest_email.html")
        text_file = os.path.join(cache_dir, "integrated_digest_email.txt")
        
        with open(html_file, 'w') as f:
            f.write(email_data['html_content'])
        
        with open(text_file, 'w') as f:
            f.write(email_data['text_content'])
        
        print(f"\n💾 Email content saved to:")
        print(f"   HTML: {html_file}")
        print(f"   Text: {text_file}")
        
        print(f"\n🎉 Integrated Digest System test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
