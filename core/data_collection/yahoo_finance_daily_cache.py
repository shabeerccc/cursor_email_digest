#!/usr/bin/env python3
"""
Yahoo Finance Daily Cache System
Calls Yahoo Finance API only once per day with robust caching and fallback logic
"""

import yfinance as yf
import pandas as pd
import json
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging
from dotenv import load_dotenv
import time
import hashlib

# Load environment
env_path = os.path.expanduser("~/stock_digest_platform/.env")
load_dotenv(env_path)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class YahooFinanceDailyCache:
    """
    Yahoo Finance Daily Cache System
    
    Features:
    - Calls Yahoo Finance API only once per day
    - Stores data in both JSON and SQLite for redundancy
    - Fallback to yesterday's cache if API fails
    - Clean, modular design for easy extension
    """
    
    def __init__(self, cache_dir: str = None, db_name: str = "stock_cache.db"):
        self.cache_dir = cache_dir or os.path.expanduser("~/stock_digest_platform/cache")
        self.json_cache_file = os.path.join(self.cache_dir, "yahoo_finance_cache.json")
        self.db_file = os.path.join(self.cache_dir, db_name)
        self.cache_metadata_file = os.path.join(self.cache_dir, "cache_metadata.json")
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache settings
        self.cache_expiry_hours = 24  # Cache expires after 24 hours
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Initialize database
        self._init_database()
        
        # Load cache metadata
        self.cache_metadata = self._load_cache_metadata()
    
    def _init_database(self):
        """Initialize SQLite database with proper schema."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Create stocks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    ticker TEXT PRIMARY KEY,
                    data_json TEXT,
                    last_updated TEXT,
                    cache_date TEXT,
                    data_hash TEXT
                )
            ''')
            
            # Create cache_metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            # Create index for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_cache_date ON stocks(cache_date)
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
    
    def _load_cache_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from file."""
        try:
            if os.path.exists(self.cache_metadata_file):
                with open(self.cache_metadata_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Cache metadata loading failed: {e}")
        
        return {
            'last_api_call': None,
            'last_successful_update': None,
            'total_stocks_cached': 0,
            'cache_status': 'empty'
        }
    
    def _save_cache_metadata(self, metadata: Dict[str, Any]):
        """Save cache metadata to file."""
        try:
            with open(self.cache_metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Cache metadata saving failed: {e}")
    
    def _is_cache_valid(self, cache_date: str) -> bool:
        """Check if cache is still valid (same day)."""
        try:
            if not cache_date:
                return False
            
            cache_time = datetime.fromisoformat(cache_date)
            current_time = datetime.now()
            
            # Cache is valid if it's from the same day
            return cache_time.date() == current_time.date()
            
        except Exception as e:
            logger.warning(f"⚠️ Cache validation error: {e}")
            return False
    
    def _get_cache_status(self) -> Dict[str, Any]:
        """Get comprehensive cache status."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Get today's cache count
            today = datetime.now().date().isoformat()
            cursor.execute("SELECT COUNT(*) FROM stocks WHERE cache_date = ?", (today,))
            today_count = cursor.fetchone()[0]
            
            # Get yesterday's cache count
            yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
            cursor.execute("SELECT COUNT(*) FROM stocks WHERE cache_date = ?", (yesterday,))
            yesterday_count = cursor.fetchone()[0]
            
            # Get total cached stocks
            cursor.execute("SELECT COUNT(*) FROM stocks")
            total_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'today_cache_count': today_count,
                'yesterday_cache_count': yesterday_count,
                'total_cached_stocks': total_count,
                'today_cache_valid': today_count > 0,
                'yesterday_cache_available': yesterday_count > 0,
                'cache_date': today,
                'last_api_call': self.cache_metadata.get('last_api_call'),
                'last_successful_update': self.cache_metadata.get('last_successful_update')
            }
            
        except Exception as e:
            logger.error(f"❌ Cache status check failed: {e}")
            return {}
    
    def _collect_stock_data_from_yahoo(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """Collect stock data from Yahoo Finance API with rate limiting."""
        logger.info(f"🔍 Collecting data for {len(tickers)} tickers from Yahoo Finance...")
        
        stock_data = []
        successful_count = 0
        failed_count = 0
        
        for i, ticker in enumerate(tickers, 1):
            try:
                logger.info(f"📊 Processing {ticker} ({i}/{len(tickers)})")
                
                # Get stock info
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # Get historical data for calculations
                hist = stock.history(period="1y")
                
                if hist.empty:
                    logger.warning(f"⚠️ No historical data for {ticker}")
                    failed_count += 1
                    continue
                
                # Calculate derived metrics
                current_price = hist['Close'].iloc[-1]
                prev_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                change = current_price - prev_close
                change_percent = (change / prev_close) * 100 if prev_close > 0 else 0
                
                # 52-week calculations
                high_52w = hist['High'].max()
                low_52w = hist['Low'].min()
                
                # Volume analysis
                avg_volume_30d = hist['Volume'].tail(30).mean()
                current_volume = hist['Volume'].iloc[-1]
                volume_ratio = current_volume / avg_volume_30d if avg_volume_30d > 0 else 1
                
                # Trend analysis
                price_30d_ago = hist['Close'].iloc[-30] if len(hist) >= 30 else current_price
                price_90d_ago = hist['Close'].iloc[-90] if len(hist) >= 90 else current_price
                trend_30d = ((current_price - price_30d_ago) / price_30d_ago) * 100 if price_30d_ago > 0 else 0
                trend_90d = ((current_price - price_90d_ago) / price_90d_ago) * 100 if price_90d_ago > 0 else 0
                
                # RSI calculation (14-day)
                if len(hist) >= 14:
                    delta = hist['Close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs.iloc[-1]))
                else:
                    rsi = 50
                
                # Volatility calculation
                returns = hist['Close'].pct_change().dropna()
                volatility = returns.std() * (252 ** 0.5)  # Annualized
                
                # Comprehensive stock data
                stock_info = {
                    'ticker': ticker,
                    'company_name': info.get('longName', ticker),
                    'sector': info.get('sector', 'Unknown'),
                    'industry': info.get('industry', 'Unknown'),
                    
                    # Price data
                    'current_price': round(current_price, 2),
                    'change': round(change, 2),
                    'change_percent': round(change_percent, 2),
                    'high_52w': round(high_52w, 2),
                    'low_52w': round(low_52w, 2),
                    'open': round(hist['Open'].iloc[-1], 2),
                    'prev_close': round(prev_close, 2),
                    
                    # Volume data
                    'volume': int(current_volume),
                    'avg_volume_30d': int(avg_volume_30d),
                    'volume_ratio': round(volume_ratio, 2),
                    
                    # Market data
                    'market_cap': info.get('marketCap', 0),
                    'enterprise_value': info.get('enterpriseValue', 0),
                    'shares_outstanding': info.get('sharesOutstanding', 0),
                    'float_shares': info.get('floatShares', 0),
                    
                    # Valuation metrics
                    'pe_ratio': info.get('trailingPE', 0),
                    'forward_pe': info.get('forwardPE', 0),
                    'peg_ratio': info.get('pegRatio', 0),
                    'pb_ratio': info.get('priceToBook', 0),
                    'ps_ratio': info.get('priceToSalesTrailing12Months', 0),
                    'ev_ebitda': info.get('enterpriseToEbitda', 0),
                    
                    # Financial metrics
                    'debt_to_equity': info.get('debtToEquity', 0),
                    'debt_to_assets': info.get('debtToAssets', 0),
                    'current_ratio': info.get('currentRatio', 0),
                    'quick_ratio': info.get('quickRatio', 0),
                    'free_cash_flow': info.get('freeCashflow', 0),
                    'operating_cash_flow': info.get('operatingCashflow', 0),
                    
                    # Profitability metrics
                    'profit_margin': info.get('profitMargins', 0),
                    'operating_margin': info.get('operatingMargins', 0),
                    'gross_margin': info.get('grossMargins', 0),
                    'ebitda_margins': info.get('ebitdaMargins', 0),
                    
                    # Growth metrics
                    'revenue_growth': info.get('revenueGrowth', 0),
                    'earnings_growth': info.get('earningsGrowth', 0),
                    'eps_growth': info.get('earningsQuarterlyGrowth', 0),
                    
                    # Efficiency metrics
                    'roe': info.get('returnOnEquity', 0),
                    'roa': info.get('returnOnAssets', 0),
                    'roic': info.get('returnOnInvestedCapital', 0),
                    'asset_turnover': info.get('assetTurnover', 0),
                    
                    # EPS data
                    'eps': info.get('trailingEps', 0),
                    'forward_eps': info.get('forwardEps', 0),
                    'book_value': info.get('bookValue', 0),
                    
                    # Dividend data
                    'dividend_rate': info.get('dividendRate', 0),
                    'dividend_yield': info.get('dividendYield', 0),
                    'payout_ratio': info.get('payoutRatio', 0),
                    
                    # Analyst data
                    'recommendation': info.get('recommendationMean', 'Hold'),
                    'recommendation_key': info.get('recommendationKey', 'hold'),
                    'target_price': info.get('targetMeanPrice', 0),
                    'target_high': info.get('targetHighPrice', 0),
                    'target_low': info.get('targetLowPrice', 0),
                    'number_of_analysts': info.get('numberOfAnalystOpinions', 0),
                    
                    # Technical indicators
                    'trend_30d': round(trend_30d, 2),
                    'trend_90d': round(trend_90d, 2),
                    'rsi': round(rsi, 2),
                    'volatility': round(volatility, 4),
                    
                    # Additional metrics
                    'beta': info.get('beta', 0),
                    'short_ratio': info.get('shortRatio', 0),
                    'shares_short': info.get('sharesShort', 0),
                    'shares_short_prev_month': info.get('sharesShortPriorMonth', 0),
                    
                    # Collection timestamp
                    'data_collected_at': datetime.now().isoformat(),
                    'cache_date': datetime.now().date().isoformat()
                }
                
                stock_data.append(stock_info)
                successful_count += 1
                
                # Rate limiting - be respectful to Yahoo Finance
                if i % 5 == 0:  # Every 5 requests
                    time.sleep(2)  # 2 second delay
                
                # Additional delay for every 20 requests
                if i % 20 == 0:
                    time.sleep(5)  # 5 second delay
                
            except Exception as e:
                logger.warning(f"⚠️ Error collecting data for {ticker}: {e}")
                failed_count += 1
                
                # If it's a rate limit error, wait longer
                if "429" in str(e) or "Too Many Requests" in str(e):
                    logger.info(f"🔄 Rate limit hit, waiting 10 seconds before continuing...")
                    time.sleep(10)
                
                continue
        
        logger.info(f"✅ Successfully collected data for {successful_count} stocks, {failed_count} failed")
        return stock_data
    
    def _save_to_cache(self, stock_data: List[Dict[str, Any]], cache_date: str):
        """Save stock data to both JSON and SQLite cache."""
        try:
            # Save to JSON cache
            cache_data = {
                'cache_date': cache_date,
                'total_stocks': len(stock_data),
                'data': stock_data,
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'data_version': '1.0'
                }
            }
            
            with open(self.json_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Save to SQLite cache
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            for stock in stock_data:
                ticker = stock['ticker']
                data_json = json.dumps(stock)
                data_hash = hashlib.md5(data_json.encode()).hexdigest()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO stocks (ticker, data_json, last_updated, cache_date, data_hash)
                    VALUES (?, ?, ?, ?, ?)
                ''', (ticker, data_json, datetime.now().isoformat(), cache_date, data_hash))
            
            # Update cache metadata
            cursor.execute('''
                INSERT OR REPLACE INTO cache_metadata (key, value)
                VALUES (?, ?)
            ''', ('last_successful_update', datetime.now().isoformat()))
            
            cursor.execute('''
                INSERT OR REPLACE INTO cache_metadata (key, value)
                VALUES (?, ?)
            ''', ('total_stocks_cached', str(len(stock_data))))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Data saved to cache: {len(stock_data)} stocks")
            
        except Exception as e:
            logger.error(f"❌ Cache saving failed: {e}")
    
    def _load_from_cache(self, cache_date: str) -> Optional[List[Dict[str, Any]]]:
        """Load stock data from cache."""
        try:
            # Try SQLite first (more reliable)
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute("SELECT data_json FROM stocks WHERE cache_date = ?", (cache_date,))
            rows = cursor.fetchall()
            
            if rows:
                stock_data = [json.loads(row[0]) for row in rows]
                conn.close()
                logger.info(f"✅ Loaded {len(stock_data)} stocks from SQLite cache")
                return stock_data
            
            conn.close()
            
            # Fallback to JSON cache
            if os.path.exists(self.json_cache_file):
                with open(self.json_cache_file, 'r') as f:
                    cache_data = json.load(f)
                    if cache_data.get('cache_date') == cache_date:
                        stock_data = cache_data.get('data', [])
                        logger.info(f"✅ Loaded {len(stock_data)} stocks from JSON cache")
                        return stock_data
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Cache loading failed: {e}")
            return None
    
    def get_stock_data(self, tickers: List[str], force_refresh: bool = False) -> pd.DataFrame:
        """
        Main function to get stock data with intelligent caching.
        
        Args:
            tickers: List of stock tickers
            force_refresh: Force refresh even if cache is valid
            
        Returns:
            DataFrame with stock data
        """
        try:
            current_date = datetime.now().date().isoformat()
            cache_status = self._get_cache_status()
            
            logger.info(f"📊 Cache Status: {cache_status}")
            
            # Check if today's cache is valid
            if not force_refresh and cache_status.get('today_cache_valid', False):
                logger.info("🔄 Using today's cached data")
                cached_data = self._load_from_cache(current_date)
                if cached_data:
                    # Filter to requested tickers
                    filtered_data = [stock for stock in cached_data if stock['ticker'] in tickers]
                    return pd.DataFrame(filtered_data)
            
            # Check if we should call API (only once per day)
            last_api_call = self.cache_metadata.get('last_api_call')
            if last_api_call:
                last_call_date = datetime.fromisoformat(last_api_call).date()
                if last_call_date == datetime.now().date() and not force_refresh:
                    logger.info("🔄 API already called today, using yesterday's cache")
                    yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
                    cached_data = self._load_from_cache(yesterday)
                    if cached_data:
                        filtered_data = [stock for stock in cached_data if stock['ticker'] in tickers]
                        return pd.DataFrame(filtered_data)
            
            # Call Yahoo Finance API
            logger.info("🔄 Calling Yahoo Finance API for fresh data...")
            self.cache_metadata['last_api_call'] = datetime.now().isoformat()
            self._save_cache_metadata(self.cache_metadata)
            
            stock_data = self._collect_stock_data_from_yahoo(tickers)
            
            if stock_data:
                # Save to cache
                self._save_to_cache(stock_data, current_date)
                
                # Update metadata
                self.cache_metadata['last_successful_update'] = datetime.now().isoformat()
                self._save_cache_metadata(self.cache_metadata)
                
                return pd.DataFrame(stock_data)
            else:
                # API failed, try yesterday's cache
                logger.warning("⚠️ API call failed, trying yesterday's cache...")
                yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
                cached_data = self._load_from_cache(yesterday)
                
                if cached_data:
                    filtered_data = [stock for stock in cached_data if stock['ticker'] in tickers]
                    logger.info(f"✅ Using yesterday's cache: {len(filtered_data)} stocks")
                    return pd.DataFrame(filtered_data)
                else:
                    raise ValueError("No data available from API or cache")
            
        except Exception as e:
            logger.error(f"❌ Error in get_stock_data: {e}")
            raise
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get comprehensive cache information."""
        cache_status = self._get_cache_status()
        
        return {
            'cache_status': cache_status,
            'cache_metadata': self.cache_metadata,
            'cache_files': {
                'json_cache': self.json_cache_file,
                'sqlite_db': self.db_file,
                'metadata_file': self.cache_metadata_file
            },
            'cache_directory': self.cache_dir
        }
    
    def clear_cache(self, older_than_days: int = 7):
        """Clear old cache data."""
        try:
            cutoff_date = (datetime.now() - timedelta(days=older_than_days)).date().isoformat()
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM stocks WHERE cache_date < ?", (cutoff_date,))
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Cleared {deleted_count} old cache entries")
            
        except Exception as e:
            logger.error(f"❌ Cache clearing failed: {e}")


def get_stock_data_daily(tickers: List[str], force_refresh: bool = False) -> pd.DataFrame:
    """
    Convenience function to get stock data with daily caching.
    
    Args:
        tickers: List of stock tickers
        force_refresh: Force refresh even if cache is valid
        
    Returns:
        DataFrame with stock data
    """
    cache_system = YahooFinanceDailyCache()
    return cache_system.get_stock_data(tickers, force_refresh)


def main():
    """Test the Yahoo Finance Daily Cache system."""
    print("🧪 Testing Yahoo Finance Daily Cache System")
    print("=" * 60)
    
    try:
        # Initialize cache system
        cache_system = YahooFinanceDailyCache()
        
        # Test tickers
        test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'META']
        
        # Get cache info
        print("\n📊 Cache Information:")
        cache_info = cache_system.get_cache_info()
        for key, value in cache_info['cache_status'].items():
            print(f"   {key}: {value}")
        
        # Get stock data
        print(f"\n📈 Getting stock data for {len(test_tickers)} tickers...")
        df = cache_system.get_stock_data(test_tickers)
        
        print(f"\n✅ Successfully retrieved data for {len(df)} stocks")
        print(f"📊 Data shape: {df.shape}")
        
        # Show sample data
        print(f"\n📋 Sample Data:")
        sample_cols = ['ticker', 'company_name', 'sector', 'current_price', 'market_cap', 'pe_ratio']
        print(df[sample_cols].head(3).to_string(index=False))
        
        # Show available metrics
        print(f"\n🔍 Available Metrics: {len(df.columns)}")
        metric_categories = {
            'Price & Volume': ['current_price', 'volume', 'high_52w', 'low_52w'],
            'Valuation': ['pe_ratio', 'pb_ratio', 'ps_ratio', 'peg_ratio'],
            'Financial': ['debt_to_equity', 'roe', 'roa', 'profit_margin'],
            'Growth': ['revenue_growth', 'eps_growth', 'earnings_growth'],
            'Technical': ['trend_30d', 'rsi', 'volatility', 'beta']
        }
        
        for category, metrics in metric_categories.items():
            available_count = sum(1 for m in metrics if m in df.columns)
            print(f"   {category}: {available_count}/{len(metrics)} metrics")
        
        print(f"\n🎉 Yahoo Finance Daily Cache system test completed successfully!")
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
