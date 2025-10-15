#!/usr/bin/env python3
"""
Multi-Source Stock Data System with Enhanced Caching
Integrates Yahoo Finance, Alpha Vantage, and local cache with proper data persistence
"""

import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests

# Add the current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import enhanced caching system
try:
    from enhanced_caching_system import EnhancedCachingSystem
    ENHANCED_CACHING_AVAILABLE = True
except ImportError:
    ENHANCED_CACHING_AVAILABLE = False
    print("⚠️ Enhanced caching system not available")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlphaVantageDataSource:
    """Alpha Vantage API data source with rate limiting."""
    
    def __init__(self):
        self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limit_delay = 12  # 5 calls per minute = 12 second delay
        self.last_call_time = 0
        
        if not self.api_key:
            logger.warning("⚠️ Alpha Vantage API key not found in environment variables")
    
    def is_available(self) -> bool:
        return bool(self.api_key)
    
    def _rate_limit_check(self):
        """Ensure rate limiting between API calls."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_call
            logger.info(f"⏳ Alpha Vantage rate limit: waiting {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()
    
    def get_stock_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get real-time stock quote data."""
        try:
            if not self.is_available():
                return None
            
            self._rate_limit_check()
            
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': ticker,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Global Quote' in data and data['Global Quote']:
                quote = data['Global Quote']
                
                return {
                    'ticker': ticker,
                    'current_price': float(quote.get('05. price', 0)) if quote.get('05. price') != 'None' else None,
                    'change': float(quote.get('09. change', 0)) if quote.get('09. change') != 'None' else None,
                    'change_percent': float(quote.get('10. change percent', '0').replace('%', '')) if quote.get('10. change percent') != 'None' else None,
                    'open': float(quote.get('02. open', 0)) if quote.get('02. open') != 'None' else None,
                    'high': float(quote.get('03. high', 0)) if quote.get('03. high') != 'None' else None,
                    'low': float(quote.get('04. low', 0)) if quote.get('04. low') != 'None' else None,
                    'prev_close': float(quote.get('08. previous close', 0)) if quote.get('08. previous close') != 'None' else None,
                    'volume': int(quote.get('06. volume', 0)) if quote.get('06. volume') != 'None' else None,
                    'data_source': 'Alpha Vantage'
                }
            else:
                logger.warning(f"⚠️ No quote data for {ticker} from Alpha Vantage")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️ Alpha Vantage quote error for {ticker}: {e}")
            return None
    
    def get_company_overview(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get company overview and fundamental data."""
        try:
            if not self.is_available():
                return None
            
            self._rate_limit_check()
            
            params = {
                'function': 'OVERVIEW',
                'symbol': ticker,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data and 'Symbol' in data:
                return {
                    'ticker': ticker,
                    'company_name': data.get('Name', f"{ticker} Corporation"),
                    'sector': data.get('Sector', 'Unknown'),
                    'industry': data.get('Industry', 'Unknown'),
                    'market_cap': self._parse_market_cap(data.get('MarketCapitalization', '0')),
                    'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') != 'None' else None,
                    'pb_ratio': float(data.get('PriceToBookRatio', 0)) if data.get('PriceToBookRatio') != 'None' else None,
                    'ps_ratio': float(data.get('PriceToSalesRatio', 0)) if data.get('PriceToSalesRatio') != 'None' else None,
                    'peg_ratio': float(data.get('PEGRatio', 0)) if data.get('PEGRatio') != 'None' else None,
                    'debt_to_equity': float(data.get('DebtToEquity', 0)) if data.get('DebtToEquity') != 'None' else None,
                    'profit_margin': float(data.get('ProfitMargin', 0)) if data.get('ProfitMargin') != 'None' else None,
                    'operating_margin': float(data.get('OperatingMargin', 0)) if data.get('OperatingMargin') != 'None' else None,
                    'roe': float(data.get('ReturnOnEquity', 0)) if data.get('ReturnOnEquity') != 'None' else None,
                    'roa': float(data.get('ReturnOnAssets', 0)) if data.get('ReturnOnAssets') != 'None' else None,
                    'eps': float(data.get('EPS', 0)) if data.get('EPS') != 'None' else None,
                    'dividend_yield': float(data.get('DividendYield', 0)) if data.get('DividendYield') != 'None' else None,
                    'data_source': 'Alpha Vantage'
                }
            else:
                logger.warning(f"⚠️ No overview data for {ticker} from Alpha Vantage")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️ Alpha Vantage overview error for {ticker}: {e}")
            return None
    
    def _parse_market_cap(self, market_cap_str: str) -> Optional[int]:
        """Parse market cap string to integer."""
        try:
            if not market_cap_str or market_cap_str == 'None':
                return None
            
            # Remove any non-numeric characters except decimal point
            cleaned = ''.join(c for c in market_cap_str if c.isdigit() or c == '.')
            if cleaned:
                return int(float(cleaned))
            return None
        except:
            return None
    
    def get_income_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get income statement data for growth calculations."""
        try:
            if not self.is_available():
                return None
            
            self._rate_limit_check()
            
            params = {
                'function': 'INCOME_STATEMENT',
                'symbol': ticker,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'annualReports' in data and data['annualReports']:
                latest = data['annualReports'][0]
                previous = data['annualReports'][1] if len(data['annualReports']) > 1 else None
                
                current_revenue = float(latest.get('totalRevenue', 0))
                current_net_income = float(latest.get('netIncome', 0))
                
                revenue_growth = 0
                earnings_growth = 0
                
                if previous:
                    prev_revenue = float(previous.get('totalRevenue', 0))
                    prev_net_income = float(previous.get('netIncome', 0))
                    
                    if prev_revenue > 0:
                        revenue_growth = (current_revenue - prev_revenue) / prev_revenue
                    if prev_net_income > 0:
                        earnings_growth = (current_net_income - prev_net_income) / prev_net_income
                
                return {
                    'ticker': ticker,
                    'revenue_growth': revenue_growth,
                    'earnings_growth': earnings_growth,
                    'current_revenue': current_revenue,
                    'current_net_income': current_net_income,
                    'data_source': 'Alpha Vantage'
                }
            else:
                logger.warning(f"⚠️ No income statement data for {ticker} from Alpha Vantage")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️ Alpha Vantage income statement error for {ticker}: {e}")
            return None

class MultiSourceStockDataSystem:
    """
    Multi-source stock data system with intelligent fallback and enhanced caching.
    
    Priority order:
    1. Yahoo Finance (if available)
    2. Alpha Vantage (if available)
    3. Local cache (CSV files)
    4. Sample data (final fallback)
    """
    
    def __init__(self):
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize enhanced caching system
        if ENHANCED_CACHING_AVAILABLE:
            self.enhanced_cache = EnhancedCachingSystem()
            logger.info("✅ Enhanced caching system initialized")
        else:
            self.enhanced_cache = None
            logger.warning("⚠️ Enhanced caching system not available")
        
        # Initialize data sources
        self.yahoo_cache = None
        self.alpha_vantage = AlphaVantageDataSource()
        
        # Try to initialize Yahoo Finance cache
        try:
            from yahoo_finance_daily_cache import YahooFinanceDailyCache
            self.yahoo_cache = YahooFinanceDailyCache()
            logger.info("✅ Yahoo Finance cache initialized")
        except ImportError:
            logger.warning("⚠️ Yahoo Finance cache not available")
        
        # Source availability status
        self.source_status = {
            'yahoo_finance': self.yahoo_cache is not None,
            'alpha_vantage': self.alpha_vantage.is_available(),
            'local_cache': True,
            'sample_data': True,
            'enhanced_caching': self.enhanced_cache is not None
        }
        
        logger.info(f"📊 Data source status: {self.source_status}")
    
    def get_comprehensive_stock_data(self, tickers: List[str], force_refresh: bool = False) -> pd.DataFrame:
        """
        Get comprehensive stock data from multiple sources with intelligent fallback.
        
        Args:
            tickers: List of stock tickers
            force_refresh: Force refresh of data
            
        Returns:
            DataFrame with comprehensive stock data
        """
        try:
            logger.info(f"🔍 Getting comprehensive stock data for {len(tickers)} tickers...")
            
            # Try Yahoo Finance first (if available)
            if self.source_status['yahoo_finance'] and not force_refresh:
                try:
                    logger.info("🔄 Attempting Yahoo Finance data collection...")
                    yahoo_data = self.yahoo_cache.get_stock_data(tickers, force_refresh=False)
                    if not yahoo_data.empty:
                        logger.info(f"✅ Yahoo Finance data collected: {len(yahoo_data)} stocks")
                        # Store in enhanced cache if available
                        if self.enhanced_cache:
                            self._store_data_in_enhanced_cache('yahoo_finance', yahoo_data)
                        return self._enrich_stock_data(yahoo_data)
                except Exception as e:
                    logger.warning(f"⚠️ Yahoo Finance failed: {e}")
            
            # Try Alpha Vantage as fallback
            if self.source_status['alpha_vantage']:
                try:
                    logger.info("🔄 Attempting Alpha Vantage data collection...")
                    alpha_data = self._collect_alpha_vantage_data(tickers)
                    if not alpha_data.empty:
                        logger.info(f"✅ Alpha Vantage data collected: {len(alpha_data)} stocks")
                        # Store in enhanced cache if available
                        if self.enhanced_cache:
                            self._store_data_in_enhanced_cache('alpha_vantage', alpha_data)
                        return self._enrich_stock_data(alpha_data)
                except Exception as e:
                    logger.warning(f"⚠️ Alpha Vantage failed: {e}")
            
            # Try local cache
            if self.source_status['local_cache']:
                try:
                    logger.info("🔄 Attempting local cache data retrieval...")
                    cached_data = self._get_cached_stock_data(tickers)
                    if not cached_data.empty:
                        logger.info(f"✅ Local cache data retrieved: {len(cached_data)} stocks")
                        return self._enrich_stock_data(cached_data)
                except Exception as e:
                    logger.warning(f"⚠️ Local cache failed: {e}")
            
            # Generate sample data as final fallback
            if self.source_status['sample_data']:
                logger.info("🔄 Generating sample data as final fallback...")
                sample_data = self._generate_sample_stock_data(tickers)
                return self._enrich_stock_data(sample_data)
            
            # If all else fails
            raise ValueError("All data sources failed")
            
        except Exception as e:
            logger.error(f"❌ Error in get_comprehensive_stock_data: {e}")
            raise
    
    def _store_data_in_enhanced_cache(self, source: str, data: pd.DataFrame):
        """Store collected data in the enhanced caching system."""
        if not self.enhanced_cache:
            return
        
        try:
            logger.info(f"💾 Storing {len(data)} stocks in enhanced cache from {source}")
            
            # Record API call and successful update
            self.enhanced_cache.record_api_call(source)
            self.enhanced_cache.record_successful_update(source)
            
            # Store each stock's data
            for _, row in data.iterrows():
                ticker = row.get('ticker')
                if ticker:
                    # Convert row to dictionary
                    stock_data = row.to_dict()
                    self.enhanced_cache.save_to_cache(source, ticker, stock_data)
            
            logger.info(f"✅ Successfully stored {len(data)} stocks in enhanced cache from {source}")
            
        except Exception as e:
            logger.error(f"❌ Error storing data in enhanced cache: {e}")
    
    def _collect_alpha_vantage_data(self, tickers: List[str]) -> pd.DataFrame:
        """Collect data from Alpha Vantage for multiple tickers."""
        logger.info(f"🔍 Collecting Alpha Vantage data for {len(tickers)} tickers...")
        
        stock_data = []
        
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"📊 Processing {ticker} ({i+1}/{len(tickers)})...")
                
                # Get quote data
                quote_data = self.alpha_vantage.get_stock_quote(ticker)
                if not quote_data:
                    continue
                
                # Get company overview
                overview_data = self.alpha_vantage.get_company_overview(ticker)
                if not overview_data:
                    continue
                
                # Get income statement for growth data
                income_data = self.alpha_vantage.get_income_statement(ticker)
                
                # Combine all data
                stock_info = {
                    **quote_data,
                    **overview_data,
                    'revenue_growth': income_data.get('revenue_growth', 0) if income_data else 0,
                    'earnings_growth': income_data.get('earnings_growth', 0) if income_data else 0,
                    'eps_growth': income_data.get('earnings_growth', 0) if income_data else 0,
                    'data_collected_at': datetime.now().isoformat(),
                    'cache_date': datetime.now().date().isoformat()
                }
                
                stock_data.append(stock_info)
                
                # Rate limiting between calls
                if i < len(tickers) - 1:  # Don't wait after last call
                    time.sleep(1)  # 1 second delay between calls
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing {ticker}: {e}")
                continue
        
        logger.info(f"✅ Alpha Vantage data collection completed: {len(stock_data)} stocks")
        return pd.DataFrame(stock_data)
    
    def _get_cached_stock_data(self, tickers: List[str]) -> pd.DataFrame:
        """Get cached stock data from local storage."""
        try:
            cache_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.csv') and 'stock_data' in f]
            
            if cache_files:
                # Get the most recent cache file
                cache_files.sort(reverse=True)
                latest_cache = os.path.join(self.cache_dir, cache_files[0])
                
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
    
    def _generate_sample_stock_data(self, tickers: List[str]) -> pd.DataFrame:
        """Generate comprehensive sample stock data."""
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
                    'data_source': 'Sample Data (Final Fallback)'
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
    
    def _enrich_stock_data(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Enrich stock data with additional calculated metrics."""
        try:
            logger.info("🔧 Enriching stock data with calculated metrics...")
            
            enriched_data = stock_data.copy()
            
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
            return stock_data
    
    def _add_calculated_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add calculated metrics to the dataset."""
        try:
            # Calculate additional volume metrics
            if 'volume' in data.columns and 'avg_volume_30d' in data.columns:
                data['volume_ratio'] = data['volume'] / data['avg_volume_30d'].replace(0, 1)
                data['volume_ratio'] = data['volume_ratio'].fillna(1)
            
            if 'current_price' in data.columns and 'prev_close' in data.columns:
                data['price_momentum'] = ((data['current_price'] - data['prev_close']) / data['prev_close'] * 100).fillna(0)
            
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
            data['data_age_hours'] = 0  # Fresh data
            
            # Add source indicator if not present
            if 'data_source' not in data.columns:
                data['data_source'] = 'Multi-Source System'
            
            return data
            
        except Exception as e:
            logger.warning(f"⚠️ Error adding data quality indicators: {e}")
            return data
    
    def get_data_source_status(self) -> Dict[str, Any]:
        """Get status of all data sources."""
        return {
            'yahoo_finance': {
                'available': self.source_status['yahoo_finance'],
                'description': 'Primary data source with caching'
            },
            'alpha_vantage': {
                'available': self.source_status['alpha_vantage'],
                'description': 'Free tier fallback (500 calls/day)',
                'api_key_set': bool(self.alpha_vantage.api_key)
            },
            'local_cache': {
                'available': self.source_status['local_cache'],
                'description': 'Local cached data files'
            },
            'sample_data': {
                'available': self.source_status['sample_data'],
                'description': 'Generated sample data'
            },
            'enhanced_caching': {
                'available': self.source_status['enhanced_caching'],
                'description': 'Enhanced caching system'
            }
        }


def main():
    """Test the multi-source stock data system."""
    print("🧪 Testing Multi-Source Stock Data System")
    print("=" * 50)
    
    try:
        # Initialize multi-source system
        multi_source = MultiSourceStockDataSystem()
        
        # Show data source status
        print("\n📊 Data Source Status:")
        status = multi_source.get_data_source_status()
        for source, info in status.items():
            print(f"   {source.replace('_', ' ').title()}: {'✅' if info['available'] else '❌'} {info['description']}")
        
        # Test with sample tickers
        test_tickers = ['AAPL', 'MSFT', 'GOOGL']
        print(f"\n🔍 Testing with {len(test_tickers)} tickers: {test_tickers}")
        
        # Get comprehensive data
        stock_data = multi_source.get_comprehensive_stock_data(test_tickers)
        
        print(f"\n✅ Data collection successful!")
        print(f"📊 Stocks collected: {len(stock_data)}")
        print(f"🔍 Metrics per stock: {len(stock_data.columns)}")
        print(f"📋 Data source: {stock_data['data_source'].iloc[0] if 'data_source' in stock_data.columns else 'Unknown'}")
        
        # Show sample data
        print(f"\n📋 Sample Data:")
        sample_cols = ['ticker', 'company_name', 'sector', 'current_price', 'data_source']
        print(stock_data[sample_cols].to_string(index=False))
        
        # Save test data
        cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        test_file = os.path.join(cache_dir, "multi_source_test_data.csv")
        stock_data.to_csv(test_file, index=False)
        print(f"\n💾 Test data saved to: {test_file}")
        
        print(f"\n🎉 Multi-source system test completed successfully!")
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
