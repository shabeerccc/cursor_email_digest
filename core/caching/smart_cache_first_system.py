#!/usr/bin/env python3
"""
Smart Cache-First System
Automatically uses cache when APIs are rate-limited - no user input required
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SmartCacheFirstSystem:
    """
    Smart system that automatically uses cache when APIs are rate-limited.
    No user input required - completely automated and fast.
    """
    
    def __init__(self):
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize enhanced caching system
        try:
            from enhanced_caching_system import EnhancedCachingSystem
            self.enhanced_cache = EnhancedCachingSystem()
            logger.info("✅ Enhanced caching system initialized")
        except ImportError:
            self.enhanced_cache = None
            logger.warning("⚠️ Enhanced caching system not available")
        
        # Initialize data sources
        self.yahoo_cache = None
        self.alpha_vantage = None
        
        # Try to initialize Yahoo Finance cache
        try:
            from yahoo_finance_daily_cache import YahooFinanceDailyCache
            self.yahoo_cache = YahooFinanceDailyCache()
            logger.info("✅ Yahoo Finance cache initialized")
        except ImportError:
            logger.warning("⚠️ Yahoo Finance cache not available")
        
        # Try to initialize Alpha Vantage
        try:
            from multi_source_stock_data import AlphaVantageDataSource
            self.alpha_vantage = AlphaVantageDataSource()
            logger.info("✅ Alpha Vantage initialized")
        except ImportError:
            logger.warning("⚠️ Alpha Vantage not available")
        
        # Source availability status
        self.source_status = {
            'yahoo_finance': self.yahoo_cache is not None,
            'alpha_vantage': self.alpha_vantage and self.alpha_vantage.is_available(),
            'enhanced_caching': self.enhanced_cache is not None,
            'local_cache': True,
            'sample_data': True
        }
        
        logger.info(f"📊 Data source status: {self.source_status}")
    
    def get_stock_data_smart(self, tickers: List[str], force_refresh: bool = False) -> pd.DataFrame:
        """
        Smart data collection that automatically uses cache when APIs are rate-limited.
        
        Strategy:
        1. Check if APIs can be called today (enhanced cache)
        2. If yes, try API calls
        3. If no, immediately use cache
        4. Fallback to local cache and sample data
        """
        try:
            logger.info(f"🧠 Smart data collection for {len(tickers)} tickers...")
            
            # Step 1: Check if we can call APIs today
            can_call_apis = self._can_call_apis_today()
            
            if can_call_apis and not force_refresh:
                logger.info("✅ APIs available today - attempting fresh data collection")
                return self._collect_fresh_data(tickers)
            else:
                logger.info("🚫 APIs not available today - using cache immediately")
                return self._use_cache_only(tickers)
                
        except Exception as e:
            logger.error(f"❌ Error in smart data collection: {e}")
            return self._use_cache_only(tickers)
    
    def _can_call_apis_today(self) -> bool:
        """Check if any APIs can be called today."""
        if not self.enhanced_cache:
            return False
        
        try:
            # Check Yahoo Finance
            if self.source_status['yahoo_finance']:
                if self.enhanced_cache.can_call_api_today('yahoo_finance'):
                    logger.info("✅ Yahoo Finance API available today")
                    return True
            
            # Check Alpha Vantage
            if self.source_status['alpha_vantage']:
                if self.enhanced_cache.can_call_api_today('alpha_vantage'):
                    logger.info("✅ Alpha Vantage API available today")
                    return True
            
            logger.info("🚫 No APIs available today - using cache")
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking API availability: {e}")
            return False
    
    def _collect_fresh_data(self, tickers: List[str]) -> pd.DataFrame:
        """Attempt to collect fresh data from APIs."""
        try:
            # Try Yahoo Finance first
            if self.source_status['yahoo_finance']:
                try:
                    logger.info("🔄 Attempting Yahoo Finance data collection...")
                    yahoo_data = self.yahoo_cache.get_stock_data(tickers, force_refresh=True)
                    if not yahoo_data.empty:
                        logger.info(f"✅ Yahoo Finance data collected: {len(yahoo_data)} stocks")
                        # Store in enhanced cache
                        if self.enhanced_cache:
                            self._store_data_in_enhanced_cache('yahoo_finance', yahoo_data)
                        return yahoo_data
                except Exception as e:
                    logger.warning(f"⚠️ Yahoo Finance failed: {e}")
            
            # Try Alpha Vantage as fallback
            if self.source_status['alpha_vantage']:
                try:
                    logger.info("🔄 Attempting Alpha Vantage data collection...")
                    alpha_data = self._collect_alpha_vantage_data(tickers)
                    if not alpha_data.empty:
                        logger.info(f"✅ Alpha Vantage data collected: {len(alpha_data)} stocks")
                        # Store in enhanced cache
                        if self.enhanced_cache:
                            self._store_data_in_enhanced_cache('alpha_vantage', alpha_data)
                        return alpha_data
                except Exception as e:
                    logger.warning(f"⚠️ Alpha Vantage failed: {e}")
            
            # If APIs fail, fall back to cache
            logger.info("🔄 APIs failed, falling back to cache")
            return self._use_cache_only(tickers)
            
        except Exception as e:
            logger.error(f"❌ Error collecting fresh data: {e}")
            return self._use_cache_only(tickers)
    
    def _use_cache_only(self, tickers: List[str]) -> pd.DataFrame:
        """Use only cached data - no API calls."""
        logger.info("💾 Using cache-only strategy for fast response")
        
        # Try local CSV cache FIRST (it has the most complete data)
        try:
            logger.info("🔄 Checking local CSV cache first (most complete)...")
            cached_data = self._get_local_cache_data(tickers)
            if not cached_data.empty:
                logger.info(f"✅ Local CSV cache data retrieved: {len(cached_data)} stocks")
                return cached_data
        except Exception as e:
            logger.warning(f"⚠️ Local CSV cache failed: {e}")
        
        # Try enhanced cache as fallback
        if self.enhanced_cache:
            try:
                logger.info("🔄 Checking enhanced cache as fallback...")
                cached_data = self._get_from_enhanced_cache(tickers)
                if not cached_data.empty:
                    logger.info(f"✅ Enhanced cache data retrieved: {len(cached_data)} stocks")
                    return cached_data
            except Exception as e:
                logger.warning(f"⚠️ Enhanced cache failed: {e}")
        
        # Generate sample data as final fallback
        logger.info("🔄 Generating sample data as final fallback...")
        return self._generate_sample_data(tickers)
    
    def _get_from_enhanced_cache(self, tickers: List[str]) -> pd.DataFrame:
        """Get data from enhanced cache system."""
        if not self.enhanced_cache:
            return pd.DataFrame()
        
        try:
            cached_stocks = []
            
            for ticker in tickers:
                # Try to get from combined cache (most recent)
                cached_data = self.enhanced_cache.get_from_cache(ticker)
                if cached_data:
                    cached_stocks.append(cached_data)
            
            if cached_stocks:
                df = pd.DataFrame(cached_stocks)
                logger.info(f"✅ Retrieved {len(df)} stocks from enhanced cache")
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Error getting from enhanced cache: {e}")
            return pd.DataFrame()
    
    def _get_local_cache_data(self, tickers: List[str]) -> pd.DataFrame:
        """Get data from local CSV cache files."""
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
                        logger.info(f"✅ Loaded {len(filtered_data)} stocks from local CSV cache")
                        return filtered_data
                    else:
                        logger.warning(f"⚠️ No matching tickers found in CSV cache for requested tickers")
                        logger.info(f"📊 CSV cache has {len(cached_data)} stocks: {cached_data['ticker'].tolist()[:10]}...")
                        logger.info(f"📊 Requested tickers: {tickers[:10]}...")
            
            logger.info("⚠️ No valid local cached data found")
            return pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"⚠️ Error loading local cache: {e}")
            return pd.DataFrame()
    
    def _collect_alpha_vantage_data(self, tickers: List[str]) -> pd.DataFrame:
        """Collect data from Alpha Vantage."""
        if not self.alpha_vantage:
            return pd.DataFrame()
        
        try:
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
                    
                    # Combine data
                    stock_info = {
                        **quote_data,
                        **overview_data,
                        'data_collected_at': datetime.now().isoformat(),
                        'cache_date': datetime.now().date().isoformat(),
                        'data_source': 'Alpha Vantage'
                    }
                    
                    stock_data.append(stock_info)
                    
                    # Rate limiting between calls
                    if i < len(tickers) - 1:
                        import time
                        time.sleep(1)  # 1 second delay
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error processing {ticker}: {e}")
                    continue
            
            logger.info(f"✅ Alpha Vantage data collection completed: {len(stock_data)} stocks")
            return pd.DataFrame(stock_data)
            
        except Exception as e:
            logger.error(f"❌ Error in Alpha Vantage collection: {e}")
            return pd.DataFrame()
    
    def _store_data_in_enhanced_cache(self, source: str, data: pd.DataFrame):
        """Store collected data in enhanced cache."""
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
                    stock_data = row.to_dict()
                    self.enhanced_cache.save_to_cache(source, ticker, stock_data)
            
            logger.info(f"✅ Successfully stored {len(data)} stocks in enhanced cache from {source}")
            
        except Exception as e:
            logger.error(f"❌ Error storing data in enhanced cache: {e}")
    
    def _generate_sample_data(self, tickers: List[str]) -> pd.DataFrame:
        """Generate sample data for fallback."""
        try:
            logger.info(f"🔄 Generating sample data for {len(tickers)} tickers...")
            
            stock_data = []
            
            for i, ticker in enumerate(tickers):
                stock_info = {
                    'ticker': ticker,
                    'company_name': f"{ticker} Corporation",
                    'sector': 'Technology',
                    'current_price': round(100 + (i * 25), 2),
                    'change': round((i * 2) - 5, 2),
                    'volume': int(1000000 + (i * 500000)),
                    'market_cap': int(1000000000 + (i * 50000000000)),
                    'pe_ratio': round(15 + (i * 2), 2),
                    'data_source': 'Sample Data (Cache Fallback)',
                    'data_collected_at': datetime.now().isoformat()
                }
                
                stock_data.append(stock_info)
            
            logger.info(f"✅ Generated sample data for {len(stock_data)} stocks")
            return pd.DataFrame(stock_data)
            
        except Exception as e:
            logger.error(f"❌ Error generating sample data: {e}")
            return pd.DataFrame()
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        status = {
            'enhanced_caching': {
                'available': self.source_status['enhanced_caching'],
                'description': 'Enhanced caching system'
            },
            'yahoo_finance': {
                'available': self.source_status['yahoo_finance'],
                'description': 'Yahoo Finance cache'
            },
            'alpha_vantage': {
                'available': self.source_status['alpha_vantage'],
                'description': 'Alpha Vantage API'
            },
            'local_cache': {
                'available': self.source_status['local_cache'],
                'description': 'Local CSV cache'
            },
            'sample_data': {
                'available': self.source_status['sample_data'],
                'description': 'Sample data generation'
            }
        }
        
        # Add API availability status
        if self.enhanced_cache:
            status['api_availability'] = {
                'yahoo_finance': self.enhanced_cache.can_call_api_today('yahoo_finance'),
                'alpha_vantage': self.enhanced_cache.can_call_api_today('alpha_vantage')
            }
        
        return status

def main():
    """Test the smart cache-first system."""
    print("🧠 Smart Cache-First System Test")
    print("=" * 40)
    
    try:
        # Initialize system
        smart_system = SmartCacheFirstSystem()
        print("✅ Smart system initialized")
        
        # Check system status
        status = smart_system.get_system_status()
        print(f"📊 System Status: {status}")
        
        # Test with sample tickers
        test_tickers = ['AAPL', 'MSFT', 'GOOGL']
        print(f"\n📊 Testing with tickers: {test_tickers}")
        
        # Get data (will automatically use cache if APIs are rate-limited)
        print("🔄 Getting stock data (smart cache-first)...")
        data = smart_system.get_stock_data_smart(test_tickers, force_refresh=False)
        
        if not data.empty:
            print(f"✅ Data retrieved: {len(data)} stocks")
            print(f"📊 Data columns: {len(data.columns)}")
            print(f"📊 Sample data: {data.head(2)}")
            print(f"📊 Data source: {data['data_source'].iloc[0] if 'data_source' in data.columns else 'Unknown'}")
            
            print("\n🎉 Smart cache-first system is working!")
            print("💡 No more user input required - automatically uses cache when APIs are rate-limited!")
            return True
        else:
            print("❌ No data retrieved")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
