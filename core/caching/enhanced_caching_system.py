#!/usr/bin/env python3
"""
Enhanced Caching System for Multi-Source Stock Data
Ensures APIs are only called once per day with proper caching
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import json
import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional

# Load environment
try:
    from dotenv import load_dotenv
    env_path = os.path.expanduser("~/stock_digest_platform/.env")
    load_dotenv(env_path)
except ImportError:
    # dotenv not available, continue without it
    pass

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedCachingSystem:
    """
    Enhanced caching system that ensures APIs are only called once per day.
    
    Features:
    - Daily API call limits for both Yahoo Finance and Alpha Vantage
    - Persistent caching of all data sources
    - Automatic fallback to cached data
    - Cache validation and cleanup
    """
    
    def __init__(self):
        self.cache_dir = os.path.expanduser("~/stock_digest_platform/cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache metadata file
        self.metadata_file = os.path.join(self.cache_dir, "enhanced_cache_metadata.json")
        
        # Database for structured data storage
        self.db_file = os.path.join(self.cache_dir, "enhanced_stock_cache.db")
        
        # Initialize cache
        self._init_cache()
        self._load_metadata()
    
    def _init_cache(self):
        """Initialize the cache database."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Create tables for different data sources
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS yahoo_finance_cache (
                    ticker TEXT,
                    data_json TEXT,
                    collected_at TEXT,
                    cache_date TEXT,
                    PRIMARY KEY (ticker, cache_date)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alpha_vantage_cache (
                    ticker TEXT,
                    data_json TEXT,
                    collected_at TEXT,
                    cache_date TEXT,
                    PRIMARY KEY (ticker, cache_date)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS combined_cache (
                    ticker TEXT,
                    data_json TEXT,
                    data_source TEXT,
                    collected_at TEXT,
                    cache_date TEXT,
                    PRIMARY KEY (ticker, cache_date)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Enhanced cache database initialized")
            
        except Exception as e:
            logger.error(f"❌ Error initializing cache database: {e}")
    
    def _load_metadata(self):
        """Load cache metadata."""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    self.metadata = json.load(f)
            else:
                self.metadata = {
                    'yahoo_finance': {
                        'last_api_call': None,
                        'last_successful_update': None,
                        'daily_calls': 0,
                        'max_daily_calls': 1
                    },
                    'alpha_vantage': {
                        'last_api_call': None,
                        'last_successful_update': None,
                        'daily_calls': 0,
                        'max_daily_calls': 1
                    },
                    'cache_created': datetime.now().isoformat(),
                    'last_cache_cleanup': None
                }
                self._save_metadata()
            
            logger.info("✅ Cache metadata loaded")
            
        except Exception as e:
            logger.error(f"❌ Error loading cache metadata: {e}")
            self.metadata = {}
    
    def _save_metadata(self):
        """Save cache metadata."""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Error saving cache metadata: {e}")
    
    def can_call_api_today(self, source: str) -> bool:
        """Check if we can call the API today for a specific source."""
        try:
            if source not in self.metadata:
                return True
            
            source_meta = self.metadata[source]
            last_call = source_meta.get('last_api_call')
            daily_calls = source_meta.get('daily_calls', 0)
            max_calls = source_meta.get('max_daily_calls', 1)
            
            if not last_call:
                return True
            
            # Check if it's a different day
            last_call_date = datetime.fromisoformat(last_call).date()
            today = datetime.now().date()
            
            if last_call_date < today:
                # New day, reset counter
                source_meta['daily_calls'] = 0
                self._save_metadata()
                return True
            
            # Same day, check if we've reached the limit
            return daily_calls < max_calls
            
        except Exception as e:
            logger.error(f"❌ Error checking API call availability: {e}")
            return True
    
    def record_api_call(self, source: str):
        """Record that an API call was made."""
        try:
            if source not in self.metadata:
                self.metadata[source] = {
                    'last_api_call': None,
                    'last_successful_update': None,
                    'daily_calls': 0,
                    'max_daily_calls': 1
                }
            
            source_meta = self.metadata[source]
            source_meta['last_api_call'] = datetime.now().isoformat()
            source_meta['daily_calls'] = source_meta.get('daily_calls', 0) + 1
            
            self._save_metadata()
            logger.info(f"📊 API call recorded for {source} (daily calls: {source_meta['daily_calls']})")
            
        except Exception as e:
            logger.error(f"❌ Error recording API call: {e}")
    
    def record_successful_update(self, source: str):
        """Record successful data update."""
        try:
            if source not in self.metadata:
                self.metadata[source] = {}
            
            self.metadata[source]['last_successful_update'] = datetime.now().isoformat()
            self._save_metadata()
            logger.info(f"✅ Successful update recorded for {source}")
            
        except Exception as e:
            logger.error(f"❌ Error recording successful update: {e}")
    
    def save_to_cache(self, source: str, ticker: str, data: Dict[str, Any]):
        """Save data to cache for a specific source and ticker."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Convert data to JSON string
            data_json = json.dumps(data)
            cache_date = datetime.now().date().isoformat()
            collected_at = datetime.now().isoformat()
            
            # Save to source-specific table
            if source == 'yahoo_finance':
                cursor.execute('''
                    INSERT OR REPLACE INTO yahoo_finance_cache 
                    (ticker, data_json, collected_at, cache_date) 
                    VALUES (?, ?, ?, ?)
                ''', (ticker, data_json, collected_at, cache_date))
            elif source == 'alpha_vantage':
                cursor.execute('''
                    INSERT OR REPLACE INTO alpha_vantage_cache 
                    (ticker, data_json, collected_at, cache_date) 
                    VALUES (?, ?, ?, ?)
                ''', (ticker, data_json, collected_at, cache_date))
            
            # Save to combined cache
            cursor.execute('''
                INSERT OR REPLACE INTO combined_cache 
                (ticker, data_json, data_source, collected_at, cache_date) 
                VALUES (?, ?, ?, ?, ?)
            ''', (ticker, data_json, source, collected_at, cache_date))
            
            conn.commit()
            conn.close()
            
            logger.info(f"💾 Data cached for {ticker} from {source}")
            
        except Exception as e:
            logger.error(f"❌ Error saving to cache: {e}")
    
    def get_from_cache(self, ticker: str, source: str = None, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
        """Get data from cache for a specific ticker."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            if source:
                # Get from specific source
                if source == 'yahoo_finance':
                    cursor.execute('''
                        SELECT data_json, collected_at FROM yahoo_finance_cache 
                        WHERE ticker = ? ORDER BY collected_at DESC LIMIT 1
                    ''', (ticker,))
                elif source == 'alpha_vantage':
                    cursor.execute('''
                        SELECT data_json, collected_at FROM alpha_vantage_cache 
                        WHERE ticker = ? ORDER BY collected_at DESC LIMIT 1
                    ''', (ticker,))
            else:
                # Get from combined cache (most recent)
                cursor.execute('''
                    SELECT data_json, collected_at FROM combined_cache 
                    WHERE ticker = ? ORDER BY collected_at DESC LIMIT 1
                ''', (ticker,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                data_json, collected_at = result
                
                # Check if data is fresh enough
                collected_time = datetime.fromisoformat(collected_at)
                age_hours = (datetime.now() - collected_time).total_seconds() / 3600
                
                if age_hours <= max_age_hours:
                    data = json.loads(data_json)
                    logger.info(f"🔄 Retrieved {ticker} from cache (age: {age_hours:.1f} hours)")
                    return data
                else:
                    logger.info(f"⚠️ Cached data for {ticker} is too old ({age_hours:.1f} hours)")
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error retrieving from cache: {e}")
            return None
    
    def get_all_cached_tickers(self, source: str = None) -> List[str]:
        """Get list of all tickers in cache."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            if source:
                if source == 'yahoo_finance':
                    cursor.execute('SELECT DISTINCT ticker FROM yahoo_finance_cache')
                elif source == 'alpha_vantage':
                    cursor.execute('SELECT DISTINCT ticker FROM alpha_vantage_cache')
            else:
                cursor.execute('SELECT DISTINCT ticker FROM combined_cache')
            
            tickers = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return tickers
            
        except Exception as e:
            logger.error(f"❌ Error getting cached tickers: {e}")
            return []
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Get comprehensive cache status."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Count records in each table
            cursor.execute('SELECT COUNT(*) FROM yahoo_finance_cache')
            yahoo_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM alpha_vantage_cache')
            alpha_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM combined_cache')
            combined_count = cursor.fetchone()[0]
            
            conn.close()
            
            status = {
                'cache_metadata': self.metadata,
                'database_records': {
                    'yahoo_finance': yahoo_count,
                    'alpha_vantage': alpha_count,
                    'combined': combined_count
                },
                'cache_directory': self.cache_dir,
                'database_file': self.db_file,
                'metadata_file': self.metadata_file
            }
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Error getting cache status: {e}")
            return {'error': str(e)}
    
    def cleanup_old_cache(self, max_age_days: int = 7):
        """Clean up old cache entries."""
        try:
            cutoff_date = (datetime.now() - timedelta(days=max_age_days)).date().isoformat()
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Clean up old entries
            cursor.execute('DELETE FROM yahoo_finance_cache WHERE cache_date < ?', (cutoff_date,))
            yahoo_deleted = cursor.rowcount
            
            cursor.execute('DELETE FROM alpha_vantage_cache WHERE cache_date < ?', (cutoff_date,))
            alpha_deleted = cursor.rowcount
            
            cursor.execute('DELETE FROM combined_cache WHERE cache_date < ?', (cutoff_date,))
            combined_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            total_deleted = yahoo_deleted + alpha_deleted + combined_deleted
            
            if total_deleted > 0:
                logger.info(f"🧹 Cleaned up {total_deleted} old cache entries")
                self.metadata['last_cache_cleanup'] = datetime.now().isoformat()
                self._save_metadata()
            
        except Exception as e:
            logger.error(f"❌ Error cleaning up cache: {e}")


def main():
    """Test the enhanced caching system."""
    print("🧪 Testing Enhanced Caching System")
    print("=" * 40)
    
    try:
        # Initialize enhanced caching system
        cache_system = EnhancedCachingSystem()
        
        # Show initial cache status
        print("\n📊 Initial Cache Status:")
        status = cache_system.get_cache_status()
        print(f"   Yahoo Finance records: {status['database_records']['yahoo_finance']}")
        print(f"   Alpha Vantage records: {status['database_records']['alpha_vantage']}")
        print(f"   Combined records: {status['database_records']['combined']}")
        
        # Test API call availability
        print("\n🔍 API Call Availability:")
        yahoo_available = cache_system.can_call_api_today('yahoo_finance')
        alpha_available = cache_system.can_call_api_today('alpha_vantage')
        
        print(f"   Yahoo Finance: {'✅ Available' if yahoo_available else '❌ Not Available'}")
        print(f"   Alpha Vantage: {'✅ Available' if alpha_available else '❌ Not Available'}")
        
        # Test caching sample data
        print("\n💾 Testing Data Caching:")
        sample_data = {
            'ticker': 'TEST',
            'price': 100.0,
            'volume': 1000000,
            'timestamp': datetime.now().isoformat()
        }
        
        # Cache data for both sources
        cache_system.save_to_cache('yahoo_finance', 'TEST', sample_data)
        cache_system.save_to_cache('alpha_vantage', 'TEST', sample_data)
        
        # Record API calls
        cache_system.record_api_call('yahoo_finance')
        cache_system.record_api_call('alpha_vantage')
        
        # Record successful updates
        cache_system.record_successful_update('yahoo_finance')
        cache_system.record_successful_update('alpha_vantage')
        
        # Test retrieval
        retrieved_data = cache_system.get_from_cache('TEST')
        if retrieved_data:
            print(f"   ✅ Data retrieved from cache: {retrieved_data['ticker']} at ${retrieved_data['price']}")
        
        # Show updated cache status
        print("\n📊 Updated Cache Status:")
        updated_status = cache_system.get_cache_status()
        print(f"   Yahoo Finance records: {updated_status['database_records']['yahoo_finance']}")
        print(f"   Alpha Vantage records: {updated_status['database_records']['alpha_vantage']}")
        print(f"   Combined records: {updated_status['database_records']['combined']}")
        
        # Show metadata
        print("\n📋 Cache Metadata:")
        for source, meta in updated_status['cache_metadata'].items():
            if isinstance(meta, dict):
                print(f"   {source}:")
                for key, value in meta.items():
                    if key == 'last_api_call' and value:
                        # Format the timestamp
                        try:
                            dt = datetime.fromisoformat(value)
                            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                            print(f"     {key}: {formatted_time}")
                        except:
                            print(f"     {key}: {value}")
                    else:
                        print(f"     {key}: {value}")
        
        print(f"\n🎉 Enhanced caching system test completed successfully!")
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
