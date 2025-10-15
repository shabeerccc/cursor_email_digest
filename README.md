# 🚀 Stock Digest - Complete Daily Stock Analysis Platform

**Standalone, Professional Daily Stock Digest Service**

A completely independent application for generating and sending comprehensive daily stock digest emails with Islamic finance scoring, market insights, and automated delivery.

## 🏗️ **Project Structure**

```
Stock Digest/
├── 📁 api/                    # FastAPI REST endpoints
├── 📁 core/                   # Core business logic (4,000+ lines)
│   ├── 📁 data_collection/    # Stock data collection
│   │   ├── yahoo_finance_daily_cache.py      # Yahoo Finance integration
│   │   ├── multi_source_stock_data.py        # Multi-source data collection
│   │   └── run_digest_with_sheets_data.py    # Google Sheets integration
│   ├── 📁 scoring/           # Stock scoring algorithms
│   │   ├── integrated_digest_with_yahoo_cache.py  # Main scoring system
│   │   └── market_insights_generator.py           # Market insights
│   ├── 📁 email/             # Email generation & sending
│   │   └── smart_email_sender.py                  # Email automation
│   └── 📁 caching/           # Multi-source caching system
│       ├── enhanced_caching_system.py             # Enhanced caching
│       └── smart_cache_first_system.py            # Smart cache-first
├── 📁 config/                 # Configuration files
├── 📁 cache/                  # Data cache storage
├── 📁 logs/                   # Application logs
├── 📁 tests/                  # Test suite
├── 📁 docs/                   # Documentation
├── 📄 requirements.txt        # Dependencies
└── 📄 README.md               # This file
```

## 🚀 **Features**

- ✅ **Complete Email Generation System**
- ✅ **Google Sheets Integration** (98 stocks, 15 sectors)
- ✅ **Smart Cache-First Data Collection**
- ✅ **Yahoo Finance + Alpha Vantage APIs**
- ✅ **Enhanced Multi-Source Caching**
- ✅ **Comprehensive Stock Scoring** (Halal, Hedge Fund, Activity, Trend, Fundamental)
- ✅ **Market Insights by Sector** (One stock per sector)
- ✅ **Automated Email Delivery**
- ✅ **Background Processing**
- ✅ **RESTful API Endpoints**

## 🎯 **Quick Start**

### 1. **Navigate to Stock Digest Directory**
```bash
cd ~/Stock\ Digest
```

### 2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 3. **Configure Environment**
Copy your `.env` file to `config/.env` with:
- Gmail credentials
- Google Sheets API credentials
- Alpha Vantage API key

### 4. **Run the Application**
```bash
python3 run.py
```

### 5. **Access the Service**
- **Service**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## 📡 **API Endpoints**

### Core Endpoints
- `GET /` - Application status
- `GET /health` - Health check
- `GET /api/v1/status` - System status

### Digest Endpoints
- `POST /api/v1/digest/send` - Send daily digest email
- `GET /api/v1/digest/status` - Digest generation status
- `POST /api/v1/digest/test` - Test email generation

### Data Endpoints
- `GET /api/v1/stocks` - Get stock data
- `GET /api/v1/cache/status` - Cache system status
- `POST /api/v1/cache/refresh` - Refresh cache data

## 🔧 **Configuration**

### Environment Variables
- `ENVIRONMENT`: `development` | `production`
- `LOG_LEVEL`: `DEBUG` | `INFO` | `WARNING` | `ERROR`
- `CACHE_EXPIRY_HOURS`: Cache expiration time
- `API_RATE_LIMIT`: API call limits per day

### Cache Settings
- **Local CSV Cache**: Primary data source
- **Enhanced SQLite Cache**: Multi-source metadata
- **Fallback Data**: Sample data when APIs unavailable

## 📊 **Data Flow**

```
Google Sheets → Smart Cache System → Scoring Engine → Email Generator → SMTP Delivery
     ↓                    ↓              ↓              ↓              ↓
   98 Stocks        Cache-First     6 Scores      Rich HTML      Gmail SMTP
   15 Sectors       API Fallback   Per Stock     + Text         Background
```

## 🧪 **Testing**

### Run All Tests
```bash
python3 test_standalone.py
```

### Test Specific Components
```bash
# Test data collection
python tests/test_data_collection.py

# Test scoring system
python tests/test_scoring.py

# Test email generation
python tests/test_email.py

# Test complete pipeline
python tests/test_integration.py
```

## 📈 **Monitoring**

### Health Checks
- API endpoint availability
- Cache system status
- Email delivery status
- Data freshness

### Logs
- Application logs: `logs/app.log`
- Error logs: `logs/error.log`
- Access logs: `logs/access.log`

## 🔒 **Security**

- Environment variable configuration
- API rate limiting
- Secure email credentials
- Input validation
- Error handling

## 🚀 **Deployment**

### Docker
```bash
docker build -t stock-digest .
docker run -p 8000:8000 stock-digest
```

### Systemd Service
```bash
sudo cp stock-digest.service /etc/systemd/system/
sudo systemctl enable stock-digest
sudo systemctl start stock-digest
```

## 📝 **Development**

### Adding New Features
1. **Core Logic**: Add to appropriate `core/` subdirectory
2. **API Endpoints**: Add to `api/main.py`
3. **Configuration**: Update `config/settings.py`
4. **Tests**: Add to `tests/` directory

### Code Organization
- **Business Logic**: `core/` directory
- **API Interface**: `api/` directory
- **Configuration**: `config/` directory
- **Data Storage**: `cache/` directory
- **Logging**: `logs/` directory

## 🤝 **Support**

For issues or questions:
1. Check the logs in `logs/` directory
2. Review configuration in `config/` directory
3. Run tests with `python3 test_standalone.py`

## 📝 **License**

This is a standalone application for internal use.

---

**Status**: 🟢 Production Ready  
**Last Updated**: 2025-08-30  
**Version**: 2.0.0  
**Location**: `~/Stock Digest/` (Completely independent from IAMHR)
