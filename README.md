# Forex Live Trader

Low-latency forex trading simulation service using Claude Haiku 4.5 predictions.

## Features

- **< 30 second latency** from session open to trade entry
- **Rolling 6-month window** for TP/SL percentile calculation
- **ECN cost simulation** with spreads, commissions, and slippage
- **Dynamic position sizing** based on risk percentage
- **PostgreSQL** for persistent storage with materialized views
- **FastAPI** async web framework

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     LIVE TRADING SIMULATION SERVICE                      │
├─────────────────────────────────────────────────────────────────────────┤
│  Scheduler (APScheduler) → Event Queue (asyncio) → Dashboard (FastAPI)  │
│  Chart Generator → Haiku Predictor → Rolling Window Engine (PostgreSQL) │
│  Risk Calculator → Position Sizer → Trade Executor (ECN Simulation)     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Setup Environment

```bash
# Clone repo
git clone https://github.com/yourusername/forex-live-trader.git
cd forex-live-trader

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your API keys and database URL
```

### 2. Setup PostgreSQL

```bash
# Install PostgreSQL (Amazon Linux 2023)
sudo dnf install postgresql15-server
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql
CREATE USER forex_user WITH PASSWORD 'your_password';
CREATE DATABASE forex_trader OWNER forex_user;
\q
```

### 3. Run Migrations

```bash
# Apply database schema
alembic upgrade head
```

### 4. Import Baseline Data

```bash
# Export from backtester (run in backtester repo)
cd /path/to/Backtester_V3/src
python scripts/export_baseline.py --months 12 --output trading_service_baseline.parquet

# Copy to this repo
cp trading_service_baseline.parquet /path/to/forex-live-trader/data/baseline.parquet

# Import to PostgreSQL
python scripts/import_baseline.py data/baseline.parquet
```

### 5. Run Service

```bash
# Development
uvicorn app.main:app --reload --port 8080

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service info |
| `/health` | GET | Health check |
| `/api/account` | GET | Account status |
| `/api/trades` | GET | Trade history |
| `/api/percentiles` | GET | All percentile targets |
| `/api/percentiles/{pair}/{session}` | GET | Specific percentiles |

## Configuration

See `.env.example` for all configuration options:

- `DATABASE_URL` - PostgreSQL connection string
- `ANTHROPIC_API_KEY` - Claude API key
- `POLYGON_API_KEY` - Polygon.io API key
- `STARTING_BALANCE` - Initial account balance
- `RISK_PERCENT` - Risk per trade (%)
- `TP_PERCENTILE` - Take profit percentile (P25/P50/P75)
- `SL_PERCENTILE` - Stop loss percentile (P25/P50/P75)

## Trading Sessions

| Session | UTC Time | Duration |
|---------|----------|----------|
| Asian_Open | 01:00 | 4 hours |
| London_Open | 08:00 | 4 hours |
| NY_Open | 14:30 | 4 hours |

## Latency Target

| Phase | Time | Strategy |
|-------|------|----------|
| OHLC fetch | 0s | Pre-warmed at T-120s |
| Chart generation | 0.5s | Pre-generated at T-60s |
| Claude Haiku API | 8-12s | Direct API call |
| Risk calculation | 0.05s | Cached percentiles |
| **Total** | **~12s** | 18s buffer |

## License

Private - All rights reserved.
