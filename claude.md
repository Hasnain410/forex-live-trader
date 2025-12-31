# Forex Live Trader

Low-latency forex trading simulation service that uses Claude Haiku 4.5 for market predictions based on chart analysis.

## Tech Stack

- **Python 3.11+** - Primary language
- **FastAPI** - Async web framework with WebSocket support
- **PostgreSQL** - Database with asyncpg driver
- **APScheduler** - DST-aware job scheduling for trading sessions
- **Anthropic SDK** - Claude Haiku 4.5 for chart analysis predictions
- **Polygon.io** - Real-time forex quotes via WebSocket + historical OHLC data
- **mplfinance** - Chart generation with session overlays and FVG zones
- **Alembic** - Database migrations
- **boto3** - S3 storage for chart backup

## Project Structure

```
forex-live-trader/
├── app/
│   ├── main.py              # FastAPI app, endpoints, WebSocket handler
│   ├── config.py            # Settings from env, trading pairs, sessions, ECN costs
│   ├── database.py          # asyncpg connection pool, trade/account queries
│   ├── services/
│   │   ├── scheduler.py     # APScheduler with pre-warm pipeline (T-120s, T-60s, T+0, T+4h)
│   │   ├── predictor.py     # Claude Haiku API calls for chart analysis
│   │   ├── chart_gen.py     # mplfinance chart generation with FVGs and session zones
│   │   ├── risk_engine.py   # Position sizing, TP/SL from rolling window percentiles
│   │   ├── trade_executor.py# Trade open/close, account balance, rolling window updates
│   │   ├── price_stream.py  # Polygon WebSocket for real-time quotes and TP/SL alerts
│   │   └── storage.py       # S3 chart upload/listing
│   ├── utils/
│   │   ├── polygon_client.py# Polygon REST API for OHLC data
│   │   ├── session_utils.py # DST-aware session time calculations
│   │   └── forex_utils.py   # Pip values, spread calculations
│   └── templates/
│       └── dashboard.html   # Real-time trading dashboard
├── alembic/                 # Database migrations
├── scripts/
│   ├── import_baseline.py   # Import historical data from backtester
│   └── run_migration_002.py # Manual migration helper
├── data/                    # Baseline parquet files
└── systemd/                 # Service files for production deployment
```

## Core Components

### Trading Session Pipeline (scheduler.py)

The scheduler orchestrates the entire trading workflow:

1. **T-120s**: Pre-warm OHLC data for all 19 pairs (parallel fetch via Polygon REST)
2. **T-60s**: Pre-generate charts + connect Polygon WebSocket for live prices
3. **T+0s**: Run Haiku predictions sequentially, open trades with live prices
4. **Real-time**: Monitor TP/SL via WebSocket alerts, close trades immediately on hit
5. **T+4h**: Verify remaining trades (TIMEOUT), update rolling window, refresh percentiles

### Trading Sessions (UTC)

| Session | Time | Duration |
|---------|------|----------|
| Asian_Open | 01:00 | 4 hours |
| London_Open | 08:00 | 4 hours |
| NY_Open | 14:30 | 4 hours |

### Predictor (predictor.py)

- Uses Claude Haiku 4.5 (`claude-haiku-4-5-20251001`)
- Sends base64-encoded chart images with structured prompt
- Parses response for: `BULLISH`, `BEARISH`, or `NEUTRAL` with conviction (1-10)
- Async with retry logic for rate limits and timeouts

### Risk Engine (risk_engine.py)

- Uses rolling 6-month window of historical MFE/MAE data
- TP: P75 of MFE (aggressive)
- SL: P50 of MAE (median)
- Position sizing: Risk % of balance, respects min/max lot limits
- ECN cost simulation: spreads, commissions ($7/lot roundtrip), slippage

### Price Stream (price_stream.py)

- Polygon WebSocket for real-time bid/ask quotes
- PriceAlert system for TP/SL monitoring
- Automatic trade closure on price trigger

## Database Schema

Main tables:
- `account` - Balance, win/loss stats, drawdown tracking
- `trades` - All trades with entry/exit, P/L, full Claude analysis
- `rolling_window` - Historical predictions with MFE/MAE for percentile calculation
- `percentile_targets` - Materialized view of TP/SL percentiles by pair/session/model

## Key API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard HTML |
| `/health` | GET | Health check with DB status |
| `/api/account` | GET | Account balance and stats |
| `/api/trades` | GET | Trade history |
| `/api/percentiles` | GET | All cached TP/SL percentiles |
| `/api/scheduler/status` | GET | Scheduler state, active trades, WebSocket status |
| `/ws` | WS | Real-time account and trade updates |

## Configuration

All settings via environment variables (see `.env.example`):

```bash
DATABASE_URL=postgresql://forex_user:password@localhost:5432/forex_trader
ANTHROPIC_API_KEY=...
POLYGON_API_KEY=...
STARTING_BALANCE=10000.00
RISK_PERCENT=1.55
TP_PERCENTILE=P75
SL_PERCENTILE=P50
```

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
alembic upgrade head

# Start development server
uvicorn app.main:app --reload --port 8080

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Code Patterns

### Async Throughout
All database operations and API calls use async/await. The scheduler runs async jobs via APScheduler's AsyncIOScheduler.

### Settings Access
```python
from app.config import settings, TRADING_PAIRS, TRADING_SESSIONS
```

### Database Access
```python
from app.database import db
row = await db.fetchrow("SELECT * FROM trades WHERE id = $1", trade_id)
```

### Service Singletons
```python
from app.services.scheduler import get_scheduler
scheduler = get_scheduler()
```

## Important Files

- `app/config.py:74-82` - TRADING_PAIRS list (19 pairs excluding CHF)
- `app/config.py:85-89` - TRADING_SESSIONS dict with UTC times
- `app/services/predictor.py:37-83` - Claude prompt template
- `app/services/scheduler.py:48-59` - TradingScheduler class docstring
- `alembic/versions/` - Database schema migrations

## Related Projects

This service works with `Backtester_V3` which:
- Exports baseline parquet data for initial rolling window
- Uses same prompt format and prediction parsing
- Shares percentile strategy (P75 MFE / P50 MAE)
