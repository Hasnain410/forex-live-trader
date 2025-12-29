"""
Application configuration loaded from environment variables.
"""

from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Literal
from decimal import Decimal


# Base directory (forex-live-trader root)
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CHARTS_DIR = BASE_DIR / "charts"


class Settings(BaseSettings):
    """Application settings from environment."""

    # Database
    database_url: str = Field(
        default="postgresql://forex_user:password@localhost:5432/forex_trader",
        description="PostgreSQL connection URL"
    )

    # API Keys
    anthropic_api_key: str = Field(default="", description="Anthropic API key for Claude")
    polygon_api_key: str = Field(default="", description="Polygon.io API key for OHLC data")

    # Trading Configuration
    starting_balance: Decimal = Field(default=Decimal("10000.00"))
    risk_percent: Decimal = Field(default=Decimal("1.55"))
    max_lot_size: Decimal = Field(default=Decimal("5.0"))
    min_lot_size: Decimal = Field(default=Decimal("0.01"))

    # ECN Costs
    commission_per_lot: Decimal = Field(
        default=Decimal("3.50"),
        description="Commission per lot per side ($7 roundtrip)"
    )
    default_spread_pips: Decimal = Field(default=Decimal("0.3"))

    # Rolling Window
    rolling_window_months: int = Field(default=6)

    # Percentile Strategy (matches backtester P/L Simulator)
    # TP uses P75 of MFE (Aggressive - 75th percentile)
    # SL uses P50 of MAE (Median - 50th percentile)
    tp_percentile: Literal["P25", "P50", "P75"] = Field(default="P75")
    sl_percentile: Literal["P25", "P50", "P75"] = Field(default="P50")

    # Pre-warm Timing
    ohlc_prewarm_seconds: int = Field(default=120)
    chart_prewarm_seconds: int = Field(default=60)

    # S3 Backup
    s3_bucket: str = Field(default="forex-backtester-hasnain")
    s3_backup_prefix: str = Field(default="live-trader-backup/")

    # Server
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)
    debug: bool = Field(default=False)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Trading pairs (19 pairs - excluding CHF pairs)
TRADING_PAIRS = [
    # Majors
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "NZDUSD",
    # Crosses
    "EURGBP", "EURJPY", "GBPJPY", "EURAUD", "EURCAD", "EURNZD",
    "GBPAUD", "GBPCAD", "GBPNZD", "AUDJPY", "CADJPY",
    # Metals
    "XAUUSD", "XAGUSD",
]

# Trading sessions (UTC times, DST-aware via session_utils)
TRADING_SESSIONS = {
    "Asian_Open": {"hour": 1, "minute": 0, "duration_hours": 4},
    "London_Open": {"hour": 8, "minute": 0, "duration_hours": 4},
    "NY_Open": {"hour": 14, "minute": 30, "duration_hours": 4},
}

# ECN Spreads by pair (typical values in pips)
ECN_SPREADS = {
    "EURUSD": 0.1,
    "GBPUSD": 0.3,
    "USDJPY": 0.2,
    "AUDUSD": 0.3,
    "USDCAD": 0.4,
    "NZDUSD": 0.5,
    "EURGBP": 0.4,
    "EURJPY": 0.5,
    "GBPJPY": 0.8,
    "EURAUD": 0.6,
    "EURCAD": 0.6,
    "EURNZD": 0.8,
    "GBPAUD": 0.9,
    "GBPCAD": 0.8,
    "GBPNZD": 1.0,
    "AUDJPY": 0.5,
    "CADJPY": 0.5,
    "XAUUSD": 0.15,  # Gold in dollars
    "XAGUSD": 0.02,  # Silver
}

# Slippage model (in pips)
SLIPPAGE = {
    "entry": 0.2,       # Market order entry
    "exit_tp": 0.1,     # Limit order at TP (less slippage)
    "exit_sl": 0.5,     # Stop order at SL (more slippage)
}


# Global settings instance
settings = Settings()
