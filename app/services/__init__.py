"""Service modules for trading simulation."""

from .chart_gen import generate_session_chart, fetch_ohlc_for_chart
from .predictor import predict, predict_session
from .risk_engine import calculate_risk_parameters, get_percentiles, get_all_percentiles
from .trade_executor import open_trade, close_trade, get_account_status
from .scheduler import get_scheduler, TradingScheduler
from .price_stream import PriceStream, PriceAlert, get_price_stream, start_price_stream, stop_price_stream

__all__ = [
    # Chart generation
    "generate_session_chart",
    "fetch_ohlc_for_chart",
    # Prediction
    "predict",
    "predict_session",
    # Risk engine
    "calculate_risk_parameters",
    "get_percentiles",
    "get_all_percentiles",
    # Trade execution
    "open_trade",
    "close_trade",
    "get_account_status",
    # Scheduler
    "get_scheduler",
    "TradingScheduler",
    # Price streaming
    "PriceStream",
    "PriceAlert",
    "get_price_stream",
    "start_price_stream",
    "stop_price_stream",
]
