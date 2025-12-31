"""
Session Scheduler with Pre-Warm Pipeline
=========================================

Manages the trading schedule with DST-aware session times.
Implements a pre-warm pipeline for low-latency execution:

Timeline (relative to session open T+0):
- T-120s: Pre-warm OHLC data for all pairs (parallel fetch)
- T-60s:  Pre-generate charts + connect WebSocket (parallel generation)
- T+0s:   Run predictions and open trades (sequential per pair)
- T+Xm:   TP/SL hit detected via WebSocket → close trade immediately
- T+4h:   Verify remaining trades (TIMEOUT) and update rolling window

Key components:
- APScheduler for DST-aware job scheduling
- Asyncio for concurrent OHLC/chart pre-warming
- Polygon WebSocket for real-time price streaming
- Sequential prediction to avoid API rate limits
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from decimal import Decimal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from ..config import settings, TRADING_PAIRS, TRADING_SESSIONS
from ..utils.session_utils import get_session_times_for_date
from ..utils.polygon_client import fetch_ohlc_data_async
from .chart_gen import generate_chart, CHARTS_DIR
from .predictor import predict
from .risk_engine import calculate_risk_parameters
from .trade_executor import open_trade, close_trade, add_to_rolling_window, refresh_percentiles, cleanup_old_rolling_data
from .price_stream import PriceStream, PriceAlert, get_price_stream

logger = logging.getLogger(__name__)


# Pre-warm timing (seconds before session open)
OHLC_PREWARM_SECONDS = 120
CHART_PREWARM_SECONDS = 60


class TradingScheduler:
    """
    Manages the trading schedule with pre-warm pipeline.

    Handles:
    - Session scheduling (Asian, London, NY opens)
    - Pre-warming (OHLC and chart generation)
    - Real-time price streaming (Polygon WebSocket)
    - Trade execution with live prices
    - TP/SL monitoring and early exit
    - Trade verification
    """

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._ohlc_cache: Dict[str, Any] = {}  # pair -> DataFrame
        self._chart_cache: Dict[str, str] = {}  # pair -> chart_path
        self._current_session: Optional[str] = None
        self._active_trades: Dict[str, Dict] = {}  # trade_id -> trade info
        self._price_stream: Optional[PriceStream] = None
        self._pending_verifications: Dict[str, Dict] = {}  # trade_id -> info for rolling window

    def start(self):
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            print("Trading scheduler started")
            self._schedule_next_session()
            self._schedule_daily_cleanup()

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            print("Trading scheduler stopped")

    def _schedule_daily_cleanup(self):
        """
        Schedule daily cleanup of old rolling window data.

        Runs at 00:00 UTC daily to remove data older than 6 months.
        """
        from apscheduler.triggers.cron import CronTrigger

        self.scheduler.add_job(
            self._run_daily_cleanup,
            trigger=CronTrigger(hour=0, minute=0, timezone='UTC'),
            id="daily_cleanup",
            replace_existing=True
        )
        print("  Daily cleanup scheduled at 00:00 UTC")

    async def _run_daily_cleanup(self):
        """
        Run daily cleanup tasks:
        1. Remove rolling window data older than 6 months
        2. Refresh percentiles after cleanup
        """
        logger.info("Running daily cleanup...")

        try:
            # Cleanup old rolling window data
            deleted = await cleanup_old_rolling_data()
            logger.info(f"  Cleaned up {deleted} old rolling window records")

            # Refresh percentiles if any data was deleted
            if deleted > 0:
                await refresh_percentiles()
                logger.info("  Refreshed percentiles after cleanup")

        except Exception as e:
            logger.error(f"Daily cleanup error: {e}")

    async def _on_price_alert(self, alert: PriceAlert):
        """
        Handle TP/SL alert from price stream.

        Called immediately when price hits TP or SL level.
        Closes trade in real-time instead of waiting for T+4h verification.
        """
        trade_id = alert.trade_id
        trade_info = self._active_trades.get(trade_id)

        if not trade_info:
            logger.warning(f"Alert for unknown trade: {trade_id}")
            return

        try:
            outcome = "WIN" if alert.trigger_type == "TP" else "LOSS"
            is_stop = alert.trigger_type == "SL"

            logger.info(f"[REAL-TIME] {trade_info['pair']}: {outcome} at {alert.trigger_price} "
                       f"(trigger: {alert.trigger_type})")

            # Close trade immediately
            result = await close_trade(
                trade_id=trade_id,
                exit_price=alert.trigger_price,
                outcome=outcome,
                is_stop_exit=is_stop
            )

            if result:
                logger.info(f"  Trade closed: P/L ${result.pnl_dollars:+.2f}")

                # Store info for rolling window update at session end
                self._pending_verifications[trade_id] = {
                    'pair': trade_info['pair'],
                    'session_name': trade_info['session_name'],
                    'session_datetime': trade_info['session_datetime'],
                    'prediction': trade_info['prediction'],
                    'model': trade_info.get('model', 'claude_haiku_45'),
                    'outcome': outcome,
                    'trigger_price': alert.trigger_price,
                    'trigger_time': alert.trigger_time,
                }

                # Remove from active trades
                del self._active_trades[trade_id]

                # Remove alert from price stream
                if self._price_stream:
                    self._price_stream.remove_alert(trade_id)

        except Exception as e:
            logger.error(f"Error handling alert for {trade_id}: {e}")

    def _get_next_session(self) -> tuple:
        """
        Calculate the next trading session.

        Returns:
            Tuple of (session_name, session_datetime)
        """
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Check today and tomorrow for next session
        for day_offset in range(2):
            check_date = today + timedelta(days=day_offset)

            # Skip weekends
            if check_date.weekday() in [5, 6]:  # Saturday, Sunday
                continue

            # Get DST-aware session times
            session_times = get_session_times_for_date(check_date)

            for session_name in ['Asian_Open', 'London_Open', 'NY_Open']:
                session_info = session_times[session_name]
                session_dt = check_date.replace(
                    hour=session_info['hour'],
                    minute=session_info['minute'],
                    second=0,
                    microsecond=0,
                    tzinfo=timezone.utc
                )

                # Only future sessions
                if session_dt > now:
                    return session_name, session_dt

        # Fallback: Monday Asian Open
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        next_monday = today + timedelta(days=days_until_monday)
        session_times = get_session_times_for_date(next_monday)
        asian = session_times['Asian_Open']
        return 'Asian_Open', next_monday.replace(
            hour=asian['hour'],
            minute=asian['minute'],
            tzinfo=timezone.utc
        )

    def _schedule_next_session(self):
        """Schedule the pre-warm and execution jobs for the next session."""
        session_name, session_dt = self._get_next_session()

        print(f"Next session: {session_name} at {session_dt.isoformat()}")

        # Schedule OHLC pre-warm (T-120s)
        ohlc_time = session_dt - timedelta(seconds=OHLC_PREWARM_SECONDS)
        if ohlc_time > datetime.now(timezone.utc):
            self.scheduler.add_job(
                self._prewarm_ohlc,
                trigger=DateTrigger(run_date=ohlc_time),
                args=[session_name, session_dt],
                id=f"ohlc_{session_name}_{session_dt.isoformat()}",
                replace_existing=True
            )
            print(f"  OHLC pre-warm scheduled at {ohlc_time.isoformat()}")

        # Schedule chart pre-warm (T-60s)
        chart_time = session_dt - timedelta(seconds=CHART_PREWARM_SECONDS)
        if chart_time > datetime.now(timezone.utc):
            self.scheduler.add_job(
                self._prewarm_charts,
                trigger=DateTrigger(run_date=chart_time),
                args=[session_name, session_dt],
                id=f"chart_{session_name}_{session_dt.isoformat()}",
                replace_existing=True
            )
            print(f"  Chart pre-warm scheduled at {chart_time.isoformat()}")

        # Schedule session execution (T+0)
        self.scheduler.add_job(
            self._execute_session,
            trigger=DateTrigger(run_date=session_dt),
            args=[session_name, session_dt],
            id=f"exec_{session_name}_{session_dt.isoformat()}",
            replace_existing=True
        )
        print(f"  Execution scheduled at {session_dt.isoformat()}")

        # Schedule verification (T+4h)
        verify_time = session_dt + timedelta(hours=4)
        self.scheduler.add_job(
            self._verify_session,
            trigger=DateTrigger(run_date=verify_time),
            args=[session_name, session_dt],
            id=f"verify_{session_name}_{session_dt.isoformat()}",
            replace_existing=True
        )
        print(f"  Verification scheduled at {verify_time.isoformat()}")

    async def _prewarm_ohlc(self, session_name: str, session_dt: datetime):
        """
        Pre-warm OHLC data for all pairs.

        Fetches 7 days of data in parallel to ensure sufficient history.
        """
        print(f"\n[T-{OHLC_PREWARM_SECONDS}s] Pre-warming OHLC data...")
        self._ohlc_cache.clear()

        start_date = session_dt - timedelta(days=7)

        async def fetch_pair(pair: str):
            try:
                df = await fetch_ohlc_data_async(
                    pair=pair,
                    start_date=start_date,
                    end_date=session_dt,
                    api_key=settings.polygon_api_key
                )
                if df is not None and not df.empty:
                    self._ohlc_cache[pair] = df
                    return True
            except Exception as e:
                print(f"  Error fetching {pair}: {e}")
            return False

        # Fetch all pairs in parallel
        tasks = [fetch_pair(pair) for pair in TRADING_PAIRS]
        results = await asyncio.gather(*tasks)

        success_count = sum(results)
        print(f"  Pre-warmed {success_count}/{len(TRADING_PAIRS)} pairs")

    async def _prewarm_charts(self, session_name: str, session_dt: datetime):
        """
        Pre-generate charts for all pairs and connect WebSocket.

        Uses cached OHLC data to generate charts in parallel.
        Also connects to Polygon WebSocket for real-time prices.
        """
        print(f"\n[T-{CHART_PREWARM_SECONDS}s] Pre-generating charts...")
        self._chart_cache.clear()

        # Connect to Polygon WebSocket for real-time prices
        if self._price_stream is None:
            self._price_stream = PriceStream(
                api_key=settings.polygon_api_key,
                on_alert=self._on_price_alert
            )

        if not self._price_stream.is_connected:
            if await self._price_stream.connect():
                await self._price_stream.subscribe(TRADING_PAIRS)
                print(f"  WebSocket connected, subscribed to {len(TRADING_PAIRS)} pairs")

        async def generate_pair_chart(pair: str):
            try:
                ohlc_df = self._ohlc_cache.get(pair)
                if ohlc_df is None:
                    print(f"  No OHLC cache for {pair}, skipping")
                    return False

                # Run chart generation in thread pool (matplotlib is not async)
                loop = asyncio.get_event_loop()
                chart_path = await loop.run_in_executor(
                    None,
                    generate_chart,
                    ohlc_df,
                    pair,
                    session_name,
                    session_dt,
                    CHARTS_DIR
                )

                if chart_path:
                    self._chart_cache[pair] = chart_path
                    return True
            except Exception as e:
                print(f"  Error generating chart for {pair}: {e}")
            return False

        # Generate charts in parallel (limit concurrency to 4)
        semaphore = asyncio.Semaphore(4)

        async def limited_generate(pair: str):
            async with semaphore:
                return await generate_pair_chart(pair)

        tasks = [limited_generate(pair) for pair in TRADING_PAIRS]
        results = await asyncio.gather(*tasks)

        success_count = sum(results)
        print(f"  Pre-generated {success_count}/{len(TRADING_PAIRS)} charts")

    async def _execute_session(self, session_name: str, session_dt: datetime):
        """
        Execute trading for the session.

        Runs predictions sequentially to avoid API rate limits.
        Opens trades for BULLISH/BEARISH predictions.
        """
        print(f"\n[T+0s] Executing {session_name} predictions...")
        self._current_session = session_name

        # Get current account balance
        from .trade_executor import get_account_status
        account = await get_account_status()
        balance = Decimal(str(account['balance']))

        predictions_made = 0
        trades_opened = 0

        for pair in TRADING_PAIRS:
            try:
                chart_path = self._chart_cache.get(pair)
                if chart_path is None:
                    print(f"  {pair}: No chart available, skipping")
                    continue

                # Run prediction
                result = await predict(chart_path, pair, session_name)
                predictions_made += 1

                prediction = result.get('prediction', 'NEUTRAL')
                conviction = result.get('conviction', 0)

                print(f"  {pair}: {prediction} (conviction: {conviction})")

                # Skip NEUTRAL predictions
                if prediction == 'NEUTRAL':
                    continue

                # Get current price (prefer real-time WebSocket, fallback to OHLC)
                entry_price = None
                spread_pips = 0.0

                if self._price_stream and self._price_stream.is_connected:
                    quote = self._price_stream.get_quote(pair)
                    if quote:
                        # Use bid for BEARISH (selling), ask for BULLISH (buying)
                        entry_price = quote.ask if prediction == 'BULLISH' else quote.bid
                        spread_pips = quote.spread_pips
                        logger.info(f"    Real-time price: {entry_price:.5f} (spread: {spread_pips:.1f} pips)")

                # Fallback to last OHLC close
                if entry_price is None:
                    ohlc_df = self._ohlc_cache.get(pair)
                    if ohlc_df is None or ohlc_df.empty:
                        continue
                    entry_price = float(ohlc_df['close'].iloc[-1])
                    logger.info(f"    Using OHLC close: {entry_price:.5f}")

                # Calculate risk parameters
                risk_params = await calculate_risk_parameters(
                    pair=pair,
                    session_name=session_name,
                    direction=prediction,
                    entry_price=entry_price,
                    balance=balance,
                )

                if risk_params is None:
                    print(f"    No risk parameters for {pair}")
                    continue

                # Open trade with full analysis
                trade = await open_trade(
                    risk_params=risk_params,
                    prediction=prediction,
                    conviction=conviction,
                    session_datetime=session_dt,
                    full_analysis=result.get('full_analysis'),  # Save Claude's full analysis
                )

                trades_opened += 1
                self._active_trades[trade.trade_id] = {
                    'pair': pair,
                    'session_name': session_name,
                    'session_datetime': session_dt,
                    'prediction': prediction,
                    'entry_price': trade.entry_price,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'model': result.get('model_key', 'claude_haiku_45'),
                }

                # Register TP/SL alert for real-time monitoring
                if self._price_stream and self._price_stream.is_connected:
                    alert = PriceAlert(
                        trade_id=trade.trade_id,
                        pair=pair,
                        direction=prediction,
                        entry_price=trade.entry_price,
                        take_profit=trade.take_profit,
                        stop_loss=trade.stop_loss,
                    )
                    self._price_stream.add_alert(alert)
                    print(f"    Trade opened: {trade.trade_id[:8]}... "
                          f"TP={trade.tp_pips:.1f} SL={trade.sl_pips:.1f} "
                          f"Lots={trade.lot_size} [LIVE MONITORING]")
                else:
                    print(f"    Trade opened: {trade.trade_id[:8]}... "
                          f"TP={trade.tp_pips:.1f} SL={trade.sl_pips:.1f} "
                          f"Lots={trade.lot_size}")

            except Exception as e:
                print(f"  Error processing {pair}: {e}")

        print(f"\nSession complete: {predictions_made} predictions, {trades_opened} trades")

        # Clear caches
        self._ohlc_cache.clear()
        self._chart_cache.clear()

        # Schedule next session
        self._schedule_next_session()

    async def _verify_session(self, session_name: str, session_dt: datetime):
        """
        Verify trades at session close.

        Handles two types of trades:
        1. Real-time closed trades (TP/SL hit via WebSocket) - update rolling window
        2. Remaining active trades (TIMEOUT) - close and update rolling window
        """
        print(f"\n[T+4h] Verifying {session_name} trades...")

        # Calculate session end time
        session_end = session_dt + timedelta(hours=4)
        from ..utils.forex_utils import get_pip_value

        # First, process trades that were already closed via WebSocket
        realtime_closed = [
            (tid, info) for tid, info in self._pending_verifications.items()
            if (info['session_name'] == session_name and
                info['session_datetime'] == session_dt)
        ]

        for trade_id, info in realtime_closed:
            try:
                pair = info['pair']

                # Fetch OHLC to calculate MFE/MAE for rolling window
                df = await fetch_ohlc_data_async(
                    pair=pair,
                    start_date=session_dt,
                    end_date=session_end,
                    api_key=settings.polygon_api_key
                )

                if df is not None and not df.empty:
                    entry = info.get('entry_price', df['open'].iloc[0])
                    pip_value = get_pip_value(pair)
                    session_high = df['high'].max()
                    session_low = df['low'].min()

                    if info['prediction'] == 'BULLISH':
                        mfe_pips = (session_high - entry) / pip_value
                        mae_pips = abs(entry - session_low) / pip_value
                    else:
                        mfe_pips = abs(entry - session_low) / pip_value
                        mae_pips = (session_high - entry) / pip_value

                    correct = info['outcome'] == "WIN"
                    await add_to_rolling_window(
                        pair=pair,
                        session_name=session_name,
                        session_datetime=session_dt,
                        prediction=info['prediction'],
                        correct=correct,
                        mfe_pips=round(mfe_pips, 1),
                        mae_pips=round(mae_pips, 1),
                        model=info.get('model', 'claude_haiku_45'),
                    )
                    print(f"  {pair}: {info['outcome']} [REAL-TIME] added to rolling window")

                del self._pending_verifications[trade_id]

            except Exception as e:
                logger.error(f"Error processing real-time trade {trade_id}: {e}")

        # Now handle remaining active trades (TIMEOUT - didn't hit TP/SL)
        trades_to_verify = [
            (tid, tinfo) for tid, tinfo in self._active_trades.items()
            if (tinfo['session_name'] == session_name and
                tinfo['session_datetime'] == session_dt)
        ]

        if not trades_to_verify and not realtime_closed:
            print("  No trades to verify")
            # Disconnect WebSocket if no more active trades
            if self._price_stream and self._price_stream.is_connected:
                await self._price_stream.disconnect()
            return

        verified = 0

        for trade_id, trade_info in trades_to_verify:
            try:
                pair = trade_info['pair']

                # Fetch session OHLC
                df = await fetch_ohlc_data_async(
                    pair=pair,
                    start_date=session_dt,
                    end_date=session_end,
                    api_key=settings.polygon_api_key
                )

                if df is None or df.empty:
                    print(f"  {pair}: No verification data")
                    continue

                # These trades didn't hit TP/SL - close as TIMEOUT
                prediction = trade_info['prediction']
                entry = trade_info['entry_price']
                session_close = df['close'].iloc[-1]
                session_high = df['high'].max()
                session_low = df['low'].min()

                pip_value = get_pip_value(pair)

                # Calculate MFE/MAE
                if prediction == 'BULLISH':
                    mfe_pips = (session_high - entry) / pip_value
                    mae_pips = abs(entry - session_low) / pip_value
                else:
                    mfe_pips = abs(entry - session_low) / pip_value
                    mae_pips = (session_high - entry) / pip_value

                # Close as TIMEOUT at session end price
                result = await close_trade(
                    trade_id=trade_id,
                    exit_price=session_close,
                    outcome="TIMEOUT",
                    is_stop_exit=False
                )

                if result:
                    print(f"  {pair}: TIMEOUT (P/L: {result.pnl_dollars:+.2f})")
                    verified += 1

                    # For TIMEOUT, check if it was actually profitable
                    # (close_trade determines actual P/L from entry vs exit)
                    correct = result.pnl_dollars > 0

                    await add_to_rolling_window(
                        pair=pair,
                        session_name=session_name,
                        session_datetime=session_dt,
                        prediction=prediction,
                        correct=correct,
                        mfe_pips=round(mfe_pips, 1),
                        mae_pips=round(mae_pips, 1),
                        model=trade_info.get('model', 'claude_haiku_45'),
                    )

                    # Remove from active trades
                    del self._active_trades[trade_id]

                    # Remove alert if still registered
                    if self._price_stream:
                        self._price_stream.remove_alert(trade_id)

            except Exception as e:
                print(f"  Error verifying {trade_id[:8]}...: {e}")

        # Refresh percentiles if any trades were verified
        total_verified = verified + len(realtime_closed)
        if total_verified > 0:
            print(f"  Refreshing percentiles...")
            await refresh_percentiles()

        print(f"  Verified {total_verified} trades ({len(realtime_closed)} real-time, {verified} timeout)")

        # Disconnect WebSocket if no more active trades
        if not self._active_trades and self._price_stream and self._price_stream.is_connected:
            await self._price_stream.disconnect()
            print("  WebSocket disconnected (no active trades)")

    def get_status(self) -> dict:
        """Get scheduler status for API."""
        session_name, session_dt = self._get_next_session()

        # Get real-time prices for active trades
        live_prices = {}
        if self._price_stream and self._price_stream.is_connected:
            for trade_id, trade_info in self._active_trades.items():
                pair = trade_info['pair']
                quote = self._price_stream.get_quote(pair)
                if quote:
                    live_prices[pair] = {
                        'bid': quote.bid,
                        'ask': quote.ask,
                        'mid': quote.mid,
                        'spread_pips': quote.spread_pips,
                        'timestamp': quote.timestamp.isoformat(),
                    }

        return {
            "running": self.scheduler.running,
            "next_session": session_name,
            "next_session_time": session_dt.isoformat(),
            "active_trades": len(self._active_trades),
            "cached_ohlc": len(self._ohlc_cache),
            "cached_charts": len(self._chart_cache),
            "websocket_connected": self._price_stream.is_connected if self._price_stream else False,
            "live_prices": live_prices,
        }


# Global scheduler instance
_scheduler: Optional[TradingScheduler] = None


def get_scheduler() -> TradingScheduler:
    """Get the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = TradingScheduler()
    return _scheduler
