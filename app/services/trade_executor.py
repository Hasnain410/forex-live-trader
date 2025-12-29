"""
Trade Executor for Live Trading Simulation
===========================================

Simulates trade execution with realistic ECN broker costs:
- Spread (entry cost)
- Slippage (variable based on order type)
- Commission ($3.50 per lot per side)

Also handles:
- Trade entry (open position)
- Trade exit (close position with P/L)
- Trade verification (update rolling window)
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Literal

from ..config import settings, ECN_SPREADS, SLIPPAGE
from ..database import get_db_pool
from ..utils.forex_utils import get_pip_value, get_pip_value_in_usd
from .risk_engine import RiskParameters


@dataclass
class TradeEntry:
    """Trade entry result."""
    trade_id: str
    pair: str
    session_name: str
    session_datetime: datetime
    prediction: str
    conviction: int
    full_analysis: Optional[str]  # Full Claude analysis text
    entry_price: float
    spread_pips: float
    stop_loss: float
    take_profit: float
    sl_pips: float
    tp_pips: float
    lot_size: float
    risk_pct: float
    mfe_percentile: str
    mae_percentile: str


@dataclass
class TradeExit:
    """Trade exit result."""
    trade_id: str
    exit_price: float
    outcome: Literal["WIN", "LOSS", "BREAKEVEN", "TIMEOUT"]
    pnl_pips: float
    pnl_dollars: float
    commission: float
    slippage_pips: float


def calculate_entry_slippage(pair: str) -> float:
    """Calculate entry slippage in pips."""
    base_slippage = SLIPPAGE.get("entry", 0.2)
    # Could add volatility adjustment here
    return base_slippage


def calculate_exit_slippage(is_stop: bool = False) -> float:
    """
    Calculate exit slippage based on order type.

    Stop orders (SL) have more slippage than limit orders (TP).
    """
    if is_stop:
        return SLIPPAGE.get("exit_sl", 0.5)
    else:
        return SLIPPAGE.get("exit_tp", 0.1)


def calculate_commission(lot_size: float) -> float:
    """
    Calculate commission for a roundtrip trade.

    Commission is $3.50 per lot per side = $7.00 roundtrip per lot.
    """
    commission_per_lot = float(settings.commission_per_lot) * 2  # Roundtrip
    return lot_size * commission_per_lot


async def open_trade(
    risk_params: RiskParameters,
    prediction: str,
    conviction: int,
    session_datetime: datetime,
    full_analysis: Optional[str] = None,
) -> TradeEntry:
    """
    Open a simulated trade.

    Records the trade in the database with all entry parameters.
    Entry price is adjusted for spread.

    Args:
        risk_params: Calculated risk parameters
        prediction: 'BULLISH' or 'BEARISH'
        conviction: Conviction score (1-10)
        session_datetime: Session datetime
        full_analysis: Full Claude analysis text (optional)

    Returns:
        TradeEntry with trade details
    """
    pool = await get_db_pool()

    # Generate unique trade ID
    trade_id = str(uuid.uuid4())

    # Adjust entry for spread (buy at ask, sell at bid)
    pip_value = get_pip_value(risk_params.pair)
    spread_adjustment = risk_params.spread_pips * pip_value

    if prediction == 'BULLISH':
        # Buying: pay the spread (enter at higher price)
        adjusted_entry = risk_params.entry_price + spread_adjustment
    else:
        # Selling: pay the spread (enter at lower price)
        adjusted_entry = risk_params.entry_price - spread_adjustment

    # Insert trade record
    query = """
        INSERT INTO trades (
            trade_id, pair, session_name, session_datetime,
            prediction, conviction, full_analysis,
            entry_price, spread_pips,
            stop_loss, take_profit, sl_pips, tp_pips,
            lot_size, risk_pct, mfe_percentile, mae_percentile,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
        )
        RETURNING id
    """

    async with pool.acquire() as conn:
        await conn.execute(
            query,
            trade_id,
            risk_params.pair,
            risk_params.session_name,
            session_datetime,
            prediction,
            conviction,
            full_analysis,
            adjusted_entry,
            risk_params.spread_pips,
            risk_params.stop_loss,
            risk_params.take_profit,
            risk_params.sl_pips,
            risk_params.tp_pips,
            risk_params.lot_size,
            float(settings.risk_percent),
            settings.tp_percentile,
            settings.sl_percentile,
            datetime.now(timezone.utc),
        )

    return TradeEntry(
        trade_id=trade_id,
        pair=risk_params.pair,
        session_name=risk_params.session_name,
        session_datetime=session_datetime,
        prediction=prediction,
        conviction=conviction,
        full_analysis=full_analysis,
        entry_price=adjusted_entry,
        spread_pips=risk_params.spread_pips,
        stop_loss=risk_params.stop_loss,
        take_profit=risk_params.take_profit,
        sl_pips=risk_params.sl_pips,
        tp_pips=risk_params.tp_pips,
        lot_size=risk_params.lot_size,
        risk_pct=float(settings.risk_percent),
        mfe_percentile=settings.tp_percentile,
        mae_percentile=settings.sl_percentile,
    )


async def close_trade(
    trade_id: str,
    exit_price: float,
    outcome: Literal["WIN", "LOSS", "BREAKEVEN", "TIMEOUT"],
    is_stop_exit: bool = False,
) -> Optional[TradeExit]:
    """
    Close a trade and update the database with results.

    Calculates P/L including:
    - Price movement (pips)
    - Commission (roundtrip)
    - Slippage (based on exit type)

    Args:
        trade_id: Trade ID to close
        exit_price: Exit price
        outcome: Trade outcome
        is_stop_exit: True if exited via stop loss (more slippage)

    Returns:
        TradeExit with P/L details
    """
    pool = await get_db_pool()

    # Get trade details
    async with pool.acquire() as conn:
        trade = await conn.fetchrow(
            "SELECT * FROM trades WHERE trade_id = $1",
            trade_id
        )

        if trade is None:
            return None

        pair = trade['pair']
        entry_price = float(trade['entry_price'])
        prediction = trade['prediction']
        lot_size = float(trade['lot_size'])

        # Calculate slippage
        slippage_pips = calculate_exit_slippage(is_stop=is_stop_exit)

        # Calculate P/L in pips
        pip_value = get_pip_value(pair)

        if prediction == 'BULLISH':
            # Long: profit = exit - entry
            raw_pips = (exit_price - entry_price) / pip_value
            # Exit slippage works against us
            pnl_pips = raw_pips - slippage_pips
        else:
            # Short: profit = entry - exit
            raw_pips = (entry_price - exit_price) / pip_value
            # Exit slippage works against us
            pnl_pips = raw_pips - slippage_pips

        # Calculate P/L in dollars using accurate pip value for quote currency
        pip_value_usd = get_pip_value_in_usd(pair)
        pnl_dollars = pnl_pips * pip_value_usd * lot_size

        # Calculate commission
        commission = calculate_commission(lot_size)

        # Net P/L after commission
        net_pnl_dollars = pnl_dollars - commission

        # Update trade record
        await conn.execute("""
            UPDATE trades
            SET exit_price = $1,
                outcome = $2,
                pnl_pips = $3,
                pnl_dollars = $4,
                commission = $5,
                verified_at = $6
            WHERE trade_id = $7
        """,
            exit_price,
            outcome,
            round(pnl_pips, 1),
            round(net_pnl_dollars, 2),
            round(commission, 2),
            datetime.now(timezone.utc),
            trade_id,
        )

        # Update account balance
        await update_account_balance(conn, Decimal(str(net_pnl_dollars)), outcome)

    return TradeExit(
        trade_id=trade_id,
        exit_price=exit_price,
        outcome=outcome,
        pnl_pips=round(pnl_pips, 1),
        pnl_dollars=round(net_pnl_dollars, 2),
        commission=round(commission, 2),
        slippage_pips=slippage_pips,
    )


async def update_account_balance(conn, pnl: Decimal, outcome: str) -> None:
    """
    Update account balance and statistics after trade close.

    Args:
        conn: Database connection
        pnl: Profit/loss in dollars
        outcome: Trade outcome
    """
    # Get current account state
    account = await conn.fetchrow("SELECT * FROM account ORDER BY id LIMIT 1")

    if account is None:
        # Create initial account
        await conn.execute("""
            INSERT INTO account (
                balance, initial_balance, total_trades,
                winning_trades, losing_trades, peak_balance
            ) VALUES ($1, $1, 0, 0, 0, $1)
        """, float(settings.starting_balance))
        account = await conn.fetchrow("SELECT * FROM account ORDER BY id LIMIT 1")

    current_balance = Decimal(str(account['balance']))
    new_balance = current_balance + pnl

    # Update trade counts
    total_trades = account['total_trades'] + 1
    winning_trades = account['winning_trades'] + (1 if outcome == "WIN" else 0)
    losing_trades = account['losing_trades'] + (1 if outcome == "LOSS" else 0)

    # Update peak balance and drawdown
    peak_balance = max(Decimal(str(account['peak_balance'])), new_balance)
    if peak_balance > 0:
        drawdown_pct = ((peak_balance - new_balance) / peak_balance) * 100
    else:
        drawdown_pct = Decimal("0.00")

    max_drawdown = max(Decimal(str(account['max_drawdown_pct'])), drawdown_pct)

    # Update account
    await conn.execute("""
        UPDATE account
        SET balance = $1,
            total_trades = $2,
            winning_trades = $3,
            losing_trades = $4,
            peak_balance = $5,
            max_drawdown_pct = $6,
            last_updated = $7
        WHERE id = $8
    """,
        float(new_balance),
        total_trades,
        winning_trades,
        losing_trades,
        float(peak_balance),
        float(max_drawdown),
        datetime.now(timezone.utc),
        account['id'],
    )


async def add_to_rolling_window(
    pair: str,
    session_name: str,
    session_datetime: datetime,
    prediction: str,
    correct: bool,
    mfe_pips: float,
    mae_pips: float,
    model: str,
    mfe_first: Optional[bool] = None,
    time_to_mfe_minutes: Optional[int] = None,
    time_to_mae_minutes: Optional[int] = None,
) -> None:
    """
    Add a verified prediction to the rolling window.

    This updates the 6-month window used for percentile calculations.
    After insertion, the materialized view should be refreshed.

    Args:
        pair: Currency pair
        session_name: Session name
        session_datetime: Session datetime
        prediction: 'BULLISH' or 'BEARISH'
        correct: Whether prediction was correct
        mfe_pips: Maximum favorable excursion in pips
        mae_pips: Maximum adverse excursion in pips
        model: AI model that made the prediction (e.g., 'claude_sonnet_45')
        mfe_first: Whether MFE was hit before MAE
        time_to_mfe_minutes: Time to MFE in minutes
        time_to_mae_minutes: Time to MAE in minutes
    """
    pool = await get_db_pool()

    query = """
        INSERT INTO rolling_window (
            pair, session_name, session_datetime,
            prediction, correct, mfe_pips, mae_pips, model,
            mfe_first, time_to_mfe_minutes, time_to_mae_minutes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (pair, session_name, session_datetime, model)
        DO UPDATE SET
            correct = EXCLUDED.correct,
            mfe_pips = EXCLUDED.mfe_pips,
            mae_pips = EXCLUDED.mae_pips,
            mfe_first = EXCLUDED.mfe_first,
            time_to_mfe_minutes = EXCLUDED.time_to_mfe_minutes,
            time_to_mae_minutes = EXCLUDED.time_to_mae_minutes
    """

    async with pool.acquire() as conn:
        await conn.execute(
            query,
            pair,
            session_name,
            session_datetime,
            prediction,
            correct,
            mfe_pips,
            mae_pips,
            model,
            mfe_first,
            time_to_mfe_minutes,
            time_to_mae_minutes,
        )


async def refresh_percentiles() -> None:
    """
    Refresh the percentile_targets materialized view.

    Should be called after adding new data to rolling_window.
    Uses CONCURRENTLY to avoid blocking reads during refresh.
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY percentile_targets")


async def cleanup_old_rolling_data() -> int:
    """
    Mark rolling window data older than 6 months as excluded.

    Instead of deleting, sets in_window = FALSE to preserve historical data
    while excluding from percentile calculations.

    Returns:
        Number of rows marked as excluded
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE rolling_window
            SET in_window = FALSE
            WHERE in_window = TRUE
              AND session_datetime < NOW() - INTERVAL '6 months'
        """)
        # Parse "UPDATE X" to get count
        count = int(result.split()[-1]) if result else 0
        return count


async def get_account_status() -> dict:
    """
    Get current account status for dashboard.

    Returns:
        Account status dictionary
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        account = await conn.fetchrow("SELECT * FROM account ORDER BY id LIMIT 1")

        if account is None:
            return {
                "balance": float(settings.starting_balance),
                "initial_balance": float(settings.starting_balance),
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "peak_balance": float(settings.starting_balance),
                "max_drawdown_pct": 0.0,
                "pnl": 0.0,
                "pnl_pct": 0.0,
            }

        total = account['total_trades'] or 0
        wins = account['winning_trades'] or 0
        win_rate = (wins / total * 100) if total > 0 else 0.0

        balance = float(account['balance'])
        initial = float(account['initial_balance'])
        pnl = balance - initial
        pnl_pct = (pnl / initial * 100) if initial > 0 else 0.0

        return {
            "balance": balance,
            "initial_balance": initial,
            "total_trades": total,
            "winning_trades": wins,
            "losing_trades": account['losing_trades'] or 0,
            "win_rate": round(win_rate, 2),
            "peak_balance": float(account['peak_balance']),
            "max_drawdown_pct": float(account['max_drawdown_pct']),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
        }
