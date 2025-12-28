"""
Risk Engine for Live Trading
=============================

Calculates take-profit and stop-loss levels from rolling window percentiles.
Also handles position sizing based on account risk parameters.

Key functions:
- get_percentiles(): Fetch cached percentiles from PostgreSQL
- calculate_tp_sl(): Calculate TP/SL prices from percentiles
- calculate_position_size(): Dynamic lot sizing based on risk %
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple

from ..config import settings, ECN_SPREADS
from ..database import get_db_pool
from ..utils.forex_utils import get_pip_value


@dataclass
class PercentileTargets:
    """Percentile data for a pair/session combination."""
    pair: str
    session_name: str
    sample_count: int
    accuracy_pct: float
    mfe_p25: float
    mfe_p50: float
    mfe_p75: float
    mae_p25: float
    mae_p50: float
    mae_p75: float


@dataclass
class RiskParameters:
    """Risk calculation result."""
    pair: str
    session_name: str
    direction: str  # BULLISH or BEARISH
    entry_price: float
    take_profit: float
    stop_loss: float
    tp_pips: float
    sl_pips: float
    lot_size: float
    risk_dollars: float
    spread_pips: float
    percentile_source: str  # e.g., "P50/P75"


async def get_percentiles(pair: str, session_name: str) -> Optional[PercentileTargets]:
    """
    Fetch cached percentiles from the materialized view.

    The percentile_targets view is refreshed after each trade verification,
    providing a 6-month rolling window of MFE/MAE data.

    Args:
        pair: Currency pair (e.g., 'EURUSD')
        session_name: Session name (e.g., 'London_Open')

    Returns:
        PercentileTargets or None if not found
    """
    pool = await get_db_pool()

    query = """
        SELECT
            pair, session_name, sample_count, accuracy_pct,
            mfe_p25, mfe_p50, mfe_p75,
            mae_p25, mae_p50, mae_p75
        FROM percentile_targets
        WHERE pair = $1 AND session_name = $2
    """

    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, pair, session_name)

        if row is None:
            return None

        return PercentileTargets(
            pair=row['pair'],
            session_name=row['session_name'],
            sample_count=row['sample_count'],
            accuracy_pct=float(row['accuracy_pct']) if row['accuracy_pct'] else 0.0,
            mfe_p25=float(row['mfe_p25']) if row['mfe_p25'] else 0.0,
            mfe_p50=float(row['mfe_p50']) if row['mfe_p50'] else 0.0,
            mfe_p75=float(row['mfe_p75']) if row['mfe_p75'] else 0.0,
            mae_p25=float(row['mae_p25']) if row['mae_p25'] else 0.0,
            mae_p50=float(row['mae_p50']) if row['mae_p50'] else 0.0,
            mae_p75=float(row['mae_p75']) if row['mae_p75'] else 0.0,
        )


def get_percentile_value(targets: PercentileTargets,
                         stat_type: str,
                         percentile: str) -> float:
    """
    Get a specific percentile value from targets.

    Args:
        targets: PercentileTargets object
        stat_type: 'mfe' or 'mae'
        percentile: 'P25', 'P50', or 'P75'

    Returns:
        Percentile value in pips
    """
    attr_name = f"{stat_type.lower()}_{percentile.lower()}"
    return getattr(targets, attr_name, 0.0)


def calculate_tp_sl(
    entry_price: float,
    direction: str,
    pair: str,
    targets: PercentileTargets,
    tp_percentile: str = None,
    sl_percentile: str = None,
) -> Tuple[float, float, float, float]:
    """
    Calculate take-profit and stop-loss prices from percentiles.

    Uses direction-aware calculation:
    - BULLISH: TP above entry, SL below entry
    - BEARISH: TP below entry, SL above entry

    Args:
        entry_price: Current market price for entry
        direction: 'BULLISH' or 'BEARISH'
        pair: Currency pair
        targets: PercentileTargets with MFE/MAE data
        tp_percentile: Override TP percentile (default from settings)
        sl_percentile: Override SL percentile (default from settings)

    Returns:
        Tuple of (take_profit, stop_loss, tp_pips, sl_pips)
    """
    if tp_percentile is None:
        tp_percentile = settings.tp_percentile
    if sl_percentile is None:
        sl_percentile = settings.sl_percentile

    # Get pip value for this pair
    pip_value = get_pip_value(pair)

    # Get percentile values
    tp_pips = get_percentile_value(targets, 'mfe', tp_percentile)
    sl_pips = get_percentile_value(targets, 'mae', sl_percentile)

    # Ensure minimum values (avoid 0 pips)
    tp_pips = max(tp_pips, 5.0)  # Minimum 5 pips TP
    sl_pips = max(sl_pips, 5.0)  # Minimum 5 pips SL

    # Calculate prices based on direction
    if direction == 'BULLISH':
        take_profit = entry_price + (tp_pips * pip_value)
        stop_loss = entry_price - (sl_pips * pip_value)
    else:  # BEARISH
        take_profit = entry_price - (tp_pips * pip_value)
        stop_loss = entry_price + (sl_pips * pip_value)

    return take_profit, stop_loss, tp_pips, sl_pips


def calculate_position_size(
    balance: Decimal,
    sl_pips: float,
    pair: str,
    risk_percent: Decimal = None,
) -> Tuple[float, float]:
    """
    Calculate position size (lots) based on risk parameters.

    Formula: lot_size = (balance * risk%) / (SL_pips * pip_value_per_lot)

    Standard lot pip values (approximate):
    - EUR/USD: $10 per pip per lot
    - GBP/USD: $10 per pip per lot
    - USD/JPY: ~$9.09 per pip per lot (varies with USD/JPY rate)
    - XAU/USD: $1 per pip per 0.01 lot

    Args:
        balance: Current account balance
        sl_pips: Stop loss in pips
        pair: Currency pair
        risk_percent: Risk per trade (default from settings)

    Returns:
        Tuple of (lot_size, risk_dollars)
    """
    if risk_percent is None:
        risk_percent = settings.risk_percent

    # Calculate risk in dollars
    risk_dollars = float(balance) * (float(risk_percent) / 100)

    # Pip value per standard lot (simplified - assumes USD account)
    # For most pairs: $10 per pip per lot
    # For JPY pairs: ~$9 per pip per lot
    # For XAU/USD: $1 per pip per 0.1 lot ($10 per pip per lot)
    if 'JPY' in pair:
        pip_value_per_lot = 9.0  # Approximate for JPY pairs
    elif 'XAU' in pair or 'XAG' in pair:
        pip_value_per_lot = 10.0  # Metals
    else:
        pip_value_per_lot = 10.0  # Standard for most pairs

    # Calculate lot size
    if sl_pips <= 0:
        sl_pips = 10.0  # Fallback to prevent division by zero

    lot_size = risk_dollars / (sl_pips * pip_value_per_lot)

    # Apply limits
    lot_size = max(float(settings.min_lot_size), lot_size)
    lot_size = min(float(settings.max_lot_size), lot_size)
    lot_size = round(lot_size, 2)  # Round to 2 decimals (0.01 lot increments)

    return lot_size, risk_dollars


async def calculate_risk_parameters(
    pair: str,
    session_name: str,
    direction: str,
    entry_price: float,
    balance: Decimal,
) -> Optional[RiskParameters]:
    """
    Calculate complete risk parameters for a trade.

    This is the main entry point for the risk engine. It:
    1. Fetches percentiles from the database
    2. Calculates TP/SL from percentiles
    3. Calculates position size from risk settings
    4. Includes spread for the pair

    Args:
        pair: Currency pair
        session_name: Session name
        direction: 'BULLISH' or 'BEARISH'
        entry_price: Current market price
        balance: Current account balance

    Returns:
        RiskParameters or None if percentiles not available
    """
    # Get percentiles
    targets = await get_percentiles(pair, session_name)

    if targets is None:
        print(f"No percentile data for {pair}/{session_name}")
        return None

    if targets.sample_count < 30:
        print(f"Insufficient samples for {pair}/{session_name}: {targets.sample_count}")
        return None

    # Calculate TP/SL
    take_profit, stop_loss, tp_pips, sl_pips = calculate_tp_sl(
        entry_price=entry_price,
        direction=direction,
        pair=pair,
        targets=targets,
    )

    # Calculate position size
    lot_size, risk_dollars = calculate_position_size(
        balance=balance,
        sl_pips=sl_pips,
        pair=pair,
    )

    # Get spread
    spread_pips = ECN_SPREADS.get(pair, float(settings.default_spread_pips))

    return RiskParameters(
        pair=pair,
        session_name=session_name,
        direction=direction,
        entry_price=entry_price,
        take_profit=take_profit,
        stop_loss=stop_loss,
        tp_pips=tp_pips,
        sl_pips=sl_pips,
        lot_size=lot_size,
        risk_dollars=risk_dollars,
        spread_pips=spread_pips,
        percentile_source=f"{settings.tp_percentile}/{settings.sl_percentile}",
    )


async def get_all_percentiles() -> list:
    """
    Fetch all percentile targets for dashboard display.

    Returns:
        List of PercentileTargets for all pair/session combinations
    """
    pool = await get_db_pool()

    query = """
        SELECT
            pair, session_name, sample_count, accuracy_pct,
            mfe_p25, mfe_p50, mfe_p75,
            mae_p25, mae_p50, mae_p75
        FROM percentile_targets
        ORDER BY pair, session_name
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        return [
            PercentileTargets(
                pair=row['pair'],
                session_name=row['session_name'],
                sample_count=row['sample_count'],
                accuracy_pct=float(row['accuracy_pct']) if row['accuracy_pct'] else 0.0,
                mfe_p25=float(row['mfe_p25']) if row['mfe_p25'] else 0.0,
                mfe_p50=float(row['mfe_p50']) if row['mfe_p50'] else 0.0,
                mfe_p75=float(row['mfe_p75']) if row['mfe_p75'] else 0.0,
                mae_p25=float(row['mae_p25']) if row['mae_p25'] else 0.0,
                mae_p50=float(row['mae_p50']) if row['mae_p50'] else 0.0,
                mae_p75=float(row['mae_p75']) if row['mae_p75'] else 0.0,
            )
            for row in rows
        ]
