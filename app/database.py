"""
PostgreSQL database connection pool using asyncpg.
"""

import asyncpg
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class Database:
    """Async PostgreSQL connection pool manager."""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create connection pool."""
        if self.pool is not None:
            return

        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        logger.info("Database connection pool created")

    async def disconnect(self):
        """Close connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed")

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get a connection from the pool."""
        if self.pool is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        async with self.pool.acquire() as conn:
            yield conn

    async def execute(self, query: str, *args):
        """Execute a query."""
        async with self.connection() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        """Fetch multiple rows."""
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Fetch a single row."""
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        """Fetch a single value."""
        async with self.connection() as conn:
            return await conn.fetchval(query, *args)


# Global database instance
db = Database()


async def get_db_pool() -> asyncpg.Pool:
    """
    Get the database connection pool.

    This is used by service modules that need direct pool access.
    """
    if db.pool is None:
        await db.connect()
    return db.pool


async def get_percentiles(pair: str, session_name: str, model: str = "claude_haiku_45") -> Optional[dict]:
    """
    Get cached percentiles from materialized view.

    Args:
        pair: Currency pair
        session_name: Session name
        model: AI model key (default: claude_haiku_45)

    Returns:
        {
            'mfe_p25': float, 'mfe_p50': float, 'mfe_p75': float,
            'mae_p25': float, 'mae_p50': float, 'mae_p75': float,
            'sample_count': int, 'accuracy_pct': float, 'model': str
        }
    """
    row = await db.fetchrow(
        """
        SELECT
            mfe_p25, mfe_p50, mfe_p75,
            mae_p25, mae_p50, mae_p75,
            sample_count, accuracy_pct, model
        FROM percentile_targets
        WHERE pair = $1 AND session_name = $2 AND model = $3
        """,
        pair,
        session_name,
        model,
    )

    if row is None:
        return None

    return dict(row)


async def insert_trade(trade_data: dict) -> str:
    """Insert a new trade and return the trade_id."""
    row = await db.fetchrow(
        """
        INSERT INTO trades (
            pair, session_name, session_datetime,
            prediction, conviction, full_analysis,
            entry_price, spread_pips,
            stop_loss, take_profit, sl_pips, tp_pips,
            lot_size, risk_pct, mfe_percentile, mae_percentile
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )
        RETURNING trade_id
        """,
        trade_data["pair"],
        trade_data["session_name"],
        trade_data["session_datetime"],
        trade_data["prediction"],
        trade_data["conviction"],
        trade_data.get("full_analysis"),  # Full Claude analysis text
        trade_data["entry_price"],
        trade_data["spread_pips"],
        trade_data["stop_loss"],
        trade_data["take_profit"],
        trade_data["sl_pips"],
        trade_data["tp_pips"],
        trade_data["lot_size"],
        trade_data["risk_pct"],
        trade_data["mfe_percentile"],
        trade_data["mae_percentile"],
    )
    return str(row["trade_id"])


async def verify_trade(
    trade_id: str,
    exit_price: float,
    outcome: str,
    pnl_pips: float,
    pnl_dollars: float,
    commission: float,
):
    """Update trade with verification results."""
    await db.execute(
        """
        UPDATE trades
        SET exit_price = $2,
            outcome = $3,
            pnl_pips = $4,
            pnl_dollars = $5,
            commission = $6,
            verified_at = NOW()
        WHERE trade_id = $1
        """,
        trade_id,
        exit_price,
        outcome,
        pnl_pips,
        pnl_dollars,
        commission,
    )


async def update_rolling_window(prediction_data: dict):
    """Add a verified prediction to the rolling window."""
    await db.execute(
        """
        INSERT INTO rolling_window (
            pair, session_name, session_datetime,
            prediction, correct, mfe_pips, mae_pips,
            mfe_first, time_to_mfe_minutes, time_to_mae_minutes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (pair, session_name, session_datetime) DO UPDATE SET
            correct = EXCLUDED.correct,
            mfe_pips = EXCLUDED.mfe_pips,
            mae_pips = EXCLUDED.mae_pips
        """,
        prediction_data["pair"],
        prediction_data["session_name"],
        prediction_data["session_datetime"],
        prediction_data["prediction"],
        prediction_data["correct"],
        prediction_data["mfe_pips"],
        prediction_data["mae_pips"],
        prediction_data.get("mfe_first"),
        prediction_data.get("time_to_mfe_minutes"),
        prediction_data.get("time_to_mae_minutes"),
    )


async def prune_old_rolling_window():
    """Mark predictions older than 6 months as excluded from rolling window."""
    result = await db.execute(
        """
        UPDATE rolling_window
        SET in_window = FALSE
        WHERE in_window = TRUE
          AND session_datetime < NOW() - INTERVAL '6 months'
        """
    )
    logger.info(f"Marked old rolling window data as excluded: {result}")


async def refresh_percentiles():
    """Refresh the materialized view (call after updating rolling window)."""
    await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY percentile_targets")
    logger.info("Refreshed percentile_targets materialized view")


async def get_account_balance() -> float:
    """Get current account balance."""
    result = await db.fetchval("SELECT balance FROM account WHERE id = 1")
    return float(result) if result else float(settings.starting_balance)


async def update_account_balance(new_balance: float, pnl: float, is_win: bool):
    """Update account balance after a trade."""
    await db.execute(
        """
        UPDATE account SET
            balance = $1,
            total_trades = total_trades + 1,
            winning_trades = winning_trades + CASE WHEN $3 THEN 1 ELSE 0 END,
            losing_trades = losing_trades + CASE WHEN $3 THEN 0 ELSE 1 END,
            peak_balance = GREATEST(peak_balance, $1),
            max_drawdown_pct = GREATEST(
                max_drawdown_pct,
                CASE WHEN peak_balance > 0
                    THEN ((peak_balance - $1) / peak_balance) * 100
                    ELSE 0
                END
            ),
            last_updated = NOW()
        WHERE id = 1
        """,
        new_balance,
        pnl,
        is_win,
    )
