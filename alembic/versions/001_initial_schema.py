"""Initial schema with all tables and materialized view.

Revision ID: 001
Create Date: 2025-12-28
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Account table
    op.execute("""
        CREATE TABLE account (
            id SERIAL PRIMARY KEY,
            balance DECIMAL(12, 2) NOT NULL DEFAULT 10000.00,
            initial_balance DECIMAL(12, 2) NOT NULL DEFAULT 10000.00,
            total_trades INTEGER DEFAULT 0,
            winning_trades INTEGER DEFAULT 0,
            losing_trades INTEGER DEFAULT 0,
            peak_balance DECIMAL(12, 2) DEFAULT 10000.00,
            max_drawdown_pct DECIMAL(5, 2) DEFAULT 0.00,
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # Insert default account
    op.execute("""
        INSERT INTO account (balance, initial_balance, peak_balance)
        VALUES (10000.00, 10000.00, 10000.00)
    """)

    # Trades table
    op.execute("""
        CREATE TABLE trades (
            id SERIAL PRIMARY KEY,
            trade_id UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
            pair VARCHAR(10) NOT NULL,
            session_name VARCHAR(20) NOT NULL,
            session_datetime TIMESTAMP WITH TIME ZONE NOT NULL,

            -- Prediction
            prediction VARCHAR(10) NOT NULL CHECK (prediction IN ('BULLISH', 'BEARISH')),
            conviction INTEGER CHECK (conviction BETWEEN 1 AND 10),

            -- Entry
            entry_price DECIMAL(12, 6) NOT NULL,
            spread_pips DECIMAL(4, 1) NOT NULL,

            -- Risk management
            stop_loss DECIMAL(12, 6) NOT NULL,
            take_profit DECIMAL(12, 6) NOT NULL,
            sl_pips DECIMAL(6, 1) NOT NULL,
            tp_pips DECIMAL(6, 1) NOT NULL,
            lot_size DECIMAL(4, 2) NOT NULL,
            risk_pct DECIMAL(4, 2) NOT NULL,
            mfe_percentile VARCHAR(3) NOT NULL,
            mae_percentile VARCHAR(3) NOT NULL,

            -- Outcome (filled on verification)
            exit_price DECIMAL(12, 6),
            outcome VARCHAR(10) CHECK (outcome IN ('WIN', 'LOSS', 'BREAKEVEN', 'TIMEOUT')),
            pnl_pips DECIMAL(8, 1),
            pnl_dollars DECIMAL(10, 2),
            commission DECIMAL(8, 2),

            -- Timestamps
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            verified_at TIMESTAMP WITH TIME ZONE
        )
    """)

    op.execute("CREATE INDEX idx_trades_pair_session ON trades (pair, session_name)")
    op.execute("CREATE INDEX idx_trades_datetime ON trades (session_datetime DESC)")
    op.execute("CREATE INDEX idx_trades_outcome ON trades (outcome)")

    # Rolling window table
    op.execute("""
        CREATE TABLE rolling_window (
            id SERIAL PRIMARY KEY,
            pair VARCHAR(10) NOT NULL,
            session_name VARCHAR(20) NOT NULL,
            session_datetime TIMESTAMP WITH TIME ZONE NOT NULL,

            -- Prediction outcome
            prediction VARCHAR(10) NOT NULL CHECK (prediction IN ('BULLISH', 'BEARISH')),
            correct BOOLEAN NOT NULL,

            -- Excursion data (in pips)
            mfe_pips DECIMAL(6, 1) NOT NULL,
            mae_pips DECIMAL(6, 1) NOT NULL,

            -- Timing (for advanced analysis)
            mfe_first BOOLEAN,
            time_to_mfe_minutes INTEGER,
            time_to_mae_minutes INTEGER,

            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

            UNIQUE(pair, session_name, session_datetime)
        )
    """)

    op.execute(
        "CREATE INDEX idx_rolling_lookup ON rolling_window (pair, session_name, session_datetime DESC)"
    )

    # Materialized view for percentiles
    op.execute("""
        CREATE MATERIALIZED VIEW percentile_targets AS
        SELECT
            pair,
            session_name,
            COUNT(*) as sample_count,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mfe_pips) as mfe_p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mfe_pips) as mfe_p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mfe_pips) as mfe_p75,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mae_pips) as mae_p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mae_pips) as mae_p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mae_pips) as mae_p75,
            AVG(CASE WHEN correct THEN 1.0 ELSE 0.0 END) * 100 as accuracy_pct,
            NOW() as updated_at
        FROM rolling_window
        WHERE session_datetime >= NOW() - INTERVAL '6 months'
        GROUP BY pair, session_name
    """)

    op.execute(
        "CREATE UNIQUE INDEX idx_percentile_lookup ON percentile_targets (pair, session_name)"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS percentile_targets")
    op.execute("DROP TABLE IF EXISTS rolling_window")
    op.execute("DROP TABLE IF EXISTS trades")
    op.execute("DROP TABLE IF EXISTS account")
