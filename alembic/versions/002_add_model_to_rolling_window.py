"""Add model column to rolling_window table

Revision ID: 002
Revises: 001
Create Date: 2025-12-29

Each AI model has different accuracy levels, so percentile targets
should be calculated per-model for more accurate TP/SL levels.
"""

from alembic import op


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add model column to rolling_window table
    op.execute("""
        ALTER TABLE rolling_window
        ADD COLUMN model VARCHAR(50) NOT NULL DEFAULT 'claude_haiku_45'
    """)

    # Drop old unique constraint and create new one including model
    op.execute("""
        ALTER TABLE rolling_window
        DROP CONSTRAINT IF EXISTS rolling_window_pair_session_name_session_datetime_key
    """)

    op.execute("""
        ALTER TABLE rolling_window
        ADD CONSTRAINT rolling_window_unique
        UNIQUE (pair, session_name, session_datetime, model)
    """)

    # Drop old index and create new one including model
    op.execute("DROP INDEX IF EXISTS idx_rolling_lookup")
    op.execute("""
        CREATE INDEX idx_rolling_lookup
        ON rolling_window (pair, session_name, model, session_datetime DESC)
    """)

    # Drop and recreate materialized view to include model grouping
    op.execute("DROP MATERIALIZED VIEW IF EXISTS percentile_targets")

    op.execute("""
        CREATE MATERIALIZED VIEW percentile_targets AS
        SELECT
            pair,
            session_name,
            model,
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
        GROUP BY pair, session_name, model
    """)

    # Create unique index for concurrent refresh
    op.execute("""
        CREATE UNIQUE INDEX idx_percentile_lookup
        ON percentile_targets (pair, session_name, model)
    """)


def downgrade() -> None:
    # Recreate old materialized view without model
    op.execute("DROP MATERIALIZED VIEW IF EXISTS percentile_targets")

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

    # Remove model column and restore old constraints
    op.execute("DROP INDEX IF EXISTS idx_rolling_lookup")
    op.execute("""
        ALTER TABLE rolling_window
        DROP CONSTRAINT IF EXISTS rolling_window_unique
    """)
    op.execute("""
        ALTER TABLE rolling_window
        ADD CONSTRAINT rolling_window_pair_session_name_session_datetime_key
        UNIQUE (pair, session_name, session_datetime)
    """)
    op.execute("""
        CREATE INDEX idx_rolling_lookup
        ON rolling_window (pair, session_name, session_datetime DESC)
    """)
    op.execute("ALTER TABLE rolling_window DROP COLUMN model")
