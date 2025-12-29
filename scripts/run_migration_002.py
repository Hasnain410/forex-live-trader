#!/usr/bin/env python3
"""Run migration 002: Add model column to rolling_window."""

import asyncio
import asyncpg
import os
import sys

# Add parent to path so we can import app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings


async def run_migration():
    db_display = settings.database_url.split("@")[-1]
    print(f"Connecting to: {db_display}")
    conn = await asyncpg.connect(settings.database_url)

    try:
        # Check if model column already exists
        result = await conn.fetchval("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'rolling_window' AND column_name = 'model'
        """)

        if result:
            print("Model column already exists, skipping migration")
            return

        print("Adding model column...")
        await conn.execute("""
            ALTER TABLE rolling_window
            ADD COLUMN model VARCHAR(50) NOT NULL DEFAULT 'claude_haiku_45'
        """)

        print("Dropping old unique constraint...")
        await conn.execute("""
            ALTER TABLE rolling_window
            DROP CONSTRAINT IF EXISTS rolling_window_pair_session_name_session_datetime_key
        """)

        print("Creating new unique constraint with model...")
        await conn.execute("""
            ALTER TABLE rolling_window
            ADD CONSTRAINT rolling_window_unique
            UNIQUE (pair, session_name, session_datetime, model)
        """)

        print("Recreating index...")
        await conn.execute("DROP INDEX IF EXISTS idx_rolling_lookup")
        await conn.execute("""
            CREATE INDEX idx_rolling_lookup
            ON rolling_window (pair, session_name, model, session_datetime DESC)
        """)

        print("Recreating percentile_targets materialized view...")
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS percentile_targets")
        await conn.execute("""
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

        print("Creating percentile_targets index...")
        await conn.execute("""
            CREATE UNIQUE INDEX idx_percentile_lookup
            ON percentile_targets (pair, session_name, model)
        """)

        print("Migration complete!")

        # Show results
        row_count = await conn.fetchval("SELECT COUNT(*) FROM rolling_window")
        print(f"rolling_window rows: {row_count}")

        pct_count = await conn.fetchval("SELECT COUNT(*) FROM percentile_targets")
        print(f"percentile_targets rows: {pct_count}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migration())
