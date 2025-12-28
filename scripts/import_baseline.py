#!/usr/bin/env python3
"""
Import Baseline Data to PostgreSQL
===================================

Imports the baseline parquet file into the rolling_window table
and refreshes the percentile materialized view.

Usage:
    python scripts/import_baseline.py data/baseline.parquet
    python scripts/import_baseline.py data/baseline.parquet --db postgresql://user:pass@localhost/forex
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


async def import_baseline(parquet_path: str, db_url: str) -> dict:
    """
    Import baseline parquet data to PostgreSQL.

    Args:
        parquet_path: Path to baseline parquet file
        db_url: PostgreSQL connection URL

    Returns:
        Import statistics
    """
    print(f"Importing baseline data...")
    print(f"  Source: {parquet_path}")
    print(f"  Database: {db_url.split('@')[-1]}")  # Hide password

    # Read parquet file
    df = pd.read_parquet(parquet_path)
    print(f"  Rows to import: {len(df):,}")

    # Connect to database
    conn = await asyncpg.connect(db_url)

    try:
        # Clear existing data (optional - comment out to append)
        result = await conn.execute("DELETE FROM rolling_window")
        print(f"  Cleared existing data: {result}")

        # Prepare records for bulk insert
        records = []
        for row in df.itertuples():
            records.append((
                row.pair,
                row.session_name,
                row.session_datetime,
                row.prediction,
                row.correct,
                float(row.mfe_pips),
                float(row.mae_pips),
                row.mfe_first if hasattr(row, 'mfe_first') and pd.notna(row.mfe_first) else None,
                int(row.time_to_mfe_minutes) if hasattr(row, 'time_to_mfe_minutes') and pd.notna(row.time_to_mfe_minutes) else None,
                int(row.time_to_mae_minutes) if hasattr(row, 'time_to_mae_minutes') and pd.notna(row.time_to_mae_minutes) else None,
            ))

        # Bulk insert using COPY
        await conn.copy_records_to_table(
            'rolling_window',
            records=records,
            columns=[
                'pair', 'session_name', 'session_datetime',
                'prediction', 'correct', 'mfe_pips', 'mae_pips',
                'mfe_first', 'time_to_mfe_minutes', 'time_to_mae_minutes'
            ]
        )
        print(f"  Inserted: {len(records):,} records")

        # Refresh materialized view
        print("  Refreshing percentile_targets view...")
        await conn.execute("REFRESH MATERIALIZED VIEW percentile_targets")

        # Get percentile stats
        stats = await conn.fetch("""
            SELECT pair, session_name, sample_count, accuracy_pct,
                   mfe_p50, mae_p50
            FROM percentile_targets
            ORDER BY pair, session_name
        """)

        print(f"\n  Percentile targets created: {len(stats)}")
        print(f"  Sample by pair/session:")
        for row in stats[:10]:  # Show first 10
            print(f"    {row['pair']} {row['session_name']}: "
                  f"n={row['sample_count']}, acc={row['accuracy_pct']:.1f}%, "
                  f"MFE_P50={row['mfe_p50']:.1f}, MAE_P50={row['mae_p50']:.1f}")
        if len(stats) > 10:
            print(f"    ... and {len(stats) - 10} more")

        return {
            "rows_imported": len(records),
            "percentile_targets": len(stats),
            "success": True,
        }

    finally:
        await conn.close()


async def main():
    parser = argparse.ArgumentParser(description="Import baseline data to PostgreSQL")
    parser.add_argument("parquet_path", help="Path to baseline parquet file")
    parser.add_argument(
        "--db",
        default=os.getenv("DATABASE_URL", "postgresql://forex_user:password@localhost:5432/forex_trader"),
        help="PostgreSQL connection URL"
    )

    args = parser.parse_args()

    if not Path(args.parquet_path).exists():
        print(f"ERROR: File not found: {args.parquet_path}")
        sys.exit(1)

    result = await import_baseline(args.parquet_path, args.db)

    if result["success"]:
        print(f"\n✅ Import complete!")
        print(f"  Rows imported: {result['rows_imported']:,}")
        print(f"  Percentile targets: {result['percentile_targets']}")
    else:
        print("\n❌ Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
