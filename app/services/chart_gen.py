"""
Chart Generation Service for Live Trading
==========================================

Optimized for low-latency chart generation at session open.
Generates a single chart for a specific pair/session combination.

Key differences from backtester:
- No S3 storage (local only for speed)
- No OHLC caching (uses pre-warmed data)
- Single chart generation (not batch)
- Simplified error handling
"""

import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

from ..utils.session_utils import get_session_times_for_date
from ..utils.polygon_client import fetch_ohlc_data_async
from ..config import settings, CHARTS_DIR
from .storage import upload_chart_to_s3_async, get_chart_https_url

# Chart configuration
SWING_PROMINENCE = 0.002
FVG_MIN_SIZE_FACTOR = 0.3
LOOKBACK_DAYS = 4


def calculate_ema(series: pd.Series, window: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=window, adjust=False).mean()


def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high_low = high - low
    high_close_prev = (high - close.shift()).abs()
    low_close_prev = (low - close.shift()).abs()
    tr = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
    return tr.rolling(window=window).mean()


def find_unfilled_gaps(df: pd.DataFrame, min_size_factor: float, avg_atr: float) -> list:
    """Find Fair Value Gaps (FVGs) with timestamps."""
    min_size = min_size_factor * avg_atr
    if pd.isna(min_size) or min_size == 0:
        return []

    prev_high = df['high'].shift(2)
    prev_low = df['low'].shift(2)
    next_high = df['high']
    next_low = df['low']

    # Bullish FVGs
    bullish_gap_bottom = prev_high
    bullish_gap_top = next_low
    bullish_gap_size = bullish_gap_top - bullish_gap_bottom
    is_bullish_gap = bullish_gap_size >= min_size

    # Bearish FVGs
    bearish_gap_top = prev_low
    bearish_gap_bottom = next_high
    bearish_gap_size = bearish_gap_top - bearish_gap_bottom
    is_bearish_gap = bearish_gap_size >= min_size

    # Filter to only unfilled gaps
    future_min_low = df['low'].iloc[::-1].expanding().min().iloc[::-1].shift(-1)
    future_max_high = df['high'].iloc[::-1].expanding().max().iloc[::-1].shift(-1)
    is_bullish_gap = is_bullish_gap & (future_min_low > bullish_gap_bottom)
    is_bearish_gap = is_bearish_gap & (future_max_high < bearish_gap_top)

    gaps_list = []

    for i in df.index[is_bullish_gap]:
        gaps_list.append({
            'type': 'bullish',
            'range': (bullish_gap_bottom.at[i], bullish_gap_top.at[i]),
            'size': bullish_gap_size.at[i],
            'timestamp': df['timestamp'].at[i]
        })

    for i in df.index[is_bearish_gap]:
        gaps_list.append({
            'type': 'bearish',
            'range': (bearish_gap_bottom.at[i], bearish_gap_top.at[i]),
            'size': bearish_gap_size.at[i],
            'timestamp': df['timestamp'].at[i]
        })

    return gaps_list


async def fetch_ohlc_for_chart(pair: str, session_dt: datetime) -> Optional[pd.DataFrame]:
    """
    Fetch OHLC data for chart generation.

    Gets 7 days of data to ensure sufficient history for indicators.

    Args:
        pair: Currency pair (e.g., 'EURUSD')
        session_dt: Session datetime (UTC)

    Returns:
        DataFrame with OHLC data or None on error
    """
    start_date = session_dt - timedelta(days=7)
    end_date = session_dt

    try:
        df = await fetch_ohlc_data_async(
            pair=pair,
            start_date=start_date,
            end_date=end_date,
            timeframe="15/minute",
            api_key=settings.polygon_api_key
        )
        return df
    except Exception as e:
        print(f"Error fetching OHLC for {pair}: {e}")
        return None


def generate_chart(
    df: pd.DataFrame,
    pair: str,
    session_name: str,
    session_dt: datetime,
    output_dir: Path
) -> Optional[str]:
    """
    Generate a chart snapshot for a specific session.

    Args:
        df: DataFrame with OHLC data
        pair: Currency pair
        session_name: Session name (e.g., 'London_Open')
        session_dt: Session datetime (UTC)
        output_dir: Directory to save chart

    Returns:
        Path to saved chart or None on error
    """
    try:
        # Filter data: 4-day lookback ending at session time
        lookback_start = session_dt - timedelta(days=LOOKBACK_DAYS)

        # Handle timezone
        if df['timestamp'].dt.tz is not None and session_dt.tzinfo is None:
            session_dt = session_dt.replace(tzinfo=timezone.utc)
            lookback_start = lookback_start.replace(tzinfo=timezone.utc)

        df_snapshot = df[
            (df['timestamp'] >= lookback_start) &
            (df['timestamp'] <= session_dt)
        ].copy()

        if df_snapshot.empty or len(df_snapshot) < 50:
            print(f"Not enough data for {pair} {session_name}: {len(df_snapshot)} candles")
            return None

        # Calculate indicators
        df_snapshot['ema_20'] = calculate_ema(df_snapshot['close'], 20)
        df_snapshot['ema_50'] = calculate_ema(df_snapshot['close'], 50)
        df_snapshot['atr'] = calculate_atr(df_snapshot['high'], df_snapshot['low'], df_snapshot['close'], 14)

        # Find FVGs
        avg_atr = df_snapshot['atr'].mean()
        gaps = find_unfilled_gaps(df_snapshot, FVG_MIN_SIZE_FACTOR, avg_atr)
        gaps_sorted = sorted(gaps, key=lambda x: x['timestamp'])

        # Calculate session highs/lows
        session_times = get_session_times_for_date(session_dt)
        session_stats = _calculate_session_stats(df_snapshot, session_dt, session_times)

        # Create chart
        fig, ax = plt.subplots(figsize=(18, 10))

        # Session backgrounds
        _draw_session_backgrounds(ax, df_snapshot, session_dt, session_times)

        # Price and indicators
        ax.plot(df_snapshot['timestamp'], df_snapshot['close'], label='Close', alpha=0.8, linewidth=1)
        ax.plot(df_snapshot['timestamp'], df_snapshot['ema_20'], label='EMA20', linestyle=':', linewidth=1)
        ax.plot(df_snapshot['timestamp'], df_snapshot['ema_50'], label='EMA50', linestyle=':', linewidth=1)

        # Swing points
        high_peaks = find_peaks(df_snapshot['high'].values, prominence=SWING_PROMINENCE)[0]
        low_peaks = find_peaks(-df_snapshot['low'].values, prominence=SWING_PROMINENCE)[0]

        ax.scatter(df_snapshot['timestamp'].iloc[high_peaks], df_snapshot['high'].iloc[high_peaks],
                   c='g', marker='^', s=50, label='Swing Highs')
        ax.scatter(df_snapshot['timestamp'].iloc[low_peaks], df_snapshot['low'].iloc[low_peaks],
                   c='r', marker='v', s=50, label='Swing Lows')

        # Session highs/lows lines
        _draw_session_levels(ax, session_stats)

        # FVGs
        _draw_fvgs(ax, gaps_sorted, df_snapshot)

        # Legend
        session_bg_handles = [
            Patch(facecolor='navajowhite', alpha=0.3, label='Asian Session'),
            Patch(facecolor='powderblue', alpha=0.3, label='London Session'),
            Patch(facecolor='plum', alpha=0.3, label='NY Session'),
        ]
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles=handles + session_bg_handles,
            labels=labels + [h.get_label() for h in session_bg_handles],
            bbox_to_anchor=(1.005, 1), loc='upper left', fontsize=10, framealpha=0.95
        )

        ax.grid(True, which='both', linestyle='--', linewidth=0.5)

        # X-axis padding
        x_min = df_snapshot['timestamp'].min()
        x_max = df_snapshot['timestamp'].max()
        ax.set_xlim(x_min, x_max + pd.Timedelta(hours=2))

        # Title
        display_name = session_name.replace('_', ' ')
        title = f"{pair} - Snapshot at {display_name} ({session_dt.strftime('%Y-%m-%d %H:%M UTC')})"
        ax.set_title(title)

        plt.subplots_adjust(top=0.92, right=0.96)
        fig.tight_layout(rect=[0, 0, 0.96, 0.96])

        # Save chart
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{pair}_{session_dt.strftime('%Y%m%d_%H%M')}_{session_name}.png"
        filepath = output_dir / pair / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        fig.savefig(filepath, format='png', dpi=100)
        plt.close(fig)

        return str(filepath)

    except Exception as e:
        print(f"Error creating chart for {pair} {session_name}: {e}")
        if 'fig' in locals():
            plt.close(fig)
        return None


def _calculate_session_stats(
    df: pd.DataFrame,
    session_dt: datetime,
    session_times: dict
) -> dict:
    """Calculate session highs/lows for chart annotations."""
    stats = {}
    today_start = session_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # Find previous trading day
    prev_trading_day = None
    for days_back in range(1, 8):
        check_date = today_start - timedelta(days=days_back)
        if check_date.weekday() not in [5, 6]:  # Skip weekend
            prev_trading_day = check_date
            break

    if prev_trading_day is None:
        prev_trading_day = today_start - timedelta(days=1)

    # Determine which sessions to show based on time
    current_hour = session_dt.hour + (session_dt.minute / 60.0)
    ny_end = session_times['NY_Close']['hour'] + (session_times['NY_Close']['minute'] / 60.0)
    london_end = session_times['London_Close']['hour'] + (session_times['London_Close']['minute'] / 60.0)

    if current_hour >= ny_end:
        last_sessions = ['NY', 'London']
    elif current_hour >= london_end:
        last_sessions = ['London', 'Asian']
    else:
        last_sessions = ['Asian', 'NY (Prev)']

    # Calculate highs/lows for each session
    for idx, sess_name in enumerate(last_sessions):
        if '(Prev)' in sess_name:
            base_date = prev_trading_day
            sess_name_clean = sess_name.replace(' (Prev)', '')
            sess_times = get_session_times_for_date(prev_trading_day)
        else:
            base_date = today_start
            sess_name_clean = sess_name
            sess_times = session_times

        open_info = sess_times[f'{sess_name_clean}_Open']
        close_info = sess_times[f'{sess_name_clean}_Close']

        start_time = base_date + timedelta(hours=open_info['hour'], minutes=open_info['minute'])
        end_time = base_date + timedelta(hours=close_info['hour'], minutes=close_info['minute'])

        session_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] < end_time)]

        key = f'session{idx + 1}'
        stats[f'{key}_name'] = sess_name
        if not session_df.empty:
            stats[f'{key}_high'] = session_df['high'].max()
            stats[f'{key}_low'] = session_df['low'].min()
        else:
            stats[f'{key}_high'] = np.nan
            stats[f'{key}_low'] = np.nan

    # Previous day high/low
    prev_day_df = df[
        (df['timestamp'] >= prev_trading_day) &
        (df['timestamp'] < prev_trading_day + timedelta(days=1))
    ]
    if not prev_day_df.empty:
        stats['prev_day_high'] = prev_day_df['high'].max()
        stats['prev_day_low'] = prev_day_df['low'].min()
    else:
        stats['prev_day_high'] = np.nan
        stats['prev_day_low'] = np.nan

    return stats


def _draw_session_backgrounds(
    ax: plt.Axes,
    df: pd.DataFrame,
    session_dt: datetime,
    session_times: dict
) -> None:
    """Draw session background colors and labels."""
    session_bg_colors = {'Asian': 'navajowhite', 'London': 'powderblue', 'NY': 'plum'}
    session_colors = {'Asian': 'black', 'London': 'chocolate', 'NY': 'olive'}

    y_min, y_max = df['close'].min(), df['close'].max()
    y_range = y_max - y_min
    label_y = y_max - (y_range * 0.01)

    today_start = session_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    for day_offset in range(-5, 1):
        base_date = today_start + timedelta(days=day_offset)
        day_session_times = get_session_times_for_date(base_date)

        for session_name in ['Asian', 'London', 'NY']:
            open_info = day_session_times[f'{session_name}_Open']
            close_info = day_session_times[f'{session_name}_Close']

            session_start = base_date + timedelta(hours=open_info['hour'], minutes=open_info['minute'])
            session_end = base_date + timedelta(hours=close_info['hour'], minutes=close_info['minute'])

            if session_start >= df['timestamp'].min() and session_start <= session_dt:
                ax.axvspan(session_start, min(session_end, session_dt),
                           alpha=0.3, color=session_bg_colors[session_name])
                ax.text(session_start, label_y, f'{session_name}\nStart',
                        horizontalalignment='center', verticalalignment='top',
                        fontsize=8, fontweight='bold', color=session_colors[session_name],
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                                  edgecolor=session_colors[session_name], alpha=0.9))


def _draw_session_levels(ax: plt.Axes, session_stats: dict) -> None:
    """Draw session high/low horizontal lines."""
    colors = ['blue', 'purple']
    styles = ['--', '-.']

    for idx, (color, ls) in enumerate(zip(colors, styles), 1):
        high = session_stats.get(f'session{idx}_high', np.nan)
        low = session_stats.get(f'session{idx}_low', np.nan)
        name = session_stats.get(f'session{idx}_name', 'Previous')

        if not np.isnan(high):
            ax.axhline(high, c=color, ls=ls, label=f"{name} High")
        if not np.isnan(low):
            ax.axhline(low, c=color, ls=ls, label=f"{name} Low")

    # Previous day
    if not np.isnan(session_stats.get('prev_day_high', np.nan)):
        ax.axhline(session_stats['prev_day_high'], c='orange', ls='-', label="Prev Day High")
    if not np.isnan(session_stats.get('prev_day_low', np.nan)):
        ax.axhline(session_stats['prev_day_low'], c='orange', ls='-', label="Prev Day Low")


def _draw_fvgs(ax: plt.Axes, gaps: list, df: pd.DataFrame) -> None:
    """Draw Fair Value Gaps on chart."""
    fvg_bull_labeled = False
    fvg_bear_labeled = False

    for num, g in enumerate(gaps, 1):
        label = None
        if g['type'] == 'bullish':
            color = 'green'
            if not fvg_bull_labeled:
                label = 'Bullish FVG'
                fvg_bull_labeled = True
        else:
            color = 'red'
            if not fvg_bear_labeled:
                label = 'Bearish FVG'
                fvg_bear_labeled = True

        ax.axhspan(g['range'][0], g['range'][1], alpha=0.2, color=color, label=label)

        fvg_mid = (g['range'][0] + g['range'][1]) / 2
        date_str = g['timestamp'].strftime('%m/%d')
        time_str = g['timestamp'].strftime('%H:%M')

        ax.text(
            df['timestamp'].iloc[0], fvg_mid,
            f' FVG #{num}\n L: {g["range"][0]:.5f}  U: {g["range"][1]:.5f}\n {date_str} {time_str}',
            verticalalignment='center', horizontalalignment='left',
            fontsize=9, fontweight='bold', color=color,
            bbox=dict(boxstyle='round,pad=0.6', facecolor='white', edgecolor=color, alpha=0.85)
        )


async def generate_session_chart(
    pair: str,
    session_name: str,
    session_dt: datetime,
    ohlc_df: Optional[pd.DataFrame] = None,
    upload_to_s3: bool = True,
    delete_local_after_upload: bool = False
) -> Dict[str, Any]:
    """
    High-level function to generate a chart for a session.

    This is the main entry point for live chart generation.
    Optionally accepts pre-fetched OHLC data for pre-warming.

    Args:
        pair: Currency pair (e.g., 'EURUSD')
        session_name: Session name (e.g., 'London_Open')
        session_dt: Session datetime (UTC)
        ohlc_df: Pre-fetched OHLC data (optional, for pre-warming)
        upload_to_s3: Whether to upload chart to S3 (default True)
        delete_local_after_upload: Whether to delete local file after S3 upload

    Returns:
        Dict with keys:
            - local_path: Path to local chart file (or None)
            - s3_url: S3 URL if uploaded (or None)
            - https_url: CloudFront HTTPS URL if uploaded (or None)
            - success: Whether chart was generated successfully
    """
    result = {
        "local_path": None,
        "s3_url": None,
        "https_url": None,
        "success": False
    }

    # Fetch OHLC if not provided
    if ohlc_df is None:
        ohlc_df = await fetch_ohlc_for_chart(pair, session_dt)

    if ohlc_df is None or ohlc_df.empty:
        print(f"No OHLC data available for {pair}")
        return result

    # Generate chart
    local_path = generate_chart(ohlc_df, pair, session_name, session_dt, CHARTS_DIR)

    if local_path is None:
        return result

    result["local_path"] = local_path
    result["success"] = True

    # Upload to S3
    if upload_to_s3:
        s3_url = await upload_chart_to_s3_async(
            local_path,
            pair,
            delete_local=delete_local_after_upload
        )
        if s3_url:
            result["s3_url"] = s3_url
            # Generate CloudFront URL
            filename = Path(local_path).name
            result["https_url"] = get_chart_https_url(pair, filename)

            if delete_local_after_upload:
                result["local_path"] = None  # Local file was deleted

    return result
