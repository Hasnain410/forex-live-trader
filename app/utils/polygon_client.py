"""
Polygon API Client - Async OHLC Data Fetching
==============================================

Single source of truth for all Polygon.io API calls.
Optimized for low-latency live trading service.

Features:
- Async support via httpx
- Global rate limiting (thread-safe)
- Pagination support for large date ranges
- Retry logic with exponential backoff
- Proper 429 rate limit handling
"""

import asyncio
import os
import time
import threading
from datetime import datetime
from typing import Optional

import httpx
import pandas as pd
import requests

from .forex_utils import get_pip_value

# ============================================================================
# CONFIGURATION
# ============================================================================

# Rate limiting - Polygon Premium allows high throughput, but be conservative
MIN_API_DELAY_SECONDS = 0.05  # 50ms = ~20 requests/sec max
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3

# Global rate limiter (thread-safe across all callers)
_polygon_api_lock = threading.Lock()
_last_polygon_call_time = 0.0


def get_api_key() -> str:
    """Get Polygon API key from environment."""
    return os.getenv("POLYGON_API_KEY", "")


# ============================================================================
# MAIN FETCH FUNCTION
# ============================================================================

def fetch_ohlc_data(
    pair: str,
    start_date: datetime,
    end_date: datetime,
    api_key: str = None,
    timeframe: str = "15/minute",
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    verbose: bool = True
) -> Optional[pd.DataFrame]:
    """
    Fetch OHLC data from Polygon.io with pagination, rate limiting, and retries.

    This is the SINGLE SOURCE OF TRUTH for Polygon API calls.
    All other modules should use this function.

    Args:
        pair: Currency pair (e.g., 'EURUSD', 'GBPUSD')
        start_date: Start datetime (timezone-aware or naive, treated as UTC)
        end_date: End datetime
        api_key: Polygon API key (uses env var if not provided)
        timeframe: Candle timeframe (default: "15/minute" for 15-min candles)
        max_retries: Max retry attempts per request
        timeout: Request timeout in seconds
        verbose: Print progress messages

    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume
        Returns None if all retries fail or no data available
    """
    global _last_polygon_call_time

    if not api_key:
        api_key = get_api_key()

    if not api_key:
        if verbose:
            print("  ❌ No Polygon API key provided")
        return None

    from_date_str = start_date.strftime('%Y-%m-%d')
    to_date_str = end_date.strftime('%Y-%m-%d')

    base_url = (
        f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/{timeframe}/"
        f"{from_date_str}/{to_date_str}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    )

    all_results = []
    url = base_url
    page = 1

    while url:
        success = False

        for attempt in range(max_retries):
            try:
                # Thread-safe rate limiting
                with _polygon_api_lock:
                    elapsed = time.time() - _last_polygon_call_time
                    if elapsed < MIN_API_DELAY_SECONDS:
                        time.sleep(MIN_API_DELAY_SECONDS - elapsed)

                    if verbose:
                        if page == 1 and attempt == 0:
                            print(f"  📡 Fetching {pair}: {from_date_str} to {to_date_str}")
                        elif page > 1 and attempt == 0:
                            print(f"  📡 Fetching {pair} page {page}...")
                        elif attempt > 0:
                            print(f"  🔄 Retry {attempt + 1}/{max_retries}...")

                    response = requests.get(url, timeout=timeout)
                    _last_polygon_call_time = time.time()

                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    if verbose:
                        print(f"  ⏳ Rate limited! Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                # Handle other HTTP errors
                if response.status_code != 200:
                    if verbose:
                        print(f"  ⚠️  HTTP {response.status_code}: {response.text[:200]}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return None

                data = response.json()

                # Check for API-level errors
                if data.get('status') == 'ERROR':
                    error_msg = data.get('error', 'Unknown API error')
                    if verbose:
                        print(f"  ❌ API Error: {error_msg}")
                    return None

                # Check for empty results
                if 'results' not in data or not data['results']:
                    if page == 1:
                        if verbose:
                            print(f"  ⚠️  No data returned for {pair}")
                        return None
                    else:
                        # No more pages, we're done
                        url = None
                        success = True
                        break

                # Accumulate results
                all_results.extend(data['results'])
                success = True

                # Check for next page (pagination)
                next_url = data.get('next_url')
                if next_url:
                    if 'apiKey=' not in next_url:
                        next_url = f"{next_url}&apiKey={api_key}"
                    url = next_url
                    page += 1
                else:
                    url = None

                break  # Success, exit retry loop

            except requests.Timeout:
                if verbose:
                    print(f"  ⏱️  Timeout on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

            except requests.RequestException as e:
                if verbose:
                    print(f"  ❌ Request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

            except Exception as e:
                if verbose:
                    print(f"  ❌ Unexpected error: {e}")
                return None

        if not success and url:
            # All retries failed for this page
            if verbose:
                print(f"  ❌ Failed after {max_retries} attempts")
            return None

    if not all_results:
        return None

    # Convert to DataFrame
    df = pd.DataFrame(all_results)
    df = df.rename(columns={
        't': 'timestamp',
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume'
    })

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)

    if verbose and page > 1:
        print(f"  ✓ Fetched {len(df)} candles across {page} pages")

    return df


def fetch_ohlc_for_session(
    pair: str,
    session_start: datetime,
    session_end: datetime,
    api_key: str = None,
    verbose: bool = True
) -> Optional[pd.DataFrame]:
    """
    Fetch OHLC data for a specific session time window.

    Convenience wrapper that filters the data to the exact session window.

    Args:
        pair: Currency pair
        session_start: Session start datetime (UTC)
        session_end: Session end datetime (UTC)
        api_key: Polygon API key
        verbose: Print progress

    Returns:
        DataFrame filtered to session window, or None
    """
    # Fetch the day's data
    df = fetch_ohlc_data(
        pair=pair,
        start_date=session_start,
        end_date=session_end,
        api_key=api_key,
        verbose=verbose
    )

    if df is None or df.empty:
        return None

    # Filter to exact session window
    df = df[(df['timestamp'] >= session_start) & (df['timestamp'] < session_end)]

    if df.empty:
        if verbose:
            print(f"  ⚠️  No candles in session window")
        return None

    return df.reset_index(drop=True)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

# Note: get_pip_value() is imported from forex_utils.py (single source of truth)

def df_to_candles_list(df: pd.DataFrame) -> list:
    """
    Convert DataFrame to list of candle dicts for storage.

    Args:
        df: DataFrame with timestamp, open, high, low, close, volume

    Returns:
        List of candle dicts
    """
    candles = []
    for _, row in df.iterrows():
        candles.append({
            'timestamp': row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row.get('volume', 0))
        })
    return candles


# ============================================================================
# ASYNC FETCH FUNCTION
# ============================================================================

async def fetch_ohlc_data_async(
    pair: str,
    start_date: datetime,
    end_date: datetime,
    api_key: str = None,
    timeframe: str = "15/minute",
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    verbose: bool = False
) -> Optional[pd.DataFrame]:
    """
    Async version of fetch_ohlc_data using httpx.

    Optimized for low-latency live trading service.

    Args:
        pair: Currency pair (e.g., 'EURUSD')
        start_date: Start datetime
        end_date: End datetime
        api_key: Polygon API key
        timeframe: Candle timeframe
        max_retries: Max retry attempts
        timeout: Request timeout
        verbose: Print progress

    Returns:
        DataFrame with OHLC data or None
    """
    global _last_polygon_call_time

    if not api_key:
        api_key = get_api_key()

    if not api_key:
        if verbose:
            print("  ❌ No Polygon API key provided")
        return None

    from_date_str = start_date.strftime('%Y-%m-%d')
    to_date_str = end_date.strftime('%Y-%m-%d')

    base_url = (
        f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/range/{timeframe}/"
        f"{from_date_str}/{to_date_str}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    )

    all_results = []
    url = base_url
    page = 1

    async with httpx.AsyncClient(timeout=timeout) as client:
        while url:
            success = False

            for attempt in range(max_retries):
                try:
                    # Rate limiting
                    with _polygon_api_lock:
                        elapsed = time.time() - _last_polygon_call_time
                        if elapsed < MIN_API_DELAY_SECONDS:
                            await asyncio.sleep(MIN_API_DELAY_SECONDS - elapsed)

                    if verbose:
                        if page == 1 and attempt == 0:
                            print(f"  📡 Fetching {pair}: {from_date_str} to {to_date_str}")

                    response = await client.get(url)
                    _last_polygon_call_time = time.time()

                    # Handle rate limiting
                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        if verbose:
                            print(f"  ⏳ Rate limited! Waiting {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status_code != 200:
                        if verbose:
                            print(f"  ⚠️  HTTP {response.status_code}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return None

                    data = response.json()

                    if data.get('status') == 'ERROR':
                        if verbose:
                            print(f"  ❌ API Error: {data.get('error', 'Unknown')}")
                        return None

                    if 'results' not in data or not data['results']:
                        if page == 1:
                            if verbose:
                                print(f"  ⚠️  No data for {pair}")
                            return None
                        else:
                            url = None
                            success = True
                            break

                    all_results.extend(data['results'])
                    success = True

                    next_url = data.get('next_url')
                    if next_url:
                        if 'apiKey=' not in next_url:
                            next_url = f"{next_url}&apiKey={api_key}"
                        url = next_url
                        page += 1
                    else:
                        url = None

                    break

                except httpx.TimeoutException:
                    if verbose:
                        print(f"  ⏱️  Timeout attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue

                except Exception as e:
                    if verbose:
                        print(f"  ❌ Error: {e}")
                    return None

            if not success and url:
                return None

    if not all_results:
        return None

    df = pd.DataFrame(all_results)
    df = df.rename(columns={
        't': 'timestamp',
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume'
    })
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    from datetime import timedelta

    print("Testing Polygon Client...")

    # Test sync fetch
    end = datetime.now()
    start = end - timedelta(days=1)

    df = fetch_ohlc_data("EURUSD", start, end)

    if df is not None:
        print(f"\n✓ Fetched {len(df)} candles")
        print(df.head())
    else:
        print("\n✗ No data returned (check API key)")
