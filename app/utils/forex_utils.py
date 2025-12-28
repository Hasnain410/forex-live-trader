"""
Forex Utilities - Shared forex-specific functions
==================================================

Single source of truth for forex-related calculations and constants.
Used by: backtest_verification.py, web_viewer.py, ohlc_storage.py, polygon_client.py

This module contains:
- Pip value calculations
- Currency pair utilities
- Common forex constants
"""

from typing import List

# ============================================================================
# CURRENCY PAIRS
# ============================================================================

# Standard forex pairs supported by the system
MAJOR_PAIRS: List[str] = [
    'EURUSD',
    'GBPUSD',
    'USDJPY',
    'USDCHF',
    'AUDUSD',
    'USDCAD',
    'NZDUSD',
]

CROSS_PAIRS: List[str] = [
    'EURGBP',
    'EURJPY',
    'GBPJPY',
    'AUDJPY',
    'EURAUD',
]

ALL_PAIRS: List[str] = MAJOR_PAIRS + CROSS_PAIRS


# ============================================================================
# PIP CALCULATIONS
# ============================================================================

def get_pip_value(pair: str) -> float:
    """
    Get the pip value for a currency pair or precious metal.

    Most pairs: 1 pip = 0.0001 (4 decimal places)
    JPY pairs: 1 pip = 0.01 (2 decimal places)
    Gold (XAU): 1 pip = 1.00 (moves in $1.00 increments for practical trading)
    Silver (XAG): 1 pip = 0.01 (moves in $0.01 increments for practical trading)

    Note: Gold/Silver pip values are scaled up from their technical minimum tick
    to match the practical significance of forex pips (roughly 0.01% of price).
    This makes SL/TP pip values comparable across instruments:
    - EURUSD: 15 pip SL ≈ 0.14% move ≈ $0.0015
    - XAUUSD: 15 pip SL ≈ 0.58% move ≈ $15.00

    Args:
        pair: Currency pair (e.g., 'EURUSD', 'USDJPY', 'XAUUSD')

    Returns:
        Pip value as float (0.0001, 0.01, 1.00)

    Examples:
        >>> get_pip_value('EURUSD')
        0.0001
        >>> get_pip_value('USDJPY')
        0.01
        >>> get_pip_value('GBPJPY')
        0.01
        >>> get_pip_value('XAUUSD')
        1.00
        >>> get_pip_value('XAGUSD')
        0.01
    """
    pair_upper = pair.upper()
    if 'XAU' in pair_upper:
        return 1.00  # Gold: 1 pip = $1 price move (practical trading unit)
    elif 'XAG' in pair_upper:
        return 0.01  # Silver: 1 pip = $0.01 price move (practical trading unit)
    elif 'JPY' in pair_upper:
        return 0.01
    else:
        return 0.0001


def get_pip_multiplier(pair: str) -> int:
    """
    Get the multiplier to convert price difference to pips.

    This is the inverse of get_pip_value().

    Args:
        pair: Currency pair

    Returns:
        Multiplier (10000 for most pairs, 100 for JPY/Silver, 1 for Gold)

    Examples:
        >>> get_pip_multiplier('EURUSD')
        10000
        >>> get_pip_multiplier('USDJPY')
        100
        >>> get_pip_multiplier('XAUUSD')
        1
        >>> get_pip_multiplier('XAGUSD')
        100
    """
    pair_upper = pair.upper()
    if 'XAU' in pair_upper:
        return 1  # Gold: 1 pip = $1
    elif 'XAG' in pair_upper:
        return 100  # Silver: 1 pip = $0.01
    elif 'JPY' in pair_upper:
        return 100
    else:
        return 10000


def price_to_pips(price_diff: float, pair: str) -> float:
    """
    Convert a price difference to pips.

    Args:
        price_diff: Price difference (e.g., 0.0025)
        pair: Currency pair

    Returns:
        Pip value (e.g., 25.0 for EURUSD)

    Examples:
        >>> price_to_pips(0.0025, 'EURUSD')
        25.0
        >>> price_to_pips(0.25, 'USDJPY')
        25.0
    """
    pip_value = get_pip_value(pair)
    return round(price_diff / pip_value, 1)


def is_jpy_pair(pair: str) -> bool:
    """Check if a pair involves Japanese Yen."""
    return 'JPY' in pair.upper()


def get_pip_value_in_usd(pair: str, current_price: float = None) -> float:
    """
    Get the dollar value of 1 pip per standard lot (100,000 units).

    This is used for position sizing calculations.

    The pip value depends on the QUOTE currency (second currency in pair):
    - USD quote: Always $10.00 per pip
    - Other quotes: Convert using USD exchange rate

    Args:
        pair: Currency pair (e.g., 'EURUSD', 'USDJPY')
        current_price: Current price of the pair (needed for non-USD quote pairs)

    Returns:
        Dollar value per pip per standard lot

    Examples:
        USD quote pairs (EURUSD, GBPUSD, AUDUSD, NZDUSD): $10.00 per pip
        JPY quote pairs (USDJPY, EURJPY, GBPJPY): ~$6.37 per pip (at USDJPY=157)
        CAD quote pairs (USDCAD, EURCAD, GBPCAD, AUDCAD): ~$6.94 per pip
        CHF quote pairs (USDCHF, EURCHF, GBPCHF): ~$11.11 per pip
        GBP quote pairs (EURGBP, AUDGBP): ~$12.60 per pip
        AUD quote pairs (EURAUD, GBPAUD): ~$6.20 per pip
        NZD quote pairs (AUDNZD, GBPNZD, EURNZD): ~$5.80 per pip
        Gold (XAUUSD): $100.00 per pip (100 oz * $1.00)
        Silver (XAGUSD): $50.00 per pip (5000 oz * $0.01)
    """
    pair_upper = pair.upper()

    # Default USD exchange rates for pip value calculation
    # These are approximate rates - used when current_price not provided
    # Format: What $1 USD buys, or what 1 unit buys in USD
    DEFAULT_USD_RATES = {
        # USDXXX pairs - how many XXX per 1 USD
        'USDJPY': 157.0,
        'USDCAD': 1.44,
        'USDCHF': 0.90,
        # XXXUSD pairs - how many USD per 1 XXX
        'GBPUSD': 1.26,
        'AUDUSD': 0.62,
        'NZDUSD': 0.58,
        'EURUSD': 1.08,
    }

    # Gold - 100 oz per lot, pip = $1.00 price move
    if 'XAU' in pair_upper:
        return 100.00  # 100 oz * $1.00 = $100 per pip

    # Silver - 5000 oz per lot, pip = $0.01 price move
    if 'XAG' in pair_upper:
        return 50.00  # 5000 oz * $0.01 = $50 per pip

    # Get quote currency (last 3 chars)
    quote_currency = pair_upper[-3:]

    # USD quote pairs - always $10 per pip
    # Examples: EURUSD, GBPUSD, AUDUSD, NZDUSD
    if quote_currency == 'USD':
        return 10.00

    # JPY quote pairs: pip value = $10 * (100 / USDJPY)
    # Examples: USDJPY, EURJPY, GBPJPY, AUDJPY, NZDJPY, CADJPY, CHFJPY
    if quote_currency == 'JPY':
        usdjpy = DEFAULT_USD_RATES.get('USDJPY', 157.0)
        return round(10.00 * (100 / usdjpy), 2)  # ~$6.37

    # CAD quote pairs: pip value = $10 / USDCAD
    # Examples: USDCAD, EURCAD, GBPCAD, AUDCAD, NZDCAD
    if quote_currency == 'CAD':
        usdcad = DEFAULT_USD_RATES.get('USDCAD', 1.44)
        return round(10.00 / usdcad, 2)  # ~$6.94

    # CHF quote pairs: pip value = $10 / USDCHF
    # Examples: USDCHF, EURCHF, GBPCHF, AUDCHF, NZDCHF, CADCHF
    if quote_currency == 'CHF':
        usdchf = DEFAULT_USD_RATES.get('USDCHF', 0.90)
        return round(10.00 / usdchf, 2)  # ~$11.11

    # GBP quote pairs: pip value = $10 * GBPUSD
    # Examples: EURGBP, AUDGBP, NZDGBP, CADGBP, CHFGBP
    if quote_currency == 'GBP':
        gbpusd = DEFAULT_USD_RATES.get('GBPUSD', 1.26)
        return round(10.00 * gbpusd, 2)  # ~$12.60

    # AUD quote pairs: pip value = $10 * AUDUSD
    # Examples: EURAUD, GBPAUD, NZDAUD
    if quote_currency == 'AUD':
        audusd = DEFAULT_USD_RATES.get('AUDUSD', 0.62)
        return round(10.00 * audusd, 2)  # ~$6.20

    # NZD quote pairs: pip value = $10 * NZDUSD
    # Examples: AUDNZD, EURNZD, GBPNZD
    if quote_currency == 'NZD':
        nzdusd = DEFAULT_USD_RATES.get('NZDUSD', 0.58)
        return round(10.00 * nzdusd, 2)  # ~$5.80

    # EUR quote pairs (rare): pip value = $10 * EURUSD
    # Examples: GBPEUR (uncommon)
    if quote_currency == 'EUR':
        eurusd = DEFAULT_USD_RATES.get('EURUSD', 1.08)
        return round(10.00 * eurusd, 2)  # ~$10.80

    # Default fallback for unknown quote currencies
    return 10.00


def calculate_lot_size(account_balance: float, risk_percent: float,
                       stop_loss_pips: float, pair: str,
                       current_price: float = None,
                       min_lot: float = 0.01, max_lot: float = 10.0,
                       lot_step: float = 0.01) -> dict:
    """
    Calculate position size based on fixed percentage risk.

    Args:
        account_balance: Total account balance in USD
        risk_percent: Risk per trade as percentage (e.g., 1.55)
        stop_loss_pips: Stop loss distance in pips
        pair: Currency pair
        current_price: Current price (for non-USD quote pairs)
        min_lot: Minimum lot size (broker constraint)
        max_lot: Maximum lot size
        lot_step: Lot size increment (typically 0.01)

    Returns:
        {
            'lot_size': float,          # Calculated lot size (rounded to lot_step)
            'lot_size_raw': float,      # Raw calculated value before rounding
            'risk_dollars': float,      # Dollar risk
            'pip_value': float,         # Pip value per lot
            'actual_risk_pct': float,   # Actual risk after rounding
            'capped': bool              # True if lot was capped to min/max
        }
    """
    risk_dollars = account_balance * (risk_percent / 100)
    pip_value_per_lot = get_pip_value_in_usd(pair, current_price)

    # Calculate raw lot size
    # risk_dollars = lot_size * stop_loss_pips * pip_value_per_lot
    if stop_loss_pips <= 0 or pip_value_per_lot <= 0:
        return {
            'lot_size': min_lot,
            'lot_size_raw': 0,
            'risk_dollars': risk_dollars,
            'pip_value': pip_value_per_lot,
            'actual_risk_pct': 0,
            'capped': True
        }

    lot_size_raw = risk_dollars / (stop_loss_pips * pip_value_per_lot)

    # Round to lot step
    lot_size = round(lot_size_raw / lot_step) * lot_step
    lot_size = round(lot_size, 2)  # Clean up floating point

    # Apply min/max constraints
    capped = False
    if lot_size < min_lot:
        lot_size = min_lot
        capped = True
    elif lot_size > max_lot:
        lot_size = max_lot
        capped = True

    # Calculate actual risk after rounding
    actual_risk_dollars = lot_size * stop_loss_pips * pip_value_per_lot
    actual_risk_pct = (actual_risk_dollars / account_balance) * 100

    return {
        'lot_size': lot_size,
        'lot_size_raw': round(lot_size_raw, 4),
        'risk_dollars': round(risk_dollars, 2),
        'pip_value': pip_value_per_lot,
        'actual_risk_pct': round(actual_risk_pct, 2),
        'capped': capped
    }


# ============================================================================
# VALIDATION
# ============================================================================

def is_valid_pair(pair: str) -> bool:
    """
    Check if a currency pair is valid (6-7 characters, all letters).

    Args:
        pair: Currency pair string

    Returns:
        True if valid format
    """
    if not pair or not isinstance(pair, str):
        return False
    pair = pair.upper()
    return len(pair) in (6, 7) and pair.isalpha()


def normalize_pair(pair: str) -> str:
    """
    Normalize a currency pair to uppercase without separators.

    Args:
        pair: Currency pair (e.g., 'eur/usd', 'EUR-USD', 'eurusd')

    Returns:
        Normalized pair (e.g., 'EURUSD')
    """
    if not pair:
        return ''
    # Remove common separators and uppercase
    return pair.upper().replace('/', '').replace('-', '').replace('_', '')


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("Testing forex_utils...")

    # Test pip values
    assert get_pip_value('EURUSD') == 0.0001
    assert get_pip_value('USDJPY') == 0.01
    assert get_pip_value('GBPJPY') == 0.01
    assert get_pip_value('audusd') == 0.0001  # lowercase
    assert get_pip_value('XAUUSD') == 1.00  # Gold: 1 pip = $1 price move
    assert get_pip_value('XAGUSD') == 0.01  # Silver: 1 pip = $0.01 price move
    print("  ✓ get_pip_value() works correctly")

    # Test pip multiplier
    assert get_pip_multiplier('EURUSD') == 10000
    assert get_pip_multiplier('USDJPY') == 100
    assert get_pip_multiplier('XAUUSD') == 1  # Gold: 1 pip = $1
    assert get_pip_multiplier('XAGUSD') == 100  # Silver: 1 pip = $0.01
    print("  ✓ get_pip_multiplier() works correctly")

    # Test price to pips
    assert price_to_pips(0.0025, 'EURUSD') == 25.0
    assert price_to_pips(0.25, 'USDJPY') == 25.0
    assert price_to_pips(25.0, 'XAUUSD') == 25.0  # Gold: $25 = 25 pips
    assert price_to_pips(0.25, 'XAGUSD') == 25.0  # Silver: $0.25 = 25 pips
    print("  ✓ price_to_pips() works correctly")

    # Test validation
    assert is_valid_pair('EURUSD') == True
    assert is_valid_pair('EUR') == False
    assert is_valid_pair('') == False
    print("  ✓ is_valid_pair() works correctly")

    # Test normalization
    assert normalize_pair('eur/usd') == 'EURUSD'
    assert normalize_pair('EUR-USD') == 'EURUSD'
    print("  ✓ normalize_pair() works correctly")

    print("\n✅ All tests passed!")
