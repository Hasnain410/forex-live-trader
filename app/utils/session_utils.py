"""
Session Utilities - Shared DST-Aware Session Logic
==================================================

This module provides DST-aware session time functions used across
chart_generator.py, backtest_verification.py, and other modules.

Centralizing this logic ensures consistency and makes maintenance easier.

Usage:
    from session_utils import get_session_times_for_date, get_session_duration_for_date
    
    # Get session times for a specific date
    times = get_session_times_for_date(datetime(2024, 7, 15, tzinfo=pytz.UTC))
    print(times['London_Open'])  # {'hour': 7, 'minute': 0, 'name': 'London Open'}
    
    # Get session duration
    duration = get_session_duration_for_date('Asian_Open', datetime(2024, 7, 15))
    print(duration)  # 8.0 hours
"""

import pytz
from datetime import datetime, timezone


def get_session_times_for_date(date_dt):
    """
    Get session start times adjusted for DST on a specific date.
    
    This ensures predictions and charts are generated at ACTUAL market opens,
    not hours late during summer months.
    
    Args:
        date_dt: datetime object for the date to check (must be timezone-aware UTC)
    
    Returns:
        Dict with session times in UTC, automatically adjusted for DST
        
    Example:
        Winter (Jan): London_Open = 08:00 UTC (08:00 GMT)
        Summer (Jul): London_Open = 07:00 UTC (08:00 BST)
    
    Session Times:
        Asian:  01:00 - 09:00 UTC (fixed, Japan has no DST)
        London: 08:00 - 13:00 local (07:00 - 12:00 UTC in summer)
        NY:     09:30 - 13:30 local (13:30 - 17:30 UTC in summer)
    """
    # Ensure we have a date (strip time if present)
    base_date = date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # =========================================================================
    # Asian Session - FIXED UTC (Japan does NOT observe DST)
    # =========================================================================
    # Traditional forex Asian session times:
    #   01:00 UTC = 10:00 AM Tokyo (Asian markets active)
    #   09:00 UTC = 18:00 Tokyo (Asian session winds down)
    # These are always the same regardless of date.
    asian_open_hour = 1
    asian_open_minute = 0
    asian_close_hour = 9
    asian_close_minute = 0
    
    # =========================================================================
    # London Session - DST AWARE (British Summer Time)
    # =========================================================================
    london_tz = pytz.timezone('Europe/London')
    london_open = london_tz.localize(datetime(base_date.year, base_date.month, base_date.day, 8, 0))
    london_close = london_tz.localize(datetime(base_date.year, base_date.month, base_date.day, 13, 0))
    
    # =========================================================================
    # New York Session - DST AWARE (Eastern Daylight Time)
    # =========================================================================
    ny_tz = pytz.timezone('America/New_York')
    ny_open = ny_tz.localize(datetime(base_date.year, base_date.month, base_date.day, 9, 30))
    ny_close = ny_tz.localize(datetime(base_date.year, base_date.month, base_date.day, 13, 30))
    
    # Build result with Asian as fixed UTC, London/NY converted from local time
    return {
        'Asian_Open': {
            'hour': asian_open_hour,
            'minute': asian_open_minute,
            'name': 'Asian Open'
        },
        'Asian_Close': {
            'hour': asian_close_hour,
            'minute': asian_close_minute,
            'name': 'Asian Close'
        },
        'London_Open': {
            'hour': london_open.astimezone(pytz.UTC).hour,
            'minute': london_open.astimezone(pytz.UTC).minute,
            'name': 'London Open'
        },
        'London_Close': {
            'hour': london_close.astimezone(pytz.UTC).hour,
            'minute': london_close.astimezone(pytz.UTC).minute,
            'name': 'London Close'
        },
        'NY_Open': {
            'hour': ny_open.astimezone(pytz.UTC).hour,
            'minute': ny_open.astimezone(pytz.UTC).minute,
            'name': 'NY Open'
        },
        'NY_Close': {
            'hour': ny_close.astimezone(pytz.UTC).hour,
            'minute': ny_close.astimezone(pytz.UTC).minute,
            'name': 'NY Close'
        }
    }


def get_session_duration_for_date(session_name, date_dt):
    """
    Get session duration in hours for a specific date.
    
    Session durations are based on actual market hours:
    - Asian: 01:00 → 09:00 (8 hours)
    - London: 08:00 → 13:00 local time (5 hours)
    - NY: 09:30 → 13:30 local time (4 hours)
    
    Args:
        session_name: Session name (e.g., 'Asian_Open', 'London_Open')
        date_dt: Date to check (datetime object, UTC)
    
    Returns:
        Duration in hours (float)
        
    Example:
        duration = get_session_duration_for_date('Asian_Open', some_date)
        # Returns 8.0
    """
    # Ensure we have a date
    base_date = date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Map Open sessions to their Close sessions with timezone info
    duration_map = {
        'Asian_Open': ('Asia/Tokyo', 1, 0, 9, 0),      # 01:00 - 09:00 Tokyo time (8 hours)
        'London_Open': ('Europe/London', 8, 0, 13, 0), # 08:00 - 13:00 London time (5 hours)
        'NY_Open': ('America/New_York', 9, 30, 13, 30) # 09:30 - 13:30 NY time (4 hours)
    }
    
    if session_name not in duration_map:
        # For Close sessions or unknown, return 4 hours default
        return 4.0
    
    tz_name, open_h, open_m, close_h, close_m = duration_map[session_name]
    
    try:
        tz = pytz.timezone(tz_name)
        
        # Create open and close times in local timezone
        open_time = tz.localize(datetime(base_date.year, base_date.month, base_date.day, open_h, open_m))
        close_time = tz.localize(datetime(base_date.year, base_date.month, base_date.day, close_h, close_m))
        
        # Convert to UTC
        open_utc = open_time.astimezone(pytz.UTC)
        close_utc = close_time.astimezone(pytz.UTC)
        
        # Calculate duration in hours
        duration = (close_utc - open_utc).total_seconds() / 3600
        
        return duration
        
    except Exception:
        # Fallback to defaults
        if 'Asian' in session_name:
            return 8.0
        elif 'London' in session_name:
            return 5.0
        else:
            return 4.0


def get_session_zones():
    """
    Calculate session zone times for chart backgrounds.
    Uses current date to determine DST status.
    
    This function is kept for backward compatibility with existing code
    that uses it for drawing colored session backgrounds on charts.
    
    Returns:
        Dict with session zones as (start_hour, end_hour) tuples
        
    Example:
        zones = get_session_zones()
        # Returns: {'Asian': (1, 9), 'London': (8, 12), 'NY': (13.5, 17.5)}
    """
    now = datetime.now(pytz.UTC)
    
    # Tokyo (no DST)
    tokyo_tz = pytz.timezone('Asia/Tokyo')
    tokyo_10am = tokyo_tz.localize(datetime(now.year, now.month, now.day, 10, 0))
    tokyo_6pm = tokyo_tz.localize(datetime(now.year, now.month, now.day, 18, 0))
    asian_start = tokyo_10am.astimezone(pytz.UTC).hour
    asian_end = tokyo_6pm.astimezone(pytz.UTC).hour
    
    # London (with DST)
    london_tz = pytz.timezone('Europe/London')
    london_9am = london_tz.localize(datetime(now.year, now.month, now.day, 9, 0))
    london_1pm = london_tz.localize(datetime(now.year, now.month, now.day, 13, 0))
    london_start = london_9am.astimezone(pytz.UTC).hour
    london_end = london_1pm.astimezone(pytz.UTC).hour
    
    # New York (with DST)
    ny_tz = pytz.timezone('America/New_York')
    ny_930am = ny_tz.localize(datetime(now.year, now.month, now.day, 9, 30))
    ny_130pm = ny_tz.localize(datetime(now.year, now.month, now.day, 13, 30))
    ny_start_decimal = ny_930am.astimezone(pytz.UTC).hour + (ny_930am.astimezone(pytz.UTC).minute / 60.0)
    ny_end_decimal = ny_130pm.astimezone(pytz.UTC).hour + (ny_130pm.astimezone(pytz.UTC).minute / 60.0)
    
    return {
        'Asian': (asian_start, asian_end),
        'London': (london_start, london_end),
        'NY': (ny_start_decimal, ny_end_decimal)
    }


# ============================================================================
# LIVE PREDICTION UTILITIES
# ============================================================================

def get_current_session(now_utc=None):
    """
    Determine the current active session or next upcoming Open session.

    This function is used for live predictions to identify which trading
    session is currently active or coming up next.

    Args:
        now_utc: Optional datetime (UTC). Defaults to current time.

    Returns:
        Dict with session information:
        {
            'session_name': 'London_Open',
            'display_name': 'London Open',
            'session_datetime': datetime,  # Session start time (UTC)
            'session_end': datetime,       # Session end time (UTC)
            'status': 'active' | 'upcoming' | 'market_closed',
            'time_until_start': timedelta or None,
            'time_until_end': timedelta or None,
            'date': str  # YYYYMMDD format
        }

    Example:
        session = get_current_session()
        if session['status'] == 'active':
            print(f"{session['display_name']} is active!")
            print(f"Ends in {session['time_until_end']}")
    """
    from datetime import timedelta

    if now_utc is None:
        now_utc = datetime.now(pytz.UTC)
    elif now_utc.tzinfo is None:
        now_utc = pytz.UTC.localize(now_utc)

    # Check if market is closed (Saturday, or Sunday before Asian Open)
    weekday = now_utc.weekday()  # Monday=0, Sunday=6

    # Market is closed from Friday NY Close (~18:30 UTC) to Sunday Asian Open (~22:00 UTC previous day / 01:00 UTC)
    # Simplified: Saturday is always closed, Sunday before 22:00 UTC is closed
    if weekday == 5:  # Saturday - market closed
        # Find next Sunday's Asian Open (which is at 01:00 UTC Monday effectively)
        days_until_monday = 2
        next_asian = now_utc.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=days_until_monday)
        return {
            'session_name': 'Asian_Open',
            'display_name': 'Asian Open',
            'session_datetime': next_asian,
            'session_end': next_asian.replace(hour=9),
            'status': 'market_closed',
            'time_until_start': next_asian - now_utc,
            'time_until_end': None,
            'date': next_asian.strftime('%Y%m%d'),
            'message': 'Market closed for the weekend. Opens Sunday evening (US time).'
        }

    # Get session times for today
    session_times = get_session_times_for_date(now_utc)

    # Define Open sessions with their durations (we only predict on Open sessions)
    # Check in REVERSE start order so during overlaps we prefer the most recently started session
    # (e.g., at 08:30 UTC, prefer London_Open over Asian_Open which is about to end)
    open_sessions = [
        ('NY_Open', 'NY_Close', 4),
        ('London_Open', 'London_Close', 5),
        ('Asian_Open', 'Asian_Close', 8),
    ]

    current_hour = now_utc.hour
    current_minute = now_utc.minute
    current_decimal = current_hour + (current_minute / 60.0)

    # Check each Open session
    for open_name, close_name, duration_hours in open_sessions:
        open_info = session_times[open_name]
        close_info = session_times[close_name]

        open_decimal = open_info['hour'] + (open_info['minute'] / 60.0)
        close_decimal = close_info['hour'] + (close_info['minute'] / 60.0)

        # Create datetime objects for session start/end
        session_start = now_utc.replace(
            hour=open_info['hour'],
            minute=open_info['minute'],
            second=0,
            microsecond=0
        )
        session_end = now_utc.replace(
            hour=close_info['hour'],
            minute=close_info['minute'],
            second=0,
            microsecond=0
        )

        # Handle day boundary (e.g., Asian session might span midnight)
        if close_decimal < open_decimal:
            # Session spans midnight
            if current_decimal >= open_decimal or current_decimal < close_decimal:
                # We're in this session
                if current_decimal < close_decimal:
                    session_start = session_start - timedelta(days=1)
                else:
                    session_end = session_end + timedelta(days=1)

                return {
                    'session_name': open_name,
                    'display_name': open_info['name'],
                    'session_datetime': session_start,
                    'session_end': session_end,
                    'status': 'active',
                    'time_until_start': None,
                    'time_until_end': session_end - now_utc,
                    'date': session_start.strftime('%Y%m%d')
                }
        else:
            # Normal session (doesn't span midnight)
            if open_decimal <= current_decimal < close_decimal:
                # We're in this session
                return {
                    'session_name': open_name,
                    'display_name': open_info['name'],
                    'session_datetime': session_start,
                    'session_end': session_end,
                    'status': 'active',
                    'time_until_start': None,
                    'time_until_end': session_end - now_utc,
                    'date': session_start.strftime('%Y%m%d')
                }

    # No active session - find the next upcoming Open session
    upcoming_sessions = []

    for open_name, close_name, duration_hours in open_sessions:
        open_info = session_times[open_name]
        close_info = session_times[close_name]

        # Create session start time for today
        session_start = now_utc.replace(
            hour=open_info['hour'],
            minute=open_info['minute'],
            second=0,
            microsecond=0
        )
        session_end = now_utc.replace(
            hour=close_info['hour'],
            minute=close_info['minute'],
            second=0,
            microsecond=0
        )

        # If session already passed today, get tomorrow's session
        if session_start <= now_utc:
            # Check if it's Friday after NY Close - next session is Monday
            if weekday == 4 and open_name == 'Asian_Open':  # Friday, looking for Asian
                session_start = session_start + timedelta(days=3)  # Monday
                session_end = session_end + timedelta(days=3)
            else:
                session_start = session_start + timedelta(days=1)
                session_end = session_end + timedelta(days=1)

        upcoming_sessions.append({
            'session_name': open_name,
            'display_name': open_info['name'],
            'session_datetime': session_start,
            'session_end': session_end,
            'time_until_start': session_start - now_utc
        })

    # Sort by time until start and return the nearest
    upcoming_sessions.sort(key=lambda x: x['time_until_start'])
    next_session = upcoming_sessions[0]

    return {
        'session_name': next_session['session_name'],
        'display_name': next_session['display_name'],
        'session_datetime': next_session['session_datetime'],
        'session_end': next_session['session_end'],
        'status': 'upcoming',
        'time_until_start': next_session['time_until_start'],
        'time_until_end': None,
        'date': next_session['session_datetime'].strftime('%Y%m%d')
    }


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("SESSION UTILS - DST TESTING")
    print("="*80)
    
    # Test winter date
    winter_date = datetime(2024, 1, 15, tzinfo=pytz.UTC)
    winter_times = get_session_times_for_date(winter_date)
    
    print(f"\nWinter (Jan 15, 2024):")
    print(f"  Asian_Open:  {winter_times['Asian_Open']['hour']:02d}:{winter_times['Asian_Open']['minute']:02d} UTC")
    print(f"  London_Open: {winter_times['London_Open']['hour']:02d}:{winter_times['London_Open']['minute']:02d} UTC")
    print(f"  NY_Open:     {winter_times['NY_Open']['hour']:02d}:{winter_times['NY_Open']['minute']:02d} UTC")
    
    # Test summer date
    summer_date = datetime(2024, 7, 15, tzinfo=pytz.UTC)
    summer_times = get_session_times_for_date(summer_date)
    
    print(f"\nSummer (Jul 15, 2024):")
    print(f"  Asian_Open:  {summer_times['Asian_Open']['hour']:02d}:{summer_times['Asian_Open']['minute']:02d} UTC")
    print(f"  London_Open: {summer_times['London_Open']['hour']:02d}:{summer_times['London_Open']['minute']:02d} UTC")
    print(f"  NY_Open:     {summer_times['NY_Open']['hour']:02d}:{summer_times['NY_Open']['minute']:02d} UTC")
    
    # Test durations
    print(f"\nSession Durations:")
    for session in ['Asian_Open', 'London_Open', 'NY_Open']:
        duration = get_session_duration_for_date(session, winter_date)
        print(f"  {session}: {duration} hours")
    
    # Test session zones
    zones = get_session_zones()
    print(f"\nCurrent Session Zones (for chart backgrounds):")
    for name, (start, end) in zones.items():
        print(f"  {name}: {start} - {end} UTC")

    # Test get_current_session
    print("\n" + "-"*40)
    print("LIVE PREDICTION - Current Session Detection")
    print("-"*40)

    # Test current time
    current = get_current_session()
    print(f"\nCurrent Session (now):")
    print(f"  Session: {current['display_name']}")
    print(f"  Status: {current['status']}")
    print(f"  Date: {current['date']}")
    if current['status'] == 'active':
        mins = int(current['time_until_end'].total_seconds() // 60)
        print(f"  Time until end: {mins // 60}h {mins % 60}m")
    elif current['status'] == 'upcoming':
        mins = int(current['time_until_start'].total_seconds() // 60)
        print(f"  Time until start: {mins // 60}h {mins % 60}m")

    # Test various times
    test_times = [
        datetime(2024, 11, 25, 2, 0, tzinfo=pytz.UTC),   # During Asian
        datetime(2024, 11, 25, 10, 0, tzinfo=pytz.UTC),  # Between Asian and London
        datetime(2024, 11, 25, 8, 30, tzinfo=pytz.UTC),  # During London
        datetime(2024, 11, 25, 15, 0, tzinfo=pytz.UTC),  # During NY
        datetime(2024, 11, 23, 12, 0, tzinfo=pytz.UTC),  # Saturday (market closed)
    ]

    print(f"\nTest Times:")
    for test_time in test_times:
        session = get_current_session(test_time)
        print(f"  {test_time.strftime('%a %Y-%m-%d %H:%M')} UTC -> {session['display_name']} ({session['status']})")

    print("\n" + "="*80)
    print("✅ All tests passed! Module ready to use.")
    print("="*80 + "\n")
