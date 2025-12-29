"""
Claude Haiku Predictor for Live Trading
========================================

Optimized for low-latency predictions at session open.
Uses Anthropic's async SDK for non-blocking API calls.

Key features:
- Async API calls via anthropic SDK
- Single model (Haiku 4.5) for speed
- Response parsing with fallback logic
- Cost tracking
"""

import base64
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import anthropic

from ..config import settings


# Haiku 4.5 model identifier
HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Model key for database storage (matches backtester convention)
MODEL_KEY = "claude_haiku_45"

# Retry configuration
MAX_RETRIES = 3
BASE_DELAY = 2.0


def build_analysis_prompt(pair: str, session_name: str) -> str:
    """
    Build the analysis prompt for Haiku.

    Uses the same prompt format as the backtester for consistency.
    """
    return f"""Analyze the provided intraday chart for {pair}.

The chart shows colored session backgrounds: TAN=Asian, BLUE=London, PURPLE=NY.
Use the Start/End labels and timestamps to identify sessions.

Horizontal dashed lines show session highs and lows (BLUE and PURPLE lines).
Orange lines show previous day high/low. These are key support/resistance levels.

FVGs are numbered by creation order with explicit price bounds and date/time shown.
- Labeled on LEFT side in 3-line format:
  Line 1: FVG #X
  Line 2: L: [lower]  U: [upper]  (exact FVG price boundaries)
  Line 3: MM/DD HH:MM
- CRITICAL: Read the DATE/TIME from the LABEL TEXT, not from where the zone appears on the X-axis
- FVG zones extend horizontally across the chart for visibility, but creation date is ONLY in the label text
- ONLY reference FVG numbers you can SEE labeled
- HIGHER NUMBERS = MORE RECENT FVGs (newer gaps)
- Green FVGs = bullish, Red FVGs = bearish
- Assess FVG SIZE visually - wider zones indicate stronger imbalances

Provide a concise technical analysis with:

1. Current Bias: [BULLISH/BEARISH/NEUTRAL]
   - If waiting for a level, add it (e.g., 'Wait for FVG #5 at 1.0850')

2. Next Hour Prediction: [Up/Down/Neutral]

3. Conviction: [1-10] (10 = highest confidence)

4. ## General Analysis
   3-5 sentences summarizing session patterns, session high/low interactions, FVG recency and visual size, and technical factors.

5. ## Bullish Factors
   List as bullet points (maximum 5), including session behaviors, recent FVGs with dates and visual sizes, and technicals.

6. ## Bearish Factors
   List as bullet points (maximum 5), including session behaviors, recent FVGs with dates and visual sizes, and technicals.

Be specific with counts, FVG numbers with dates, and session high/low levels. Be decisive: LONG, SHORT, or WAIT.
Ensure each section has content and strictly follow this format."""


def parse_response(text: str) -> Dict[str, Any]:
    """
    Parse Claude's response to extract prediction and conviction.

    Handles multiple formats:
    - Same-line: "Current Bias: BEARISH"
    - Multi-line with bold: "## Current Bias" followed by "**BEARISH**"
    """
    result = {
        'prediction': 'NEUTRAL',
        'conviction': 5
    }

    text_upper = text.upper()
    lines = text.split('\n')

    # Look for "Current Bias" section
    if "CURRENT BIAS" in text_upper:
        for i, line in enumerate(lines):
            if 'CURRENT BIAS' in line.upper():
                # Check same line first
                if 'BULLISH' in line.upper():
                    result['prediction'] = 'BULLISH'
                    break
                elif 'BEARISH' in line.upper():
                    result['prediction'] = 'BEARISH'
                    break
                elif 'NEUTRAL' in line.upper():
                    result['prediction'] = 'NEUTRAL'
                    break

                # Multi-line format: check next few lines
                for j in range(i + 1, min(i + 4, len(lines))):
                    next_line = lines[j].upper().strip()
                    if '**BULLISH**' in next_line or next_line == 'BULLISH':
                        result['prediction'] = 'BULLISH'
                        break
                    elif '**BEARISH**' in next_line or next_line == 'BEARISH':
                        result['prediction'] = 'BEARISH'
                        break
                    elif '**NEUTRAL**' in next_line or next_line == 'NEUTRAL':
                        result['prediction'] = 'NEUTRAL'
                        break
                break

    # Fallback: check first 500 chars
    if result['prediction'] == 'NEUTRAL':
        first_section = text_upper[:500]
        if 'BULLISH' in first_section:
            result['prediction'] = 'BULLISH'
        elif 'BEARISH' in first_section:
            result['prediction'] = 'BEARISH'

    # Extract conviction
    match = re.search(r'Conviction:\s*(\d+)\s*/?\s*10?', text, re.IGNORECASE)
    if match:
        result['conviction'] = int(match.group(1))
    else:
        match = re.search(r'conviction[:\s]+(\d+)', text, re.IGNORECASE)
        if match:
            conviction = int(match.group(1))
            if 1 <= conviction <= 10:
                result['conviction'] = conviction

    return result


async def predict(
    chart_path: str,
    pair: str,
    session_name: str,
) -> Dict[str, Any]:
    """
    Generate a prediction using Claude Haiku.

    Args:
        chart_path: Path to the chart image
        pair: Currency pair (e.g., 'EURUSD')
        session_name: Session name (e.g., 'London_Open')

    Returns:
        Dictionary with prediction, conviction, full_analysis, etc.
    """
    start_time = time.perf_counter()

    # Read and encode image
    chart_file = Path(chart_path)
    if not chart_file.exists():
        return {
            'prediction': 'NEUTRAL',
            'conviction': 0,
            'full_analysis': f'Error: Chart not found at {chart_path}',
            'model_version': HAIKU_MODEL,
            'api_cost': 0.0,
            'execution_time_ms': 0,
            'error': 'Chart not found'
        }

    with open(chart_file, 'rb') as f:
        image_data = base64.standard_b64encode(f.read()).decode('utf-8')

    # Build prompt
    prompt = build_analysis_prompt(pair, session_name)

    # Create async client
    client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": image_data
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }]
            )

            execution_time_ms = round((time.perf_counter() - start_time) * 1000, 2)

            # Parse response
            full_text = response.content[0].text
            parsed = parse_response(full_text)

            return {
                'prediction': parsed['prediction'],
                'conviction': parsed['conviction'],
                'full_analysis': full_text,
                'model_version': HAIKU_MODEL,
                'model_key': MODEL_KEY,  # For database storage
                'api_cost': 0.001,  # ~$0.001 per prediction for Haiku
                'execution_time_ms': execution_time_ms,
            }

        except anthropic.RateLimitError as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                print(f"  Rate limited, retrying in {delay:.1f}s...")
                import asyncio
                await asyncio.sleep(delay)
                continue

        except anthropic.APITimeoutError as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                print(f"  Timeout, retrying in {delay:.1f}s...")
                import asyncio
                await asyncio.sleep(delay)
                continue

        except Exception as e:
            last_error = e
            break

    # All retries failed
    execution_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    print(f"  Haiku prediction failed: {last_error}")

    return {
        'prediction': 'NEUTRAL',
        'conviction': 0,
        'full_analysis': f'Error: {str(last_error)}',
        'model_version': HAIKU_MODEL,
        'model_key': MODEL_KEY,
        'api_cost': 0.0,
        'execution_time_ms': execution_time_ms,
        'error': str(last_error)
    }


async def predict_session(
    pair: str,
    session_name: str,
    session_dt: datetime,
    chart_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    High-level function to generate a prediction for a session.

    If chart_path is not provided, generates the chart first.

    Args:
        pair: Currency pair
        session_name: Session name
        session_dt: Session datetime
        chart_path: Path to pre-generated chart (optional)

    Returns:
        Prediction result dictionary
    """
    from .chart_gen import generate_session_chart

    # Generate chart if not provided
    if chart_path is None:
        chart_path = await generate_session_chart(pair, session_name, session_dt)

    if chart_path is None:
        return {
            'prediction': 'NEUTRAL',
            'conviction': 0,
            'full_analysis': 'Error: Failed to generate chart',
            'model_version': HAIKU_MODEL,
            'api_cost': 0.0,
            'execution_time_ms': 0,
            'error': 'Chart generation failed'
        }

    # Run prediction
    result = await predict(chart_path, pair, session_name)
    result['chart_path'] = chart_path
    result['pair'] = pair
    result['session_name'] = session_name
    result['session_datetime'] = session_dt.isoformat()

    return result
