"""
Polygon WebSocket Price Streaming
==================================

Real-time forex price streaming for:
- Accurate entry prices at session open
- Live TP/SL monitoring during sessions
- Sub-second price updates

Polygon WebSocket: wss://socket.polygon.io/forex
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Callable, Set
from dataclasses import dataclass, field

import websockets
from websockets.exceptions import ConnectionClosed

from ..config import settings, TRADING_PAIRS

logger = logging.getLogger(__name__)


@dataclass
class Quote:
    """Real-time forex quote."""
    pair: str
    bid: float
    ask: float
    timestamp: datetime

    @property
    def mid(self) -> float:
        """Mid price."""
        return (self.bid + self.ask) / 2

    @property
    def spread_pips(self) -> float:
        """Spread in pips."""
        diff = self.ask - self.bid
        if 'JPY' in self.pair:
            return diff * 100
        elif 'XAU' in self.pair:
            return diff
        elif 'XAG' in self.pair:
            return diff * 100
        return diff * 10000


@dataclass
class PriceAlert:
    """TP/SL price alert."""
    trade_id: str
    pair: str
    direction: str  # BULLISH or BEARISH
    entry_price: float
    take_profit: float
    stop_loss: float
    triggered: bool = False
    trigger_type: Optional[str] = None  # 'TP' or 'SL'
    trigger_price: Optional[float] = None
    trigger_time: Optional[datetime] = None


class PriceStream:
    """
    Polygon WebSocket client for real-time forex prices.

    Usage:
        stream = PriceStream(api_key)
        await stream.connect()
        await stream.subscribe(['EURUSD', 'GBPUSD'])

        # Get latest price
        quote = stream.get_quote('EURUSD')

        # Set TP/SL alert
        stream.add_alert(PriceAlert(...))

        await stream.disconnect()
    """

    POLYGON_WS_URL = "wss://socket.polygon.io/forex"

    def __init__(self, api_key: str, on_alert: Optional[Callable] = None):
        self.api_key = api_key
        self.on_alert = on_alert  # Callback when TP/SL hit

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._quotes: Dict[str, Quote] = {}
        self._alerts: Dict[str, PriceAlert] = {}  # trade_id -> alert
        self._subscribed_pairs: Set[str] = set()
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._connected = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        # websockets 12+ uses state, older versions use open
        try:
            from websockets.protocol import State
            return self._ws.state == State.OPEN
        except (ImportError, AttributeError):
            # Fallback for older versions
            return getattr(self._ws, 'open', False)

    def get_quote(self, pair: str) -> Optional[Quote]:
        """Get latest quote for a pair."""
        return self._quotes.get(pair)

    def get_price(self, pair: str) -> Optional[float]:
        """Get latest mid price for a pair."""
        quote = self._quotes.get(pair)
        return quote.mid if quote else None

    async def connect(self) -> bool:
        """Connect to Polygon WebSocket and authenticate."""
        try:
            logger.info(f"Connecting to Polygon WebSocket...")
            self._ws = await websockets.connect(
                self.POLYGON_WS_URL,
                ping_interval=30,
                ping_timeout=10,
            )

            # Wait for connection message
            msg = await self._ws.recv()
            data = json.loads(msg)
            if data[0].get('status') != 'connected':
                logger.error(f"Unexpected connection response: {data}")
                return False

            # Authenticate
            auth_msg = {"action": "auth", "params": self.api_key}
            await self._ws.send(json.dumps(auth_msg))

            msg = await self._ws.recv()
            data = json.loads(msg)
            if data[0].get('status') != 'auth_success':
                logger.error(f"Authentication failed: {data}")
                return False

            logger.info("Polygon WebSocket connected and authenticated")
            self._running = True
            self._connected.set()

            # Start receive loop
            self._recv_task = asyncio.create_task(self._receive_loop())

            return True

        except Exception as e:
            logger.error(f"Failed to connect to Polygon WebSocket: {e}")
            return False

    async def disconnect(self):
        """Disconnect from WebSocket."""
        self._running = False
        self._connected.clear()

        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            await self._ws.close()
            self._ws = None

        self._quotes.clear()
        self._subscribed_pairs.clear()
        logger.info("Polygon WebSocket disconnected")

    async def subscribe(self, pairs: list[str]):
        """Subscribe to forex pairs."""
        if not self.is_connected:
            logger.warning("Cannot subscribe: not connected")
            return

        # Convert to Polygon format: EURUSD -> C.EUR/USD
        symbols = []
        for pair in pairs:
            if pair not in self._subscribed_pairs:
                symbol = self._to_polygon_symbol(pair)
                symbols.append(symbol)
                self._subscribed_pairs.add(pair)

        if symbols:
            sub_msg = {"action": "subscribe", "params": ",".join(symbols)}
            await self._ws.send(json.dumps(sub_msg))
            logger.info(f"Subscribed to {len(symbols)} pairs")

    async def unsubscribe(self, pairs: list[str]):
        """Unsubscribe from forex pairs."""
        if not self.is_connected:
            return

        symbols = []
        for pair in pairs:
            if pair in self._subscribed_pairs:
                symbol = self._to_polygon_symbol(pair)
                symbols.append(symbol)
                self._subscribed_pairs.discard(pair)

        if symbols:
            unsub_msg = {"action": "unsubscribe", "params": ",".join(symbols)}
            await self._ws.send(json.dumps(unsub_msg))
            logger.info(f"Unsubscribed from {len(symbols)} pairs")

    def add_alert(self, alert: PriceAlert):
        """Add a TP/SL price alert."""
        self._alerts[alert.trade_id] = alert
        logger.info(f"Added alert for {alert.pair}: TP={alert.take_profit}, SL={alert.stop_loss}")

    def remove_alert(self, trade_id: str):
        """Remove a price alert."""
        if trade_id in self._alerts:
            del self._alerts[trade_id]

    def get_alert(self, trade_id: str) -> Optional[PriceAlert]:
        """Get alert by trade ID."""
        return self._alerts.get(trade_id)

    async def _receive_loop(self):
        """Main receive loop for WebSocket messages."""
        while self._running and self._ws:
            try:
                msg = await self._ws.recv()
                data = json.loads(msg)

                for item in data if isinstance(data, list) else [data]:
                    await self._handle_message(item)

            except ConnectionClosed:
                logger.warning("WebSocket connection closed")
                self._connected.clear()
                if self._running:
                    await self._reconnect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in receive loop: {e}")

    async def _handle_message(self, msg: dict):
        """Handle incoming WebSocket message."""
        ev = msg.get('ev')

        # Quote update (C = Currency/Forex)
        if ev == 'C':
            pair = self._from_polygon_symbol(msg.get('p', ''))
            if pair:
                quote = Quote(
                    pair=pair,
                    bid=msg.get('b', 0),
                    ask=msg.get('a', 0),
                    timestamp=datetime.fromtimestamp(msg.get('t', 0) / 1000, tz=timezone.utc)
                )
                self._quotes[pair] = quote

                # Check alerts
                await self._check_alerts(pair, quote)

        # Status messages
        elif msg.get('status'):
            logger.debug(f"Status: {msg.get('message', msg.get('status'))}")

    async def _check_alerts(self, pair: str, quote: Quote):
        """Check if any TP/SL alerts are triggered."""
        for trade_id, alert in list(self._alerts.items()):
            if alert.pair != pair or alert.triggered:
                continue

            price = quote.mid
            triggered = False
            trigger_type = None

            if alert.direction == 'BULLISH':
                # Long: TP above entry, SL below entry
                if price >= alert.take_profit:
                    triggered = True
                    trigger_type = 'TP'
                elif price <= alert.stop_loss:
                    triggered = True
                    trigger_type = 'SL'
            else:  # BEARISH
                # Short: TP below entry, SL above entry
                if price <= alert.take_profit:
                    triggered = True
                    trigger_type = 'TP'
                elif price >= alert.stop_loss:
                    triggered = True
                    trigger_type = 'SL'

            if triggered:
                alert.triggered = True
                alert.trigger_type = trigger_type
                alert.trigger_price = price
                alert.trigger_time = quote.timestamp

                logger.info(f"Alert triggered: {pair} {trigger_type} at {price}")

                if self.on_alert:
                    try:
                        await self.on_alert(alert)
                    except Exception as e:
                        logger.error(f"Error in alert callback: {e}")

    async def _reconnect(self):
        """Attempt to reconnect after connection loss."""
        logger.info("Attempting to reconnect...")
        await asyncio.sleep(5)

        if await self.connect():
            # Resubscribe to pairs
            if self._subscribed_pairs:
                pairs = list(self._subscribed_pairs)
                self._subscribed_pairs.clear()
                await self.subscribe(pairs)

    def _to_polygon_symbol(self, pair: str) -> str:
        """Convert EURUSD to C.EUR/USD format."""
        if pair.startswith('XAU') or pair.startswith('XAG'):
            # Metals: XAUUSD -> C.XAU/USD
            return f"C.{pair[:3]}/{pair[3:]}"
        # Forex: EURUSD -> C.EUR/USD
        return f"C.{pair[:3]}/{pair[3:]}"

    def _from_polygon_symbol(self, symbol: str) -> Optional[str]:
        """Convert C.EUR/USD to EURUSD format."""
        if not symbol or not symbol.startswith('C.'):
            return None
        # Remove C. prefix and slash
        return symbol[2:].replace('/', '')


# Global price stream instance
_price_stream: Optional[PriceStream] = None


def get_price_stream() -> PriceStream:
    """Get the global price stream instance."""
    global _price_stream
    if _price_stream is None:
        _price_stream = PriceStream(settings.polygon_api_key)
    return _price_stream


async def start_price_stream(pairs: list[str] = None) -> bool:
    """Start the global price stream."""
    stream = get_price_stream()
    if not stream.is_connected:
        if await stream.connect():
            await stream.subscribe(pairs or TRADING_PAIRS)
            return True
    return stream.is_connected


async def stop_price_stream():
    """Stop the global price stream."""
    stream = get_price_stream()
    await stream.disconnect()
