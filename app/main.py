"""
Forex Live Trading Simulation Service

FastAPI application with APScheduler for session-based trading.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from app.config import settings
from app.database import db
from app.services.scheduler import get_scheduler

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.active_connections.discard(connection)


ws_manager = ConnectionManager()

# Dashboard HTML path
TEMPLATES_DIR = Path(__file__).parent / "templates"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Starting Forex Live Trader...")
    await db.connect()

    # Initialize scheduler
    scheduler = get_scheduler()
    scheduler.start()
    logger.info("Trading scheduler initialized")

    logger.info("Forex Live Trader started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Forex Live Trader...")
    scheduler.stop()
    await db.disconnect()
    logger.info("Forex Live Trader shutdown complete")


app = FastAPI(
    title="Forex Live Trader",
    description="Low-latency forex trading simulation service",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the trading dashboard."""
    html_path = TEMPLATES_DIR / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(), status_code=200)
    return HTMLResponse(content="<h1>Dashboard not found</h1>", status_code=404)


@app.get("/api/info")
async def api_info():
    """API info endpoint."""
    return {
        "service": "Forex Live Trader",
        "version": "0.1.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        result = await db.fetchval("SELECT 1")
        db_status = "healthy" if result == 1 else "unhealthy"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "database": db_status,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/account")
async def get_account():
    """Get current account status."""
    row = await db.fetchrow("SELECT * FROM account WHERE id = 1")
    if row is None:
        return {
            "balance": float(settings.starting_balance),
            "initial_balance": float(settings.starting_balance),
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "peak_balance": float(settings.starting_balance),
            "max_drawdown_pct": 0.0,
        }

    win_rate = (
        (row["winning_trades"] / row["total_trades"] * 100)
        if row["total_trades"] > 0
        else 0.0
    )

    return {
        "balance": float(row["balance"]),
        "initial_balance": float(row["initial_balance"]),
        "total_trades": row["total_trades"],
        "winning_trades": row["winning_trades"],
        "losing_trades": row["losing_trades"],
        "win_rate": round(win_rate, 2),
        "peak_balance": float(row["peak_balance"]),
        "max_drawdown_pct": float(row["max_drawdown_pct"]),
        "pnl": float(row["balance"] - row["initial_balance"]),
        "pnl_pct": round(
            (row["balance"] - row["initial_balance"]) / row["initial_balance"] * 100, 2
        ),
    }


@app.get("/api/trades")
async def get_trades(limit: int = 50, offset: int = 0):
    """Get recent trades."""
    rows = await db.fetch(
        """
        SELECT * FROM trades
        ORDER BY session_datetime DESC
        LIMIT $1 OFFSET $2
        """,
        limit,
        offset,
    )
    return [dict(row) for row in rows]


@app.get("/api/percentiles")
async def get_all_percentiles():
    """Get all cached percentiles."""
    rows = await db.fetch(
        """
        SELECT * FROM percentile_targets
        ORDER BY pair, session_name
        """
    )
    return [dict(row) for row in rows]


@app.get("/api/percentiles/{pair}/{session_name}")
async def get_pair_percentiles(pair: str, session_name: str):
    """Get percentiles for a specific pair and session."""
    from app.database import get_percentiles

    result = await get_percentiles(pair, session_name)
    if result is None:
        return {"error": f"No percentiles found for {pair} {session_name}"}
    return result


@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get scheduler status."""
    scheduler = get_scheduler()
    return scheduler.get_status()


@app.post("/api/scheduler/start")
async def start_scheduler():
    """Start the trading scheduler."""
    scheduler = get_scheduler()
    scheduler.start()
    return {"status": "started", "details": scheduler.get_status()}


@app.post("/api/scheduler/stop")
async def stop_scheduler():
    """Stop the trading scheduler."""
    scheduler = get_scheduler()
    scheduler.stop()
    return {"status": "stopped"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await ws_manager.connect(websocket)
    try:
        # Send initial data
        account_data = await get_account()
        scheduler_data = get_scheduler().get_status()
        await websocket.send_json({"type": "account", "data": account_data})
        await websocket.send_json({"type": "scheduler", "data": scheduler_data})

        # Keep connection alive and listen for messages
        while True:
            try:
                # Wait for any message (ping/pong or client messages)
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send periodic updates every 30 seconds
                account_data = await get_account()
                scheduler_data = get_scheduler().get_status()
                await websocket.send_json({"type": "account", "data": account_data})
                await websocket.send_json({"type": "scheduler", "data": scheduler_data})
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


# Broadcast helper for scheduler to notify clients
async def broadcast_trade_update(trade_data: dict):
    """Broadcast trade update to all connected clients."""
    await ws_manager.broadcast({"type": "trade", "data": trade_data})


async def broadcast_account_update():
    """Broadcast account update to all connected clients."""
    account_data = await get_account()
    await ws_manager.broadcast({"type": "account", "data": account_data})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
