"""
AlgoForAll ArbEdge Backend
===============
Run: python3 main.py
Then open: http://localhost:8000        (homepage)
           http://localhost:8000/arb    (arbitrage finder)
           http://localhost:8000/docs   (API docs)
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.config import settings
from routers.arb import router as arb_router, _refresh_odds
from routers.props import router as props_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Absolute path to the oddsshopper/ folder (one level up from backend/)
FRONTEND_DIR = Path(__file__).parent.parent.resolve()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting AlgoForAll ArbEdge...")
    if settings.redis_url:
        from services import cache
        cache.init_redis(settings.redis_url)
    # Fetch once on startup — all subsequent refreshes are manual (click Refresh in UI)
    await _refresh_odds()
    logger.info("Odds loaded. Auto-refresh is OFF — use the Refresh button in the UI.")
    yield


app = FastAPI(
    title="AlgoForAll ArbEdge Arbitrage API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes first (must be registered before the catch-all static mount)
app.include_router(arb_router)
app.include_router(props_router)

# Serve CSS, JS, and other static assets from the frontend folder at /assets
app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR)), name="assets")


# Explicit HTML page routes — these must come after the API router
# but the static mount is last so it never intercepts /arb or /
@app.get("/", include_in_schema=False)
async def serve_home():
    f = FRONTEND_DIR / "index.html"
    if not f.exists():
        return {"error": f"index.html not found at {f}"}
    return FileResponse(str(f))


@app.get("/arb", include_in_schema=False)
async def serve_arb():
    f = FRONTEND_DIR / "arb.html"
    if not f.exists():
        return {"error": f"arb.html not found at {f}"}
    return FileResponse(str(f))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
        log_level="info",
    )
