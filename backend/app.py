# backend/app.py
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi import APIRouter

from backend.replay_streamer import ReplayManager
from backend.kafka_context_store import KafkaContextStore
from backend.rag_engine_simple import SimpleRagEngine

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ✅ IMPORTANT: use env var on Railway. Local dev still defaults to localhost.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TICK_SEC_DEFAULT = int(os.getenv("TICK_SEC_DEFAULT", "60"))

# Optional (for later if you use secured Kafka like Redpanda Cloud)
# For Railway Kafka (internal), you can leave these empty.
KAFKA_CONFIG = {}
security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "").strip()
sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "").strip()
sasl_username = os.getenv("KAFKA_USERNAME", "").strip()
sasl_password = os.getenv("KAFKA_PASSWORD", "").strip()

if security_protocol:
    KAFKA_CONFIG["security_protocol"] = security_protocol
if sasl_mechanism:
    KAFKA_CONFIG["sasl_mechanism"] = sasl_mechanism
if sasl_username:
    KAFKA_CONFIG["sasl_plain_username"] = sasl_username
if sasl_password:
    KAFKA_CONFIG["sasl_plain_password"] = sasl_password

print("KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ "https://frontend-production-6b8f.up.railway.app",  # <-- your frontend public URL
        "http://localhost:3000",
        "http://localhost:3001",],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class StartGameItem(BaseModel):
    game_id: str
    startAtMinute: int = 0
    startAtExtra: int = 0


class StartReplayReq(BaseModel):
    games: List[StartGameItem]


class ChatReq(BaseModel):
    game_id: str
    message: str


# ✅ These are safe now because producer/consumers connect lazily (see below).
replay_manager = ReplayManager(
    project_root=PROJECT_ROOT,
    kafka_bootstrap=KAFKA_BOOTSTRAP,
    kafka_config=KAFKA_CONFIG,
)

context_store = KafkaContextStore(
    project_root=PROJECT_ROOT,
    kafka_bootstrap=KAFKA_BOOTSTRAP,
    top_k=10,
    kafka_config=KAFKA_CONFIG,
)

rag_engine = SimpleRagEngine(project_root=PROJECT_ROOT)


def next_run_id() -> str:
    return "run_" + str(int(time.time()))


@app.get("/")
def root():
    return {"ok": True}


@app.get("/api/games")
def list_games():
    return replay_manager.list_games()


@app.get("/api/replay/progress")
def replay_progress():
    return replay_manager.get_progress()


@app.post("/api/replay/reset")
def replay_reset():
    replay_manager.stop_all()
    context_store.reset()
    return {"ok": True}


@app.post("/api/replay/start")
def start_replay(req: StartReplayReq):
    run_id = next_run_id()

    start_map: Dict[str, Tuple[int, int]] = {
        g.game_id: (int(g.startAtMinute), int(g.startAtExtra))
        for g in req.games
    }

    # 1) Context store bootstrap + consumers
    context_store.reset()
    context_store.start(run_id=run_id, active_game_ids=list(start_map.keys()), game_start_offsets=start_map)

    # 2) Producer starts streaming
    replay_manager.start_games(run_id=run_id, game_start_offsets=start_map, tick_sec=TICK_SEC_DEFAULT)

    return {"ok": True, "run_id": run_id}


@app.post("/api/chat")
def chat(req: ChatReq):
    prog = replay_manager.get_progress()
    run_id = prog.get("run_id")

    if not run_id:
        return {"answer": "No active replay. Start streaming first.", "contexts": {}}

    return rag_engine.answer(
        question=req.message,
        match_id=req.game_id,
        run_id=run_id,
    )


@app.get("/api/debug/fs")
def debug_fs():
    base = Path("/app")
    return {
        "app_exists": base.exists(),
        "app": [p.name for p in base.iterdir()] if base.exists() else [],
        "data_exists": (base / "data").exists(),
        "data": [p.name for p in (base / "data").iterdir()] if (base / "data").exists() else [],
        "games_exists": (base / "data" / "games").exists(),
        "games": [p.name for p in (base / "data" / "games").iterdir()] if (base / "data" / "games").exists() else [],
    }
