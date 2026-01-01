# backend/app.py
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.replay_streamer import ReplayManager
from backend.kafka_context_store import KafkaContextStore
from backend.rag_engine_simple import SimpleRagEngine


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# -------------------------------------------------------------------
# Kafka configuration (🚨 THIS IS THE IMPORTANT PART)
# -------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

KAFKA_CONFIG = {
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", ""),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", ""),
    "sasl_username": os.getenv("KAFKA_USERNAME", ""),
    "sasl_password": os.getenv("KAFKA_PASSWORD", ""),
}

print("🚀 KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP)


# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------
class StartReplayReq(BaseModel):
    game_id: str
    speed: float = 1.0


class ChatReq(BaseModel):
    message: str
    game_id: str


# -------------------------------------------------------------------
# Services (⚠️ created once, but Kafka connects lazily)
# -------------------------------------------------------------------
replay_manager = ReplayManager(
    project_root=PROJECT_ROOT,
    kafka_bootstrap=KAFKA_BOOTSTRAP,
    kafka_config=KAFKA_CONFIG,
)

context_store = KafkaContextStore(
    kafka_bootstrap=KAFKA_BOOTSTRAP,
    kafka_config=KAFKA_CONFIG,
)

rag_engine = SimpleRagEngine(context_store=context_store)


# -------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "kafka_bootstrap": KAFKA_BOOTSTRAP,
    }


@app.post("/api/replay/start")
def start_replay(req: StartReplayReq):
    replay_manager.start_replay(
        game_id=req.game_id,
        speed=req.speed,
    )
    return {"status": "started", "game_id": req.game_id}


@app.post("/api/replay/stop")
def stop_replay():
    replay_manager.stop_replay()
    return {"status": "stopped"}


@app.get("/api/replay/progress")
def replay_progress():
    return replay_manager.get_progress()


@app.post("/api/chat")
def chat(req: ChatReq):
    prog = replay_manager.get_progress()
    run_id = prog.get("run_id")

    if not run_id:
        return {
            "answer": "No active replay. Start streaming first.",
            "contexts": {},
        }

    return rag_engine.answer(
        question=req.message,
        match_id=req.game_id,
        run_id=run_id,
    )
