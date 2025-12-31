# backend/app.py
from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.replay_streamer import ReplayManager
from backend.kafka_context_store import KafkaContextStore
from backend.rag_engine_simple import SimpleRagEngine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
KAFKA_BOOTSTRAP = "localhost:9092"
TICK_SEC_DEFAULT = 60

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

replay_manager = ReplayManager(project_root=PROJECT_ROOT, kafka_bootstrap=KAFKA_BOOTSTRAP)
context_store = KafkaContextStore(project_root=PROJECT_ROOT, kafka_bootstrap=KAFKA_BOOTSTRAP, top_k=10)
rag_engine = SimpleRagEngine(project_root=PROJECT_ROOT)


class StartGameItem(BaseModel):
    game_id: str
    startAtMinute: int = 0
    startAtExtra: int = 0


class StartReplayReq(BaseModel):
    games: List[StartGameItem]


class ChatReq(BaseModel):
    game_id: str
    message: str


def next_run_id() -> str:
    return "run_" + str(int(time.time()))


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

    print("\n" + "=" * 70)
    print("🎬 START REPLAY")
    print("=" * 70)
    print(f"run_id: {run_id}")
    print("games:")
    for gid, (m, x) in start_map.items():
        print(f"  - {gid}: {m}'+{x}'")
    print("=" * 70 + "\n")

    # 1) context store bootstrap + consumers
    context_store.reset()
    context_store.start(run_id=run_id, active_game_ids=list(start_map.keys()), game_start_offsets=start_map)

    # 2) start producer
    replay_manager.start_games(run_id=run_id, game_start_offsets=start_map, tick_sec=TICK_SEC_DEFAULT)

    return {"ok": True, "run_id": run_id, "games": {k: f"{v[0]}'+{v[1]}'" for k, v in start_map.items()}}


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