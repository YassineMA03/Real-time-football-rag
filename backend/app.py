from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.replay_streamer import ReplayManager
from backend.rag_engine import RagEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]  # repo root
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
rag_engine = RagEngine()


# ------------- Models -------------

class StartGameItem(BaseModel):
    game_id: str
    startAtSec: int = 0


class StartReplayReq(BaseModel):
    games: List[StartGameItem]


class ChatReq(BaseModel):
    game_id: str
    message: str
    top_k: int = 6
    minute_window: Optional[int] = 10  # last 10 minutes by default


# ------------- Helpers -------------

def next_run_id() -> str:
    return "run_" + str(int(time.time()))


# ------------- API -------------

@app.get("/api/games")
def list_games():
    return replay_manager.list_games()


@app.get("/api/replay/progress")
def replay_progress():
    return replay_manager.get_progress()


@app.post("/api/replay/reset")
def replay_reset():
    replay_manager.stop_all()
    return {"ok": True}


@app.post("/api/replay/start")
def start_replay(req: StartReplayReq):
    run_id = next_run_id()
    start_map: Dict[str, int] = {g.game_id: int(g.startAtSec) for g in req.games}
    replay_manager.start_games(run_id=run_id, game_start_offsets_sec=start_map, tick_sec=TICK_SEC_DEFAULT)
    return {"ok": True, "run_id": run_id, "games": start_map}


@app.post("/api/chat")
def chat(req: ChatReq):
    # DEBUG: Print what we received
    print("\n" + "="*70)
    print("📨 CHAT REQUEST RECEIVED")
    print("="*70)
    print(f"game_id: {req.game_id}")
    print(f"message: {req.message}")
    print(f"top_k: {req.top_k}")
    print(f"minute_window: {req.minute_window}")
    
    # take current minute from progress (for minute_window retrieval)
    prog = replay_manager.get_progress()
    print(f"\n📊 Current Progress:")
    print(f"  run_id: {prog.get('run_id')}")
    print(f"  active_game_ids: {prog.get('active_game_ids')}")
    
    p = (prog.get("progress") or {}).get(req.game_id) or {}
    current_minute = p.get("known_minute")
    run_id = prog.get("run_id")  # isolate by current run
    
    print(f"\n🎮 Game Info:")
    print(f"  current_minute: {current_minute}")
    print(f"  run_id: {run_id}")
    print(f"  game progress: {p}")
    
    print(f"\n🔍 Calling RAG engine with:")
    print(f"  match_id: {req.game_id}")
    print(f"  run_id: {run_id}")
    print(f"  current_minute: {current_minute}")
    print(f"  minute_window: {req.minute_window}")
    print("="*70 + "\n")

    result = rag_engine.answer(
        question=req.message,
        match_id=req.game_id,
        top_k=req.top_k,
        minute_window=req.minute_window,
        current_minute=int(current_minute) if current_minute is not None else None,
        run_id=run_id,
    )
    
    print(f"✅ RAG Response:")
    print(f"  contexts found: {len(result.get('contexts', []))}")
    print(f"  answer: {result.get('answer', '')[:100]}...")
    print("="*70 + "\n")
    
    return result