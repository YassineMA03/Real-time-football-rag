from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.replay_streamer import ReplayManager
from backend.rag_engine_simple import SimpleRagEngine
from backend.kafka_context_store import KafkaContextStore


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

# Initialize services
replay_manager = ReplayManager(project_root=PROJECT_ROOT, kafka_bootstrap=KAFKA_BOOTSTRAP)
rag_engine = SimpleRagEngine()
context_store = KafkaContextStore(project_root=PROJECT_ROOT, kafka_bootstrap=KAFKA_BOOTSTRAP, top_k=10)


# ------------- Models -------------

class StartGameItem(BaseModel):
    game_id: str
    startAtMinute: int = 0
    startAtExtra: int = 0


class StartReplayReq(BaseModel):
    games: List[StartGameItem]


class ChatReq(BaseModel):
    game_id: str
    message: str


# ------------- Helpers -------------

def next_run_id() -> str:
    return "run_" + str(int(time.time()))


# ------------- API -------------

@app.get("/")
def root():
    """Root endpoint with service status."""
    return {
        "service": "Football RAG System (Simple Time-Based)",
        "status": "running",
        "approach": "File-based context retrieval (No vector search)",
        "context_files": [
            "rag_comments.txt - Last 10 comments",
            "rag_events.txt - Last 10 events", 
            "rag_scores.txt - Last 10 goals"
        ],
        "endpoints": {
            "games": "/api/games",
            "replay_start": "/api/replay/start",
            "replay_progress": "/api/replay/progress",
            "replay_reset": "/api/replay/reset",
            "chat": "/api/chat",
            "docs": "/docs"
        }
    }


@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "context_store_running": context_store.get_run_id() is not None,
        "timestamp": int(time.time())
    }


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
    
    # Convert minute+extra to seconds
    # Formula: (minute + extra) * 60
    start_map: Dict[str, int] = {
        g.game_id: (g.startAtMinute + g.startAtExtra) * 60 
        for g in req.games
    }
    
    print(f"\n🎬 Starting replay:")
    print(f"   Run ID: {run_id}")
    for g in req.games:
        total_sec = (g.startAtMinute + g.startAtExtra) * 60
        print(f"   {g.game_id}: minute={g.startAtMinute}, extra={g.startAtExtra} → {total_sec}s")
    
    # Start the replay manager (Kafka producer)
    replay_manager.start_games(run_id=run_id, game_start_offsets_sec=start_map, tick_sec=TICK_SEC_DEFAULT)
    
    # Start the context store (Kafka consumer)
    game_ids = list(start_map.keys())
    context_store.start(run_id=run_id, active_game_ids=game_ids, game_start_offsets_sec=start_map)
    
    print(f"   Context files: backend/runtime/{run_id}/{{game_id}}/")
    print("="*70 + "\n")
    
    return {"ok": True, "run_id": run_id, "games": start_map}


@app.post("/api/chat")
def chat(req: ChatReq):
    # DEBUG: Print what we received
    print("\n" + "="*70)
    print("💬 CHAT REQUEST RECEIVED")
    print("="*70)
    print(f"game_id: {req.game_id}")
    print(f"message: {req.message}")
    
    # Get current progress
    prog = replay_manager.get_progress()
    run_id = prog.get("run_id")
    
    print(f"\n📊 Current State:")
    print(f"  run_id: {run_id}")
    print(f"  active_games: {prog.get('active_game_ids')}")
    
    if not run_id:
        return {
            "answer": "No active stream. Please start a replay first.",
            "contexts": {}
        }
    
    # Call simple RAG engine (reads from files)
    print(f"\n🔍 Calling Simple RAG Engine...")
    print(f"  Will read context from: backend/runtime/{run_id}/{req.game_id}/")
    
    result = rag_engine.answer(
        question=req.message,
        match_id=req.game_id,
        run_id=run_id,
    )
    
    print(f"\n✅ RAG Response:")
    print(f"  Answer: {result.get('answer', '')[:100]}...")
    print("="*70 + "\n")
    
    return result


@app.on_event("shutdown")
def shutdown_event():
    """Clean shutdown of context store."""
    print("\n🛑 Shutting down...")
    context_store.stop()
    print("✅ Context store stopped")