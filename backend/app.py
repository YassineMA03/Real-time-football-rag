from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.replay_streamer import ReplayManager
from backend.rag_engine_simple import SimpleRagEngine
from backend.kafka_context_store import KafkaContextStore

# Try to import vector RAG engine (optional for deep think mode)
try:
    from backend.rag_engine import RagEngine
    VECTOR_RAG_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Warning: Vector RAG engine not available: {e}")
    print("   Deep Think mode will be disabled. Only Fast mode will work.")
    print("   To enable Deep Think, ensure rag_engine.py is properly configured.")
    RagEngine = None
    VECTOR_RAG_AVAILABLE = False


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
simple_rag_engine = SimpleRagEngine()  # Fast mode - file-based
vector_rag_engine = RagEngine() if VECTOR_RAG_AVAILABLE else None  # Deep think mode - vector search
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
    deep_think: bool = False  # Toggle between fast mode and deep think mode


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
    
    # Keep minute+extra as tuples (NO conversion to seconds!)
    # This preserves the distinction between (45, 2) and (47, 0)
    start_map: Dict[str, Tuple[int, int]] = {
        g.game_id: (g.startAtMinute, g.startAtExtra)
        for g in req.games
    }
    
    print(f"\n🎬 Starting replay:")
    print(f"   Run ID: {run_id}")
    for g in req.games:
        print(f"   {g.game_id}: ({g.startAtMinute}, {g.startAtExtra}) = {g.startAtMinute}' +{g.startAtExtra}'")
    
    # Start the replay manager (Kafka producer)
    replay_manager.start_games(run_id=run_id, game_start_offsets=start_map, tick_sec=TICK_SEC_DEFAULT)
    
    # Start the context store (Kafka consumer)
    game_ids = list(start_map.keys())
    context_store.start(run_id=run_id, active_game_ids=game_ids, game_start_offsets=start_map)
    
    print(f"   Context files: backend/runtime/{run_id}/{{game_id}}/")
    print("="*70 + "\n")
    
    return {"ok": True, "run_id": run_id, "games": {k: f"{v[0]}'+{v[1]}'" for k, v in start_map.items()}}


@app.post("/api/chat")
def chat(req: ChatReq):
    # DEBUG: Print what we received
    print("\n" + "="*70)
    mode_icon = "🧠" if req.deep_think else "⚡"
    mode_name = "DEEP THINK" if req.deep_think else "FAST"
    print(f"{mode_icon} CHAT REQUEST - {mode_name} MODE")
    print("="*70)
    print(f"game_id: {req.game_id}")
    print(f"message: {req.message}")
    print(f"deep_think: {req.deep_think}")
    
    # Get current progress
    prog = replay_manager.get_progress()
    run_id = prog.get("run_id")
    
    print(f"\n📊 Current State:")
    print(f"  run_id: {run_id}")
    print(f"  active_games: {prog.get('active_game_ids')}")
    
    if not run_id:
        return {
            "answer": "No active stream. Please start a replay first.",
            "contexts": {},
            "mode": "none"
        }
    
    # Choose RAG engine based on mode
    if req.deep_think:
        # DEEP THINK MODE: Use vector search on all historical data
        if not VECTOR_RAG_AVAILABLE or vector_rag_engine is None:
            return {
                "answer": "Deep Think mode is not available. Vector RAG engine (rag_engine.py) is not properly configured. Please use Fast mode or check your setup.",
                "contexts": [],
                "mode": "error"
            }
        
        print(f"\n🧠 Using Vector RAG Engine (Deep Think)...")
        print(f"  Searching ALL historical data with semantic similarity")
        
        result = vector_rag_engine.answer(
            question=req.message,
            match_id=req.game_id,
            run_id=run_id,
            deep_think=True,  # Enable deep think mode
        )
    else:
        # FAST MODE: Use simple file-based retrieval
        print(f"\n⚡ Using Simple RAG Engine (Fast)...")
        print(f"  Will read context from: backend/runtime/{run_id}/{req.game_id}/")
        
        result = simple_rag_engine.answer(
            question=req.message,
            match_id=req.game_id,
            run_id=run_id,
        )
    
    print(f"\n✅ RAG Response ({mode_name}):")
    print(f"  Answer: {result.get('answer', '')[:100]}...")
    print("="*70 + "\n")
    
    return result


@app.on_event("startup")
async def startup_event():
    """Start Kafka consumers automatically on backend startup."""
    print("\n" + "="*70)
    print("🚀 STARTING FOOTBALL RAG SYSTEM")
    print("="*70)
    
    # Start Kafka → Context Store consumer
    print("\n📡 Starting Kafka → Context Store consumer...")
    print("   - Listening to: games.comments, games.events, games.scores")
    print("   - Writing context files to: backend/runtime/")
    print("   ✅ Context consumer ready (starts when match begins)")
    
    # Start Kafka → Qdrant consumer (for Deep Think mode)
    if VECTOR_RAG_AVAILABLE:
        print("\n📡 Starting Kafka → Qdrant consumer...")
        try:
            # Import and start the Qdrant consumer in a background thread
            import subprocess
            import threading
            
            def run_qdrant_consumer():
                """Run Qdrant consumer in background"""
                try:
                    # Start as subprocess
                    process = subprocess.Popen(
                        [sys.executable, "backend/kafka_to_qdrant_consumer.py"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    
                    # Store process for shutdown
                    app.state.qdrant_consumer_process = process
                    
                    # Log output
                    for line in process.stdout:
                        if line.strip():
                            print(f"   [Qdrant] {line.strip()}")
                    
                except Exception as e:
                    print(f"   ⚠️ Qdrant consumer error: {e}")
            
            # Start in background thread
            consumer_thread = threading.Thread(target=run_qdrant_consumer, daemon=True)
            consumer_thread.start()
            app.state.qdrant_consumer_thread = consumer_thread
            
            print("   ✅ Qdrant consumer starting in background")
            print("   - Ingesting data to Qdrant for Deep Think mode")
            
        except Exception as e:
            print(f"   ⚠️ Could not start Qdrant consumer: {e}")
            print("   - Deep Think mode will not work until Qdrant consumer is started manually")
    else:
        print("\n⚠️  Qdrant consumer not started (Vector RAG unavailable)")
    
    print("\n✅ Backend ready!")
    print("="*70 + "\n")


@app.on_event("shutdown")
def shutdown_event():
    """Clean shutdown of all consumers."""
    print("\n" + "="*70)
    print("🛑 SHUTTING DOWN")
    print("="*70)
    
    print("\n📡 Stopping Context Store consumer...")
    context_store.stop()
    print("   ✅ Context store stopped")
    
    # Stop Qdrant consumer if running
    if hasattr(app.state, 'qdrant_consumer_process'):
        print("\n📡 Stopping Qdrant consumer...")
        try:
            app.state.qdrant_consumer_process.terminate()
            app.state.qdrant_consumer_process.wait(timeout=5)
            print("   ✅ Qdrant consumer stopped")
        except Exception as e:
            print(f"   ⚠️ Error stopping Qdrant consumer: {e}")
    
    print("\n✅ Shutdown complete")
    print("="*70 + "\n")