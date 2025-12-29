# backend/kafka_to_qdrant_consumer.py
"""
Kafka → Qdrant Consumer Service

This service acts as the data ingestion pipeline that would normally receive
data from a real streaming API. In your architecture:
  
  Real API → Kafka → THIS CONSUMER → Qdrant → RAG

Since you're simulating the API with replay_streamer.py, the flow becomes:
  
  Replay Producer → Kafka → THIS CONSUMER → Qdrant → RAG

This consumer:
  1. Subscribes to Kafka topics (comments, events, scores)
  2. Processes incoming messages
  3. Generates embeddings
  4. Stores in Qdrant with proper metadata

This is a standalone service that should run independently from the FastAPI app.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
import uuid
from typing import Optional

from kafka import KafkaConsumer
from qdrant_client import QdrantClient


# -------------------------
# Configuration
# -------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")

# Consumer group ID - use consistent group to avoid reprocessing
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "qdrant-ingestor-group")


# -------------------------
# Kafka to Qdrant Ingestor
# -------------------------

class KafkaToQdrantConsumer:
    """
    Consumes Kafka topics and ingests into Qdrant for RAG.
    
    This simulates what would happen if you were consuming from a real
    streaming API that publishes to Kafka.
    """

    def __init__(self):
        print("🔧 Initializing Kafka → Qdrant Consumer...")
        
        if not QDRANT_URL:
            raise ValueError("Missing env var QDRANT_URL")
        if not QDRANT_API_KEY:
            raise ValueError("Missing env var QDRANT_API_KEY")

        # Don't initialize embedder here to avoid threading issues
        # Will be lazy-loaded in each thread
        self._embedder = None
        self._embedder_lock = threading.Lock()
        
        # Initialize Qdrant client
        print(f"🔗 Connecting to Qdrant: {QDRANT_URL}")
        self.qdrant = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY,
            check_compatibility=False,
        )
        
        # Verify collection exists
        try:
            collection_info = self.qdrant.get_collection(QDRANT_COLLECTION)
            print(f"✅ Connected to collection '{QDRANT_COLLECTION}'")
            print(f"   Points count: {collection_info.points_count}")
        except Exception as e:
            print(f"⚠️  Collection '{QDRANT_COLLECTION}' not found or error: {e}")
            print("   You may need to create it first!")
            
        self._stop = threading.Event()
        self._threads = []
        
        # Stats
        self._stats_lock = threading.Lock()
        self._comments_processed = 0
        self._events_processed = 0
        self._scores_processed = 0
        self._errors = 0
        
        print("✅ Initialization complete!\n")

    def _get_embedder(self):
        """Lazy-load embedder to avoid threading issues."""
        if self._embedder is None:
            with self._embedder_lock:
                if self._embedder is None:
                    print(f"📦 Loading embedding model in thread {threading.current_thread().name}: {EMBED_MODEL_NAME}")
                    from sentence_transformers import SentenceTransformer
                    self._embedder = SentenceTransformer(EMBED_MODEL_NAME)
        return self._embedder

    def start(self, background=False):
        """
        Start all consumer threads.
        
        Args:
            background: If True, returns immediately after starting threads.
                       If False, blocks and prints stats (for standalone mode).
        """
        print("🚀 Starting Kafka consumers...")
        print(f"   Bootstrap servers: {KAFKA_BOOTSTRAP}")
        print(f"   Consumer group: {CONSUMER_GROUP}")
        print(f"   Topics: games.comments, games.events, games.scores\n")
        
        # Start 3 consumer threads (one per topic)
        threads = [
            threading.Thread(target=self._consume_comments, daemon=True, name="CommentConsumer"),
            threading.Thread(target=self._consume_events, daemon=True, name="EventConsumer"),
            threading.Thread(target=self._consume_scores, daemon=True, name="ScoreConsumer"),
        ]
        
        for t in threads:
            t.start()
            self._threads.append(t)
            
        print("✅ All consumers started!")
        
        if background:
            # Start stats printer in background thread
            stats_thread = threading.Thread(target=self._print_stats_loop, daemon=True, name="StatsReporter")
            stats_thread.start()
            print("📊 Stats will be printed every 10 seconds\n")
        else:
            # Block and print stats in foreground (standalone mode)
            print("📊 Waiting for messages... (Press Ctrl+C to stop)\n")
            self._print_stats_loop()

    def stop(self):
        """Stop all consumers."""
        print("\n🛑 Stopping consumers...")
        self._stop.set()
        
        for t in self._threads:
            t.join(timeout=5)
            
        print("✅ All consumers stopped")
        self._print_final_stats()

    def _print_stats_loop(self):
        """Print stats every 10 seconds."""
        try:
            while not self._stop.is_set():
                time.sleep(10)
                if not self._stop.is_set():
                    self._print_stats()
        except KeyboardInterrupt:
            pass

    def _print_stats(self):
        """Print current processing stats."""
        with self._stats_lock:
            total = self._comments_processed + self._events_processed + self._scores_processed
            print(f"📊 Stats: Comments={self._comments_processed} | Events={self._events_processed} | "
                  f"Scores={self._scores_processed} | Total={total} | Errors={self._errors}")

    def _print_final_stats(self):
        """Print final stats on shutdown."""
        print("\n" + "="*70)
        print("📊 FINAL STATISTICS")
        print("="*70)
        with self._stats_lock:
            print(f"  Comments ingested: {self._comments_processed}")
            print(f"  Events ingested:   {self._events_processed}")
            print(f"  Scores ingested:   {self._scores_processed}")
            print(f"  Total ingested:    {self._comments_processed + self._events_processed + self._scores_processed}")
            print(f"  Errors:            {self._errors}")
        print("="*70 + "\n")

    # -------------------------
    # Consumer threads
    # -------------------------

    def _consume_comments(self):
        """Consume games.comments topic."""
        try:
            consumer = KafkaConsumer(
                "games.comments",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                enable_auto_commit=True,
                auto_offset_reset="earliest",  # Start from beginning for new consumer groups
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            
            print("✅ Comment consumer started")
            
            while not self._stop.is_set():
                for msg in consumer:
                    if self._stop.is_set():
                        break
                        
                    try:
                        payload = msg.value
                        self._ingest_comment(payload)
                        with self._stats_lock:
                            self._comments_processed += 1
                    except Exception as e:
                        print(f"❌ Error processing comment: {e}")
                        with self._stats_lock:
                            self._errors += 1
                        
            consumer.close()
            print("🛑 Comment consumer stopped")
        except Exception as e:
            print(f"❌ Comment consumer error: {e}")

    def _consume_events(self):
        """Consume games.events topic."""
        try:
            consumer = KafkaConsumer(
                "games.events",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            
            print("✅ Event consumer started")
            
            while not self._stop.is_set():
                for msg in consumer:
                    if self._stop.is_set():
                        break
                        
                    try:
                        payload = msg.value
                        self._ingest_event(payload)
                        with self._stats_lock:
                            self._events_processed += 1
                    except Exception as e:
                        print(f"❌ Error processing event: {e}")
                        with self._stats_lock:
                            self._errors += 1
                        
            consumer.close()
            print("🛑 Event consumer stopped")
        except Exception as e:
            print(f"❌ Event consumer error: {e}")

    def _consume_scores(self):
        """Consume games.scores topic."""
        try:
            consumer = KafkaConsumer(
                "games.scores",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            
            print("✅ Score consumer started")
            
            while not self._stop.is_set():
                for msg in consumer:
                    if self._stop.is_set():
                        break
                        
                    try:
                        payload = msg.value
                        self._ingest_score(payload)
                        with self._stats_lock:
                            self._scores_processed += 1
                    except Exception as e:
                        print(f"❌ Error processing score: {e}")
                        with self._stats_lock:
                            self._errors += 1
                        
            consumer.close()
            print("🛑 Score consumer stopped")
        except Exception as e:
            print(f"❌ Score consumer error: {e}")

    # -------------------------
    # Ingestion handlers
    # -------------------------

    def _ingest_comment(self, payload: dict):
        """Process a comment from Kafka and store in Qdrant."""
        run_id = str(payload.get("run_id", ""))
        game_id = str(payload.get("game_id", ""))
        tsec = int(payload.get("event_time_sec", 0))
        minute = int(payload.get("minute", 0))
        text = str(payload.get("text", "")).strip()
        comment_type = str(payload.get("type", "Commentary"))
        
        if not text:
            return
            
        # Generate embedding (lazy-loaded per thread)
        embedder = self._get_embedder()
        vec = embedder.encode(text).tolist()
        
        # Prepare payload for Qdrant
        point_payload = {
            "run_id": run_id,
            "match_id": game_id,
            "minute": minute,
            "tsec": tsec,
            "source": "comment",
            "text": text,
            "event_type": comment_type,
            "team": None,  # Comments don't have team info
        }
        
        # Store in Qdrant
        point = {
            "id": str(uuid.uuid4()),
            "vector": vec,
            "payload": point_payload,
        }
        
        self.qdrant.upsert(collection_name=QDRANT_COLLECTION, points=[point])

    def _ingest_event(self, payload: dict):
        """Process an event from Kafka and store in Qdrant."""
        run_id = str(payload.get("run_id", ""))
        game_id = str(payload.get("game_id", ""))
        tsec = int(payload.get("event_time_sec", 0))
        minute = tsec // 60
        event_type = str(payload.get("type", "Event"))
        
        # Extract event data
        event_data = payload.get("data", {})
        if not isinstance(event_data, dict):
            event_data = {}
            
        # Build event summary text
        text = self._build_event_summary(event_data)
        
        if not text:
            return
            
        # Extract team info
        team_name = self._extract_team_name(event_data)
        
        # Generate embedding (lazy-loaded per thread)
        embedder = self._get_embedder()
        vec = embedder.encode(text).tolist()
        
        # Prepare payload for Qdrant
        point_payload = {
            "run_id": run_id,
            "match_id": game_id,
            "minute": minute,
            "tsec": tsec,
            "source": "event",
            "text": text,
            "event_type": event_type,
            "team": team_name,
        }
        
        # Store in Qdrant
        point = {
            "id": str(uuid.uuid4()),
            "vector": vec,
            "payload": point_payload,
        }
        
        self.qdrant.upsert(collection_name=QDRANT_COLLECTION, points=[point])

    def _ingest_score(self, payload: dict):
        """Process a score update from Kafka and store in Qdrant."""
        run_id = str(payload.get("run_id", ""))
        game_id = str(payload.get("game_id", ""))
        tsec = int(payload.get("event_time_sec", 0))
        minute = tsec // 60
        score_str = str(payload.get("score_str", "0-0"))
        why = str(payload.get("why", "score update"))
        
        # Build text for embedding
        text = f"Score update: {score_str}. Reason: {why}"
        
        # Generate embedding (lazy-loaded per thread)
        embedder = self._get_embedder()
        vec = embedder.encode(text).tolist()
        
        # Prepare payload for Qdrant
        point_payload = {
            "run_id": run_id,
            "match_id": game_id,
            "minute": minute,
            "tsec": tsec,
            "source": "score",
            "text": text,
            "event_type": "ScoreUpdate",
            "team": None,
        }
        
        # Store in Qdrant
        point = {
            "id": str(uuid.uuid4()),
            "vector": vec,
            "payload": point_payload,
        }
        
        self.qdrant.upsert(collection_name=QDRANT_COLLECTION, points=[point])

    # -------------------------
    # Helper methods
    # -------------------------

    def _build_event_summary(self, event_data: dict) -> str:
        """Build a readable summary from event data."""
        etype = event_data.get("type") or event_data.get("event") or event_data.get("name") or "Event"
        detail = event_data.get("detail")
        team = self._extract_team_name(event_data)
        
        player = None
        if isinstance(event_data.get("player"), dict):
            player = event_data["player"].get("name")
        
        parts = [str(etype)]
        if isinstance(detail, str) and detail.strip():
            parts.append(detail.strip())
        if isinstance(team, str):
            parts.append(f"team={team}")
        if isinstance(player, str) and player.strip():
            parts.append(f"player={player.strip()}")
            
        return " | ".join(parts)

    def _extract_team_name(self, event_data: dict) -> Optional[str]:
        """Extract team name from event data."""
        team = event_data.get("team")
        if isinstance(team, dict):
            name = team.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
        return None


# -------------------------
# Main entry point
# -------------------------

def main():
    """Run the Kafka → Qdrant consumer service."""
    # Set multiprocessing start method to 'spawn' for better compatibility
    # This helps avoid issues with forked processes on macOS
    import multiprocessing
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        pass  # Already set
    
    print("\n" + "="*70)
    print("⚽ KAFKA → QDRANT CONSUMER SERVICE")
    print("="*70)
    print("This service consumes football match data from Kafka topics")
    print("and ingests it into Qdrant for RAG-based chat.\n")
    print("Configuration:")
    print(f"  Kafka:      {KAFKA_BOOTSTRAP}")
    print(f"  Qdrant:     {QDRANT_URL}")
    print(f"  Collection: {QDRANT_COLLECTION}")
    print(f"  Group ID:   {CONSUMER_GROUP}")
    print("="*70 + "\n")
    
    consumer = KafkaToQdrantConsumer()
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        consumer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consuming (foreground mode for standalone)
    consumer.start(background=False)


if __name__ == "__main__":
    main()