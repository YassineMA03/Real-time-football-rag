# backend/qdrant_ingest.py
from __future__ import annotations

import os
import uuid
from typing import Optional, Dict, Any

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient


QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")


class QdrantIngestor:
    """
    Stores streamed items in Qdrant for RAG retrieval.
    Payload fields are designed for filtering by:
      - match_id (game_id)
      - run_id (optional)
      - minute window
      - source type: comment/event/score
    """

    def __init__(self):
        if not QDRANT_URL:
            raise ValueError("Missing env var QDRANT_URL")
        if not QDRANT_API_KEY:
            raise ValueError("Missing env var QDRANT_API_KEY")

        self.embedder = SentenceTransformer(EMBED_MODEL_NAME)
        self.qdrant = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY,
            check_compatibility=False,
        )

    def store_stream_item(
        self,
        *,
        run_id: str,
        match_id: str,   # your game_id
        minute: int,
        tsec: int,
        source: str,     # "comment" | "event" | "score"
        text: str,
        team: Optional[str] = None,
        event_type: Optional[str] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        text = (text or "").strip()
        if not text:
            return

        vec = self.embedder.encode(text).tolist()

        payload: Dict[str, Any] = {
            "run_id": run_id,
            "match_id": match_id,
            "minute": int(minute),
            "tsec": int(tsec),
            "source": source,
            "team": team,
            "event_type": event_type,
            "text": text,
        }
        if extra_payload:
            payload.update(extra_payload)

        point = {
            "id": str(uuid.uuid4()),
            "vector": vec,
            "payload": payload,
        }

        self.qdrant.upsert(collection_name=QDRANT_COLLECTION, points=[point])
