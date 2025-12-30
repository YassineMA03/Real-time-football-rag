# backend/rag_engine.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
from mistralai import Mistral


QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")

EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")


@dataclass
class RetrievedItem:
    minute: int
    tsec: int
    source: str
    text: str
    event_type: Optional[str] = None
    team: Optional[str] = None


class RagEngine:
    """
    Same logic as your notebook:
      1) embed query
      2) retrieve from Qdrant (filtered by match_id, optional run_id + minute window)
      3) build prompt
      4) call Mistral
    """

    def __init__(self):
        if not QDRANT_URL:
            raise ValueError("Missing env var QDRANT_URL")
        if not QDRANT_API_KEY:
            raise ValueError("Missing env var QDRANT_API_KEY")
        if not MISTRAL_API_KEY:
            raise ValueError("Missing env var MISTRAL_API_KEY")

        self.embedder = SentenceTransformer(EMBED_MODEL_NAME)
        self.qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        self.mistral = Mistral(api_key=MISTRAL_API_KEY)

    def retrieve(
        self,
        user_query: str,
        match_id: str,
        top_k: int = 6,
        minute_gte: Optional[int] = None,
        minute_lte: Optional[int] = None,
        run_id: Optional[str] = None,
        deep_think: bool = False,
    ) -> List[RetrievedItem]:
        try:
            qvec = self.embedder.encode(user_query).tolist()

            must = [
                FieldCondition(key="match_id", match=MatchValue(value=match_id)),
            ]
            
            # IMPORTANT: Only filter by run_id if it's explicitly provided AND not None
            # This allows querying data without run_id (legacy data)
            if run_id and run_id != "None":
                must.append(FieldCondition(key="run_id", match=MatchValue(value=run_id)))

            # In deep_think mode, we DON'T filter by minute - we search ALL history
            if not deep_think:
                if minute_gte is not None or minute_lte is not None:
                    must.append(
                        FieldCondition(
                            key="minute",
                            range=Range(
                                gte=minute_gte if minute_gte is not None else None,
                                lte=minute_lte if minute_lte is not None else None,
                            ),
                        )
                    )

            mode_str = "🧠 DEEP THINK" if deep_think else "⚡ FAST"
            minute_range = "ALL" if deep_think else f"{minute_gte}-{minute_lte}"
            print(f"{mode_str} RAG Query - match_id: {match_id}, run_id: {run_id}, minute range: {minute_range}")

            # Try different methods depending on qdrant-client version
            try:
                # Newer API (>= 1.7.0)
                results = self.qdrant.query_points(
                    collection_name=QDRANT_COLLECTION,
                    query=qvec,
                    limit=top_k,
                    with_payload=True,
                    query_filter=Filter(must=must),
                ).points
            except AttributeError:
                # Older API (< 1.7.0)
                results = self.qdrant.search(
                    collection_name=QDRANT_COLLECTION,
                    query_vector=qvec,
                    limit=top_k,
                    with_payload=True,
                    query_filter=Filter(must=must),
                )

            print(f"✓ Found {len(results)} results")

            items: List[RetrievedItem] = []
            for hit in results:
                p = hit.payload or {}
                items.append(
                    RetrievedItem(
                        minute=int(p.get("minute", 0) or 0),
                        tsec=int(p.get("tsec", 0) or 0),
                        source=str(p.get("source", "comment") or "comment"),
                        text=str(p.get("text", "") or ""),
                        event_type=(p.get("event_type") if isinstance(p.get("event_type"), str) else None),
                        team=(p.get("team") if isinstance(p.get("team"), str) else None),
                    )
                )

            # IMPORTANT: chronological order helps the LLM
            items.sort(key=lambda x: (x.minute, x.tsec))
            return items
        except Exception as e:
            print(f"❌ Error in retrieve: {e}")
            import traceback
            traceback.print_exc()
            return []

    def build_prompt(self, user_query: str, contexts: List[RetrievedItem], deep_think: bool = False) -> str:
        ctx_lines: List[str] = []
        for c in contexts:
            meta = []
            if c.team:
                meta.append(f"team={c.team}")
            if c.event_type:
                meta.append(f"type={c.event_type}")
            meta_s = (" " + " ".join(meta)) if meta else ""
            ctx_lines.append(f"[{c.source.upper()} {c.minute:02d}m]{meta_s} {c.text}")

        if deep_think:
            system_msg = (
                "SYSTEM:\n"
                "You are a football match analyst with access to ALL match data.\n"
                "The context below contains the MOST RELEVANT information from the entire match history.\n"
                "These moments were selected by semantic similarity to the question.\n"
                "Analyze the context deeply and provide a comprehensive answer.\n"
                "If you need to reference specific moments, mention the minute.\n\n"
            )
        else:
            system_msg = (
                "SYSTEM:\n"
                "You are a live football match assistant.\n"
                "Use ONLY the context to answer.\n"
                "If the answer is not in context, say you don't know.\n\n"
            )

        return (
            system_msg
            + "CONTEXT:\n"
            + "\n".join(ctx_lines)
            + "\n\nQUESTION:\n"
            + user_query
            + "\n\nANSWER:\n"
        )

    def generate_answer(self, prompt: str) -> str:
        resp = self.mistral.chat.complete(
            model=MISTRAL_MODEL,
            messages=[
                {"role": "system", "content": "You are a live football match assistant."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=250,
            temperature=0.2,
        )
        return resp.choices[0].message.content

    def answer(
        self,
        question: str,
        match_id: str,
        top_k: int = 6,
        minute_window: Optional[int] = None,
        current_minute: Optional[int] = None,
        run_id: Optional[str] = None,
        deep_think: bool = False,
    ) -> Dict[str, Any]:
        """
        Answer a question using RAG.
        
        Args:
            question: User's question
            match_id: Game ID
            top_k: Number of results (ignored in deep_think mode, uses 20)
            minute_window: Window size for time filtering (ignored in deep_think)
            current_minute: Current minute (ignored in deep_think)
            run_id: Optional run ID
            deep_think: If True, uses deep thinking mode with full vector search
        """
        try:
            # In deep_think mode, ignore minute window and get more results
            if deep_think:
                minute_gte = None
                minute_lte = None
                top_k = 20  # Get more context for deep thinking
                print("🧠 DEEP THINK MODE: Searching ALL historical data with semantic similarity")
            else:
                minute_gte = None
                minute_lte = None
                if minute_window is not None and current_minute is not None:
                    minute_gte = max(0, int(current_minute) - int(minute_window))
                    minute_lte = int(current_minute)

            ctx = self.retrieve(
                user_query=question,
                match_id=match_id,
                top_k=top_k,
                minute_gte=minute_gte,
                minute_lte=minute_lte,
                run_id=run_id,
                deep_think=deep_think,
            )
            
            if not ctx:
                return {
                    "answer": "I don't have any context to answer this question yet. Please make sure the game is streaming.",
                    "contexts": [],
                }
            
            prompt = self.build_prompt(question, ctx, deep_think=deep_think)
            answer = self.generate_answer(prompt)

            return {
                "answer": answer,
                "contexts": [c.__dict__ for c in ctx],
                "mode": "deep_think" if deep_think else "fast",
            }
        except Exception as e:
            print(f"Error in answer: {e}")
            import traceback
            traceback.print_exc()
            return {
                "answer": f"Sorry, I encountered an error: {str(e)}",
                "contexts": [],
            }