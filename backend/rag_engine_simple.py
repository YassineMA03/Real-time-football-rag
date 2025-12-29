# backend/rag_engine_simple.py
"""
Simple RAG Engine - No Vector Search Required

This RAG implementation uses time-based context retrieval instead of 
semantic vector search. It reads three structured context files:
  1. Last 10 comments
  2. Last 10 events
  3. Last 10 score changes (goals)

This approach is simpler, faster, and perfectly suited for live match 
streaming where users care about recent context.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any, Optional

from mistralai import Mistral


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")


class SimpleRagEngine:
    """
    Simple RAG engine that uses file-based context retrieval.
    No embeddings, no vector database, just straightforward context assembly.
    """

    def __init__(self, runtime_dir: Optional[Path] = None):
        if not MISTRAL_API_KEY:
            raise ValueError("Missing env var MISTRAL_API_KEY")
        
        self.mistral = Mistral(api_key=MISTRAL_API_KEY)
        
        # Default runtime directory
        if runtime_dir is None:
            runtime_dir = Path(__file__).resolve().parents[1] / "backend" / "runtime"
        self.runtime_dir = runtime_dir

    def _read_context_file(self, filepath: Path) -> str:
        """Read a context file, return empty string if not found."""
        if not filepath.exists():
            return ""
        try:
            return filepath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"⚠️  Error reading {filepath}: {e}")
            return ""

    def retrieve_context(
        self, 
        run_id: str, 
        match_id: str
    ) -> Dict[str, str]:
        """
        Retrieve the three context files for a match.
        
        Returns:
            {
                'comments': str,  # Last 10 comments
                'events': str,    # Last 10 events
                'scores': str     # Last 10 score changes
            }
        """
        game_dir = self.runtime_dir / run_id / match_id
        
        contexts = {
            'comments': self._read_context_file(game_dir / "rag_comments.txt"),
            'events': self._read_context_file(game_dir / "rag_events.txt"),
            'scores': self._read_context_file(game_dir / "rag_scores.txt"),
        }
        
        return contexts

    def build_prompt(
        self, 
        user_question: str, 
        contexts: Dict[str, str]
    ) -> str:
        """
        Build a prompt for the LLM with structured context.
        
        The context is organized into three sections for clarity:
          1. Recent commentary
          2. Recent events
          3. Recent score changes
        """
        prompt_parts = [
            "You are a live football match assistant.",
            "Answer the user's question based ONLY on the provided context from the match.",
            "If the answer is not in the context, say you don't have that information.",
            "",
            "=== MATCH CONTEXT ===",
            ""
        ]
        
        # Add comments section
        if contexts['comments']:
            prompt_parts.append("📝 RECENT COMMENTARY:")
            prompt_parts.append(contexts['comments'])
            prompt_parts.append("")
        
        # Add events section
        if contexts['events']:
            prompt_parts.append("⚽ RECENT EVENTS:")
            prompt_parts.append(contexts['events'])
            prompt_parts.append("")
        
        # Add scores section
        if contexts['scores']:
            prompt_parts.append("🎯 RECENT GOALS/SCORE CHANGES:")
            prompt_parts.append(contexts['scores'])
            prompt_parts.append("")
        
        # Check if we have any context at all
        if not any(contexts.values()):
            prompt_parts.append("(No match context available yet - streaming may not have started)")
            prompt_parts.append("")
        
        # Add user question
        prompt_parts.append("=== USER QUESTION ===")
        prompt_parts.append(user_question)
        prompt_parts.append("")
        prompt_parts.append("=== YOUR ANSWER ===")
        
        return "\n".join(prompt_parts)

    def generate_answer(self, prompt: str) -> str:
        """Call Mistral LLM to generate an answer."""
        try:
            resp = self.mistral.chat.complete(
                model=MISTRAL_MODEL,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a helpful football match assistant. Be concise and accurate."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    },
                ],
                max_tokens=300,
                temperature=0.2,
            )
            return resp.choices[0].message.content
        except Exception as e:
            print(f"❌ Error calling Mistral: {e}")
            return f"Sorry, I encountered an error generating a response: {str(e)}"

    def answer(
        self,
        question: str,
        match_id: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point for RAG queries.
        
        Args:
            question: User's question
            match_id: Game identifier
            run_id: Stream run identifier (required)
            
        Returns:
            {
                'answer': str,
                'contexts': {
                    'comments': str,
                    'events': str, 
                    'scores': str
                }
            }
        """
        # Validate inputs
        if not run_id:
            return {
                "answer": "No active stream found. Please start a replay first.",
                "contexts": {}
            }
        
        # Retrieve context from files
        print(f"\n🔍 Retrieving context for: run_id={run_id}, match_id={match_id}")
        contexts = self.retrieve_context(run_id, match_id)
        
        # Check if we have any context
        has_context = any(contexts.values())
        if not has_context:
            return {
                "answer": "No match context available yet. The stream may not have started, or no data has been received yet.",
                "contexts": contexts
            }
        
        # Count available context
        comments_count = len([l for l in contexts['comments'].split('\n') if l.strip() and l.startswith('[GAME')])
        events_count = len([l for l in contexts['events'].split('\n') if l.strip() and l.startswith('[GAME')])
        scores_count = len([l for l in contexts['scores'].split('\n') if l.strip() and l.startswith('[GAME')])
        
        print(f"📊 Context retrieved:")
        print(f"   Comments: {comments_count} items")
        print(f"   Events: {events_count} items")
        print(f"   Scores: {scores_count} items")
        
        # Build prompt
        prompt = self.build_prompt(question, contexts)
        
        # Generate answer
        print(f"🤖 Generating answer with Mistral...")
        answer = self.generate_answer(prompt)
        
        print(f"✅ Answer generated: {answer[:100]}...")
        
        return {
            "answer": answer,
            "contexts": contexts
        }


# Backwards compatibility: create an alias
RagEngine = SimpleRagEngine