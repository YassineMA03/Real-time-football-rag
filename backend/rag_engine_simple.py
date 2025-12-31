# backend/rag_engine_simple.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any

from mistralai import Mistral


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")


class SimpleRagEngine:
    """
    Simple RAG Engine:
    1. Reads rag_top10.txt (combines ALL available context)
       - Last 10 scores (goals)
       - Last 10 events
       - Last 10 comments
       = Up to 30 items total for the LLM
    2. Builds prompt with full context
    3. Calls Mistral LLM for answer
    """

    def __init__(self, project_root: Path):
        if not MISTRAL_API_KEY:
            raise ValueError("Missing env var MISTRAL_API_KEY")
        
        self.root = project_root
        self.runtime = self.root / "backend" / "runtime"
        self.mistral = Mistral(api_key=MISTRAL_API_KEY)

    def _read_file(self, game_id: str, name: str) -> str:
        """Read a context file from runtime directory (game_id only, no run_id)."""
        p = self.runtime / game_id / name
        if p.exists():
            return p.read_text(encoding="utf-8")
        return ""

    def build_prompt(self, question: str, context: str) -> str:
        """
        Build prompt with context from rag_recap.txt
        
        The context contains ALL available items (up to 30 total):
        - Last 10 scores (goals)
        - Last 10 events
        - Last 10 comments
        """
        if not context.strip():
            return f"""You are a football match assistant.

The user asked: {question}

However, there is no match context available yet. The stream may not have started, or no data has been received yet.

Respond politely that you don't have match information yet."""

        return f"""You are a live football match assistant.

You have access to comprehensive match context including:
- Last 10 score changes (goals)
- Last 10 match events
- Last 10 commentary items
(Up to 30 items total)

Answer the user's question based ONLY on the provided context.
If the answer is not in the context, say you don't have that information.
Be concise and accurate.

=== MATCH CONTEXT ===
{context}

=== USER QUESTION ===
{question}

=== YOUR ANSWER ==="""

    def generate_answer(self, prompt: str) -> str:
        """Call Mistral LLM to generate answer."""
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
            return f"Sorry, I encountered an error: {str(e)}"

    def answer(self, question: str, match_id: str, run_id: str) -> Dict[str, Any]:
        """
        Main entry point for RAG queries.
        
        Args:
            question: User's question
            match_id: Game identifier (used for file path)
            run_id: Stream run identifier (kept for compatibility but not used in path)
            
        Returns:
            {
                'answer': str,
                'contexts': {
                    'rag_recap': str
                }
            }
        """
        # Read combined context file (only uses game_id in path)
        print(f"\n📖 Reading context: match_id={match_id}")
        context = self._read_file(match_id, "rag_recap.txt")
        
        # Debug: Show what RAG sees
        print("\n" + "=" * 70)
        print("🧠 RAG CONTEXT (rag_recap.txt)")
        print("=" * 70)
        if context.strip():
            # Count items in context
            lines = [l for l in context.split('\n') if l.strip() and l.startswith('[GAME')]
            print(f"Context items: {len(lines)}")
            print("-" * 70)
            print(context[:500] + "..." if len(context) > 500 else context)
        else:
            print("(No context available yet)")
        print("=" * 70 + "\n")
        
        # Build prompt
        prompt = self.build_prompt(question, context)
        
        # Generate answer with LLM
        print("🤖 Generating answer with Mistral...")
        answer = self.generate_answer(prompt)
        
        print(f"✅ Answer: {answer[:100]}...")
        
        return {
            "answer": answer,
            "contexts": {
                "rag_recap": context,
            }
        }