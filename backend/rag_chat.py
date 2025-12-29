# backend/rag_chat.py
from __future__ import annotations
from pathlib import Path

def read_rag_last10(runtime_dir: Path, game_id: str) -> str:
    p = runtime_dir / game_id / "rag_last10.txt"
    if not p.exists():
        return (
            "NO CONTEXT FILE YET.\n"
            f"Expected: {p}\n"
            "Start streaming this game first.\n"
        )
    return p.read_text(encoding="utf-8", errors="replace")


def build_prompt(context: str, user_question: str) -> str:
    return (
        "You are a football match assistant.\n"
        "Answer ONLY using the provided context.\n"
        "If the answer is not in context, say you don't know.\n\n"
        "=== CONTEXT (last 10 items: comments/events/scores) ===\n"
        f"{context}\n"
        "=== USER QUESTION ===\n"
        f"{user_question}\n"
        "=== ANSWER ===\n"
    )


def rag_stub_answer(context: str, user_question: str) -> str:
    # Replace this later with your real RAG/LLM call.
    # For now we prove we are using context.
    top = "\n".join(context.splitlines()[:25])
    return (
        "RAG STUB (replace with your LLM later)\n\n"
        f"Question: {user_question}\n\n"
        "Context preview:\n"
        f"{top}"
    )
def answer_last_event_from_context(context: str) -> str:
    lines = [ln.strip() for ln in context.splitlines() if ln.strip()]
    # keep only event-like lines (ignore header)
    event_lines = [ln for ln in lines if ln.startswith("[STREAM")]
    if not event_lines:
        return "I don't see any streamed events yet for this game."
    return f"Last item in the stream:\n{event_lines[-1]}"
