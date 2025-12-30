# backend/kafka_context_store.py
from __future__ import annotations

import json
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

from kafka import KafkaConsumer


# -------------------------
# Small helpers
# -------------------------

def now_stream_time_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def fmt_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}:{s:02d}"


def load_json_or_jsonl(game_folder: Path, base: str) -> List[dict]:
    """
    Supports:
      - <base>.json as a JSON list
      - <base>.json as API-style wrapper dict containing "response": [...]
      - <base>.jsonl as JSON Lines (one object per line)
    """
    p_json = game_folder / f"{base}.json"
    p_jsonl = game_folder / f"{base}.jsonl"

    if p_json.exists():
        obj = json.loads(p_json.read_text(encoding="utf-8"))

        # Case 1: already a list
        if isinstance(obj, list):
            return obj

        # Case 2: wrapper dict with "response" list (API-Football, etc.)
        if isinstance(obj, dict):
            resp = obj.get("response")
            if isinstance(resp, list):
                return resp

        raise ValueError(f"{p_json.name} must be a list OR a dict with a 'response' list")

    if p_jsonl.exists():
        out = []
        for line in p_jsonl.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
        return out

    return []



def comment_time_sec(c: dict) -> int:
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    second = int(c.get("second", 0) or 0)
    return (minute + extra) * 60 + second


def event_time_sec(e: dict) -> Optional[int]:
    # API-Football fixtures/events schema
    t = e.get("time")
    if isinstance(t, dict) and "elapsed" in t:
        minute = int(t.get("elapsed", 0) or 0)
        extra = int(t.get("extra", 0) or 0)
        second = int(t.get("second", 0) or 0)
        return (minute + extra) * 60 + second

    # fallback
    if "event_time_sec" in e and isinstance(e["event_time_sec"], (int, float)):
        return int(e["event_time_sec"])

    if "minute" in e:
        minute = int(e.get("minute", 0) or 0)
        extra = int(e.get("extra", 0) or 0)
        second = int(e.get("second", 0) or 0)
        return (minute + extra) * 60 + second

    return None


# -------------------------
# NEW: Tuple-based time extractors (for accurate comparison)
# -------------------------

def comment_time_tuple(c: dict) -> Tuple[int, int]:
    """Extract (minute, extra) tuple from comment."""
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    return (minute, extra)


def event_time_tuple(e: dict) -> Tuple[int, int]:
    """Extract (minute, extra) tuple from event."""
    t = e.get("time")
    if isinstance(t, dict) and "elapsed" in t:
        minute = int(t.get("elapsed", 0) or 0)
        extra = int(t.get("extra", 0) or 0)
        return (minute, extra)
    
    if "minute" in e:
        minute = int(e.get("minute", 0) or 0)
        extra = int(e.get("extra", 0) or 0)
        return (minute, extra)
    
    if "event_time_sec" in e:
        tsec = int(e["event_time_sec"])
        minute = tsec // 60
        return (minute, 0)
    
    return (0, 0)


GOAL_DETAILS_OK = {"Normal Goal", "Penalty", "Own Goal"}


def is_goal_event(e: dict) -> bool:
    etype = e.get("type")
    if isinstance(etype, str) and etype.lower() == "goal":
        detail = e.get("detail")
        if detail is None:
            return True
        if isinstance(detail, str):
            return detail.strip() in GOAL_DETAILS_OK
        return True
    return False


def extract_team_name(e: dict) -> Optional[str]:
    team = e.get("team")
    if isinstance(team, dict):
        name = team.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return None


def apply_goal(score_home: int, score_away: int, scoring_team: str, home: str, away: str, detail: Optional[str]) -> Tuple[int, int, str]:
    d = (detail or "").strip()
    if d == "Own Goal":
        if scoring_team == home:
            return score_home, score_away + 1, f"Own Goal by {home}"
        if scoring_team == away:
            return score_home + 1, score_away, f"Own Goal by {away}"
        return score_home, score_away, "Own Goal (unknown team)"

    if scoring_team == home:
        return score_home + 1, score_away, f"Goal for {home}"
    if scoring_team == away:
        return score_home, score_away + 1, f"Goal for {away}"
    return score_home, score_away, "Goal (unknown team)"


def event_summary(e: dict) -> str:
    etype = e.get("type") or e.get("event") or e.get("name") or "Event"
    detail = e.get("detail")
    team = extract_team_name(e)
    player = None
    if isinstance(e.get("player"), dict):
        player = e["player"].get("name")

    parts = [str(etype)]
    if isinstance(detail, str) and detail.strip():
        parts.append(detail.strip())
    if isinstance(team, str):
        parts.append(f"team={team}")
    if isinstance(player, str) and player.strip():
        parts.append(f"player={player.strip()}")
    return " | ".join(parts)


def comment_line(tsec: int, text: str, etype: str = "COMMENT", minute: Optional[int] = None, extra: Optional[int] = None) -> str:
    """
    Format a comment line with time, half, minute, and extra time.
    
    Example:
        [GAME 45' +2' | 1st Half] COMMENT: Ball possession
    """
    if minute is not None and extra is not None:
        half = "1st Half" if minute <= 45 else "2nd Half"
        if extra > 0:
            time_str = f"{minute}' +{extra}' | {half}"
        else:
            time_str = f"{minute}' | {half}"
    else:
        # Fallback to seconds
        time_str = fmt_mmss(tsec)
    
    return f"[GAME {time_str}] {etype}: {text}\n"


def event_line(tsec: int, summary: str, minute: Optional[int] = None, extra: Optional[int] = None) -> str:
    """
    Format an event line with time, half, minute, and extra time.
    
    Example:
        [GAME 45' +2' | 1st Half] EVENT: Yellow Card | team=Nigeria
    """
    if minute is not None and extra is not None:
        half = "1st Half" if minute <= 45 else "2nd Half"
        if extra > 0:
            time_str = f"{minute}' +{extra}' | {half}"
        else:
            time_str = f"{minute}' | {half}"
    else:
        # Fallback to seconds
        time_str = fmt_mmss(tsec)
    
    return f"[GAME {time_str}] EVENT: {summary}\n"


def score_line(tsec: int, score_str: str, why: str, scorer: Optional[str] = None, 
               assist: Optional[str] = None, goal_type: Optional[str] = None,
               minute: Optional[int] = None, extra: Optional[int] = None) -> str:
    """
    Format a score line with detailed goal information including half.
    
    Examples:
        [GAME 12' | 1st Half] SCORE: 1-0 | ⚽ Moses Simon | Type: Normal Goal
        [GAME 45' +2' | 1st Half] SCORE: 2-0 | ⚽ Victor Osimhen | 🅰️ Moses Simon
        [GAME 68' | 2nd Half] SCORE: 2-1 | ⚽ Youssef Msakni | Type: Penalty
    """
    if minute is not None and extra is not None:
        half = "1st Half" if minute <= 45 else "2nd Half"
        if extra > 0:
            time_str = f"{minute}' +{extra}' | {half}"
        else:
            time_str = f"{minute}' | {half}"
    else:
        # Fallback to seconds
        time_str = fmt_mmss(tsec)
    
    parts = [f"[GAME {time_str}] SCORE: {score_str}"]
    
    # Add scorer
    if scorer:
        parts.append(f"⚽ {scorer}")
    
    # Add assist
    if assist:
        parts.append(f"🅰️ {assist}")
    
    # Add goal type
    if goal_type:
        parts.append(f"Type: {goal_type}")
    
    # Add reason (fallback if no detailed info)
    if not scorer and why:
        parts.append(f"({why})")
    
    return " | ".join(parts) + "\n"


def header_text(run_id: str, game_id: str, known_tsec: int, score_str: str) -> str:
    return (
        f"RUN: {run_id}\n"
        f"GAME: {game_id}\n"
        f"Current game time: {fmt_mmss(known_tsec)}\n"
        f"SCORE: {score_str}\n"
        "----------------------------------------\n"
    )


# -------------------------
# Store per active game with separate buffers
# -------------------------

@dataclass
class GameBuffers:
    """Separate buffers for each type of information."""
    comments: deque  # deque[str] - last 10 comments
    events: deque    # deque[str] - last 10 events
    scores: deque    # deque[str] - last 10 score changes (goals)
    score_str: str   # current score
    known_tsec: int  # current game time


class KafkaContextStore:
    """
    Kafka-first context builder with SEPARATE buffers:
      - Last 10 comments  → rag_comments.txt
      - Last 10 events    → rag_events.txt
      - Last 10 scores    → rag_scores.txt
    
    This provides richer, more structured context for the RAG system.
    """

    def __init__(self, project_root: Path, kafka_bootstrap: str = "localhost:9092", top_k: int = 10):
        self.root = project_root
        self.kafka_bootstrap = kafka_bootstrap
        self.top_k = top_k

        self.data_dir = self.root / "data" / "games"
        self.runtime_dir = self.root / "backend" / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._run_id: Optional[str] = None
        self._active_game_ids: set[str] = set()
        self._buffers: Dict[str, GameBuffers] = {}

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

    # ---------- public ----------

    def reset(self):
        self.stop()
        with self._lock:
            self._run_id = None
            self._active_game_ids.clear()
            self._buffers.clear()

    def stop(self):
        self._stop.set()
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=1.0)
        self._threads.clear()
        self._stop.clear()

    def start(self, run_id: str, active_game_ids: List[str], game_start_offsets: Dict[str, Tuple[int, int]]):
        """
        Start consuming from Kafka for specified games.
        Bootstrap each game's context from disk up to its offset.
        
        Args:
            run_id: Unique run identifier
            active_game_ids: List of game IDs to consume
            game_start_offsets: Dict mapping game_id to (minute, extra) tuple
        """
        with self._lock:
            if self._run_id is not None:
                raise RuntimeError("Already started. Call reset() first.")
            self._run_id = run_id
            self._active_game_ids = set(active_game_ids)

        # Bootstrap each game from disk
        for gid in active_game_ids:
            offset_tuple = game_start_offsets.get(gid, (0, 0))
            try:
                self._bootstrap_game_from_disk(run_id, gid, offset_tuple)
            except Exception as e:
                print(f"Error bootstrapping {gid}: {e}")

        # Start Kafka consumers
        self._threads = [
            threading.Thread(target=self._consume_comments, daemon=True),
            threading.Thread(target=self._consume_events, daemon=True),
            threading.Thread(target=self._consume_scores, daemon=True),
        ]
        for t in self._threads:
            t.start()

    def get_comments_text(self, game_id: str) -> str:
        """Get last 10 comments as text."""
        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return ""
            return "".join(list(buf.comments))

    def get_events_text(self, game_id: str) -> str:
        """Get last 10 events as text."""
        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return ""
            return "".join(list(buf.events))

    def get_scores_text(self, game_id: str) -> str:
        """Get last 10 score changes as text."""
        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return ""
            return "".join(list(buf.scores))

    def get_run_id(self) -> Optional[str]:
        with self._lock:
            return self._run_id

    # ---------- disk bootstrap ----------

    def _game_runtime_dir(self, run_id: str, game_id: str) -> Path:
        p = self.runtime_dir / run_id / game_id
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _bootstrap_game_from_disk(self, run_id: str, game_id: str, start_at_tuple: Tuple[int, int]):
        """
        Bootstrap game buffers from disk files.
        Pre-fills buffers with last 10 items BEFORE start_at_tuple.
        
        Args:
            run_id: Unique run identifier
            game_id: Game identifier
            start_at_tuple: (minute, extra) tuple to start from
        """
        gfolder = self.data_dir / game_id
        meta_path = gfolder / "meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"Missing meta.json for {game_id}")

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        home = (meta.get("team_home") or "").strip()
        away = (meta.get("team_away") or "").strip()

        comments = load_json_or_jsonl(gfolder, "comments")
        events = load_json_or_jsonl(gfolder, "events")

        comments.sort(key=comment_time_tuple)
        events = [e for e in events if event_time_tuple(e) is not None]
        events.sort(key=event_time_tuple)

        # Find indices before offset using TUPLE comparison
        c_idx = 0
        while c_idx < len(comments) and comment_time_tuple(comments[c_idx]) < start_at_tuple:
            c_idx += 1
        e_idx = 0
        while e_idx < len(events) and event_time_tuple(events[e_idx]) < start_at_tuple:
            e_idx += 1

        # ========================================
        # PRE-FILL BUFFERS WITH HISTORICAL DATA
        # ========================================
        
        # Collect last 10 comments BEFORE offset
        recent_comments = []
        start_c = max(0, c_idx - self.top_k)
        for i in range(start_c, c_idx):
            tsec = comment_time_sec(comments[i])
            txt = str(comments[i].get("text") or "")
            etype = str(comments[i].get("type") or "Commentary")
            minute = comments[i].get("minute")
            extra = comments[i].get("extra")
            recent_comments.append(comment_line(tsec, txt, etype=etype, minute=minute, extra=extra))
        
        # Collect last 10 events BEFORE offset
        recent_events = []
        start_e = max(0, e_idx - self.top_k)
        for i in range(start_e, e_idx):
            tsec = event_time_sec(events[i]) or 0
            e = events[i]
            # Extract minute/extra from event
            minute = None
            extra = None
            if "time" in e and isinstance(e["time"], dict):
                minute = e["time"].get("elapsed")
                extra = e["time"].get("extra")
            elif "minute" in e:
                minute = e.get("minute")
                extra = e.get("extra", 0)
            recent_events.append(event_line(tsec, event_summary(e), minute=minute, extra=extra))
        
        # Compute score changes and collect last 10 goals BEFORE offset
        recent_goals = []
        score_home = 0
        score_away = 0
        
        for i in range(0, e_idx):
            e = events[i]
            if not is_goal_event(e):
                continue
            team = extract_team_name(e)
            if not team:
                continue
            
            # Compute new score
            detail = e.get("detail") if isinstance(e.get("detail"), str) else None
            new_home, new_away, why = apply_goal(score_home, score_away, team, home, away, detail)
            score_home, score_away = new_home, new_away
            
            # Extract goal details
            scorer = None
            assist = None
            goal_type = detail or "Normal Goal"
            
            if isinstance(e.get("player"), dict):
                scorer = e["player"].get("name")
            
            if isinstance(e.get("assist"), dict):
                assist = e["assist"].get("name")
            
            # Extract minute/extra
            minute = None
            extra = None
            if "time" in e and isinstance(e["time"], dict):
                minute = e["time"].get("elapsed")
                extra = e["time"].get("extra")
            elif "minute" in e:
                minute = e.get("minute")
                extra = e.get("extra", 0)
            
            # Record this goal with details
            tsec = event_time_sec(e) or 0
            goal_str = f"{score_home}-{score_away}"
            recent_goals.append(score_line(tsec, goal_str, why, scorer=scorer, assist=assist, goal_type=goal_type, minute=minute, extra=extra))
        
        # Keep only last 10 goals
        if len(recent_goals) > self.top_k:
            recent_goals = recent_goals[-self.top_k:]
        
        score_str = f"{score_home}-{score_away}"
        
        # Initialize buffers WITH historical data
        comments_deque = deque(recent_comments, maxlen=self.top_k)
        events_deque = deque(recent_events, maxlen=self.top_k)
        scores_deque = deque(recent_goals, maxlen=self.top_k)
        
        with self._lock:
            self._buffers[game_id] = GameBuffers(
                comments=comments_deque,
                events=events_deque,
                scores=scores_deque,
                score_str=score_str,
                known_tsec=(start_at_tuple[0] + start_at_tuple[1]) * 60,
            )
        
        # Write initial files with historical data
        rdir = self._game_runtime_dir(run_id, game_id)
        
        minute, extra = start_at_tuple
        print(f"📦 Pre-filled buffers for {game_id}:")
        print(f"   Comments: {len(recent_comments)} items")
        print(f"   Events: {len(recent_events)} items")
        print(f"   Goals: {len(recent_goals)} items")
        print(f"   Starting at: {minute}' +{extra}' (Score: {score_str})")
        
        # Write all three files with pre-filled data
        known_tsec = (minute + extra) * 60
        self._write_comments_file(run_id, game_id, known_tsec, score_str, comments_deque)
        self._write_events_file(run_id, game_id, known_tsec, score_str, events_deque)
        self._write_scores_file(run_id, game_id, known_tsec, score_str, scores_deque)

    # ---------- consumers ----------

    def _consume_comments(self):
        consumer = KafkaConsumer(
            "games.comments",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-comments-{int(time.time())}",
        )
        while not self._stop.is_set():
            for msg in consumer:
                item = msg.value
                self._handle_comment(item)
                if self._stop.is_set():
                    break

    def _consume_events(self):
        consumer = KafkaConsumer(
            "games.events",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-events-{int(time.time())}",
        )
        while not self._stop.is_set():
            for msg in consumer:
                item = msg.value
                self._handle_event(item)
                if self._stop.is_set():
                    break

    def _consume_scores(self):
        consumer = KafkaConsumer(
            "games.scores",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-scores-{int(time.time())}",
        )
        while not self._stop.is_set():
            for msg in consumer:
                item = msg.value
                self._handle_score(item)
                if self._stop.is_set():
                    break

    # ---------- handlers ----------

    def _handle_comment(self, payload: dict):
        run_id = str(payload.get("run_id") or "")
        game_id = str(payload.get("game_id") or "")
        tsec = int(payload.get("event_time_sec") or 0)
        text = str(payload.get("text") or "")
        etype = str(payload.get("type") or "Commentary")
        
        # Extract minute and extra from payload
        minute = payload.get("minute")
        extra = payload.get("extra")

        with self._lock:
            if run_id != self._run_id or game_id not in self._active_game_ids:
                return
            buf = self._buffers.get(game_id)
            if not buf:
                return
            buf.known_tsec = max(buf.known_tsec, tsec)
            line = comment_line(tsec, text, etype=etype, minute=minute, extra=extra)
            buf.comments.append(line)
            score_str = buf.score_str

        self._write_comments_file(run_id, game_id, buf.known_tsec, score_str, buf.comments)

    def _handle_event(self, payload: dict):
        run_id = str(payload.get("run_id") or "")
        game_id = str(payload.get("game_id") or "")
        tsec = int(payload.get("event_time_sec") or 0)

        raw = payload.get("data") or {}
        if not isinstance(raw, dict):
            raw = {}

        # Extract minute and extra from event data
        minute = None
        extra = None
        if "time" in raw and isinstance(raw["time"], dict):
            minute = raw["time"].get("elapsed")
            extra = raw["time"].get("extra")
        elif "minute" in raw:
            minute = raw.get("minute")
            extra = raw.get("extra", 0)

        summary = event_summary(raw)
        line = event_line(tsec, summary, minute=minute, extra=extra)

        with self._lock:
            if run_id != self._run_id or game_id not in self._active_game_ids:
                return
            buf = self._buffers.get(game_id)
            if not buf:
                return
            buf.known_tsec = max(buf.known_tsec, tsec)
            buf.events.append(line)
            score_str = buf.score_str

        self._write_events_file(run_id, game_id, buf.known_tsec, score_str, buf.events)

    def _handle_score(self, payload: dict):
        run_id = str(payload.get("run_id") or "")
        game_id = str(payload.get("game_id") or "")
        tsec = int(payload.get("event_time_sec") or 0)
        score_str = str(payload.get("score_str") or "0-0")
        why = str(payload.get("why") or "score update")
        
        # Extract detailed goal information
        scorer = payload.get("scorer")
        assist = payload.get("assist")
        goal_type = payload.get("goal_type")
        minute = payload.get("minute")
        extra = payload.get("extra")
        
        line = score_line(
            tsec, 
            score_str, 
            why, 
            scorer=scorer if isinstance(scorer, str) else None,
            assist=assist if isinstance(assist, str) else None,
            goal_type=goal_type if isinstance(goal_type, str) else None,
            minute=minute,
            extra=extra
        )

        with self._lock:
            if run_id != self._run_id or game_id not in self._active_game_ids:
                return
            buf = self._buffers.get(game_id)
            if not buf:
                return
            buf.score_str = score_str
            buf.known_tsec = max(buf.known_tsec, tsec)
            buf.scores.append(line)

        self._write_scores_file(run_id, game_id, buf.known_tsec, score_str, buf.scores)

    # ---------- file writers ----------

    def _write_comments_file(self, run_id: str, game_id: str, known_tsec: int, score_str: str, comments_deque: deque):
        rdir = self._game_runtime_dir(run_id, game_id)
        path = rdir / "rag_comments.txt"
        
        hdr = header_text(run_id, game_id, known_tsec, score_str)
        body = "".join(list(comments_deque))
        
        content = hdr + f"LAST {self.top_k} COMMENTS:\n" + "-"*40 + "\n" + body
        path.write_text(content, encoding="utf-8")

    def _write_events_file(self, run_id: str, game_id: str, known_tsec: int, score_str: str, events_deque: deque):
        rdir = self._game_runtime_dir(run_id, game_id)
        path = rdir / "rag_events.txt"
        
        hdr = header_text(run_id, game_id, known_tsec, score_str)
        body = "".join(list(events_deque))
        
        content = hdr + f"LAST {self.top_k} EVENTS:\n" + "-"*40 + "\n" + body
        path.write_text(content, encoding="utf-8")

    def _write_scores_file(self, run_id: str, game_id: str, known_tsec: int, score_str: str, scores_deque: deque):
        rdir = self._game_runtime_dir(run_id, game_id)
        path = rdir / "rag_scores.txt"
        
        hdr = header_text(run_id, game_id, known_tsec, score_str)
        body = "".join(list(scores_deque))
        
        if not body:
            body = "(No goals yet)\n"
        
        content = hdr + f"LAST {self.top_k} GOALS/SCORE CHANGES:\n" + "-"*40 + "\n" + body
        path.write_text(content, encoding="utf-8")