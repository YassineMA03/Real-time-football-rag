# backend/kafka_context_store.py
from __future__ import annotations

import json
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import deque
from typing import Any, Dict, Optional
from kafka import KafkaConsumer
import os

# -------------------------
# Formatting helpers
# -------------------------

def fmt_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}:{s:02d}"


def half_from_minute(minute: Optional[int]) -> str:
    if minute is None:
        return "—"
    return "1st Half" if minute <= 45 else "2nd Half"


def time_str(minute: Optional[int], extra: Optional[int], tsec: Optional[int]) -> str:
    if minute is None:
        return fmt_mmss(int(tsec or 0))
    extra = int(extra or 0)
    h = half_from_minute(minute)
    if extra > 0:
        return f"{minute}' +{extra}' | {h}"
    return f"{minute}' | {h}"


def comment_line(item: dict) -> str:
    tsec = int(item.get("event_time_sec", 0) or 0)
    minute = item.get("minute")
    extra = item.get("extra")
    etype = item.get("type") or "Comment"
    text = (item.get("text") or "").strip()
    ts = time_str(minute, extra, tsec)
    return f"[GAME {ts}] COMMENT {etype}: {text}\n"


def event_line(item: dict) -> str:
    tsec = int(item.get("event_time_sec", 0) or 0)
    minute = item.get("minute")
    extra = item.get("extra")
    etype = item.get("type") or "Event"
    detail = item.get("detail")
    ts = time_str(minute, extra, tsec)

    # Try to extract a readable description from data
    data = item.get("data")
    desc = None
    if isinstance(data, dict):
        for k in ["comments", "comment", "description", "text", "detail"]:
            if isinstance(data.get(k), str) and data.get(k).strip():
                desc = data[k].strip()
                break

        # add player/team if exists
        player = data.get("player")
        team = data.get("team")
        player_name = player.get("name") if isinstance(player, dict) else None
        team_name = team.get("name") if isinstance(team, dict) else None
    else:
        player_name = None
        team_name = None

    parts = [f"[GAME {ts}] EVENT {etype}"]
    if detail:
        parts.append(f"({detail})")
    if team_name:
        parts.append(f"- {team_name}")
    if player_name:
        parts.append(f"- {player_name}")
    if desc:
        parts.append(f": {desc}")

    return " ".join(parts).strip() + "\n"


def score_line(item: dict) -> str:
    # Try event_time_sec first (used by Kafka messages), fall back to tsec
    tsec = int(item.get("event_time_sec", 0) or item.get("tsec", 0) or 0)
    minute = item.get("minute")
    extra = item.get("extra")
    score_str = item.get("score_str") or f"{item.get('score_home', 0)}-{item.get('score_away', 0)}"
    ts = time_str(minute, extra, tsec)

    # Extract goal data - could be in different formats
    goal_data = item.get("goal_data") or {}
    
    team = goal_data.get("team") or item.get("team")
    scorer = goal_data.get("scorer") or item.get("scorer")
    assist = goal_data.get("assist") or item.get("assist")
    goal_type = goal_data.get("type") or item.get("goal_type") or item.get("why")

    parts = [f"[GAME {ts}] SCORE: {score_str}"]
    if team:
        parts.append(f"| Team: {team}")
    if scorer:
        parts.append(f"| ⚽ {scorer}")
    if assist:
        parts.append(f"| 🅰️ {assist}")
    if goal_type:
        parts.append(f"| Type: {goal_type}")
    return " ".join(parts).strip() + "\n"


def header_text(run_id: str, game_id: str, known_tsec: int, score_home: int, score_away: int) -> str:
    return (
        f"RUN: {run_id}\n"
        f"GAME: {game_id}\n"
        f"Current game time: {fmt_mmss(known_tsec)}\n"
        f"SCORE: {score_home}-{score_away}\n"
        "----------------------------------------\n"
    )


# -------------------------
# Disk loader (supports API-Sports dumps)
# -------------------------

def load_json_or_jsonl(game_folder: Path, base_name: str) -> List[dict]:
    p_json = game_folder / f"{base_name}.json"
    p_jsonl = game_folder / f"{base_name}.jsonl"

    if p_json.exists():
        raw = json.loads(p_json.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict) and isinstance(raw.get("response"), list):
            return raw["response"]
        raise ValueError(f"{p_json.name} must be a list OR dict with 'response' list")

    if p_jsonl.exists():
        out: List[dict] = []
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


def event_time_tuple(e: dict) -> Tuple[int, int]:
    t = e.get("time")
    if isinstance(t, dict):
        minute = int(t.get("elapsed", 0) or 0)
        extra = int(t.get("extra", 0) or 0)
        return (minute, extra)
    minute = int(e.get("minute", 0) or 0)
    extra = int(e.get("extra", 0) or 0)
    return (minute, extra)


def event_time_sec(e: dict) -> Optional[int]:
    m, x = event_time_tuple(e)
    return (m + x) * 60


def is_goal_event(e: dict) -> bool:
    return e.get("type") == "Goal"


def extract_team_name(e: dict) -> Optional[str]:
    team = e.get("team")
    if isinstance(team, dict):
        name = team.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return None


# -------------------------
# Store buffers
# -------------------------

@dataclass
class GameBuffers:
    comments: deque
    events: deque
    scores: deque  # Last 10 score events (for LLM context)
    score_str: str
    known_tsec: int
    
    # NEW: Track full match score (accurate even with 100+ goals)
    score_home: int = 0
    score_away: int = 0

class KafkaContextStore:
    """
    Kafka-first context:
      - bootstrap from disk up to offset (so if you start at 70', you still have memory)
      - then consume Kafka topics to keep files updated continuously
    """

    def _find_repo_root(self, p: Path) -> Path:
        """
        Accepts /app, /app/backend, /app/backend/..., etc.
        Returns the folder that contains BOTH: data/ and backend/
        """
        cur = p.resolve()
        for _ in range(6):  # climb a few levels max
            if (cur / "data").exists() and (cur / "backend").exists():
                return cur
            if cur.parent == cur:
                break
            cur = cur.parent
        # fallback: keep your old logic
        return p.resolve() if (p.resolve() / "data").exists() else p.resolve().parent


    def __init__(self, project_root: Path, kafka_bootstrap: str = "localhost:9092",
                top_k: int = 10, kafka_config: Optional[Dict[str, Any]] = None):

        self.root = self._find_repo_root(Path(project_root))

        self.kafka_bootstrap = kafka_bootstrap
        self.top_k = top_k
        self.kafka_config = kafka_config or {}

        # Data location (Railway best: set REPLAY_DATA_DIR=/app/data/games)
        self.data_dir = Path(os.getenv("REPLAY_DATA_DIR", str(self.root / "data" / "games"))).resolve()

        # Runtime location (optional env override)
        self.runtime_dir = Path(os.getenv("REPLAY_RUNTIME_DIR", str(self.root / "backend" / "runtime"))).resolve()
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._run_id: Optional[str] = None
        self._active_game_ids: set[str] = set()
        self._buffers: Dict[str, GameBuffers] = {}
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

    def get_run_id(self) -> Optional[str]:
        with self._lock:
            return self._run_id

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
        Bootstraps from disk + start consumers.
        Consumers use auto_offset_reset="latest" so we only get live stream messages for this run.
        """
        with self._lock:
            if self._run_id is not None:
                raise RuntimeError("Already started. Call reset() first.")
            self._run_id = run_id
            self._active_game_ids = set(active_game_ids)

        # bootstrap from disk for each game
        for gid in active_game_ids:
            start_at = game_start_offsets.get(gid, (0, 0))
            self._bootstrap_game_from_disk(run_id, gid, start_at)

        # start consumers
        self._threads = [
            threading.Thread(target=self._consume_comments, daemon=True),
            threading.Thread(target=self._consume_events, daemon=True),
            threading.Thread(target=self._consume_scores, daemon=True),
        ]
        for t in self._threads:
            t.start()

    # -------------------------
    # runtime dirs + writers
    # -------------------------

    def _game_runtime_dir(self, run_id: str, game_id: str) -> Path:
        # Only use game_id in path - overwrite same file each time
        return self.runtime_dir / game_id

    def _write_all_files(self, run_id: str, game_id: str):
        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return

            # Use only game_id directory (no run_id subdirectory)
            rdir = self._game_runtime_dir(run_id, game_id)
            rdir.mkdir(parents=True, exist_ok=True)

            # Use actual tracked score (accurate even with 100+ goals)
            hdr = header_text(run_id, game_id, buf.known_tsec, buf.score_home, buf.score_away)

            # separate files (still create these for reference)
            (rdir / "rag_comments.txt").write_text(hdr + "".join(list(buf.comments)), encoding="utf-8")
            (rdir / "rag_events.txt").write_text(hdr + "".join(list(buf.events)), encoding="utf-8")
            (rdir / "rag_scores.txt").write_text(hdr + "".join(list(buf.scores)), encoding="utf-8")

            # combined file (what the chatbot will use)
            # Include ALL items from each buffer (up to 10 each = 30 total)
            combined = []
            combined.extend(list(buf.scores))     # Up to 10 scores
            combined.extend(list(buf.events))     # Up to 10 events
            combined.extend(list(buf.comments))   # Up to 10 comments
            # Don't limit - give LLM all available context (max 30 items)
            (rdir / "rag_recap.txt").write_text(hdr + "".join(combined), encoding="utf-8")

    def get_topk_text(self, game_id: str) -> str:
        with self._lock:
            rid = self._run_id
        if not rid:
            return ""
        p = self._game_runtime_dir(rid, game_id) / "rag_recap.txt"
        return p.read_text(encoding="utf-8") if p.exists() else ""

    # -------------------------
    # bootstrap from disk
    # -------------------------

    def _bootstrap_game_from_disk(self, run_id: str, game_id: str, start_at_tuple: Tuple[int, int]):
        gfolder = self.data_dir / game_id
        minute, extra = int(start_at_tuple[0]), int(start_at_tuple[1])
        offset_tsec = (minute + extra) * 60

        comments = load_json_or_jsonl(gfolder, "comments")
        events = load_json_or_jsonl(gfolder, "events")

        comments.sort(key=comment_time_sec)
        events = [e for e in events if event_time_sec(e) is not None]
        events.sort(key=lambda e: event_time_sec(e) or 0)

        # last K comments BEFORE offset
        past_comments = [c for c in comments if comment_time_sec(c) <= offset_tsec]
        past_comments = past_comments[-self.top_k:]
        comment_lines = deque(maxlen=self.top_k)
        for c in past_comments:
            comment_lines.append(
                comment_line({
                    "event_time_sec": comment_time_sec(c),
                    "minute": int(c.get("minute", 0) or 0),
                    "extra": int(c.get("extra", 0) or 0),
                    "type": c.get("type") or "Comment",
                    "text": c.get("text") or "",
                })
            )

        # last K events BEFORE offset
        past_events = [e for e in events if (event_time_sec(e) or 0) <= offset_tsec]
        past_events = past_events[-self.top_k:]
        event_lines = deque(maxlen=self.top_k)
        for e in past_events:
            m, x = event_time_tuple(e)
            event_lines.append(
                event_line({
                    "event_time_sec": event_time_sec(e) or 0,
                    "minute": m,
                    "extra": x,
                    "type": e.get("type") or "Event",
                    "detail": e.get("detail"),
                    "data": e,
                })
            )

        # score history (goals) BEFORE offset
        meta_path = gfolder / "meta.json"
        home = "Home"
        away = "Away"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                home = meta.get("team_home", home)
                away = meta.get("team_away", away)
            except Exception:
                pass

        score_home = 0
        score_away = 0
        goal_lines = []
        for e in past_events:
            if not is_goal_event(e):
                continue
            team = extract_team_name(e)
            if not team:
                continue
            # update
            if team == home:
                score_home += 1
            elif team == away:
                score_away += 1
            m, x = event_time_tuple(e)
            scorer = e.get("player", {}).get("name") if isinstance(e.get("player"), dict) else None
            assist = e.get("assist", {}).get("name") if isinstance(e.get("assist"), dict) else None
            goal_type = e.get("detail")

            goal_lines.append(score_line({
                "tsec": event_time_sec(e) or 0,
                "minute": m,
                "extra": x,
                "score_home": score_home,
                "score_away": score_away,
                "score_str": f"{score_home}-{score_away}",
                "team": team,
                "scorer": scorer,
                "assist": assist,
                "goal_type": goal_type,
            }))

        goal_lines = goal_lines[-self.top_k:]
        score_deque = deque(goal_lines, maxlen=self.top_k)
        score_str = f"{score_home}-{score_away}"

        with self._lock:
            self._buffers[game_id] = GameBuffers(
                comments=comment_lines,
                events=event_lines,
                scores=score_deque,
                score_str=score_str,
                known_tsec=offset_tsec,
                score_home=score_home,  # ✅ Initialize with calculated score
                score_away=score_away,  # ✅ Initialize with calculated score
            )

        self._write_all_files(run_id, game_id)

    # -------------------------
    # Kafka consumers (LIVE)
    # -------------------------

    def _consume_comments(self):
        consumer = KafkaConsumer(
            "games.comments",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-comments-{int(time.time())}",
            **self.kafka_config,
        )
        while not self._stop.is_set():
            for msg in consumer:
                self._handle_comment(msg.value)
                if self._stop.is_set():
                    break

    def _consume_events(self):
        consumer = KafkaConsumer(
            "games.comments",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-comments-{int(time.time())}",
            **self.kafka_config,
        )
        while not self._stop.is_set():
            for msg in consumer:
                self._handle_event(msg.value)
                if self._stop.is_set():
                    break

    def _consume_scores(self):
        consumer = KafkaConsumer(
            "games.comments",
            bootstrap_servers=self.kafka_bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=500,
            group_id=f"ctx-comments-{int(time.time())}",
            **self.kafka_config,
        )
        while not self._stop.is_set():
            for msg in consumer:
                self._handle_score(msg.value)
                if self._stop.is_set():
                    break

    # -------------------------
    # handlers
    # -------------------------

    def _accept(self, item: dict) -> Tuple[Optional[str], Optional[str]]:
        run_id = item.get("run_id")
        game_id = item.get("game_id")
        with self._lock:
            if self._run_id != run_id:
                return None, None
            if game_id not in self._active_game_ids:
                return None, None
        return run_id, game_id

    def _handle_comment(self, item: dict):
        run_id, game_id = self._accept(item)
        if not run_id:
            return

        line = comment_line(item)

        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return
            buf.comments.append(line)
            # update known time
            tsec = int(item.get("event_time_sec", 0) or 0)
            buf.known_tsec = max(buf.known_tsec, tsec)

        self._write_all_files(run_id, game_id)

    def _handle_event(self, item: dict):
        run_id, game_id = self._accept(item)
        if not run_id:
            return

        line = event_line(item)

        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return
            buf.events.append(line)
            tsec = int(item.get("event_time_sec", 0) or 0)
            buf.known_tsec = max(buf.known_tsec, tsec)

        self._write_all_files(run_id, game_id)

    def _handle_score(self, item: dict):
        run_id, game_id = self._accept(item)
        if not run_id:
            return

        line = score_line(item)

        with self._lock:
            buf = self._buffers.get(game_id)
            if not buf:
                return
            
            # Add to rolling window (last 10 for LLM)
            buf.scores.append(line)
            
            # Update actual score (tracks full history)
            score_home = item.get("score_home")
            score_away = item.get("score_away")
            if score_home is not None:
                buf.score_home = int(score_home)
            if score_away is not None:
                buf.score_away = int(score_away)
            
            # Also keep score_str for compatibility
            buf.score_str = item.get("score_str") or f"{buf.score_home}-{buf.score_away}"
            
            # Update timestamp
            tsec = int(item.get("event_time_sec", 0) or item.get("tsec", 0) or 0)
            buf.known_tsec = max(buf.known_tsec, tsec)

        self._write_all_files(run_id, game_id)