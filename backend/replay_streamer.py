# backend/replay_streamer.py
"""
Replay Streamer - Simulates a Live Streaming API

This module simulates what a real streaming API would do:
  1. Load historical match data from disk
  2. Stream it chronologically through Kafka topics
  3. Publish to Kafka ONLY (no direct database writes)

In a real system:
  Real API → Kafka

In your simulation:
  This Producer → Kafka

The downstream consumer (kafka_to_qdrant_consumer.py) handles all data ingestion.
"""

import json
import time
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from kafka import KafkaProducer


# -------------------------
# Helpers: time + formatting
# -------------------------

def stream_time_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def fmt_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}:{s:02d}"


# -------------------------
# Loaders: JSON or JSONL
# -------------------------

def load_json_or_jsonl(game_folder: Path, base_name: str) -> List[dict]:
    json_path = game_folder / f"{base_name}.json"
    jsonl_path = game_folder / f"{base_name}.jsonl"

    if json_path.exists():
        data = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"{base_name}.json is not a JSON list")
        return data

    if jsonl_path.exists():
        out: List[dict] = []
        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
        return out

    return []


def load_comments(game_folder: Path) -> List[dict]:
    data = load_json_or_jsonl(game_folder, "comments")
    if not data:
        raise FileNotFoundError("missing comments.json or comments.jsonl")
    return data


def load_events(game_folder: Path) -> List[dict]:
    return load_json_or_jsonl(game_folder, "events")


def load_meta(game_folder: Path) -> dict:
    meta_path = game_folder / "meta.json"
    if meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))
    return {}


# -------------------------
# Time extractors
# -------------------------

def comment_time_sec(c: dict) -> int:
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    second = int(c.get("second", 0) or 0)
    return (minute + extra) * 60 + second


def event_time_sec(e: dict) -> Optional[int]:
    if "event_time_sec" in e and isinstance(e["event_time_sec"], (int, float)):
        return int(e["event_time_sec"])

    if "minute" in e:
        minute = int(e.get("minute", 0) or 0)
        second = int(e.get("second", 0) or 0)
        extra = int(e.get("extra", 0) or 0)
        return (minute + extra) * 60 + second

    t = e.get("time")
    if isinstance(t, dict) and "elapsed" in t:
        minute = int(t.get("elapsed", 0) or 0)
        extra = int(t.get("extra", 0) or 0)
        second = int(t.get("second", 0) or 0)
        return (minute + extra) * 60 + second

    if "elapsed" in e and isinstance(e["elapsed"], (int, float)):
        return int(e["elapsed"]) * 60

    return None


# -------------------------
# NEW: Tuple-based time extractors (for accurate comparison)
# -------------------------

def comment_time_tuple(c: dict) -> Tuple[int, int]:
    """
    Extract (minute, extra) tuple from comment.
    Returns (minute, extra) where minute is base minute and extra is injury time.
    """
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    
    # Fallback: if no minute field, try to extract from time_sec
    if minute == 0 and "time_sec" in c:
        time_sec = int(c.get("time_sec", 0) or 0)
        minute = time_sec // 60
        extra = 0  # Can't determine extra from seconds alone
    
    return (minute, extra)


def event_time_tuple(e: dict) -> Tuple[int, int]:
    """
    Extract (minute, extra) tuple from event.
    Returns (minute, extra) where minute is base minute and extra is injury time.
    """
    # Try different event formats
    if "minute" in e:
        minute = int(e.get("minute", 0) or 0)
        extra = int(e.get("extra", 0) or 0)
        return (minute, extra)
    
    t = e.get("time")
    if isinstance(t, dict) and "elapsed" in t:
        minute = int(t.get("elapsed", 0) or 0)
        extra = int(t.get("extra", 0) or 0)
        return (minute, extra)
    
    if "elapsed" in e and isinstance(e["elapsed"], (int, float)):
        minute = int(e["elapsed"] or 0)
        return (minute, 0)
    
    # Fallback to converting from seconds
    if "event_time_sec" in e:
        tsec = int(e["event_time_sec"])
        minute = tsec // 60
        return (minute, 0)
    
    return (0, 0)


# -------------------------
# Score extraction from events (API-Football style)
# -------------------------

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
        # other team gets the goal
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


# -------------------------
# ReplayManager
# -------------------------

class ReplayManager:
    """
    Streams multiple games concurrently (1 thread per game)

    Kafka topics:
      - games.comments : commentary stream
      - games.events   : events stream
      - games.state    : progress snapshots
      - games.scores   : score changes

    NOTE: This producer does NOT write to Qdrant directly.
    All data ingestion is handled by the separate kafka_to_qdrant_consumer service.
    """

    def __init__(self, project_root: Path, kafka_bootstrap: str = "localhost:9092"):
        self.root = project_root
        self.data_dir = self.root / "data" / "games"
        self.kafka_bootstrap = kafka_bootstrap

        self._lock = threading.Lock()
        self._active_run_id: Optional[str] = None

        self._progress: Dict[str, Dict[str, Any]] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

    def list_games(self) -> List[dict]:
        games: List[dict] = []
        if not self.data_dir.exists():
            return games

        for folder in sorted(self.data_dir.iterdir()):
            if folder.is_dir():
                meta_path = folder / "meta.json"
                if meta_path.exists():
                    try:
                        games.append(json.loads(meta_path.read_text(encoding="utf-8")))
                    except Exception:
                        pass
        return games

    def get_progress(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "run_id": self._active_run_id,
                "active_game_ids": list(self._progress.keys()),
                "progress": self._progress,
            }

    def stop_all(self):
        with self._lock:
            for flag in self._stop_flags.values():
                flag.set()
            self._active_run_id = None

        for t in self._threads.values():
            t.join(timeout=5)

        with self._lock:
            self._progress.clear()
            self._threads.clear()
            self._stop_flags.clear()

    def start_games(self, run_id: str, game_start_offsets: Dict[str, Tuple[int, int]], tick_sec: int = 60):
        """Start streaming from (minute, extra) tuples."""
        with self._lock:
            self._active_run_id = run_id
            for gid, offset_tuple in game_start_offsets.items():
                if gid in self._threads:
                    continue

                minute, extra = offset_tuple
                
                self._progress[gid] = {
                    "status": "starting",
                    "known_minute": minute,
                    "known_extra": extra,
                    "comments_sent_total": 0,
                    "events_sent_total": 0,
                    "score_str": "0-0",
                    "error": None,
                    "last_update_unix": int(time.time()),
                }

                stop_flag = threading.Event()
                self._stop_flags[gid] = stop_flag

                t = threading.Thread(
                    target=self._stream_one_game,
                    args=(run_id, gid, tick_sec, offset_tuple, stop_flag),
                    daemon=True,
                )
                t.start()
                self._threads[gid] = t

    # ---------- Kafka publishers ----------

    def _publish_state(self, run_id: str, game_id: str, tick_sec: int, known_time_sec: int, status: str, comments_sent: int, events_sent: int, score_str: str, error: Optional[str] = None):
        payload = {
            "run_id": run_id,
            "game_id": game_id,
            "known_time_sec": known_time_sec,
            "known_minute": known_time_sec // 60,
            "tick_sec": tick_sec,
            "status": status,
            "comments_sent_in_tick": comments_sent,
            "events_sent_in_tick": events_sent,
            "score_str": score_str,
            "updated_at_unix": int(time.time()),
        }
        if error:
            payload["error"] = error
        self.producer.send("games.state", value=payload)

    def _publish_score(self, run_id: str, game_id: str, tsec: int, score_home: int, score_away: int, why: str, goal_data: Optional[dict] = None):
        """
        Publish score update with detailed goal information.
        
        Args:
            goal_data: Dict containing:
                - scorer: Player who scored
                - assist: Player who assisted (if any)
                - goal_type: Type of goal (Normal Goal, Penalty, Own Goal, etc.)
                - team: Team that scored
                - minute: Base minute
                - extra: Extra time
        """
        payload = {
            "run_id": run_id,
            "game_id": game_id,
            "event_time_sec": tsec,
            "score_home": score_home,
            "score_away": score_away,
            "score_str": f"{score_home}-{score_away}",
            "why": why,
            "updated_at_unix": int(time.time()),
        }
        
        # Add minute and extra if available from goal_data, otherwise calculate from tsec
        if goal_data and "minute" in goal_data:
            payload["minute"] = goal_data["minute"]
            payload["extra"] = goal_data.get("extra", 0)
        else:
            payload["minute"] = tsec // 60
            payload["extra"] = 0
        
        # Add detailed goal information if available
        if goal_data:
            if goal_data.get("scorer"):
                payload["scorer"] = goal_data["scorer"]
            if goal_data.get("assist"):
                payload["assist"] = goal_data["assist"]
            if goal_data.get("goal_type"):
                payload["goal_type"] = goal_data["goal_type"]
            if goal_data.get("team"):
                payload["team"] = goal_data["team"]
        
        self.producer.send("games.scores", value=payload)

    # ---------- main streaming loop ----------

    def _stream_one_game(self, run_id: str, game_id: str, tick_sec: int, start_at_tuple: Tuple[int, int], stop_event: threading.Event):
        """Stream game data starting from (minute, extra) tuple."""
        game_folder = self.data_dir / game_id

        def set_error(msg: str):
            with self._lock:
                p = self._progress.get(game_id, {})
                p["status"] = "error"
                p["error"] = msg
                p["last_update_unix"] = int(time.time())
                self._progress[game_id] = p
            minute, extra = start_at_tuple
            self._publish_state(run_id, game_id, tick_sec, (minute + extra) * 60, "error", 0, 0, p.get("score_str", "0-0"), error=msg)
            self.producer.flush()

        # Load data
        try:
            meta = load_meta(game_folder)
            home = (meta.get("team_home") or "").strip()
            away = (meta.get("team_away") or "").strip()
            comments = load_comments(game_folder)
        except Exception as e:
            set_error(str(e))
            return

        try:
            events = load_events(game_folder)
        except Exception:
            events = []

        comments.sort(key=comment_time_tuple)
        events = [e for e in events if event_time_tuple(e) is not None]
        events.sort(key=event_time_tuple)

        # Find starting indices using TUPLE comparison
        c_idx = 0
        while c_idx < len(comments) and comment_time_tuple(comments[c_idx]) < start_at_tuple:
            c_idx += 1

        e_idx = 0
        while e_idx < len(events) and event_time_tuple(events[e_idx]) < start_at_tuple:
            e_idx += 1
        
        # DEBUG: Log what we're starting at
        print(f"\n🎬 Starting stream for {game_id}:")
        print(f"   Requested: {start_at_tuple} (minute={start_at_tuple[0]}, extra={start_at_tuple[1]})")
        if c_idx < len(comments):
            actual_comment_tuple = comment_time_tuple(comments[c_idx])
            print(f"   Starting at comment: {actual_comment_tuple} (index={c_idx}/{len(comments)})")
        else:
            print(f"   No comments at or after requested time")
        if e_idx < len(events):
            actual_event_tuple = event_time_tuple(events[e_idx])
            print(f"   Starting at event: {actual_event_tuple} (index={e_idx}/{len(events)})")
        else:
            print(f"   No events at or after requested time")
        print()

        # Precompute score up to offset
        score_home = 0
        score_away = 0
        for i in range(0, e_idx):
            e = events[i]
            if not is_goal_event(e):
                continue
            team_name = extract_team_name(e)
            if not team_name:
                continue
            detail = e.get("detail") if isinstance(e.get("detail"), str) else None
            score_home, score_away, _ = apply_goal(score_home, score_away, team_name, home, away, detail)
        score_str = f"{score_home}-{score_away}"

        known_minute, known_extra = start_at_tuple
        known_time_sec = (known_minute + known_extra) * 60  # For progress tracking

        with self._lock:
            p = self._progress[game_id]
            p["status"] = "streaming"
            p["known_minute"] = known_minute
            p["known_extra"] = known_extra
            p["score_str"] = score_str
            p["error"] = None
            p["last_update_unix"] = int(time.time())

        self._publish_state(run_id, game_id, tick_sec, known_time_sec, "streaming", 0, 0, score_str)
        self.producer.flush()

        # ========================================
        # PUBLISH ALL HISTORICAL DATA TO KAFKA
        # ========================================
        # This ensures Deep Think mode can see the entire match history,
        # not just data from the start time onwards.
        
        if c_idx > 0 or e_idx > 0:
            print(f"\n📦 Publishing historical data to Kafka (for Deep Think mode)...")
            historical_start = time.time()
            
            # Publish historical comments
            if c_idx > 0:
                print(f"   📝 Publishing {c_idx} historical comments (0 to {start_at_tuple})...")
                for i in range(0, c_idx):
                    c = comments[i]
                    tsec = comment_time_sec(c)
                    payload = {
                        "run_id": run_id,
                        "game_id": game_id,
                        "event_time_sec": tsec,
                        "minute": int(c.get("minute", 0) or 0),
                        "extra": int(c.get("extra", 0) or 0),
                        "type": c.get("type"),
                        "text": c.get("text"),
                    }
                    self.producer.send("games.comments", value=payload)
            
            # Publish historical events
            if e_idx > 0:
                print(f"   🎯 Publishing {e_idx} historical events (0 to {start_at_tuple})...")
                for i in range(0, e_idx):
                    e = events[i]
                    tsec = event_time_sec(e) or 0
                    etype = e.get("type") or e.get("event") or e.get("name") or "Event"
                    payload = {
                        "run_id": run_id,
                        "game_id": game_id,
                        "event_time_sec": tsec,
                        "type": etype,
                        "data": e,
                    }
                    self.producer.send("games.events", value=payload)
            
            # Publish historical scores (goals only)
            # Reset score counters for this loop
            hist_score_home = 0
            hist_score_away = 0
            goals_published = 0
            
            for i in range(0, e_idx):
                e = events[i]
                if not is_goal_event(e):
                    continue
                    
                team_name = extract_team_name(e)
                if not team_name:
                    continue
                
                detail = e.get("detail") if isinstance(e.get("detail"), str) else None
                new_home, new_away, why = apply_goal(hist_score_home, hist_score_away, team_name, home, away, detail)
                
                if (new_home, new_away) != (hist_score_home, hist_score_away):
                    hist_score_home, hist_score_away = new_home, new_away
                    
                    # Extract minute/extra from event
                    event_minute = None
                    event_extra = None
                    if "time" in e and isinstance(e["time"], dict):
                        event_minute = e["time"].get("elapsed")
                        event_extra = e["time"].get("extra", 0)
                    elif "minute" in e:
                        event_minute = e.get("minute")
                        event_extra = e.get("extra", 0)
                    
                    # Extract goal details
                    goal_data = {
                        "team": team_name,
                        "goal_type": detail or "Normal Goal",
                        "minute": event_minute,
                        "extra": event_extra,
                    }
                    
                    if isinstance(e.get("player"), dict):
                        scorer_name = e["player"].get("name")
                        if scorer_name:
                            goal_data["scorer"] = scorer_name
                    
                    if isinstance(e.get("assist"), dict):
                        assist_name = e["assist"].get("name")
                        if assist_name:
                            goal_data["assist"] = assist_name
                    
                    tsec = event_time_sec(e) or 0
                    self._publish_score(run_id, game_id, tsec, hist_score_home, hist_score_away, why, goal_data=goal_data)
                    goals_published += 1
            
            # Flush all historical data
            self.producer.flush()
            
            historical_duration = time.time() - historical_start
            print(f"   ⚡ Published {goals_published} historical goals")
            print(f"   ✅ Historical data published in {historical_duration:.2f}s")
            print(f"   🔄 Qdrant consumer will now ingest this data...")
            
            # Give Qdrant consumer a moment to start ingesting
            # (This is optional but helps ensure data is available quickly)
            time.sleep(1)
            
            print(f"\n▶️  Starting live stream from {start_at_tuple}...\n")
        else:
            print(f"\n▶️  Starting live stream from beginning (no historical data)...\n")

        cursor = known_time_sec  # Use seconds for cursor progression

        while not stop_event.is_set():
            cursor += tick_sec
            window_end = cursor

            comments_batch: List[Tuple[int, dict]] = []
            while c_idx < len(comments):
                tsec = comment_time_sec(comments[c_idx])
                if tsec <= window_end:
                    comments_batch.append((tsec, comments[c_idx]))
                    c_idx += 1
                else:
                    break

            events_batch: List[Tuple[int, dict]] = []
            while e_idx < len(events):
                tsec = event_time_sec(events[e_idx]) or 0
                if tsec <= window_end:
                    events_batch.append((tsec, events[e_idx]))
                    e_idx += 1
                else:
                    break

            # Stream comments to Kafka ONLY
            for tsec, c in comments_batch:
                payload = {
                    "run_id": run_id,
                    "game_id": game_id,
                    "event_time_sec": tsec,
                    "minute": int(c.get("minute", 0) or 0),
                    "extra": int(c.get("extra", 0) or 0),
                    "type": c.get("type"),
                    "text": c.get("text"),
                }
                self.producer.send("games.comments", value=payload)

            # Stream events to Kafka ONLY
            for tsec, e in events_batch:
                etype = e.get("type") or e.get("event") or e.get("name") or "Event"
                payload = {
                    "run_id": run_id,
                    "game_id": game_id,
                    "event_time_sec": tsec,
                    "type": etype,
                    "data": e,
                }
                self.producer.send("games.events", value=payload)

                # Score updates
                if is_goal_event(e) and home and away:
                    scoring_team = extract_team_name(e)
                    if scoring_team:
                        detail = e.get("detail") if isinstance(e.get("detail"), str) else None
                        new_home, new_away, why = apply_goal(score_home, score_away, scoring_team, home, away, detail)
                        if (new_home, new_away) != (score_home, score_away):
                            score_home, score_away = new_home, new_away
                            score_str = f"{score_home}-{score_away}"

                            # Extract minute and extra from event
                            event_minute = None
                            event_extra = None
                            if "time" in e and isinstance(e["time"], dict):
                                event_minute = e["time"].get("elapsed")
                                event_extra = e["time"].get("extra", 0)
                            elif "minute" in e:
                                event_minute = e.get("minute")
                                event_extra = e.get("extra", 0)

                            # Extract detailed goal information
                            goal_data = {
                                "team": scoring_team,
                                "goal_type": detail or "Normal Goal",
                                "minute": event_minute,
                                "extra": event_extra,
                            }
                            
                            # Extract scorer (player who scored)
                            if isinstance(e.get("player"), dict):
                                scorer_name = e["player"].get("name")
                                if scorer_name:
                                    goal_data["scorer"] = scorer_name
                            
                            # Extract assist (player who assisted)
                            if isinstance(e.get("assist"), dict):
                                assist_name = e["assist"].get("name")
                                if assist_name:
                                    goal_data["assist"] = assist_name

                            # Publish score update to Kafka with goal details
                            self._publish_score(run_id, game_id, tsec, score_home, score_away, why, goal_data=goal_data)

            # Update known time from latest data
            latest_minute = known_minute
            latest_extra = known_extra
            
            if comments_batch:
                known_time_sec = comments_batch[-1][0]
                # Extract minute and extra from the latest comment
                last_comment = comments_batch[-1][1]
                latest_minute = int(last_comment.get("minute", 0) or 0)
                latest_extra = int(last_comment.get("extra", 0) or 0)
            elif events_batch:
                known_time_sec = events_batch[-1][0]
                # Extract minute and extra from the latest event
                last_event = events_batch[-1][1]
                t = last_event.get("time")
                if isinstance(t, dict):
                    latest_minute = int(t.get("elapsed", 0) or 0)
                    latest_extra = int(t.get("extra", 0) or 0)

            # publish state
            self._publish_state(
                run_id, game_id, tick_sec, known_time_sec, "streaming",
                comments_sent=len(comments_batch),
                events_sent=len(events_batch),
                score_str=score_str
            )
            self.producer.flush()

            with self._lock:
                p = self._progress[game_id]
                p["known_minute"] = latest_minute
                p["known_extra"] = latest_extra
                p["comments_sent_total"] = int(p.get("comments_sent_total", 0)) + len(comments_batch)
                p["events_sent_total"] = int(p.get("events_sent_total", 0)) + len(events_batch)
                p["score_str"] = score_str
                p["status"] = "streaming"
                p["last_update_unix"] = int(time.time())

            if c_idx >= len(comments) and e_idx >= len(events):
                with self._lock:
                    self._progress[game_id]["status"] = "finished"
                    self._progress[game_id]["last_update_unix"] = int(time.time())
                self._publish_state(run_id, game_id, tick_sec, known_time_sec, "finished", 0, 0, score_str)
                self.producer.flush()
                break

            time.sleep(tick_sec)