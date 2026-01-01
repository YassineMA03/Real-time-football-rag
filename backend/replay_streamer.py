# backend/replay_streamer.py
from __future__ import annotations

import json
import time
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from kafka import KafkaProducer


# -------------------------
# Helpers: time extraction
# -------------------------

def fmt_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}:{s:02d}"


def comment_time_sec(c: dict) -> int:
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    second = int(c.get("second", 0) or 0)
    return (minute + extra) * 60 + second


def comment_time_tuple(c: dict) -> Tuple[int, int]:
    minute = int(c.get("minute", 0) or 0)
    extra = int(c.get("extra", 0) or 0)
    return (minute, extra)


def event_time_tuple(e: dict) -> Tuple[int, int]:
    """
    Handles typical API-Sports event schema:
      e["time"] = {"elapsed": 90, "extra": 2}
    Also supports {"minute":..., "extra":...}
    """
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
    return (e.get("type") == "Goal") or ((e.get("detail") or "").lower() in ["penalty", "normal goal"] and e.get("type") == "Goal")


def extract_team_name(e: dict) -> Optional[str]:
    team = e.get("team")
    if isinstance(team, dict):
        name = team.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(e.get("team"), str):
        return e["team"]
    return None


def apply_goal(score_home: int, score_away: int, team_name: str, home: str, away: str, detail: Optional[str]) -> Tuple[int, int, str]:
    """
    Very simple scoring:
    - If team_name matches home: home++
    - If matches away: away++
    Otherwise: unchanged
    """
    if team_name == home:
        return score_home + 1, score_away, "goal_home"
    if team_name == away:
        return score_home, score_away + 1, "goal_away"
    return score_home, score_away, "goal_unknown_team"


def load_json_or_jsonl(game_folder: Path, base_name: str) -> List[dict]:
    """
    Supports:
      - <base_name>.json  : JSON array OR {"response":[...]} (API-Sports dump)
      - <base_name>.jsonl : JSON lines
    """
    p_json = game_folder / f"{base_name}.json"
    p_jsonl = game_folder / f"{base_name}.jsonl"

    if p_json.exists():
        raw = json.loads(p_json.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return raw
        # API-Sports style
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


def load_comments(game_folder: Path) -> List[dict]:
    data = load_json_or_jsonl(game_folder, "comments")
    if not data:
        raise FileNotFoundError("missing comments.json or comments.jsonl")
    return data


def load_events(game_folder: Path) -> List[dict]:
    return load_json_or_jsonl(game_folder, "events")


# -------------------------
# ReplayManager (Kafka Producer)
# -------------------------

class ReplayManager:
    """
    Streams games into Kafka.
    Topics:
      - games.comments
      - games.events
      - games.scores
      - games.state
    """

    def __init__(self, project_root: Path, kafka_bootstrap: str = "localhost:9092", kafka_config: Optional[Dict[str, Any]] = None):
        self.root = project_root
        self.data_dir = self.root / "data" / "games"
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_config = kafka_config or {}
        self._lock = threading.Lock()
        self._active_run_id: Optional[str] = None
        self._progress: Dict[str, Dict[str, Any]] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            **self.kafka_config,
        )

    def list_games(self) -> List[dict]:
        games: List[dict] = []
        if not self.data_dir.exists():
            return games
        for folder in sorted(self.data_dir.iterdir()):
            if folder.is_dir():
                meta = folder / "meta.json"
                if meta.exists():
                    try:
                        games.append(json.loads(meta.read_text(encoding="utf-8")))
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
        for ev in self._stop_flags.values():
            ev.set()

        with self._lock:
            for gid in list(self._progress.keys()):
                self._progress[gid]["status"] = "stopped"
                self._progress[gid]["last_update_unix"] = int(time.time())
            self._progress.clear()
            self._threads.clear()
            self._stop_flags.clear()
            self._active_run_id = None

    def start_games(self, run_id: str, game_start_offsets: Dict[str, Tuple[int, int]], tick_sec: int):
        tick_sec = max(1, int(tick_sec))
        if self._active_run_id is None:
            self._active_run_id = run_id

        for game_id, start_at_tuple in game_start_offsets.items():
            if game_id in self._threads and self._threads[game_id].is_alive():
                continue

            stop_event = threading.Event()
            self._stop_flags[game_id] = stop_event

            with self._lock:
                self._progress[game_id] = {
                    "status": "starting",
                    "tick_sec": tick_sec,
                    "start_at_minute": int(start_at_tuple[0]),
                    "start_at_extra": int(start_at_tuple[1]),
                    "known_minute": int(start_at_tuple[0]),
                    "known_extra": int(start_at_tuple[1]),
                    "known_time_sec": int((start_at_tuple[0] + start_at_tuple[1]) * 60),
                    "comments_sent_total": 0,
                    "events_sent_total": 0,
                    "score_str": "0-0",
                    "error": None,
                    "last_update_unix": int(time.time()),
                }

            t = threading.Thread(
                target=self._stream_one_game,
                args=(run_id, game_id, tick_sec, start_at_tuple, stop_event),
                daemon=True,
            )
            self._threads[game_id] = t
            t.start()

    # --------------- Kafka publishes ---------------

    def _publish_state(
        self,
        run_id: str,
        game_id: str,
        tick_sec: int,
        known_time_sec: int,
        known_minute: int,
        known_extra: int,
        status: str,
        score_str: str,
        comments_sent: int,
        events_sent: int,
        error: Optional[str] = None,
    ):
        payload = {
            "run_id": run_id,
            "game_id": game_id,
            "tick_sec": tick_sec,
            "status": status,
            "known_time_sec": known_time_sec,
            "known_minute": known_minute,
            "known_extra": known_extra,
            "score_str": score_str,
            "comments_sent_in_tick": comments_sent,
            "events_sent_in_tick": events_sent,
            "updated_at_unix": int(time.time()),
        }
        if error:
            payload["error"] = error
        self.producer.send("games.state", value=payload)

    def _publish_score(
        self,
        run_id: str,
        game_id: str,
        tsec: int,
        score_home: int,
        score_away: int,
        minute: Optional[int],
        extra: Optional[int],
        team: Optional[str],
        scorer: Optional[str],
        assist: Optional[str],
        goal_type: Optional[str],
    ):
        payload = {
            "run_id": run_id,
            "game_id": game_id,
            "tsec": tsec,
            "minute": minute,
            "extra": extra,
            "score_home": score_home,
            "score_away": score_away,
            "score_str": f"{score_home}-{score_away}",
            "team": team,
            "scorer": scorer,
            "assist": assist,
            "goal_type": goal_type,
            "updated_at_unix": int(time.time()),
        }
        self.producer.send("games.scores", value=payload)

    # --------------- streaming loop ---------------

    def _stream_one_game(
        self,
        run_id: str,
        game_id: str,
        tick_sec: int,
        start_at_tuple: Tuple[int, int],
        stop_event: threading.Event,
    ):
        game_folder = self.data_dir / game_id

        def set_error(msg: str):
            with self._lock:
                p = self._progress.get(game_id, {})
                p["status"] = "error"
                p["error"] = msg
                p["last_update_unix"] = int(time.time())
                self._progress[game_id] = p
            self._publish_state(
                run_id, game_id, tick_sec,
                known_time_sec=int((start_at_tuple[0] + start_at_tuple[1]) * 60),
                known_minute=int(start_at_tuple[0]),
                known_extra=int(start_at_tuple[1]),
                status="error",
                score_str=p.get("score_str", "0-0"),
                comments_sent=0,
                events_sent=0,
                error=msg,
            )
            self.producer.flush()

        try:
            comments = load_comments(game_folder)
        except Exception as e:
            set_error(str(e))
            return

        try:
            events = load_events(game_folder)
        except Exception:
            events = []

        # sort
        comments.sort(key=comment_time_sec)
        events = [e for e in events if event_time_sec(e) is not None]
        events.sort(key=lambda e: (event_time_tuple(e)[0], event_time_tuple(e)[1]))

        # meta teams
        meta_path = game_folder / "meta.json"
        home = "Home"
        away = "Away"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                home = meta.get("team_home", home)
                away = meta.get("team_away", away)
            except Exception:
                pass

        # find start indices
        c_idx = 0
        while c_idx < len(comments) and comment_time_tuple(comments[c_idx]) < start_at_tuple:
            c_idx += 1

        e_idx = 0
        while e_idx < len(events) and event_time_tuple(events[e_idx]) < start_at_tuple:
            e_idx += 1

        # score at offset (from events before offset)
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
        known_minute, known_extra = int(start_at_tuple[0]), int(start_at_tuple[1])
        known_time_sec = int((known_minute + known_extra) * 60)

        with self._lock:
            p = self._progress[game_id]
            p["status"] = "streaming"
            p["known_minute"] = known_minute
            p["known_extra"] = known_extra
            p["known_time_sec"] = known_time_sec
            p["score_str"] = score_str
            p["error"] = None
            p["last_update_unix"] = int(time.time())

        # initial state
        self._publish_state(
            run_id, game_id, tick_sec,
            known_time_sec=known_time_sec,
            known_minute=known_minute,
            known_extra=known_extra,
            status="streaming",
            score_str=score_str,
            comments_sent=0,
            events_sent=0,
        )
        self.producer.flush()

        # cursor in seconds
        cursor = known_time_sec

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

            # publish comments
            for tsec, c in comments_batch:
                payload = {
                    "run_id": run_id,
                    "game_id": game_id,
                    "event_time_sec": tsec,
                    "minute": int(c.get("minute", 0) or 0),
                    "extra": int(c.get("extra", 0) or 0),
                    "type": c.get("type") or "Comment",
                    "text": c.get("text") or "",
                }
                self.producer.send("games.comments", value=payload)

            # publish events + score updates
            for tsec, e in events_batch:
                m, x = event_time_tuple(e)
                etype = e.get("type") or e.get("event") or e.get("name") or "Event"

                self.producer.send("games.events", value={
                    "run_id": run_id,
                    "game_id": game_id,
                    "event_time_sec": tsec,
                    "minute": m,
                    "extra": x,
                    "type": etype,
                    "detail": e.get("detail"),
                    "data": e,
                })

                # score topic only on goals
                if is_goal_event(e):
                    team_name = extract_team_name(e)
                    detail = e.get("detail") if isinstance(e.get("detail"), str) else None
                    new_home, new_away, _ = apply_goal(score_home, score_away, team_name or "", home, away, detail)
                    if (new_home, new_away) != (score_home, score_away):
                        score_home, score_away = new_home, new_away
                        score_str = f"{score_home}-{score_away}"

                        scorer = None
                        assist = None
                        if isinstance(e.get("player"), dict):
                            scorer = e["player"].get("name")
                        if isinstance(e.get("assist"), dict):
                            assist = e["assist"].get("name")

                        self._publish_score(
                            run_id, game_id, tsec,
                            score_home, score_away,
                            minute=m, extra=x,
                            team=team_name,
                            scorer=scorer,
                            assist=assist,
                            goal_type=detail,
                        )

            # update known time using last emitted thing
            if comments_batch:
                last_c = comments_batch[-1][1]
                known_minute = int(last_c.get("minute", 0) or 0)
                known_extra = int(last_c.get("extra", 0) or 0)
                known_time_sec = comments_batch[-1][0]
            elif events_batch:
                last_e = events_batch[-1][1]
                known_minute, known_extra = event_time_tuple(last_e)
                known_time_sec = events_batch[-1][0]

            # state update
            self._publish_state(
                run_id, game_id, tick_sec,
                known_time_sec=known_time_sec,
                known_minute=known_minute,
                known_extra=known_extra,
                status="streaming",
                score_str=score_str,
                comments_sent=len(comments_batch),
                events_sent=len(events_batch),
            )
            self.producer.flush()

            with self._lock:
                p = self._progress[game_id]
                p["known_minute"] = known_minute
                p["known_extra"] = known_extra
                p["known_time_sec"] = known_time_sec
                p["score_str"] = score_str
                p["comments_sent_total"] = int(p.get("comments_sent_total", 0)) + len(comments_batch)
                p["events_sent_total"] = int(p.get("events_sent_total", 0)) + len(events_batch)
                p["status"] = "streaming"
                p["last_update_unix"] = int(time.time())

            if c_idx >= len(comments) and e_idx >= len(events):
                with self._lock:
                    self._progress[game_id]["status"] = "finished"
                    self._progress[game_id]["last_update_unix"] = int(time.time())
                self._publish_state(
                    run_id, game_id, tick_sec,
                    known_time_sec=known_time_sec,
                    known_minute=known_minute,
                    known_extra=known_extra,
                    status="finished",
                    score_str=score_str,
                    comments_sent=0,
                    events_sent=0,
                )
                self.producer.flush()
                break

            time.sleep(tick_sec)
