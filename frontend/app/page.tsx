"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

type GameMeta = {
  game_id: string;
  team_home: string;
  team_away: string;
  competition?: string;
  date?: string | null;
  duration_sec?: number;
  final_score?: string;
};

type ChatMsg = {
  role: "user" | "assistant";
  text: string;
  ts: number;
};

async function fetchAvailableGames(): Promise<GameMeta[]> {
  const res = await fetch("http://localhost:8000/api/games");
  if (!res.ok) throw new Error("Failed to load games");
  return await res.json();
}

async function startReplay(payload: { games: { game_id: string; startAtSec: number }[] }): Promise<any> {
  const res = await fetch("http://localhost:8000/api/replay/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Failed to start streaming");
  return await res.json();
}

async function resetReplay(): Promise<void> {
  const res = await fetch("http://localhost:8000/api/replay/reset", { method: "POST" });
  if (!res.ok) throw new Error("Failed to reset simulation");
}

async function fetchProgress(): Promise<any> {
  const res = await fetch("http://localhost:8000/api/replay/progress");
  if (!res.ok) throw new Error("Failed to load progress");
  return await res.json();
}

// Placeholder chat (until RAG endpoint exists)
async function chatForGame(params: { game_id: string; message: string }) {
  const res = await fetch("http://localhost:8000/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ game_id: params.game_id, message: params.message }),
  });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

function formatDuration(sec?: number) {
  if (!sec || sec <= 0) return "—";
  return `${Math.floor(sec / 60)} min`;
}

function clampInt(v: number, min: number, max: number) {
  const n = Math.floor(Number.isFinite(v) ? v : min);
  return Math.max(min, Math.min(max, n));
}

function fmtMmSs(totalSec: number) {
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function digitsOnly(s: string) {
  return s.replace(/[^\d]/g, "");
}

function parseOffsetSec(raw: string | undefined): number {
  if (!raw) return 0;
  const t = raw.trim();
  if (t === "") return 0;
  const n = Number(t);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

export default function Page() {
  const [allGames, setAllGames] = useState<GameMeta[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [offsets, setOffsets] = useState<Record<string, string>>({});

  const [streamingGames, setStreamingGames] = useState<GameMeta[]>([]);
  const [progress, setProgress] = useState<any>(null);

  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Chat state
  const [activeChatGameId, setActiveChatGameId] = useState<string | null>(null);
  const [chatInput, setChatInput] = useState("");
  const [chatSending, setChatSending] = useState(false);
  const [chatByGame, setChatByGame] = useState<Record<string, ChatMsg[]>>({});
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  // Load games
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        const list = await fetchAvailableGames();
        setAllGames(list);
        setSelected(Object.fromEntries(list.map((g) => [g.game_id, false])));
        setOffsets(Object.fromEntries(list.map((g) => [g.game_id, ""])));
      } catch (e: any) {
        setErr(e?.message ?? "Error loading games");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  // Poll progress
  useEffect(() => {
    const id = setInterval(async () => {
      try {
        const p = await fetchProgress();
        setProgress(p);

        const activeIds: string[] = p?.active_game_ids ?? [];
        if (activeIds.length > 0) {
          setStreamingGames((prev) => {
            const prevIds = new Set(prev.map((x) => x.game_id));
            const merged = [...prev];
            for (const gid of activeIds) {
              if (!prevIds.has(gid)) {
                const g = allGames.find((x) => x.game_id === gid);
                if (g) merged.push(g);
              }
            }
            return merged;
          });
        } else {
          setStreamingGames([]);
          setActiveChatGameId(null);
        }
      } catch {
        // ignore
      }
    }, 1000);
    return () => clearInterval(id);
  }, [allGames]);

  // Autoscroll chat
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [activeChatGameId, chatByGame]);

  const streamingIds = useMemo(() => new Set(streamingGames.map((g) => g.game_id)), [streamingGames]);
  const historyGames = useMemo(() => allGames.filter((g) => !streamingIds.has(g.game_id)), [allGames, streamingIds]);

  const selectedIds = useMemo(
    () => Object.entries(selected).filter(([, v]) => v).map(([k]) => k),
    [selected]
  );

  const canStart = selectedIds.length > 0 && !starting && !resetting;

  async function onStart() {
    setErr(null);
    if (!canStart) return;

    const payloadGames = selectedIds
      .filter((gid) => !streamingIds.has(gid))
      .map((gid) => ({
        game_id: gid,
        startAtSec: clampInt(parseOffsetSec(offsets[gid]), 0, 20000),
      }));

    try {
      setStarting(true);
      await startReplay({ games: payloadGames });

      // unselect
      setSelected((prev) => {
        const next = { ...prev };
        for (const pg of payloadGames) next[pg.game_id] = false;
        return next;
      });
    } catch (e: any) {
      setErr(e?.message ?? "Failed to start streaming");
    } finally {
      setStarting(false);
    }
  }

  async function onRestart() {
    setErr(null);
    try {
      setResetting(true);
      await resetReplay();

      setStreamingGames([]);
      setProgress(null);
      setActiveChatGameId(null);
      setChatByGame({});

      setSelected(Object.fromEntries(allGames.map((g) => [g.game_id, false])));
    } catch (e: any) {
      setErr(e?.message ?? "Failed to reset");
    } finally {
      setResetting(false);
    }
  }

  const activeChatMessages = useMemo(() => {
    if (!activeChatGameId) return [];
    return chatByGame[activeChatGameId] ?? [];
  }, [activeChatGameId, chatByGame]);

  async function onSendChat() {
    if (!activeChatGameId) return;
    const msg = chatInput.trim();
    if (!msg || chatSending) return;

    setChatInput("");
    setChatSending(true);

    const userMsg: ChatMsg = { role: "user", text: msg, ts: Date.now() };
    setChatByGame((prev) => ({
      ...prev,
      [activeChatGameId]: [...(prev[activeChatGameId] ?? []), userMsg],
    }));

    try {
      const res = await chatForGame({ game_id: activeChatGameId, message: msg });
      const botMsg: ChatMsg = { role: "assistant", text: res.answer, ts: Date.now() };
      setChatByGame((prev) => ({
        ...prev,
        [activeChatGameId]: [...(prev[activeChatGameId] ?? []), botMsg],
      }));
    } finally {
      setChatSending(false);
    }
  }

  return (
    <main style={{ minHeight: "100vh", background: "#f4f4f5", color: "#111827", padding: 20 }}>

      <div style={{ maxWidth: 1100, margin: "0 auto", display: "flex", flexDirection: "column", gap: 14 }}>
        {/* Header */}
        <div style={{ background: "white", border: "1px solid #e5e7eb", borderRadius: 16, padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
            <div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>Replay setup</div>
              <div style={{ fontSize: 13, color: "#52525b", marginTop: 4 }}>
                Offsets are in seconds. Preview (MM:SS) should appear under each input.
              </div>
            </div>

            <div style={{ display: "flex", gap: 8 }}>
              <button
                onClick={onStart}
                disabled={!canStart}
                style={{
                  background: "#111",
                  color: "white",
                  padding: "10px 14px",
                  borderRadius: 12,
                  border: "none",
                  opacity: canStart ? 1 : 0.5,
                  cursor: canStart ? "pointer" : "not-allowed",
                  fontWeight: 600,
                }}
              >
                {starting ? "Starting…" : "Stream selected"}
              </button>

              <button
                onClick={onRestart}
                disabled={resetting}
                style={{
                  background: "white",
                  padding: "10px 14px",
                  borderRadius: 12,
                  border: "1px solid #e5e7eb",
                  opacity: resetting ? 0.6 : 1,
                  cursor: resetting ? "not-allowed" : "pointer",
                  fontWeight: 600,
                }}
              >
                {resetting ? "Restarting…" : "Restart"}
              </button>
            </div>
          </div>
        </div>

        {err && (
          <div style={{ background: "#fee2e2", border: "1px solid #fecaca", borderRadius: 12, padding: 12, color: "#7f1d1d" }}>
            {err}
          </div>
        )}

        {/* History */}
        <section style={{ background: "white", border: "1px solid #e5e7eb", borderRadius: 16, padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
            <div>
              <div style={{ fontSize: 16, fontWeight: 700 }}>Match history</div>
              <div style={{ fontSize: 13, color: "#52525b" }}>Select games + set offset.</div>
            </div>
            <div style={{ fontSize: 13, color: "#52525b" }}>{selectedIds.length} selected</div>
          </div>

          <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 10 }}>
            {loading ? (
              <div style={{ fontSize: 13, color: "#52525b" }}>Loading…</div>
            ) : historyGames.length === 0 ? (
              <div style={{ fontSize: 13, color: "#52525b" }}>No games left in history.</div>
            ) : (
              historyGames.map((g) => {
                const checked = !!selected[g.game_id];
                const raw = offsets[g.game_id] ?? "";
                const offsetSec = parseOffsetSec(raw);

                return (
                  <div key={g.game_id} style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 12 }}>
                    <div style={{ display: "flex", gap: 10 }}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(e) => setSelected((p) => ({ ...p, [g.game_id]: e.target.checked }))}
                        style={{ marginTop: 4 }}
                      />

                      <div style={{ flex: 1 }}>
                        <div style={{ fontWeight: 700 }}>
                          {g.team_home} <span style={{ color: "#71717a" }}>vs</span> {g.team_away}
                        </div>
                        <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>
                          {(g.competition ?? "—") + " • Duration: " + formatDuration(g.duration_sec)}
                        </div>

                        <div style={{ marginTop: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                          <div>
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#3f3f46" }}>Start offset (seconds)</div>

                            {/* digits-only input for smooth typing */}
                            <input
                              type="text"
                              inputMode="numeric"
                              pattern="[0-9]*"
                              placeholder="e.g. 600"
                              value={raw}
                              onChange={(e) => setOffsets((p) => ({ ...p, [g.game_id]: digitsOnly(e.target.value) }))}
                              style={{
                                marginTop: 6,
                                width: "100%",
                                padding: "8px 10px",
                                borderRadius: 12,
                                border: "1px solid #e5e7eb",
                                fontSize: 14,
                              }}
                            />

                            {/* ✅ GUARANTEED visible preview + debug */}
                            <div
                              style={{
                                marginTop: 6,
                                fontSize: 12,
                                color: "#52525b",
                              }}
                            >
                              Preview: <span style={{ fontFamily: "monospace" }}>{fmtMmSs(offsetSec)}</span>{" "}
                              <span style={{ color: "#a1a1aa" }}>({offsetSec}s)</span>
                            </div>
                          </div>
                          <div style={{ background: "#fafafa", borderRadius: 12, padding: 10 }}>
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#3f3f46" }}>Game ID</div>
                            <div style={{ marginTop: 6, fontFamily: "monospace", fontSize: 12, color: "#52525b" }}>
                              {g.game_id}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </section>

        {/* Streams + Chat */}
        <section style={{ background: "white", border: "1px solid #e5e7eb", borderRadius: 16, padding: 16 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            {/* streams */}
            <div>
              <div style={{ fontSize: 16, fontWeight: 700 }}>Live replays</div>
              <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>Click a game to open its chat.</div>

              <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 10 }}>
                {streamingGames.length === 0 ? (
                  <div style={{ fontSize: 13, color: "#52525b", padding: 10, background: "#fafafa", borderRadius: 12 }}>
                    No active streams.
                  </div>
                ) : (
                  streamingGames.map((g) => {
                    const p = progress?.progress?.[g.game_id];
                    const status = p?.status ?? "—";
                    const curSec =
                      (typeof p?.known_time_sec === "number" ? p.known_time_sec : null) ??
                      (typeof p?.current_time_sec === "number" ? p.current_time_sec : null);

                    const isActive = activeChatGameId === g.game_id;

                    return (
                      <button
                        key={g.game_id}
                        onClick={() => setActiveChatGameId(g.game_id)}
                        style={{
                          textAlign: "left",
                          padding: 12,
                          borderRadius: 14,
                          border: isActive ? "2px solid #111" : "1px solid #e5e7eb",
                          background: isActive ? "#fafafa" : "white",
                          cursor: "pointer",
                        }}
                      >
                        <div style={{ fontWeight: 700 }}>
                          {g.team_home} <span style={{ color: "#71717a" }}>vs</span> {g.team_away}
                        </div>
                        <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>
                          {status} • {curSec == null ? "—" : fmtMmSs(curSec)}
                        </div>
                        <div style={{ fontSize: 12, color: "#a1a1aa", marginTop: 6, fontFamily: "monospace" }}>
                          {g.game_id}
                        </div>
                      </button>
                    );
                  })
                )}
              </div>
            </div>

            {/* chat */}
            <div style={{ border: "1px solid #e5e7eb", borderRadius: 14, overflow: "hidden", display: "flex", flexDirection: "column", minHeight: 360 }}>
              <div style={{ borderBottom: "1px solid #e5e7eb", padding: 10, fontWeight: 700, fontSize: 13 }}>
                {activeChatGameId ? `Chat • ${activeChatGameId}` : "Select a streaming game"}
              </div>

              <div style={{ flex: 1, overflow: "auto", padding: 10, background: "#fafafa", display: "flex", flexDirection: "column", gap: 8 }}>
                {!activeChatGameId ? (
                  <div style={{ fontSize: 13, color: "#52525b" }}>No game selected.</div>
                ) : activeChatMessages.length === 0 ? (
                  <div style={{ fontSize: 13, color: "#52525b" }}>Ask a question.</div>
                ) : (
                  activeChatMessages.map((m, idx) => (
                    <div
                      key={idx}
                      style={{
                        alignSelf: m.role === "user" ? "flex-end" : "flex-start",
                        background: m.role === "user" ? "#111" : "white",
                        color: m.role === "user" ? "white" : "#111",
                        border: m.role === "user" ? "none" : "1px solid #e5e7eb",
                        padding: "8px 10px",
                        borderRadius: 14,
                        maxWidth: "92%",
                        fontSize: 13,
                      }}
                    >
                      {m.text}
                    </div>
                  ))
                )}
                <div ref={chatEndRef} />
              </div>

              <div style={{ borderTop: "1px solid #e5e7eb", padding: 10, display: "flex", gap: 8 }}>
                <input
                  disabled={!activeChatGameId || chatSending}
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") onSendChat();
                  }}
                  placeholder={activeChatGameId ? "Type…" : "Select a game first"}
                  style={{
                    flex: 1,
                    border: "1px solid #e5e7eb",
                    borderRadius: 12,
                    padding: "8px 10px",
                    fontSize: 13,
                  }}
                />
                <button
                  onClick={onSendChat}
                  disabled={!activeChatGameId || chatSending || chatInput.trim().length === 0}
                  style={{
                    background: "#111",
                    color: "white",
                    padding: "8px 12px",
                    borderRadius: 12,
                    border: "none",
                    opacity: !activeChatGameId || chatSending || chatInput.trim().length === 0 ? 0.5 : 1,
                    cursor: !activeChatGameId || chatSending || chatInput.trim().length === 0 ? "not-allowed" : "pointer",
                    fontWeight: 700,
                    fontSize: 13,
                  }}
                >
                  Send
                </button>
              </div>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
