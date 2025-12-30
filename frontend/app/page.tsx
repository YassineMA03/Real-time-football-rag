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
  mode?: "fast" | "deep_think";  // Track which mode was used
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

async function chatForGame(params: { game_id: string; message: string; deep_think: boolean }) {
  const res = await fetch("http://localhost:8000/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ 
      game_id: params.game_id, 
      message: params.message,
      deep_think: params.deep_think 
    }),
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

function parseInt2(raw: string | undefined): number {
  if (!raw) return 0;
  const t = raw.trim();
  if (t === "") return 0;
  const n = Number(t);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

// Calculate max extra time for a half based on duration
function calculateMaxExtra(durationSec: number, half: 1 | 2): number {
  const totalMinutes = Math.floor(durationSec / 60);
  
  if (half === 1) {
    // 1st half: max extra = anything up to 45 minutes
    return Math.max(0, Math.min(totalMinutes, 50) - 45); // Cap at ~5 min extra
  } else {
    // 2nd half: max extra = (total - 45) minutes
    return Math.max(0, totalMinutes - 90); // All time after 90 is 2nd half extra
  }
}

export default function Page() {
  const [allGames, setAllGames] = useState<GameMeta[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  
  // NEW: Separate minute and extra inputs
  const [minutes, setMinutes] = useState<Record<string, string>>({});
  const [extras, setExtras] = useState<Record<string, string>>({});
  const [selectedHalf, setSelectedHalf] = useState<Record<string, 1 | 2>>({});

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
  const [deepThinkMode, setDeepThinkMode] = useState(false);  // NEW: Deep think toggle
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  // Load games
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        const list = await fetchAvailableGames();
        setAllGames(list);
        setSelected(Object.fromEntries(list.map((g) => [g.game_id, false])));
        setMinutes(Object.fromEntries(list.map((g) => [g.game_id, ""])));
        setExtras(Object.fromEntries(list.map((g) => [g.game_id, ""])));
        setSelectedHalf(Object.fromEntries(list.map((g) => [g.game_id, 1])));
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

  const canStart = useMemo(() => {
    if (selectedIds.length === 0 || starting || resetting) return false;
    
    // Check if all selected games have valid time inputs
    for (const gid of selectedIds) {
      const minute = parseInt2(minutes[gid]);
      const extra = parseInt2(extras[gid]);
      const half = selectedHalf[gid] || 1;
      
      // Check minute constraints
      const minuteMin = half === 1 ? 0 : 45;
      const minuteMax = half === 1 ? 45 : 90;
      
      if (minute < minuteMin || minute > minuteMax) {
        return false; // Invalid minute
      }
      
      // Check extra constraints
      const extraAllowed = (half === 1 && minute === 45) || (half === 2 && minute === 90);
      if (!extraAllowed && extra > 0) {
        return false; // Extra not allowed at this minute
      }
    }
    
    return true;
  }, [selectedIds, starting, resetting, minutes, extras, selectedHalf]);

  async function onStart() {
    setErr(null);
    if (!canStart) return;

    const payloadGames = selectedIds
      .filter((gid) => !streamingIds.has(gid))
      .map((gid) => {
        const minute = parseInt2(minutes[gid]);
        const extra = parseInt2(extras[gid]);
        const half = selectedHalf[gid] || 1;
        
        // Send minute and extra directly to backend
        // Backend will handle the comparison with data
        return {
          game_id: gid,
          startAtMinute: minute,
          startAtExtra: extra,
        };
      });

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
    if (!activeChatGameId || chatInput.trim().length === 0 || chatSending) return;
    const q = chatInput.trim();
    setChatInput("");
    setChatSending(true);

    setChatByGame((prev) => ({
      ...prev,
      [activeChatGameId]: [...(prev[activeChatGameId] ?? []), { role: "user", text: q, ts: Date.now() }],
    }));

    try {
      const resp = await chatForGame({ 
        game_id: activeChatGameId, 
        message: q,
        deep_think: deepThinkMode  // Pass the deep think mode
      });
      
      // Store the mode that was used
      const usedMode = deepThinkMode ? "deep_think" : "fast";
      
      setChatByGame((prev) => ({
        ...prev,
        [activeChatGameId]: [
          ...(prev[activeChatGameId] ?? []),
          { 
            role: "assistant", 
            text: resp.answer ?? "No response", 
            ts: Date.now(),
            mode: usedMode  // Store which mode was used
          },
        ],
      }));
    } catch (e: any) {
      setChatByGame((prev) => ({
        ...prev,
        [activeChatGameId]: [
          ...(prev[activeChatGameId] ?? []),
          { role: "assistant", text: `Error: ${e?.message ?? "unknown"}`, ts: Date.now() },
        ],
      }));
    } finally {
      setChatSending(false);
    }
  }

  return (
    <main style={{ minHeight: "100vh", background: "#fafafa", padding: "20px 16px" }}>
      <div style={{ maxWidth: 1200, margin: "0 auto", display: "flex", flexDirection: "column", gap: 16 }}>
        {/* Header */}
        <div style={{ background: "white", border: "1px solid #e5e7eb", borderRadius: 16, padding: 16 }}>
          <h1 style={{ margin: 0, fontSize: 24, fontWeight: 700 , color: "#000000ff"}}>⚽ Football Match Replay + RAG Chat</h1>
          <p style={{ margin: "4px 0 0", fontSize: 14, color: "#52525b" }}>
            Select games, choose half, minute, and extra time.
          </p>
        </div>

        {err && (
          <div style={{ background: "#fef2f2", border: "1px solid #fca5a5", borderRadius: 12, padding: 12 }}>
            <div style={{ fontSize: 13, color: "#991b1b" }}>{err}</div>
          </div>
        )}

        {/* History + Start/Reset */}
        <section style={{ background: "white", border: "1px solid #e5e7eb", borderRadius: 16, padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <div style={{ fontSize: 16, fontWeight: 700, color: "#000000ff" }}>Available Games</div>
              <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>
                Select half, then set minute + extra time.
              </div>
            </div>

            <div style={{ display: "flex", gap: 8 }}>
              <button
                onClick={onStart}
                disabled={!canStart}
                style={{
                  background: canStart ? "#111" : "#d1d5db",
                  color: "white",
                  padding: "10px 20px",
                  borderRadius: 12,
                  border: "none",
                  cursor: canStart ? "pointer" : "not-allowed",
                  fontWeight: 700,
                  fontSize: 14,
                }}
              >
                {starting ? "Starting..." : "Start Replay"}
              </button>

              <button
                onClick={onRestart}
                disabled={resetting}
                style={{
                  background: "white",
                  color: "#111",
                  padding: "10px 20px",
                  borderRadius: 12,
                  border: "1px solid #e5e7eb",
                  cursor: resetting ? "not-allowed" : "pointer",
                  fontWeight: 700,
                  fontSize: 14,
                }}
              >
                {resetting ? "Resetting..." : "Reset All"}
              </button>
            </div>
          </div>

          <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 10 }}>
            {loading ? (
              <div style={{ fontSize: 13, color: "#52525b" }}>Loading…</div>
            ) : historyGames.length === 0 ? (
              <div style={{ fontSize: 13, color: "#52525b" }}>No games left in history.</div>
            ) : (
              historyGames.map((g) => {
                const checked = !!selected[g.game_id];
                const minute = parseInt2(minutes[g.game_id]);
                const extra = parseInt2(extras[g.game_id]);
                const half = selectedHalf[g.game_id] || 1;
                
                // Strict minute constraints based on half
                // 1st half: 0-45 only
                // 2nd half: 45-90 only
                const minuteMin = half === 1 ? 0 : 45;
                const minuteMax = half === 1 ? 45 : 90;
                
                // Only allow extra time at exactly 45 (1st half) or 90 (2nd half)
                const extraAllowed = (half === 1 && minute === 45) || (half === 2 && minute === 90);
                const maxExtra = extraAllowed ? calculateMaxExtra(g.duration_sec || 5400, half) : 0;
                
                // Validation: Enforce strict minute ranges
                const minuteInvalid = minute < minuteMin || minute > minuteMax;
                const extraInvalid = extra > maxExtra || (!extraAllowed && extra > 0);
                
                // Display the actual match time (minute + extra)
                const displayMinute = minute + extra;

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
                        <div style={{ fontWeight: 700 , color: "#000000ff"}}>
                          {g.team_home} <span style={{ color: "#71717a" }}>vs</span> {g.team_away}
                        </div>
                        <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>
                          {(g.competition ?? "—") + " • Duration: " + formatDuration(g.duration_sec)}
                        </div>

                        <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 10 }}>
                          {/* Half selector */}
                          <div>
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#3f3f46", marginBottom: 6 }}>
                              1. Select Half
                            </div>
                            <div style={{ display: "flex", gap: 8 }}>
                              <button
                                onClick={() => {
                                  setSelectedHalf((p) => ({ ...p, [g.game_id]: 1 }));
                                  // Clear minute if it's outside 1st half range (0-45)
                                  const currentMinute = parseInt2(minutes[g.game_id]);
                                  if (currentMinute > 45) {
                                    setMinutes((p) => ({ ...p, [g.game_id]: "" }));
                                  }
                                  // Clear extra if not at 45
                                  if (currentMinute !== 45) {
                                    setExtras((p) => ({ ...p, [g.game_id]: "" }));
                                  }
                                }}
                                style={{
                                  flex: 1,
                                  padding: "8px 12px",
                                  borderRadius: 10,
                                  border: half === 1 ? "2px solid #10b981" : "1px solid #e5e7eb",
                                  background: half === 1 ? "#ecfdf5" : "white",
                                  color: half === 1 ? "#7b4949ff" : "#4e4e5dff",
                                  fontWeight: half === 1 ? 700 : 500,
                                  fontSize: 13,
                                  cursor: "pointer",
                                }}
                              >
                                ⚽ 1st Half (0-{fmtMmSs(Math.min(2700, g.duration_sec || 5400))})
                              </button>
                              <button
                                onClick={() => {
                                  setSelectedHalf((p) => ({ ...p, [g.game_id]: 2 }));
                                  // Clear minute if it's outside 2nd half range (45-90)
                                  const currentMinute = parseInt2(minutes[g.game_id]);
                                  if (currentMinute < 45) {
                                    setMinutes((p) => ({ ...p, [g.game_id]: "" }));
                                  }
                                  // Clear extra if not at 90
                                  if (currentMinute !== 90) {
                                    setExtras((p) => ({ ...p, [g.game_id]: "" }));
                                  }
                                }}
                                style={{
                                  flex: 1,
                                  padding: "8px 12px",
                                  borderRadius: 10,
                                  border: half === 2 ? "2px solid #ef4444" : "1px solid #e5e7eb",
                                  background: half === 2 ? "#fef2f2" : "white",
                                  color: half === 2 ? "#dc2626" : "#52525b",
                                  fontWeight: half === 2 ? 700 : 500,
                                  fontSize: 13,
                                  cursor: "pointer",
                                }}
                              >
                                🔥 2nd Half ({fmtMmSs(2700)}-{fmtMmSs(g.duration_sec || 5400)})
                              </button>
                            </div>
                          </div>

                          {/* Minute and Extra inputs */}
                          <div>
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#3f3f46", marginBottom: 6 }}>
                              2. Set Time (Minute + Extra)
                            </div>
                            
                            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                              {/* Minute input */}
                              <div>
                                <div style={{ fontSize: 11, color: "#000", marginBottom: 4 }}>
                                  Minute ({half === 1 ? "0-45" : "45-90"})
                                </div>
                                <input
                                  type="text"
                                  inputMode="numeric"
                                  pattern="[0-9]*"
                                  placeholder={half === 1 ? "e.g. 10" : "e.g. 60"}
                                  value={minutes[g.game_id] || ""}
                                  onChange={(e) => setMinutes((p) => ({ ...p, [g.game_id]: digitsOnly(e.target.value) }))}
                                  style={{
                                    width: "100%",
                                    padding: "8px 10px",
                                    borderRadius: 10,
                                    border: minuteInvalid ? "2px solid #ef4444" : "1px solid #e5e7eb",
                                    background: minuteInvalid ? "#fef2f2" : "white",
                                    fontSize: 14,
                                    color: "#000",  // ← Add this line
                                  }}
                                />
                                {minuteInvalid && (
                                  <div style={{ fontSize: 11, color: "#dc2626", marginTop: 2 }}>
                                    {half === 1 ? "Must be 0-45" : "Must be 45-90"}
                                  </div>
                                )}
                              </div>

                              {/* Extra input */}
                              <div>
                                <div style={{ fontSize: 11, color: "#000", marginBottom: 4 }}>
                                  Extra (0-{maxExtra}) {!extraAllowed && "- Only at 45' or 90'"}
                                </div>
                                <input
                                  type="text"
                                  inputMode="numeric"
                                  pattern="[0-9]*"
                                  placeholder={extraAllowed ? "e.g. 2" : "N/A"}
                                  value={extras[g.game_id] || ""}
                                  onChange={(e) => setExtras((p) => ({ ...p, [g.game_id]: digitsOnly(e.target.value) }))}
                                  disabled={!extraAllowed}
                                  style={{
                                    width: "100%",
                                    padding: "8px 10px",
                                    borderRadius: 10,
                                    border: extraInvalid ? "2px solid #ef4444" : "1px solid #e5e7eb",
                                    background: extraInvalid ? "#fef2f2" : (!extraAllowed ? "#f3f4f6" : "white"),
                                    fontSize: 14,
                                    opacity: extraAllowed ? 1 : 0.5,
                                    cursor: extraAllowed ? "text" : "not-allowed",
                                    color: "#000",
                                  }}
                                />
                                {extraInvalid && extraAllowed && (
                                  <div style={{ fontSize: 11, color: "#dc2626", marginTop: 2 }}>
                                    Max: {maxExtra} min
                                  </div>
                                )}
                                {!extraAllowed && extra > 0 && (
                                  <div style={{ fontSize: 11, color: "#dc2626", marginTop: 2 }}>
                                    Extra only at 45' or 90'
                                  </div>
                                )}
                              </div>
                            </div>

                            {/* Preview */}
                            <div
                              style={{
                                marginTop: 8,
                                padding: "8px 10px",
                                background: minuteInvalid || extraInvalid ? "#fef2f2" : "#f0fdf4",
                                border: minuteInvalid || extraInvalid ? "1px solid #fca5a5" : "1px solid #bbf7d0",
                                borderRadius: 10,
                                fontSize: 13,
                              }}
                            >
                              <div style={{ fontWeight: 600, color: "#000" }}>
                                {half === 1 ? "⚽" : "🔥"} Preview:
                              </div>
                              <div style={{ marginTop: 4, color: "#000" }}>
                                Match time: <span style={{ fontFamily: "monospace", fontWeight: 600 }}>{displayMinute}'</span>
                                {extra > 0 && <span style={{ color: "#059669" }}> +{extra}'</span>}
                              </div>
                              {(minuteInvalid || extraInvalid) && (
                                <div style={{ marginTop: 4, color: "#dc2626", fontSize: 12 }}>
                                  ⚠️ Time exceeds match duration
                                </div>
                              )}
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
              <div style={{ fontSize: 16, fontWeight: 700, color: "#000000ff" }}>Live replays</div>
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
                    
                    // Get minute and extra from progress
                    const minute = p?.known_minute ?? 0;
                    const extra = p?.known_extra ?? 0;
                    
                    // Determine half correctly:
                    // - 1st Half: 0-45 (including 45+X)
                    // - 2nd Half: 46-90 (including 90+X)
                    const half = minute <= 45 ? "1st Half" : "2nd Half";
                    
                    // Format time display
                    let timeDisplay = "—";
                    if (minute !== undefined) {
                      if (extra > 0) {
                        timeDisplay = `${minute}' +${extra}' (${half})`;
                      } else {
                        timeDisplay = `${minute}' (${half})`;
                      }
                    }

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
                        <div style={{ fontWeight: 700 , color: "#000000ff"}}>
                          {g.team_home} <span style={{ color: "#71717a" }}>vs</span> {g.team_away}
                        </div>
                        <div style={{ fontSize: 13, color: "#52525b", marginTop: 2 }}>
                          {status} • {timeDisplay}
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
              {/* Header with Deep Think toggle */}
              <div style={{ borderBottom: "1px solid #e5e7eb", padding: 10, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div style={{ fontWeight: 700, fontSize: 13, color: "#000" }}>
                  {activeChatGameId ? `Chat • ${activeChatGameId}` : "Select a streaming game"}
                </div>
                
                {activeChatGameId && (
                  <button
                    onClick={() => setDeepThinkMode(!deepThinkMode)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      padding: "6px 12px",
                      borderRadius: 8,
                      border: deepThinkMode ? "2px solid #8b5cf6" : "1px solid #e5e7eb",
                      background: deepThinkMode ? "#f5f3ff" : "white",
                      color: deepThinkMode ? "#6d28d9" : "#52525b",
                      fontSize: 12,
                      fontWeight: 600,
                      cursor: "pointer",
                      transition: "all 0.2s",
                    }}
                  >
                    <span style={{ fontSize: 14 }}>{deepThinkMode ? "🧠" : "⚡"}</span>
                    <span>{deepThinkMode ? "Deep Think" : "Fast Mode"}</span>
                  </button>
                )}
              </div>

              <div 
                style={{ 
                  flex: 1, 
                  overflow: "auto",  // Make scrollable
                  padding: 10, 
                  background: "#fafafa", 
                  display: "flex", 
                  flexDirection: "column", 
                  gap: 8,
                  maxHeight: "500px"  // Set max height for scrolling
                }}
              >
                {/* Mode explanation */}
                {activeChatGameId && (
                  <div style={{
                    padding: "8px 10px",
                    background: deepThinkMode ? "#faf5ff" : "#f0fdf4",
                    border: deepThinkMode ? "1px solid #e9d5ff" : "1px solid #bbf7d0",
                    borderRadius: 10,
                    fontSize: 11,
                    color: "#000",
                  }}>
                    {deepThinkMode ? (
                      <>
                        <strong>🧠 Deep Think Mode:</strong> Searches ALL match history with semantic similarity. Best for analysis and "big picture" questions.
                      </>
                    ) : (
                      <>
                        <strong>⚡ Fast Mode:</strong> Uses recent context only. Best for "What just happened?" questions.
                      </>
                    )}
                  </div>
                )}
                
                {!activeChatGameId ? (
                  <div style={{ fontSize: 13, color: "#000" }}>No game selected.</div>
                ) : activeChatMessages.length === 0 ? (
                  <div style={{ fontSize: 13, color: "#000" }}>Ask a question.</div>
                ) : (
                  activeChatMessages.map((m, idx) => (
                    <div
                      key={idx}
                      style={{
                        alignSelf: m.role === "user" ? "flex-end" : "flex-start",
                        background: m.role === "user" ? "#111" : "white",
                        color: m.role === "user" ? "white" : "#000",
                        border: m.role === "user" ? "none" : "1px solid #e5e7eb",
                        padding: "8px 10px",
                        borderRadius: 14,
                        maxWidth: "92%",
                        fontSize: 13,
                        display: "flex",
                        flexDirection: "column",
                        gap: 6,
                      }}
                    >
                      {/* Message text */}
                      <div>{m.text}</div>
                      
                      {/* Mode badge for assistant messages */}
                      {m.role === "assistant" && m.mode && (
                        <div style={{
                          display: "inline-flex",
                          alignItems: "center",
                          alignSelf: "flex-start",
                          padding: "2px 8px",
                          borderRadius: 6,
                          fontSize: 10,
                          fontWeight: 600,
                          background: m.mode === "deep_think" ? "#f5f3ff" : "#f0fdf4",
                          color: m.mode === "deep_think" ? "#7c3aed" : "#16a34a",
                          border: m.mode === "deep_think" ? "1px solid #e9d5ff" : "1px solid #bbf7d0",
                        }}>
                          {m.mode === "deep_think" ? "🧠 Deep Think" : "⚡ Fast"}
                        </div>
                      )}
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
                  placeholder={activeChatGameId ? "Ask a question..." : "Select a game first"}
                  style={{
                    flex: 1,
                    border: "1px solid #e5e7eb",
                    borderRadius: 12,
                    padding: "8px 10px",
                    fontSize: 13,
                    color: "#000",
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