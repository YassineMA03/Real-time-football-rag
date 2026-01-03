"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

// 🔧 Use environment variable for API URL
const API_BASE_URL = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

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
  const res = await fetch(`${API_BASE_URL}/api/games`);
  if (!res.ok) throw new Error("Failed to load games");
  return await res.json();
}

async function startReplay(payload: { games: { game_id: string; startAtMinute: number; startAtExtra: number }[] }) {
  const res = await fetch(`${API_BASE_URL}/api/replay/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

async function resetReplay(): Promise<void> {
  const res = await fetch(`${API_BASE_URL}/api/replay/reset`, { method: "POST" });
  if (!res.ok) throw new Error("Failed to reset simulation");
}

async function fetchProgress(): Promise<any> {
  const res = await fetch(`${API_BASE_URL}/api/replay/progress`);
  if (!res.ok) throw new Error("Failed to load progress");
  return await res.json();
}

async function chatForGame(params: { game_id: string; message: string }): Promise<{ answer: string; contexts?: any }> {
  const res = await fetch(`${API_BASE_URL}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ game_id: params.game_id, message: params.message }),
  });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

function fmtMmSs(totalSec: number) {
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function toIntSafe(v: string): number {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

export default function Page() {
  const [allGames, setAllGames] = useState<GameMeta[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [startMin, setStartMin] = useState<Record<string, string>>({});
  const [startExtra, setStartExtra] = useState<Record<string, string>>({});

  const [streamingGames, setStreamingGames] = useState<GameMeta[]>([]);
  const [progress, setProgress] = useState<any>(null);

  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Chat
  const [activeChatGameId, setActiveChatGameId] = useState<string | null>(null);
  const [chatInput, setChatInput] = useState("");
  const [chatSending, setChatSending] = useState(false);
  const [chatByGame, setChatByGame] = useState<Record<string, ChatMsg[]>>({});
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        const list = await fetchAvailableGames();
        setAllGames(list);
        setSelected(Object.fromEntries(list.map((g) => [g.game_id, false])));
        setStartMin(Object.fromEntries(list.map((g) => [g.game_id, ""])));
        setStartExtra(Object.fromEntries(list.map((g) => [g.game_id, ""])));
      } catch (e: any) {
        setErr(e?.message ?? "Error loading games");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  useEffect(() => {
    const id = setInterval(async () => {
      try {
        const p = await fetchProgress();
        setProgress(p);

        const activeIds: string[] = p?.active_game_ids ?? [];
        setStreamingGames(allGames.filter((g) => activeIds.includes(g.game_id)));
        if (activeIds.length === 0) setActiveChatGameId(null);
      } catch {
        // ignore
      }
    }, 1000);
    return () => clearInterval(id);
  }, [allGames]);

  const selectedIds = useMemo(
    () => Object.entries(selected).filter(([, v]) => v).map(([k]) => k),
    [selected]
  );

  const availableGames = useMemo(
    () => allGames.filter((g) => !streamingGames.some((sg) => sg.game_id === g.game_id)),
    [allGames, streamingGames]
  );

  const allSelected = useMemo(
    () => availableGames.length > 0 && availableGames.every((g) => selected[g.game_id]),
    [availableGames, selected]
  );

  const canStart = selectedIds.length > 0 && !starting && !resetting;

  function onToggleSelectAll() {
    setSelected((prev) => {
      const next = { ...prev };
      const newValue = !allSelected;
      for (const g of availableGames) {
        next[g.game_id] = newValue;
      }
      return next;
    });
  }

  async function onStart() {
    setErr(null);
    if (!canStart) return;

    const payloadGames = selectedIds.map((gid) => ({
      game_id: gid,
      startAtMinute: toIntSafe(startMin[gid] ?? ""),
      startAtExtra: toIntSafe(startExtra[gid] ?? ""),
    }));

    try {
      setStarting(true);
      await startReplay({ games: payloadGames });
      setSelected((prev) => {
        const next = { ...prev };
        for (const gid of selectedIds) next[gid] = false;
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

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [activeChatMessages.length]);

  async function onSendChat() {
    if (!activeChatGameId) return;
    const q = chatInput.trim();
    if (!q || chatSending) return;

    setChatInput("");
    setChatSending(true);

    setChatByGame((prev) => ({
      ...prev,
      [activeChatGameId]: [...(prev[activeChatGameId] ?? []), { role: "user", text: q, ts: Date.now() }],
    }));

    try {
      const resp = await chatForGame({ game_id: activeChatGameId, message: q });
      setChatByGame((prev) => ({
        ...prev,
        [activeChatGameId]: [
          ...(prev[activeChatGameId] ?? []),
          { role: "assistant", text: resp.answer ?? "(no answer)", ts: Date.now() },
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
    <main className="min-h-screen bg-zinc-50 text-zinc-900">
      <div className="mx-auto max-w-6xl px-4 py-8 space-y-6">
        <div className="rounded-2xl border bg-white p-6 shadow-sm">
          <h1 className="text-2xl font-semibold tracking-tight">⚽ Replay + Kafka RAG (Simple)</h1>
          <p className="mt-1 text-sm text-zinc-600">Select games, choose start minute + extra, then stream.</p>
          {/* 🔧 Debug info */}
          <p className="mt-1 text-xs text-zinc-400">API: {API_BASE_URL}</p>

          <div className="mt-4 flex gap-2">
            <button
              onClick={onStart}
              disabled={!canStart}
              className="rounded-xl bg-black px-4 py-3 text-sm font-medium text-white disabled:opacity-50"
            >
              {starting ? "Starting…" : "Stream selected games"}
            </button>

            <button
              onClick={onRestart}
              disabled={resetting}
              className="rounded-xl border border-zinc-200 bg-white px-4 py-3 text-sm font-medium hover:bg-zinc-50 disabled:opacity-50"
            >
              {resetting ? "Restarting…" : "Restart simulation"}
            </button>
          </div>
        </div>

        {err && <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-800">{err}</div>}

        <div className="grid gap-6 lg:grid-cols-2">
          <section className="rounded-2xl border bg-white p-6 shadow-sm">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Match history</h2>
                <p className="text-sm text-zinc-600">Pick games + offsets.</p>
              </div>
              {availableGames.length > 0 && (
                <button
                  onClick={onToggleSelectAll}
                  className="rounded-lg border border-zinc-200 bg-white px-3 py-1.5 text-xs font-medium hover:bg-zinc-50"
                >
                  {allSelected ? "Deselect all" : "Select all"}
                </button>
              )}
            </div>

            <div className="mt-4 max-h-[600px] overflow-y-auto space-y-3 pr-2">
              {loading ? (
                <div className="text-sm text-zinc-600">Loading…</div>
              ) : availableGames.length === 0 ? (
                <div className="text-sm text-zinc-500">All games are currently streaming.</div>
              ) : (
                availableGames.map((g) => (
                  <div key={g.game_id} className="rounded-xl border border-zinc-200 p-4 hover:bg-zinc-50">
                    <div className="flex items-start gap-3">
                      <input
                        type="checkbox"
                        checked={!!selected[g.game_id]}
                        onChange={(e) => setSelected((p) => ({ ...p, [g.game_id]: e.target.checked }))}
                        className="mt-1 h-4 w-4"
                      />

                      <div className="flex-1">
                        <div className="font-medium">
                          {g.team_home} <span className="text-zinc-500">vs</span> {g.team_away}
                        </div>

                        <div className="mt-3 grid gap-2 md:grid-cols-2">
                          <div>
                            <label className="block text-xs font-medium text-zinc-700">Start minute</label>
                            <input
                              type="number"
                              placeholder="0"
                              value={startMin[g.game_id] ?? ""}
                              onChange={(e) => setStartMin((p) => ({ ...p, [g.game_id]: e.target.value }))}
                              className="mt-1 w-full rounded-xl border border-zinc-200 bg-white p-2 text-sm"
                            />
                          </div>

                          <div>
                            <label className="block text-xs font-medium text-zinc-700">Extra</label>
                            <input
                              type="number"
                              placeholder="0"
                              value={startExtra[g.game_id] ?? ""}
                              onChange={(e) => setStartExtra((p) => ({ ...p, [g.game_id]: e.target.value }))}
                              className="mt-1 w-full rounded-xl border border-zinc-200 bg-white p-2 text-sm"
                            />
                          </div>
                        </div>

                        <div className="mt-2 text-xs font-mono text-zinc-500">{g.game_id}</div>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="rounded-2xl border bg-white p-6 shadow-sm">
            <h2 className="text-lg font-semibold">Currently streaming</h2>
            <p className="text-sm text-zinc-600">Click a match to open the chatbot.</p>

            <div className="mt-4 max-h-[600px] overflow-y-auto space-y-3 pr-2">
              {streamingGames.length === 0 ? (
                <div className="text-sm text-zinc-500">No active streams.</div>
              ) : (
                streamingGames.map((g) => {
                  const p = progress?.progress?.[g.game_id];
                  const status = p?.status ?? "—";
                  const score = p?.score_str ?? "—";
                  const minute = typeof p?.known_minute === "number" ? p.known_minute : null;
                  const extra = typeof p?.known_extra === "number" ? p.known_extra : 0;
                  const curSec = typeof p?.known_time_sec === "number" ? p.known_time_sec : null;

                  return (
                    <button
                      key={g.game_id}
                      onClick={() => setActiveChatGameId(g.game_id)}
                      className={`w-full text-left rounded-xl border p-4 hover:bg-zinc-50 ${
                        activeChatGameId === g.game_id ? "border-black" : "border-zinc-200"
                      }`}
                    >
                      <div className="font-medium">
                        {g.team_home} <span className="text-zinc-500">vs</span> {g.team_away}
                      </div>
                      <div className="mt-1 text-sm text-zinc-600">
                        Status: {status} • Score: <span className="font-semibold">{score}</span>
                      </div>
                      <div className="mt-1 text-sm text-zinc-600">
                        Time:{" "}
                        {minute === null
                          ? curSec === null
                            ? "—"
                            : fmtMmSs(curSec)
                          : extra > 0
                          ? `${minute}'+${extra}'`
                          : `${minute}'`}
                      </div>
                      <div className="mt-2 text-xs font-mono text-zinc-500">{g.game_id}</div>
                    </button>
                  );
                })
              )}
            </div>
          </section>
        </div>

        <section className="rounded-2xl border bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold">Chatbot</h2>
          <p className="text-sm text-zinc-600">
            {activeChatGameId ? `Selected: ${activeChatGameId}` : "Select a streaming match to chat."}
          </p>

          <div className="mt-4 h-96 overflow-y-auto rounded-xl border border-zinc-200 bg-zinc-50 p-3">
            {activeChatMessages.length === 0 ? (
              <div className="text-sm text-zinc-500">No messages.</div>
            ) : (
              activeChatMessages.map((m, idx) => (
                <div key={idx} className="mb-2">
                  <div className="text-xs text-zinc-500">{m.role}</div>
                  <div className="rounded-lg bg-white border border-zinc-200 p-2 text-sm">{m.text}</div>
                </div>
              ))
            )}
            <div ref={chatEndRef} />
          </div>

          <div className="mt-3 flex gap-2">
            <input
              value={chatInput}
              onChange={(e) => setChatInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") onSendChat();
              }}
              disabled={!activeChatGameId || chatSending}
              className="flex-1 rounded-xl border border-zinc-200 p-3 text-sm"
              placeholder={activeChatGameId ? "Ask something…" : "Select a match first"}
            />
            <button
              onClick={onSendChat}
              disabled={!activeChatGameId || chatSending || chatInput.trim().length === 0}
              className="rounded-xl bg-black px-4 py-3 text-sm font-medium text-white disabled:opacity-50"
            >
              {chatSending ? "Sending…" : "Send"}
            </button>
          </div>
        </section>
      </div>
    </main>
  );
}