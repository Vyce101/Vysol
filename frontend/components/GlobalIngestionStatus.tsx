"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ChevronDown, ChevronUp, Loader2, Upload } from "lucide-react";
import { apiFetch } from "@/lib/api";

interface WorldSummary {
    world_id: string;
    world_name: string;
    ingestion_status: string;
    active_ingestion_run?: boolean;
}

interface CheckpointInfo {
    chunk_index?: number;
    chunks_total?: number;
    active_ingestion_run?: boolean;
    stage_counters?: {
        expected_chunks?: number;
        extracted_chunks?: number;
        embedded_chunks?: number;
    };
    progress_phase?: "extracting" | "embedding" | "aborting" | "idle";
    completed_chunks_current_phase?: number;
    total_chunks_current_phase?: number;
    progress_percent?: number;
    active_operation?: string;
}

interface ActiveWorldProgress {
    world_id: string;
    world_name: string;
    ingestion_status: string;
    completed_chunks: number;
    total_chunks: number;
    percent: number;
    phase: "extracting" | "embedding" | "aborting" | "idle";
    operation: string;
}

const STORAGE_KEY = "global-ingestion-status-expanded";

function clampPercent(value: number): number {
    if (!Number.isFinite(value)) return 0;
    return Math.max(0, Math.min(100, value));
}

function resolveProgress(checkpoint: CheckpointInfo | null): {
    completed_chunks: number;
    total_chunks: number;
    percent: number;
} {
    const explicitTotal = Math.max(0, Number(checkpoint?.total_chunks_current_phase ?? 0));
    const explicitCompleted = Math.max(0, Number(checkpoint?.completed_chunks_current_phase ?? 0));
    const explicitPercent = Number(checkpoint?.progress_percent ?? 0);
    const checkpointTotal = Math.max(0, Number(checkpoint?.chunks_total ?? 0));
    const checkpointCompleted = Math.max(0, Number(checkpoint?.chunk_index ?? 0));

    const stageCounters = checkpoint?.stage_counters ?? {};
    const stageTotal = Math.max(0, Number(stageCounters.expected_chunks ?? 0));
    const stageCompleted = Math.max(
        0,
        Number(
            stageCounters.embedded_chunks
            ?? stageCounters.extracted_chunks
            ?? 0
        ),
    );

    const total_chunks = explicitTotal || checkpointTotal || stageTotal;
    const completed_chunks = total_chunks
        ? Math.min(total_chunks, Math.max(explicitCompleted || checkpointCompleted, stageCompleted))
        : 0;
    const percent = total_chunks > 0
        ? clampPercent(explicitTotal > 0 ? explicitPercent : (completed_chunks / total_chunks) * 100)
        : 0;

    return {
        completed_chunks,
        total_chunks,
        percent,
    };
}

export function GlobalIngestionStatus() {
    const [activeWorlds, setActiveWorlds] = useState<ActiveWorldProgress[]>([]);
    const [expanded, setExpanded] = useState(false);
    const [hydrated, setHydrated] = useState(false);

    useEffect(() => {
        setHydrated(true);
        try {
            const stored = window.localStorage.getItem(STORAGE_KEY);
            if (stored === "true") {
                setExpanded(true);
            }
        } catch {
            // Ignore localStorage issues.
        }
    }, []);

    useEffect(() => {
        if (!hydrated) return;

        let cancelled = false;
        let timeoutId: number | undefined;

        const poll = async () => {
            try {
                const worlds = await apiFetch<WorldSummary[]>("/worlds");
                const runningWorlds = worlds.filter(
                    (world) => world.active_ingestion_run || world.ingestion_status === "in_progress",
                );

                if (runningWorlds.length === 0) {
                    if (!cancelled) {
                        setActiveWorlds([]);
                    }
                } else {
                    const checkpoints = await Promise.all(
                        runningWorlds.map(async (world) => {
                            const checkpoint = await apiFetch<CheckpointInfo>(
                                `/worlds/${world.world_id}/ingest/checkpoint`,
                            ).catch(() => null);
                            const progress = resolveProgress(checkpoint);
                            return {
                                world_id: world.world_id,
                                world_name: world.world_name,
                                ingestion_status: world.ingestion_status,
                                phase: checkpoint?.progress_phase || "idle",
                                operation: checkpoint?.active_operation || "default",
                                ...progress,
                            };
                        }),
                    );

                    if (!cancelled) {
                        setActiveWorlds(checkpoints);
                    }
                }
            } catch {
                if (!cancelled) {
                    setActiveWorlds([]);
                }
            } finally {
                if (!cancelled) {
                    const nextDelay = activeWorlds.length > 0 ? 2500 : 6000;
                    timeoutId = window.setTimeout(poll, nextDelay);
                }
            }
        };

        void poll();

        return () => {
            cancelled = true;
            if (timeoutId) {
                window.clearTimeout(timeoutId);
            }
        };
    }, [hydrated, activeWorlds.length]);

    useEffect(() => {
        if (!hydrated) return;
        try {
            window.localStorage.setItem(STORAGE_KEY, String(expanded));
        } catch {
            // Ignore localStorage issues.
        }
    }, [expanded, hydrated]);

    const aggregate = useMemo(() => {
        const totalChunks = activeWorlds.reduce((sum, world) => sum + world.total_chunks, 0);
        const completedChunks = activeWorlds.reduce((sum, world) => sum + world.completed_chunks, 0);
        const percent = totalChunks > 0 ? clampPercent((completedChunks / totalChunks) * 100) : 0;
        return {
            totalChunks,
            completedChunks,
            percent,
        };
    }, [activeWorlds]);

    if (!activeWorlds.length) {
        return null;
    }

    return (
        <div
            style={{
                position: "fixed",
                right: 20,
                bottom: 20,
                zIndex: 2000,
                width: expanded ? 340 : "auto",
                maxWidth: "calc(100vw - 32px)",
            }}
        >
            {expanded ? (
                <div
                    style={{
                        background: "var(--floating-surface)",
                        border: "1px solid var(--border-strong)",
                        borderRadius: 18,
                        boxShadow: "0 18px 40px var(--shadow-color)",
                        backdropFilter: "blur(14px)",
                        overflow: "hidden",
                    }}
                >
                    <button
                        onClick={() => setExpanded(false)}
                        style={{
                            width: "100%",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "space-between",
                            gap: 12,
                            padding: "14px 16px 12px",
                            background: "linear-gradient(180deg, var(--primary-soft-strong), var(--primary-soft))",
                            border: "none",
                            color: "var(--text-primary)",
                            cursor: "pointer",
                            textAlign: "left",
                        }}
                    >
                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                            <div
                                style={{
                                    width: 28,
                                    height: 28,
                                    borderRadius: 999,
                                    display: "grid",
                                    placeItems: "center",
                                    background: "var(--primary-soft-strong)",
                                    color: "var(--primary-light)",
                                }}
                            >
                                <Upload size={15} />
                            </div>
                            <div>
                                <div style={{ fontSize: 13, fontWeight: 600 }}>
                                    {activeWorlds.length === 1 ? "World ingest in progress" : `${activeWorlds.length} worlds ingesting`}
                                </div>
                                <div style={{ fontSize: 12, color: "var(--text-subtle)" }}>
                                    {activeWorlds.every((world) => world.phase === "aborting")
                                        ? "Stopping in-flight work..."
                                        : `${aggregate.completedChunks}/${aggregate.totalChunks || "?"} chunks processed`}
                                </div>
                            </div>
                        </div>
                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                            <span style={{ fontSize: 12, color: "var(--primary-light)", fontWeight: 600 }}>
                                {Math.round(aggregate.percent)}%
                            </span>
                            <ChevronDown size={16} />
                        </div>
                    </button>

                    <div style={{ padding: "10px 12px 12px", display: "grid", gap: 10 }}>
                        {activeWorlds.map((world) => (
                            <Link
                                key={world.world_id}
                                href={`/worlds/${world.world_id}/ingest`}
                                style={{
                                    display: "block",
                                    padding: 12,
                                    borderRadius: 14,
                                    border: "1px solid var(--border)",
                                    background: "var(--overlay)",
                                    textDecoration: "none",
                                    color: "inherit",
                                }}
                            >
                                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 8 }}>
                                    <div style={{ minWidth: 0 }}>
                                        <div
                                            style={{
                                                fontSize: 13,
                                                fontWeight: 600,
                                                whiteSpace: "nowrap",
                                                overflow: "hidden",
                                                textOverflow: "ellipsis",
                                            }}
                                        >
                                            {world.world_name}
                                        </div>
                                        <div style={{ fontSize: 12, color: "var(--text-subtle)" }}>
                                            {world.phase === "aborting"
                                                ? "Aborting..."
                                                : `${world.completed_chunks}/${world.total_chunks || "?"} chunks`}
                                        </div>
                                    </div>
                                    <div style={{
                                        display: "flex",
                                        alignItems: "center",
                                        gap: 6,
                                        color: world.phase === "aborting" ? "#fca5a5" : "var(--warning)",
                                        fontSize: 12,
                                    }}>
                                        <Loader2 size={13} style={{ animation: "spin 1s linear infinite" }} />
                                        {world.phase === "aborting" ? "Stopping" : `${Math.round(world.percent)}%`}
                                    </div>
                                </div>
                                <div
                                    style={{
                                        height: 8,
                                        borderRadius: 999,
                                        background: "var(--overlay-heavy)",
                                        overflow: "hidden",
                                    }}
                                >
                                    <div
                                        style={{
                                            width: `${world.percent}%`,
                                            height: "100%",
                                            borderRadius: 999,
                                            background: "linear-gradient(90deg, var(--primary), var(--primary-hover))",
                                            transition: "width 0.25s ease",
                                        }}
                                    />
                                </div>
                            </Link>
                        ))}
                    </div>
                </div>
            ) : (
                <button
                    onClick={() => setExpanded(true)}
                    style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 10,
                        background: "var(--floating-surface)",
                        color: "var(--text-primary)",
                        border: "1px solid var(--border-strong)",
                        borderRadius: 999,
                        padding: "10px 14px",
                        boxShadow: "0 16px 34px var(--shadow-color)",
                        backdropFilter: "blur(14px)",
                        cursor: "pointer",
                    }}
                >
                    <span
                        style={{
                            width: 9,
                            height: 9,
                            borderRadius: "50%",
                            background: "var(--warning)",
                            boxShadow: "0 0 0 6px rgba(245, 158, 11, 0.12)",
                            animation: "pulse-glow 2s ease-in-out infinite",
                            flexShrink: 0,
                        }}
                    />
                    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", minWidth: 0 }}>
                        <span style={{ fontSize: 12, fontWeight: 600, lineHeight: 1.1 }}>
                            {activeWorlds.length === 1 ? activeWorlds[0].world_name : `${activeWorlds.length} worlds ingesting`}
                        </span>
                        <span style={{ fontSize: 11, color: "var(--text-subtle)", lineHeight: 1.1 }}>
                            {activeWorlds.every((world) => world.phase === "aborting")
                                ? "Aborting..."
                                : aggregate.totalChunks > 0
                                ? `${aggregate.completedChunks}/${aggregate.totalChunks} chunks`
                                : "In progress"}
                        </span>
                    </div>
                    <span style={{ fontSize: 12, color: "var(--primary-light)", fontWeight: 600 }}>
                        {activeWorlds.every((world) => world.phase === "aborting")
                            ? "..."
                            : `${Math.round(aggregate.percent)}%`}
                    </span>
                    <ChevronUp size={16} />
                </button>
            )}
        </div>
    );
}
