"use client";

import { useEffect, useRef, useState } from "react";
import {
    AlertTriangle,
    CheckCircle,
    Loader2,
    Play,
    RotateCcw,
    XCircle,
} from "lucide-react";
import {
    abortEntityResolution,
    getEntityResolutionCurrent,
    getEntityResolutionStatus,
    startEntityResolution,
    streamEntityResolutionEvents,
    type EntityResolutionEvent,
    type EntityResolutionStatus,
} from "@/lib/api";

interface EntityResolutionPanelProps {
    worldId: string;
    canResolve: boolean;
    allComplete: boolean;
    isIngesting: boolean;
}

interface ResolutionLogRow {
    id: string;
    event: EntityResolutionEvent;
    summary: string;
    timestamp: string;
}

function isActiveStatus(value?: string) {
    const normalized = value?.trim().toLowerCase();
    return normalized === "in_progress" || normalized === "running" || normalized === "active" || normalized === "processing";
}

function isTerminalStatus(value?: string) {
    const normalized = value?.trim().toLowerCase();
    return normalized === "complete" || normalized === "completed" || normalized === "aborted" || normalized === "error" || normalized === "failed";
}

function formatCount(value: number | undefined | null) {
    return value === undefined || value === null ? "-" : value.toLocaleString();
}

function formatTitle(value?: string) {
    if (!value) return "Update";
    return value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function summarizeEvent(event: EntityResolutionEvent) {
    if (typeof event.message === "string" && event.message.trim()) {
        return event.message.trim();
    }
    if (typeof event.reason === "string" && event.reason.trim()) {
        return event.reason.trim();
    }
    if (typeof event.phase === "string" && event.phase.trim()) {
        return `Phase: ${formatTitle(event.phase)}`;
    }
    if (typeof event.status === "string" && event.status.trim()) {
        return `Status: ${formatTitle(event.status)}`;
    }

    const anchor = event.current_anchor as { display_name?: string } | undefined;
    if (anchor?.display_name) {
        return `Current anchor: ${anchor.display_name}`;
    }

    const candidates = event.current_candidates as unknown[] | undefined;
    if (Array.isArray(candidates)) {
        return `${candidates.length} candidate${candidates.length === 1 ? "" : "s"} ready`;
    }

    return "Resolution update received";
}

function statusBadge(status?: string) {
    const normalized = status?.trim().toLowerCase();
    if (isActiveStatus(normalized)) {
        return { bg: "var(--status-progress-bg)", fg: "var(--status-progress-fg)", label: "Running" };
    }
    if (normalized === "complete" || normalized === "completed") {
        return { bg: "var(--status-success-bg)", fg: "var(--status-success-fg)", label: "Complete" };
    }
    if (normalized === "aborted") {
        return { bg: "var(--status-pending-bg)", fg: "var(--status-pending-fg)", label: "Aborted" };
    }
    if (normalized === "error" || normalized === "failed") {
        return { bg: "var(--status-error-bg)", fg: "var(--status-error-fg)", label: "Error" };
    }
    return { bg: "var(--background-tertiary)", fg: "var(--text-subtle)", label: "Idle" };
}

export default function EntityResolutionPanel({
    worldId,
    canResolve,
    allComplete,
    isIngesting,
}: EntityResolutionPanelProps) {
    const [open, setOpen] = useState(false);
    const [topK, setTopK] = useState(50);
    const [reviewMode, setReviewMode] = useState(true);
    const [includeNormalizedExactPass, setIncludeNormalizedExactPass] = useState(true);
    const [status, setStatus] = useState<EntityResolutionStatus | null>(null);
    const [logs, setLogs] = useState<ResolutionLogRow[]>([]);
    const [running, setRunning] = useState(false);
    const [streamState, setStreamState] = useState<"idle" | "connecting" | "streaming">("idle");
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [lastSyncedAt, setLastSyncedAt] = useState<string | null>(null);
    const eventSourceRef = useRef<EventSource | null>(null);
    const logIdRef = useRef(0);

    function closeStream() {
        eventSourceRef.current?.close();
        eventSourceRef.current = null;
        setStreamState("idle");
    }

    function applyStatus(next: EntityResolutionStatus) {
        setStatus(next);
        setRunning(isActiveStatus(next.status));
        setLastSyncedAt(new Date().toISOString());
        setError(null);
        if (typeof next.top_k === "number") {
            setTopK(next.top_k);
        }
    }

    function pushEvent(event: EntityResolutionEvent) {
        setStreamState("streaming");
        setStatus((prev) => ({ ...(prev || {}), ...event }));
        setRunning(!isTerminalStatus(event.status) && !isTerminalStatus(event.event));
        setLogs((prev) => {
            logIdRef.current += 1;
            const nextRow: ResolutionLogRow = {
                id: `entity-resolution-log-${logIdRef.current}`,
                event,
                summary: summarizeEvent(event),
                timestamp: new Date().toISOString(),
            };
            return [...prev.slice(-49), nextRow];
        });
    }

    function openStream() {
        closeStream();
        setStreamState("connecting");
        setError(null);
        eventSourceRef.current = streamEntityResolutionEvents(
            worldId,
            pushEvent,
            () => {
                closeStream();
                void loadSnapshot(false);
            },
            (streamError) => {
                setError(streamError.message);
                closeStream();
            }
        );
    }

    async function loadSnapshot(allowReconnect = true) {
        try {
            const live = await getEntityResolutionStatus(worldId);
            applyStatus(live);
            if (isActiveStatus(live.status) && allowReconnect) {
                openStream();
            } else {
                closeStream();
            }
            return;
        } catch {
            try {
                const snapshot = await getEntityResolutionCurrent(worldId);
                applyStatus(snapshot);
                if (isActiveStatus(snapshot.status) && allowReconnect) {
                    openStream();
                } else {
                    closeStream();
                }
            } catch (snapshotError) {
                closeStream();
                setStatus(null);
                setRunning(false);
                setLastSyncedAt(new Date().toISOString());
                setError(snapshotError instanceof Error ? snapshotError.message : "Entity-resolution status is not available yet.");
            }
        }
    }
    useEffect(() => {
        void loadSnapshot(true);
        return () => closeStream();
        // We intentionally only bind to world changes so live SSE can continue across re-renders.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [worldId]);

    const gateMessage = running
        ? "Entity resolution is running and can be monitored here."
        : isIngesting
            ? "Wait for ingestion to finish before starting entity resolution."
            : canResolve
                ? "Ready for post-ingestion entity resolution."
                : !allComplete
                    ? "Finish ingestion or retry failed chunks before resolving entities."
                    : "Entity resolution is currently unavailable for this world.";

    const triggerLabel = running ? "Monitor Entities" : "Resolve Entities";
    const triggerDisabled = !running && !canResolve;
    const badge = statusBadge(status?.status || (running ? "running" : "idle"));
    const displayedTopK = typeof status?.top_k === "number" ? status.top_k : topK;
    const eventRows = [...logs].reverse();

    const handleStart = async () => {
        setBusy(true);
        setError(null);
        try {
            await startEntityResolution(worldId, {
                top_k: topK,
                review_mode: reviewMode,
                include_normalized_exact_pass: includeNormalizedExactPass,
            });
            setOpen(true);
            await loadSnapshot(true);
        } catch (startError) {
            setError(startError instanceof Error ? startError.message : "Unable to start entity resolution.");
            setRunning(false);
        } finally {
            setBusy(false);
        }
    };

    const handleAbort = async () => {
        setBusy(true);
        setError(null);
        try {
            await abortEntityResolution(worldId);
            await loadSnapshot(false);
        } catch (abortError) {
            setError(abortError instanceof Error ? abortError.message : "Unable to abort entity resolution.");
        } finally {
            setBusy(false);
        }
    };

    return (
        <div style={{ marginTop: 16, borderTop: "1px solid var(--border)", paddingTop: 12 }}>
            <button
                onClick={() => setOpen(true)}
                disabled={triggerDisabled}
                style={{
                    ...buttonStyle,
                    width: "100%",
                    background: running ? "var(--primary-soft-strong)" : "var(--primary)",
                    color: running ? "var(--primary-light)" : "var(--primary-contrast)",
                    opacity: triggerDisabled ? 0.45 : 1,
                }}
            >
                {running ? <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} /> : <Play size={14} />}
                {triggerLabel}
            </button>
            <div style={{ marginTop: 8, fontSize: 12, color: "var(--text-muted)", lineHeight: 1.45 }}>
                {gateMessage}
            </div>

            {open && (
                <div
                    role="dialog"
                    aria-modal="true"
                    style={{
                        position: "fixed",
                        inset: 0,
                        zIndex: 1000,
                        background: "var(--overlay-strong)",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        padding: 20,
                    }}
                >
                    <div
                        style={{
                            width: "100%",
                            maxWidth: 1100,
                            maxHeight: "92vh",
                            overflow: "hidden",
                            display: "flex",
                            flexDirection: "column",
                            background: "var(--background)",
                            border: "1px solid var(--border)",
                            borderRadius: "var(--radius)",
                            boxShadow: "0 24px 48px var(--shadow-color)",
                        }}
                    >
                        <div
                            style={{
                                display: "flex",
                                alignItems: "flex-start",
                                justifyContent: "space-between",
                                gap: 16,
                                padding: "18px 20px",
                                borderBottom: "1px solid var(--border)",
                            }}
                        >
                            <div>
                                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                                    <h3 style={{ fontSize: 18, fontWeight: 700 }}>Entity Resolution</h3>
                                    <span
                                        style={{
                                            padding: "3px 10px",
                                            borderRadius: 9999,
                                            fontSize: 11,
                                            fontWeight: 700,
                                            background: badge.bg,
                                            color: badge.fg,
                                        }}
                                    >
                                        {badge.label}
                                    </span>
                                </div>
                                <p style={{ fontSize: 13, color: "var(--text-muted)", maxWidth: 820, lineHeight: 1.5 }}>
                                    First pass uses exact matching after normalization, then the resolver can review top-K embedding candidates and merge only entities. Temporal edges stay intact.
                                </p>
                            </div>
                            <button
                                onClick={() => setOpen(false)}
                                style={{
                                    background: "none",
                                    border: "none",
                                    color: "var(--text-muted)",
                                    cursor: "pointer",
                                    padding: 4,
                                    flexShrink: 0,
                                }}
                            >
                                <XCircle size={20} />
                            </button>
                        </div>

                        <div
                            style={{
                                flex: 1,
                                overflowY: "auto",
                                padding: 20,
                                display: "flex",
                                flexDirection: "column",
                                gap: 16,
                            }}
                        >
                            <div
                                style={{
                                    display: "flex",
                                    flexWrap: "wrap",
                                    gap: 12,
                                    alignItems: "stretch",
                                }}
                            >
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Status</div>
                                    <div style={summaryValueStyle}>{formatTitle(status?.status || (running ? "in_progress" : "idle"))}</div>
                                </div>
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Phase</div>
                                    <div style={summaryValueStyle}>{formatTitle((status?.phase as string | undefined) || "waiting")}</div>
                                </div>
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Top K</div>
                                    <div style={summaryValueStyle}>{formatCount(displayedTopK)}</div>
                                </div>
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Resolved</div>
                                    <div style={summaryValueStyle}>{formatCount(status?.resolved_entities as number | undefined)}</div>
                                </div>
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Unresolved</div>
                                    <div style={summaryValueStyle}>{formatCount(status?.unresolved_entities as number | undefined)}</div>
                                </div>
                                <div style={{ ...summaryCardStyle, flex: "1 1 220px" }}>
                                    <div style={summaryLabelStyle}>Exact pass</div>
                                    <div style={summaryValueStyle}>{includeNormalizedExactPass ? "On" : "Off"}</div>
                                </div>
                            </div>

                            {status?.current_anchor && (
                                <div style={panelStyle}>
                                    <div style={panelHeaderStyle}>Current Anchor</div>
                                    <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 4 }}>
                                        {(status.current_anchor as { display_name?: string }).display_name || "Unnamed entity"}
                                    </div>
                                    {(status.current_anchor as { description?: string }).description && (
                                        <div style={{ fontSize: 13, color: "var(--text-muted)", lineHeight: 1.5 }}>
                                            {(status.current_anchor as { description?: string }).description}
                                        </div>
                                    )}
                                </div>
                            )}

                            {status?.current_candidates && Array.isArray(status.current_candidates) && status.current_candidates.length > 0 && (
                                <div style={panelStyle}>
                                    <div style={panelHeaderStyle}>Top Candidates</div>
                                    <div style={{ display: "grid", gap: 10 }}>
                                        {(status.current_candidates as Array<{ node_id?: string; display_name?: string; description?: string; score?: number }>).slice(0, 6).map((candidate, index) => (
                                            <div
                                                key={`${candidate.node_id || index}`}
                                                style={{
                                                    border: "1px solid var(--border)",
                                                    borderRadius: 10,
                                                    padding: 12,
                                                    background: "var(--card)",
                                                }}
                                            >
                                                <div style={{ display: "flex", justifyContent: "space-between", gap: 10, marginBottom: 6 }}>
                                                    <div style={{ fontSize: 14, fontWeight: 600 }}>
                                                        {candidate.display_name || candidate.node_id || `Candidate ${index + 1}`}
                                                    </div>
                                                    {candidate.score !== undefined && (
                                                        <div style={{ fontSize: 11, color: "var(--text-muted)", fontFamily: "monospace" }}>
                                                            {candidate.score.toFixed(3)}
                                                        </div>
                                                    )}
                                                </div>
                                                {candidate.description && (
                                                    <div style={{ fontSize: 12, color: "var(--text-muted)", lineHeight: 1.5 }}>
                                                        {candidate.description}
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div style={panelStyle}>
                                <div style={panelHeaderStyle}>Controls</div>
                                <div
                                    style={{
                                        display: "grid",
                                        gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                                        gap: 12,
                                        marginTop: 8,
                                    }}
                                >
                                    <label style={controlLabelStyle}>
                                        <span style={controlTextStyle}>Top K candidates</span>
                                        <input
                                            type="number"
                                            min={1}
                                            max={250}
                                            value={topK}
                                            onChange={(e) => setTopK(Number(e.target.value) || 1)}
                                            disabled={busy || running}
                                            style={controlInputStyle}
                                        />
                                    </label>
                                    <label style={controlLabelStyle}>
                                        <span style={controlTextStyle}>Review mode</span>
                                        <input
                                            type="checkbox"
                                            checked={reviewMode}
                                            onChange={(e) => setReviewMode(e.target.checked)}
                                            disabled={busy || running}
                                        />
                                    </label>
                                    <label style={controlLabelStyle}>
                                        <span style={controlTextStyle}>Include exact normalized pass</span>
                                        <input
                                            type="checkbox"
                                            checked={includeNormalizedExactPass}
                                            onChange={(e) => setIncludeNormalizedExactPass(e.target.checked)}
                                            disabled={busy || running}
                                        />
                                    </label>
                                </div>

                                <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 16 }}>
                                    {!running ? (
                                        <button
                                            onClick={() => void handleStart()}
                                            disabled={!canResolve || busy}
                                            style={{
                                                ...buttonStyle,
                                                background: "var(--primary)",
                                                color: "var(--primary-contrast)",
                                                opacity: !canResolve || busy ? 0.45 : 1,
                                            }}
                                        >
                                            {busy ? <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} /> : <Play size={14} />}
                                            Start Resolution
                                        </button>
                                    ) : (
                                        <button
                                            onClick={() => void handleAbort()}
                                            disabled={busy}
                                            style={{
                                                ...buttonStyle,
                                                background: "var(--status-error-bg)",
                                                color: "var(--status-error-fg)",
                                                opacity: busy ? 0.45 : 1,
                                            }}
                                        >
                                            {busy ? <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} /> : <AlertTriangle size={14} />}
                                            Abort Run
                                        </button>
                                    )}

                                    <button
                                        onClick={() => void loadSnapshot(true)}
                                        disabled={busy}
                                        style={{
                                            ...buttonStyle,
                                            background: "var(--card)",
                                            color: "var(--text-primary)",
                                            opacity: busy ? 0.45 : 1,
                                        }}
                                    >
                                        <RotateCcw size={14} />
                                        Refresh Status
                                    </button>
                                </div>

                                {error && (
                                    <div
                                        style={{
                                            marginTop: 14,
                                            padding: "10px 12px",
                                            borderRadius: 10,
                                            border: "1px solid var(--status-error-soft-border)",
                                            background: "var(--status-error-soft-bg)",
                                            color: "var(--status-error-fg)",
                                            fontSize: 13,
                                            lineHeight: 1.5,
                                        }}
                                    >
                                        {error}
                                    </div>
                                )}
                            </div>

                            <div style={panelStyle}>
                                <div style={panelHeaderStyle}>Live Events</div>
                                <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                    {streamState === "connecting" && "Connecting to live updates..."}
                                    {streamState === "streaming" && "Receiving live SSE updates."}
                                    {streamState === "idle" && "Live updates will appear here when the resolver is active."}
                                    {lastSyncedAt && ` Last synced ${new Date(lastSyncedAt).toLocaleTimeString()}.`}
                                </div>

                                <div
                                    style={{
                                        marginTop: 12,
                                        maxHeight: 320,
                                        overflowY: "auto",
                                        border: "1px solid var(--border)",
                                        borderRadius: 10,
                                        background: "var(--background)",
                                    }}
                                >
                                    {eventRows.length === 0 ? (
                                        <div style={{ padding: 16, fontSize: 13, color: "var(--text-muted)" }}>
                                            No events yet.
                                        </div>
                                    ) : (
                                        eventRows.map((row) => {
                                            const rawType = row.event.event || row.event.phase || row.event.status || "update";
                                            const normalizedType = formatTitle(typeof rawType === "string" ? rawType : "update");
                                            const isError = rawType === "error" || rawType === "failed";
                                            const isDone = rawType === "complete" || rawType === "completed" || rawType === "aborted";
                                            return (
                                                <div
                                                    key={row.id}
                                                    style={{
                                                        padding: 12,
                                                        borderBottom: "1px solid var(--border)",
                                                        display: "flex",
                                                        gap: 10,
                                                        alignItems: "flex-start",
                                                    }}
                                                >
                                                    <div style={{ marginTop: 1, color: isError ? "var(--status-error-fg)" : isDone ? "var(--status-success-fg)" : "var(--text-muted)" }}>
                                                        {isError ? (
                                                            <XCircle size={14} />
                                                        ) : isDone ? (
                                                            <CheckCircle size={14} />
                                                        ) : (
                                                            <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} />
                                                        )}
                                                    </div>
                                                    <div style={{ flex: 1, minWidth: 0 }}>
                                                        <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 3 }}>
                                                            <div style={{ fontSize: 13, fontWeight: 600 }}>{normalizedType}</div>
                                                            <div style={{ fontSize: 11, color: "var(--text-muted)" }}>
                                                                {new Date(row.timestamp).toLocaleTimeString()}
                                                            </div>
                                                        </div>
                                                        <div style={{ fontSize: 12, color: "var(--text-muted)", lineHeight: 1.5 }}>
                                                            {row.summary}
                                                        </div>
                                                    </div>
                                                </div>
                                            );
                                        })
                                    )}
                                </div>
                            </div>

                            <div style={{ fontSize: 12, color: "var(--text-muted)", lineHeight: 1.5, display: "flex", gap: 8 }}>
                                <AlertTriangle size={14} style={{ flexShrink: 0, marginTop: 1 }} />
                                <span>
                                    Assumption: the backend exposes `start`, `status`, `events`, `abort`, and `current` under `/worlds/{worldId}/entity-resolution`. If the event payload shape changes, this panel will still render the generic message and the latest status snapshot.
                                </span>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

const buttonStyle: React.CSSProperties = {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    padding: "8px 16px",
    borderRadius: "var(--radius)",
    border: "none",
    fontSize: 13,
    fontWeight: 600,
    cursor: "pointer",
    transition: "opacity 0.2s",
};

const panelStyle: React.CSSProperties = {
    border: "1px solid var(--border)",
    borderRadius: 12,
    padding: 16,
    background: "var(--card)",
};

const panelHeaderStyle: React.CSSProperties = {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
    marginBottom: 10,
    fontSize: 14,
    fontWeight: 700,
};

const summaryCardStyle: React.CSSProperties = {
    border: "1px solid var(--border)",
    borderRadius: 12,
    padding: 14,
    background: "var(--card)",
};

const summaryLabelStyle: React.CSSProperties = {
    fontSize: 11,
    textTransform: "uppercase",
    letterSpacing: "0.08em",
    color: "var(--text-muted)",
    marginBottom: 8,
};

const summaryValueStyle: React.CSSProperties = {
    fontSize: 16,
    fontWeight: 700,
};

const controlLabelStyle: React.CSSProperties = {
    display: "flex",
    flexDirection: "column",
    gap: 8,
    fontSize: 13,
};

const controlTextStyle: React.CSSProperties = {
    fontWeight: 600,
    color: "var(--text-primary)",
};

const controlInputStyle: React.CSSProperties = {
    width: "100%",
    maxWidth: 240,
};
