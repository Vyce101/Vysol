"use client";

import { useState } from "react";
import { X } from "lucide-react";
import { apiFetch } from "@/lib/api";

export function CreateWorldModal({
    onClose,
    onCreated,
}: {
    onClose: () => void;
    onCreated: () => void;
}) {
    const [name, setName] = useState("");
    const [loading, setLoading] = useState(false);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!name.trim()) return;
        setLoading(true);
        try {
            await apiFetch("/worlds", {
                method: "POST",
                body: JSON.stringify({ world_name: name }),
            });
            onCreated();
        } catch (err: unknown) {
            alert((err as Error).message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div
            style={{
                position: "fixed", inset: 0, background: "var(--overlay-strong)",
                display: "flex", alignItems: "center", justifyContent: "center", zIndex: 50,
            }}
            onClick={onClose}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: "var(--card)", border: "1px solid var(--border)",
                    borderRadius: "var(--radius)", padding: 24, width: 400,
                    animation: "toast-in 0.2s ease-out",
                }}
            >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
                    <h2 style={{ fontSize: 18, fontWeight: 600 }}>Create World</h2>
                    <button onClick={onClose} style={{ background: "none", border: "none", color: "var(--text-subtle)", cursor: "pointer" }}>
                        <X size={18} />
                    </button>
                </div>

                <form onSubmit={handleSubmit}>
                    <label style={{ display: "block", marginBottom: 6, fontSize: 13, color: "var(--text-subtle)" }}>
                        World Name
                    </label>
                    <input
                        autoFocus
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="e.g. Mushoku Tensei"
                        style={{ width: "100%", marginBottom: 16 }}
                    />
                    <button
                        type="submit"
                        disabled={!name.trim() || loading}
                        style={{
                            width: "100%", padding: "10px 0", background: "var(--primary)",
                            color: "var(--primary-contrast)", border: "none", borderRadius: "var(--radius)",
                            fontSize: 14, fontWeight: 600, cursor: "pointer",
                            opacity: !name.trim() || loading ? 0.5 : 1,
                            transition: "background 0.2s",
                        }}
                    >
                        {loading ? "Creating..." : "Create World"}
                    </button>
                </form>
            </div>
        </div>
    );
}
