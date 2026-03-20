"use client";

export type UITheme = "dark" | "light";

const THEME_STORAGE_KEY = "vysol-ui-theme";

export function normalizeTheme(value: unknown): UITheme {
    return typeof value === "string" && value.toLowerCase() === "light" ? "light" : "dark";
}

export function applyTheme(value: unknown) {
    if (typeof document === "undefined") return;
    const theme = normalizeTheme(value);
    document.documentElement.setAttribute("data-theme", theme);
    document.documentElement.style.colorScheme = theme;
    document.body?.setAttribute("data-theme", theme);
    try {
        window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
        // Ignore storage issues.
    }
}

export function readCachedTheme(): UITheme | null {
    if (typeof window === "undefined") return null;
    try {
        const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
        return stored ? normalizeTheme(stored) : null;
    } catch {
        return null;
    }
}
