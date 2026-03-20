"use client";

import { useEffect } from "react";
import { apiFetch } from "@/lib/api";
import { applyTheme, readCachedTheme } from "@/lib/theme";

export function ThemeController() {
    useEffect(() => {
        const cachedTheme = readCachedTheme();
        if (cachedTheme) {
            applyTheme(cachedTheme);
        } else {
            applyTheme("dark");
        }

        let cancelled = false;

        const syncTheme = async () => {
            try {
                const data = await apiFetch<{ ui_theme?: string }>("/settings");
                if (!cancelled) {
                    applyTheme(data.ui_theme);
                }
            } catch {
                if (!cancelled) {
                    applyTheme(cachedTheme || "dark");
                }
            }
        };

        void syncTheme();

        return () => {
            cancelled = true;
        };
    }, []);

    return null;
}
