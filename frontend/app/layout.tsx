import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { GlobalIngestionStatus } from "@/components/GlobalIngestionStatus";
import { ThemeController } from "@/components/ThemeController";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
    title: "Vysol",
    description: "Accessible graph RAG for building worlds, graphs, and chat workflows in one place.",
};

export default function RootLayout({
    children,
}: {
    children: React.ReactNode;
}) {
    return (
        <html lang="en" data-theme="dark">
            <body className={`${inter.variable} font-sans antialiased`}>
                <ThemeController />
                {children}
                <GlobalIngestionStatus />
            </body>
        </html>
    );
}
