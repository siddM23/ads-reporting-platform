import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Sidebar from "@/components/Sidebar";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Unified Ads Portfolio Dashboard",
  description: "Aggregate performance metrics across ad platforms",
  icons: {
    icon: '/cube_logo.png',
  },
};

import { AuthProvider } from "@/lib/auth-context";
import SidebarWrapper from "@/components/SidebarWrapper";
import ReactQueryProvider from "@/components/providers/ReactQueryProvider";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} flex h-screen overflow-hidden bg-slate-50`}>
        <ReactQueryProvider>
          <AuthProvider>
            <div className="flex w-full h-screen">
              <SidebarWrapper />
              <main className="flex-1 overflow-y-auto">
                {children}
              </main>
            </div>
          </AuthProvider>
        </ReactQueryProvider>
      </body>
    </html>
  );
}
