"use client";

import { useAuth } from "@/lib/auth-context";
import { usePathname } from "next/navigation";
import Sidebar from "./Sidebar";

export default function SidebarWrapper() {
    const { isAuthenticated, isLoading } = useAuth();
    const pathname = usePathname();

    // Hide sidebar on auth pages or if not authenticated
    const isAuthPage = pathname?.startsWith("/auth");

    if (isLoading || isAuthPage || !isAuthenticated) {
        return null;
    }

    return <Sidebar />;
}
