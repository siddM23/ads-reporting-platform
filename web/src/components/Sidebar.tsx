"use client";

import React from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname } from "next/navigation";
import { LayoutDashboard, Settings, LogOut } from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuth } from "@/lib/auth-context";

const Sidebar = () => {
    const pathname = usePathname();
    const { logout } = useAuth();

    const navItems = [
        { name: "Dashboard", icon: LayoutDashboard, href: "/dash" },
        { name: "Integrations", icon: Settings, href: "/integrations" },
    ];

    return (
        <div className="flex flex-col h-screen w-16 md:w-20 bg-white border-r border-slate-200">
            <div className="flex items-center justify-center h-16 border-b border-slate-100">
                <div className="w-14 h-14 flex items-center justify-center">
                    <Image src="/cube_logo.png" alt="Cube Logo" width={48} height={48} className="object-contain" />
                </div>
            </div>

            <nav className="flex-1 px-2 py-4 space-y-2 flex flex-col items-center">
                {navItems.map((item) => (
                    <Link
                        key={item.href}
                        href={item.href}
                        className={cn(
                            "p-3 rounded-xl transition-all duration-200 group flex justify-center items-center w-full",
                            pathname === item.href
                                ? "bg-indigo-50 text-indigo-600 shadow-sm"
                                : "text-slate-400 hover:text-slate-600 hover:bg-slate-50"
                        )}
                        title={item.name}
                    >
                        <item.icon size={24} />
                    </Link>
                ))}
            </nav>

            <div className="px-2 py-6 border-t border-slate-100 flex flex-col items-center">
                <button
                    onClick={logout}
                    className="p-3 rounded-xl transition-all duration-200 group flex justify-center items-center w-full text-slate-400 hover:text-red-600 hover:bg-red-50"
                    title="Logout"
                >
                    <LogOut size={24} />
                </button>
            </div>
        </div>
    );
};

export default Sidebar;
