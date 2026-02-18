"use client";

import React, { useState, useEffect } from "react";
import { Plus, X, CheckCircle2, AlertCircle, Loader2 } from "lucide-react";
import Image from "next/image";
import { cn } from "@/lib/utils";
import { authFetch } from "@/lib/auth-context";

interface PlatformCardProps {
    title: string;
    description: string;
    icon: React.ReactNode;
    accounts: {
        id: string;
        email: string;
        status: "Active" | "Inactive";
        account_id: string;
        account_name: string;
        needs_reauth?: boolean;
    }[];
    accentColor: string;
    onConnect: () => void;
    onDelete: (id: string) => void;
}

const PlatformCard: React.FC<PlatformCardProps> = ({
    title,
    description,
    icon,
    accounts,
    accentColor,
    onConnect,
    onDelete
}) => {
    return (
        <div className="bg-white rounded-3xl border border-slate-200 shadow-xl shadow-slate-200/50 overflow-hidden flex flex-col min-h-[450px] transition-all duration-300 hover:shadow-indigo-100/50 hover:-translate-y-1">
            <div className="p-8 border-b border-slate-100 flex items-start gap-5">
                <div className={cn("w-14 h-14 rounded-2xl flex items-center justify-center text-white shadow-lg", accentColor)}>
                    {icon}
                </div>
                <div>
                    <h3 className="text-xl font-bold text-slate-900 mb-1">{title}</h3>
                    <p className="text-sm text-slate-500 leading-relaxed font-medium">{description}</p>
                </div>
            </div>

            <div className="p-8 flex-1 bg-slate-50/10">
                <div className="flex items-center justify-between mb-6">
                    <h4 className="text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400">Connected Accounts</h4>
                    <span className="text-[10px] font-bold bg-indigo-50 text-indigo-600 px-2 py-0.5 rounded-full uppercase tracking-wider">{accounts.length} Total</span>
                </div>
                <div className="space-y-4 max-h-[200px] overflow-y-auto pr-2">
                    {accounts.length > 0 ? (
                        accounts.map((acc, i) => (
                            <div key={acc.id || i} className="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-2xl hover:border-slate-300 transition-all shadow-sm shadow-slate-100 hover:shadow-md">
                                <div className="flex flex-col gap-0.5">
                                    <span className="text-[14px] font-bold text-slate-900">{acc.account_name}</span>
                                    <span className="text-[11px] text-slate-500 font-medium">{acc.email}</span>
                                    <div className={cn(
                                        "flex items-center gap-1.5 px-2 py-0.5 w-fit rounded-full text-[10px] font-bold uppercase tracking-wider mt-1",
                                        acc.needs_reauth ? "bg-red-50 text-red-600" : "bg-emerald-50 text-emerald-600"
                                    )}>
                                        {acc.needs_reauth ? <AlertCircle size={10} /> : <CheckCircle2 size={10} />}
                                        {acc.needs_reauth ? "Needs Action" : acc.status}
                                    </div>
                                </div>
                                <div className="flex gap-2">
                                    {acc.needs_reauth && (
                                        <button
                                            onClick={onConnect}
                                            className="text-white bg-red-500 hover:bg-red-600 px-3 py-1 rounded-lg text-xs font-bold transition-all shadow-md shadow-red-200"
                                        >
                                            Fix
                                        </button>
                                    )}
                                    <button
                                        onClick={() => onDelete(acc.id)}
                                        className="text-slate-300 hover:text-red-500 hover:bg-red-50 p-2 rounded-xl transition-all"
                                    >
                                        <X size={18} />
                                    </button>
                                </div>
                            </div>
                        ))
                    ) : (
                        <div className="border-2 border-dashed border-slate-100 rounded-3xl h-40 flex flex-col items-center justify-center text-slate-300 gap-3 bg-slate-50/30">
                            <AlertCircle size={32} className="opacity-20" />
                            <span className="text-xs font-bold uppercase tracking-widest opacity-60">No accounts connected</span>
                        </div>
                    )}
                </div>
            </div>

            <div className="p-8 pt-0">
                <button
                    onClick={onConnect}
                    className="w-full py-4 bg-slate-900 text-white rounded-2xl text-sm font-bold hover:bg-slate-800 transition-all duration-300 flex items-center justify-center gap-3 shadow-lg shadow-slate-200 group active:scale-[0.98]"
                >
                    <Plus size={20} className="text-white/70 group-hover:text-white transition-colors" />
                    Connect {title.split(' ')[0]} Account
                </button>
            </div>
        </div>
    );
};

export default function IntegrationsPage() {
    const [connectedAccounts, setConnectedAccounts] = useState<any[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    const fetchIntegrations = async () => {
        setIsLoading(true);
        try {
            const res = await authFetch(`${process.env.NEXT_PUBLIC_API_URL || ''}/api/integrations`);
            if (res.ok) {
                const data = await res.json();
                setConnectedAccounts(data);
            }
        } catch (error) {
            console.error("Failed to fetch integrations", error);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchIntegrations();

        // Listen for success params
        if (typeof window !== "undefined") {
            const params = new URLSearchParams(window.location.search);
            if (params.get("success") === "true") {
                // Clear URL params
                window.history.replaceState({}, "", "/integrations");
                fetchIntegrations();
            }
        }
    }, []);

    const handleConnectGoogle = async () => {
        // Need current user ID to pass in state
        // We can create a dedicated endpoint to get the OAuth URL which handles user linkage via session/token
        // Or assume the backend handles it via 'state' if we initiate it.
        // For now, let's fetch the URL from backend
        try {
            // Get user ID from local storage or context if available? 
            // Better: Let backend extract user_id from token if we call an authenticated endpoint to get the URL.
            // But currently the OAuth initiation endpoints are unauthenticated GETs usually?
            // Wait, we updated them to accept 'user_id' query param!

            // However, for security, the backend should ideally discern user from token.
            // But since OAuth initiation is often a direct link...
            // Let's call a protected endpoint to get the redirect URL

            // For now, simply redirecting to the updated endpoint with user_id is tricky if we don't have user_id in frontend state easily accessible here without context.
            // But authFetch handles the token.

            // Let's call the endpoint using authFetch to get the URL
            const res = await authFetch(`${process.env.NEXT_PUBLIC_API_URL || ''}/api/auth/google/login`);
            const data = await res.json();
            if (data.url) window.location.href = data.url;
        } catch (e) {
            console.error("Error initiating Google login", e);
        }
    };

    const handleConnectMeta = async () => {
        try {
            const res = await authFetch(`${process.env.NEXT_PUBLIC_API_URL || ''}/api/auth/meta/login`);
            const data = await res.json();
            if (data.url) window.location.href = data.url;
        } catch (e) {
            console.error("Error initiating Meta login", e);
        }
    };

    const handleDelete = async (id: string) => {
        if (!confirm("Are you sure you want to disconnect this account?")) return;

        try {
            const res = await authFetch(`${process.env.NEXT_PUBLIC_API_URL || ''}/api/integrations/${id}`, {
                method: "DELETE"
            });
            if (res.ok) {
                fetchIntegrations();
            }
        } catch (e) {
            console.error("Error deleting integration", e);
        }
    };

    const filterAccounts = (platform: string) => {
        const accounts = Array.isArray(connectedAccounts) ? connectedAccounts : [];
        return accounts
            .filter(a => a?.platform?.toLowerCase() === platform.toLowerCase())
            .map(a => ({
                id: a.id,
                email: a.email,
                status: a.status as "Active" | "Inactive",
                account_id: a.account_id,
                account_name: a.account_name || a.account_id,
                needs_reauth: a.needs_reauth
            }));
    };

    const platforms = [
        {
            id: "google",
            title: "Google Ads",
            description: "Connect your Google Ads accounts",
            icon: <Image src="/google.png" alt="Google Ads" width={28} height={28} className="object-contain" />,
            accentColor: "bg-white",
            onConnect: handleConnectGoogle
        },
        {
            id: "meta",
            title: "Meta Ads",
            description: "Connect your Meta Ads accounts",
            icon: <Image src="/facebook.png" alt="Meta Ads" width={28} height={28} className="object-contain" />,
            accentColor: "bg-gradient-to-br from-blue-600 to-blue-700",
            onConnect: handleConnectMeta
        }
    ];

    return (
        <div className="p-8">
            <div className="mb-8 border-b border-slate-100 pb-8 flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <h1 className="text-3xl font-bold text-slate-900">Integrations</h1>
                    {isLoading && <Loader2 className="animate-spin text-slate-400" size={20} />}
                </div>
            </div>

            <div className="mb-8">
                <h2 className="text-2xl font-bold text-slate-900 mb-2">Platform Settings</h2>
                <p className="text-slate-500">Manage your ad platform integrations</p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {platforms.map((platform) => (
                    <PlatformCard
                        key={platform.id}
                        {...platform}
                        accounts={filterAccounts(platform.id)}
                        accentColor={platform.accentColor}
                        onConnect={platform.onConnect}
                        onDelete={(id) => handleDelete(id)}
                    />
                ))}
            </div>
        </div>
    );
}
