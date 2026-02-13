"use client";

import React, { useState, useEffect } from "react";
import { Plus, X, CheckCircle2, AlertCircle, Loader2 } from "lucide-react";
import Image from "next/image";
import { cn } from "@/lib/utils";

interface PlatformCardProps {
    title: string;
    description: string;
    icon: React.ReactNode;
    accounts: { email: string; status: "Active" | "Inactive"; account_id: string; account_name: string }[];
    accentColor: string;
    onConnect: () => void;
}

const PlatformCard: React.FC<PlatformCardProps> = ({
    title,
    description,
    icon,
    accounts,
    accentColor,
    onConnect
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
                            <div key={acc.account_id || i} className="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-2xl hover:border-slate-300 transition-all shadow-sm shadow-slate-100 hover:shadow-md">
                                <div className="flex flex-col gap-0.5">
                                    <span className="text-[14px] font-bold text-slate-900">{acc.account_name}</span>
                                    <span className="text-[11px] text-slate-500 font-medium">{acc.email}</span>
                                    <div className="flex items-center gap-1.5 px-2 py-0.5 w-fit rounded-full bg-emerald-50 text-emerald-600 text-[10px] font-bold uppercase tracking-wider mt-1">
                                        <CheckCircle2 size={10} />
                                        {acc.status}
                                    </div>
                                </div>
                                <button className="text-slate-300 hover:text-red-500 hover:bg-red-50 p-2 rounded-xl transition-all">
                                    <X size={18} />
                                </button>
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

const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";

export default function IntegrationsPage() {
    const [connectedAccounts, setConnectedAccounts] = useState<any[]>([]);
    const [isLoading, setIsLoading] = useState(true);


    useEffect(() => {
        fetchIntegrations();
    }, []);

    const fetchIntegrations = async () => {
        try {
            const res = await fetch(`${API_URL}/integrations`);
            if (!res.ok) {
                console.error(`API Error: ${res.status} ${res.statusText}`);
                return;
            }
            const data = await res.json();
            if (Array.isArray(data)) {
                setConnectedAccounts(data);
            } else {
                console.error("Expected array from /integrations, got:", data);
            }
        } catch (err) {
            console.error("Failed to fetch integrations", err);
        } finally {
            setIsLoading(false);
        }
    };



    const handleConnectMeta = async () => {
        try {
            const res = await fetch(`${API_URL}/auth/meta/login`);
            const data = await res.json();
            if (data.url) {
                window.location.href = data.url;
            }
        } catch (err) {
            alert("Failed to start Meta OAuth flow");
        }
    };

    const handleConnectGoogle = async () => {
        try {
            const res = await fetch(`${API_URL}/auth/google/login`);
            const data = await res.json();
            if (data.url) {
                window.location.href = data.url;
            }
        } catch (err) {
            alert("Failed to start Google OAuth flow");
        }
    };

    const filterAccounts = (platform: string) => {
        const accounts = Array.isArray(connectedAccounts) ? connectedAccounts : [];
        return accounts
            .filter(a => a?.platform?.toLowerCase() === platform.toLowerCase())
            .map(a => ({
                email: a.email,
                status: a.status as "Active" | "Inactive",
                account_id: a.account_id,
                account_name: a.account_name || a.account_id
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
            accentColor: "bg-white",
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
                <p className="text-slate-500">Manage your ad platform integrations and API connections</p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {platforms.map((platform) => (
                    <PlatformCard
                        key={platform.id}
                        {...platform}
                        accounts={filterAccounts(platform.id)}
                    />
                ))}
            </div>
        </div>
    );
}
