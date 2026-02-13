"use client";

import React, { useState, useEffect, useMemo } from "react";
import { ChevronDown, ChevronRight, AlertCircle, RefreshCcw, Clock } from "lucide-react";
import { cn } from "@/lib/utils";

interface MetricRowProps {
    label: string;
    isHeader?: boolean;
    isExpanded?: boolean;
    onToggle?: () => void;
    level?: number;
    metrics: {
        last7: { spend: string; roas: string; rev: string; res: string; cac: string };
        prevMonth: { spend: string; roas: string; rev: string; res: string; cac: string };
        sixMonth: { res: string; roas: string; cac: string };
    };
}

const MetricRow: React.FC<MetricRowProps> = ({
    label,
    isHeader = false,
    isExpanded = false,
    onToggle,
    level = 0,
    metrics
}) => {
    const isCampaign = level > 0;

    return (
        <tr className={cn(
            "border-b border-slate-100 transition-colors duration-150",
            isHeader ? "bg-white font-semibold group/row" : "bg-slate-50/10 text-slate-600",
            !isHeader && "hover:bg-indigo-50/30"
        )}>
            <td className="py-4 px-6 min-w-[280px]">
                <div className="flex items-center gap-3" style={{ paddingLeft: `${level * 24}px` }}>
                    {isHeader ? (
                        <button
                            onClick={onToggle}
                            className="p-1 hover:bg-slate-100 rounded-lg transition-colors text-slate-400 hover:text-slate-600"
                        >
                            {isExpanded ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                        </button>
                    ) : (
                        <div className="w-8 shrink-0 flex justify-end pr-2">
                            <div className="h-[1px] w-4 bg-slate-200" />
                        </div>
                    )}
                    <span className={cn(
                        isCampaign ? "text-sm text-slate-600 font-medium" : "text-[14px] text-slate-900 font-semibold",
                    )}>
                        {label}
                    </span>
                </div>
            </td>

            {/* Last 7 Days */}
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-900">{metrics.last7.spend}</td>
            <td className="py-4 px-4 text-center text-sm font-medium">
                <span className={cn(
                    "px-2 py-1 rounded-lg text-[13px] font-bold",
                    parseFloat(metrics.last7.roas) < 3 ? "text-red-500 bg-red-50" : "text-slate-900"
                )}>
                    {metrics.last7.roas}
                </span>
            </td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-900">{metrics.last7.rev}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-400 font-light">{metrics.last7.res}</td>
            <td className="py-4 px-4 text-center text-sm font-medium border-r border-slate-100/50">
                <span className={cn(
                    "px-2 py-1 rounded-lg text-[13px] font-bold",
                    parseFloat(metrics.last7.cac.replace('$', '')) > 30 ? "text-red-500 bg-red-50" : "text-slate-900"
                )}>
                    {metrics.last7.cac}
                </span>
            </td>

            {/* Previous Month */}
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600 bg-slate-50/20">{metrics.prevMonth.spend}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600 bg-slate-50/20">{metrics.prevMonth.roas}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600 bg-slate-50/20">{metrics.prevMonth.rev}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-400 bg-slate-50/20 font-light">{metrics.prevMonth.res}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600 bg-slate-50/20 border-r border-slate-100/50">{metrics.prevMonth.cac}</td>

            {/* 6 Months Avg */}
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-400 font-light">{metrics.sixMonth.res}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600">{metrics.sixMonth.roas}</td>
            <td className="py-4 px-4 text-center text-sm font-medium text-slate-600">{metrics.sixMonth.cac}</td>
        </tr>
    );
};

const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";

export default function DashPage() {
    const [activeTab, setActiveTab] = useState("Meta Ads");
    const [rawApiData, setRawApiData] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
    const [syncStatus, setSyncStatus] = useState<{
        syncs_remaining: number;
        max_syncs: number;
        can_sync: boolean;
        cooldown_seconds_remaining: number;
        next_free_at: string | null;
    } | null>(null);
    const [cooldownDisplay, setCooldownDisplay] = useState("");

    const toggleRow = (label: string) => {
        setExpandedRows(prev => ({ ...prev, [label]: !prev[label] }));
    };

    const platforms = ["Meta Ads", "Google Ads"];

    // Transform API data from all ranges into the nested structure: Brand -> Campaigns
    const processData = (allRanges: { "7": any[], "30": any[], "180": any[] }, platformFilter: string) => {
        const brands: Record<string, any> = {};
        const targetPlatform = platformFilter.toLowerCase().includes("meta") ? "meta" : "google";

        if (!allRanges) return [];

        // Helper to parse Meta's nested action lists
        const getActionValue = (list: any[], actionType: string) => {
            if (!list) return 0;
            const item = list.find((x: any) => x.action_type === actionType);
            return item ? parseFloat(item.value) : 0;
        };

        // Helper to extract metrics from a row
        const extractMetrics = (row: any) => {
            const spend = parseFloat(row.spend || "0");
            const revenue = getActionValue(row.action_values, "purchase") || getActionValue(row.action_values, "omni_purchase");
            const results = getActionValue(row.actions, "purchase") || getActionValue(row.actions, "omni_purchase");
            const roas = spend > 0 ? (revenue / spend) : 0;
            const cac = results > 0 ? (spend / results) : 0;
            return { spend, revenue, results, roas, cac };
        };

        // Build lookup maps for 30-day and 180-day data by campaign_id
        const data30Map: Record<string, any> = {};
        const data180Map: Record<string, any> = {};

        (allRanges["30"] || []).forEach((row: any) => {
            data30Map[row.campaign_id] = extractMetrics(row);
        });

        (allRanges["180"] || []).forEach((row: any) => {
            data180Map[row.campaign_id] = extractMetrics(row);
        });

        // Process 7-day data as primary, then merge in 30/180 data
        (allRanges["7"] || []).forEach((row: any) => {
            if (row.platform !== targetPlatform) return;
            const brandName = row.account_name || "Unknown Account";
            const campaignId = row.campaign_id;

            // Initialize Brand Entry if missing
            if (!brands[brandName]) {
                brands[brandName] = {
                    brand: brandName,
                    metrics: {
                        last7: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 },
                        prevMonth: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 },
                        sixMonth: { res: 0, roas: 0, cac: 0 }
                    },
                    campaigns: []
                };
            }

            // Extract 7-day metrics
            const m7 = extractMetrics(row);

            // Get 30-day and 180-day metrics for this campaign
            const m30 = data30Map[campaignId] || { spend: 0, revenue: 0, results: 0, roas: 0, cac: 0 };
            const m180 = data180Map[campaignId] || { spend: 0, revenue: 0, results: 0, roas: 0, cac: 0 };

            // Add Campaign with all ranges
            brands[brandName].campaigns.push({
                label: row.campaign_name,
                campaignId,
                metrics: {
                    last7: {
                        spend: `$${m7.spend.toFixed(2)}`,
                        roas: m7.roas.toFixed(2),
                        rev: `$${m7.revenue.toFixed(2)}`,
                        res: m7.results.toString(),
                        cac: `$${m7.cac.toFixed(2)}`
                    },
                    prevMonth: {
                        spend: `$${m30.spend.toFixed(2)}`,
                        roas: m30.roas.toFixed(2),
                        rev: `$${m30.revenue.toFixed(2)}`,
                        res: m30.results.toString(),
                        cac: `$${m30.cac.toFixed(2)}`
                    },
                    sixMonth: {
                        res: m180.results.toString(),
                        roas: m180.roas.toFixed(2),
                        cac: `$${m180.cac.toFixed(2)}`
                    }
                }
            });

            // Aggregate to Brand Level
            brands[brandName].metrics.last7.spend += m7.spend;
            brands[brandName].metrics.last7.rev += m7.revenue;
            brands[brandName].metrics.last7.res += m7.results;

            brands[brandName].metrics.prevMonth.spend += m30.spend;
            brands[brandName].metrics.prevMonth.rev += m30.revenue;
            brands[brandName].metrics.prevMonth.res += m30.results;

            brands[brandName].metrics.sixMonth.res += m180.results;
        });

        // Finalize Brand Aggregates
        Object.values(brands).forEach((brand: any) => {
            // Last 7 days
            const m7 = brand.metrics.last7;
            m7.roas = m7.spend > 0 ? (m7.rev / m7.spend).toFixed(2) : "0.00";
            m7.cac = m7.res > 0 ? (m7.spend / m7.res).toFixed(2) : "0.00";
            m7.spend = `$${m7.spend.toFixed(2)}`;
            m7.rev = `$${m7.rev.toFixed(2)}`;
            m7.res = m7.res.toString();
            m7.cac = `$${m7.cac}`;

            // Previous Month (30 days)
            const m30 = brand.metrics.prevMonth;
            m30.roas = m30.spend > 0 ? (m30.rev / m30.spend).toFixed(2) : "0.00";
            m30.cac = m30.res > 0 ? (m30.spend / m30.res).toFixed(2) : "0.00";
            m30.spend = `$${m30.spend.toFixed(2)}`;
            m30.rev = `$${m30.rev.toFixed(2)}`;
            m30.res = m30.res.toString();
            m30.cac = `$${m30.cac}`;

            // 6 Months (180 days)
            const m180 = brand.metrics.sixMonth;
            // For 6 months, we need aggregate spend from all 180-day campaigns
            let sixMonthSpend = 0;
            brand.campaigns.forEach((c: any) => {
                const cid = c.campaignId;
                if (data180Map[cid]) {
                    sixMonthSpend += data180Map[cid].spend;
                }
            });
            m180.roas = sixMonthSpend > 0 ? (sixMonthSpend * parseFloat(m180.roas || "0") / sixMonthSpend).toFixed(2) : "0.00";
            m180.cac = m180.res > 0 ? (sixMonthSpend / parseFloat(m180.res)).toFixed(2) : "0.00";
            m180.res = m180.res.toString();
            m180.cac = `$${m180.cac}`;
        });

        return Object.values(brands);
    };

    const data = useMemo(() => {
        return processData(rawApiData, activeTab);
    }, [rawApiData, activeTab]);

    const fetchSyncStatus = async () => {
        try {
            const res = await fetch(`${API_URL}/insights/sync-status`);
            if (!res.ok) return null;
            const json = await res.json();
            if (json && typeof json === 'object' && 'can_sync' in json) {
                setSyncStatus(json);
                return json;
            }
            return null;
        } catch (e) {
            console.error("Failed to fetch sync status", e);
            return null;
        }
    };

    // Cooldown countdown timer
    useEffect(() => {
        if (!syncStatus || syncStatus.can_sync) {
            setCooldownDisplay("");
            return;
        }

        const updateCountdown = () => {
            if (!syncStatus.next_free_at) return;
            const freeAt = new Date(syncStatus.next_free_at + "Z"); // UTC
            const now = new Date();
            const diffMs = freeAt.getTime() - now.getTime();

            if (diffMs <= 0) {
                setCooldownDisplay("");
                fetchSyncStatus(); // Re-check, a slot may have freed
                return;
            }

            const hours = Math.floor(diffMs / 3600000);
            const minutes = Math.floor((diffMs % 3600000) / 60000);
            const seconds = Math.floor((diffMs % 60000) / 1000);
            setCooldownDisplay(
                hours > 0 ? `${hours}h ${minutes}m ${seconds}s` : `${minutes}m ${seconds}s`
            );
        };

        updateCountdown();
        const interval = setInterval(updateCountdown, 1000);
        return () => clearInterval(interval);
    }, [syncStatus]);

    const handleSync = async () => {
        // Refresh status first
        const status = await fetchSyncStatus();
        if (status && !status.can_sync) {
            return; // UI will show the limit message
        }

        setIsLoading(true);

        // 1. Trigger the background sync
        console.log("Triggering sync...");
        try {
            const syncRes = await fetch(`${API_URL}/insights/sync`, { method: "POST" });
            if (syncRes.status === 429) {
                const errData = await syncRes.json();
                console.warn("Sync rate limited:", errData.detail);
                await fetchSyncStatus();
                setIsLoading(false);
                return;
            }
        } catch (e) {
            console.error("Sync trigger failed:", e);
            setIsLoading(false);
            return;
        }

        // 2. Poll DynamoDB every 2 seconds for 30 seconds to show live progress
        let pollCount = 0;
        const maxPolls = 15; // 15 polls * 2s = 30s max

        const pollInterval = setInterval(async () => {
            try {
                const res = await fetch(`${API_URL}/insights/all`);
                const json = await res.json();
                setRawApiData(json);
                console.log(`Poll ${pollCount + 1}: 7d=${json["7"]?.length || 0}, 30d=${json["30"]?.length || 0}, 180d=${json["180"]?.length || 0}`);
            } catch (e) {
                console.error("Poll failed:", e);
            }

            pollCount++;
            if (pollCount >= maxPolls) {
                clearInterval(pollInterval);
                setIsLoading(false);
                fetchSyncStatus(); // Refresh status after sync completes
                console.log("Sync polling complete");
            }
        }, 2000);

        // Safety: Stop after 32s regardless
        setTimeout(() => {
            clearInterval(pollInterval);
            setIsLoading(false);
            fetchSyncStatus();
        }, 32000);
    };

    useEffect(() => {
        // Initial load - Fetch all ranges from DB
        const loadInitialData = async () => {
            try {
                const res = await fetch(`${API_URL}/insights/all`);
                if (!res.ok) {
                    console.error(`API Error: ${res.status}`);
                    return;
                }
                const json = await res.json();
                if (json && typeof json === 'object') {
                    setRawApiData(json);
                }
            } catch (e) {
                console.error("Initial load failed", e);
            }
        };
        loadInitialData();
        fetchSyncStatus();
    }, []);

    return (
        <div className="p-8">
            {/* Header Tabs - Hidden when only one platform */}
            {platforms.length > 1 && (
                <div className="flex justify-center mb-8">
                    <div className="bg-slate-200/50 p-1 rounded-xl flex gap-1">
                        {platforms.map(p => (
                            <button
                                key={p}
                                onClick={() => setActiveTab(p)}
                                className={cn(
                                    "px-6 py-2 rounded-lg text-sm font-medium transition-all duration-200",
                                    activeTab === p
                                        ? "bg-white text-slate-900 shadow-sm"
                                        : "text-slate-500 hover:text-slate-700"
                                )}
                            >
                                {p}
                            </button>
                        ))}
                    </div>
                </div>
            )}

            <div className="mb-8 flex justify-between items-end">
                <div>
                    <h1 className="text-3xl font-bold text-slate-900 mb-2">Portfolio Performance</h1>
                    <p className="text-slate-500">Track and analyze your ad campaign performance across platforms</p>
                </div>
                <div className="flex items-center gap-3">
                    {syncStatus && !syncStatus.can_sync && (
                        <div className="flex items-center gap-2 px-4 py-2 bg-amber-50 border border-amber-200 rounded-xl text-sm">
                            <Clock size={14} className="text-amber-500" />
                            <span className="text-amber-700 font-medium">
                                Limit reached · Next sync in {cooldownDisplay || "..."}
                            </span>
                        </div>
                    )}
                    <button
                        onClick={handleSync}
                        disabled={isLoading || (syncStatus !== null && !syncStatus.can_sync)}
                        className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 rounded-xl text-sm font-semibold text-slate-600 hover:bg-slate-50 transition-all shadow-sm disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        <RefreshCcw size={16} className={cn(isLoading && "animate-spin")} />
                        {isLoading
                            ? "Syncing..."
                            : syncStatus
                                ? `Sync Data (${syncStatus.syncs_remaining}/${syncStatus.max_syncs})`
                                : "Sync Data"
                        }
                    </button>
                </div>
            </div>

            {/* Table Section */}
            <div className="bg-white rounded-3xl border border-slate-200 shadow-xl shadow-slate-200/50 overflow-hidden overflow-x-auto min-h-[400px]">
                {data.length > 0 ? (
                    <table className="w-full text-left border-collapse min-w-[1300px]">
                        <thead className="sticky top-0 z-10 shadow-sm">
                            <tr>
                                <th className="py-6 px-6 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 border-r border-slate-100">
                                    Account Name
                                </th>
                                <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center border-r border-slate-100/50 relative">
                                    <span className="relative z-10">Last 7 Days</span>
                                    <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                </th>
                                <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center border-r border-slate-100/50 relative">
                                    <span className="relative z-10">Previous Month</span>
                                    <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                </th>
                                <th colSpan={3} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center relative">
                                    <span className="relative z-10">6 Months Avg</span>
                                    <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                </th>
                            </tr>
                            <tr className="border-b border-slate-100 text-[10px] font-bold text-slate-400 uppercase tracking-widest bg-slate-50">
                                <th className="py-3 px-6 border-r border-slate-100/50 bg-slate-50"></th>
                                {/* Last 7 Days */}
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                {/* Prev Month */}
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                {/* 6 Months */}
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                <th className="py-3 px-4 text-center font-bold bg-slate-50">CAC</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data.map((brand) => (
                                <React.Fragment key={brand.brand}>
                                    <MetricRow
                                        label={brand.brand}
                                        isHeader
                                        isExpanded={expandedRows[brand.brand]}
                                        onToggle={() => toggleRow(brand.brand)}
                                        metrics={brand.metrics}
                                    />
                                    {expandedRows[brand.brand] && brand.campaigns.map((camp: any) => (
                                        <MetricRow
                                            key={camp.label}
                                            label={camp.label}
                                            level={1}
                                            metrics={camp.metrics}
                                        />
                                    ))}
                                </React.Fragment>
                            ))}
                        </tbody>
                    </table>
                ) : (
                    <div className="flex flex-col items-center justify-center h-[400px] text-slate-400 gap-4">
                        <div className="w-16 h-16 bg-slate-50 rounded-2xl flex items-center justify-center">
                            <AlertCircle size={32} className="opacity-20" />
                        </div>
                        <div className="text-center">
                            <p className="font-bold text-slate-900">No performance data found</p>
                            <p className="text-sm">Connect an account in Integrations and sync data to get started</p>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
