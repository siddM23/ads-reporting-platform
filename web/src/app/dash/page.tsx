"use client";

import React, { useState, useEffect, useMemo } from "react";
import { ChevronDown, ChevronRight, AlertCircle, RefreshCcw, Clock } from "lucide-react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { cn } from "@/lib/utils";
import { authFetch, useAuth } from "@/lib/auth-context";
import { DateFilter } from "@/components/ui/date-filter";
import { Badge } from "@/components/ui/badge";
import { Spinner } from "@/components/ui/spinner";

import { format } from "date-fns";

interface MetricRowProps {
    label: string;
    isHeader?: boolean;
    isExpanded?: boolean;
    onToggle?: () => void;
    level?: number;
    metrics: {
        last7?: { spend: string; roas: string; rev: string; res: string; cac: string };
        prevMonth?: { spend: string; roas: string; rev: string; res: string; cac: string };
        sixMonth?: { spend: string; roas: string; rev: string; res: string; cac: string };
        custom?: { spend: string; roas: string; rev: string; res: string; cac: string };
    };
    selectedRange?: string;
}

const MetricRow: React.FC<MetricRowProps> = ({
    label,
    isHeader = false,
    isExpanded = false,
    onToggle,
    level = 0,
    metrics,
    selectedRange = "All Ranges"
}) => {
    const isCampaign = level > 0;
    const showAll = selectedRange === "All Ranges";
    const showLast7 = showAll || selectedRange === "Last 7 days";
    const showPrevMonth = showAll || selectedRange === "Last 30 days";
    const showSixMonth = showAll || selectedRange === "Last 6 months";
    const showCustom = selectedRange === "Custom";

    // Reusable cell renderer for consistent styling
    const renderCell = (value: string, type: 'text' | 'roas' | 'cac' | 'currency' | 'number', isLight = false) => {
        let content: React.ReactNode = value;
        let className = "py-4 px-4 text-center text-sm font-medium text-slate-900";

        if (type === 'roas') {
            const numVal = parseFloat(value);
            const isBad = !isNaN(numVal) && numVal < 1;
            return (
                <td className="py-4 px-4 text-center text-sm font-medium">
                    <span className={cn(
                        "px-2 py-1 rounded-lg text-[13px] font-bold",
                        isBad ? "text-red-500 bg-red-50" : "text-slate-900"
                    )}>
                        {value}
                    </span>
                </td>
            );
        }

        if (type === 'cac') {
            return (
                <td className="py-4 px-4 text-center text-sm font-medium border-r border-slate-100/50">
                    <span className="px-2 py-1 rounded-lg text-[13px] font-bold text-slate-900">
                        {value}
                    </span>
                </td>
            );
        }

        if (isLight) {
            className += " text-slate-400 font-light";
        }

        return <td className={className}>{content}</td>;
    };

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

            {/* Custom Range */}
            {showCustom && metrics.custom && (
                <>
                    {renderCell(metrics.custom.spend, 'currency')}
                    {renderCell(metrics.custom.roas, 'roas')}
                    {renderCell(metrics.custom.rev, 'currency')}
                    {renderCell(metrics.custom.res, 'number', true)}
                    {renderCell(metrics.custom.cac, 'cac')}
                </>
            )}

            {/* Last 7 Days */}
            {showLast7 && metrics.last7 && (
                <>
                    {renderCell(metrics.last7.spend, 'currency')}
                    {renderCell(metrics.last7.roas, 'roas')}
                    {renderCell(metrics.last7.rev, 'currency')}
                    {renderCell(metrics.last7.res, 'number', true)}
                    {renderCell(metrics.last7.cac, 'cac')}
                </>
            )}

            {/* Previous Month */}
            {showPrevMonth && metrics.prevMonth && (
                <>
                    {renderCell(metrics.prevMonth.spend, 'currency')}
                    {renderCell(metrics.prevMonth.roas, 'roas')}
                    {renderCell(metrics.prevMonth.rev, 'currency')}
                    {renderCell(metrics.prevMonth.res, 'number', true)}
                    {renderCell(metrics.prevMonth.cac, 'cac')}
                </>
            )}

            {/* 6 Months Avg */}
            {showSixMonth && metrics.sixMonth && (
                <>
                    {renderCell(metrics.sixMonth.spend, 'currency')}
                    {renderCell(metrics.sixMonth.roas, 'roas')}
                    {renderCell(metrics.sixMonth.rev, 'currency')}
                    {renderCell(metrics.sixMonth.res, 'number', true)}
                    {renderCell(metrics.sixMonth.cac, 'cac')}
                </>
            )}
        </tr>
    );
};

const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";

export default function DashPage() {
    const { preferences, updatePreferences } = useAuth();
    const [activeTab, setActiveTab] = useState("Meta Ads");
    const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
    const [cooldownDisplay, setCooldownDisplay] = useState("");
    const [isSyncing, setIsSyncing] = useState(false);

    // Initialize from preferences if available, else defaults
    const [selectedRange, setSelectedRange] = useState<string>(preferences?.selected_label || "Last 7 days");
    const [customStart, setCustomStart] = useState<string | undefined>(preferences?.custom_range?.start);
    const [customEnd, setCustomEnd] = useState<string | undefined>(preferences?.custom_range?.end);

    // Sync state when preferences load (e.g. after login)
    useEffect(() => {
        if (preferences) {
            if (preferences.selected_label) setSelectedRange(preferences.selected_label);
            if (preferences.custom_range) {
                setCustomStart(preferences.custom_range.start);
                setCustomEnd(preferences.custom_range.end);
            }
        }
    }, [preferences]);

    const queryClient = useQueryClient();

    // Fetch Insights Data
    const { data: rawApiData, isLoading: isDataLoading, refetch: refetchData } = useQuery({
        queryKey: ["insights", selectedRange, customStart, customEnd],
        queryFn: async () => {
            if (selectedRange === "Custom" && customStart && customEnd) {
                const res = await authFetch(`${API_URL}/insights/custom?start_date=${customStart}&end_date=${customEnd}`);
                if (!res.ok) throw new Error("Failed to fetch custom data");
                return res.json();
            } else {
                const res = await authFetch(`${API_URL}/insights/all`);
                if (!res.ok) throw new Error("Failed to fetch data");
                return res.json();
            }
        },
        staleTime: 1000 * 60 * 5, // 5 minutes
    });

    const handleRangeChange = (range: string, start?: Date, end?: Date) => {
        setSelectedRange(range);

        const newPrefs: any = { selected_label: range };

        if (range === "Custom" && start && end) {
            const s = format(start, "yyyy-MM-dd");
            const e = format(end, "yyyy-MM-dd");
            setCustomStart(s);
            setCustomEnd(e);
            newPrefs.custom_range = { start: s, end: e };
        } else {
            setCustomStart(undefined);
            setCustomEnd(undefined);
            if (range !== "Custom") newPrefs.custom_range = null;
        }

        updatePreferences(newPrefs);
    };

    // Fetch Sync Status
    const { data: syncStatus, refetch: refetchSyncStatus } = useQuery({
        queryKey: ["sync-status"],
        queryFn: async () => {
            const res = await authFetch(`${API_URL}/insights/sync-status`);
            if (!res.ok) return null;
            return res.json();
        },
        refetchInterval: 1000 * 30, // Check every 30s
    });

    const toggleRow = (label: string) => {
        setExpandedRows(prev => ({ ...prev, [label]: !prev[label] }));
    };

    const platforms = ["Meta Ads", "Google Ads"];

    // Transform API data
    const processData = (apiData: any, platformFilter: string) => {
        const brands: Record<string, any> = {};
        const targetPlatform = platformFilter.toLowerCase().includes("meta") ? "meta" : "google";

        if (!apiData) return [];

        // Helper to parse Meta/Google nested action lists
        const getActionValue = (list: any[], actionType: string) => {
            if (!list) return 0;
            const item = list.find((x: any) => x.action_type === actionType);
            return item ? parseFloat(item.value) : 0;
        };

        const extractMetrics = (row: any) => {
            const spend = parseFloat(row.spend || "0");
            const revenue = getActionValue(row.action_values, "purchase") || getActionValue(row.action_values, "omni_purchase") || getActionValue(row.action_values, "conversions_value") || 0;
            const results = getActionValue(row.actions, "purchase") || getActionValue(row.actions, "omni_purchase") || getActionValue(row.actions, "conversions") || 0;
            const roas = spend > 0 ? (revenue / spend) : 0;
            const cac = results > 0 ? (spend / results) : 0;
            return { spend, revenue, results, roas, cac };
        };

        const formatMetrics = (m: any) => ({
            spend: `$${m.spend.toFixed(2)}`,
            roas: m.roas.toFixed(2),
            rev: `$${m.revenue.toFixed(2)}`,
            res: m.results.toFixed(0),
            cac: `$${m.cac.toFixed(2)}`
        });

        // Check if we have standard ranges or a custom list
        const isCustom = Array.isArray(apiData);

        if (isCustom) {
            // Process Custom Range Data (flat list)
            apiData.forEach((row: any) => {
                if (row.platform !== targetPlatform) return;
                const brandName = row.account_name || "Unknown Account";

                const m = extractMetrics(row);

                // Skip campaign if spend is 0
                if (m.spend <= 0) return;

                if (!brands[brandName]) {
                    brands[brandName] = {
                        brand: brandName,
                        metrics: { custom: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 } },
                        campaigns: []
                    };
                }

                brands[brandName].campaigns.push({
                    label: row.campaign_name,
                    metrics: { custom: formatMetrics(m) }
                });

                // Aggregate
                const agg = brands[brandName].metrics.custom;
                agg.spend += m.spend;
                agg.rev += m.revenue;
                agg.res += m.results;
            });

            // Finalize Aggregates
            Object.values(brands).forEach((brand: any) => {
                const m = brand.metrics.custom;
                m.roas = m.spend > 0 ? (m.rev / m.spend) : 0;
                m.cac = m.res > 0 ? (m.spend / m.res) : 0;
                brand.metrics.custom = formatMetrics({ spend: m.spend, revenue: m.rev, results: m.res, roas: m.roas, cac: m.cac });
            });

        } else {
            // Process Standard Ranges (7, 30, 180)
            // Build lookup maps
            const data30Map: Record<string, any> = {};
            const data180Map: Record<string, any> = {};
            (apiData["30"] || []).forEach((row: any) => data30Map[row.campaign_id] = extractMetrics(row));
            (apiData["180"] || []).forEach((row: any) => data180Map[row.campaign_id] = extractMetrics(row));

            (apiData["7"] || []).forEach((row: any) => {
                if (row.platform !== targetPlatform) return;
                const brandName = row.account_name || "Unknown Account";

                const m7 = extractMetrics(row);
                const m30 = data30Map[row.campaign_id] || { spend: 0, revenue: 0, results: 0, roas: 0, cac: 0 };
                const m180 = data180Map[row.campaign_id] || { spend: 0, revenue: 0, results: 0, roas: 0, cac: 0 };

                // Determine if we should show this campaign based on spend in the selected range
                let shouldShow = false;
                if (selectedRange === "All Ranges") {
                    shouldShow = m7.spend > 0 || m30.spend > 0 || m180.spend > 0;
                } else if (selectedRange === "Last 7 days") {
                    shouldShow = m7.spend > 0;
                } else if (selectedRange === "Last 30 days") {
                    shouldShow = m30.spend > 0;
                } else if (selectedRange === "Last 6 months") {
                    shouldShow = m180.spend > 0;
                }

                if (!shouldShow) return;

                if (!brands[brandName]) {
                    brands[brandName] = {
                        brand: brandName,
                        metrics: {
                            last7: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 },
                            prevMonth: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 },
                            sixMonth: { spend: 0, roas: 0, rev: 0, res: 0, cac: 0 }
                        },
                        campaigns: []
                    };
                }

                brands[brandName].campaigns.push({
                    label: row.campaign_name,
                    metrics: {
                        last7: formatMetrics(m7),
                        prevMonth: formatMetrics(m30),
                        sixMonth: formatMetrics(m180)
                    }
                });

                // Aggregate
                const agg = brands[brandName].metrics;

                agg.last7.spend += m7.spend;
                agg.last7.rev += m7.revenue;
                agg.last7.res += m7.results;

                agg.prevMonth.spend += m30.spend;
                agg.prevMonth.rev += m30.revenue;
                agg.prevMonth.res += m30.results;

                agg.sixMonth.res += m180.results;
                agg.sixMonth.spend += m180.spend;
                agg.sixMonth.rev += m180.revenue;
            });

            // Finalize Aggregates
            Object.values(brands).forEach((brand: any) => {
                const m7 = brand.metrics.last7;
                m7.roas = m7.spend > 0 ? (m7.rev / m7.spend) : 0;
                m7.cac = m7.res > 0 ? (m7.spend / m7.res) : 0;
                brand.metrics.last7 = formatMetrics({ spend: m7.spend, revenue: m7.rev, results: m7.res, roas: m7.roas, cac: m7.cac });

                const m30 = brand.metrics.prevMonth;
                m30.roas = m30.spend > 0 ? (m30.rev / m30.spend) : 0;
                m30.cac = m30.res > 0 ? (m30.spend / m30.res) : 0;
                brand.metrics.prevMonth = formatMetrics({ spend: m30.spend, revenue: m30.rev, results: m30.res, roas: m30.roas, cac: m30.cac });

                const m180 = brand.metrics.sixMonth;
                m180.roas = m180.spend > 0 ? (m180.rev / m180.spend) : 0;
                m180.cac = m180.res > 0 ? (m180.spend / m180.res) : 0;
                brand.metrics.sixMonth = formatMetrics({ spend: m180.spend, revenue: m180.rev, results: m180.res, roas: m180.roas, cac: m180.cac });
            });
        }

        return Object.values(brands);
    };

    const data = useMemo(() => {
        return processData(rawApiData, activeTab);
    }, [rawApiData, activeTab]);

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
                refetchSyncStatus(); // Re-check, a slot may have freed
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
    }, [syncStatus, refetchSyncStatus]);

    const handleSync = async () => {
        // Refresh status first
        const { data: status } = await refetchSyncStatus();
        if (status && !status.can_sync) {
            return; // UI will show the limit message
        }

        setIsSyncing(true);

        // 1. Trigger the background sync
        console.log("Triggering sync...");
        try {
            const syncRes = await authFetch(`${API_URL}/insights/sync`, { method: "POST" });
            if (syncRes.status === 429) {
                const errData = await syncRes.json();
                console.warn("Sync rate limited:", errData.detail);
                await refetchSyncStatus();
                setIsSyncing(false);
                return;
            }
        } catch (e) {
            console.error("Sync trigger failed:", e);
            setIsSyncing(false);
            return;
        }

        // 2. Poll every 2 seconds for 30 seconds to show live progress
        let pollCount = 0;
        const maxPolls = 15; // 15 polls * 2s = 30s max

        const pollInterval = setInterval(async () => {
            try {
                // Determine if we should really Refetch or just invalidate. 
                // Refetch gives us the new data to stick in state if we were using state,
                // but here useQuery handles it. Calling refetchData() updates the cache.
                await refetchData();
            } catch (e) {
                console.error("Poll failed:", e);
            }

            pollCount++;
            if (pollCount >= maxPolls) {
                clearInterval(pollInterval);
                setIsSyncing(false);
                refetchSyncStatus();
                console.log("Sync polling complete");
            }
        }, 2000);

        // Safety: Stop after 32s regardless
        setTimeout(() => {
            clearInterval(pollInterval);
            setIsSyncing(false);
            refetchSyncStatus();
        }, 32000);
    };

    // Fetch Integrations to check for re-auth needed
    const { data: brokenIntegrations } = useQuery({
        queryKey: ["broken-integrations"],
        queryFn: async () => {
            const res = await authFetch(`${API_URL}/integrations`);
            if (!res.ok) return [];
            const data = await res.json();
            return Array.isArray(data) ? data.filter((acc: any) => acc.needs_reauth) : [];
        },
    });

    return (
        <div className="p-8">
            {brokenIntegrations && brokenIntegrations.length > 0 && (
                <div className="mb-8 bg-red-50 border border-red-200 rounded-2xl p-4 flex items-center justify-between shadow-sm">
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 bg-red-100 text-red-600 rounded-full flex items-center justify-center shrink-0">
                            <AlertCircle size={20} />
                        </div>
                        <div>
                            <h3 className="text-sm font-bold text-red-900">Action Required</h3>
                            <p className="text-sm text-red-700">
                                {brokenIntegrations.length} {brokenIntegrations.length === 1 ? "account needs" : "accounts need"} re-authentication to continue syncing data.
                            </p>
                        </div>
                    </div>
                    <a
                        href="/integrations"
                        className="px-4 py-2 bg-red-600 text-white text-sm font-bold rounded-xl hover:bg-red-700 transition-colors shadow-md shadow-red-200 whitespace-nowrap"
                    >
                        Resolving Issues
                    </a>
                </div>
            )}

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
                    <div className="flex items-center gap-4 mb-2">
                        <h1 className="text-3xl font-bold text-slate-900">Portfolio Performance</h1>
                        {isSyncing && (
                            <Badge className="gap-2 bg-indigo-50 text-indigo-600 border-indigo-100 hover:bg-indigo-50">
                                <Spinner size={12} className="text-indigo-600" />
                                Processing
                            </Badge>
                        )}
                        {isDataLoading && (
                            <Badge className="gap-2 bg-indigo-50 text-indigo-600 border-indigo-100 hover:bg-indigo-50">
                                <Spinner size={12} className="text-indigo-600" />
                                Syncing
                            </Badge>
                        )}
                    </div>
                    <p className="text-slate-500">Track and analyze your ad campaign performance across platforms</p>
                </div>
                <div className="flex items-center gap-3">
                    {syncStatus && typeof syncStatus === 'object' && !syncStatus.can_sync && (
                        <div className="flex items-center gap-2 px-4 py-2 bg-amber-50 border border-amber-200 rounded-xl text-sm">
                            <Clock size={14} className="text-amber-500" />
                            <span className="text-amber-700 font-medium whitespace-nowrap">
                                Limit reached · {cooldownDisplay || "..."}
                            </span>
                        </div>
                    )}
                    <DateFilter
                        onRangeChange={handleRangeChange}
                        className="w-auto"
                    />
                    <button
                        onClick={handleSync}
                        disabled={isSyncing || (syncStatus && typeof syncStatus === 'object' && !syncStatus.can_sync)}
                        className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 rounded-xl text-sm font-semibold text-slate-600 hover:bg-slate-50 transition-all shadow-sm disabled:opacity-50 disabled:cursor-not-allowed h-10"
                    >
                        <RefreshCcw size={16} className={cn(isSyncing && "animate-spin")} />
                        {isSyncing
                            ? "Syncing..."
                            : (syncStatus && typeof syncStatus === 'object')
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
                                {(selectedRange === "All Ranges" || selectedRange === "Last 7 days") && (
                                    <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center border-r border-slate-100/50 relative">
                                        <span className="relative z-10">Last 7 Days</span>
                                        <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                    </th>
                                )}
                                {(selectedRange === "Custom") && (
                                    <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center border-r border-slate-100/50 relative">
                                        <span className="relative z-10">
                                            {customStart && customEnd ? `${format(new Date(customStart), 'MMM d')} - ${format(new Date(customEnd), 'MMM d')}` : 'Custom Range'}
                                        </span>
                                        <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                    </th>
                                )}
                                {(selectedRange === "All Ranges" || selectedRange === "Last 30 days") && (
                                    <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center border-r border-slate-100/50 relative">
                                        <span className="relative z-10">Previous Month</span>
                                        <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                    </th>
                                )}
                                {(selectedRange === "All Ranges" || selectedRange === "Last 6 months") && (
                                    <th colSpan={5} className="py-4 px-4 bg-slate-50 text-[11px] font-bold uppercase tracking-[0.2em] text-slate-400 text-center relative">
                                        <span className="relative z-10">6 Months Avg</span>
                                        <div className="absolute inset-x-4 bottom-2 h-[1px] bg-slate-200/50"></div>
                                    </th>
                                )}
                            </tr>
                            <tr className="border-b border-slate-100 text-[10px] font-bold text-slate-400 uppercase tracking-widest bg-slate-50">
                                <th className="py-3 px-6 border-r border-slate-100/50 bg-slate-50"></th>
                                {/* Last 7 Days */}
                                {(selectedRange === "All Ranges" || selectedRange === "Last 7 days") && (
                                    <>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                        <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                    </>
                                )}
                                {/* Custom Range */}
                                {(selectedRange === "Custom") && (
                                    <>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                        <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                    </>
                                )}
                                {/* Prev Month */}
                                {(selectedRange === "All Ranges" || selectedRange === "Last 30 days") && (
                                    <>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                        <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                    </>
                                )}
                                {/* 6 Months */}
                                {(selectedRange === "All Ranges" || selectedRange === "Last 6 months") && (
                                    <>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Spends</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">ROAS</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Revenue</th>
                                        <th className="py-3 px-4 text-center font-bold bg-slate-50">Results</th>
                                        <th className="py-3 px-4 text-center border-r border-slate-100/50 font-bold bg-slate-50">CAC</th>
                                    </>
                                )}
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
                                        selectedRange={selectedRange}
                                    />
                                    {expandedRows[brand.brand] && brand.campaigns.map((camp: any) => (
                                        <MetricRow
                                            key={camp.label}
                                            label={camp.label}
                                            level={1}
                                            metrics={camp.metrics}
                                            selectedRange={selectedRange}
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
