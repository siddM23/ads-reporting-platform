"use client";

import React, { createContext, useContext, useState, useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';

interface AuthContextType {
    token: string | null;
    user: { email: string } | null;
    preferences: { custom_range?: { start: string, end: string }, selected_label?: string } | null;
    login: (token: string, email: string, preferences?: any) => void;
    logout: () => void;
    updatePreferences: (newPrefs: any) => void;
    isAuthenticated: boolean;
    isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
    const [token, setToken] = useState<string | null>(null);
    const [user, setUser] = useState<{ email: string } | null>(null);
    const [preferences, setPreferences] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(true);
    const router = useRouter();
    const pathname = usePathname();

    useEffect(() => {
        const savedToken = localStorage.getItem('auth_token');
        const savedEmail = localStorage.getItem('user_email');
        const savedPrefs = localStorage.getItem('user_prefs');

        if (savedToken && savedEmail) {
            setToken(savedToken);
            setUser({ email: savedEmail });
            if (savedPrefs) {
                try {
                    setPreferences(JSON.parse(savedPrefs));
                } catch (e) {
                    console.error("Failed to parse saved preferences", e);
                }
            }
        }
        setIsLoading(false);
    }, []);

    const login = (newToken: string, email: string, newPrefs?: any) => {
        setToken(newToken);
        setUser({ email });
        localStorage.setItem('auth_token', newToken);
        localStorage.setItem('user_email', email);

        if (newPrefs) {
            setPreferences(newPrefs);
            localStorage.setItem('user_prefs', JSON.stringify(newPrefs));
        }

        router.push('/dash');
    };

    const logout = () => {
        setToken(null);
        setUser(null);
        setPreferences(null);
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user_email');
        localStorage.removeItem('user_prefs');
        router.push('/auth/login');
    };

    const updatePreferences = async (newPrefs: any) => {
        // Optimistic update
        const updated = { ...preferences, ...newPrefs };
        setPreferences(updated);
        localStorage.setItem('user_prefs', JSON.stringify(updated));

        // Sync with backend
        try {
            const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";
            await authFetch(`${API_URL}/user/preferences`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(newPrefs)
            });
        } catch (e) {
            console.error("Failed to sync preferences", e);
        }
    };

    // Auto-redirect if not logged in
    useEffect(() => {
        if (!isLoading) {
            const isAuthPage = pathname.startsWith('/auth');
            if (!token && !isAuthPage) {
                router.push('/auth/login');
            } else if (token && isAuthPage) {
                router.push('/dash');
            }
        }
    }, [token, isLoading, pathname, router]);

    return (
        <AuthContext.Provider value={{
            token,
            user,
            preferences,
            login,
            logout,
            updatePreferences,
            isAuthenticated: !!token,
            isLoading
        }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
}

/**
 * Utility to fetch with the auth token automatically added.
 */
export async function authFetch(url: string, options: RequestInit = {}) {
    const token = localStorage.getItem('auth_token');

    const headers = new Headers(options.headers || {});
    if (token) {
        headers.set('Authorization', `Bearer ${token}`);
    }

    const response = await fetch(url, {
        ...options,
        headers,
    });

    if (response.status === 401 || response.status === 403) {
        // Token expired or invalid
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user_email');
        window.location.href = '/auth/login';
    }

    return response;
}
