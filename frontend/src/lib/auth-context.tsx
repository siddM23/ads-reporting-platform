"use client";

import React, { createContext, useContext, useState, useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';

interface AuthContextType {
    token: string | null;
    user: { email: string } | null;
    login: (token: string, email: string) => void;
    logout: () => void;
    isAuthenticated: boolean;
    isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
    const [token, setToken] = useState<string | null>(null);
    const [user, setUser] = useState<{ email: string } | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const router = useRouter();
    const pathname = usePathname();

    useEffect(() => {
        // Load token from localStorage on mount
        const savedToken = localStorage.getItem('auth_token');
        const savedEmail = localStorage.getItem('user_email');

        if (savedToken && savedEmail) {
            setToken(savedToken);
            setUser({ email: savedEmail });
        }
        setIsLoading(false);
    }, []);

    const login = (newToken: string, email: string) => {
        setToken(newToken);
        setUser({ email });
        localStorage.setItem('auth_token', newToken);
        localStorage.setItem('user_email', email);
        router.push('/dash');
    };

    const logout = () => {
        setToken(null);
        setUser(null);
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user_email');
        router.push('/auth/login');
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
            login,
            logout,
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
