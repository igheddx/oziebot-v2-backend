"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

import type { BillingSummary } from "@/lib/dashboard-types";
import type { CoinbaseStatus, SessionProduct, SessionUser, UserRole } from "@/lib/auth-types";
import {
  clearStoredSession,
  fetchSessionBootstrap,
  hasStoredTokens,
  login,
  logout,
  updateDefaultProduct,
} from "@/lib/auth-service";
import { fetchTeacherAssistV2Context } from "@/lib/teacher-assist-v2-api";
import { productKeyForPathname, routeForProductKey } from "@/lib/products";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

type AuthContextValue = {
  status: AuthStatus;
  user: SessionUser | null;
  role: UserRole | null;
  billing: BillingSummary | null;
  coinbase: CoinbaseStatus | null;
  products: SessionProduct[];
  defaultProduct: string | null;
  loginWithPassword: (email: string, password: string) => Promise<void>;
  setDefaultProduct: (productKey: string) => Promise<void>;
  logoutUser: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function isPublicPath(pathname: string): boolean {
  return pathname === "/login" || pathname.startsWith("/login/");
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<SessionUser | null>(null);
  const [billing, setBilling] = useState<BillingSummary | null>(null);
  const [coinbase, setCoinbase] = useState<CoinbaseStatus | null>(null);

  const hydrateSession = useCallback(async () => {
    const bootstrap = await fetchSessionBootstrap();
    setUser(bootstrap.user);
    setBilling(bootstrap.billing);
    setCoinbase(bootstrap.coinbase);
    setStatus("authenticated");
    return bootstrap;
  }, []);

  useEffect(() => {
    let active = true;

    const run = async () => {
      if (!hasStoredTokens()) {
        if (!active) return;
        setStatus("unauthenticated");
        return;
      }

      try {
        await hydrateSession();
      } catch {
        if (!active) return;
        clearStoredSession();
        setStatus("unauthenticated");
        setUser(null);
        setBilling(null);
        setCoinbase(null);
      }
    };

    run();
    return () => {
      active = false;
    };
  }, [hydrateSession]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const onSessionCleared = () => {
      setStatus("unauthenticated");
      setUser(null);
      setBilling(null);
      setCoinbase(null);
    };
    window.addEventListener("oziebot:session-cleared", onSessionCleared);
    return () => {
      window.removeEventListener("oziebot:session-cleared", onSessionCleared);
    };
  }, []);

  useEffect(() => {
    if (status === "loading") return;
    const publicPath = isPublicPath(pathname);
    const isAdminPath = pathname.startsWith("/admin");
    const routeProductKey = productKeyForPathname(pathname);
    const products = user?.products ?? [];
    const defaultProduct =
      user?.default_product ??
      products.find((product) => product.is_default)?.product_key ??
      products[0]?.product_key ??
      null;
    const productKeys = new Set(products.map((product) => product.product_key));
    const defaultRoute = routeForProductKey(defaultProduct);

    if (status === "unauthenticated" && !publicPath) {
      router.replace("/login");
      return;
    }
    if (status === "authenticated" && isAdminPath && user?.role !== "root_admin") {
      router.replace(defaultRoute);
      return;
    }
    if (status === "authenticated" && (publicPath || pathname === "/")) {
      router.replace(defaultRoute);
      return;
    }
    if (status === "authenticated" && routeProductKey && products.length > 0 && !productKeys.has(routeProductKey)) {
      router.replace(defaultRoute);
    }
  }, [pathname, router, status, user]);

  const loginWithPassword = useCallback(
    async (email: string, password: string) => {
      await login(email, password);
      const bootstrap = await hydrateSession();
      const defaultProduct =
        bootstrap.user.default_product ??
        bootstrap.user.products.find((product) => product.is_default)?.product_key ??
        bootstrap.user.products[0]?.product_key ??
        null;
      if (defaultProduct === "teacher_assist") {
        try {
          const context = await fetchTeacherAssistV2Context();
          router.replace(context.landing_route);
          return;
        } catch {
          /* fall through to product route */
        }
      }
      router.replace(routeForProductKey(defaultProduct));
    },
    [hydrateSession, router],
  );

  const setDefaultProduct = useCallback(async (productKey: string) => {
    const payload = await updateDefaultProduct(productKey);
    setUser((current) =>
      current
        ? {
            ...current,
            products: payload.products,
            default_product: payload.default_product,
          }
        : current,
    );
  }, []);

  const logoutUser = useCallback(async () => {
    await logout();
    setStatus("unauthenticated");
    setUser(null);
    setBilling(null);
    setCoinbase(null);
    router.replace("/login");
  }, [router]);

  const value = useMemo<AuthContextValue>(
    () => ({
      status,
      user,
      role: user?.role ?? null,
      billing,
      coinbase,
      products: user?.products ?? [],
      defaultProduct:
        user?.default_product ??
        user?.products.find((product) => product.is_default)?.product_key ??
        user?.products[0]?.product_key ??
        null,
      loginWithPassword,
      setDefaultProduct,
      logoutUser,
    }),
    [billing, coinbase, loginWithPassword, logoutUser, setDefaultProduct, status, user],
  );

  if (status === "loading" && !isPublicPath(pathname)) {
    return (
      <div className="mx-auto flex min-h-dvh max-w-lg items-center justify-center px-6 text-sm text-muted">
        Loading session...
      </div>
    );
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
