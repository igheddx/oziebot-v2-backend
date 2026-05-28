"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

import { useAuth } from "@/components/providers/auth-provider";
import { productKeyForPathname, routeForProductKey } from "@/lib/products";

export function AppSwitcher() {
  const pathname = usePathname();
  const router = useRouter();
  const { products, defaultProduct, setDefaultProduct } = useAuth();
  const [open, setOpen] = useState(false);
  const [pendingDefaultKey, setPendingDefaultKey] = useState<string | null>(null);

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  const currentProduct = useMemo(() => {
    const activeKey = productKeyForPathname(pathname);
    return (
      products.find((product) => product.product_key === activeKey) ??
      products.find((product) => product.product_key === defaultProduct) ??
      products[0] ??
      null
    );
  }, [defaultProduct, pathname, products]);

  if (products.length < 2 || currentProduct == null) return null;

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="inline-flex h-11 items-center gap-2 rounded-2xl border border-border bg-card/85 px-3 text-sm font-semibold text-foreground shadow-sm transition hover:bg-card"
      >
        <span className="text-xs uppercase tracking-[0.18em] text-muted">App</span>
        <span>{currentProduct.display_name}</span>
        <span className="text-muted">{open ? "−" : "+"}</span>
      </button>
      {open ? (
        <div className="absolute right-0 z-50 mt-2 w-80 rounded-3xl border border-border bg-card p-3 shadow-[0_24px_60px_rgba(2,6,23,0.28)]">
          <p className="px-2 text-[11px] font-semibold uppercase tracking-[0.2em] text-muted">
            Available apps
          </p>
          <div className="mt-2 space-y-2">
            {products.map((product) => {
              const isCurrent = product.product_key === currentProduct.product_key;
              const isPending = pendingDefaultKey === product.product_key;
              return (
                <div key={product.product_key} className="rounded-2xl border border-border/80 bg-background/60 p-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-sm font-semibold text-foreground">{product.display_name}</p>
                      <p className="mt-1 text-xs text-muted">
                        {product.status === "trial" ? "Trial access" : "Active access"}
                        {product.is_default ? " · default app" : ""}
                        {isCurrent ? " · current app" : ""}
                      </p>
                    </div>
                    {product.is_default ? (
                      <span className="rounded-full border border-emerald-300/60 bg-emerald-500/10 px-2 py-1 text-[11px] font-semibold text-emerald-600">
                        Default
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-3 flex gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        setOpen(false);
                        router.push(routeForProductKey(product.product_key));
                      }}
                      className={`inline-flex h-10 items-center rounded-xl px-3 text-sm font-semibold transition ${
                        isCurrent
                          ? "border border-border bg-card text-foreground"
                          : "bg-sky-500 text-slate-950 hover:bg-sky-400"
                      }`}
                    >
                      {isCurrent ? "Already open" : "Open app"}
                    </button>
                    {!product.is_default ? (
                      <button
                        type="button"
                        disabled={isPending}
                        onClick={() => {
                          setPendingDefaultKey(product.product_key);
                          void setDefaultProduct(product.product_key).finally(() => {
                            setPendingDefaultKey(null);
                          });
                        }}
                        className="inline-flex h-10 items-center rounded-xl border border-border px-3 text-sm font-semibold text-muted transition hover:bg-card disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {isPending ? "Saving..." : "Set default"}
                      </button>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}
