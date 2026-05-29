"use client";

import Link from "next/link";

export type TeacherAssistAlertVariant = "info" | "success" | "warning" | "error";

const VARIANT_CLASS: Record<TeacherAssistAlertVariant, string> = {
  info: "ta-alert-info",
  success: "ta-alert-success",
  warning: "ta-alert-warning",
  error: "ta-alert-error",
};

export function TeacherAssistAlert({
  variant = "info",
  title,
  description,
  actionLabel,
  actionHref,
  onAction,
  onDismiss,
  className = "",
  children,
}: {
  variant?: TeacherAssistAlertVariant;
  title?: string;
  description?: React.ReactNode;
  actionLabel?: string;
  actionHref?: string;
  onAction?: () => void;
  className?: string;
  onDismiss?: () => void;
  children?: React.ReactNode;
}) {
  const body = children ?? description;

  return (
    <section className={`ta-alert ${VARIANT_CLASS[variant]} ${className}`.trim()} role="alert">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          {title ? <p className="font-semibold">{title}</p> : null}
          {body ? <div className={title ? "mt-1 leading-6" : "leading-6"}>{body}</div> : null}
          {actionLabel && actionHref ? (
            <Link href={actionHref} className="mt-2 inline-block text-sm font-semibold underline">
              {actionLabel}
            </Link>
          ) : null}
          {actionLabel && onAction && !actionHref ? (
            <button type="button" onClick={onAction} className="mt-2 text-sm font-semibold underline">
              {actionLabel}
            </button>
          ) : null}
        </div>
        {onDismiss ? (
          <button
            type="button"
            onClick={onDismiss}
            className="shrink-0 rounded-lg px-2 py-1 text-xs font-semibold text-slate-500 hover:bg-white/60"
            aria-label="Dismiss"
          >
            Dismiss
          </button>
        ) : null}
      </div>
    </section>
  );
}
