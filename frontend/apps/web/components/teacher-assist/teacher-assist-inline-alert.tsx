"use client";

import { useCallback, useState } from "react";

import {
  TeacherAssistAlert,
  type TeacherAssistAlertVariant,
} from "@/components/teacher-assist/teacher-assist-alert";

export type TeacherAssistSectionAlert = {
  type: TeacherAssistAlertVariant;
  title?: string;
  description?: React.ReactNode;
  actionLabel?: string;
  onAction?: () => void;
};

export function TeacherAssistInlineAlert({
  alert,
  onDismiss,
  className = "",
}: {
  alert?: TeacherAssistSectionAlert | null;
  onDismiss?: () => void;
  className?: string;
}) {
  if (!alert) return null;
  return (
    <TeacherAssistAlert
      variant={alert.type}
      title={alert.title}
      description={alert.description}
      actionLabel={alert.actionLabel}
      onAction={alert.onAction}
      onDismiss={onDismiss}
      className={`py-2 ${className}`.trim()}
    />
  );
}

export function useTeacherAssistSectionAlerts() {
  const [sectionAlerts, setSectionAlerts] = useState<Record<string, TeacherAssistSectionAlert | null>>({});

  const setSectionAlert = useCallback((key: string, alert: TeacherAssistSectionAlert | null) => {
    setSectionAlerts((current) => ({ ...current, [key]: alert }));
  }, []);

  const clearSectionAlert = useCallback((key: string) => {
    setSectionAlerts((current) => ({ ...current, [key]: null }));
  }, []);

  const getSectionAlert = useCallback(
    (key: string) => sectionAlerts[key] ?? null,
    [sectionAlerts],
  );

  return { sectionAlerts, setSectionAlert, clearSectionAlert, getSectionAlert };
}

export function sectionSuccess(description: string, title?: string): TeacherAssistSectionAlert {
  return { type: "success", title, description };
}

export function sectionError(
  description: string,
  title = "Unable to save",
): TeacherAssistSectionAlert {
  return { type: "error", title, description };
}

export function sectionWarning(description: string, title?: string): TeacherAssistSectionAlert {
  return { type: "warning", title, description };
}
