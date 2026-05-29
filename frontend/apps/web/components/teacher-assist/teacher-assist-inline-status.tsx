"use client";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";

export function TeacherAssistInlineStatus({
  variant = "success",
  message,
  onDismiss,
}: {
  variant?: "success" | "info";
  message?: string | null;
  onDismiss?: () => void;
}) {
  if (!message) return null;
  return (
    <TeacherAssistAlert
      variant={variant}
      description={message}
      onDismiss={onDismiss}
      className="py-2"
    />
  );
}
