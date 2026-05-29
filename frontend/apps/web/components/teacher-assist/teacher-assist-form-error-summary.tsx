import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";

export function TeacherAssistFormErrorSummary({
  title = "Please correct the highlighted fields",
  message,
}: {
  title?: string;
  message?: string | null;
}) {
  if (!message) return null;
  return <TeacherAssistAlert variant="error" title={title} description={message} />;
}
