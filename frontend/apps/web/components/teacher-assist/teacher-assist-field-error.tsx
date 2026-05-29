export function TeacherAssistFieldError({ message }: { message?: string | null }) {
  if (!message) return null;
  return <p className="ta-field-error">{message}</p>;
}

export function fieldErrorInputClass(hasError: boolean) {
  return hasError ? "ta-input ta-input-error" : "ta-input";
}
