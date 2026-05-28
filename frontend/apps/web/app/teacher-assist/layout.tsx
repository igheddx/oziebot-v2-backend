import { TeacherAssistShell } from "@/components/teacher-assist/teacher-assist-shell";

export default function TeacherAssistLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return <TeacherAssistShell>{children}</TeacherAssistShell>;
}
