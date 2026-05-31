import { TeacherAssistLegacyRedirect } from "@/components/teacher-assist-v2/teacher-assist-legacy-redirect";

export default function TeacherAssistLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return <TeacherAssistLegacyRedirect />;
}
