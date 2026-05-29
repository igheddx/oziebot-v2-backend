import { TeacherAssistClassWorkspaceScreen } from "@/components/teacher-assist/teacher-assist-class-workspace-screen";

export default async function TeacherAssistClassWorkspacePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  return <TeacherAssistClassWorkspaceScreen classId={id} />;
}
