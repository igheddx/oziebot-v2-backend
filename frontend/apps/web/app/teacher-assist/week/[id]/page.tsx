import { TeacherAssistInstructionalWeekScreen } from "@/components/teacher-assist/teacher-assist-instructional-week-screen";

export default async function TeacherAssistInstructionalWeekPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  return <TeacherAssistInstructionalWeekScreen weekId={id} />;
}
