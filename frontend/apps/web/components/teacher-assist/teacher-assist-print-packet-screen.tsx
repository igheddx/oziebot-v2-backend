"use client";

import Image from "next/image";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import {
  fetchAssignmentPrintPacket,
  fetchAssignmentPrintPacketPages,
} from "@/lib/teacher-assist-api";
import type { AssignmentPrintPacket, AssignmentPrintPage } from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function TemplateBody({
  templateType,
}: {
  templateType: AssignmentPrintPacket["template_type"];
}) {
  if (templateType === "lined_writing_page") {
    return (
      <div className="mt-6 space-y-4">
        {Array.from({ length: 12 }, (_, index) => (
          <div key={index} className="border-b border-slate-300 pb-4" />
        ))}
      </div>
    );
  }
  if (templateType === "short_answer_page") {
    return (
      <div className="mt-6 space-y-4">
        <div className="rounded-2xl border border-slate-300 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Prompt</p>
          <div className="mt-3 h-20 rounded-xl border border-dashed border-slate-300 bg-slate-50" />
        </div>
        {Array.from({ length: 4 }, (_, index) => (
          <div key={index} className="rounded-2xl border border-slate-300 p-4">
            <div className="h-16 rounded-xl border border-dashed border-slate-300 bg-white" />
          </div>
        ))}
      </div>
    );
  }
  return <div className="mt-6 min-h-[32rem] rounded-3xl border border-dashed border-slate-300 bg-white" />;
}

function PacketPageCard({
  packet,
  page,
}: {
  packet: AssignmentPrintPacket;
  page: AssignmentPrintPage;
}) {
  return (
    <article
      className="mx-auto flex min-h-[11in] w-full max-w-[8.5in] flex-col rounded-3xl border border-slate-200 bg-white p-8 shadow-sm shadow-slate-950/5 print:break-after-page print:border-none print:shadow-none"
      style={{ pageBreakAfter: "always" }}
    >
      <div className="flex items-start justify-between gap-6">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">
            TeacherAssist Printable Packet
          </p>
          <h1 className="mt-2 text-2xl font-semibold text-slate-900">
            STUDENT #{page.student_number}
          </h1>
          <p className="mt-2 text-sm text-slate-600">
            {labelize(packet.template_type)} · Page {page.page_number} of {packet.pages_per_student}
          </p>
        </div>
        <Image
          src={page.qr_svg_data_uri}
          alt={`QR code for student ${page.student_number} page ${page.page_number}`}
          width={128}
          height={128}
          unoptimized
          className="h-32 w-32 rounded-2xl border border-slate-200 bg-white p-2"
        />
      </div>

      <TemplateBody templateType={packet.template_type} />

      <div className="mt-auto flex items-end justify-between gap-4 pt-8 text-xs text-slate-500">
        <div>
          <p>Packet {packet.id}</p>
          <p>Assignment {packet.assignment_id}</p>
        </div>
        <div className="text-right">
          <p>Anonymous student number only</p>
          <p>QR payload contains no names, emails, or real student ids</p>
        </div>
      </div>
    </article>
  );
}

export function TeacherAssistPrintPacketScreen() {
  const searchParams = useSearchParams();
  const packetId = searchParams.get("id");
  const [packet, setPacket] = useState<AssignmentPrintPacket | null>(null);
  const [pages, setPages] = useState<AssignmentPrintPage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!packetId) {
      setLoading(false);
      setError("Missing print packet id.");
      return;
    }
    setLoading(true);
    setError(null);
    Promise.all([fetchAssignmentPrintPacket(packetId), fetchAssignmentPrintPacketPages(packetId)])
      .then(([nextPacket, nextPages]) => {
        setPacket(nextPacket);
        setPages(nextPages);
      })
      .catch((nextError) => {
        setError(nextError instanceof Error ? nextError.message : "Could not load printable packet.");
      })
      .finally(() => {
        setLoading(false);
      });
  }, [packetId]);

  const samplePayload = useMemo(() => pages[0]?.qr_payload_json ?? {}, [pages]);

  if (loading) {
    return (
      <div className="ta-panel p-6 text-sm text-slate-600">Loading printable packet...</div>
    );
  }

  if (error || !packet) {
    return (
      <div className="space-y-4">
        <section className="ta-alert ta-alert-error">{error ?? "Printable packet not found."}</section>
        <Link href="/teacher-assist/assignments" className="ta-button-secondary">
          Back to Assignments
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6 print:space-y-0">
      <section className="ta-panel p-6 print:hidden">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
              TeacherAssist Printable Packet
            </p>
            <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900">
              QR-coded assignment packet
            </h1>
            <p className="mt-3 text-sm leading-6 text-slate-600">
              This view is print-ready and uses anonymous student numbers only. The QR payload preview
              below includes ids, page metadata, and a packet token without exposing names or emails.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button type="button" onClick={() => window.print()} className="ta-button-primary">
              Print Packet
            </button>
            <Link href="/teacher-assist/assignments" className="ta-button-secondary">
              Back to Assignments
            </Link>
          </div>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-500">Students</p>
            <p className="mt-2 text-lg font-semibold text-slate-900">{packet.student_count}</p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-500">Total pages</p>
            <p className="mt-2 text-lg font-semibold text-slate-900">{packet.total_page_count}</p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-500">Template</p>
            <p className="mt-2 text-lg font-semibold text-slate-900">{labelize(packet.template_type)}</p>
          </div>
        </div>

        <pre className="mt-5 overflow-auto rounded-2xl bg-slate-900 p-4 text-xs text-slate-100">
          {JSON.stringify(samplePayload, null, 2)}
        </pre>
      </section>

      <section className="space-y-6 print:space-y-0">
        {pages.map((page) => (
          <PacketPageCard key={page.id} packet={packet} page={page} />
        ))}
      </section>
    </div>
  );
}
