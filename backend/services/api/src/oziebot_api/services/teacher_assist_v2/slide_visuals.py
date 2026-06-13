"""Inline SVG visuals for teaching slides and artifact previews."""

from __future__ import annotations

from typing import Any


def render_visual_svg(visual_type: str | None, visual_data: dict[str, Any] | None = None) -> str:
    visual_data = visual_data or {}
    if not visual_type or visual_type == "none":
        return ""

    if visual_type == "main_idea_web":
        return """
<svg viewBox="0 0 320 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Main idea web">
  <circle cx="160" cy="110" r="42" fill="#dbeafe" stroke="#2563eb" stroke-width="3"/>
  <text x="160" y="115" text-anchor="middle" font-size="13" fill="#1e3a8a">Main Idea</text>
  <line x1="160" y1="68" x2="160" y2="28" stroke="#64748b" stroke-width="2"/>
  <circle cx="160" cy="20" r="24" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <text x="160" y="24" text-anchor="middle" font-size="10" fill="#155e75">Detail</text>
  <line x1="118" y1="132" x2="58" y2="172" stroke="#64748b" stroke-width="2"/>
  <circle cx="48" cy="182" r="24" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <text x="48" y="186" text-anchor="middle" font-size="10" fill="#155e75">Detail</text>
  <line x1="202" y1="132" x2="262" y2="172" stroke="#64748b" stroke-width="2"/>
  <circle cx="272" cy="182" r="24" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <text x="272" y="186" text-anchor="middle" font-size="10" fill="#155e75">Detail</text>
</svg>"""

    if visual_type == "supporting_details_chart":
        return """
<svg viewBox="0 0 320 180" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Supporting details chart">
  <rect x="20" y="20" width="280" height="140" rx="12" fill="#f8fafc" stroke="#cbd5e1"/>
  <rect x="40" y="40" width="240" height="28" rx="6" fill="#dbeafe"/>
  <text x="50" y="58" font-size="12" fill="#1e3a8a">Main Idea Row</text>
  <rect x="40" y="78" width="240" height="22" rx="4" fill="#e2e8f0"/>
  <text x="50" y="93" font-size="11" fill="#334155">Detail 1</text>
  <rect x="40" y="106" width="240" height="22" rx="4" fill="#e2e8f0"/>
  <text x="50" y="121" font-size="11" fill="#334155">Detail 2</text>
  <rect x="40" y="134" width="240" height="22" rx="4" fill="#e2e8f0"/>
  <text x="50" y="149" font-size="11" fill="#334155">Detail 3</text>
</svg>"""

    if visual_type == "text_evidence_icon":
        return """
<svg viewBox="0 0 160 160" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Text evidence">
  <circle cx="70" cy="70" r="34" fill="none" stroke="#0f766e" stroke-width="8"/>
  <line x1="96" y1="96" x2="132" y2="132" stroke="#0f766e" stroke-width="8" stroke-linecap="round"/>
  <rect x="42" y="52" width="56" height="36" rx="4" fill="#ccfbf1" stroke="#0f766e"/>
  <line x1="50" y1="62" x2="88" y2="62" stroke="#115e59" stroke-width="2"/>
  <line x1="50" y1="72" x2="84" y2="72" stroke="#115e59" stroke-width="2"/>
  <line x1="50" y1="82" x2="78" y2="82" stroke="#115e59" stroke-width="2"/>
</svg>"""

    if visual_type == "vocabulary_card":
        return """
<svg viewBox="0 0 280 120" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Vocabulary cards">
  <rect x="10" y="20" width="75" height="80" rx="8" fill="#fef3c7" stroke="#d97706"/>
  <rect x="102" y="20" width="75" height="80" rx="8" fill="#fce7f3" stroke="#db2777"/>
  <rect x="194" y="20" width="75" height="80" rx="8" fill="#dcfce7" stroke="#16a34a"/>
  <text x="47" y="65" text-anchor="middle" font-size="11" fill="#92400e">Word</text>
  <text x="139" y="65" text-anchor="middle" font-size="11" fill="#9d174d">Word</text>
  <text x="231" y="65" text-anchor="middle" font-size="11" fill="#166534">Word</text>
</svg>"""

    if visual_type == "paragraph_structure":
        return """
<svg viewBox="0 0 280 160" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Paragraph structure">
  <rect x="20" y="20" width="240" height="24" rx="4" fill="#dbeafe"/>
  <text x="30" y="36" font-size="11" fill="#1e3a8a">Topic sentence / Main idea</text>
  <rect x="20" y="54" width="240" height="18" rx="3" fill="#e2e8f0"/>
  <rect x="20" y="78" width="240" height="18" rx="3" fill="#e2e8f0"/>
  <rect x="20" y="102" width="240" height="18" rx="3" fill="#e2e8f0"/>
  <text x="30" y="140" font-size="11" fill="#475569">Supporting details + closing</text>
</svg>"""

    if visual_type == "checklist":
        return """
<svg viewBox="0 0 220 120" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Checklist">
  <rect x="16" y="16" width="16" height="16" rx="3" fill="#dcfce7" stroke="#16a34a"/>
  <text x="40" y="29" font-size="12" fill="#334155">I can find the main idea</text>
  <rect x="16" y="46" width="16" height="16" rx="3" fill="#dcfce7" stroke="#16a34a"/>
  <text x="40" y="59" font-size="12" fill="#334155">I can cite supporting details</text>
  <rect x="16" y="76" width="16" height="16" rx="3" fill="#f1f5f9" stroke="#94a3b8"/>
  <text x="40" y="89" font-size="12" fill="#334155">I can explain my evidence</text>
</svg>"""

    return ""
