"""Slide layouts, visual recommendations, and inline SVG renderers."""

from __future__ import annotations

from typing import Any

PUBLIC_VISUAL_SOURCES = ["Openverse", "Wikimedia Commons", "Unsplash", "Pixabay"]


def _visual_profile(slide_type: str, title: str) -> dict[str, Any]:
    key = f"{slide_type} {title}".lower()
    if "vocab" in key:
        return {
            "visual_type": "vocabulary_card",
            "layout": "vocabulary_card",
            "placement": "full_width",
            "description": "Large classroom vocabulary card with picture cue, definition space, and example sentence.",
            "purpose": "Helps students connect academic vocabulary to meaning and classroom examples.",
            "search_terms": ["elementary vocabulary card", "picture vocabulary organizer", "word meaning visual"],
            "sources": ["Teacher-created SVG illustrations", "Openverse"],
        }
    if "exit" in key or "check for understanding" in key:
        return {
            "visual_type": "icon_illustration",
            "layout": "exit_ticket",
            "placement": "right",
            "description": "Simple response icon set with answer box, pencil, and evidence cue.",
            "purpose": "Signals that students should respond independently and keep the focus on the assessment prompt.",
            "search_terms": ["exit ticket classroom icon", "student response illustration", "assessment prompt visual"],
            "sources": ["Teacher-created SVG illustrations", "Wikimedia Commons"],
        }
    if "guided practice" in key:
        return {
            "visual_type": "graphic_organizer",
            "layout": "guided_practice",
            "placement": "right",
            "description": "Graphic organizer with modeled example and space for student partner thinking.",
            "purpose": "Shows the steps students should follow during guided practice.",
            "search_terms": ["guided practice organizer", "student partner work organizer", "modeled example chart"],
            "sources": ["Teacher-created SVG illustrations", "Openverse", "Pixabay"],
        }
    if "independent practice" in key:
        return {
            "visual_type": "process_flow",
            "layout": "independent_practice",
            "placement": "left",
            "description": "Three-step process flow showing read, think, and write independently.",
            "purpose": "Clarifies the sequence students should follow without needing teacher narration.",
            "search_terms": ["student process flow", "read think write poster", "independent work visual"],
            "sources": ["Teacher-created SVG illustrations", "Wikimedia Commons"],
        }
    if "supporting detail" in key:
        return {
            "visual_type": "graphic_organizer",
            "layout": "visual_top_text_bottom",
            "placement": "top",
            "description": "Main idea organizer with labeled supporting detail boxes.",
            "purpose": "Helps students distinguish the main idea from details that support it.",
            "search_terms": ["main idea supporting details organizer", "supporting details chart", "reading organizer"],
            "sources": ["Teacher-created SVG illustrations", "Openverse", "Pixabay"],
        }
    if "main idea" in key or "concept" in key or "objective" in key:
        return {
            "visual_type": "concept_map",
            "layout": "concept_map",
            "placement": "right",
            "description": "Central concept map with one main idea node and supporting branches.",
            "purpose": "Makes the relationship between the main idea and supporting details visible for students.",
            "search_terms": ["main idea concept map", "main idea graphic organizer", "reading concept map"],
            "sources": ["Teacher-created SVG illustrations", "Openverse", "Wikimedia Commons"],
        }
    if "timeline" in key or "sequence" in key:
        return {
            "visual_type": "timeline",
            "layout": "timeline",
            "placement": "full_width",
            "description": "Horizontal classroom timeline showing events or steps in sequence.",
            "purpose": "Helps students see order and sequence at a glance.",
            "search_terms": ["elementary timeline infographic", "process timeline classroom", "sequence organizer"],
            "sources": ["Teacher-created SVG illustrations", "Openverse"],
        }
    if "compare" in key:
        return {
            "visual_type": "comparison_table",
            "layout": "comparison_table",
            "placement": "full_width",
            "description": "Two-column comparison table with headers and student-friendly examples.",
            "purpose": "Supports compare-and-contrast thinking with a clean side-by-side structure.",
            "search_terms": ["compare contrast chart", "two column comparison organizer", "Venn alternative organizer"],
            "sources": ["Teacher-created SVG illustrations", "Openverse"],
        }
    if "title" in slide_type or "wrap-up" in key:
        return {
            "visual_type": "image",
            "layout": "title_only",
            "placement": "background",
            "description": "Friendly classroom illustration that previews the lesson theme.",
            "purpose": "Sets lesson tone and creates an inviting classroom presentation opener.",
            "search_terms": ["children reading informational text", "elementary classroom reading", "public domain school illustration"],
            "sources": ["Unsplash", "Pixabay", "Wikimedia Commons"],
        }
    return {
        "visual_type": "diagram",
        "layout": "text_left_visual_right",
        "placement": "right",
        "description": "Student-friendly diagram that makes the lesson concept concrete.",
        "purpose": "Adds a visual anchor so students can understand the concept without relying only on narration.",
        "search_terms": ["elementary lesson diagram", "education concept visual", "classroom anchor chart"],
        "sources": PUBLIC_VISUAL_SOURCES,
    }


def build_visual_recommendation(
    *,
    slide_id: str,
    slide_type: str,
    title: str,
    objective_text: str | None,
    teks_ids: list[str] | None = None,
) -> dict[str, Any]:
    profile = _visual_profile(slide_type, title)
    prompt_hint = f"{title}: {profile['description']}"
    if objective_text:
        prompt_hint += f" Align tightly to objective: {objective_text}."
    if teks_ids:
        prompt_hint += f" Standards: {', '.join(teks_ids)}."
    return {
        "slide_id": slide_id,
        "visual_type": profile["visual_type"],
        "title": f"{title} Visual",
        "description": profile["description"],
        "educational_purpose": profile["purpose"],
        "suggested_placement": profile["placement"],
        "layout_template": profile["layout"],
        "search_terms": profile["search_terms"],
        "suggested_sources": profile["sources"],
        "source_type": "public_search",
        "source_url": None,
        "attribution": None,
        "local_asset_key": None,
        "prompt_hint": prompt_hint,
        "visual_generation_status": "recommendation_only",
    }


def add_slide_visual_metadata(
    slides: list[dict[str, Any]],
    *,
    objective_text: str | None,
    teks_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for index, slide in enumerate(slides, start=1):
        slide_id = str(slide.get("id") or f"slide-{index}")
        slide_type = str(slide.get("slideType") or slide.get("slide_type") or "content")
        recommendation = build_visual_recommendation(
            slide_id=slide_id,
            slide_type=slide_type,
            title=str(slide.get("title") or f"Slide {index}"),
            objective_text=objective_text,
            teks_ids=teks_ids,
        )
        merged = dict(slide)
        merged["id"] = slide_id
        merged["slideType"] = slide_type
        merged["layout"] = str(merged.get("layout") or recommendation["layout_template"])
        merged["visualType"] = str(merged.get("visualType") or recommendation["visual_type"])
        merged["visualRecommendation"] = recommendation
        enriched.append(merged)
    return enriched


def build_slide_visual_assets(slides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    for slide in slides:
        slide_id = str(slide.get("id") or "")

        # New schema: slide has a `visual` block with image_search metadata
        visual_block = slide.get("visual")
        if isinstance(visual_block, dict) and visual_block.get("type") == "image_search":
            img_search = visual_block.get("image_search") or {}
            search_terms = [t for t in (img_search.get("search_terms") or []) if isinstance(t, str)]
            assets.append(
                {
                    "slide_id": slide_id,
                    "visual_type": "image",
                    "title": f"{slide.get('title') or slide_id} — Image",
                    "description": str(img_search.get("image_alt_text") or img_search.get("educational_purpose") or ""),
                    "source_type": "pixabay",
                    "source_url": visual_block.get("source_url"),
                    "attribution": visual_block.get("attribution"),
                    "local_asset_key": visual_block.get("local_asset_key"),
                    "prompt_hint": str(img_search.get("image_alt_text") or ""),
                    "educational_purpose": str(img_search.get("educational_purpose") or ""),
                    "suggested_placement": str(visual_block.get("placement") or "right"),
                    "layout_template": str(slide.get("layout") or ""),
                    "visual_generation_status": "pending",
                    "search_terms_json": search_terms,
                    "suggested_sources_json": ["Pixabay"],
                }
            )
            continue

        # Legacy schema: slide has a `visualRecommendation` block
        recommendation = slide.get("visualRecommendation")
        if not isinstance(recommendation, dict):
            continue
        assets.append(
            {
                "slide_id": str(slide.get("id") or recommendation.get("slide_id") or ""),
                "visual_type": str(recommendation.get("visual_type") or "diagram"),
                "title": str(recommendation.get("title") or slide.get("title") or "Slide visual"),
                "description": str(recommendation.get("description") or ""),
                "source_type": str(recommendation.get("source_type") or "public_search"),
                "source_url": recommendation.get("source_url"),
                "attribution": recommendation.get("attribution"),
                "local_asset_key": recommendation.get("local_asset_key"),
                "prompt_hint": recommendation.get("prompt_hint"),
                "educational_purpose": recommendation.get("educational_purpose"),
                "suggested_placement": recommendation.get("suggested_placement"),
                "layout_template": recommendation.get("layout_template"),
                "visual_generation_status": str(recommendation.get("visual_generation_status") or "recommendation_only"),
                "search_terms_json": list(recommendation.get("search_terms") or []),
                "suggested_sources_json": list(recommendation.get("suggested_sources") or []),
            }
        )
    return assets


def render_visual_svg(visual_type: str | None, visual_data: dict[str, Any] | None = None) -> str:
    visual_data = visual_data or {}
    label = visual_data.get("label") or "Teaching visual"
    if not visual_type or visual_type in {"none", "image"}:
        return ""

    if visual_type in {"concept_map", "main_idea_web"}:
        return f"""
<svg viewBox="0 0 360 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <circle cx="180" cy="118" r="52" fill="#dbeafe" stroke="#2563eb" stroke-width="4"/>
  <text x="180" y="115" text-anchor="middle" font-size="18" font-weight="700" fill="#1e3a8a">Main Idea</text>
  <text x="180" y="136" text-anchor="middle" font-size="12" fill="#1e40af">Big idea</text>
  <line x1="180" y1="64" x2="180" y2="24" stroke="#64748b" stroke-width="3"/>
  <line x1="132" y1="156" x2="64" y2="198" stroke="#64748b" stroke-width="3"/>
  <line x1="228" y1="156" x2="296" y2="198" stroke="#64748b" stroke-width="3"/>
  <rect x="144" y="6" width="72" height="28" rx="12" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <rect x="24" y="190" width="92" height="30" rx="12" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <rect x="244" y="190" width="92" height="30" rx="12" fill="#ecfeff" stroke="#0891b2" stroke-width="2"/>
  <text x="180" y="24" text-anchor="middle" font-size="12" fill="#155e75">Detail</text>
  <text x="70" y="209" text-anchor="middle" font-size="12" fill="#155e75">Detail</text>
  <text x="290" y="209" text-anchor="middle" font-size="12" fill="#155e75">Detail</text>
</svg>"""

    if visual_type in {"graphic_organizer", "chart"}:
        return f"""
<svg viewBox="0 0 360 230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <rect x="24" y="24" width="312" height="182" rx="18" fill="#f8fafc" stroke="#cbd5e1" stroke-width="2"/>
  <rect x="44" y="42" width="272" height="34" rx="10" fill="#dbeafe"/>
  <text x="58" y="64" font-size="14" font-weight="700" fill="#1e3a8a">Main Idea</text>
  <rect x="44" y="92" width="272" height="26" rx="8" fill="#e2e8f0"/>
  <rect x="44" y="128" width="272" height="26" rx="8" fill="#e2e8f0"/>
  <rect x="44" y="164" width="272" height="26" rx="8" fill="#e2e8f0"/>
  <text x="58" y="109" font-size="12" fill="#334155">Supporting Detail 1</text>
  <text x="58" y="145" font-size="12" fill="#334155">Supporting Detail 2</text>
  <text x="58" y="181" font-size="12" fill="#334155">Supporting Detail 3</text>
</svg>"""

    if visual_type in {"process_flow", "timeline"}:
        return f"""
<svg viewBox="0 0 360 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <line x1="52" y1="112" x2="308" y2="112" stroke="#38bdf8" stroke-width="8" stroke-linecap="round"/>
  <circle cx="72" cy="112" r="24" fill="#e0f2fe" stroke="#0284c7" stroke-width="3"/>
  <circle cx="180" cy="112" r="24" fill="#e0f2fe" stroke="#0284c7" stroke-width="3"/>
  <circle cx="288" cy="112" r="24" fill="#e0f2fe" stroke="#0284c7" stroke-width="3"/>
  <text x="72" y="118" text-anchor="middle" font-size="12" fill="#075985">1</text>
  <text x="180" y="118" text-anchor="middle" font-size="12" fill="#075985">2</text>
  <text x="288" y="118" text-anchor="middle" font-size="12" fill="#075985">3</text>
  <text x="72" y="160" text-anchor="middle" font-size="12" fill="#334155">Read</text>
  <text x="180" y="160" text-anchor="middle" font-size="12" fill="#334155">Think</text>
  <text x="288" y="160" text-anchor="middle" font-size="12" fill="#334155">Explain</text>
</svg>"""

    if visual_type == "vocabulary_card":
        return f"""
<svg viewBox="0 0 360 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <rect x="30" y="30" width="300" height="160" rx="20" fill="#fff7ed" stroke="#fb923c" stroke-width="3"/>
  <rect x="54" y="56" width="92" height="92" rx="16" fill="#ffedd5" stroke="#ea580c" stroke-width="2"/>
  <rect x="164" y="56" width="142" height="28" rx="10" fill="#ffffff" stroke="#fdba74" stroke-width="2"/>
  <rect x="164" y="96" width="142" height="18" rx="8" fill="#fffbeb"/>
  <rect x="164" y="124" width="126" height="18" rx="8" fill="#fffbeb"/>
  <rect x="54" y="160" width="252" height="14" rx="7" fill="#fed7aa"/>
</svg>"""

    if visual_type == "comparison_table":
        return f"""
<svg viewBox="0 0 360 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <rect x="30" y="34" width="300" height="152" rx="16" fill="#f8fafc" stroke="#94a3b8" stroke-width="2"/>
  <line x1="180" y1="34" x2="180" y2="186" stroke="#94a3b8" stroke-width="2"/>
  <line x1="30" y1="74" x2="330" y2="74" stroke="#94a3b8" stroke-width="2"/>
  <line x1="30" y1="114" x2="330" y2="114" stroke="#cbd5e1" stroke-width="2"/>
  <line x1="30" y1="150" x2="330" y2="150" stroke="#cbd5e1" stroke-width="2"/>
  <text x="105" y="60" text-anchor="middle" font-size="14" fill="#0f172a">Option A</text>
  <text x="255" y="60" text-anchor="middle" font-size="14" fill="#0f172a">Option B</text>
</svg>"""

    if visual_type == "icon_illustration":
        return f"""
<svg viewBox="0 0 220 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <rect x="54" y="34" width="108" height="152" rx="16" fill="#f8fafc" stroke="#94a3b8" stroke-width="3"/>
  <line x1="74" y1="76" x2="142" y2="76" stroke="#64748b" stroke-width="4" stroke-linecap="round"/>
  <line x1="74" y1="108" x2="142" y2="108" stroke="#64748b" stroke-width="4" stroke-linecap="round"/>
  <line x1="74" y1="140" x2="126" y2="140" stroke="#64748b" stroke-width="4" stroke-linecap="round"/>
  <circle cx="154" cy="156" r="26" fill="#dbeafe" stroke="#2563eb" stroke-width="3"/>
  <path d="M146 156l6 6 12-16" fill="none" stroke="#2563eb" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""

    if visual_type == "diagram":
        return f"""
<svg viewBox="0 0 360 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{label}">
  <rect x="34" y="38" width="118" height="64" rx="16" fill="#dbeafe" stroke="#2563eb" stroke-width="3"/>
  <rect x="208" y="38" width="118" height="64" rx="16" fill="#dcfce7" stroke="#16a34a" stroke-width="3"/>
  <rect x="120" y="132" width="118" height="54" rx="16" fill="#fef3c7" stroke="#d97706" stroke-width="3"/>
  <path d="M152 70h56M180 70v60" stroke="#64748b" stroke-width="4" stroke-linecap="round"/>
</svg>"""

    return ""
