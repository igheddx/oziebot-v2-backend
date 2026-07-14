"""Instructional variety tracking for TeacherAssist v2 package generation.

Tracks strategy and image usage across a package so the AI avoids excessive
repetition. Goals:
  1. Guide AI to vary hooks, strategies, discussion prompts, closures, and layouts.
  2. Provide a shared Pixabay ID registry so image_search.py skips already-used images.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from typing import Any


# ── Strategy library ──────────────────────────────────────────────────────────

STRATEGY_LIBRARY: dict[str, dict[str, Any]] = {
    "turn_and_talk": {
        "display": "Turn and Talk",
        "best_for": ["discussion", "comprehension", "text_evidence", "vocabulary"],
        "description": "Partners share thinking about a prompt before whole-class discussion.",
    },
    "think_pair_share": {
        "display": "Think-Pair-Share",
        "best_for": ["analysis", "inference", "open_response"],
        "description": "Individual think time → partner share → group share.",
    },
    "quick_write": {
        "display": "Quick Write",
        "best_for": ["written_response", "closure", "summarizing"],
        "description": "2–3 minute timed written response to a prompt.",
    },
    "picture_walk": {
        "display": "Picture Walk",
        "best_for": ["prediction", "vocabulary_preview", "pre_reading"],
        "description": "Preview images and text features before reading to build anticipation.",
    },
    "vocabulary_sort": {
        "display": "Vocabulary Sort",
        "best_for": ["vocabulary", "categorization", "word_study"],
        "description": "Students sort vocabulary cards by concept, category, or definition.",
    },
    "gallery_walk": {
        "display": "Gallery Walk",
        "best_for": ["synthesis", "review", "multi_text", "writing_craft"],
        "description": "Students rotate stations to view and respond to posted work or prompts.",
    },
    "notebook_reflection": {
        "display": "Notebook Reflection",
        "best_for": ["metacognition", "closure", "written_response", "summarizing"],
        "description": "Students write a reflection or summary in their reading/writing notebook.",
    },
    "anchor_chart": {
        "display": "Anchor Chart",
        "best_for": ["strategy_introduction", "vocabulary", "skill_modeling"],
        "description": "Co-constructed class reference chart built during teacher modeling.",
    },
    "partner_discussion": {
        "display": "Partner Discussion",
        "best_for": ["text_evidence", "discussion", "reading_response"],
        "description": "Structured partner conversation using a sentence stem or guiding question.",
    },
    "teacher_modeling": {
        "display": "Teacher Modeling",
        "best_for": ["think_aloud", "skill_introduction", "strategy_demonstration"],
        "description": "Teacher models thinking explicitly while students observe and annotate.",
    },
    "graphic_organizer": {
        "display": "Graphic Organizer",
        "best_for": ["text_structure", "comparison", "summarizing", "note_taking"],
        "description": "Visual tool for organizing ideas: T-chart, Venn, sequence map, etc.",
    },
}

# ── Strategy categories ───────────────────────────────────────────────────────

# Core Practices: foundational teaching steps — never penalize for frequency.
CORE_PRACTICES: frozenset[str] = frozenset({"teacher_modeling"})

# Instructional Supports: tools/visuals — flag only on consecutive-day reuse.
INSTRUCTIONAL_SUPPORTS: frozenset[str] = frozenset({"graphic_organizer", "anchor_chart"})

# Engagement Strategies: interactive approaches — apply percentage-based variety check.
ENGAGEMENT_STRATEGIES: frozenset[str] = frozenset(
    {
        "turn_and_talk",
        "think_pair_share",
        "quick_write",
        "picture_walk",
        "vocabulary_sort",
        "gallery_walk",
        "notebook_reflection",
        "partner_discussion",
    }
)

# Configurable variety thresholds
ENGAGEMENT_OVERUSE_RATIO: float = 0.40  # flag if used in ≥40% of instructional days
CONSECUTIVE_SUPPORT_THRESHOLD: int = 3  # flag support tool used N+ consecutive days

# Lowercase search patterns for extracting strategy usage from generated artifact JSON.
_STRATEGY_KEYWORDS: dict[str, list[str]] = {
    "turn_and_talk": ["turn and talk", "turn-and-talk"],
    "think_pair_share": ["think-pair-share", "think pair share"],
    "quick_write": ["quick write", "quick-write"],
    "picture_walk": ["picture walk", "picture-walk"],
    "vocabulary_sort": ["vocabulary sort", "word sort"],
    "gallery_walk": ["gallery walk"],
    "notebook_reflection": ["notebook reflection", "notebook entry", "notebook response"],
    "anchor_chart": ["anchor chart"],
    "partner_discussion": ["partner discussion", "partner talk"],
    "teacher_modeling": ["teacher modeling", "teacher model", "think aloud", "think-aloud"],
    "graphic_organizer": ["graphic organizer", "t-chart", "venn diagram"],
}


# ── Tracker ───────────────────────────────────────────────────────────────────


class VarietyTracker:
    """Thread-safe usage tracker for a single package generation run.

    Created once per package in instructional_package_generation.py, then
    passed through the week loop. Records strategies from each generated artifact
    and maintains the shared Pixabay ID set used by image_search.py to avoid
    duplicate images within a package.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._strategy_history: list[str] = []  # ordered; most-recent last
        self._layout_history: list[str] = []  # slide layout names
        self._used_pixabay_ids: set[int] = set()

    # ── Recording ────────────────────────────────────────────────────────────

    def record_from_artifact(self, content: dict[str, Any], artifact_type: str) -> None:
        """Extract instructional strategies and slide layouts from generated content."""
        if not content:
            return
        text = json.dumps(content).lower()
        found: list[str] = []
        for key, keywords in _STRATEGY_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                found.append(key)

        layouts: list[str] = []
        if artifact_type in ("student_lesson_deck", "subject_slide_deck"):
            for slide in content.get("slides") or []:
                layout = (slide.get("layout") or "").strip()
                if layout:
                    layouts.append(layout)

        with self._lock:
            self._strategy_history.extend(found)
            self._layout_history.extend(layouts)

    # ── Pixabay dedup ─────────────────────────────────────────────────────────

    def claim_pixabay_id(self, pixabay_id: int) -> bool:
        """Mark a Pixabay image ID as used for this package.

        Returns True if the ID was unclaimed (safe to use this image).
        Returns False if already used (skip this image, try another).
        """
        with self._lock:
            if pixabay_id in self._used_pixabay_ids:
                return False
            self._used_pixabay_ids.add(pixabay_id)
            return True

    @property
    def used_pixabay_ids(self) -> frozenset[int]:
        with self._lock:
            return frozenset(self._used_pixabay_ids)

    # ── Context building ──────────────────────────────────────────────────────

    def build_variety_context(self, window: int = 8) -> dict[str, Any]:
        """Return a structured variety context dict for injection into AI prompt_payload.

        `window` controls how many recent strategy entries are considered.

        Category rules (mirrored in instructional_quality_review.py):
          CORE_PRACTICES — never flagged; always available for use.
          INSTRUCTIONAL_SUPPORTS — flagged only if used in every recent artifact (saturation).
          ENGAGEMENT_STRATEGIES — flagged if used in ≥40% of the window.
        """
        with self._lock:
            recent_strategies = list(self._strategy_history[-window:])
            recent_layouts = list(self._layout_history[-8:])

        counts = Counter(recent_strategies)
        overuse_threshold = max(2, int(window * ENGAGEMENT_OVERUSE_RATIO))

        # Only engagement strategies are flagged for variety guidance.
        # Core practices (Teacher Modeling, etc.) are expected every day — never flag them.
        # Instructional supports are not flagged by raw count; flag only at full saturation.
        overused_keys = [
            k for k, n in counts.items() if k in ENGAGEMENT_STRATEGIES and n >= overuse_threshold
        ]

        # Core practices remain available even if just used; engagement strategies rotate.
        fresh_off_limits = set(recent_strategies[-3:]) - CORE_PRACTICES
        available = [v["display"] for k, v in STRATEGY_LIBRARY.items() if k not in fresh_off_limits]

        guidance_parts: list[str] = []
        if overused_keys:
            overused_display = [
                STRATEGY_LIBRARY[k]["display"] for k in overused_keys if k in STRATEGY_LIBRARY
            ]
            guidance_parts.append(f"Vary these (used heavily): {', '.join(overused_display)}.")
        if available:
            guidance_parts.append(f"Recommended: {', '.join(available[:5])}.")

        layout_counts = Counter(recent_layouts)
        overused_layouts = [k for k, n in layout_counts.items() if n >= 3]
        if overused_layouts:
            guidance_parts.append(
                f"Slide layout '{overused_layouts[0]}' repeated — vary layout structure."
            )

        recent_display = [STRATEGY_LIBRARY.get(k, {}).get("display", k) for k in recent_strategies]
        return {
            "recent_strategies": recent_display,
            "recent_slide_layouts": recent_layouts,
            "guidance": " ".join(guidance_parts)
            or "Vary engagement strategies based on today's objective.",
        }
