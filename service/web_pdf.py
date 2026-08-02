from __future__ import annotations

import html
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4, A5, B5
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import BaseDocTemplate, Frame, KeepTogether, PageTemplate, Paragraph, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parents[1]
FONT_DIR = Path(os.environ.get("LEXORA_FONT_DIR", ROOT / "assets" / "fonts"))
FONT_REGULAR = FONT_DIR / "NotoSansSC-Regular.ttf"
FONT_BOLD = FONT_DIR / "NotoSansSC-Bold.ttf"
FONT_IPA = FONT_DIR / "NotoSans-Regular.ttf"


def register_fonts() -> None:
    required = (FONT_REGULAR, FONT_BOLD, FONT_IPA)
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"PDF fonts are missing: {', '.join(missing)}")
    if "LexoraCJK" not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont("LexoraCJK", FONT_REGULAR))
        pdfmetrics.registerFont(TTFont("LexoraCJKBold", FONT_BOLD))
        pdfmetrics.registerFont(TTFont("LexoraIPA", FONT_IPA))
        pdfmetrics.registerFontFamily("LexoraCJK", normal="LexoraCJK", bold="LexoraCJKBold")


def _safe(value: Any) -> str:
    return html.escape(str(value or "").strip()).replace("\n", "<br/>")


def _short_lines(value: Any, limit: int, max_chars: int) -> str:
    lines = [line.strip() for line in str(value or "").replace("\\n", "\n").splitlines() if line.strip()]
    selected: list[str] = []
    remaining = max_chars
    for line in lines[:limit]:
        if remaining <= 0:
            break
        clipped = line[:remaining]
        if len(line) > remaining:
            boundary = max(clipped.rfind(". "), clipped.rfind("; "), clipped.rfind("。"))
            if boundary > max(40, remaining // 2):
                clipped = clipped[: boundary + 1]
            clipped = clipped.rstrip() + "…"
        selected.append(clipped)
        remaining -= len(clipped)
    return "<br/>".join(_safe(line) for line in selected)


def _list(value: Any, limit: int) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = []
    return [str(item).strip() for item in (value or []) if str(item).strip()][:limit]


def _styles(preset: str, typography: dict[str, float] | None = None) -> dict[str, ParagraphStyle]:
    scale = {"small": 0.86, "medium": 1.0, "large": 1.25}.get(preset, 1.0)
    base = 8.2 * scale
    typography = typography or {}
    word_size = max(6.0, min(32.0, float(typography.get("word", 15 * scale))))
    phonetic_size = max(6.0, min(24.0, float(typography.get("phonetic", 7.4 * scale))))
    body_size = max(6.0, min(24.0, float(typography.get("definition", base))))
    related_size = max(6.0, min(24.0, float(typography.get("related", base))))
    example_size = max(6.0, min(24.0, float(typography.get("example", base))))
    phrase_size = max(6.0, min(24.0, float(typography.get("phrase", base))))
    return {
        "word": ParagraphStyle("word", fontName="LexoraCJKBold", fontSize=word_size, leading=word_size * 1.18, textColor=colors.HexColor("#10131d"), spaceAfter=1.5 * mm),
        "ipa": ParagraphStyle("ipa", fontName="LexoraIPA", fontSize=phonetic_size, leading=phonetic_size * 1.3, textColor=colors.HexColor("#697183"), spaceAfter=1 * mm),
        "body": ParagraphStyle("body", fontName="LexoraCJK", fontSize=body_size, leading=body_size * 1.36, textColor=colors.HexColor("#242936"), spaceAfter=.8 * mm),
        "related": ParagraphStyle("related", fontName="LexoraCJK", fontSize=related_size, leading=related_size * 1.34, textColor=colors.HexColor("#242936"), spaceAfter=.8 * mm),
        "example": ParagraphStyle("example", fontName="LexoraCJK", fontSize=example_size, leading=example_size * 1.34, textColor=colors.HexColor("#242936"), spaceAfter=.8 * mm),
        "phrase": ParagraphStyle("phrase", fontName="LexoraCJK", fontSize=phrase_size, leading=phrase_size * 1.34, textColor=colors.HexColor("#242936"), spaceAfter=.8 * mm),
        "zh": ParagraphStyle("zh", fontName="LexoraCJKBold", fontSize=body_size, leading=body_size * 1.36, textColor=colors.HexColor("#304cac"), spaceAfter=1 * mm),
        "label": ParagraphStyle("label", fontName="LexoraCJKBold", fontSize=max(6, related_size * .86), leading=max(7, related_size * 1.12), textColor=colors.HexColor("#304cac"), spaceBefore=.7 * mm),
        "meta": ParagraphStyle("meta", fontName="LexoraCJK", fontSize=base * .72, leading=base, textColor=colors.HexColor("#697183"), alignment=TA_LEFT),
    }


def _entry_card(entry: dict[str, Any], index: int, preset: str, example_count: int, typography: dict[str, float] | None = None):
    style = _styles(preset, typography)
    word = _safe(entry.get("word"))
    requested = str(entry.get("requested_term") or "").strip()
    matched_note = f' <font name="LexoraCJK" color="#8b92a1" size="7">({_safe(requested)})</font>' if requested and requested.lower() != str(entry.get("normalized_word") or "").lower() else ""
    difficulty = _safe(entry.get("difficulty") or "—")
    frequency = entry.get("frequency")
    frequency_text = f"{float(frequency):.1f}" if isinstance(frequency, (float, int)) else "—"
    parts: list[Any] = [
        Paragraph(f"{index}. {word}{matched_note}", style["word"]),
        Paragraph(f"{difficulty}  ·  freq {frequency_text}", style["meta"]),
    ]
    us = str(entry.get("us_phonetic") or "").strip()
    uk = str(entry.get("uk_phonetic") or "").strip()
    if us or uk:
        parts.append(Paragraph(f"US /{_safe(us).strip('/')} / &nbsp;&nbsp; UK /{_safe(uk).strip('/')} /", style["ipa"]))
    text_limit = {"small": 250, "medium": 390, "large": 680}.get(preset, 390)
    definition = _short_lines(entry.get("definition"), 3 if preset != "large" else 2, text_limit)
    definition_zh = _short_lines(entry.get("definition_zh"), 3 if preset != "large" else 2, text_limit // 2)
    if definition:
        parts.append(Paragraph(definition, style["body"]))
    if definition_zh:
        parts.append(Paragraph(definition_zh, style["zh"]))
    synonyms = _list(entry.get("synonyms"), 8)
    antonyms = _list(entry.get("antonyms"), 6)
    if synonyms:
        parts.extend((Paragraph("Synonyms / 近义词", style["label"]), Paragraph(_safe(" · ".join(synonyms)), style["related"])))
    if antonyms:
        parts.extend((Paragraph("Antonyms / 反义词", style["label"]), Paragraph(_safe(" · ".join(antonyms)), style["related"])))
    examples = _list(entry.get("examples"), max(0, min(example_count, 3)))
    if examples:
        parts.append(Paragraph("Examples / 例句", style["label"]))
        example_limit = {"small": 150, "medium": 240, "large": 420}.get(preset, 240)
        parts.extend(Paragraph(_safe(example[:example_limit].rstrip() + ("…" if len(example) > example_limit else "")), style["example"]) for example in examples)
    phrases = _list(entry.get("phrases"), 5)
    if phrases:
        parts.extend((Paragraph("Phrases / 常用短语", style["label"]), Paragraph(_safe(" · ".join(phrases)), style["phrase"])))
    return KeepTogether(Table([[parts]], style=TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f7f8fb")),
        ("BOX", (0, 0), (-1, -1), .55, colors.HexColor("#dfe3ec")),
        ("LEFTPADDING", (0, 0), (-1, -1), 2.8 * mm),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2.8 * mm),
        ("TOPPADDING", (0, 0), (-1, -1), 2.4 * mm),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.4 * mm),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROUNDEDCORNERS", [5 * mm]),
    ])))


def build_pdf(entries: Iterable[dict[str, Any]], title: str, preset: str, example_count: int, page_size: str = "a4", typography: dict[str, float] | None = None) -> tuple[Path, str]:
    register_fonts()
    entries = list(entries)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"lexora-{timestamp}.pdf"
    output = Path(tempfile.mkstemp(prefix="lexora-web-", suffix=".pdf")[1])
    page_width, page_height = {"a5": A5, "b5": B5}.get(page_size.lower(), A4)
    margin = 12 * mm
    gap = 4 * mm
    columns = 3 if preset == "small" else 2 if preset == "medium" else 1
    column_width = (page_width - 2 * margin - gap * (columns - 1)) / columns
    frames = [Frame(margin + index * (column_width + gap), margin, column_width, page_height - 2 * margin - 13 * mm, leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0, id=f"col-{index}") for index in range(columns)]

    def page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor("#10131d"))
        canvas.setFont("LexoraCJKBold", 9)
        canvas.drawString(margin, page_height - 10 * mm, "LEXORA")
        canvas.setFillColor(colors.HexColor("#8b92a1"))
        canvas.setFont("LexoraCJK", 7)
        canvas.drawRightString(page_width - margin, page_height - 10 * mm, f"{len(entries)} entries / 词条  ·  {datetime.now():%Y-%m-%d}")
        canvas.drawRightString(page_width - margin, 6 * mm, str(doc.page))
        canvas.restoreState()

    document = BaseDocTemplate(str(output), pagesize=(page_width, page_height), leftMargin=margin, rightMargin=margin, topMargin=margin + 13 * mm, bottomMargin=margin, title=title or "Lexora vocabulary book", author="Lexora")
    document.addPageTemplates(PageTemplate(id="Lexora", frames=frames, onPage=page))
    heading = ParagraphStyle("heading", fontName="LexoraCJKBold", fontSize=20 if preset != "large" else 24, leading=25, textColor=colors.HexColor("#20388f"), spaceAfter=2 * mm)
    subtitle = ParagraphStyle("subtitle", fontName="LexoraCJK", fontSize=8, leading=11, textColor=colors.HexColor("#697183"), spaceAfter=4 * mm)
    story: list[Any] = [Paragraph(_safe(title or "My vocabulary book"), heading), Paragraph("我的双语词汇书", subtitle)]
    for index, entry in enumerate(entries, 1):
        story.extend((_entry_card(entry, index, preset, example_count, typography), Spacer(1, 1.8 * mm)))
    document.build(story)
    return output, filename
