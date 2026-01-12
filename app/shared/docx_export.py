from __future__ import annotations

import base64
import io
import re
from typing import Any, Dict, List, Optional

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Inches

from .report_blocks import ReportBlock


def _safe_filename(filename: str) -> str:
    name = filename.strip() or "export.docx"
    name = name.replace("\\", "_").replace("/", "_")
    name = re.sub(r"[^A-Za-z0-9._\- ]+", "_", name)
    if not name.lower().endswith(".docx"):
        name = name + ".docx"
    return name


def _add_text_lines(doc: Document, text: str) -> None:
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("- ") or line.startswith("* "):
            doc.add_paragraph(line[2:].strip(), style="List Bullet")
            continue

        if re.match(r"^\d+\.\s+", line):
            doc.add_paragraph(re.sub(r"^\d+\.\s+", "", line), style="List Number")
            continue

        doc.add_paragraph(line)


def build_docx_bytes_from_report_blocks(
    report_blocks: List[ReportBlock],
    *,
    visualizations: Optional[Dict[str, str]] = None,
    include_sources: bool = True,
    include_visualizations: bool = True,
) -> bytes:
    doc = Document()
    visualizations = visualizations or {}

    for block in report_blocks:
        if block.type == "heading":
            level = int(block.level or 1)
            level = min(max(level, 1), 9)
            doc.add_heading(block.text or "", level=level)
            continue

        if block.type == "paragraph":
            _add_text_lines(doc, block.text or "")
            continue

        if block.type == "figure":
            if not include_visualizations:
                continue

            fig_id = (block.figure_id or "").strip()
            fig_b64 = visualizations.get(fig_id)
            if not fig_id or not fig_b64:
                continue

            try:
                img_bytes = base64.b64decode(fig_b64)
            except Exception:
                continue

            width = float(block.width) if block.width is not None else 6.0
            width = max(1.0, min(width, 7.0))

            buf = io.BytesIO(img_bytes)
            p = doc.add_paragraph()
            run = p.add_run()
            run.add_picture(buf, width=Inches(width))
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER

            if block.caption:
                cap = doc.add_paragraph(block.caption)
                cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
            continue

        if block.type == "references":
            if not include_sources:
                continue

            refs = block.references or []
            if not refs:
                continue

            doc.add_heading("References", level=2)
            for ref in refs:
                if not isinstance(ref, dict):
                    continue

                doc_id = str(ref.get("doc_id", "")).strip()
                source = str(ref.get("source", "")).strip()
                date = str(ref.get("date", "")).strip()
                title = str(ref.get("title", "")).strip()
                url = str(ref.get("url", "")).strip()

                parts: List[str] = []
                if doc_id:
                    parts.append(f"[{doc_id}]")
                if source:
                    parts.append(source)
                if date:
                    parts.append(f"({date})")
                if title:
                    parts.append(title)

                doc.add_paragraph(" ".join(parts).strip(), style="List Number")
                if url:
                    doc.add_paragraph(url)
            continue

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.read()


def build_content_disposition(filename: str) -> str:
    safe = _safe_filename(filename)
    return f'attachment; filename="{safe}"'
