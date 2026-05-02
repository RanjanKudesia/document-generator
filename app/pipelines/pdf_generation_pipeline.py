"""PDF document generation pipeline using ReportLab."""
import base64
import logging
import re
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image as RLImage,
    ListFlowable,
    ListItem,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    ExtractedData,
    ExtractedMediaItem,
    ExtractedParagraph,
    ExtractedTable,
    ExtractedXmlData,
    ExtractedXmlParagraph,
    ParagraphBlock,
    TableBlock,
)

H1 = "Heading 1"
H2 = "Heading 2"
H3 = "Heading 3"
H4 = "Heading 4"
H5 = "Heading 5"
H6 = "Heading 6"
H7 = "Heading 7"
H8 = "Heading 8"
H9 = "Heading 9"
LIST_BULLET = "List Bullet"
LIST_NUMBER = "List Number"
BR_TAG = "<br/>"


class PdfGenerationPipeline:
    """Generate a PDF directly from a DocumentGenerationRequest using ReportLab.

    No MS Word or LibreOffice required — pure Python.
    Supports blocks, JSON (ExtractedData) and XML (ExtractedXmlData) payloads.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, payload: DocumentGenerationRequest, file_name: str) -> bytes:
        """Build a PDF document from the payload and return raw PDF bytes."""
        _ = file_name
        data_type = type(
            payload.extracted_data).__name__ if payload.extracted_data else "blocks"
        self.logger.info(
            "pdf_pipeline_start file=%s data_type=%s", file_name, data_type)
        styles = self._build_styles()
        story = self._build_story(payload, styles)
        with BytesIO() as output:
            doc = SimpleDocTemplate(
                output,
                pagesize=A4,
                leftMargin=72,
                rightMargin=72,
                topMargin=72,
                bottomMargin=72,
            )
            doc.build(story)
            result = output.getvalue()
        self.logger.info(
            "pdf_pipeline_complete file=%s size_bytes=%d", file_name, len(result))
        return result

    # ── Style registry ─────────────────────────────────────────────────────

    def _build_styles(self) -> dict:
        base = getSampleStyleSheet()

        def _clone(name: str, parent_key: str = "Normal", **kwargs) -> ParagraphStyle:
            parent = base[parent_key] if parent_key in base else base["Normal"]
            return ParagraphStyle(name=name, parent=parent, **kwargs)

        styles: dict = {
            "Normal": base["Normal"],
            H1: base["Heading1"],
            H2: base["Heading2"],
            H3: base["Heading3"],
            H4: base.get("Heading4") or _clone(
                H4, fontSize=12, leading=16, fontName="Helvetica-Bold", spaceAfter=4
            ),
            H5: base.get("Heading5") or _clone(
                H5, fontSize=11, leading=14, fontName="Helvetica-BoldOblique", spaceAfter=2
            ),
            H6: base.get("Heading6") or _clone(
                H6, fontSize=10, leading=12, fontName="Helvetica-BoldOblique", spaceAfter=2
            ),
            H7: _clone(H7, fontSize=10, leading=12, fontName="Helvetica-Bold"),
            H8: _clone(H8, fontSize=9, leading=11, fontName="Helvetica-Bold"),
            H9: _clone(H9, fontSize=9, leading=11, fontName="Helvetica"),
            LIST_BULLET: _clone(
                LIST_BULLET, leftIndent=24, firstLineIndent=0, spaceBefore=2, spaceAfter=2
            ),
            LIST_NUMBER: _clone(
                LIST_NUMBER, leftIndent=24, firstLineIndent=0, spaceBefore=2, spaceAfter=2
            ),
        }
        return styles

    # ── Story builder ──────────────────────────────────────────────────────

    def _build_story(self, payload: DocumentGenerationRequest, styles: dict) -> list:
        story: list = []

        if payload.extracted_data is not None:
            if isinstance(payload.extracted_data, ExtractedXmlData):
                self._add_xml_extracted(story, payload.extracted_data, styles)
            else:
                self._add_json_extracted(story, payload.extracted_data, styles)
        else:
            if payload.title:
                story.append(
                    Paragraph(escape(payload.title), styles["Heading 1"]))
                story.append(Spacer(1, 6))
            for block in payload.blocks:
                if isinstance(block, ParagraphBlock):
                    self._add_block_paragraph(story, block, styles)
                elif isinstance(block, TableBlock):
                    self._add_block_table(story, block, styles)

        return story

    # ── JSON / ExtractedData path ──────────────────────────────────────────

    def _add_json_extracted(  # NOSONAR  # pylint: disable=too-many-branches
        self, story: list, data: ExtractedData, styles: dict
    ) -> None:
        para_by_idx = {p.index: p for p in data.paragraphs}
        table_by_idx = {t.index: t for t in data.tables}
        media_by_idx = dict(enumerate(data.media))
        rendered_media: set[int] = set()
        current_page_index: int | None = None

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    p = para_by_idx.get(item.index)
                    current_page_index = self._append_page_break_if_needed(
                        story, current_page_index, self._coerce_page_index(p))
                    if p:
                        self._add_json_paragraph(story, p, styles)
                elif item.type == "table":
                    t = table_by_idx.get(item.index)
                    current_page_index = self._append_page_break_if_needed(
                        story, current_page_index, self._coerce_page_index(t))
                    if t:
                        self._add_json_table(story, t, styles)
                elif item.type == "media":
                    m = media_by_idx.get(item.index)
                    current_page_index = self._append_page_break_if_needed(
                        story, current_page_index, self._coerce_page_index(m))
                    if m is not None:
                        img = self._make_image(m)
                        if img:
                            story.append(img)
                            story.append(Spacer(1, 4))
                            rendered_media.add(item.index)
        else:
            for p in sorted(data.paragraphs, key=lambda x: x.index):
                self._add_json_paragraph(story, p, styles)
            for t in sorted(data.tables, key=lambda x: x.index):
                self._add_json_table(story, t, styles)

        # Render any media not already placed via document_order
        for idx, m in sorted(media_by_idx.items()):
            if idx not in rendered_media:
                img = self._make_image(m)
                if img:
                    story.append(img)
                    story.append(Spacer(1, 4))

    def _append_page_break_if_needed(
        self,
        story: list,
        current_page_index: int | None,
        next_page_index: int | None,
    ) -> int | None:
        """Insert page break when source page index advances."""
        if next_page_index is None:
            return current_page_index
        if current_page_index is None:
            return next_page_index
        if next_page_index > current_page_index:
            story.append(PageBreak())
            return next_page_index
        return current_page_index

    def _coerce_page_index(self, item: Any) -> int | None:
        """Safely parse page_index from extracted items."""
        if item is None:
            return None
        value = getattr(item, "page_index", None)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _add_json_paragraph(self, story: list, para: ExtractedParagraph, styles: dict) -> None:
        for run in para.runs:
            for media in run.embedded_media:
                img = self._make_image(media)
                if img:
                    story.append(img)
                    story.append(Spacer(1, 4))

        style = self._resolve_json_style(para, styles)
        markup = self._runs_to_markup_json(
            para.runs) if para.runs else escape(para.text or "")

        if not markup.strip():
            story.append(Spacer(1, max(style.leading or 12, 6)))
            return

        if para.is_bullet:
            story.append(ListFlowable(
                [ListItem(Paragraph(markup, style), leftIndent=20)],
                bulletType="bullet",
                leftIndent=10,
            ))
        elif para.is_numbered:
            story.append(ListFlowable(
                [ListItem(Paragraph(markup, style), leftIndent=20)],
                bulletType="1",
                leftIndent=10,
            ))
        else:
            story.append(Paragraph(markup, style))

    def _add_json_table(self, story: list, table: ExtractedTable, styles: dict) -> None:  # NOSONAR
        rows_data = []
        nested_flowables: list = []
        for row in table.rows:
            row_cells = []
            for cell in row.cells:
                if cell.paragraphs:
                    cell_markup = BR_TAG.join(
                        self._runs_to_markup_json(
                            p.runs) if p.runs else escape(p.text or "")
                        for p in cell.paragraphs
                    )
                else:
                    cell_markup = escape(cell.text or "")
                row_cells.append(Paragraph(cell_markup, styles["Normal"]))
                # Collect nested tables to render after the parent table.
                for nested in (cell.tables or []):
                    nested_flowables.append(("nested", nested, styles))
            rows_data.append(row_cells)

        if not rows_data:
            return

        # Compute proportional column widths from max text length per column.
        col_count = max(len(r) for r in rows_data)
        col_max_len: list[int] = [1] * col_count
        for row in table.rows:
            for c_i, cell in enumerate(row.cells):
                cell_text = cell.text or ""
                if c_i < col_count:
                    col_max_len[c_i] = max(
                        col_max_len[c_i], len(cell_text) or 1)
        total_len = sum(col_max_len) or 1
        available_width = 6.5 * inch  # default body width
        col_widths = [available_width * (w / total_len) for w in col_max_len]

        tbl = Table(rows_data, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(self._default_table_style())
        story.append(tbl)
        story.append(Spacer(1, 6))

        # Render nested tables after the parent table.
        for _, nested_table, nested_styles in nested_flowables:
            self._add_json_table(story, nested_table, nested_styles)

    def _runs_to_markup_json(self, runs: list) -> str:  # NOSONAR
        parts: list[str] = []
        for run in runs:
            text = escape(run.text or "")
            text = text.replace("\n", BR_TAG).replace("\t",
                                                      "&#160;&#160;&#160;&#160;")
            if run.bold:
                text = f"<b>{text}</b>"
            if run.italic:
                text = f"<i>{text}</i>"
            if run.underline:
                text = f"<u>{text}</u>"
            if getattr(run, "strikethrough", None):
                text = f"<strike>{text}</strike>"
            font_attrs: list[str] = []
            if run.font_name:
                safe_face = self._sanitize_reportlab_font_name(run.font_name)
                if safe_face:
                    font_attrs.append(f'face="{escape(safe_face)}"')
            if run.font_size_pt and run.font_size_pt > 0:
                font_attrs.append(f'size="{run.font_size_pt}"')
            if run.color_rgb:
                hex_color = run.color_rgb.replace("#", "").strip()
                font_attrs.append(f'color="#{hex_color}"')
            if font_attrs:
                text = f'<font {" ".join(font_attrs)}>{text}</font>'
            if run.hyperlink_url:
                safe_url = escape(run.hyperlink_url)
                text = f'<a href="{safe_url}" color="blue">{text}</a>'
            parts.append(text)
        return "".join(parts)

    def _resolve_json_style(self, para: ExtractedParagraph, styles: dict) -> ParagraphStyle:
        style_name = para.style or ""
        if style_name in styles:
            return styles[style_name]
        _map = {
            "Heading1": H1, "heading1": H1,
            "Heading2": H2, "heading2": H2,
            "Heading3": H3, "heading3": H3,
            "heading 1": H1, "heading 2": H2, "heading 3": H3,
        }
        mapped = _map.get(style_name)
        if mapped and mapped in styles:
            return styles[mapped]
        return styles["Normal"]

    # ── XML / ExtractedXmlData path ────────────────────────────────────────

    def _add_xml_extracted(self, story: list, data: ExtractedXmlData, styles: dict) -> None:
        for item in data.parsed_body:
            if item.type == "paragraph" and item.paragraph is not None:
                self._add_xml_paragraph(
                    story, item.paragraph, data.relationships, styles)
            elif item.type == "table" and item.table is not None:
                self._add_xml_table(story, item.table,
                                    data.relationships, styles)

    def _add_xml_paragraph(
        self,
        story: list,
        para: ExtractedXmlParagraph,
        relationships: dict,
        styles: dict,
    ) -> None:
        for run in para.runs:
            for media in run.embedded_media:
                img = self._make_image(media)
                if img:
                    story.append(img)
                    story.append(Spacer(1, 4))

        style = self._resolve_xml_style(para, styles)
        markup = (
            self._runs_to_markup_xml(para.runs, relationships)
            if para.runs
            else escape(para.text or "")
        )

        if not markup.strip():
            story.append(Spacer(1, max(style.leading or 12, 6)))
            return

        if para.is_bullet:
            story.append(ListFlowable(
                [ListItem(Paragraph(markup, style), leftIndent=20)],
                bulletType="bullet",
                leftIndent=10,
            ))
        elif para.is_numbered:
            story.append(ListFlowable(
                [ListItem(Paragraph(markup, style), leftIndent=20)],
                bulletType="1",
                leftIndent=10,
            ))
        else:
            story.append(Paragraph(markup, style))

    def _add_xml_table(
        self,
        story: list,
        table_data,
        relationships: dict,
        styles: dict,
    ) -> None:
        rows_data = []
        for row in table_data.rows:
            row_cells = []
            for cell in row.cells:
                if cell.paragraphs:
                    cell_markup = BR_TAG.join(
                        self._runs_to_markup_xml(p.runs, relationships)
                        if p.runs
                        else escape(p.text or "")
                        for p in cell.paragraphs
                    )
                else:
                    cell_markup = escape(cell.text or "")
                row_cells.append(Paragraph(cell_markup, styles["Normal"]))
            rows_data.append(row_cells)

        if not rows_data:
            return

        tbl = Table(rows_data, repeatRows=1)
        tbl.setStyle(self._default_table_style())
        story.append(tbl)
        story.append(Spacer(1, 6))

    def _runs_to_markup_xml(self, runs: list, relationships: dict) -> str:  # NOSONAR
        parts: list[str] = []
        for run in runs:
            text = escape(run.text or "")
            text = text.replace("\n", BR_TAG).replace("\t",
                                                      "&#160;&#160;&#160;&#160;")
            if run.bold:
                text = f"<b>{text}</b>"
            if run.italic:
                text = f"<i>{text}</i>"
            if run.underline:
                text = f"<u>{text}</u>"
            font_attrs: list[str] = []
            if run.font_name:
                safe_face = self._sanitize_reportlab_font_name(run.font_name)
                if safe_face:
                    font_attrs.append(f'face="{escape(safe_face)}"')
            if run.font_size_pt and run.font_size_pt > 0:
                font_attrs.append(f'size="{run.font_size_pt}"')
            if run.color_rgb:
                hex_color = run.color_rgb.replace("#", "").strip()
                font_attrs.append(f'color="#{hex_color}"')
            if font_attrs:
                text = f'<font {" ".join(font_attrs)}>{text}</font>'
            target = run.hyperlink_target
            if not target and run.hyperlink_rid:
                target = relationships.get(run.hyperlink_rid, "")
            if target:
                safe_url = escape(target)
                text = f'<a href="{safe_url}" color="blue">{text}</a>'
            parts.append(text)
        return "".join(parts)

    def _resolve_xml_style(self, para: ExtractedXmlParagraph, styles: dict) -> ParagraphStyle:
        if para.is_numbered:
            return styles.get(LIST_NUMBER, styles["Normal"])
        if para.is_bullet:
            return styles.get(LIST_BULLET, styles["Normal"])
        style_id = para.style_id or ""
        if style_id in styles:
            return styles[style_id]
        _map = {
            "Heading1": H1, "Heading2": H2, "Heading3": H3,
            "Heading4": H4, "Heading5": H5, "Heading6": H6,
            "Heading7": H7, "Heading8": H8, "Heading9": H9,
        }
        mapped = _map.get(style_id)
        if mapped and mapped in styles:
            return styles[mapped]
        return styles["Normal"]

    # ── Blocks path ────────────────────────────────────────────────────────

    def _add_block_paragraph(self, story: list, block: ParagraphBlock, styles: dict) -> None:
        if block.heading_level and 1 <= block.heading_level <= 9:
            style = styles.get(
                f"Heading {block.heading_level}", styles["Normal"])
        else:
            style = styles["Normal"]

        text = escape(block.text)
        if block.bold:
            text = f"<b>{text}</b>"
        if block.italic:
            text = f"<i>{text}</i>"
        if block.underline:
            text = f"<u>{text}</u>"
        font_attrs: list[str] = []
        if block.font_name:
            safe_face = self._sanitize_reportlab_font_name(block.font_name)
            if safe_face:
                font_attrs.append(f'face="{escape(safe_face)}"')
        if block.font_size_pt and block.font_size_pt > 0:
            font_attrs.append(f'size="{block.font_size_pt}"')
        if font_attrs:
            text = f'<font {" ".join(font_attrs)}>{text}</font>'

        story.append(Paragraph(text, style))

    def _add_block_table(self, story: list, block: TableBlock, styles: dict) -> None:
        if not block.rows:
            return
        table_data = [
            [Paragraph(escape(cell), styles["Normal"]) for cell in row]
            for row in block.rows
        ]
        tbl = Table(table_data, repeatRows=1)
        tbl.setStyle(self._default_table_style())
        story.append(tbl)
        story.append(Spacer(1, 6))

    # ── Helpers ────────────────────────────────────────────────────────────

    def _sanitize_reportlab_font_name(self, font_name: str | None) -> str | None:
        """Map extracted font names to ReportLab-safe built-in fonts.

        PDFs often contain subset-prefixed names like "AAAAAB+InterVariable" that
        ReportLab cannot parse in <font face="..."> tags.
        """
        if not font_name:
            return None

        raw = font_name.strip()
        if not raw:
            return None

        # Drop subset prefix (e.g. "AAAAAB+") commonly found in embedded PDF fonts.
        if "+" in raw:
            raw = raw.split("+", 1)[1]

        key = re.sub(r"[^A-Za-z0-9]+", "", raw).lower()

        # Keep to core fonts that ReportLab always understands.
        if "courier" in key:
            return "Courier"
        if "times" in key or "georgia" in key or "palatino" in key or "garamond" in key:
            return "Times-Roman"
        if "symbol" in key:
            return "Symbol"
        if "zapf" in key or "dingbats" in key:
            return "ZapfDingbats"

        # Default sans families -> Helvetica to avoid parser failures.
        if any(token in key for token in (
            "helvetica", "arial", "inter", "roboto", "sans",
            "calibri", "verdana", "tahoma", "trebuchet", "gill",
            "futura", "optima", "franklin", "myriad", "segoe",
        )):
            return "Helvetica"

        # Unknown families are omitted; style default font will be used.
        return None

    def _make_image(self, media: ExtractedMediaItem) -> RLImage | None:
        try:
            media_b64 = media.base64_data or media.base64
            if media_b64:
                img_bytes = base64.b64decode(media_b64, validate=True)
                img_buf: BytesIO | str = BytesIO(img_bytes)
            elif media.local_file_path and Path(media.local_file_path).exists():
                img_buf = media.local_file_path
            else:
                return None

            width = (media.width_emu / 914400) * 72 if media.width_emu else 300
            height = (media.height_emu / 914400) * \
                72 if media.height_emu else None
            return RLImage(img_buf, width=width, height=height)
        except (TypeError, ValueError, OSError):
            return None

    def _default_table_style(self) -> TableStyle:
        return TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [colors.white, colors.Color(0.95, 0.95, 1.0)]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ])
