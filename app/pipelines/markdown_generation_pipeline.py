import logging
import re

from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    ExtractedData,
    ExtractedParagraph,
    ExtractedTable,
    ExtractedXmlData,
    ExtractedXmlParagraph,
    ParagraphBlock,
    TableBlock,
)


class MarkdownGenerationPipeline:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, payload: DocumentGenerationRequest, file_name: str) -> bytes:
        _ = file_name
        content = self._build_content(payload).rstrip() + "\n"
        return content.encode("utf-8")

    def _build_content(self, payload: DocumentGenerationRequest) -> str:
        parts: list[str] = []

        if payload.extracted_data is not None:
            if isinstance(payload.extracted_data, ExtractedXmlData):
                parts.extend(self._from_xml(payload.extracted_data))
            else:
                parts.extend(self._from_json(payload.extracted_data))
        else:
            if payload.title:
                parts.append(f"# {payload.title}")
            for block in payload.blocks:
                if isinstance(block, ParagraphBlock):
                    parts.append(self._block_paragraph_to_md(block))
                elif isinstance(block, TableBlock):
                    parts.append(self._table_to_md(block.rows))

        return "\n\n".join(part for part in parts if part.strip())

    def _from_json(self, data: ExtractedData) -> list[str]:
        paragraph_by_index = {item.index: item for item in data.paragraphs}
        table_by_index = {item.index: item for item in data.tables}
        parts: list[str] = []

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    paragraph = paragraph_by_index.get(item.index)
                    if paragraph is not None:
                        parts.append(self._paragraph_to_md(paragraph))
                elif item.type == "table":
                    table = table_by_index.get(item.index)
                    if table is not None:
                        parts.append(self._extracted_table_to_md(table))
            return parts

        for paragraph in sorted(data.paragraphs, key=lambda item: item.index):
            parts.append(self._paragraph_to_md(paragraph))
        for table in sorted(data.tables, key=lambda item: item.index):
            parts.append(self._extracted_table_to_md(table))
        return parts

    def _from_xml(self, data: ExtractedXmlData) -> list[str]:
        parts: list[str] = []
        for item in data.parsed_body:
            if item.type == "paragraph" and item.paragraph is not None:
                parts.append(self._xml_paragraph_to_md(item.paragraph))
            elif item.type == "table" and item.table is not None:
                rows = []
                for row in item.table.rows:
                    rows.append([(cell.text or "") for cell in row.cells])
                parts.append(self._table_to_md(rows))
        return parts

    def _paragraph_to_md(self, paragraph: ExtractedParagraph) -> str:
        heading_level = self._heading_level_from_style(paragraph.style)

        # Fenced code block
        code_lang = getattr(paragraph, "code_fence_language", None)
        if code_lang is not None or paragraph.style == "CodeBlock":
            lang = code_lang if code_lang is not None else ""
            body = paragraph.text or ""
            return f"```{lang}\n{body}\n```"

        text = self._runs_to_md(paragraph.runs) if paragraph.runs else (
            paragraph.text or "")
        text = text.strip()
        if heading_level:
            text = re.sub(r"^#+\s*", "", text)
            return f"{'#' * heading_level} {text}"
        if paragraph.is_bullet:
            indent_level = self._list_indent_level(paragraph)
            prefix = "  " * indent_level
            return f"{prefix}- {text}"
        if paragraph.is_numbered:
            indent_level = self._list_indent_level(paragraph)
            prefix = "  " * indent_level
            marker = paragraph.numbering_format or "1."
            if not re.match(r"^\d+[.)]$", marker):
                marker = "1."
            return f"{prefix}{marker} {text}"
        return text

    def _list_indent_level(self, paragraph) -> int:
        """Return the nesting depth stored in list_info or list_level."""
        list_level = getattr(paragraph, "list_level", None)
        if list_level is not None:
            return int(list_level)
        list_info = getattr(paragraph, "list_info", None)
        if isinstance(list_info, dict):
            return int(list_info.get("level") or list_info.get("indent_level") or 0)
        if list_info is not None:
            return int(getattr(list_info, "level", 0) or 0)
        return 0

    def _xml_paragraph_to_md(self, paragraph: ExtractedXmlParagraph) -> str:
        heading_level = self._heading_level_from_style(paragraph.style_id)
        if paragraph.runs:
            text = self._xml_runs_to_md(paragraph.runs)
        else:
            text = paragraph.text or ""
        text = text.strip()
        if heading_level:
            return f"{'#' * heading_level} {text}"
        if paragraph.is_bullet:
            return f"- {text}"
        if paragraph.is_numbered:
            marker = paragraph.list_format or "1."
            return f"{marker if re.match(r'^\d+[.)]$', marker or '') else '1.'} {text}"
        return text

    def _block_paragraph_to_md(self, block: ParagraphBlock) -> str:
        text = self._apply_inline_markdown(
            block.text, block.bold, block.italic, block.underline)
        if block.heading_level:
            return f"{'#' * block.heading_level} {text}"
        return text

    def _extracted_table_to_md(self, table: ExtractedTable) -> str:
        rows: list[list[str]] = []
        for row in table.rows:
            rows.append([cell.text or "" for cell in row.cells])
        return self._table_to_md(rows)

    def _table_to_md(self, rows: list[list[str]]) -> str:
        if not rows:
            return ""

        max_cols = max((len(row) for row in rows), default=0)
        normalized = [row + [""] * (max_cols - len(row)) for row in rows]
        header = normalized[0]
        separator = ["---"] * max_cols
        body = normalized[1:]

        lines = [self._pipe_row(header), self._pipe_row(separator)]
        lines.extend(self._pipe_row(row) for row in body)
        return "\n".join(lines)

    def _pipe_row(self, row: list[str]) -> str:
        def _escape_cell(cell: str) -> str:
            return (cell or "").replace("|", "\\|").replace("\n", " ").strip()
        return "| " + " | ".join(_escape_cell(cell) for cell in row) + " |"

    def _runs_to_md(self, runs: list) -> str:
        return "".join(
            self._apply_inline_markdown(
                run.text or "", run.bold, run.italic, run.underline,
                run.hyperlink_url, getattr(run, "code", None))
            for run in runs
        ).replace("\n", "  \n")

    def _xml_runs_to_md(self, runs: list) -> str:
        parts: list[str] = []
        for run in runs:
            target = run.hyperlink_target or run.hyperlink_anchor
            parts.append(
                self._apply_inline_markdown(
                    run.text or "", run.bold, run.italic, run.underline, target)
            )
        return "".join(parts).replace("\n", "  \n")

    def _apply_inline_markdown(
        self,
        text: str,
        bold: bool | None,
        italic: bool | None,
        underline: bool | None,
        link: str | None = None,
        code: bool | None = None,
    ) -> str:
        if code:
            return f"`{text}`"
        result = text
        if bold and italic:
            result = f"***{result}***"
        elif bold:
            result = f"**{result}**"
        elif italic:
            result = f"*{result}*"
        if underline:
            result = f"<u>{result}</u>"
        if link:
            result = f"[{result}]({link})"
        return result

    def _heading_level_from_style(self, style: str | None) -> int | None:
        if not style:
            return None
        match = re.search(r"heading\s*([1-6])", style, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
        m2 = re.match(r"^h([1-6])$", style.lower())
        if m2:
            return int(m2.group(1))
        return None
