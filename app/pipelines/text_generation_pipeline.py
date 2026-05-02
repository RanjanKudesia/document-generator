import logging

from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    ExtractedData,
    ExtractedXmlData,
    ParagraphBlock,
    TableBlock,
)


class TextGenerationPipeline:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, payload: DocumentGenerationRequest, file_name: str) -> bytes:
        _ = file_name
        content = self._build_content(payload).rstrip() + "\n"
        return content.encode("utf-8")

    def _build_content(self, payload: DocumentGenerationRequest) -> str:
        lines: list[str] = []

        if payload.extracted_data is not None:
            if isinstance(payload.extracted_data, ExtractedXmlData):
                lines.extend(self._from_xml(payload.extracted_data))
            else:
                lines.extend(self._from_json(payload.extracted_data))
        else:
            if payload.title:
                lines.append(payload.title)
                lines.append("")
            for block in payload.blocks:
                if isinstance(block, ParagraphBlock):
                    lines.append(block.text)
                    lines.append("")
                elif isinstance(block, TableBlock):
                    lines.extend(self._table_to_text(block.rows))
                    lines.append("")

        return "\n".join(lines)

    def _from_json(self, data: ExtractedData) -> list[str]:
        paragraph_by_index = {item.index: item for item in data.paragraphs}
        table_by_index = {item.index: item for item in data.tables}
        lines: list[str] = []

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    p = paragraph_by_index.get(item.index)
                    if p is not None:
                        lines.append(self._paragraph_to_text(p))
                        lines.append("")
                elif item.type == "table":
                    t = table_by_index.get(item.index)
                    if t is not None:
                        rows = [[(cell.text or "") for cell in row.cells]
                                for row in t.rows]
                        lines.extend(self._table_to_text(rows))
                        lines.append("")
            return lines

        for p in sorted(data.paragraphs, key=lambda x: x.index):
            lines.append(self._paragraph_to_text(p))
            lines.append("")
        for t in sorted(data.tables, key=lambda x: x.index):
            rows = [[(cell.text or "") for cell in row.cells]
                    for row in t.rows]
            lines.extend(self._table_to_text(rows))
            lines.append("")
        # Collapse consecutive blank lines to at most one blank line.
        collapsed: list[str] = []
        prev_blank = False
        for line in lines:
            is_blank = not line.strip()
            if is_blank and prev_blank:
                continue
            collapsed.append(line)
            prev_blank = is_blank
        return collapsed

    def _paragraph_to_text(self, paragraph) -> str:
        """Render paragraph text with bullet/numbered prefix and heading decoration."""
        text = (paragraph.text or "").strip()
        if not text and paragraph.runs:
            text = "".join((run.text or "") for run in paragraph.runs).strip()
        if paragraph.is_bullet:
            return f"- {text}"
        if paragraph.is_numbered:
            fmt = (paragraph.numbering_format or "1.").rstrip()
            return f"{fmt} {text}"
        # Heading decoration
        style = (paragraph.style or "").strip().lower()
        if style.startswith("heading"):
            parts = paragraph.style.strip().split()
            level = parts[-1] if len(parts) > 1 else "1"
            if level == "1":
                return f"{text}\n{'=' * len(text)}"
            if level == "2":
                return f"{text}\n{'-' * len(text)}"
            return f"\n{text}"
        return text

    def _from_xml(self, data: ExtractedXmlData) -> list[str]:
        lines: list[str] = []
        for item in data.parsed_body:
            if item.type == "paragraph" and item.paragraph is not None:
                text = item.paragraph.text or ""
                if not text and item.paragraph.runs:
                    text = "".join((run.text or "")
                                   for run in item.paragraph.runs)
                lines.append(text.strip())
                lines.append("")
            elif item.type == "table" and item.table is not None:
                rows = [[(cell.text or "") for cell in row.cells]
                        for row in item.table.rows]
                lines.extend(self._table_to_text(rows))
                lines.append("")
        return lines

    def _table_to_text(self, rows: list[list[str]]) -> list[str]:
        if not rows:
            return []
        n_cols = max(len(r) for r in rows)
        col_widths = [0] * n_cols
        for row in rows:
            for i, cell in enumerate(row):
                if i < n_cols:
                    col_widths[i] = max(
                        col_widths[i], len((cell or "").strip()))
        result: list[str] = []
        for row in rows:
            padded = [
                (row[i].strip() if i < len(row) else "").ljust(col_widths[i])
                for i in range(n_cols)
            ]
            result.append(" | ".join(padded))
        return result
