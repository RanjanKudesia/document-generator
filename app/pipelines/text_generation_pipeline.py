from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    ExtractedData,
    ExtractedXmlData,
    ParagraphBlock,
    TableBlock,
)


class TextGenerationPipeline:
    def run(self, payload: DocumentGenerationRequest, file_name: str) -> bytes:
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
                        lines.append((p.text or "").strip())
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
            lines.append((p.text or "").strip())
            lines.append("")
        for t in sorted(data.tables, key=lambda x: x.index):
            rows = [[(cell.text or "") for cell in row.cells]
                    for row in t.rows]
            lines.extend(self._table_to_text(rows))
            lines.append("")
        return lines

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
        return [" | ".join(cell.strip() for cell in row) for row in rows]
