import logging
from html import escape
from itertools import groupby
from typing import Any

try:
    from bs4 import BeautifulSoup, Tag
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    ExtractedData,
    HtmlMetadata,
    ExtractedXmlData,
    ParagraphBlock,
    TableBlock,
)


class HtmlGenerationPipeline:
    _BLOCK_PRESERVE_TAGS = {"ul", "ol", "table", "figure", "blockquote", "div",
                            "section", "pre", "details", "dl"}
    _DEFAULT_CSS: str = (
        "body{font-family:Arial,Helvetica,sans-serif;line-height:1.6;margin:24px;color:#333;}"
        "table{border-collapse:collapse;width:100%;margin:12px 0;}"
        "th,td{border:1px solid #ccc;padding:8px;vertical-align:top;text-align:left;}"
        "thead th{background:#f0f0f0;font-weight:bold;}"
        "h1,h2,h3,h4,h5,h6{margin:16px 0 8px;}"
        "p{margin:8px 0;}"
        "ul,ol{margin:8px 0 8px 22px;}"
        "ul li{list-style-type:disc;}"
        "ol li{list-style-type:decimal;}"
        "ul ul,ol ul{list-style-type:circle;margin:4px 0 4px 20px;}"
        "ul ol,ol ol{list-style-type:lower-alpha;margin:4px 0 4px 20px;}"
        "code{background:#272822;color:#f8f8f2;padding:2px 5px;border-radius:3px;font-family:monospace;}"
        ".rtl{direction:rtl;unicode-bidi:bidi-override;}"
        "hr.doc-divider{border:none;border-top:1px solid #ccc;margin:16px 0;}"
        ".nested-table-note{font-size:0.8em;color:#888;font-style:italic;}"
    )

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, payload: DocumentGenerationRequest, file_name: str) -> bytes:
        _ = file_name
        html = self._build_document(payload)
        return html.encode("utf-8")

    def _build_document(self, payload: DocumentGenerationRequest) -> str:
        if payload.extracted_data is not None and not isinstance(
            payload.extracted_data, ExtractedXmlData
        ):
            metadata = self._extract_metadata(payload.extracted_data)
            source_body = self._try_build_body_from_source_html(
                payload.extracted_data, metadata)
            if source_body is not None:
                doctype = self._doctype_from_metadata(metadata)
                html_attrs = self._attrs_to_html(
                    metadata.get("html_attributes"))
                head_html = self._head_from_metadata(metadata, payload.title)
                return (
                    f"{doctype}\n"
                    f"<html{html_attrs}>\n"
                    f"{head_html}\n"
                    f"{source_body}\n"
                    "</html>\n"
                )

        body_parts: list[str] = []

        if payload.extracted_data is not None:
            if isinstance(payload.extracted_data, ExtractedXmlData):
                body_parts.extend(self._from_xml(payload.extracted_data))
            else:
                body_parts.extend(self._from_json(payload.extracted_data))
        else:
            if payload.title:
                body_parts.append(f"<h1>{escape(payload.title)}</h1>")
            body_parts.extend(self._from_blocks(payload.blocks))

        css = self._DEFAULT_CSS
        body = "\n".join(body_parts)
        return (
            "<!doctype html>\n"
            '<html lang="en">\n'
            "<head>\n"
            '  <meta charset="utf-8">\n'
            '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
            "  <title>Generated Document</title>\n"
            f"  <style>{css}</style>\n"
            "</head>\n"
            "<body>\n"
            f"{body}\n"
            "</body>\n"
            "</html>\n"
        )

    # -- metadata helpers --

    def _extract_metadata(self, data: ExtractedData) -> dict:
        """Extract HTML metadata dict from ExtractedData.metadata."""
        meta = data.metadata
        if meta is None:
            return {}
        if isinstance(meta, HtmlMetadata):
            return meta.model_dump(exclude_none=True)
        if isinstance(meta, dict):
            return meta
        return {}

    def _doctype_from_metadata(self, metadata: dict) -> str:
        return metadata.get("doctype") or "<!doctype html>"

    def _attrs_to_html(self, attrs: Any) -> str:
        if not attrs or not isinstance(attrs, dict):
            return ""
        parts = []
        for k, v in attrs.items():
            if isinstance(v, bool):
                parts.append(str(k))
            else:
                parts.append(
                    f'{escape(str(k), quote=True)}="{escape(str(v), quote=True)}"')
        return " " + " ".join(parts) if parts else ""

    def _head_from_metadata(self, metadata: dict, title: str | None) -> str:
        head_html = metadata.get("head_html")
        if head_html:
            return head_html
        css = self._DEFAULT_CSS
        doc_title = title or metadata.get("title") or "Generated Document"
        return (
            "<head>\n"
            '  <meta charset="utf-8">\n'
            '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f"  <title>{escape(doc_title)}</title>\n"
            f"  <style>{css}</style>\n"
            "</head>"
        )

    # -- BeautifulSoup source-body patching --

    def _try_build_body_from_source_html(
        self,
        data: ExtractedData,
        metadata: dict,
    ) -> str | None:
        """Reconstruct body by patching paragraph edits into original source body_html."""
        if not _BS4_AVAILABLE:
            return None
        body_html = metadata.get("body_html")
        if not isinstance(body_html, str) or not body_html.strip():
            return None
        try:
            return self._patch_source_body(body_html, data)
        except (ValueError, AttributeError, RuntimeError) as exc:
            self.logger.warning(
                "Source-body patching failed, falling back: %s", exc)
            return None

    def _patch_source_body(self, body_html: str, data: ExtractedData) -> str:
        soup = BeautifulSoup(body_html, "lxml")
        body = soup.body
        if not isinstance(body, Tag):
            body = soup

        para_node_map = self._build_paragraph_node_map(body, data.paragraphs)

        for paragraph in data.paragraphs:
            raw_html = self._paragraph_raw_html(paragraph)
            if not raw_html:
                continue
            node = para_node_map.get(paragraph.index)
            if node is None:
                continue
            new_html = (
                self._runs_to_html(paragraph.runs)
                if paragraph.runs
                else escape(paragraph.text or "")
            )
            self._replace_node_content(node, new_html, paragraph.text or "")

        for paragraph in data.paragraphs:
            if not self._paragraph_raw_html(paragraph):
                self._insert_new_paragraph_in_body(
                    body, soup, paragraph, data.paragraphs, para_node_map)

        body_tag = soup.find("body")
        if isinstance(body_tag, Tag):
            attrs_str = self._attrs_to_html(dict(body_tag.attrs))
            inner = body_tag.decode_contents()
            return f"<body{attrs_str}>\n{inner}\n</body>"

        return f"<body>\n{body.decode_contents()}\n</body>"

    def _build_paragraph_node_map(self, body: "Tag", paragraphs: list) -> "dict[int, Tag]":
        node_map: dict[int, "Tag"] = {}
        for paragraph in paragraphs:
            raw_html = self._paragraph_raw_html(paragraph)
            if not raw_html:
                continue
            node = self._find_body_node_for_paragraph(
                body, raw_html, paragraph)
            if node is not None:
                node_map[paragraph.index] = node
        return node_map

    def _paragraph_raw_html(self, paragraph: Any) -> str | None:
        source = self._paragraph_source(paragraph)
        raw = source.get("raw_html")
        return raw.strip() if isinstance(raw, str) and raw.strip() else None

    def _paragraph_source(self, paragraph: Any) -> dict:
        source = getattr(paragraph, "source", None)
        if isinstance(source, dict):
            return source
        if source is not None:
            return {k: getattr(source, k, None) for k in ("format", "tag", "attrs", "raw_html")}
        return {}

    def _find_body_node_for_paragraph(
        self, body: "Tag", raw_html: str, paragraph: Any
    ) -> "Tag | None":
        for node in body.find_all(True):
            if not isinstance(node, Tag):
                continue
            if str(node).strip() == raw_html:
                return node
            if self._html_text_matches(node, paragraph):
                return node
        return None

    def _html_text_matches(self, node: "Tag", paragraph: Any) -> bool:
        node_text = " ".join(node.get_text(separator=" ").split())
        para_text = " ".join((paragraph.text or "").split())
        if not (para_text and node_text == para_text):
            return False
        source_tag = str(self._paragraph_source(
            paragraph).get("tag") or "").lower()
        if source_tag and node.name and node.name.lower() != source_tag:
            return False
        return True

    def _replace_node_content(self, node: "Tag", new_html: str, fallback_text: str) -> None:
        block_children = [
            c for c in node.children
            if isinstance(c, Tag) and c.name
            and c.name.lower() in self._BLOCK_PRESERVE_TAGS
        ]
        node.clear()
        frag = BeautifulSoup(new_html, "lxml").body
        if frag:
            for child in frag.children:
                node.append(child.__copy__() if hasattr(
                    child, "__copy__") else child)
        else:
            node.string = fallback_text
        for bc in block_children:
            node.append(bc)

    def _insert_new_paragraph_in_body(
        self,
        body: "Tag",
        soup: "BeautifulSoup",
        paragraph: Any,
        all_paragraphs: list,
        para_node_map: "dict[int, Tag] | None" = None,
    ) -> None:
        new_html = (
            self._runs_to_html(paragraph.runs)
            if paragraph.runs
            else escape(paragraph.text or "")
        )
        new_tag = soup.new_tag("p")
        frag = BeautifulSoup(new_html, "lxml").body
        if frag:
            for child in frag.children:
                new_tag.append(child.__copy__() if hasattr(
                    child, "__copy__") else child)
        else:
            new_tag.string = paragraph.text or ""

        # Insert after nearest predecessor with a known node
        predecessor = None
        for other in sorted(all_paragraphs, key=lambda p: p.index):
            if other.index >= paragraph.index:
                break
            if para_node_map and other.index in para_node_map:
                predecessor = para_node_map[other.index]

        if predecessor is not None:
            predecessor.insert_after(new_tag)
        else:
            body.append(new_tag)

    # -- from blocks --

    def _from_blocks(self, blocks: list) -> list[str]:
        parts: list[str] = []
        for block in blocks:
            if isinstance(block, ParagraphBlock):
                level = block.heading_level if block.heading_level else None
                text = self._inline_text(
                    block.text, block.bold, block.italic, block.underline,
                    None, None, None, None)
                if level and 1 <= level <= 6:
                    parts.append(f"<h{level}>{text}</h{level}>")
                else:
                    parts.append(f"<p>{text}</p>")
            elif isinstance(block, TableBlock):
                parts.append(self._simple_rows_to_table(block.rows))
        return parts

    # -- from JSON extracted_data --

    def _from_json(self, data: ExtractedData) -> list[str]:
        paragraph_by_index = {p.index: p for p in data.paragraphs}
        table_by_index = {t.index: t for t in data.tables}
        media_by_index = {idx: m for idx, m in enumerate(data.media)}
        parts: list[str] = []
        list_stack: list[tuple[str, int]] = []

        def close_lists_to(target_level: int) -> None:
            while list_stack and list_stack[-1][1] > target_level:
                tag, _ = list_stack.pop()
                parts.append(f"</{tag}>")

        def close_all_lists() -> None:
            while list_stack:
                tag, _ = list_stack.pop()
                parts.append(f"</{tag}>")

        def open_list(tag: str, level: int, start) -> None:
            if tag == "ol" and start and start != 1:
                parts.append(f'<ol start="{start}">')
            else:
                parts.append(f"<{tag}>")
            list_stack.append((tag, level))

        def add_paragraph(p) -> None:
            text = self._runs_to_html(
                p.runs) if p.runs else escape(p.text or "")
            heading = self._heading_level(p.style)

            if p.style == "HorizontalRule":
                close_all_lists()
                parts.append('<hr class="doc-divider">')
                return

            if heading:
                close_all_lists()
                dir_attr = ' dir="rtl" class="rtl"' if p.direction == "rtl" else ""
                parts.append(f"<h{heading}{dir_attr}>{text}</h{heading}>")
                return

            level = p.list_level or 0
            if p.is_bullet or p.is_numbered:
                desired_tag = "ul" if p.is_bullet else "ol"
                start = None
                if p.list_info and isinstance(p.list_info, dict):
                    start = p.list_info.get("start")

                if not list_stack:
                    open_list(desired_tag, level, start)
                elif list_stack[-1][1] < level:
                    open_list(desired_tag, level, start)
                elif list_stack[-1][1] > level:
                    close_lists_to(level)
                    if not list_stack or list_stack[-1][1] != level:
                        open_list(desired_tag, level, start)
                elif list_stack[-1][0] != desired_tag:
                    close_lists_to(level - 1)
                    open_list(desired_tag, level, start)

                parts.append(f"<li>{text}</li>")
                return

            close_all_lists()
            dir_attr = ' dir="rtl" class="rtl"' if p.direction == "rtl" else ""
            parts.append(f"<p{dir_attr}>{text}</p>")

        def add_table(t) -> None:
            close_all_lists()
            parts.append(self._extracted_table_to_html(t))

        def add_media(m) -> None:
            close_all_lists()
            src = (m.local_url or m.local_file_path or "").strip()
            if not src:
                return
            alt = escape((m.alt_text or "").strip())
            parts.append(
                f'<p><img src="{escape(src, quote=True)}" alt="{alt}" '
                f'style="max-width:100%;height:auto;"></p>'
            )

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    p = paragraph_by_index.get(item.index)
                    if p is not None:
                        add_paragraph(p)
                elif item.type == "table":
                    t = table_by_index.get(item.index)
                    if t is not None:
                        add_table(t)
                elif item.type == "media":
                    m = media_by_index.get(item.index)
                    if m is not None:
                        add_media(m)
        else:
            for p in sorted(data.paragraphs, key=lambda x: x.index):
                add_paragraph(p)
            for t in sorted(data.tables, key=lambda x: x.index):
                add_table(t)
            for idx in sorted(media_by_index.keys()):
                add_media(media_by_index[idx])

        close_all_lists()
        return parts

    # -- from XML extracted_data --

    def _from_xml(self, data: ExtractedXmlData) -> list[str]:
        parts: list[str] = []
        list_state: str | None = None

        def close_list() -> None:
            nonlocal list_state
            if list_state == "ul":
                parts.append("</ul>")
            elif list_state == "ol":
                parts.append("</ol>")
            list_state = None

        for item in data.parsed_body:
            if item.type == "paragraph" and item.paragraph is not None:
                p = item.paragraph
                text = self._xml_runs_to_html(
                    p.runs) if p.runs else escape(p.text or "")
                heading = self._heading_level(p.style_id)
                if heading:
                    close_list()
                    parts.append(f"<h{heading}>{text}</h{heading}>")
                elif p.is_bullet:
                    if list_state != "ul":
                        close_list()
                        parts.append("<ul>")
                        list_state = "ul"
                    parts.append(f"<li>{text}</li>")
                elif p.is_numbered:
                    if list_state != "ol":
                        close_list()
                        parts.append("<ol>")
                        list_state = "ol"
                    parts.append(f"<li>{text}</li>")
                else:
                    close_list()
                    parts.append(f"<p>{text}</p>")
            elif item.type == "table" and item.table is not None:
                close_list()
                rows = [[(cell.text or "") for cell in row.cells]
                        for row in item.table.rows]
                parts.append(self._simple_rows_to_table(rows))

        close_list()
        return parts

    # -- table helpers --

    def _extracted_table_to_html(self, t) -> str:
        rows_html: list[str] = []
        for row in t.rows:
            cells_html: list[str] = []
            for cell in row.cells:
                tag = "th" if getattr(cell, "is_header", False) else "td"
                cs = getattr(cell, "colspan", 1) or 1
                rs = getattr(cell, "rowspan", 1) or 1
                attrs = ""
                if cs > 1:
                    attrs += f' colspan="{cs}"'
                if rs > 1:
                    attrs += f' rowspan="{rs}"'
                cell_parts: list[str] = []
                for para in (cell.paragraphs or []):
                    if para.runs:
                        cell_parts.append(self._runs_to_html(para.runs))
                    elif para.text:
                        cell_parts.append(escape(para.text))
                nested = getattr(cell, "nested_table_indices", [])
                if nested:
                    ids = ", ".join(str(i) for i in nested)
                    cell_parts.append(
                        f'<span class="nested-table-note">[nested table(s): {ids}]</span>'
                    )
                cell_content = " ".join(
                    cell_parts) if cell_parts else (cell.text or "")
                cells_html.append(f"<{tag}{attrs}>{cell_content}</{tag}>")
            rows_html.append("<tr>" + "".join(cells_html) + "</tr>")
        return "<table>" + "".join(rows_html) + "</table>"

    def _simple_rows_to_table(self, rows: list) -> str:
        if not rows:
            return "<table></table>"
        cells_html: list[str] = []
        for row in rows:
            if isinstance(row, list):
                cells_html.append(
                    "<tr>" +
                    "".join(
                        f"<td>{escape(str(c))}</td>" for c in row) + "</tr>"
                )
        return "<table>" + "".join(cells_html) + "</table>"

    # -- inline run rendering --

    def _runs_to_html(self, runs: list) -> str:
        parts: list[str] = []
        for (link_url, strike), group in groupby(
            runs,
            key=lambda r: (r.hyperlink_url, getattr(r, "strikethrough", None)),
        ):
            group_runs = list(group)
            inner = "".join(
                self._inline_text(
                    r.text or "",
                    r.bold,
                    r.italic,
                    r.underline,
                    None,
                    getattr(r, "code", None),
                    getattr(r, "color_rgb", None),
                    None,
                    font_name=getattr(r, "font_name", None),
                    font_size_pt=getattr(r, "font_size_pt", None),
                )
                for r in group_runs
            )
            if strike:
                inner = f"<s>{inner}</s>"
            if link_url:
                inner = f'<a href="{escape(link_url, quote=True)}">{inner}</a>'
            parts.append(inner)
        return "".join(parts).replace("\n", "<br>")

    def _xml_runs_to_html(self, runs: list) -> str:
        parts: list[str] = []
        for run in runs:
            link = run.hyperlink_target or run.hyperlink_anchor
            parts.append(self._inline_text(
                run.text or "",
                run.bold,
                run.italic,
                run.underline,
                None,
                None,
                run.color_rgb,
                link,
                font_name=getattr(run, "font_name", None),
                font_size_pt=getattr(run, "font_size_pt", None),
            ))
        return "".join(parts).replace("\n", "<br>")

    def _inline_text(
        self,
        text: str,
        bold: bool | None,
        italic: bool | None,
        underline: bool | None,
        strikethrough: bool | None,
        code: bool | None,
        color_rgb: str | None,
        link: str | None,
        font_name: str | None = None,
        font_size_pt: float | None = None,
    ) -> str:
        out = escape(text)
        if code:
            out = f"<code>{out}</code>"
        if strikethrough:
            out = f"<s>{out}</s>"
        if underline:
            out = f"<u>{out}</u>"
        if italic:
            out = f"<em>{out}</em>"
        if bold:
            out = f"<strong>{out}</strong>"
        # Consolidate all CSS properties into a single <span style="..."> to
        # respect cascade specificity order and avoid unnecessary nesting.
        css_props: list[str] = []
        if color_rgb:
            css_props.append(f"color:{escape(color_rgb)}")
        if font_name:
            css_props.append(
                f"font-family:{escape(font_name, quote=True)}")
        if font_size_pt and font_size_pt > 0:
            css_props.append(f"font-size:{font_size_pt}pt")
        if css_props:
            style_str = ";".join(css_props)
            out = f'<span style="{style_str}">{out}</span>'
        if link:
            out = f'<a href="{escape(link, quote=True)}">{out}</a>'
        return out

    # -- heading level helper --

    def _heading_level(self, style: str | None) -> int | None:
        if not style:
            return None
        style_l = style.lower()
        for level in range(1, 7):
            if f"heading {level}" in style_l or f"heading{level}" in style_l:
                return level
        return None
