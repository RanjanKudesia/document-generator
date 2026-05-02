"""Pydantic schemas for the Document Generator service."""
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ListInfo(BaseModel):
    """List formatting metadata for a paragraph."""

    kind: Literal["bullet", "numbered"] | None = None
    numbering_format: str | None = None
    level: int = 0
    start: int | None = None
    model_config = ConfigDict(extra="ignore")


class SourceInfo(BaseModel):
    """Source HTML tag/attribute metadata for an extracted element."""

    format: str | None = None
    tag: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
    raw_html: str | None = None
    model_config = ConfigDict(extra="ignore")


class HtmlMetadata(BaseModel):
    """HTML document-level metadata extracted from the source file."""

    source_type: str | None = None
    extraction_mode: str | None = None
    title: str | None = None
    doctype: str | None = None
    full_html: str | None = None
    head_html: str | None = None
    body_html: str | None = None
    html_attributes: dict[str, Any] = Field(default_factory=dict)
    body_attributes: dict[str, Any] = Field(default_factory=dict)
    style_blocks: list[str] = Field(default_factory=list)
    meta_tags: list[dict[str, Any]] = Field(default_factory=list)
    link_tags: list[dict[str, Any]] = Field(default_factory=list)
    script_blocks: list[dict[str, Any]] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ParagraphBlock(BaseModel):
    """A simple paragraph or heading block for legacy block-based payloads."""

    type: Literal["paragraph"]
    text: str
    heading_level: int | None = Field(default=None, ge=1, le=9)
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    font_name: str | None = None
    font_size_pt: float | None = Field(default=None, gt=0)
    alignment: Literal["LEFT", "CENTER", "RIGHT", "JUSTIFY"] | None = None


class TableBlock(BaseModel):
    """A simple table block for legacy block-based payloads."""

    type: Literal["table"]
    rows: list[list[str]]
    style: str | None = None


DocumentBlock = Annotated[ParagraphBlock |
                          TableBlock, Field(discriminator="type")]


class ExtractedMediaItem(BaseModel):
    """An extracted media item (image/video) with optional base64 data."""

    relationship_id: str | None = None
    content_type: str | None = None
    file_name: str | None = None
    local_file_path: str | None = None
    local_url: str | None = None
    width_emu: int | None = None
    height_emu: int | None = None
    alt_text: str | None = None
    source: dict | None = None
    base64_data: str | None = None
    base64: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedRun(BaseModel):
    """A run of text with uniform formatting within a paragraph."""

    index: int | None = None
    text: str | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    strikethrough: bool | None = None
    code: bool | None = None
    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    hyperlink_url: str | None = None
    embedded_media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedStyleFont(BaseModel):
    """Font properties for a named document style."""

    name: str | None = None
    size_pt: float | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedStyle(BaseModel):
    """A named style definition extracted from the source document."""

    style_id: str | None = None
    name: str | None = None
    type: str | None = None
    font: ExtractedStyleFont | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedDocumentDefaults(BaseModel):
    """Document-level default font and color settings."""

    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedParagraph(BaseModel):
    """A fully parsed paragraph with runs, list info, and source metadata."""

    index: int
    text: str | None = None
    style: str | None = None
    code_fence_language: str | None = None
    is_bullet: bool | None = None
    is_numbered: bool | None = None
    list_info: ListInfo | None = None
    numbering_format: str | None = None
    list_level: int | None = None
    alignment: str | None = None
    direction: str | None = None
    space_before_pt: float | None = None
    space_after_pt: float | None = None
    line_spacing: float | None = None
    page_index: int | None = None
    runs: list[ExtractedRun] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedTableCell(BaseModel):
    """A single cell in an extracted table, possibly containing nested content."""

    text: str | None = None
    paragraphs: list["ExtractedParagraph"] = Field(default_factory=list)
    tables: list["ExtractedTable"] = Field(default_factory=list)
    is_header: bool | None = None
    cell_index: int | None = None
    colspan: int | None = None
    rowspan: int | None = None
    nested_table_indices: list[int] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedTableRow(BaseModel):
    """A row of cells in an extracted table."""

    cells: list[ExtractedTableCell] = Field(default_factory=list)
    row_index: int | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedTable(BaseModel):
    """An extracted table with rows, style, and source metadata."""

    index: int
    row_count: int | None = None
    column_count: int | None = None
    style: str | None = None
    rows: list[ExtractedTableRow] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedOrderItem(BaseModel):
    """An entry in the document_order list specifying element type and index."""

    type: Literal["paragraph", "table", "media"]
    index: int


class ExtractedData(BaseModel):
    """Top-level structured extraction result for JSON-adapter outputs."""

    metadata: HtmlMetadata | dict[str, Any] | None = None
    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    sections: list[dict] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlPart(BaseModel):
    """A raw XML part (file path + content) from a DOCX/PPTX archive."""

    path: str
    xml: str
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlRun(BaseModel):
    """A run of text extracted from raw XML with formatting and hyperlink info."""

    index: int | None = None
    text: str | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    color_rgb: str | None = None
    font_name: str | None = None
    font_size_pt: float | None = None
    hyperlink_rid: str | None = None
    hyperlink_target: str | None = None
    hyperlink_anchor: str | None = None
    embedded_media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlParagraph(BaseModel):
    """A paragraph extracted from raw XML with style and list metadata."""

    text: str | None = None
    style_id: str | None = None
    alignment: str | None = None
    is_bullet: bool | None = None
    is_numbered: bool | None = None
    list_level: int | None = None
    list_number_id: int | None = None
    list_format: str | None = None
    runs: list[ExtractedXmlRun] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlTableCell(BaseModel):
    """A table cell extracted from raw XML."""

    row_index: int | None = None
    cell_index: int | None = None
    text: str | None = None
    paragraphs: list[ExtractedXmlParagraph] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlTableRow(BaseModel):
    """A table row extracted from raw XML."""

    row_index: int | None = None
    cells: list[ExtractedXmlTableCell] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlTable(BaseModel):
    """A table extracted from raw XML."""

    rows: list[ExtractedXmlTableRow] = Field(default_factory=list)
    row_count: int | None = None
    column_count: int | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlBodyItem(BaseModel):
    """A discriminated-union item in the XML body (paragraph or table)."""

    type: Literal["paragraph", "table"]
    index: int
    paragraph: ExtractedXmlParagraph | None = None
    table: ExtractedXmlTable | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlData(BaseModel):
    """Top-level structured extraction result for XML-pipeline outputs."""

    format: str | None = None
    metadata: dict | None = None
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    relationships: dict[str, str] = Field(default_factory=dict)
    parsed_body: list[ExtractedXmlBodyItem] = Field(default_factory=list)
    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedPptSlide(BaseModel):
    """High-level metadata and indices for a single PowerPoint slide."""

    index: int | None = None
    slide_number: int | None = None
    slide_id: int | None = None
    path: str | None = None
    title: str | None = None
    text: str | None = None
    notes_text: str | None = None
    paragraph_indices: list[int] = Field(default_factory=list)
    table_indices: list[int] = Field(default_factory=list)
    media_indices: list[int] = Field(default_factory=list)
    shape_count: int | None = None
    image_count: int | None = None
    table_count: int | None = None
    relationships: dict | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedPptParsedSlide(BaseModel):
    """Fully parsed content (shapes, notes, relationships) for a single slide."""

    index: int | None = None
    slide_id: int | None = None
    rid: str | None = None
    path: str | None = None
    relationships_path: str | None = None
    relationships: dict | None = None
    title: str | None = None
    text: str | None = None
    shape_count: int | None = None
    image_count: int | None = None
    table_count: int | None = None
    shapes: list[dict] = Field(default_factory=list)
    notes: dict | None = None
    parse_error: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedPptData(BaseModel):
    """Top-level extraction result for PowerPoint files."""

    format: str | None = None
    document_type: Literal["pptx"]
    metadata: dict | None = None

    # JSON-adapter shape
    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    styles: list[ExtractedStyle] = Field(default_factory=list)
    numbering: list[dict] = Field(default_factory=list)
    sections: list[dict] = Field(default_factory=list)
    slides: list[ExtractedPptSlide] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)

    # XML-pipeline shape
    content_types: dict | None = None
    presentation: dict | None = None
    presentation_relationships: dict | None = None
    parsed_slides: list[ExtractedPptParsedSlide] = Field(default_factory=list)
    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    binary_parts: list[dict] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")


class DocumentGenerationRequest(BaseModel):
    """Request body for document generation (POST /generate)."""

    content_id: str = Field(
        description="Content ID returned by the extractor service.",
        examples=["69f64331423c9bfe1bf883a1"],
    )
    version: int = Field(
        description="Version number of the extracted content to render.",
        examples=[0],
    )
    force_regenerate: bool = Field(
        default=False,
        description="When true, bypass cache and regenerate the document even if one exists.",
    )


class ResolvedDocumentGenerationPayload(BaseModel):
    """Fully resolved payload including extracted data and upload metadata."""

    content_id: str
    version: int
    source_content_updated_at: str | None = None
    document_name: str | None = None
    title: str | None = None
    blocks: list[DocumentBlock] = Field(default_factory=list)

    # Content-extractor payload compatibility
    original_filename: str | None = None
    stored_filename: str | None = None
    extension: str | None = None
    extracted_data: ExtractedPptData | ExtractedData | ExtractedXmlData | None = None
    json_file_path: str | None = None


class DocumentGenerationResponse(BaseModel):
    """Response body for a successful document generation request."""

    id: str = Field(
        description="Generated document record ID in MongoDB.",
        examples=["69f644f6423c9bfe1bf883c7"],
    )
    file_name: str = Field(
        description="Generated output filename.",
        examples=["invoice.docx"],
    )
    output_file_s3_key: str = Field(
        description="S3 key for generated output file.",
        examples=["document-generator/generated/content-id/version-0/invoice.docx"],
    )
    download_url: str = Field(
        description="Presigned download URL for the generated output.",
    )
    url_expires_in_seconds: int = Field(
        default=3600,
        description="Presigned URL validity in seconds.",
        examples=[3600],
    )
    url_expires_at: str = Field(
        description="ISO-8601 UTC expiry timestamp for download_url.",
    )
    extension: str = Field(
        default="docx",
        description="Normalized generated file extension.",
        examples=["docx"],
    )


class HealthResponse(BaseModel):
    """Basic service health status response."""

    status: str = Field(description="Overall health status.", examples=["ok"])
    service: str = Field(description="Service name.",
                         examples=["document-generator"])


class DependencyStatus(BaseModel):
    """Reachability status for downstream dependencies."""

    s3: bool = Field(description="Whether S3 is reachable.")
    mongodb: bool = Field(description="Whether MongoDB is reachable.")


class HealthWithDependenciesResponse(BaseModel):
    """Health status response including dependency reachability."""

    status: str = Field(description="Overall health status.", examples=["ok"])
    service: str = Field(description="Service name.",
                         examples=["document-generator"])
    dependencies: DependencyStatus


class GeneratedDocumentRecord(BaseModel):
    """A stored generated document record from MongoDB."""

    id: str = Field(description="MongoDB document ID.",
                    examples=["69f644f6423c9bfe1bf883c7"])
    content_id: str
    version: int
    file_name: str
    extension: str
    output_file_s3_key: str
    source_content_updated_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class GeneratedDocumentListResponse(BaseModel):
    """Paginated list of generated document records."""

    items: list[GeneratedDocumentRecord]
    total: int
    limit: int
    offset: int


class BatchGenerateItem(BaseModel):
    """A single item in a batch generation request."""

    content_id: str
    version: int


class BatchGenerateResult(BaseModel):
    """Result for a single item in a batch generation response."""

    content_id: str
    version: int
    extension: str | None
    status: Literal["success", "error"]
    result: "DocumentGenerationResponse | None" = None
    error: str | None = None


class BatchGenerateResponse(BaseModel):
    """Response body for a batch document generation request."""

    results: list[BatchGenerateResult]
    total: int
    succeeded: int
    failed: int


class GenerationCapabilitiesResponse(BaseModel):
    """Service capabilities and configuration limits."""

    supported_extensions: list[str]
    max_json_bytes: int
    max_media_bytes: int
    max_hydration_depth: int
    max_hydration_nodes: int
    url_expiry_seconds: int
    force_regenerate_supported: bool = True


class DeleteGeneratedResponse(BaseModel):
    """Response body for a delete generated document request."""

    id: str
    deleted_s3_key: str
    message: str


class FreshDownloadUrlResponse(BaseModel):
    """Response body for a fresh presigned download URL request."""

    id: str
    download_url: str
    url_expires_in_seconds: int
    url_expires_at: str
