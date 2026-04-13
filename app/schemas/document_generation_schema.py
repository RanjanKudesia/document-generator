from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class ParagraphBlock(BaseModel):
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
    type: Literal["table"]
    rows: list[list[str]]
    style: str | None = None


DocumentBlock = Annotated[ParagraphBlock |
                          TableBlock, Field(discriminator="type")]


class ExtractedMediaItem(BaseModel):
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
    name: str | None = None
    size_pt: float | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedStyle(BaseModel):
    style_id: str | None = None
    name: str | None = None
    type: str | None = None
    font: ExtractedStyleFont | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedDocumentDefaults(BaseModel):
    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedParagraph(BaseModel):
    index: int
    text: str | None = None
    style: str | None = None
    is_bullet: bool | None = None
    is_numbered: bool | None = None
    list_info: dict | None = None
    numbering_format: str | None = None
    list_level: int | None = None
    alignment: str | None = None
    direction: str | None = None
    runs: list[ExtractedRun] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedTableCell(BaseModel):
    text: str | None = None
    paragraphs: list["ExtractedParagraph"] = Field(default_factory=list)
    tables: list["ExtractedTable"] = Field(default_factory=list)
    is_header: bool | None = None
    colspan: int | None = None
    rowspan: int | None = None
    nested_table_indices: list[int] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedTableRow(BaseModel):
    cells: list[ExtractedTableCell] = Field(default_factory=list)
    row_index: int | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedTable(BaseModel):
    index: int
    row_count: int | None = None
    column_count: int | None = None
    style: str | None = None
    rows: list[ExtractedTableRow] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedOrderItem(BaseModel):
    type: Literal["paragraph", "table", "media"]
    index: int


class ExtractedData(BaseModel):
    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlPart(BaseModel):
    path: str
    xml: str
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlRun(BaseModel):
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
    row_index: int | None = None
    cell_index: int | None = None
    text: str | None = None
    paragraphs: list[ExtractedXmlParagraph] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlTableRow(BaseModel):
    row_index: int | None = None
    cells: list[ExtractedXmlTableCell] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlTable(BaseModel):
    rows: list[ExtractedXmlTableRow] = Field(default_factory=list)
    row_count: int | None = None
    column_count: int | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlBodyItem(BaseModel):
    type: Literal["paragraph", "table"]
    index: int
    paragraph: ExtractedXmlParagraph | None = None
    table: ExtractedXmlTable | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlData(BaseModel):
    format: str | None = None
    metadata: dict | None = None
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    relationships: dict[str, str] = Field(default_factory=dict)
    parsed_body: list[ExtractedXmlBodyItem] = Field(default_factory=list)
    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedPptSlide(BaseModel):
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
    content_id: str
    version: int


class ResolvedDocumentGenerationPayload(BaseModel):
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
    id: str
    file_name: str
    output_file_s3_key: str
    download_url: str
    url_expires_in_seconds: int = 3600
    url_expires_at: str
    extension: str = "docx"


class HealthResponse(BaseModel):
    status: str
    service: str
