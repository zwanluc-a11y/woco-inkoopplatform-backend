from __future__ import annotations
from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict


class ImportUploadResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    file_name: str
    file_type: str
    status: str
    detected_columns: list[str]
    suggested_mapping: dict[str, Optional[str]]
    preview_rows: list[dict[str, Any]]
    created_at: datetime


class ImportConfirmRequest(BaseModel):
    import_session_id: int
    column_mapping: dict[str, str]
    year: Optional[int] = None


class ImportStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    file_name: str
    file_type: str
    year: Optional[int] = None
    row_count: Optional[int] = None
    status: str
    column_mapping: Optional[dict[str, Optional[str]]] = None
    error_log: Optional[str] = None
    progress_current: Optional[int] = None
    progress_total: Optional[int] = None
    uploaded_by: int
    created_at: datetime
