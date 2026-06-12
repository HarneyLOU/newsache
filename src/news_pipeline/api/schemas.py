from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SourceOut(BaseModel):
    id: int
    name: str
    url: str
    category: str | None
    is_active: bool


class ArticleListItemOut(BaseModel):
    id: int
    title: str
    source_name: str
    category: str | None
    canonical_url: str
    published_at: datetime | None
    extracted_at: datetime
    word_count: int | None
    reading_status: str = "unread"
    is_saved: bool = False


class ArticleDetailOut(BaseModel):
    id: int
    title: str
    source_name: str
    category: str | None
    canonical_url: str
    published_at: datetime | None
    extracted_at: datetime
    word_count: int | None
    clean_text: str | None
    reading_status: str = "unread"
    is_saved: bool = False


class ArticleStateUpdateIn(BaseModel):
    status: str | None = Field(default=None, pattern="^(unread|read|archived)$")
    is_saved: bool | None = None
    user_rating: int | None = Field(default=None, ge=1, le=5)


class ArticleStateOut(BaseModel):
    clean_article_id: int
    status: str
    is_saved: bool
    user_rating: int | None


class IngestionRunOut(BaseModel):
    id: int
    pipeline_name: str
    asset_name: str | None
    started_at: datetime
    finished_at: datetime | None
    status: str
    sources_fetched: int
    entries_parsed: int
    items_inserted: int
    pages_fetched: int
    pages_failed: int
    articles_extracted: int
    articles_failed: int
    error_message: str | None