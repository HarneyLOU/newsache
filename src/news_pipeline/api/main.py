from __future__ import annotations

from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query
from psycopg.rows import DictRow

from news_pipeline.api.dependencies import get_db_connection
from news_pipeline.api.repositories import (
    get_article,
    list_articles,
    list_ingestion_runs,
    list_sources,
    upsert_article_state,
)
from news_pipeline.api.schemas import (
    ArticleDetailOut,
    ArticleListItemOut,
    ArticleStateOut,
    ArticleStateUpdateIn,
    IngestionRunOut,
    SourceOut,
)


app = FastAPI(
    title="Personal News Intelligence API",
    version="0.1.0",
)


DbConnection = Annotated[psycopg.Connection, Depends(get_db_connection)]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/sources", response_model=list[SourceOut])
def get_sources(conn: DbConnection) -> list[dict]:
    return list_sources(conn)


@app.get("/articles", response_model=list[ArticleListItemOut])
def get_articles(
    conn: DbConnection,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    source_id: int | None = None,
    category: str | None = None,
    status: Annotated[str | None, Query(pattern="^(unread|read|archived)$")] = None,
    saved: bool | None = None,
) -> list[dict]:
    return list_articles(
        conn,
        limit=limit,
        offset=offset,
        source_id=source_id,
        category=category,
        status=status,
        saved=saved,
    )


@app.get("/articles/{article_id}", response_model=ArticleDetailOut)
def get_article_detail(
    article_id: int,
    conn: DbConnection,
) -> dict:
    article = get_article(conn, article_id)

    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return article


@app.patch("/articles/{article_id}/state", response_model=ArticleStateOut)
def update_article_state(
    article_id: int,
    payload: ArticleStateUpdateIn,
    conn: DbConnection,
) -> DictRow | None:
    article = get_article(conn, article_id)

    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return upsert_article_state(
        conn,
        article_id=article_id,
        status=payload.status,
        is_saved=payload.is_saved,
        user_rating=payload.user_rating,
    )


@app.get("/ingestion-runs", response_model=list[IngestionRunOut])
def get_ingestion_runs(
    conn: DbConnection,
    limit: Annotated[int, Query(ge=1, le=100)] = 30,
) -> list[dict]:
    return list_ingestion_runs(conn, limit=limit)