from __future__ import annotations

import psycopg


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    asset_name TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    sources_fetched INTEGER NOT NULL DEFAULT 0,
    entries_parsed INTEGER NOT NULL DEFAULT 0,
    items_inserted INTEGER NOT NULL DEFAULT 0,
    pages_fetched INTEGER NOT NULL DEFAULT 0,
    pages_failed INTEGER NOT NULL DEFAULT 0,
    articles_extracted INTEGER NOT NULL DEFAULT 0,
    articles_failed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL UNIQUE,
    category TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rss_feed_items (
    id BIGSERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    entry_id TEXT,
    url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    author TEXT,
    published_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT rss_feed_items_unique_hash UNIQUE (content_hash),
    CONSTRAINT rss_feed_items_unique_canonical_per_source UNIQUE (source_id, canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_rss_feed_items_source_id
ON rss_feed_items(source_id);

CREATE INDEX IF NOT EXISTS idx_rss_feed_items_published_at
ON rss_feed_items(published_at DESC);

CREATE TABLE IF NOT EXISTS article_pages (
    id BIGSERIAL PRIMARY KEY,
    feed_item_id BIGINT NOT NULL REFERENCES rss_feed_items(id),
    url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    final_url TEXT,
    http_status INTEGER,
    raw_html TEXT,
    raw_html_object_key TEXT,
    content_type TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fetch_status TEXT NOT NULL,
    error_message TEXT,

    CONSTRAINT article_pages_unique_feed_item UNIQUE (feed_item_id)
);

CREATE INDEX IF NOT EXISTS idx_article_pages_fetch_status
ON article_pages(fetch_status);

CREATE TABLE IF NOT EXISTS clean_articles (
    id BIGSERIAL PRIMARY KEY,
    feed_item_id BIGINT NOT NULL REFERENCES rss_feed_items(id),
    article_page_id BIGINT NOT NULL REFERENCES article_pages(id),
    source_id INTEGER NOT NULL REFERENCES sources(id),
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    clean_text TEXT,
    language TEXT,
    word_count INTEGER,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extraction_status TEXT NOT NULL,
    error_message TEXT,

    CONSTRAINT clean_articles_unique_feed_item UNIQUE (feed_item_id)
);

CREATE INDEX IF NOT EXISTS idx_clean_articles_source_id
ON clean_articles(source_id);

CREATE INDEX IF NOT EXISTS idx_clean_articles_extraction_status
ON clean_articles(extraction_status);

CREATE INDEX IF NOT EXISTS idx_clean_articles_extracted_at
ON clean_articles(extracted_at DESC);

CREATE TABLE IF NOT EXISTS article_reading_state (
    id BIGSERIAL PRIMARY KEY,
    clean_article_id BIGINT NOT NULL REFERENCES clean_articles(id),
    status TEXT NOT NULL DEFAULT 'unread',
    is_saved BOOLEAN NOT NULL DEFAULT FALSE,
    user_rating INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT article_reading_state_unique_article UNIQUE (clean_article_id),
    CONSTRAINT article_reading_state_status_check
        CHECK (status IN ('unread', 'read', 'archived'))
);

CREATE INDEX IF NOT EXISTS idx_article_reading_state_status
ON article_reading_state(status);

CREATE INDEX IF NOT EXISTS idx_article_reading_state_is_saved
ON article_reading_state(is_saved);
"""


def get_connection(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def ensure_schema(database_url: str) -> None:
    with get_connection(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()