import json
from datetime import datetime, timezone
from typing import Any

import feedparser
import httpx
import trafilatura
import yaml
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dateutil import parser as date_parser
from tenacity import retry, stop_after_attempt, wait_exponential

from news_pipeline.db import ensure_schema, get_connection
from news_pipeline.resources import PostgresResource, SourcesConfigResource
from news_pipeline.utils import normalize_url, stable_hash


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        parsed = date_parser.parse(str(value))
    except Exception:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed


def as_non_empty_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    return normalized or None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
def fetch_url(url: str) -> httpx.Response:
    headers = {
        "User-Agent": "personal-news-intelligence/0.1 (+https://localhost)"
    }

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
        return response


def load_sources_from_yaml(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = yaml.safe_load(f)

    sources = payload.get("sources", [])
    if not isinstance(sources, list):
        raise ValueError("sources.yaml must contain a top-level 'sources' list")

    return sources


def start_ingestion_run(
    database_url: str,
    *,
    pipeline_name: str,
    asset_name: str,
) -> int:
    with get_connection(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (pipeline_name, asset_name, status)
                VALUES (%s, %s, 'running')
                RETURNING id
                """,
                (pipeline_name, asset_name),
            )
            run_row = cur.fetchone()
            if run_row is None:
                raise RuntimeError("Failed to create ingestion run row")

            run_id_value = run_row[0]
            if not isinstance(run_id_value, int):
                raise TypeError(f"Unexpected ingestion run id type: {type(run_id_value)!r}")

            run_id = run_id_value
        conn.commit()

    return run_id


def finish_ingestion_run(
    database_url: str,
    *,
    run_id: int,
    status: str,
    sources_fetched: int = 0,
    entries_parsed: int = 0,
    items_inserted: int = 0,
    pages_fetched: int = 0,
    pages_failed: int = 0,
    articles_extracted: int = 0,
    articles_failed: int = 0,
    error_message: str | None = None,
) -> None:
    with get_connection(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion_runs
                SET
                    finished_at = NOW(),
                    status = %s,
                    sources_fetched = %s,
                    entries_parsed = %s,
                    items_inserted = %s,
                    pages_fetched = %s,
                    pages_failed = %s,
                    articles_extracted = %s,
                    articles_failed = %s,
                    error_message = %s
                WHERE id = %s
                """,
                (
                    status,
                    sources_fetched,
                    entries_parsed,
                    items_inserted,
                    pages_fetched,
                    pages_failed,
                    articles_extracted,
                    articles_failed,
                    error_message,
                    run_id,
                ),
            )
        conn.commit()


@asset
def schema(postgres: PostgresResource) -> MaterializeResult:
    ensure_schema(postgres.database_url)

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text("schema ensured"),
        }
    )


@asset(deps=[schema])
def sources(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    sources_config: SourcesConfigResource,
) -> MaterializeResult:
    configured_sources = load_sources_from_yaml(sources_config.path)

    inserted_or_updated = 0

    with get_connection(postgres.database_url) as conn:
        with conn.cursor() as cur:
            for source in configured_sources:
                cur.execute(
                    """
                    INSERT INTO sources (name, url, category, is_active, updated_at)
                    VALUES (%s, %s, %s, TRUE, NOW())
                    ON CONFLICT (url)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        is_active = TRUE,
                        updated_at = NOW()
                    """,
                    (
                        source["name"],
                        source["url"],
                        source.get("category"),
                    ),
                )
                inserted_or_updated += 1

        conn.commit()

    context.log.info("Loaded %s sources", inserted_or_updated)

    return MaterializeResult(
        metadata={
            "sources_loaded": MetadataValue.int(inserted_or_updated),
        }
    )


@asset(deps=[sources])
def rss_feed_items(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    run_id = start_ingestion_run(
        postgres.database_url,
        pipeline_name="news_ingestion",
        asset_name="rss_feed_items",
    )

    fetched_sources = 0
    parsed_entries = 0
    inserted_items = 0
    failed_sources = 0

    try:
        with get_connection(postgres.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, url
                    FROM sources
                    WHERE is_active = TRUE
                    ORDER BY name
                    """
                )
                active_sources = cur.fetchall()

            for source_id, source_name, source_url in active_sources:
                fetched_sources += 1
                context.log.info("Fetching RSS source: %s <%s>", source_name, source_url)

                try:
                    response = fetch_url(source_url)
                    parsed_feed = feedparser.parse(response.content)
                except Exception as exc:
                    failed_sources += 1
                    context.log.warning(
                        "Failed to fetch or parse source %s: %s",
                        source_name,
                        exc,
                    )
                    continue

                if getattr(parsed_feed, "bozo", False):
                    context.log.warning(
                        "Feed parser warning for %s: %s",
                        source_name,
                        getattr(parsed_feed, "bozo_exception", None),
                    )

                entries = parsed_feed.entries or []
                parsed_entries += len(entries)

                with conn.cursor() as cur:
                    for entry in entries:
                        url = as_non_empty_str(entry.get("link"))
                        title = as_non_empty_str(entry.get("title"))

                        if not url or not title:
                            continue

                        canonical_url = normalize_url(url)
                        entry_id = (
                            as_non_empty_str(entry.get("id"))
                            or as_non_empty_str(entry.get("guid"))
                            or canonical_url
                        )
                        summary = entry.get("summary")
                        author = entry.get("author")

                        published_raw = (
                            entry.get("published")
                            or entry.get("updated")
                            or entry.get("created")
                        )
                        published_at = parse_datetime(published_raw)

                        raw_payload = json.loads(json.dumps(entry, default=str))

                        content_hash = stable_hash(
                            str(source_id),
                            entry_id,
                            canonical_url,
                            title,
                        )

                        cur.execute(
                            """
                            INSERT INTO rss_feed_items (
                                source_id,
                                entry_id,
                                url,
                                canonical_url,
                                title,
                                summary,
                                author,
                                published_at,
                                raw_payload,
                                content_hash
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                            """,
                            (
                                source_id,
                                entry_id,
                                url,
                                canonical_url,
                                title,
                                summary,
                                author,
                                published_at,
                                json.dumps(raw_payload),
                                content_hash,
                            ),
                        )

                        inserted = cur.fetchone()
                        if inserted:
                            inserted_items += 1

            conn.commit()

        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="success",
            sources_fetched=fetched_sources,
            entries_parsed=parsed_entries,
            items_inserted=inserted_items,
        )

    except Exception as exc:
        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="failed",
            sources_fetched=fetched_sources,
            entries_parsed=parsed_entries,
            items_inserted=inserted_items,
            error_message=str(exc),
        )
        raise

    return MaterializeResult(
        metadata={
            "sources_fetched": MetadataValue.int(fetched_sources),
            "entries_parsed": MetadataValue.int(parsed_entries),
            "items_inserted": MetadataValue.int(inserted_items),
            "failed_sources": MetadataValue.int(failed_sources),
            "ingestion_run_id": MetadataValue.int(run_id),
        }
    )


@asset(deps=[rss_feed_items])
def article_pages(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    run_id = start_ingestion_run(
        postgres.database_url,
        pipeline_name="news_ingestion",
        asset_name="article_pages",
    )

    pages_fetched = 0
    pages_failed = 0

    try:
        with get_connection(postgres.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.id,
                        r.url,
                        r.canonical_url
                    FROM rss_feed_items r
                    LEFT JOIN article_pages p
                        ON p.feed_item_id = r.id
                    WHERE p.id IS NULL
                    ORDER BY r.published_at DESC NULLS LAST, r.id DESC
                    LIMIT 100
                    """
                )
                pending_items = cur.fetchall()

            context.log.info("Found %s article pages to fetch", len(pending_items))

            for feed_item_id, url, canonical_url in pending_items:
                try:
                    response = fetch_url(url)

                    content_type = response.headers.get("content-type")
                    raw_html = response.text
                    final_url = str(response.url)
                    http_status = response.status_code

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO article_pages (
                                feed_item_id,
                                url,
                                canonical_url,
                                final_url,
                                http_status,
                                raw_html,
                                content_type,
                                fetch_status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'success')
                            ON CONFLICT (feed_item_id)
                            DO NOTHING
                            """,
                            (
                                feed_item_id,
                                url,
                                canonical_url,
                                final_url,
                                http_status,
                                raw_html,
                                content_type,
                            ),
                        )

                    pages_fetched += 1

                except Exception as exc:
                    context.log.warning(
                        "Failed to fetch article page %s: %s",
                        url,
                        exc,
                    )

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO article_pages (
                                feed_item_id,
                                url,
                                canonical_url,
                                fetch_status,
                                error_message
                            )
                            VALUES (%s, %s, %s, 'failed', %s)
                            ON CONFLICT (feed_item_id)
                            DO UPDATE SET
                                fetch_status = 'failed',
                                error_message = EXCLUDED.error_message,
                                fetched_at = NOW()
                            """,
                            (
                                feed_item_id,
                                url,
                                canonical_url,
                                str(exc),
                            ),
                        )

                    pages_failed += 1

            conn.commit()

        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="success",
            pages_fetched=pages_fetched,
            pages_failed=pages_failed,
        )

    except Exception as exc:
        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="failed",
            pages_fetched=pages_fetched,
            pages_failed=pages_failed,
            error_message=str(exc),
        )
        raise

    return MaterializeResult(
        metadata={
            "pages_fetched": MetadataValue.int(pages_fetched),
            "pages_failed": MetadataValue.int(pages_failed),
            "ingestion_run_id": MetadataValue.int(run_id),
        }
    )


@asset(deps=[article_pages])
def clean_articles(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    run_id = start_ingestion_run(
        postgres.database_url,
        pipeline_name="news_ingestion",
        asset_name="clean_articles",
    )

    articles_extracted = 0
    articles_failed = 0

    try:
        with get_connection(postgres.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        p.id AS article_page_id,
                        p.feed_item_id,
                        r.source_id,
                        r.canonical_url,
                        r.title,
                        p.raw_html
                    FROM article_pages p
                    JOIN rss_feed_items r
                        ON r.id = p.feed_item_id
                    LEFT JOIN clean_articles c
                        ON c.feed_item_id = p.feed_item_id
                    WHERE
                        p.fetch_status = 'success'
                        AND p.raw_html IS NOT NULL
                        AND c.id IS NULL
                    ORDER BY p.fetched_at DESC
                    LIMIT 100
                    """
                )
                pending_pages = cur.fetchall()

            context.log.info("Found %s article pages to extract", len(pending_pages))

            for (
                article_page_id,
                feed_item_id,
                source_id,
                canonical_url,
                title,
                raw_html,
            ) in pending_pages:
                try:
                    clean_text = trafilatura.extract(
                        raw_html,
                        include_comments=False,
                        include_tables=False,
                    )

                    if not clean_text:
                        raise ValueError("No clean text extracted")

                    words = clean_text.split()
                    word_count = len(words)

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO clean_articles (
                                feed_item_id,
                                article_page_id,
                                source_id,
                                canonical_url,
                                title,
                                clean_text,
                                word_count,
                                extraction_status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'success')
                            ON CONFLICT (feed_item_id)
                            DO NOTHING
                            """,
                            (
                                feed_item_id,
                                article_page_id,
                                source_id,
                                canonical_url,
                                title,
                                clean_text,
                                word_count,
                            ),
                        )

                    articles_extracted += 1

                except Exception as exc:
                    context.log.warning(
                        "Failed to extract article feed_item_id=%s: %s",
                        feed_item_id,
                        exc,
                    )

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO clean_articles (
                                feed_item_id,
                                article_page_id,
                                source_id,
                                canonical_url,
                                title,
                                extraction_status,
                                error_message
                            )
                            VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                            ON CONFLICT (feed_item_id)
                            DO UPDATE SET
                                extraction_status = 'failed',
                                error_message = EXCLUDED.error_message,
                                extracted_at = NOW()
                            """,
                            (
                                feed_item_id,
                                article_page_id,
                                source_id,
                                canonical_url,
                                title,
                                str(exc),
                            ),
                        )

                    articles_failed += 1

            conn.commit()

        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="success",
            articles_extracted=articles_extracted,
            articles_failed=articles_failed,
        )

    except Exception as exc:
        finish_ingestion_run(
            postgres.database_url,
            run_id=run_id,
            status="failed",
            articles_extracted=articles_extracted,
            articles_failed=articles_failed,
            error_message=str(exc),
        )
        raise

    return MaterializeResult(
        metadata={
            "articles_extracted": MetadataValue.int(articles_extracted),
            "articles_failed": MetadataValue.int(articles_failed),
            "ingestion_run_id": MetadataValue.int(run_id),
        }
    )
