from __future__ import annotations

import psycopg
from psycopg import sql
from psycopg.rows import DictRow, dict_row


def list_articles(
    conn: psycopg.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    source_id: int | None = None,
    category: str | None = None,
    status: str | None = None,
    saved: bool | None = None,
) -> list[dict]:
    filters = [sql.SQL("c.extraction_status = 'success'")]
    params: list[object] = []

    if source_id is not None:
        filters.append(sql.SQL("c.source_id = %s"))
        params.append(source_id)

    if category is not None:
        filters.append(sql.SQL("s.category = %s"))
        params.append(category)

    if status is not None:
        filters.append(sql.SQL("COALESCE(rs.status, 'unread') = %s"))
        params.append(status)

    if saved is not None:
        filters.append(sql.SQL("COALESCE(rs.is_saved, FALSE) = %s"))
        params.append(saved)

    params.extend([limit, offset])

    query = sql.SQL("""
        SELECT
            c.id,
            c.title,
            s.name AS source_name,
            s.category,
            c.canonical_url,
            r.published_at,
            c.extracted_at,
            c.word_count,
            COALESCE(rs.status, 'unread') AS reading_status,
            COALESCE(rs.is_saved, FALSE) AS is_saved
        FROM clean_articles c
        JOIN sources s
            ON s.id = c.source_id
        JOIN rss_feed_items r
            ON r.id = c.feed_item_id
        LEFT JOIN article_reading_state rs
            ON rs.clean_article_id = c.id
        WHERE {where_clause}
        ORDER BY r.published_at DESC NULLS LAST, c.extracted_at DESC
        LIMIT %s OFFSET %s
    """).format(where_clause=sql.SQL(" AND ").join(filters))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def get_article(conn: psycopg.Connection, article_id: int) -> dict | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                c.id,
                c.title,
                s.name AS source_name,
                s.category,
                c.canonical_url,
                r.published_at,
                c.extracted_at,
                c.word_count,
                c.clean_text,
                COALESCE(rs.status, 'unread') AS reading_status,
                COALESCE(rs.is_saved, FALSE) AS is_saved
            FROM clean_articles c
            JOIN sources s
                ON s.id = c.source_id
            JOIN rss_feed_items r
                ON r.id = c.feed_item_id
            LEFT JOIN article_reading_state rs
                ON rs.clean_article_id = c.id
            WHERE c.id = %s
            """,
            (article_id,),
        )
        return cur.fetchone()


def list_sources(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                url,
                category,
                is_active
            FROM sources
            ORDER BY category NULLS LAST, name
            """
        )
        return list(cur.fetchall())


def list_ingestion_runs(
    conn: psycopg.Connection,
    *,
    limit: int = 30,
) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                id,
                pipeline_name,
                asset_name,
                started_at,
                finished_at,
                status,
                sources_fetched,
                entries_parsed,
                items_inserted,
                pages_fetched,
                pages_failed,
                articles_extracted,
                articles_failed,
                error_message
            FROM ingestion_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return list(cur.fetchall())


def upsert_article_state(
    conn: psycopg.Connection,
    *,
    article_id: int,
    status: str | None,
    is_saved: bool | None,
    user_rating: int | None,
) -> DictRow | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            INSERT INTO article_reading_state (
                clean_article_id,
                status,
                is_saved,
                user_rating,
                updated_at
            )
            VALUES (
                %s,
                COALESCE(%s, 'unread'),
                COALESCE(%s, FALSE),
                %s,
                NOW()
            )
            ON CONFLICT (clean_article_id)
            DO UPDATE SET
                status = COALESCE(EXCLUDED.status, article_reading_state.status),
                is_saved = COALESCE(EXCLUDED.is_saved, article_reading_state.is_saved),
                user_rating = COALESCE(EXCLUDED.user_rating, article_reading_state.user_rating),
                updated_at = NOW()
            RETURNING
                clean_article_id,
                status,
                is_saved,
                user_rating
            """,
            (article_id, status, is_saved, user_rating),
        )

        row = cur.fetchone()
        conn.commit()
        return row
