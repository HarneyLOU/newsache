# newsache

Personal daily tech-news intelligence pipeline.

`newsache` helps turn information overload into a focused daily reading flow.
The goal is simple: stay current with fast-moving tech topics without reading hundreds of sources end-to-end.

It is inspired by tools like Feedly, but tailored for personal workflows:
- aggregate selected RSS sources,
- extract clean full-article text,
- generate concise high-level summaries,
- open full original articles whenever deeper reading is needed.

## Why this project

The technology landscape moves fast, and high-quality sources are fragmented.
This project is built to answer:
- What changed today that actually matters?
- Can I get a short summary first?
- Can I drill down to full context only when I need to?

## Current status

Right now, the project includes a Dagster + PostgreSQL ingestion pipeline that:
- loads RSS sources from [sources.yaml](sources.yaml),
- fetches and stores feed entries,
- downloads article pages,
- extracts clean article text,
- tracks ingestion runs and article reading state in Postgres.

Key code:
- [src/news_pipeline/assets.py](src/news_pipeline/assets.py)
- [src/news_pipeline/db.py](src/news_pipeline/db.py)
- [src/news_pipeline/definitions.py](src/news_pipeline/definitions.py)

## Tech stack (current)

- Python 3.12
- Dagster (orchestration/scheduling)
- PostgreSQL
- feedparser + httpx (RSS and HTTP fetching)
- trafilatura (article text extraction)
- psycopg + pydantic

## Quick start

1. Install dependencies:
```bash
uv sync
```

2. Start PostgreSQL:
```bash
docker compose up -d
```

3. Start Dagster UI:
```bash
uv run dagster dev
```

4. Materialize assets from the Dagster UI to run ingestion.

## Configuration

- RSS sources are managed in [sources.yaml](sources.yaml)
- Default database URL is currently set to:
  `postgresql://news:news@localhost:5432/news`

## Roadmap (next steps)

Planned next features:
- FastAPI backend for serving articles, summaries, and assistant endpoints.
- Streamlit interface for daily reading workflows.
- LLM-powered assistant for article summarization.
- LLM-powered assistant for translations.
- LLM-powered assistant for explanations of complex topics.
- LLM-powered assistant for interactive Q&A over collected articles.

Longer term:
- better ranking/prioritization of "must-read" items,
- topic clustering and deduplication,
- personalization of summary depth and style.

## Project direction

This is a personal system first, optimized for:
- concise signal over noise,
- high-level understanding first,
- full-article context on demand,
- AI assistance that helps explain and translate content, not just summarize it.
