from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets
from .resources import PostgresResource, SourcesConfigResource


all_assets = load_assets_from_modules([assets])

news_ingestion_job = define_asset_job(
    name="news_ingestion_job",
    selection=AssetSelection.all(),
)

hourly_news_ingestion_schedule = ScheduleDefinition(
    name="hourly_news_ingestion_schedule",
    job=news_ingestion_job,
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=all_assets,
    jobs=[news_ingestion_job],
    schedules=[hourly_news_ingestion_schedule],
    resources={
        "postgres": PostgresResource(
            database_url="postgresql://news:news@localhost:5432/news",
        ),
        "sources_config": SourcesConfigResource(
            path="sources.yaml",
        ),
    },
)
