from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field


class PostgresResource(ConfigurableResource):
    database_url: str = Field(
        default="postgresql://news:news@localhost:5432/news",
        description="Postgres connection URL",
    )


class SourcesConfigResource(ConfigurableResource):
    path: str = Field(
        default="sources.yaml",
        description="Path to YAML file with RSS sources",
    )