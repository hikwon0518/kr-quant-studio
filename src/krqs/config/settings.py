from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dart_api_key: str = Field(default="", alias="DART_API_KEY")
    dart_rate_limit_per_sec: float = Field(
        default=5.0, alias="DART_RATE_LIMIT_PER_SEC"
    )
    db_path: Path = Field(
        default=Path("data/warehouse.duckdb"), alias="KRQS_DB_PATH"
    )
    staging_db_path: Path = Field(
        default=Path("data/staging.duckdb"), alias="KRQS_STAGING_DB_PATH"
    )
    snapshots_dir: Path = Field(
        default=Path("data/snapshots"), alias="KRQS_SNAPSHOTS_DIR"
    )
    fsc_api_key: str = Field(default="", alias="FSC_API_KEY")
    fsc_api_base_url: str = Field(
        default="https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService",
        alias="FSC_API_BASE_URL",
    )


def get_settings() -> Settings:
    return Settings()
