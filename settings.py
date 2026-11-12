from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """專案環境設定"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # PostgreSQL
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str

    # CWA History
    cwa_weather_history_url: str
    cwa_weather_predict_url: str
    cwa_weather_realtime_url: str
    cwa_token: str
    timezone: str


def get_settings() -> Settings:
    return Settings()


settings = get_settings()