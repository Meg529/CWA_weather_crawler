from typing import Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import UniqueConstraint

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import settings


# 使用SQLite測試
# engine = create_async_engine("sqlite+aiosqlite:///./cwb_weather.db", echo=False)
# async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# 使用PostgreSQL
DATABASE_URL = (
    f"postgresql+asyncpg://"
    f"{settings.settings.postgres_user}:"
    f"{settings.settings.postgres_password}@"
    f"{settings.settings.postgres_host}:"
    f"{settings.settings.postgres_port}/"
    f"{settings.settings.postgres_db}"
)

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine,expire_on_commit=False,class_=AsyncSession,)


class CwaStation(SQLModel, table=True):
    """要收集的測站清單，排程只會收集這張表內的測站"""

    __tablename__ = "cwa_station"

    station_id: str = Field(primary_key=True)
    station_name: str = Field(nullable=False)
    station_type: str = Field(nullable=False)  # CODiS 歷史 API 的 stn_type，例如 cwb / auto_C0
    created_at: int = Field(nullable=False)


class HistoryWeather(SQLModel, table=True):
    """歷史氣象資料表"""

    __tablename__ = "history_weather"
    __table_args__ = (
        UniqueConstraint("station_id", "time", name="uq_history_station_time"),
    )

    time: int = Field(primary_key=True)
    station_id: str = Field(primary_key=True)
    station_name: str = Field(nullable=False)
    pressure: Optional[float] = Field(default=None)
    temperature: Optional[float] = Field(default=None)
    dew_point: Optional[float] = Field(default=None)
    humidity: Optional[float] = Field(default=None)
    solar_radiation: Optional[float] = Field(default=None)
    precipitation: Optional[float] = Field(default=None)
    wind_direction: Optional[float] = Field(default=None)
    wind_speed: Optional[float] = Field(default=None)
    cloud_amount: Optional[float] = Field(default=None)
    cloud_height: Optional[float] = Field(default=None)
    visibility: Optional[float] = Field(default=None)


class RealtimeWeather(SQLModel, table=True):
    """即時氣象資料表"""

    __tablename__ = "realtime_weather"
    __table_args__ = (
        UniqueConstraint("station_id", "time", name="uq_realtime_station_time"),
    )

    time: int = Field(primary_key=True)
    station_id: str = Field(primary_key=True)
    station_name: str = Field(nullable=False)
    pressure: Optional[float] = Field(default=None)
    temperature: Optional[float] = Field(default=None)
    humidity: Optional[float] = Field(default=None)
    precipitation: Optional[float] = Field(default=None)
    wind_direction: Optional[str] = Field(default=None)
    wind_speed: Optional[float] = Field(default=None)
    visibility: Optional[float] = Field(default=None)


class PredictWeather(SQLModel, table=True):
    """天氣預報資料表"""

    __tablename__ = "predict_weather"
    __table_args__ = (
        UniqueConstraint(
            "county",
            "location",
            "forecast_time",
            name="uq_predict_county_location_forecast_time",
        ),
    )

    forecast_time: int = Field(primary_key=True)
    county: str = Field(primary_key=True)
    location: str = Field(primary_key=True)
    temperature: Optional[float] = Field(default=None)
    dew_point: Optional[float] = Field(default=None)
    humidity: Optional[float] = Field(default=None)
    apparent_temperature: Optional[float] = Field(default=None)
    wind_speed: Optional[str] = Field(default=None)
    wind_direction: Optional[str] = Field(default=None)
    precipitation_probability: Optional[float] = Field(default=None)
    weather: Optional[str] = Field(default=None)
    weather_description: Optional[str] = Field(default=None)


class HistoryCollectionLog(SQLModel, table=True):
    """歷史氣象資料取得紀錄表"""

    __tablename__ = "history_collection_log"

    id: Optional[int] = Field(default=None, primary_key=True)
    collected_at: int = Field(nullable=False, index=True)
    start_time: int = Field(nullable=False)
    end_time: int = Field(nullable=False)
    record_count: int = Field(nullable=False)
    status: str = Field(nullable=False)
    message: Optional[str] = Field(default=None, max_length=500)


class RealtimeCollectionLog(SQLModel, table=True):
    """即時氣象資料取得紀錄表"""

    __tablename__ = "realtime_collection_log"

    id: Optional[int] = Field(default=None, primary_key=True)
    collected_at: int = Field(nullable=False, index=True)
    start_time: int = Field(nullable=False)
    end_time: int = Field(nullable=False)
    record_count: int = Field(nullable=False)
    status: str = Field(nullable=False)
    message: Optional[str] = Field(default=None, max_length=1000)


class PredictCollectionLog(SQLModel, table=True):
    """天氣預報資料取得紀錄表"""

    __tablename__ = "predict_collection_log"

    id: Optional[int] = Field(default=None, primary_key=True)
    collected_at: int = Field(nullable=False, index=True)
    record_count: int = Field(nullable=False)
    status: str = Field(nullable=False)
    message: Optional[str] = Field(default=None, max_length=500)


async def create_tables_if_not_exists():
    """建立資料表"""

    async with engine.begin() as conn:
        # await conn.run_sync(SQLModel.metadata.drop_all)  #刪除table
        await conn.run_sync(SQLModel.metadata.create_all)  #建立table


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_tables_if_not_exists())

    print("SQLite 資料表建立完成")
