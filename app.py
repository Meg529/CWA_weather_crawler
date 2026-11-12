from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import BackgroundTasks, FastAPI, HTTPException

from data_source.get_predict_data import get_predict_cwb_data
from data_source.station import find_station, get_active_cwb_stations, get_cwa_station
from db import db_schema
from db.db_interaction import DBManager
from settings import get_settings
from scheduler.history_sch import (
    backfill_history,
    collect_history,
    start_scheduler as start_history_scheduler,
    stop_scheduler as stop_history_scheduler,
)
from scheduler.predict_sch import (
    collect_predict,
    start_scheduler as start_predict_scheduler,
    stop_scheduler as stop_predict_scheduler,
)
from scheduler.realtime_sch import (
    collect_realtime,
    start_scheduler as start_realtime_scheduler,
    stop_scheduler as stop_realtime_scheduler,
)


async def ensure_default_cwa_stations():
    """將目前有效的局屬 cwb 測站寫入收集清單，已存在的略過。"""

    now_timestamp = int(datetime.now(timezone.utc).timestamp())

    records = [
        dict(
            station_id=station["stationID"],
            station_name=station["stationName"],
            station_type="cwb",
            created_at=now_timestamp,
        )
        for station in get_active_cwb_stations()
    ]

    async with db_schema.async_session() as session:
        db = DBManager(session)
        inserted = await db.ensure_cwa_stations(records)

    print(f">>> 局屬測站確認完成，共 {len(records)} 站，新增 {inserted} 站")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """管理 FastAPI 啟動與關閉時的流程。"""

    print(">>> CWA Weather Service 啟動")

    # 確認資料表
    await db_schema.create_tables_if_not_exists()
    print(">>> 資料表確認完成")

    # 確保局屬 cwb 測站都在收集清單內
    await ensure_default_cwa_stations()

    # 啟動時先收集一次資料
    print(">>> 開始執行啟動時資料收集")
    await collect_history()
    await collect_realtime()
    await collect_predict()

    # 啟動定期排程
    start_history_scheduler()
    start_realtime_scheduler()
    start_predict_scheduler()
    print(">>> 所有 Scheduler 已啟動")

    yield

    # 關閉所有 Scheduler
    print(">>> CWA Weather Service 關閉")
    stop_history_scheduler()
    stop_realtime_scheduler()
    stop_predict_scheduler()
    print(">>> 所有 Scheduler 已停止")


# 建立 FastAPI 應用程式
app = FastAPI(
    title="CWA Weather Service",
    description="中央氣象署氣象資料收集服務",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """提供服務基本狀態。"""

    return {
        "service": "CWA Weather Service",
        "status": "running",
    }


@app.get("/health")
async def health():
    """提供服務健康檢查。"""

    return {"status": "ok"}


# CWA station
@app.get("/cwa-stations", tags=["CWA station"])
async def list_cwa_stations():
    """列出要收集的測站清單。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)
        return await db.get_all_cwa_stations()


@app.post("/cwa-stations", tags=["CWA station"], status_code=201)
async def add_cwa_station(
    station_id: str,
    background_tasks: BackgroundTasks,
    backfill_from: date | None = None,
):
    """新增要收集的測站，可指定 backfill_from（YYYY-MM-DD）在背景回補歷史資料。"""

    station_id = station_id.strip().upper()

    # 回補起始日轉成設定時區當天 00:00 的 timestamp
    backfill_start = None
    if backfill_from:
        backfill_start = int(
            datetime.combine(
                backfill_from,
                datetime.min.time(),
                tzinfo=ZoneInfo(get_settings().timezone),
            ).timestamp()
        )

    async with db_schema.async_session() as session:
        db = DBManager(session)

        if await db.get_cwa_station_by_id(station_id):
            raise HTTPException(409, f"{station_id} 已在收集清單內")

        # 到 CODiS 確認測站存在且仍在使用
        station = find_station(station_id)

        if station is None:
            raise HTTPException(404, f"CODiS 找不到測站 {station_id}")

        if station["stationEndDate"]:
            raise HTTPException(400, f"{station_id} 已於 {station['stationEndDate']} 停用")

        if station["stationType"] is None:
            raise HTTPException(400, f"{station_id} 的測站類型目前不支援")

        record = await db.add_cwa_station(
            db_schema.CwaStation(
                station_id=station_id,
                station_name=station["stationName"],
                station_type=station["stationType"],
                created_at=int(datetime.now(timezone.utc).timestamp()),
            )
        )

    if backfill_start is not None:
        background_tasks.add_task(backfill_history, station, backfill_start)

    return {
        "station": record,
        "backfill": backfill_from is not None,
    }


@app.delete("/cwa-stations/{station_id}", tags=["CWA station"])
async def delete_cwa_station(station_id: str):
    """從收集清單移除測站，已收集的資料會保留；局屬 cwb 測站不可刪除。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)
        station = await db.get_cwa_station_by_id(station_id)

        if station is None:
            raise HTTPException(404, f"{station_id} 不在收集清單內")

        if station.station_type == "cwb":
            raise HTTPException(400, f"{station_id} 為局屬測站，不可刪除")

        await db.delete_cwa_station(station_id)

    return {"status": "success", "message": f"{station_id} 已移除"}


# History
@app.get("/climate-data/history/stations", tags=["History"])
async def get_history_stations():
    """取得目前要收集的 History 測站。"""

    stations = await get_cwa_station()

    return [
        {
            "station_id": station["stationID"],
            "station_name": station["stationName"],
        }
        for station in stations
    ]


@app.get("/climate-data/history/data", tags=["History"])
async def get_history_data(
    station_id: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
):
    """查詢 HistoryWeather 資料。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)

        records = await db.get_history_weather(
            station_id,
            start_time,
            end_time,
        )

        return records


# Realtime
@app.get("/climate-data/realtime/stations", tags=["Realtime"])
async def get_realtime_stations():
    """取得目前要收集的 Realtime 測站。"""

    stations = await get_cwa_station()

    return [
        {
            "station_id": station["stationID"],
            "station_name": station["stationName"],
        }
        for station in stations
    ]


@app.get("/climate-data/realtime/data", tags=["Realtime"])
async def get_realtime_data(
    station_id: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
):
    """查詢 RealtimeWeather 資料。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)

        records = await db.get_realtime_weather(
            station_id,
            start_time,
            end_time,
        )

        return records


# Predict
@app.get("/climate-data/predict/stations", tags=["Predict"])
async def get_predict_stations():
    """取得目前有效的預報地點。"""

    df = await get_predict_cwb_data()

    return [
        {
            "county": county,
            "location": location,
        }
        for county, location in df[["縣市", "地點"]]
        .drop_duplicates()
        .itertuples(index=False)
    ]


@app.get("/climate-data/predict/data", tags=["Predict"] )
async def get_predict_data(
    county: str | None = None,
    location: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
):
    """查詢 PredictWeather 資料。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)

        records = await db.get_predict_weather(
            county,
            location,
            start_time,
            end_time,
        )

        return records


# Collection log
@app.get("/collection-log/history", tags=["Collection log"])
async def get_history_collection_logs():
    """查詢 History 資料收集紀錄。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)
        return await db.get_history_collection_logs()


@app.get("/collection-log/realtime", tags=["Collection log"])
async def get_realtime_collection_logs():
    """查詢 Realtime 資料收集紀錄。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)
        return await db.get_realtime_collection_logs()


@app.get("/collection-log/predict", tags=["Collection log"])
async def get_predict_collection_logs():
    """查詢 Predict 資料收集紀錄。"""

    async with db_schema.async_session() as session:
        db = DBManager(session)
        return await db.get_predict_collection_logs()


# Crawler
@app.post("/crawler/history", tags=["Crawler"])
async def trigger_history():
    """手動執行一次 History 資料收集。"""

    await collect_history()

    return {
        "status": "success",
        "message": "History collection completed",
    }


@app.post("/crawler/realtime", tags=["Crawler"])
async def trigger_realtime():
    """手動執行一次 Realtime 資料收集。"""

    await collect_realtime()

    return {
        "status": "success",
        "message": "Realtime collection completed",
    }


@app.post("/crawler/predict", tags=["Crawler"])
async def trigger_predict():
    """手動執行一次 Predict 資料收集。"""

    await collect_predict()

    return {
        "status": "success",
        "message": "Predict collection completed",
    }


if __name__ == "__main__":
    import uvicorn

    # 啟動 FastAPI 應用程式
    uvicorn.run(app, host="0.0.0.0", port=8000) 
