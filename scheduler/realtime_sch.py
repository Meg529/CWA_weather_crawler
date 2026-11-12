import asyncio
import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from data_source.get_realtime_data import get_realtime_data
from data_source.utils import to_float, to_str
from db import db_schema
from db.db_interaction import DBManager


# 建立非同步排程器
scheduler = AsyncIOScheduler()


async def collect_realtime():
    print(">>> collect_realtime 被執行了")

    # 取得目前時間的 Unix timestamp
    now = datetime.datetime.now(datetime.timezone.utc)
    now_timestamp = int(now.timestamp())

    # 取得 CWA 近 24 小時資料
    df = await get_realtime_data()

    if df.empty:
        print(">>> 沒有取得即時氣象資料")
        return

    print(">>> 即時氣象原始資料筆數：", len(df))

    # 將 DataFrame 轉成資料庫欄位；每次都寫入網頁上全部 24 小時資料，
    # 已存在的 (time, station_id) 會被略過，新加入的測站也能一次取得 24 小時
    records = []

    for time, row in df.iterrows():
        record = dict(
            station_id=row["測站編號"],
            station_name=row["測站名稱"],
            time=int(time),
            pressure=to_float(row.get("氣壓")),
            temperature=to_float(row.get("氣溫")),
            humidity=to_float(row.get("濕度")),
            precipitation=to_float(row.get("雨量")),
            wind_direction=to_str(row.get("風向")),
            wind_speed=to_float(row.get("風速")),
            visibility=to_float(row.get("能見度")),
        )
        records.append(record)

    async with db_schema.async_session() as session:
        db = DBManager(session)

        # 寫入資料庫，已存在的資料會被略過
        inserted = await db.add_realtime_weather(records)
        print(f">>> RealtimeWeather 已寫入 DB，新增 {inserted} 筆")

        # 建立收集紀錄，時間範圍為本次網頁上的資料範圍
        log = db_schema.RealtimeCollectionLog(
            collected_at=now_timestamp,
            start_time=int(df.index.min()),
            end_time=int(df.index.max()),
            record_count=inserted,
            status="success",
        )

        await db.add_realtime_collection_log(log)

    print(f">>> Realtime 資料收集完成，取得 {len(records)} 筆，新增 {inserted} 筆")


def start_scheduler():
    # 每 1 小時執行一次
    scheduler.add_job(
        collect_realtime,
        "interval",
        hours=1,
        id="realtime_collection",
        replace_existing=True,
    )

    scheduler.start()

    print(">>> Realtime Job 已加入")
    print(">>> 每 1 小時執行一次")


def stop_scheduler():
    """停止即時氣象資料排程"""

    if scheduler.running:
        scheduler.shutdown()


async def main():
    print(">>> Realtime Scheduler 啟動")

    # 啟動時先執行一次
    await collect_realtime()

    # 開始每小時排程
    start_scheduler()

    # 維持程式運作
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())