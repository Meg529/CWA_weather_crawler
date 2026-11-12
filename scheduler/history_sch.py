import asyncio
import datetime
import traceback
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from data_source.get_history_data import CwbHistoryCollector
from data_source.utils import to_float
from db import db_schema
from db.db_interaction import DBManager
from settings import get_settings

settings = get_settings()
timezone = ZoneInfo(settings.timezone)


# 建立非同步排程器
scheduler = AsyncIOScheduler(timezone=timezone)


def to_history_records(df):
    """將 DataFrame 轉成資料庫欄位，缺少的數值存成 None"""

    records = []

    for _, row in df.iterrows():
        record = dict(
            station_id=row["測站編號"],
            station_name=row["測站名稱"],
            time=int(row.name),
            pressure=to_float(row["氣壓"]),
            temperature=to_float(row["氣溫"]),
            dew_point=to_float(row["露點"]),
            humidity=to_float(row["濕度"]),
            solar_radiation=to_float(row["日射量"]),
            precipitation=to_float(row["雨量"]),
            wind_direction=to_float(row["風向"]),
            wind_speed=to_float(row["風速"]),
            cloud_amount=to_float(row["雲量"]),
            cloud_height=to_float(row["雲幕高"]),
            visibility=to_float(row["可見度"]),
        )
        records.append(record)

    return records


async def backfill_history(station: dict, start_time: int):
    """替單一測站回補歷史資料，回補到最近一次成功收集的結束時間"""

    print(f">>> 開始回補 {station['stationID']} {station['stationName']} 歷史資料")

    now_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    try:
        async with db_schema.async_session() as session:
            db = DBManager(session)

            # 之後的區段交給一般排程收集
            last_log = await db.get_latest_history_collection()
            end_time = last_log.end_time if last_log else now_timestamp

            if start_time >= end_time:
                print(">>> 回補起始時間晚於最近一次收集，不需要回補")
                return

            df = await CwbHistoryCollector(
                start_date=start_time,
                end_date=end_time,
                stations=[station],
            )

            records = to_history_records(df) if not df.empty else []
            inserted = await db.add_history_weather(records) if records else 0

            # status 用 backfill，避免影響 get_latest_history_collection 的補抓判斷
            log = db_schema.HistoryCollectionLog(
                collected_at=now_timestamp,
                start_time=start_time,
                end_time=end_time,
                record_count=inserted,
                status="backfill",
                message=station["stationID"],
            )
            await db.add_history_collection_log(log)

        print(f">>> {station['stationID']} 回補完成，新增 {inserted} 筆")

    except Exception as e:
        traceback.print_exc()

        async with db_schema.async_session() as session:
            db = DBManager(session)
            log = db_schema.HistoryCollectionLog(
                collected_at=now_timestamp,
                start_time=start_time,
                end_time=0,
                record_count=0,
                status="backfill_failed",
                message=f"{station['stationID']} {type(e).__name__}: {e}"[:500],
            )
            await db.add_history_collection_log(log)


async def collect_history():
    """收集歷史氣象資料，失敗時寫入 failed 紀錄"""

    try:
        await _collect_history()
    except Exception as e:
        traceback.print_exc()

        # 寫入失敗紀錄，方便從 /collection-log/history 查看錯誤
        now_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        async with db_schema.async_session() as session:
            db = DBManager(session)
            log = db_schema.HistoryCollectionLog(
                collected_at=now_timestamp,
                start_time=0,
                end_time=0,
                record_count=0,
                status="failed",
                message=f"{type(e).__name__}: {e}"[:500],
            )
            await db.add_history_collection_log(log)

        raise


async def _collect_history():
    """收集歷史氣象資料"""

    print(">>> collect_history 被執行了")

    # 建立資料庫連線
    async with db_schema.async_session() as session:
        db = DBManager(session)
        last_log = await db.get_latest_history_collection()

        # 取得目前 UTC 時間與 timestamp
        now = datetime.datetime.now(datetime.timezone.utc)
        now_timestamp = int(now.timestamp())

        # 建立台灣時區並找出最近一次 12:00
        current_settings = get_settings()
        timezone = ZoneInfo(current_settings.timezone)

        time_now = now.astimezone(timezone)
        today_noon = time_now.replace(hour=12, minute=0, second=0, microsecond=0)
        latest_noon = int(today_noon.timestamp())

        if now_timestamp < latest_noon:
            latest_noon -= 24 * 60 * 60

        # 第一次執行或檢查是否需要補抓
        if last_log is None:
            start_time = int(
                datetime.datetime(
                    2026, 9, 1, tzinfo=timezone
                ).timestamp()
            )
        elif last_log.end_time < latest_noon:
            print(">>> 發現錯過 12:00，開始補抓資料")
            start_time = last_log.end_time
        else:
            print(">>> 尚未需要進行新的 History 收集")
            return

        query_end_time = now_timestamp

        df = await CwbHistoryCollector(
            start_date=start_time,
            end_date=query_end_time,
        )

        if df.empty:
            print(">>> 沒有取得歷史氣象資料")
            return

        # 以實際抓到的最後一筆資料作為 end_time
        end_time = int(df.index.max())

        records = to_history_records(df)

        # 寫入資料庫，與上次重疊的資料會被略過
        inserted = await db.add_history_weather(records)

        log = db_schema.HistoryCollectionLog(
            collected_at=now_timestamp,
            start_time=start_time,
            end_time=end_time,
            record_count=inserted,
            status="success",
        )

        await db.add_history_collection_log(log)

        print(
            f">>> 歷史氣象資料收集完成，取得 {len(records)} 筆，"
            f"新增 {inserted} 筆"
        )


def start_scheduler():
    """啟動歷史氣象資料排程"""

    # 每天 12:00 執行
    scheduler.add_job(
        collect_history,
        "cron",
        hour=12,
        minute=0,
        id="history_collection",
        replace_existing=True,
        # 12:00 當下若程式忙碌，1 小時內仍會補執行
        misfire_grace_time=3600,
        coalesce=True,
    )

    scheduler.start()

    job = scheduler.get_job("history_collection")
    print(f">>> History 下一次執行時間：{job.next_run_time}")

    print(">>> History Job 已加入")
    print(">>> 每天 12:00 執行一次")


def stop_scheduler():
    """停止歷史氣象資料排程"""

    if scheduler.running:
        scheduler.shutdown()


async def main():
    print(">>> History Scheduler 啟動")

    # 啟動時先執行一次
    await collect_history()

    # 啟動 Scheduler
    start_scheduler()

    # 維持程式運作
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())