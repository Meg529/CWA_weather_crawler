import asyncio
import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from data_source.get_predict_data import get_predict_cwb_data
from db import db_schema
from db.db_interaction import DBManager


# 建立非同步排程器
scheduler = AsyncIOScheduler()


async def collect_predict():
    """收集氣象預報資料"""

    print(">>> collect_predict 被執行了")

    # 建立資料庫連線
    async with db_schema.async_session() as session:
        db = DBManager(session)

        # 取得最新預報資料
        df = await get_predict_cwb_data()

        if df.empty:
            print(">>> 沒有取得氣象預報資料")
            return

        print(f">>> 取得預報資料，共 {len(df)} 筆")

        # 將 DataFrame 轉成資料庫物件
        records = []

        for _, row in df.iterrows():
            record = db_schema.PredictWeather(
                county=row["縣市"],
                location=row["地點"],
                forecast_time=row.name,
                temperature=float(row["溫度"]),
                dew_point=float(row["露點溫度"]),
                humidity=float(row["相對濕度"]),
                apparent_temperature=float(row["體感溫度"]),
                wind_speed=str(row["風速"]),
                wind_direction=row["風向"],
                precipitation_probability=float(row["3小時降雨機率"]),
                weather=row["天氣現象"],
                weather_description=row["天氣預報綜合描述"],
            )
            records.append(record)

        # 刪除舊資料並寫入最新預報
        await db.delete_all_predict_weather()
        await db.add_predict_weather(records)

        print(f">>> PredictWeather 已更新，共 {len(records)} 筆")

        # 建立收集紀錄
        now = datetime.datetime.now(datetime.timezone.utc)
        collected_at = int(now.timestamp())

        log = db_schema.PredictCollectionLog(
            collected_at=collected_at,
            record_count=len(records),
            status="success",
        )

        await db.add_predict_collection_log(log)

        print(f">>> Predict 資料收集完成，共 {len(records)} 筆")


def start_scheduler():
    # 每 3 小時執行一次
    scheduler.add_job(
        collect_predict,
        "interval",
        hours=3,
        id="predict_collection",
        replace_existing=True,
    )

    scheduler.start()

    print(">>> Predict Job 已加入")
    print(">>> 每 3 小時執行一次")


def stop_scheduler():
    """停止氣象預報資料排程"""

    if scheduler.running:
        scheduler.shutdown()


async def main():
    print(">>> Predict Scheduler 啟動")

    # 啟動時先執行一次
    await collect_predict()

    # 開始每 3 小時排程
    start_scheduler()

    # 維持程式運作
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())