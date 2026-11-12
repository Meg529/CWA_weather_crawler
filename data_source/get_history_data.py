import asyncio
import datetime

import httpx
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from data_source.station import get_cwa_station
from data_source.utils import request_with_retry
import settings


# 動態取得設定
get_settings = settings.get_settings


# 系統欄位對應的 CWA 觀測項目，用來判斷測站是否有提供該項目
HISTORY_ELEMENTS = {
    "氣壓": "StationPressure",
    "氣溫": "AirTemperature",
    "露點": "DewPointTemperature",
    "濕度": "RelativeHumidity",
    "日射量": "GlobalSolarRadiation",
    "雨量": "Precipitation",
    "風向": "WindDirection",
    "風速": "WindSpeed",
    "雲量": "TotalCloudAmount",
    "可見度": "Visibility",
}


def get_value(dt, element, field):
    """取得 CWA 觀測欄位，自動站缺少的欄位（例如露點、雲量、能見度）回傳 NaN。"""

    return (dt.get(element) or {}).get(field, np.nan)


async def fetch_station_data(
    client,
    start_time,
    end_time,
    station,
    current_settings,
    semaphore,
):
    """取得單一測站的 History 氣象資料。"""

    async with semaphore:
        station_id = station["stationID"]

        # 將 Unix timestamp 轉成台灣時間
        timezone = ZoneInfo(current_settings.timezone)

        start_datetime = datetime.datetime.fromtimestamp(start_time, timezone)
        end_datetime = datetime.datetime.fromtimestamp(end_time, timezone)

        # 建立 API 請求
        url = current_settings.cwa_weather_history_url
        payload = {
            "date": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "type": "report_date",
            "stn_ID": station_id,
            "stn_type": station["stationType"],
            "start": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "end": end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        # 發送請求並取得資料，失敗時回傳空 dict，下游會略過此測站區段
        try:
            response = await request_with_retry(
                lambda: client.post(url, data=payload)
            )
        except httpx.HTTPError as e:
            print(
                f">>> {station_id} {station['stationName']} "
                f"History 資料取得失敗：{e!r}"
            )
            return station, {}

        data = response.json()

        return station, data


async def CwbHistoryCollector(
    start_date: int,
    end_date: int,
    stations: list[dict] | None = None,
) -> pd.DataFrame:
    """收集測站 History 氣象資料，未指定 stations 時收集 DB 清單內全部測站。"""

    # 取得測站與設定
    if stations is None:
        stations = await get_cwa_station()
    current_settings = get_settings()
    timezone = ZoneInfo(current_settings.timezone)

    # 限制同時請求的測站數量
    semaphore = asyncio.Semaphore(5)

    # 將大範圍切成一天一天查詢
    time_ranges = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(
            current_start + 24 * 60 * 60,
            end_date,
        )
        time_ranges.append((current_start, current_end))
        current_start = current_end

    print(f">>> 共需要查詢 {len(time_ranges)} 個時間區段")

    async with httpx.AsyncClient(timeout=60.0) as client:

        # 每個測站 × 每個時間區段建立任務
        tasks = [
            fetch_station_data(
                client,
                range_start,
                range_end,
                station,
                current_settings,
                semaphore,
            )
            for station in stations
            for range_start, range_end in time_ranges
        ]

        # 等待所有 API 請求完成
        results = await asyncio.gather(*tasks)

    # 合併所有測站資料
    df_all = pd.DataFrame()

    for station, data in results:
        station_id = station["stationID"]
        station_name = station["stationName"]

        # API 沒有 data，代表 API 回傳錯誤
        if "data" not in data:
            print(
                f">>> {station_id} {station_name} API 回傳錯誤："
                f"{data}"
            )
            continue

        # 沒有資料就跳過
        if data["data"] == []:
            print(f"data is empty: {station_id} {station_name} done")
            continue

        # 將 CWA 資料轉成 DataFrame
        df = pd.DataFrame.from_dict([
            {
                # CWA 時間轉成台灣時間，再轉 Unix timestamp
                "DateTime": int(
                    datetime.datetime.strptime(
                        dt["DataTime"],
                        "%Y-%m-%dT%H:%M:%S",
                    ).replace(tzinfo=timezone).timestamp()
                ),
                "測站氣壓 (hPa)": get_value(dt, "StationPressure", "Instantaneous"),
                "氣溫 (℃)": get_value(dt, "AirTemperature", "Instantaneous"),
                "露點溫度 (℃)": get_value(dt, "DewPointTemperature", "Instantaneous"),
                "相對溼度 (%)": get_value(dt, "RelativeHumidity", "Instantaneous"),
                "全天空日射量 (MJ/㎡)": get_value(dt, "GlobalSolarRadiation", "Accumulation"),
                "降水量 (mm)": get_value(dt, "Precipitation", "Accumulation"),
                "風向 (360degree)": get_value(dt, "WindDirection", "Mean"),
                "風速 (m/s)": get_value(dt, "WindSpeed", "Mean"),
                "總雲量 (0~10)": get_value(dt, "TotalCloudAmount", "Instantaneous"),
                "能見度 (km)": get_value(dt, "Visibility", "Instantaneous"),
            }
            for dt in data["data"][0]["dts"]
        ])

        # 設定時間索引
        df = df.set_index("DateTime")
        df.index = df.index.rename("Time")

        # 將 CWA 欄位名稱轉成系統名稱
        df = df.rename(columns={
            "測站氣壓 (hPa)": "氣壓",
            "氣溫 (℃)": "氣溫",
            "露點溫度 (℃)": "露點",
            "相對溼度 (%)": "濕度",
            "全天空日射量 (MJ/㎡)": "日射量",
            "降水量 (mm)": "雨量",
            "風向 (360degree)": "風向",
            "風速 (m/s)": "風速",
            "總雲量 (0~10)": "雲量",
            "能見度 (km)": "可見度",
        })

        # 修正資料與單位
        df.loc[df["雨量"] < 0, "雨量"] = 0
        df["雲幕高"] = -9900
        df["日射量"] = df["日射量"] * 1000000 / 3600

        # 處理 CWA 缺失值
        df = df.replace(["...", "/", "T"], np.nan)
        df = df.fillna(0)

        # 測站本身沒有提供的觀測項目保留 NaN，寫入 DB 為 NULL
        dts = data["data"][0]["dts"]
        missing_columns = [
            column
            for column, element in HISTORY_ELEMENTS.items()
            if not any(element in dt for dt in dts)
        ]
        df[missing_columns] = np.nan

        # 將 timestamp 轉成設定的時區，取得年月日時
        taiwan_time = pd.to_datetime(
            df.index,
            unit="s",
            utc=True,
        ).tz_convert(timezone)

        df["年"] = taiwan_time.year
        df["月"] = taiwan_time.month
        df["日"] = taiwan_time.day
        df["時"] = taiwan_time.hour

        # 加入測站資訊
        df["測站編號"] = station_id
        df["測站名稱"] = station_name

        # 保留系統需要的欄位
        df = df.loc[:, [
            "測站編號",
            "測站名稱",
            "年",
            "月",
            "日",
            "時",
            "氣壓",
            "氣溫",
            "露點",
            "濕度",
            "日射量",
            "雨量",
            "風向",
            "風速",
            "雲量",
            "雲幕高",
            "可見度",
        ]]

        # 加入全部測站資料
        df_all = pd.concat([df_all, df])
        print(f"{station_id} {station_name} done")

    if df_all.empty:
        return df_all

    # 依測站與時間排序
    df_all = df_all.sort_values(["測站編號", "Time"])

    return df_all


if __name__ == "__main__":

    # 設定時區與測試時間
    current_settings = get_settings()
    timezone = ZoneInfo(current_settings.timezone)

    start_time = int(datetime.datetime(2024, 1, 1, tzinfo=timezone).timestamp())
    end_time = int(datetime.datetime(2024, 1, 9, tzinfo=timezone).timestamp())

    # 測試收集 History 資料
    df = asyncio.run(CwbHistoryCollector(start_time, end_time))

    print("\n===== 資料筆數 =====")
    print(len(df))

    print("\n===== 欄位 =====")
    print(df.columns.tolist())

    print("\n===== 前 10 筆 =====")
    print(df.head(10))

    print("\n===== 資料索引範圍 =====")
    print(df.index.min())
    print(df.index.max())