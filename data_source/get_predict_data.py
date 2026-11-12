import asyncio

import httpx
import pandas as pd
from zoneinfo import ZoneInfo

from data_source.utils import request_with_retry
from settings import get_settings

settings = get_settings()


# 建立 CWA 預報資料集 ID
PREDICT_DATASETS = [
    f"F-D0047-{i:03d}"
    for i in range(1, 88, 4)
]


async def fetch_predict_data(client, dataset, token, semaphore):
    """取得單一 CWA 預報資料集。"""

    async with semaphore:

        # 建立 API URL
        url = f"{settings.cwa_weather_predict_url}{dataset}"

        # 發送 API 請求，失敗時跳過此資料集，不影響其他資料集
        try:
            response = await request_with_retry(
                lambda: client.get(
                    url=url,
                    params={"Authorization": token},
                )
            )
        except httpx.HTTPError as e:
            print(f">>> {dataset} 預報資料取得失敗：{e!r}")
            return None

        return response.json()


async def get_predict_cwb_data():
    """收集 CWA 所有縣市與地點的氣象預報資料。"""

    all_cols = [
        "Temperature",
        "DewPoint",
        "RelativeHumidity",
        "ApparentTemperature",
        "ComfortIndex",
        "WindSpeed",
        "WindDirection",
        "ProbabilityOfPrecipitation",
        "Weather",
        "WeatherDescription",
    ]

    # 將 CWA 英文欄位名稱轉成系統使用的中文名稱
    mapping_dict = {
        "Temperature": "溫度",
        "DewPoint": "露點溫度",
        "RelativeHumidity": "相對濕度",
        "ApparentTemperature": "體感溫度",
        "ComfortIndex": "舒適度指數",
        "WindSpeed": "風速",
        "WindDirection": "風向",
        "ProbabilityOfPrecipitation": "3小時降雨機率",
        "Weather": "天氣現象",
        "WeatherDescription": "天氣預報綜合描述",
    }

    # 取得設定並限制同時請求數量
    current_settings = get_settings()
    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient(timeout=60.0) as client:

        # 建立所有預報資料集的收集任務
        tasks = [
            fetch_predict_data(
                client,
                dataset,
                current_settings.cwa_token,
                semaphore,
            )
            for dataset in PREDICT_DATASETS
        ]

        # 等待所有資料集完成
        all_data = await asyncio.gather(*tasks)

    all_df = []

    for data in all_data:

        # 取得失敗的資料集直接跳過
        if data is None:
            continue

        # 取得縣市名稱
        county = data["records"]["Locations"][0]["LocationsName"]

        for location_data in data["records"]["Locations"][0]["Location"]:

            # 建立每小時的時間索引
            start_time = pd.to_datetime(
                location_data["WeatherElement"][0]["Time"][0]["DataTime"]
            )
            end_time = pd.to_datetime(
                location_data["WeatherElement"][0]["Time"][-1]["DataTime"]
            )
            all_idx = pd.date_range(start_time, end_time, freq="1h")

            df = pd.DataFrame(index=all_idx, columns=all_cols)

            # 將各氣象元素填入對應時間與欄位
            for weather_element in location_data["WeatherElement"]:

                element_name = weather_element["ElementName"]

                col = next(
                    col
                    for col, name in mapping_dict.items()
                    if name == element_name
                )

                for weather_data in weather_element["Time"]:

                    time = weather_data.get(
                        "DataTime",
                        weather_data.get("StartTime"),
                    )

                    df.loc[time, col] = weather_data["ElementValue"][0][col]

            # 將欄位名稱轉成系統使用的中文名稱
            df = df.rename(columns=mapping_dict)

            # 將台灣時間轉成 Unix timestamp
            timezone = ZoneInfo(current_settings.timezone)

            df.index = pd.to_datetime(df.index)
            df.index = (
                df.index
                .tz_convert(timezone)
                .astype("int64")
                // 10**6
            )
            df.index = df.index.rename("Time")

            # 移除資料不完整的時間點
            df = df.dropna()

            # 加入縣市與地點資訊
            df["縣市"] = county
            df["地點"] = location_data["LocationName"]

            # 將縣市與地點放在最前面
            df = df[
                ["縣市", "地點"]
                + [
                    col
                    for col in df.columns
                    if col not in ["縣市", "地點"]
                ]
            ]

            all_df.append(df)

    if not all_df:
        return pd.DataFrame()

    # 合併所有縣市與地點的預報資料
    return pd.concat(all_df)


if __name__ == "__main__":

    # 測試取得 CWA 預報資料
    df = asyncio.run(get_predict_cwb_data())

    print(">>> DataFrame columns:")
    print(df.columns.tolist())

    print("\n>>> DataFrame shape:")
    print(df.shape)

    print("\n>>> 前 5 筆資料:")
    print(df.head())