import asyncio
import re
from io import StringIO
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from data_source.station import get_cwa_station
from data_source.utils import request_with_retry
from settings import get_settings


settings = get_settings()


# 根據測站編號建立 CWA 官網近 24 小時觀測資料 URL
def cwa_weather_observation_url(station_id: str) -> str:
    return f"{settings.cwa_weather_realtime_url}{station_id[:-1]}.html"


async def fetch_station_realtime(client, station):
    """取得單一測站的近 24 小時觀測資料。"""
    current_settings = get_settings()
    timezone = ZoneInfo(current_settings.timezone)

    station_id = station["stationID"]
    station_name = station["stationName"]
    url = cwa_weather_observation_url(station_id)

    # 發送 HTTP 請求，單站失敗只跳過，不影響其他測站
    try:
        response = await request_with_retry(lambda: client.get(url))
    except httpx.HTTPError as e:
        print(f">>> {station_id} {station_name} 即時資料取得失敗：{e!r}")
        return pd.DataFrame()

    data = response.text

    # 將網頁內容包成完整 HTML table
    data = f"""
    <table>
    <thead>
        <tr>
            <th>觀測時間</th>
            <th>溫度(C)</th>
            <th>天氣描述</th>
            <th>風向(方位)</th>
            <th>風力(級)</th>
            <th>陣風(級)</th>
            <th>能見度</th>
            <th>相對濕度(%)</th>
            <th>氣壓(hpa)</th>
            <th>本日累積雨量(mm)</th>
            <th>本日總日照時數(h)</th>
        </tr>
    </thead>
    <tbody>
        {data}
    </tbody>
    </table>
    """

    # 將天氣圖示的 alt 文字取代 img 標籤
    regex = re.compile(r'<img src=".*" title=".*" alt="(.*)">')
    data = regex.sub(lambda m: m.groups()[0], data)

    # 移除隱藏的華氏溫度與蒲福風級，避免與攝氏、m/s 黏在一起（例如 33.5 + 92 → 33.592）
    data = re.sub(r'<span class="[^"]*is-hidden">.*?</span>', "", data)

    # 將 HTML table 轉成 DataFrame
    df = pd.read_html(StringIO(data))[0]

    # 將 CWA 的月/日時間補上目前年份
    dt = pd.to_datetime(
        df["觀測時間"],
        format="%m/%d %H:%M",
        errors="coerce",
    )

    if dt.notna().sum() == 0:
        return pd.DataFrame()

    dt = dt.dropna()
    df = df.loc[dt.index]

    now = pd.Timestamp.now(tz=timezone)

    # 補上目前年份後，明確指定為台灣時間
    # （用帶時區的 now，避免容器時區是 UTC 時誤判）
    dt = dt.map(lambda x: x.replace(year=now.year)).dt.tz_localize(timezone)

    # 若補上今年後變成未來時間，代表資料屬於去年
    dt.loc[dt > now] -= pd.DateOffset(years=1)

    df["觀測時間"] = dt

    # 將台灣時間轉成 Unix timestamp
    df["觀測時間"] = (
        df["觀測時間"]
        .dt.tz_convert("UTC")
        .astype("int64")
        // 10**6
    )

    # 設定時間索引
    df = df.set_index("觀測時間")
    df = df.sort_index()
    df.index = df.index.rename("Time")

    # 將 CWA 欄位名稱轉成系統使用的名稱
    df = df.rename(
        columns={
            "溫度(C)": "氣溫",
            "風向(方位)": "風向",
            "風力(級)": "風速",
            "相對濕度(%)": "濕度",
            "氣壓(hpa)": "氣壓",
            "本日累積雨量(mm)": "雨量",
            "本日總日照時數(h)": "日照時數",
        }
    )

    # 將 CWA 缺值符號轉成空值
    df = df.replace("-", pd.NA)

    # 將氣象數值欄位轉成數字
    numeric_columns = [
        "氣溫",
        "風速",
        "陣風(級)",
        "能見度",
        "濕度",
        "氣壓",
        "雨量",
        "日照時數",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    # 加入測站資訊
    df["測站編號"] = station_id
    df["測站名稱"] = station_name

    return df


async def get_realtime_data():
    """收集清單內所有測站的近 24 小時資料。"""

    # 從 DB 取得要收集的測站
    stations = await get_cwa_station()

    async with httpx.AsyncClient(timeout=60.0) as client:

        # 建立所有測站的收集任務
        tasks = [
            fetch_station_realtime(client, station)
            for station in stations
        ]

        # 等待所有測站完成
        results = await asyncio.gather(*tasks)

    # 合併所有測站資料（過濾掉空 DataFrame）
    frames = [result for result in results if not result.empty]

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames)


if __name__ == "__main__":
    df = asyncio.run(get_realtime_data())

    print("資料筆數：", len(df))
    print("最早 timestamp：", df.index.min())
    print("最晚 timestamp：", df.index.max())

    print(
        "最早時間：",
        pd.to_datetime(df.index.min(), unit="s", utc=True)
        .tz_convert("Asia/Taipei"),
    )

    print(
        "最晚時間：",
        pd.to_datetime(df.index.max(), unit="s", utc=True)
        .tz_convert("Asia/Taipei"),
    )
