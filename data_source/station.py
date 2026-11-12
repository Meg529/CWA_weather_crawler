import requests


STATION_LIST_URL = "https://codis.cwa.gov.tw/api/station_list"


def get_station_list():
    """取得 CWA 全部測站資料。"""

    # 向 CODiS 取得測站清單
    response = requests.get(STATION_LIST_URL)
    response.raise_for_status()

    data = response.json()

    return data


def get_active_cwb_stations():
    """取得目前仍在使用的 CWB 測站。"""

    # 取得完整測站清單
    data = get_station_list()

    # 找出 CWB 測站群組
    cwb_group = next(
        group
        for group in data["data"]
        if group["stationAttribute"] == "cwb"
    )

    stations = cwb_group["item"]

    # 篩選尚未停用的測站
    active_stations = [
        station
        for station in stations
        if station["stationEndDate"] == ""
    ]

    return active_stations


def get_station_type(group_attribute: str, station_id: str) -> str | None:
    """依 CODiS 群組決定歷史 API 要帶的 stn_type，不支援的群組回傳 None。"""

    if group_attribute == "cwb":
        return "cwb"

    # 自動站依站號前兩碼區分，例如 C0D560 → auto_C0
    if group_attribute == "auto":
        return f"auto_{station_id[:2]}"

    return None


def find_station(station_id: str):
    """在 CODiS 全部測站中搜尋指定站號，找不到回傳 None。"""

    data = get_station_list()

    for group in data["data"]:
        for station in group["item"]:
            if station["stationID"] == station_id:
                return {
                    "stationID": station["stationID"],
                    "stationName": station["stationName"],
                    "stationEndDate": station["stationEndDate"],
                    "stationType": get_station_type(
                        group["stationAttribute"],
                        station_id,
                    ),
                }

    return None


async def get_cwa_station():
    """從 DB 讀取要收集的測站清單，格式與 CODiS 測站相同。"""

    # 延後 import，避免單獨執行本檔時需要 DB 設定
    from db import db_schema
    from db.db_interaction import DBManager

    async with db_schema.async_session() as session:
        db = DBManager(session)
        stations = await db.get_all_cwa_stations()

    return [
        {
            "stationID": station.station_id,
            "stationName": station.station_name,
            "stationType": station.station_type,
        }
        for station in stations
    ]


if __name__ == "__main__":
    # 測試目前有效測站
    stations = get_active_cwb_stations()

    print("目前有效測站數量：", len(stations))

    for station in stations:
        print(
            station["stationID"],
            station["stationName"],
            station["stationEndDate"],
        )