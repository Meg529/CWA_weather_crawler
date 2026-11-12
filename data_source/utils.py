import asyncio

import httpx
import pandas as pd


def to_float(value):
    """將 NaN 轉成 None，其餘轉成 float。"""
    if value is None or pd.isna(value):
        return None
    return float(value)


def to_str(value):
    """將 NaN 轉成 None，其餘轉成 str。"""
    if value is None or pd.isna(value):
        return None
    return str(value)


async def request_with_retry(send, retries=3, delay=5):
    """發送 HTTP 請求，連線錯誤或 5xx 時重試；4xx（例如 404）直接丟出不重試。"""

    for attempt in range(1, retries + 1):
        try:
            response = await send()
            response.raise_for_status()
            return response
        except httpx.HTTPError as e:
            is_client_error = (
                isinstance(e, httpx.HTTPStatusError)
                and e.response.status_code < 500
            )
            if is_client_error or attempt == retries:
                raise
            print(f">>> 請求失敗（第 {attempt} 次），{delay} 秒後重試：{e!r}")
            await asyncio.sleep(delay)
