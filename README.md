# CWA Weather Crawler

定期收集中央氣象署（CWA）氣象資料並存入 PostgreSQL 的 FastAPI 服務。

## 服務內容

| 資料 | 來源 | 排程 |
|---|---|---|
| History 歷史觀測 | CODiS 測站逐時資料 | 每天 12:00（`TIMEZONE` 時區），自動補抓上次之後缺漏的資料；首次執行從 2026-09-01 開始回補 |
| Realtime 即時觀測 | CWA 網站各測站近 24 小時觀測 | 每 1 小時 |
| Predict 天氣預報 | CWA 開放資料平臺 `F-D0047` 鄉鎮預報 | 每 3 小時 |

- 服務啟動時會建立資料表、把局屬（cwb）測站加入收集清單，並先執行一次三種資料的收集，之後再依排程執行。
- 收集清單可透過 API 新增其他測站，並可指定日期回補歷史資料。
- 每次收集都會寫入 collection log，可查詢成功或失敗紀錄。

## 設定 .env

```bash
cp .env.example .env
```

再依 `.env.example` 的註解填入各項值：

- `POSTGRES_*`：資料庫連線設定。用 docker compose 執行時，`POSTGRES_HOST=postgres`、`POSTGRES_PORT=5432`；帳號、密碼、資料庫名稱自訂即可，postgres 容器第一次啟動時會依此建立。
- `CWA_WEATHER_*_URL`：三種資料來源的網址，`.env.example` 註解中有範例。
- `CWA_TOKEN`：中央氣象署開放資料平臺授權碼，請至 <https://opendata.cwa.gov.tw> 註冊取得。
- `TIMEZONE`：例如 `Asia/Taipei`。

所有變數都是必填，缺少任何一個服務都無法啟動。

## 執行

### Docker（建議）

```bash
docker compose up -d --build
```

- API：<http://localhost:8012>，API 文件：<http://localhost:8012/docs>
- PostgreSQL：對外 port `5440`，資料存放在 `./pgdata/`（不納入 git）

### 本機開發

需要 [uv](https://docs.astral.sh/uv/) 以及可連線的 PostgreSQL（可只啟動 compose 的資料庫：`docker compose up -d postgres`，此時 `.env` 改用 `POSTGRES_HOST=localhost`、`POSTGRES_PORT=5440`）。

```bash
uv sync
uv run uvicorn app:app --reload
```

API 文件：<http://localhost:8000/docs>

## 主要 API

| 方法 | 路徑 | 說明 |
|---|---|---|
| GET | `/health` | 健康檢查 |
| GET / POST / DELETE | `/cwa-stations` | 查詢、新增（可帶 `backfill_from` 回補）、刪除收集的測站 |
| GET | `/climate-data/{history,realtime,predict}/stations` | 各類資料的測站或地點 |
| GET | `/climate-data/{history,realtime,predict}/data` | 查詢資料，可依測站與時間區間（Unix timestamp）篩選 |
| GET | `/collection-log/{history,realtime,predict}` | 收集紀錄 |
| POST | `/crawler/{history,realtime,predict}` | 手動觸發一次收集 |

## 專案結構

```
app.py          FastAPI 入口與 API
settings.py     從 .env 讀取設定
data_source/    向 CWA / CODiS 取得資料
db/             資料表定義與資料庫存取
scheduler/      三種資料的排程
```
