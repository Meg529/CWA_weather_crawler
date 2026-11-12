from sqlmodel import select
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from db import db_schema



class DBManager:

    def __init__(self, session):
        self.session = session


    async def get_all_cwa_stations(self):
        """取得全部要收集的測站"""

        statement = select(db_schema.CwaStation).order_by(
            db_schema.CwaStation.station_id
        )
        result = await self.session.execute(statement)
        return result.scalars().all()


    async def get_cwa_station_by_id(self, station_id: str):
        """取得單一收集測站，不存在回傳 None"""

        return await self.session.get(db_schema.CwaStation, station_id)


    async def add_cwa_station(self, station):
        """新增收集測站"""

        self.session.add(station)
        await self.session.commit()
        await self.session.refresh(station)

        return station


    async def delete_cwa_station(self, station_id: str):
        """刪除收集測站（局屬 cwb 測站不可刪除），回傳是否有刪到資料"""

        statement = delete(db_schema.CwaStation).where(
            db_schema.CwaStation.station_id == station_id,
            db_schema.CwaStation.station_type != "cwb",
        )
        result = await self.session.execute(statement)
        await self.session.commit()

        return result.rowcount > 0


    async def ensure_cwa_stations(self, records: list[dict]):
        """確保指定測站都在收集清單內，已存在的略過，回傳新增筆數"""

        if not records:
            return 0

        statement = insert(db_schema.CwaStation).values(
            records
        ).on_conflict_do_nothing(
            index_elements=["station_id"]
        )
        result = await self.session.execute(statement)
        await self.session.commit()

        return result.rowcount


    async def get_history_weather(
        self,
        station_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ):
        """取得歷史氣象資料"""

        statement = select(db_schema.HistoryWeather)

        if station_id:
            statement = statement.where(
                db_schema.HistoryWeather.station_id == station_id.strip().upper()
            )

        if start_time is not None:
            statement = statement.where(
                db_schema.HistoryWeather.time >= start_time
            )

        if end_time is not None:
            statement = statement.where(
                db_schema.HistoryWeather.time < end_time
            )

        result = await self.session.execute(statement)
        return result.scalars().all()


    async def add_history_weather(self, records: list[dict], batch_size: int = 1000):
        """新增歷史氣象資料，已存在的 (time, station_id) 直接略過，回傳實際寫入筆數"""

        inserted = 0

        # 分批寫入，避免超過 asyncpg 參數數量上限
        for i in range(0, len(records), batch_size):
            statement = insert(db_schema.HistoryWeather).values(
                records[i:i + batch_size]
            ).on_conflict_do_nothing(
                index_elements=["time", "station_id"]
            )
            result = await self.session.execute(statement)
            inserted += result.rowcount

        await self.session.commit()

        return inserted


    async def get_latest_history_collection(self):
        """取得最近一次成功的歷史氣象資料收集紀錄"""

        statement = select(db_schema.HistoryCollectionLog).where(
            db_schema.HistoryCollectionLog.status == "success"
        ).order_by(
            db_schema.HistoryCollectionLog.collected_at.desc()
        ).limit(1)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()


    async def get_history_collection_logs(self):
        """取得全部的歷史氣象資料收集紀錄"""
        statement = (
            select(db_schema.HistoryCollectionLog)
            .order_by(
                db_schema.HistoryCollectionLog.collected_at.desc()
            )
        )

        result = await self.session.execute(statement)
        return result.scalars().all()


    async def add_history_collection_log(self, log):
        """新增歷史氣象資料收集紀錄"""

        self.session.add(log)
        await self.session.commit()


    async def get_realtime_weather(
        self,
        station_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ):
        """取得即時氣象資料"""

        statement = select(db_schema.RealtimeWeather)

        if station_id:
            statement = statement.where(
                db_schema.RealtimeWeather.station_id == station_id.strip().upper()
            )

        if start_time is not None:
            statement = statement.where(
                db_schema.RealtimeWeather.time >= start_time
            )

        if end_time is not None:
            statement = statement.where(
                db_schema.RealtimeWeather.time < end_time
            )

        result = await self.session.execute(statement)
        return result.scalars().all()


    async def add_realtime_weather(self, records: list[dict], batch_size: int = 1000):
        """新增即時氣象資料，已存在的 (time, station_id) 直接略過，回傳實際寫入筆數"""

        inserted = 0

        # 分批寫入，避免超過 asyncpg 參數數量上限
        for i in range(0, len(records), batch_size):
            statement = insert(db_schema.RealtimeWeather).values(
                records[i:i + batch_size]
            ).on_conflict_do_nothing(
                index_elements=["time", "station_id"]
            )
            result = await self.session.execute(statement)
            inserted += result.rowcount

        await self.session.commit()

        return inserted


    async def get_realtime_collection_logs(self):
        """取得全部的即時氣象資料收集紀錄"""                
        statement = (
            select(db_schema.RealtimeCollectionLog)
            .order_by(
                db_schema.RealtimeCollectionLog.collected_at.desc()
            )
        )

        result = await self.session.execute(statement)
        return result.scalars().all()


    async def add_realtime_collection_log(self, log):
        """新增即時氣象資料收集紀錄"""

        self.session.add(log)
        await self.session.commit()


    async def get_predict_weather(
        self,
        county: str | None = None,
        location: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ):
        """取得天氣預報資料"""

        statement = select(db_schema.PredictWeather)

        if county:
            statement = statement.where(
                db_schema.PredictWeather.county == county
            )

        if location:
            statement = statement.where(
                db_schema.PredictWeather.location == location
            )

        if start_time is not None:
            statement = statement.where(
                db_schema.PredictWeather.forecast_time >= start_time
            )

        if end_time is not None:
            statement = statement.where(
                db_schema.PredictWeather.forecast_time < end_time
            )

        result = await self.session.execute(statement)
        return result.scalars().all()


    async def add_predict_weather(self, records):
        """新增天氣預報資料"""

        self.session.add_all(records)
        await self.session.commit()


    async def delete_all_predict_weather(self):
        """刪除所有天氣預報資料"""

        statement = delete(db_schema.PredictWeather)
        await self.session.execute(statement)
        await self.session.commit()


    async def get_latest_predict_collection(self):
        """取得最近一次成功的天氣預報收集紀錄"""

        statement = select(db_schema.PredictCollectionLog).where(
            db_schema.PredictCollectionLog.status == "success"
        ).order_by(
            db_schema.PredictCollectionLog.collected_at.desc()
        ).limit(1)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()


    async def get_predict_collection_logs(self):
        """取得全部的天氣預報資料收集紀錄"""
        statement = (
            select(db_schema.PredictCollectionLog)
            .order_by(
                db_schema.PredictCollectionLog.collected_at.desc()
            )
        )

        result = await self.session.execute(statement)
        return result.scalars().all()

    
    async def add_predict_collection_log(self, log):
        """新增天氣預報資料收集紀錄"""

        self.session.add(log)
        await self.session.commit()