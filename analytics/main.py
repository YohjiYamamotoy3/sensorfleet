from fastapi import FastAPI, HTTPException
import asyncpg
import os
from datetime import datetime, timedelta

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://sensoruser:sensorpass@localhost:5432/sensorfleet")

db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(database_url)

@app.on_event("shutdown")
async def shutdown():
    global db_pool
    if db_pool:
        await db_pool.close()

@app.get("/analytics/average")
async def get_average(sensor_id: str = None, hours: int = 24):
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        async with db_pool.acquire() as conn:
            if sensor_id:
                row = await conn.fetchrow("""
                    SELECT 
                        AVG(temperature) as avg_temp,
                        AVG(humidity) as avg_humidity,
                        AVG(vibration) as avg_vibration,
                        AVG(load) as avg_load
                    FROM sensor_readings
                    WHERE sensor_id = $1 AND timestamp >= $2
                """, sensor_id, cutoff_time)
            else:
                row = await conn.fetchrow("""
                    SELECT 
                        AVG(temperature) as avg_temp,
                        AVG(humidity) as avg_humidity,
                        AVG(vibration) as avg_vibration,
                        AVG(load) as avg_load
                    FROM sensor_readings
                    WHERE timestamp >= $1
                """, cutoff_time)
            
            if row and row["avg_temp"]:
                return {
                    "sensor_id": sensor_id or "all",
                    "period_hours": hours,
                    "average": {
                        "temperature": round(float(row["avg_temp"]), 2),
                        "humidity": round(float(row["avg_humidity"]), 2),
                        "vibration": round(float(row["avg_vibration"]), 2),
                        "load": round(float(row["avg_load"]), 2)
                    }
                }
            else:
                return {"sensor_id": sensor_id or "all", "message": "no data"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/maximum")
async def get_maximum(sensor_id: str = None, hours: int = 24):
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        async with db_pool.acquire() as conn:
            if sensor_id:
                row = await conn.fetchrow("""
                    SELECT 
                        MAX(temperature) as max_temp,
                        MAX(humidity) as max_humidity,
                        MAX(vibration) as max_vibration,
                        MAX(load) as max_load
                    FROM sensor_readings
                    WHERE sensor_id = $1 AND timestamp >= $2
                """, sensor_id, cutoff_time)
            else:
                row = await conn.fetchrow("""
                    SELECT 
                        MAX(temperature) as max_temp,
                        MAX(humidity) as max_humidity,
                        MAX(vibration) as max_vibration,
                        MAX(load) as max_load
                    FROM sensor_readings
                    WHERE timestamp >= $1
                """, cutoff_time)
            
            if row and row["max_temp"]:
                return {
                    "sensor_id": sensor_id or "all",
                    "period_hours": hours,
                    "maximum": {
                        "temperature": round(float(row["max_temp"]), 2),
                        "humidity": round(float(row["max_humidity"]), 2),
                        "vibration": round(float(row["max_vibration"]), 2),
                        "load": round(float(row["max_load"]), 2)
                    }
                }
            else:
                return {"sensor_id": sensor_id or "all", "message": "no data"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/top-sensors")
async def get_top_sensors(metric: str = "load", limit: int = 10, hours: int = 24):
    try:
        if metric not in ["load", "temperature", "vibration", "humidity"]:
            raise HTTPException(status_code=400, detail="invalid metric")
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        async with db_pool.acquire() as conn:
            query = f"""
                SELECT 
                    sensor_id,
                    AVG({metric}) as avg_value,
                    MAX({metric}) as max_value
                FROM sensor_readings
                WHERE timestamp >= $1
                GROUP BY sensor_id
                ORDER BY avg_value DESC
                LIMIT $2
            """
            rows = await conn.fetch(query, cutoff_time, limit)
            
            result = []
            for row in rows:
                result.append({
                    "sensor_id": row["sensor_id"],
                    "average": round(float(row["avg_value"]), 2),
                    "maximum": round(float(row["max_value"]), 2)
                })
            
            return {
                "metric": metric,
                "period_hours": hours,
                "top_sensors": result
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/sensor-stats/{sensor_id}")
async def get_sensor_stats(sensor_id: str, hours: int = 24):
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as count,
                    AVG(temperature) as avg_temp,
                    AVG(humidity) as avg_humidity,
                    AVG(vibration) as avg_vibration,
                    AVG(load) as avg_load,
                    MAX(temperature) as max_temp,
                    MAX(humidity) as max_humidity,
                    MAX(vibration) as max_vibration,
                    MAX(load) as max_load,
                    MIN(temperature) as min_temp,
                    MIN(humidity) as min_humidity,
                    MIN(vibration) as min_vibration,
                    MIN(load) as min_load
                FROM sensor_readings
                WHERE sensor_id = $1 AND timestamp >= $2
            """, sensor_id, cutoff_time)
            
            if row and row["count"] > 0:
                return {
                    "sensor_id": sensor_id,
                    "period_hours": hours,
                    "readings_count": row["count"],
                    "temperature": {
                        "avg": round(float(row["avg_temp"]), 2),
                        "max": round(float(row["max_temp"]), 2),
                        "min": round(float(row["min_temp"]), 2)
                    },
                    "humidity": {
                        "avg": round(float(row["avg_humidity"]), 2),
                        "max": round(float(row["max_humidity"]), 2),
                        "min": round(float(row["min_humidity"]), 2)
                    },
                    "vibration": {
                        "avg": round(float(row["avg_vibration"]), 2),
                        "max": round(float(row["max_vibration"]), 2),
                        "min": round(float(row["min_vibration"]), 2)
                    },
                    "load": {
                        "avg": round(float(row["avg_load"]), 2),
                        "max": round(float(row["max_load"]), 2),
                        "min": round(float(row["min_load"]), 2)
                    }
                }
            else:
                return {"sensor_id": sensor_id, "message": "no data"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

