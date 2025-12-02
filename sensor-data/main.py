from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import os
import redis.asyncio as redis
import json

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://sensoruser:sensorpass@localhost:5432/sensorfleet")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

db_pool = None
redis_client = None

class SensorData(BaseModel):
    sensor_id: str
    temperature: float
    humidity: float
    vibration: float
    load: float
    timestamp: datetime = None

@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(database_url)
    redis_client = await redis.from_url(redis_url)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(100) NOT NULL,
                temperature FLOAT NOT NULL,
                humidity FLOAT NOT NULL,
                vibration FLOAT NOT NULL,
                load FLOAT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_readings(sensor_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_readings(timestamp)
        """)

@app.on_event("shutdown")
async def shutdown():
    global db_pool, redis_client
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

@app.post("/sensor-data")
async def receive_sensor_data(data: SensorData):
    if data.timestamp is None:
        data.timestamp = datetime.now()
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO sensor_readings (sensor_id, temperature, humidity, vibration, load, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, data.sensor_id, data.temperature, data.humidity, data.vibration, data.load, data.timestamp)
        
        message = {
            "sensor_id": data.sensor_id,
            "temperature": data.temperature,
            "humidity": data.humidity,
            "vibration": data.vibration,
            "load": data.load,
            "timestamp": data.timestamp.isoformat()
        }
        
        await redis_client.lpush("sensor_queue", json.dumps(message))
        
        return {"status": "ok", "message": "data saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sensor-data/{sensor_id}")
async def get_sensor_data(sensor_id: str, limit: int = 100):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT sensor_id, temperature, humidity, vibration, load, timestamp
                FROM sensor_readings
                WHERE sensor_id = $1
                ORDER BY timestamp DESC
                LIMIT $2
            """, sensor_id, limit)
            
            result = []
            for row in rows:
                result.append({
                    "sensor_id": row["sensor_id"],
                    "temperature": row["temperature"],
                    "humidity": row["humidity"],
                    "vibration": row["vibration"],
                    "load": row["load"],
                    "timestamp": row["timestamp"].isoformat()
                })
            
            return {"sensor_id": sensor_id, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

