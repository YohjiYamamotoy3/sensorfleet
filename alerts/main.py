from fastapi import FastAPI, HTTPException
import asyncpg
import os
import redis.asyncio as redis
import json
from datetime import datetime

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://sensoruser:sensorpass@localhost:5432/sensorfleet")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

db_pool = None
redis_client = None

temp_threshold = 50.0
vibration_threshold = 80.0
humidity_threshold = 90.0
load_threshold = 95.0


@app.on_event("shutdown")
async def shutdown():
    global db_pool, redis_client
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

async def check_thresholds(data):
    alerts_created = []
    
    if data["temperature"] > temp_threshold:
        alert = {
            "sensor_id": data["sensor_id"],
            "alert_type": "temperature_high",
            "value": data["temperature"],
            "threshold": temp_threshold,
            "message": f"temperature {data['temperature']} exceeds threshold {temp_threshold}",
            "timestamp": datetime.now()
        }
        alerts_created.append(alert)
    
    if data["vibration"] > vibration_threshold:
        alert = {
            "sensor_id": data["sensor_id"],
            "alert_type": "vibration_high",
            "value": data["vibration"],
            "threshold": vibration_threshold,
            "message": f"vibration {data['vibration']} exceeds threshold {vibration_threshold}",
            "timestamp": datetime.now()
        }
        alerts_created.append(alert)
    
    if data["humidity"] > humidity_threshold:
        alert = {
            "sensor_id": data["sensor_id"],
            "alert_type": "humidity_high",
            "value": data["humidity"],
            "threshold": humidity_threshold,
            "message": f"humidity {data['humidity']} exceeds threshold {humidity_threshold}",
            "timestamp": datetime.now()
        }
        alerts_created.append(alert)
    
    if data["load"] > load_threshold:
        alert = {
            "sensor_id": data["sensor_id"],
            "alert_type": "load_high",
            "value": data["load"],
            "threshold": load_threshold,
            "message": f"load {data['load']} exceeds threshold {load_threshold}",
            "timestamp": datetime.now()
        }
        alerts_created.append(alert)
    
    return alerts_created

async def process_queue():
    while True:
        try:
            message = await redis_client.brpop("sensor_queue", timeout=5)
            if message:
                data = json.loads(message[1])
                alerts = await check_thresholds(data)
                
                if alerts:
                    async with db_pool.acquire() as conn:
                        for alert in alerts:
                            await conn.execute("""
                                INSERT INTO alerts (sensor_id, alert_type, value, threshold, message, timestamp)
                                VALUES ($1, $2, $3, $4, $5, $6)
                            """, alert["sensor_id"], alert["alert_type"], alert["value"], 
                                alert["threshold"], alert["message"], alert["timestamp"])
                            
                            await redis_client.lpush("alert_notifications", json.dumps(alert))
        except Exception as e:
            print(f"error processing queue: {e}")

@app.on_event("startup")
async def startup_with_background():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(database_url)
    redis_client = await redis.from_url(redis_url)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(100) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                value FLOAT NOT NULL,
                threshold FLOAT NOT NULL,
                message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON alerts(sensor_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)
        """)
    
    import asyncio
    asyncio.create_task(process_queue())

@app.get("/alerts")
async def get_alerts(sensor_id: str = None, limit: int = 100):
    try:
        async with db_pool.acquire() as conn:
            if sensor_id:
                rows = await conn.fetch("""
                    SELECT sensor_id, alert_type, value, threshold, message, timestamp
                    FROM alerts
                    WHERE sensor_id = $1
                    ORDER BY timestamp DESC
                    LIMIT $2
                """, sensor_id, limit)
            else:
                rows = await conn.fetch("""
                    SELECT sensor_id, alert_type, value, threshold, message, timestamp
                    FROM alerts
                    ORDER BY timestamp DESC
                    LIMIT $1
                """, limit)
            
            result = []
            for row in rows:
                result.append({
                    "sensor_id": row["sensor_id"],
                    "alert_type": row["alert_type"],
                    "value": row["value"],
                    "threshold": row["threshold"],
                    "message": row["message"],
                    "timestamp": row["timestamp"].isoformat()
                })
            
            return {"alerts": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/stats")
async def get_alert_stats():
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM alerts")
            by_type = await conn.fetch("""
                SELECT alert_type, COUNT(*) as count
                FROM alerts
                GROUP BY alert_type
            """)
            
            stats = {
                "total_alerts": total,
                "by_type": {row["alert_type"]: row["count"] for row in by_type}
            }
            
            return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

