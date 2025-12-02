# sensorfleetmonitor

микросервисная система мониторинга сенсоров.

## микросервисы

- **sensor-data** (8001) - прием данных сенсоров, сохранение в postgresql
- **alerts** (8002) - проверка порогов, создание алертов (температура>50, вибрация>80, влажность>90, нагрузка>95)
- **analytics** (8003) - агрегация данных, отчеты (средние, максимумы, топ сенсоров)
- **files** (8004) - хранение логов и графиков

## запуск

```bash
docker-compose up -d
```

сервисы:
- postgresql:5432
- redis:6379
- микросервисы: 8001-8004

## примеры

отправить данные:
```bash
curl -X POST http://localhost:8001/sensor-data -H "Content-Type: application/json" -d '{"sensor_id":"sensor_001","temperature":55,"humidity":65,"vibration":85,"load":80}'
```

алерты: `GET http://localhost:8002/alerts`
аналитика: `GET http://localhost:8003/analytics/average`
файлы: `GET http://localhost:8004/files/list`
