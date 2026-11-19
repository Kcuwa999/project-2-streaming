# Currency Streaming (Kafka → Postgres)

Потоковая обработка событий о курсах валют: генератор событий шлёт сообщения в Kafka, Python-консьюмер валидирует их через Pydantic и пишет в Postgres с ретраями и обработкой ошибок.

## Стек

- **Брокер сообщений**: Apache Kafka (Docker, Confluent)
- **Консьюмер**: Python (kafka-python)
- **Валидация**: Pydantic
- **Хранилище**: PostgreSQL
- **Ретраи**: tenacity (экспоненциальная задержка)

## Архитектура

1. **simulator** — генерирует события (event_id, валюта, курс, timestamp, источник) и отправляет в топик.
2. **Kafka** — брокер, топик создаётся автоматически.
3. **consumer** — читает сообщения, валидирует схемой `CurrencyEvent`, пишет в таблицу `currency_stream`.
4. **Гарантии**: дедупликация по `event_id` (PK + ON CONFLICT DO NOTHING), ретраи записи при сбоях БД, невалидные сообщения логируются и не ломают поток.

## Как запустить

1. Заполните подключения: значения `ВПИШИТЕ_*` в `configs/config.yaml` и `docker/docker-compose.yml`.
2. Поднять инфраструктуру:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
