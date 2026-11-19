# Currency Streaming (Kafka → Postgres)

Потоковая обработка событий о курсах валют: генератор событий шлёт сообщения в Kafka, Python-консьюмер валидирует их через Pydantic и пишет в Postgres с ретраями и обработкой ошибок.

## Стек

- **Брокер сообщений**: Apache Kafka (Docker, Confluent)
- **Консьюмер**: Python (kafka-python)
- **Валидация**: Pydantic
- **Хранилище**: PostgreSQL
- **Ретраи**: tenacity (экспоненциальная задержка)

## Архитектура

1. **simulator** — генерирует фейковые события (event_id, валюта, курс, timestamp, источник) и отправляет в топик `currency_stream`.
2. **Kafka** — брокер, топик создаётся автоматически.
3. **consumer** — читает сообщения из топика, валидирует схемой `CurrencyEvent`, пишет в таблицу `currency_stream`.
4. **Гарантии**: дедупликация по `event_id` (PK + ON CONFLICT DO NOTHING), ретраи записи при сбоях БД, битые сообщения логируются и не ломают поток.

## Как запустить

1. Поднять инфраструктуру:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
