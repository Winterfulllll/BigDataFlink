# BigDataFlink — Streaming Processing с помощью Apache Flink

Лабораторная работа №3 — потоковая обработка данных: **Kafka -> Flink -> PostgreSQL (модель звезда)**.

## Что сделано

1. **Kafka Producer** (`producer/producer.py`) — Python-приложение, которое читает 10 CSV-файлов из папки `исходные данные/`, преобразует каждую строку в JSON и отправляет в Kafka-топик `mock_data`.

2. **Flink SQL Streaming Job** (`flink-job/job.sql`) — SQL-скрипт для Flink, который в режиме streaming:
   - читает JSON-сообщения из Kafka-топика `mock_data`;
   - трансформирует плоские данные в модель звезда (5 измерений + 1 факт);
   - записывает результат в PostgreSQL с upsert-семантикой.

3. **Модель звезда в PostgreSQL** (`postgres/init.sql`):
   - `dim_customer` — покупатели (PK: `customer_id`)
   - `dim_seller` — продавцы (PK: `seller_id`)
   - `dim_product` — товары (PK: `product_id`)
   - `dim_store` — магазины (PK: `store_name`)
   - `dim_supplier` — поставщики (PK: `supplier_name`)
   - `fact_sales` — факт продаж (PK: `sale_id`)

4. **Docker Compose** (`docker-compose.yml`) — полная инфраструктура:
   - Zookeeper + Kafka (Confluent 7.5)
   - PostgreSQL 15
   - Apache Flink 1.17.2 (JobManager + TaskManager)
   - Producer (автоматическая отправка данных в Kafka)

## Архитектура

```
CSV-файлы ──> Producer ──> Kafka ──> Flink (streaming) ──> PostgreSQL
               (Python)     (topic:       (SQL job)        (star schema)
                            mock_data)
```

## Требования

- Docker и Docker Compose
- Свободные порты: `2181` (Zookeeper), `9092` (Kafka), `5433` (PostgreSQL), `8081` (Flink UI)

## Запуск

### 1. Запустить инфраструктуру

```bash
docker-compose up -d --build
```

Дождаться запуска всех сервисов (30–60 секунд):

```bash
docker-compose ps
```

Все контейнеры должны быть в состоянии `Up` (producer завершится после отправки данных).

### 2. Проверить, что данные отправлены в Kafka

```bash
docker-compose logs producer
```

В логах должно быть: `Done! Total records sent: 10000`.

### 3. Запустить Flink Streaming Job

```bash
docker-compose exec jobmanager ./bin/sql-client.sh -f /opt/flink-job/job.sql
```

После выполнения задание будет работать в streaming-режиме в кластере Flink.

### 4. Мониторинг Flink

Flink Web UI: [http://localhost:8081](http://localhost:8081)

Во вкладке **Running Jobs** должна появиться джоба `insert-into_default_catalog.default_database...`.

## Проверка результатов

### Подключение к PostgreSQL

```bash
docker-compose exec postgres psql -U admin -d starschema
```

### SQL-запросы для проверки

Количество записей в таблицах:

```sql
SELECT 'dim_customer'  AS tbl, COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_seller',            COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_product',           COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_store',             COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_supplier',          COUNT(*) FROM dim_supplier
UNION ALL
SELECT 'fact_sales',            COUNT(*) FROM fact_sales;
```

Пример данных из таблицы фактов:

```sql
SELECT * FROM fact_sales LIMIT 10;
```

Пример данных из измерения покупателей:

```sql
SELECT * FROM dim_customer LIMIT 10;
```

Аналитический запрос — продажи по странам покупателей:

```sql
SELECT c.country, COUNT(*) AS sales_count, SUM(f.total_price) AS total_revenue
FROM   fact_sales f
JOIN   dim_customer c ON f.customer_id = c.customer_id
GROUP  BY c.country
ORDER  BY total_revenue DESC
LIMIT  10;
```

## Остановка

```bash
docker-compose down -v
```

Флаг `-v` удаляет тома (данные PostgreSQL).

## Структура проекта

```
BigDataFlink/
├── исходные данные/         # 10 CSV-файлов (MOCK_DATA*.csv, по 1000 строк)
├── docker-compose.yml       # Docker Compose — вся инфраструктура
├── producer/
│   ├── Dockerfile           # Образ для Producer
│   ├── requirements.txt     # Python-зависимости (kafka-python-ng)
│   └── producer.py          # CSV -> JSON -> Kafka
├── flink/
│   └── Dockerfile           # Flink 1.17.2 + коннекторы (Kafka, JDBC, PostgreSQL)
├── flink-job/
│   └── job.sql              # Flink SQL: Kafka -> star schema -> PostgreSQL
├── postgres/
│   └── init.sql             # DDL для модели звезда
├── TASK.md                  # Описание задания
└── README.md                # Этот файл
```
