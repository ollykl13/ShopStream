# 🛒 ShopStream: Real-time Analytics Platform

**ShopStream** — это end-to-end платформа аналитики интернет-магазина, обрабатывающая поток событий в режиме, близком к реальному времени. Проект демонстрирует современный стек данных: от сбора сырых событий до построения интерактивных BI-дашбордов с автоматической оркестрацией через Airflow.

---

## 📑 Оглавление
1. [Описание решения](#-описание-решения)
2. [Архитектура](#-архитектура)
3. [Технологический стек](#-технологический-стек)
4. [Детали инфраструктуры](#-детали-инфраструктуры)
5. [Модели данных](#-модели-данных)
6. [Ключевые результаты](#-ключевые-результаты)
7. [Шаги развертывания](#-шаги-развертывания)
8. [Троблшутинг (Windows)](#-троблшутинг-windows)
9. [Changelog](#-changelog)

---

## 📖 Описание решения

**Бизнес-проблема:** Классические реляционные СУБД (PostgreSQL, MySQL) не справляются с аналитикой на десятках миллионов строк в реальном времени — запросы агрегации занимают минуты, а дашборды отстают от реальности.

**Наше решение:** Гибридная архитектура (Lambda-like), где:
1. **Слой скорости (Real-time):** события из Kafka стекаются в ClickHouse через Kafka Engine и Materialized View — данные доступны за секунды.
2. **Слой трансформаций (Batch/Speed):** dbt строит очищенные слои (staging) и бизнес-витрины (marts). Инкрементальные модели обрабатывают только новые данные.
3. **Оркестрация:** Apache Airflow запускает `dbt build` **только при появлении новых данных** (watermark-паттерн), не тратя ресурсы вхолостую.
4. **Визуализация:** Grafana подключается напрямую к витринам ClickHouse и обновляется каждые 5 секунд.

**Объём данных:**
- **Октябрь 2019:** 42.4M событий (загружено полностью, ~4.5 ГБ).
- **Ноябрь 2019:** поток в реальном времени (~500 событий/сек, на момент написания — 1.9M+ строк).


---

## 🏗 Архитектура

**Поток данных:**

1. **CSV (42.4M events)** → Python Producer (~500 ev/s) → **Redpanda/Kafka** (topic `shop_events`, 3 partitions, 1 replica)
2. **Redpanda** → Kafka Engine (2 consumers, consumer group `clickhouse_consumer_v8`) → **ClickHouse** `raw_events` (MergeTree, partition by month)
3. **raw_events** → dbt `stg_events` (View) → dbt marts (5 витрин)
4. **marts** → **Grafana** (autorefresh 5s, ClickHouse datasource)
5. **Airflow** каждую минуту проверяет `shop.dbt_watermark` → запускает `dbt build` при новых данных

**Слои данных:**
- `shop.raw_events` — сырые события (MergeTree)
- `shop_staging.stg_events` — очищенные события (View)
- `shop_marts.*` — 5 материализованных витрин

---

## 🛠 Технологический стек

| Слой | Технология | Роль |
|---|---|---|
| Источник | REES46 Dataset (Kaggle) | 42.4M событий (октябрь) + поток (ноябрь) |
| Продюсер | Python 3 + `kafka-python-ng` | Отправка событий в Kafka |
| Брокер | **Redpanda v24.2.7** | Kafka-совместимый, буферизация |
| Хранилище | **ClickHouse 24.10** | OLAP, real-time аналитика |
| Трансформации | **dbt-core 1.12.6 + dbt-clickhouse 1.10.3** | 6 моделей, 26 тестов |
| Оркестрация | **Apache Airflow 2.11.2** | Watermark-пайплайн |
| Визуализация | **Grafana 11.x** + ClickHouse datasource | Live-дашборд 5s |
| Инфраструктура | Docker Compose + named volumes | 6 контейнеров |
| Метаданные | PostgreSQL 13 | Backend для Airflow |


---

## 🔧 Детали инфраструктуры

### Kafka / Redpanda

| Параметр | Значение |
|---|---|
| **Топик** | `shop_events` |
| **Партиций** | **3** |
| **Реплик** | **1** (single-node) |
| **Продюсеров** | **1** (Python `replay.py`, ~500 ev/s) |
| **Консьюмеров ClickHouse** | **2** (`kafka_num_consumers = 2`) |
| **Consumer Group** | `clickhouse_consumer_v8` |
| **`auto_offset_reset`** | `earliest` (через `config.d/kafka.xml`) |

> **Почему 3 партиции:** параллелизм чтения (2 консьюмера на 3 партициях).

### ClickHouse

| Параметр | Значение |
|---|---|
| **Версия** | 24.10 |
| **Режим** | **Single-node** (без шардов и реплик) |
| **Движок** | MergeTree family |
| **Порты** | 8123 (HTTP), 9000 (Native) |
| **Named volume** | `clickhouse-data` |

**Про шарды и реплики:**
- В текущей конфигурации — **single-node**, без шардирования и репликации.
- Для production: `ReplicatedMergeTree` + ClickHouse Keeper (2–3 реплики), `Distributed` таблицы для шардирования, `ON CLUSTER` DDL.
- Для учебного проекта single-node достаточно — 42M строк агрегируются за 0.18 сек.

### Airflow

| Параметр | Значение |
|---|---|
| **Версия** | 2.11.2 |
| **Executor** | `LocalExecutor` |
| **DAG** | `shopstream_realtime_dbt` |
| **Schedule** | `* * * * *` (каждую минуту) |
| **Tasks** | 3: `check_for_new_events` → `run_dbt_build` → `update_watermark` |
| **Watermark** | `shop.dbt_watermark` (ReplacingMergeTree) |

### Grafana

| Параметр | Значение |
|---|---|
| **Datasource** | ClickHouse (HTTP, `clickhouse:8123`) |
| **Autorefresh** | 5 секунд |
| **Панели** | 8 (4 исторических + 4 live) |
| **Named volume** | `grafana-data` |


---

## 🗄 Модели данных

### ClickHouse (raw layer)

| Таблица | Движок | Назначение | Строк |
|---|---|---|---|
| `shop.raw_events` | MergeTree (partition: `toYYYYMM`) | Сырые события | ~44M |
| `shop.kafka_events` | Kafka Engine | Читает из топика | — |
| `shop.kafka_events_errors` | MergeTree | Карантин битых сообщений | 0 |
| `shop.dbt_watermark` | ReplacingMergeTree | Метка последней обработки | 1 |
| `shop.mv_kafka_to_raw` | Materialized View | Kafka → raw_events | — |
| `shop.mv_kafka_errors` | Materialized View | Kafka → quarantine | — |

**Партиции `raw_events`:** `201910` (~42.4M), `201911` (~1.7M).

### dbt-модели

| Слой | Модель | Тип | Назначение |
|---|---|---|---|
| staging | `stg_events` | View | Очищенные события |
| marts | `mart_daily_sales` | **Incremental** | GMV, заказы, AOV по дням |
| marts | `mart_realtime_funnel` | **Incremental** | Воронка по часам |
| marts | `mart_top_products` | Table | Топ товаров/брендов |
| marts | `mart_rfm_segments` | Table | RFM-сегментация (347K) |
| marts | `mart_cohorts` | Table | Когортный retention |

**Data-тесты:** 26. `dbt build` → `PASS=32` (6 моделей + 26 тестов).

**Производительность:**
- `--full-refresh` (все модели): ~10 сек.
- `dbt build` (инкремент, без новых): **< 2 сек**.
- `--select mart_daily_sales mart_realtime_funnel`: **0.79 сек**.

---

## 📈 Ключевые результаты

### Данные
- **Обработано:** 42 470 945 событий (октябрь) + ~1.9M ноябрьских.
- **Уникальных пользователей:** 3 022 290.
- **Уникальных товаров:** 166 794.

### Бизнес-инсайты
- **GMV за октябрь:** $230M.
- **Топ-бренд:** Apple ($111M) — за счёт высокого среднего чека.
- **RFM `cant_lose`:** 54% выручки ($105M).
- **Retention:** 15.2% (1-я неделя), 9.35% (4-я).
- **Конверсия view → purchase:** 1.6–2.1%.

### Производительность
- **Запрос 31 день × агрегация:** **0.176 сек** на 42M строк.
- **Сжатие:** 3.81 ГБ → 1.82 ГБ (**2.1x**).


---

## 🚀 Шаги развертывания

### 1. Клонирование и установка

    git clone <url>
    cd shopstream
    python -m venv .venv
    source .venv/Scripts/activate
    pip install -r requirements.txt

### 2. Запуск инфраструктуры

    docker compose -f infra/docker-compose.yml up -d

Проверка: 6 контейнеров `Up`.

### 3. Инициализация ClickHouse

    MSYS_NO_PATHCONV=1 docker compose -f infra/docker-compose.yml exec -T clickhouse \
        clickhouse-client --multiquery < clickhouse/init/01_schema.sql

### 4. Данные

Скачать `2019-Oct.csv` и `2019-Nov.csv` с [Kaggle REES46](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) в `data/rees46/`.

### 5. Запуск потока

Первый запуск:

    python producer/replay.py --file data/rees46/2019-Oct.csv --rate 500 --skip-errors

Возобновление после остановки:

    python producer/replay.py --file data/rees46/2019-Nov.csv --resume --rate 500 --skip-errors

### 6. dbt

    cd dbt_shop
    dbt debug
    dbt build
    dbt docs serve --port 8085

### 7. Airflow

1. Открыть `http://localhost:8080` (admin/admin).
2. **Admin → Connections:** `clickhouse_shop` (Host: `clickhouse`, Port: `8123`, Type: `Generic`).
3. Включить DAG `shopstream_realtime_dbt`.

### 8. Grafana

1. Открыть `http://localhost:3000` (admin/admin).
2. Datasource: `ClickHouse-Shop` (Host: `clickhouse`, Port: `8123`).
3. Импорт: `grafana/dashboards/shopstream_overview.json`.

---

## 🩹 Троблшутинг (Windows)

| Проблема | Причина | Решение |
|---|---|---|
| `Permission denied` при вставке | Bind-mount на NTFS | Named volume `clickhouse-data` |
| DAG не виден в Airflow | Относительный путь `./` от `infra/` | Заменить на `../airflow/dags` |
| `NoBrokersAvailable` | `localhost` в контейнере | `redpanda:9092` |
| `dbt: Path does not exist` | Airflow не видит профиль | `--profiles-dir /opt/airflow/dbt_shop` |
| `Connection refused` для `clickhouse` | dbt с хоста ≠ dbt в контейнере | 2 outputs в `profiles.yml` |
| `BrokenPipeError` в scheduler | `LocalExecutor` на Windows | `SequentialExecutor` |


---

## 📝 Changelog

### [1.5.0] - 2026-11-05
#### Добавлено
- Непрерывная загрузка ноябрьского файла (~500 ev/s).
- Инкрементальные модели `mart_daily_sales` и `mart_realtime_funnel`.
- Watermark-паттерн в Airflow.
- Resume для продюсера (`--skip`, `--offset`, `--resume`).
- Live-панели Grafana.
- Named volume `grafana-data`.

### [1.4.0] - 2026-11-03
#### Добавлено
- Apache Airflow 2.11.2 с DAG `shopstream_realtime_dbt`.
- Таблица `shop.dbt_watermark`.
- Автозапуск `dbt build` при новых данных.

### [1.3.0] - 2026-10-23
#### Добавлено
- Grafana datasource + дашборд `ShopStream Overview`.

### [1.2.0] - 2026-10-23
#### Добавлено
- dbt-проект: 6 моделей, 26 тестов, `PASS=32`.
- 5 витрин + dbt docs.

### [1.1.0] - 2026-10-22
#### Добавлено
- Загрузка 42.4M строк в ClickHouse.
- Python-продюсер `replay.py`.

### [1.0.1] - 2026-10-22
#### Исправлено
- `Permission denied` (bind-mount → volumes).
- MV теряли связь с Kafka Engine.
- Формат данных (`UTC`, пустые поля).

### [1.0.0] - 2026-10-22
#### Добавлено
- Docker Compose: ClickHouse, Redpanda, Grafana.
- Схема БД: `raw_events`, `kafka_events`, `mv_kafka_to_raw`.
- Сквозной пайплайн на 10 событиях.
