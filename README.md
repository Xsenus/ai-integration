# AI Integration Service

Асинхронное FastAPI‑приложение, которое объединяет Bitrix24, DaData, ScraperAPI, собственные витрины PostgreSQL и внешний сервис AI‑анализа. Сервис хранит текстовые снимки сайтов, каталожные результаты и вычисленные классы/оборудование, а также умеет запускать полный пайплайн по ИНН в один клик.

## Краткая архитектура

* **Веб‑слой:** `app/main.py` и роутеры в `app/api/*` формируют REST API, настраивают CORS и инициализируют подключения к БД.
* **Данные:** четыре отдельные базы (`bitrix_data`, `parsing_data`, `pp719`, основная `postgres`) и вспомогательные таблицы (`clients_requests`, `pars_site`, `ai_site_*`). Подключения создаются лениво — если DSN отсутствует, соответствующий функционал отключается.
* **Интеграции:**
  * Bitrix24 — клиент `app/bitrix/b24_client.py` и фоновая синхронизация `app/jobs/b24_sync_job.py` создают/обновляют `b24_companies_raw`.
  * Парсинг сайтов — ScraperAPI и собственные утилиты (`app/services/parse_site.py`) формируют текстовые снапшоты и embeddings.
  * Внешний AI‑анализ — HTTP‑клиент в `app/services/analyze_client.py` и обработчик `app/api/analyze_json.py`.
  * Каталоги и расчёт оборудования — функции в `app/services/ib_match.py` и `app/services/equipment_selection.py` работают с справочниками из `postgres`.

Подробные заметки о ключевых потоках лежат в каталоге `docs/`.

## Быстрый старт

1. Подготовьте окружение:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Скопируйте `.env.example` → `.env` и заполните DSN БД, ключи DaData/ScraperAPI и URL внешнего AI‑анализа.
3. Запустите приложение:
   ```bash
   uvicorn app.main:app --reload
   ```

При старте сервис проверяет подключение к доступным БД, создаёт `b24_companies_raw` в `bitrix_data`, обеспечивает схему `parsing_data` и (при `B24_SYNC_ENABLED=true`) запускает фоновую синхронизацию Bitrix24.

## Настройки окружения

`.env.example` содержит полный список переменных. Ключевые группы:

* **Базы данных:** `POSTGRES_DATABASE_URL`, `BITRIX_DATABASE_URL`, `PARSING_DATABASE_URL`, `PP719_DATABASE_URL`.
* **Логирование/SQL:** `LOG_LEVEL`, `ECHO_SQL` (подробный вывод SQLAlchemy).
* **DaData/ScraperAPI:** `DADATA_API_KEY`, `DADATA_SECRET_KEY`, `SCRAPERAPI_KEY`, ограничения парсинга (`PARSE_MAX_CHUNK_SIZE`, `PARSE_MIN_HTML_LEN`).
* **Внешний AI‑анализ:** `AI_ANALYZE_BASE`/`ANALYZE_BASE`, таймаут `AI_ANALYZE_TIMEOUT`/`ANALYZE_TIMEOUT`.
* **Bitrix24:** `B24_BASE_URL`, лимиты запросов (`B24_PAGE_LIMIT`, `B24_BATCH_*`), параметры синхронизации (`B24_SYNC_*`) и флаги логирования (`B24_LOG_*`).
* **CORS:** `CORS_ALLOW_ORIGINS`, `CORS_ALLOW_METHODS`, `CORS_ALLOW_HEADERS`, `CORS_ALLOW_CREDENTIALS`.

## API‑карточка

| Маршрут | Назначение |
| --- | --- |
| `GET /health` | Проверяет доступность всех настроенных БД. |
| `POST /v1/lookup/card`, `GET /v1/lookup/{inn}/card` | Создание/чтение расширенной карточки компании с сохранением в `bitrix_data` и `parsing_data`. |
| `POST /v1/parse-site`, `GET /v1/parse-site/{inn}` | Парсинг главных страниц доменов, сохранение текста/embedding в `pars_site` и привязка к ИНН. |
| `POST /v1/analyze-json`, `GET /v1/analyze-json/{inn}` | Подготовка снапшотов, вызов внешнего AI‑сервиса и запись каталожных ответов в БД. |
| `POST /v1/ib-match`, `POST/GET /v1/ib-match/by-inn` | Сопоставление `pars_site.text_vector` с каталогами `ib_prodclass`/оборудования. |
| `GET /v1/equipment-selection`, `GET /v1/equipment-selection/by-inn/{inn}` | Расчёт комплекта оборудования по последней записи клиента в `clients_requests`. |
| `GET /v1/analyze-service/health` | Проверка доступности внешнего сервиса анализа. |
| `GET /api/ai-analysis/companies` | Список компаний с агрегированным статусом AI-анализа, длительностью и прогрессом для UI-поллинга. |
| `POST /v1/pipeline/full` | Последовательный запуск lookup → parse-site → analyze-json → ib-match → equipment-selection для переданного ИНН. |
| `POST /v1/pipeline/auto` | Автообработка нескольких ИНН без готового результата расчёта оборудования. |

Детализированные сценарии и структуры ответов описаны в документах `docs/analyze_json_flow.md`, `docs/parse_site_pipeline.md`, `docs/analyze_json_changes.md`, `docs/prodclass_matching.md`, `docs/industry_detection.md`, `docs/pipeline_overview.md` и `docs/ai_analysis_timer_sync_contract.md`.

## Полезные заметки

* DSN для БД не обязательны: сервис автоматически отключает функции, завязанные на отсутствующие подключения.
* Для запуска Bitrix‑синхронизации нужны одновременно `BITRIX_DATABASE_URL`, `B24_BASE_URL` и `B24_SYNC_ENABLED=true`.
* Парсер сайтов обновляет `clients_requests` и может повторно использовать домен, уже сохранённый для компании.
* Все косинусные сравнения embeddings выполняются общей функцией `services.vector_similarity.cosine_similarity`, поэтому результаты согласованы между сервисами.
