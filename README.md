# AI Integration Service

AI Integration Service — асинхронное FastAPI-приложение, которое объединяет работу с данными Bitrix24, DaData и внутренними витринами PostgreSQL. Приложение хранит результаты DaData и витринные данные в нескольких базах, синхронизирует компании из Bitrix24 и умеет инициировать внешний AI-анализ по домену компании.

## Архитектура

- **FastAPI** (`app/main.py`) — точка входа, инициализирует подключения к четырём БД (`bitrix_data`, `parsing_data`, `pp719`, основная `postgres`), настраивает CORS и запускает фоновый цикл синхронизации компаний из Bitrix24.
- **API-модули**
  - `app/api/routes.py` — расширенные карточки компании `/v1/lookup/card` (POST/GET), запись сведений в PostgreSQL и зеркало `parsing_data`, а также парсинг главной страницы домена `/v1/parse-site` (POST/GET) c сохранением текста в `pars_site`.
  - `app/api/ai_analyzer.py` — маршрут `/v1/lookup/ai-analyzer`, который дополняет данные из БД запросом к внешнему AI-сервису.
- **Бэкенд-слой**
  - `app/db/*` — подключение и вспомогательные операции для каждой базы, включая создание таблиц Bitrix и зеркалирование данных в `clients_requests`/`pars_site`.
  - `app/bitrix/b24_client.py` и `app/jobs/b24_sync_job.py` — клиент Bitrix24 и фоновая синхронизация компаний в `bitrix_data`.
  - `app/services/*` — интеграции с DaData, ScraperAPI и сервисом AI-анализа.

## Предварительные требования

- Python 3.11+
- PostgreSQL c доступом к базам `bitrix_data`, `parsing_data`, `pp719` и основной `postgres`
- Доступ к сторонним сервисам (DaData, ScraperAPI, внешний AI-анализ) при необходимости

## Настройка окружения

1. Создайте виртуальное окружение и установите зависимости:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Скопируйте `.env.example` в `.env` и заполните значения под свою инфраструктуру.

### Переменные окружения

`.env.example` содержит все доступные ключи и комментарии. Основные группы настроек:

- **Базы данных**
  - `POSTGRES_DATABASE_URL`, `BITRIX_DATABASE_URL`, `PARSING_DATABASE_URL`, `PP719_DATABASE_URL`
- **Логирование и SQL**
  - `LOG_LEVEL`, `ECHO_SQL`
- **DaData и ScraperAPI**
  - `DADATA_API_KEY`, `DADATA_SECRET_KEY`, `SCRAPERAPI_KEY`
- **Внешний AI-анализ**
  - `AI_ANALYZE_BASE` (`ANALYZE_BASE` в качестве альтернативы)
  - `AI_ANALYZE_TIMEOUT` (`ANALYZE_TIMEOUT` как альтернативное имя)
- **Bitrix24**
  - `B24_BASE_URL`, лимиты и параметры batch (`B24_PAGE_LIMIT`, `B24_BATCH_*`)
  - настройки фоновой синхронизации (`B24_SYNC_*`)
  - флаги логирования (`B24_LOG_VERBOSE`, `B24_LOG_BODIES`, `B24_LOG_BODY_CHARS`)
- **CORS** — `CORS_ALLOW_ORIGINS`, `CORS_ALLOW_METHODS`, `CORS_ALLOW_HEADERS`, `CORS_ALLOW_CREDENTIALS`
- **Парсинг сайтов** — `PARSE_MAX_CHUNK_SIZE`, `PARSE_MIN_HTML_LEN`

## Запуск приложения

```bash
uvicorn app.main:app --reload
```

При старте приложение проверяет доступность подключений, создаёт при необходимости таблицу `b24_companies_raw` в `bitrix_data` и, если включена опция `B24_SYNC_ENABLED`, запускает фоновый цикл синхронизации с Bitrix24.

## Основные эндпоинты

- `GET /health` — пинг всех сконфигурированных баз.
- `POST /v1/lookup/card` и `GET /v1/lookup/{inn}/card` — получение и сохранение расширенных карточек компании по ИНН с записью в обе БД.
- `POST /v1/lookup/ai-analyzer` — агрегация сохранённых данных с результатами внешнего AI-анализа по домену.
- `POST /v1/analyze-json` — полный цикл подготовки текста, запроса к внешнему JSON-интерфейсу анализа и записи ответа в БД.
- `POST /v1/parse-site` и `GET /v1/parse-site/{inn}` — загрузка главной страницы компании через ScraperAPI, нарезка текста на чанки и сохранение в `pars_site` (GET автоматически ищет домен по ИНН).

## Полезные заметки

- Подключения к базам создаются лениво; при отсутствии DSN соответствующий функционал отключается.
- Для запуска синхронизации с Bitrix24 необходимо одновременно задать `BITRIX_DATABASE_URL`, `B24_BASE_URL` и включить `B24_SYNC_ENABLED`.
- Парсер сайтов автоматически дополняет `clients_requests` данными о домене и может переиспользовать последний домен компании из основной БД.

