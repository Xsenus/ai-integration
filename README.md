# AI Integration Service

Сервис объединяет несколько источников данных (Bitrix24, DaData, парсинг сайтов, внутренние каталоги и внешний AI‑анализ) в единый API для обработки компаний по ИНН. Основная цель — автоматизировать путь от «ввели ИНН» до «получили отрасль, продуктовые классы и подбор оборудования».

## 1) Что делает сервис

- хранит и обновляет карточку компании по ИНН;
- находит/нормализует домены компании и парсит сайт;
- получает AI‑описание сайта и сохраняет вычисленные признаки;
- сопоставляет компанию с внутренним каталогом `ib_prodclass`;
- рассчитывает финальный набор оборудования;
- запускает все шаги последовательно через единый pipeline.

Сервис реализован как асинхронное FastAPI‑приложение и рассчитан на работу с несколькими БД одновременно.

---

## 2) Архитектура по слоям

### API слой
- Точка входа: `app/main.py`.
- Роутеры:
  - `app/api/lookup.py` — карточка компании и обогащение по DaData;
  - `app/api/routes.py` — parse-site, ib-match, equipment-selection, health внешнего анализа;
  - `app/api/analyze_json.py` — работа с внешним AI‑анализатором;
  - `app/api/pipeline.py` — полный и авто‑pipeline;
  - `app/api/ai_analysis.py` — агрегированные статусы AI‑обработки для UI.

### Сервисный слой
- `app/services/parse_site.py` — парсинг доменов, нормализация, fallback по ОКВЭД.
- `app/services/analyze_client.py` — HTTP‑клиент внешнего AI сервиса.
- `app/services/ib_match.py` — сопоставление эмбеддингов с каталогом классов.
- `app/services/equipment_selection.py` — стратегии источника данных для подбора оборудования.
- `app/services/dadata_client.py`, `app/services/mapping.py` — интеграция и маппинг DaData.

### Доступ к данным
- SQLAlchemy engine/session в `app/db/*.py`.
- Репозитории для Bitrix/DaData: `app/repo/*.py`.
- Основные ORM‑модели: `app/models/*.py`.

---

## 3) Базы и ответственность

Сервис может работать частично: если какой‑то DSN не задан, соответствующие функции отключаются.

- `BITRIX_DATABASE_URL` (`bitrix_data`):
  - кэш карточек DaData (`DaDataResult`),
  - хранение сырых данных,
  - служебная таблица `b24_companies_raw`.
- `PARSING_DATABASE_URL` (`parsing_data`):
  - хранение запросов клиентов,
  - синхронизация парсинга и доменов.
- `POSTGRES_DATABASE_URL` (`postgres`):
  - `pars_site`, `ai_site_*`, `ib_*`, `EQUIPMENT_*` и витрины для подбора.
- `PP719_DATABASE_URL` (`pp719`):
  - дополнительные справочные проверки (в т.ч. для lookup‑логики).

---

## 4) Жизненный цикл приложения

При старте (`startup`):
1. Создаются подключения к БД, для которых есть DSN.
2. Обеспечивается схема `parsing_data`.
3. Проверяется/создаётся `b24_companies_raw`.
4. При `B24_SYNC_ENABLED=true` запускается фоновая синхронизация Bitrix24.

При остановке (`shutdown`):
1. Отменяются фоновые задачи.
2. Закрываются HTTP‑клиенты внешних сервисов.
3. Диспозятся все SQLAlchemy engines.

`GET /health` проверяет доступность подключённых БД и возвращает агрегированный статус.

---

## 5) API и реальное назначение ручек

| Роут | Для чего нужен |
| --- | --- |
| `GET /health` | Проверка состояния БД и сервиса. |
| `POST /v1/lookup/card`, `GET /v1/lookup/{inn}/card` | Получение/обновление карточки компании по ИНН и запись в локальные таблицы. |
| `POST /v1/parse-site`, `GET /v1/parse-site/{inn}` | Парсинг главных страниц доменов, сохранение текста и векторов. |
| `POST /v1/analyze-json`, `GET /v1/analyze-json/{inn}` | Отправка снапшота сайта во внешний AI‑анализатор и сохранение результатов. |
| `POST /v1/ib-match`, `POST/GET /v1/ib-match/by-inn` | Матчинг компании с внутренними классами/оборудованием по embeddings. |
| `GET /v1/equipment-selection`, `GET /v1/equipment-selection/by-inn/{inn}` | Финальный расчёт оборудования по свежим данным компании. |
| `GET /v1/analyze-service/health` | Проверка доступности внешнего сервиса анализа. |
| `GET /api/ai-analysis/companies` | Список компаний с текущим статусом AI‑обработки для интерфейса. |
| `POST /v1/pipeline/full` | Запуск полного процесса по одному ИНН. |
| `POST /v1/pipeline/auto` | Пакетный автозапуск для ИНН без завершённого расчёта оборудования. |

---

## 6) Что происходит в полном pipeline

`POST /v1/pipeline/full` делает шаги строго по порядку:

1. `lookup_card`: получает/обновляет компанию, валидирует ИНН, синхронизирует базовые записи.
2. `parse_site`: собирает кандидаты доменов, парсит сайты, режет текст на chunks, строит embeddings, пишет результат в БД.
3. `analyze_json`: передаёт актуальный текст и каталоги во внешний AI сервис, сохраняет `db_payload` в `ai_site_*`.
4. `ib_match`: сравнивает вектор сайта со справочниками `ib_prodclass` и сохраняет соответствия.
5. `equipment_selection`: выбирает стратегию (данные сайта или fallback по ОКВЭД), формирует итоговый набор оборудования.

Если фатально падает первый шаг (`lookup_card`) — pipeline останавливается. Ошибки остальных шагов аккумулируются в `errors`, но ответ всё равно возвращается.

---

## 7) Переменные окружения (минимум для запуска)

Обязательные для полноценной работы:
- `POSTGRES_DATABASE_URL`
- `BITRIX_DATABASE_URL`
- `PARSING_DATABASE_URL`
- `DADATA_API_KEY`, `DADATA_SECRET_KEY`
- `SCRAPERAPI_KEY`
- `AI_ANALYZE_BASE` (или `ANALYZE_BASE`)

Опциональные:
- `PP719_DATABASE_URL`
- `B24_BASE_URL`, `B24_SYNC_ENABLED`, `B24_SYNC_INTERVAL`
- `CORS_ALLOW_*`
- `LOG_LEVEL`, `ECHO_SQL`

Смотрите полный список в `.env.example`.

---

## 8) Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

Swagger UI: `http://127.0.0.1:8000/docs`

---

## 9) Где читать подробности

- Полная техническая карта исполнения программы: `docs/program_runtime_explained.md`.
- Детализация отдельных подсистем в `docs/`:
  - `pipeline_overview.md`
  - `parse_site_pipeline.md`
  - `analyze_json_flow.md`
  - `prodclass_matching.md`
  - `industry_detection.md`
  - `ai_analysis_timer_sync_contract.md`

