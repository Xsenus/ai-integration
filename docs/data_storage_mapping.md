# Куда сохраняются ответы анализа и как их искать

Ниже описано, какие таблицы заполняются после вызова `/v1/analyze-json`, какие новые поля в них появляются и как найти сохранённые данные по ИНН и домену.

## Таблицы и ключевые колонки

### `public.pars_site`
* Обновляется при применении `db_payload`: описание сайта (`description`) и вектор (`text_vector`).
* Привязка к компании: `company_id` указывает на запись в `public.clients_requests`, `id` используется как `text_pars_id` в связанных таблицах.

### `public.ai_site_prodclass`
* Хранит основной продкласс и метрики качества.
* Колонки (создаются автоматически при первом сохранении, миграции не требуются):
  * `prodclass`, `prodclass_score` — итоговый класс и его уверенность.
  * `description_score`, `okved_score`, `description_okved_score` — метрики совпадения текста с названием и ОКВЭД.
  * `prodclass_by_okved` — класс по ОКВЭД, используется как фолбэк, если сайт недоступен или внешний ответ без `prodclass`.
  * `text_pars_id` (внешний ключ на `pars_site.id`), опционально `created_at`.
* При отсутствии `prodclass` в ответе сервис записывает `prodclass_by_okved` и использует `okved_score`/`description_okved_score` как оценку.

### `public.ai_site_openai_responses`
* Лог ответов внешнего анализа по каждому домену/компании (всегда создаём новую строку — история не затирается).
* Связки: `text_pars_id` → `pars_site.id`, `company_id` → `clients_requests.id`.
* Содержит сырой текст описания (`description`), метрики (`description_score`, `okved_score`, `prodclass_by_okved`, `prodclass`, `prodclass_score`), списки (`equipment_site`, `goods`, `goods_type`) и штамп времени (`created_at`).
* Создаётся автоматически вместе с остальными таблицами `parsing_data`.

### `public.ai_site_goods_types`
* Список товаров/услуг: `goods_type` (название), `goods_type_id`/`match_id`, `goods_types_score`, `text_vector`.
* Каждая строка связана с конкретным парсом через `text_par_id` (`pars_site.id`).

### `public.ai_site_equipment`
* Оборудование с сайта: `equipment` (название), `equipment_id`/`match_id`, `equipment_score`, `text_vector`.
* Также привязано к `text_par_id` (`pars_site.id`).

## Как найти данные по ИНН
1. Получить список доменов и `company_id` по ИНН:
   ```sql
   SELECT id AS company_id, domain_1, domain_2
   FROM public.clients_requests
   WHERE inn = :inn
   ORDER BY id DESC;
   ```
2. Для выбранного `company_id` найти последний парс сайта и его `pars_site.id` (равен `text_pars_id`):
   ```sql
   SELECT id AS pars_id, created_at, domain_1, domain_2, url
   FROM public.pars_site
   WHERE company_id = :company_id
   ORDER BY created_at DESC NULLS LAST, id DESC
   LIMIT 1;
   ```
3. По `pars_id` выбрать результаты анализа:
   ```sql
   SELECT prodclass, prodclass_score, description_score, okved_score, description_okved_score, prodclass_by_okved
   FROM public.ai_site_prodclass
   WHERE text_pars_id = :pars_id
   ORDER BY id DESC
   LIMIT 1;

   SELECT goods_type, goods_type_id, goods_types_score, text_vector
   FROM public.ai_site_goods_types
   WHERE text_par_id = :pars_id
   ORDER BY id;

   SELECT equipment, equipment_id, equipment_score, text_vector
   FROM public.ai_site_equipment
   WHERE text_par_id = :pars_id
   ORDER BY id;
   ```

4. Посмотреть историю ответов внешнего анализа (каждый вызов — отдельная строка):
   ```sql
   SELECT created_at, domain, url, description, description_score, okved_score, prodclass_by_okved,
          prodclass, prodclass_score, equipment_site, goods, goods_type
   FROM public.ai_site_openai_responses
   WHERE text_pars_id = :pars_id
   ORDER BY created_at DESC, id DESC;
   ```

## Поиск по конкретному домену
Если нужно уточнить по сайту, используйте тот же порядок, но во втором запросе добавьте фильтр по домену (при наличии колонки `domain_1`/`domain_2`):
```sql
SELECT id AS pars_id, created_at, domain_1, domain_2, url
FROM public.pars_site
WHERE company_id = :company_id
  AND (domain_1 = :domain OR domain_2 = :domain)
ORDER BY created_at DESC NULLS LAST, id DESC
LIMIT 1;
```

Чтобы просмотреть историю по домену без привязки к конкретной версии парса, можно выбрать все записи сразу:
```sql
SELECT created_at, domain, url, description, description_score, okved_score, prodclass_by_okved,
       prodclass, prodclass_score, equipment_site, goods, goods_type
FROM public.ai_site_openai_responses
WHERE domain = :domain
ORDER BY created_at DESC, id DESC;
```

## Когда срабатывает фолбэк по ОКВЭД
* Если внешний сервис вернул только `prodclass_by_okved`, сервис всё равно запишет строку в `ai_site_prodclass` с этим значением и выставит `prodclass_score` из `okved_score` или `description_okved_score`.
* При последующих вызовах `/v1/lookup/{inn}/ai-analyzer` эта запись вернётся как основной продкласс, чтобы ответ был непустым даже без текста сайта.
