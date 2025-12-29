# Карточка AI-анализатора: откуда берётся каждый параметр

Документ описывает, из каких таблиц и полей формируются атрибуты ответа
`GET /v1/lookup/{inn}/ai-analyzer` (который отображается на скриншоте),
и какие fallback-ы применяются. Все ссылки на код приведены на основе
`app/services/ai_analyzer.py` и `app/api/lookup.py`.

## Основные шаги пайплайна
1. Загружаем последнюю строку из `public.clients_requests` по ИНН
   (домены, описания сайтов, UTP, письмо, ОКВЭД).【F:app/services/ai_analyzer.py†L792-L863】
2. Если нашли `company_id`, подтягиваем связанные сайты из
   `public.pars_site` и результаты AI из `public.ai_site_goods_types`,
   `public.ai_site_prodclass`, `public.ai_site_equipment`; параллельно
   подгружаем справочники для названий групп/классов.【F:app/services/ai_analyzer.py†L835-L903】
3. Собираем список URL и описаний сайтов (в том числе фолбеком из
   `clients_requests`).【F:app/services/ai_analyzer.py†L904-L955】
4. Формируем продукцию, оборудование и лучший продкласс с учётом скорингов
   и справочников.【F:app/services/ai_analyzer.py†L956-L1032】
5. Определяем индустрию: сначала по продклассу, затем по `okved_main`
   из `clients_requests` или `dadata_result` (Bitrix → Postgres).【F:app/services/ai_analyzer.py†L978-L1037】
6. Склеиваем итоговый payload (`domain1/2`, `sites`, `products`,
   `equipment`, `prodclass`, `industry`, `utp`, `letter`, `note`).【F:app/services/ai_analyzer.py†L1040-L1053】
7. Роутер `/v1/lookup/{inn}/ai-analyzer` просто валидирует этот payload в
   схемы `AiAnalyzerResponse` без дополнительных вычислений или запросов.
   【F:app/api/lookup.py†L217-L264】【F:app/schemas/ai_analyzer.py†L9-L67】

## Карточка: поле → источник
* **company.domain1 / company.domain2** — описания сайтов. Берутся из
  `pars_site.description` (два последних уникальных описания), при
  нехватке — из `clients_requests.site_1_description/ site_2_description`.
  【F:app/services/ai_analyzer.py†L904-L955】

* **ai.sites[]** — URL сайтов (https, без хвостов). Сначала домены из
  `clients_requests.domain_1/domain_2`, затем `pars_site.url/domain_1`,
  с дедупликацией порядка вставки. Пустой список, если таблицы пусты.
  【F:app/services/ai_analyzer.py†L904-L955】

* **ai.products[]** — каждая запись строится из связки:
  `ai_site_prodclass` (основное название продукции) +
  `ai_site_goods_types` (группа/код продукции по тому же `text_pars_id`).
  
  - `name`: название продкласса из строки `ai_site_prodclass` или из
    справочника `ib_prodclass`/`prodclass` и т.п.; если названия нет —
    заглушка `Prodclass {id}`.
  - `goods_group`: группа продукции, вычисляемая по таблице справочника
    (колонки `goods_type_name`, `name`, `group_name`, `code` и др.) или
    тексту `goods_type` в строке `ai_site_goods_types`.
  - `domain` / `url`: берутся из `pars_site` связанной с обеими таблицами.
  
  Если нет строк в `ai_site_prodclass`, список пуст; если нет
  `ai_site_goods_types`, группа может быть пустой, но товары всё равно
  появятся по продклассам.【F:app/services/ai_analyzer.py†L835-L975】【F:app/schemas/ai_analyzer.py†L21-L35】

* **ai.equipment[]** — формируется из `ai_site_equipment` + `pars_site`.
  
  - `name`: название оборудования из строки или справочника
    `equipment/ib_equipment` и т.п.
  - `equip_group`: группа/класс оборудования по справочнику (колонки
    `group_name/section_name/...`), с фолбеком на текст `equipment`.
  - `domain` / `url`: из связанной строки `pars_site`.
  
  Список пуст, если для `company_id` нет записей в `ai_site_equipment`.
  【F:app/services/ai_analyzer.py†L835-L905】【F:app/services/ai_analyzer.py†L956-L1032】【F:app/schemas/ai_analyzer.py†L37-L53】

* **ai.prodclass** — “лучший” продкласс по наибольшему скору из
  `ai_site_prodclass` с фолбеком названия через `ib_prodclass` (если в
  строке нет текстового поля). Поля: `id`, `name`, `label` (`[id] name`),
  `score`. Пусто, если продклассов нет.
  【F:app/services/ai_analyzer.py†L956-L975】【F:app/services/ai_analyzer.py†L1040-L1052】

* **ai.industry** — выводится из индустрии продкласса (если в справочнике
  есть `industry_*` поля); иначе берётся `okved_main` из
  `clients_requests`, затем fallback из `dadata_result` Bitrix или
  Postgres и переводится в человекочитаемую отрасль. Пусто, если нет ни
  продкласса, ни ОКВЭД.
  【F:app/services/ai_analyzer.py†L1102-L1131】【F:app/schemas/ai_analyzer.py†L76-L95】

* **ai.utp** — напрямую `clients_requests.utp`. Если поле пустое в
  таблице, в карточке будет `null/—`.
  【F:app/services/ai_analyzer.py†L978-L1051】

* **ai.letter** — напрямую `clients_requests.pismo`. Нет других
  источников, поэтому пустое поле означает отсутствие данных в CR.
  【F:app/services/ai_analyzer.py†L978-L1052】

* **note** — строка диагностики, показывающая, какие таблицы реально
  участвовали (`clients_requests`, `pars_site`, `ai_site_*`,
  `dadata_result(Bitrix/PG)`). Если ничего не найдено, значение
  `"no sources found"`.
  【F:app/services/ai_analyzer.py†L1133-L1150】

## Что НЕ вычисляется
* AI-роутер не запускает анализ заново и не обращается к внешним
  сервисам: он только читает уже сохранённые строки. Если таблицы пусты,
  поле остаётся пустым — перерасчёта нет.【F:app/services/ai_analyzer.py†L792-L807】
* Нет автозаполнения UTP/письма/доменов из других источников, кроме
  перечисленных; отсутствие в `clients_requests` приводит к пустым полям.
* Нет вычисления “оборудование/продукция” без записей в `ai_site_*`.

## Параметры из формы заказчика (что заполняем, а что нет)
Ниже перечислены поля с вашего скриншота и их соответствие нашему ответу
`GET /v1/lookup/{inn}/ai-analyzer`. Если поле не вычисляется — указано
отдельно, чтобы было понятно, нужен ли дополнительный код.

1. **«Уровень соответствия и найденный класс предприятия».**
   * Это `ai.prodclass` из ответа: берём лучшую запись `ai_site_prodclass`
     по максимальному `prodclass_score` и выводим её `id`, `name`,
     `label=[id] name`, `score` (тот самый “уровень соответствия”).
     Данные появляются только если в БД есть строки `ai_site_prodclass`
     для `company_id`; анализатор сам ничего не досчитывает.
     【F:app/services/ai_analyzer.py†L935-L1131】【F:app/schemas/ai_analyzer.py†L17-L45】
   * Дополнительно в `ai.prodclass` возвращаются `description_score`
     (сходство описания с названием компании), `okved_score`
     (сходство описания с ОКВЭД) и `prodclass_by_okved` — он заполняется,
     если внешний сервис смог классифицировать только по ОКВЭД при
     недоступном сайте. Все значения берутся напрямую из
     `ai_site_prodclass`, поэтому старые ответы остаются совместимыми.

2. **«Домен для парсинга».**
   * В ответе это `ai.sites[]` и вспомогательные `domain1_site/domain2_site`.
     Список строится из `pars_site.url/domain_1` по `company_id`, дальше
     — из `clients_requests.domain_1/2` с дедупликацией. Если таблицы
     пустые, блок остаётся пустым. Никаких вычислений/валидации кроме
     нормализации URL нет.【F:app/services/ai_analyzer.py†L995-L1040】

3. **«Соответствие ИИ-описания сайта и ОКВЭД».**
   * Теперь считаем при разборе `analyze-json`: берём описание сайта
     (payload → `pars_site.description`) и `okved_main` из
     `clients_requests`, считаем SequenceMatcher.ratio и кладём в колонку
     `ai_site_prodclass.description_okved_score`, если она есть. В ответе
     значение возвращается как `ai.prodclass.description_okved_score`; если
     колонки нет, скор вычисляется на лету и подставляется только в ответ.
     【F:app/api/analyze_json.py†L1316-L1520】【F:app/services/ai_analyzer.py†L1102-L1131】【F:app/schemas/ai_analyzer.py†L17-L45】

4. **«ИИ-описание сайта».**
  * Это `company.domain1/domain2` в ответе: берём два последних
    `pars_site.description` по `company_id`, при нехватке —
    `clients_requests.site_1/2_description`. Никакой генерации текста в
    момент запроса нет; если описаний нет в таблицах, поле пустое.
    【F:app/services/ai_analyzer.py†L995-L1040】

5. **«Топ-10 оборудования».**
  * Мы возвращаем `ai.equipment[]`, собранный из `ai_site_equipment`
    (сортировка по `equipment_score`, дедупликация по названию+URL).
    Лимит по коду — 100 записей; отдельного “топ-10” не режем. Если нужны
    строго 10 позиций, это надо обрезать на фронте или добавить фильтр в
    сервисе. Без строк в `ai_site_equipment` блок будет пустым.
    【F:app/services/ai_analyzer.py†L835-L905】【F:app/services/ai_analyzer.py†L935-L1131】

6. **«Виды найденной продукции на сайте и ТНВЭД».**
   * Это `ai.products[]`: собираем связки из `ai_site_prodclass` и
     `ai_site_goods_types` по одному `text_pars_id`, берем название
     продукции/группы, домен/URL. Лимит — 100 элементов. Теперь в ответ
     добавлен `tnved_code` (берём `goods_type_id` или код из справочника),
     поэтому UI может вывести код ТНВЭД/ЕАЭС без доп. запросов. При
     отсутствии строк в `ai_site_prodclass/ai_site_goods_types` список
     пустой.
     【F:app/services/ai_analyzer.py†L617-L709】【F:app/schemas/ai_analyzer.py†L29-L38】

Таким образом, все поля на скриншоте уже читаются из БД, а дополнительно
доработаны два пункта: (1) скор сходства описания и ОКВЭД теперь считается
и может сохраняться в `ai_site_prodclass`, (2) код ТНВЭД возвращается в
`ai.products[].tnved_code`.

Этот список соответствует всем блокам, которые видны на скриншоте, и
фиксирует, для каких полей требуется наличие данных в конкретных
таблицах/колонках БД.
