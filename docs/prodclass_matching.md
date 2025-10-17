# Prodclass Matching Pipeline

Этот документ фиксирует, как сервисы определяют `prodclass` внутри
репозитория, чтобы снять вопросы о том, откуда берётся итоговый класс.

## Источник данных pars_site

* Во время `parse-site` собранный текст домена отправляется во внешний сервис
  (см. `fetch_site_description`). Ответ содержит описание и вектор. Вектор
  приводится к литералу и сохраняется в `pars_site.text_vector` через
  `pars_site_update_vector`/`pars_site_update_metadata_pg`.
* Таким образом, `pars_site.text_vector` — это embedding, рассчитанный из
  реального текстового контента сайта, а не ID класса из стороннего скрипта.

## Каталог ib_prodclass

* Конфигурация `IB_PRODCLASS_*` (см. `app/config.py`) указывает, что нужно
  читать столбцы `ib_prodclass.id`, `ib_prodclass.prodclass` и
  `ib_prodclass.prodclass_vector`.
* Эти значения загружаются в `assign_ib_matches` и превращаются в объекты
  `CatalogEntry`, пригодные для сравнения векторами.

## Сопоставление

* Функция `_match_rows` проходит по каждой записи `pars_site` и вычисляет
  косинусное сходство между `pars_site.text_vector` и каждым
  `ib_prodclass.prodclass_vector` через общую утилиту
  `services.vector_similarity.cosine_similarity`.
* Находится кандидат с максимальным score. Результат обрезается к диапазону
  `[0, 1]`, округляется и записывается в `ai_site_prodclass`.
* Если у строки `pars_site` нет вектора или векторы каталога пусты, запись
  пропускается с поясняющей пометкой — никаких fallback-скриптов на GPT не
  вызывается.

## Вывод

Итоговый `prodclass` в `ai_site_prodclass` получается строго путём сравнения
`pars_site.text_vector` ↔ `ib_prodclass.prodclass_vector` внутри сервиса
`assign_ib_matches`. Сторонний код, похожий на Jupyter-скрипт с обращением к
GPT, не задействован в этом процессе и может использоваться только вручную
вне данного репозитория.
