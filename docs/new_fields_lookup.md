# Источник новых полей description_score, okved_score и prodclass_by_okved

* **База данных:** `public` схема PostgreSQL.
* **Таблица:** `ai_site_prodclass`.
* **Колонки:** `description_score`, `okved_score`, `prodclass_by_okved` (а также `description_okved_score` для обратной совместимости).
* **Рекомендованный SQL-запрос** для проверки актуальных значений по конкретному парсу:
  ```sql
  SELECT prodclass, prodclass_score, description_score, okved_score, prodclass_by_okved, description_okved_score
  FROM public.ai_site_prodclass
  WHERE text_pars_id = :pars_id
  ORDER BY id DESC
  LIMIT 1;
  ```

Эти поля заполняются при применении `db_payload` в обработчике `analyze-json`; при первом сохранении недостающие колонки добавляются автоматически, поэтому дополнительной миграции не требуется.
