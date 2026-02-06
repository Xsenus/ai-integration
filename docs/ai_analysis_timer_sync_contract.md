# AI analysis timer/progress sync contract (UI + API)

Этот документ фиксирует проверяемый контракт между UI и endpoint `GET /api/ai-analysis/companies` для корректного отображения таймера и прогресс-бара при запуске, деградации API и автовосстановлении.

## UX-инварианты (обязательно)

1. После клика «ПУСК» в UI немедленно отображаются:
   - активный статус (`queued` или `running`),
   - прогресс-бар,
   - таймер (не `—`, старт от `00:00:00`).
2. При временной недоступности API таблица не очищается; сохраняется last-known-good snapshot и локальный тик таймера.
3. После восстановления API UI автоматически пересинхронизируется по серверным значениям без перезагрузки страницы.
4. В рамках одного запуска длительность монотонна (без визуального отката назад).

## Контракт endpoint `GET /api/ai-analysis/companies`

Для каждой компании endpoint должен возвращать:

- `analysis_status`: `queued | running | completed | failed | stopped`
- `queued_at`: ISO datetime (обязателен для `queued`, желателен всегда)
- `analysis_started_at`: ISO datetime (обязателен для `running/completed/failed/stopped`)
- `analysis_duration_ms`: integer >= 0
  - для `queued/running` — текущая накопленная длительность,
  - для terminal-статусов — финальная фиксированная длительность,
  - не убывает в пределах одного run.
- `analysis_finished_at`: ISO datetime для terminal-статусов (`completed/failed/stopped`)
- `analysis_progress`: float в диапазоне `[0.0, 1.0]` (рекомендуется)

## Рекомендации для backend реализации

1. `analysis_duration_ms` рассчитывать на backend из монотонного/стабильного источника времени и публиковать на каждый poll.
2. При новом run:
   - сбрасывать `analysis_duration_ms` в `0`,
   - выставлять новый `analysis_started_at` (и при необходимости новый `queued_at`).
3. Никогда не отдавать уменьшенное `analysis_duration_ms` в пределах одного и того же run-id.
4. Для terminal-статуса фиксировать `analysis_finished_at` и финальный `analysis_duration_ms`.

## Рекомендации для UI реализации

### Единый расчёт длительности

В таблице и модальном окне длительность обязана считаться одинаково:

```text
duration = formatDuration(getSyncedDurationMs(company, isActive, nowMs, syncPoint))
```

Где:
- `getActiveElapsedMs(company, nowMs)` — fallback от `analysis_started_at`/`queued_at`.
- `DurationSyncPoint = { baseDurationMs, syncedAtMs }`.
- `getSyncedDurationMs(...)` — объединяет локальный тик и серверную «опору».

### Локальный старт

При `markQueued`/`markRunning`:
- немедленно помечать компанию как active,
- создавать `syncPoint` с `baseDurationMs=0` и `syncedAtMs=Date.now()`,
- обеспечивать немедленный рендер таймера с `00:00:00`.

При terminal (`completed/failed/stopped`):
- удалять active-state,
- очищать sync-point (чтобы не было «залипания» таймера).

### Успешный poll

На каждом успешном fetch:
- для active компаний обновлять sync-point из `analysis_duration_ms`,
- если `analysis_duration_ms` отсутствует/невалиден — fallback к `analysis_started_at`/`queued_at`,
- сохранять монотонность (не применять значение меньше уже показанного в текущем run).

### Деградационный режим (outage)

Если fetch упал:
- не очищать список компаний,
- запомнить момент ошибки (`lastFetchErrorAt`),
- показать единичный toast: «API временно недоступен, показываем последние данные»,
- запускать retry polling каждые `10s` до первого успешного ответа.

После первого успешного ответа:
- сбрасывать error/outage-флаги,
- возвращаться к normal sync с серверной опорой.

## Чеклист приёмки

- [ ] Таймер и прогресс появляются мгновенно после «ПУСК».
- [ ] При API outage таблица не очищается; таймер продолжает тикать.
- [ ] После восстановления API синхронизация восстанавливается без reload.
- [ ] В пределах одного run отсутствуют скачки времени назад.
- [ ] Логика duration едина в таблице и модалке.
- [ ] Контракт API задокументирован и проверяем.


## Статус реализации в текущем backend

В этом репозитории реализован endpoint `GET /api/ai-analysis/companies` (см. `app/api/ai_analysis.py`).

Текущая серверная агрегация строится на данных `public.clients_requests`:
- `queued_at` ← `created_at`,
- `analysis_started_at` ← `started_at`,
- `analysis_finished_at` ← `ended_at`,
- `analysis_duration_ms` ← `max(sec_duration*1000, timeline_fallback)`,
- `analysis_progress` ← доля выставленных `step_1..step_12` (для `completed` = `1.0`).

Это обеспечивает стабильную серверную опору для UI-поллинга и деградационного режима на клиенте.
