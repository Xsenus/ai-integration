from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class PromptSiteAvailableRequest(BaseModel):
    """Запрос на генерацию промпта для компании с доступным сайтом."""

    text_par: str = Field(..., description="Текст, собранный с сайта компании")
    company_name: str = Field(..., description="Название компании")
    okved: str = Field(..., description="Код ОКВЭД компании")
    chat_model: Optional[str] = Field(
        None,
        description="Переопределение модели OpenAI для генерации ответа",
    )
    embed_model: Optional[str] = Field(
        None,
        description="Переопределение модели эмбеддингов",
    )


class PromptSiteUnavailableRequest(BaseModel):
    """Запрос на генерацию промпта для компании без доступного сайта."""

    okved: str = Field(..., description="Код ОКВЭД компании")
    chat_model: Optional[str] = Field(
        None,
        description="Переопределение модели OpenAI",
    )


class PromptGenerationEvent(BaseModel):
    """Этап обработки при генерации промпта."""

    step: str = Field(..., description="Идентификатор этапа")
    status: str = Field(..., description="Статус выполнения этапа")
    detail: Optional[str] = Field(None, description="Дополнительная информация")
    duration_ms: Optional[int] = Field(None, description="Длительность этапа в мс")


class PromptGenerationTimings(BaseModel):
    """Сводные длительности этапов генерации промпта."""

    build_prompt_ms: Optional[int] = Field(None, description="Длительность подготовки промпта")
    openai_ms: Optional[int] = Field(None, description="Длительность вызова OpenAI")
    parse_ms: Optional[int] = Field(None, description="Длительность постобработки ответа")


class PromptGenerationResponse(BaseModel):
    """Ответ эндпоинтов генерации промптов."""

    model_config = ConfigDict(extra="allow")

    success: bool = Field(..., description="Признак успешного завершения операции")
    prompt: Optional[str] = Field(None, description="Построенный промпт")
    prompt_len: Optional[int] = Field(None, description="Длина промпта в символах")
    answer: Optional[str] = Field(None, description="Ответ модели OpenAI")
    answer_len: Optional[int] = Field(None, description="Длина ответа в символах")
    parsed: Optional[dict[str, Any]] = Field(None, description="Результат постобработки ответа")
    started_at: Optional[datetime] = Field(None, description="Время начала обработки")
    finished_at: Optional[datetime] = Field(None, description="Время окончания обработки")
    duration_ms: Optional[int] = Field(None, description="Общая длительность обработки")
    events: list[PromptGenerationEvent] = Field(
        default_factory=list,
        description="Хронологический список этапов обработки",
    )
    timings: Optional[PromptGenerationTimings] = Field(
        None,
        description="Сводные длительности этапов",
    )
    chat_model: Optional[str] = Field(None, description="Использованная модель OpenAI")
    embed_model: Optional[str] = Field(None, description="Использованная модель эмбеддингов")
    error: Optional[str] = Field(None, description="Сообщение об ошибке")


class AnalyzeFromInnRequest(BaseModel):
    """Запрос на запуск анализа по ИНН."""

    inn: str = Field(..., min_length=5, description="ИНН компании")
    refresh_site: bool = Field(
        False,
        description="Если true — перед анализом принудительно обновить pars_site через /v1/parse-site",
    )
    include_catalogs: bool = Field(
        True,
        description="Передавать ли каталоги товаров и оборудования во внешний сервис",
    )
    chat_model: Optional[str] = Field(
        None,
        description="Явное имя модели LLM для внешнего сервиса (override settings.CHAT_MODEL)",
    )
    embed_model: Optional[str] = Field(
        None,
        description="Явное имя модели эмбеддингов для внешнего сервиса (override settings.embed_model)",
    )
    return_prompt: Optional[bool] = Field(
        None,
        description="Запросить ли у внешнего сервиса возвращение построенного промпта",
    )
    return_answer_raw: Optional[bool] = Field(
        None,
        description="Запросить ли у внешнего сервиса возвращение сырого ответа модели",
    )


class AnalyzeFromInnRun(BaseModel):
    """Детализация обработки конкретного домена."""

    domain: Optional[str] = Field(None, description="Домен, для которого выполнялся анализ")
    domain_source: Optional[str] = Field(
        None,
        description="Источник домена (domain_1/domain_2/auto)",
    )
    pars_id: Optional[int] = Field(None, description="ID строки pars_site")
    company_id: Optional[int] = Field(None, description="ID записи clients_requests")
    created_at: Optional[datetime] = Field(
        None, description="Метка created_at чанков pars_site"
    )
    text_length: int = Field(..., description="Длина текста, отправленного во внешний сервис")
    chunk_count: int = Field(..., description="Количество чанков pars_site, вошедших в текст")
    catalog_goods_size: int = Field(..., description="Размер каталога товаров")
    catalog_equipment_size: int = Field(..., description="Размер каталога оборудования")
    saved_goods: int = Field(..., description="Сколько строк записано в ai_site_goods_types")
    saved_equipment: int = Field(..., description="Сколько строк записано в ai_site_equipment")
    prodclass_id: Optional[int] = Field(None, description="ID сохранённого класса производства")
    prodclass_score: Optional[float] = Field(None, description="Сохранённый скор класса производства")
    external_request: Optional[dict[str, Any]] = Field(
        None,
        description=(
            "Запрос во внешний сервис (каталоги заменены на счётчики для снижения объёма данных)"
        ),
    )
    external_status: int = Field(..., description="HTTP-статус ответа внешнего сервиса")
    external_response: Optional[dict[str, Any]] = Field(
        None,
        description="Полный ответ внешнего сервиса (для отладки)",
    )
    external_response_raw: Optional[Any] = Field(
        None,
        description="Непреобразованный ответ внешнего сервиса (включая исходный текст LLM)",
    )


class AnalyzeFromInnResponse(BaseModel):
    """Результат анализа и сохранения ответа внешнего сервиса."""

    status: str = Field(..., description="Статус обработки (ok/partial/error)")
    inn: str = Field(..., description="ИНН, по которому выполнялся анализ")
    pars_id: Optional[int] = Field(None, description="ID строки pars_site (первый обработанный домен)")
    company_id: Optional[int] = Field(
        None, description="ID записи clients_requests/pars_site (первый обработанный домен)"
    )
    text_length: int = Field(
        ..., description="Длина текста по первому обработанному домену"
    )
    catalog_goods_size: int = Field(
        ..., description="Размер каталога товаров, отправленного во внешний сервис"
    )
    catalog_equipment_size: int = Field(
        ..., description="Размер каталога оборудования, отправленного во внешний сервис"
    )
    saved_goods: int = Field(
        ..., description="Сколько строк записано в ai_site_goods_types (первый домен)"
    )
    saved_equipment: int = Field(
        ..., description="Сколько строк записано в ai_site_equipment (первый домен)"
    )
    prodclass_id: Optional[int] = Field(
        None, description="ID сохранённого класса производства (первый домен)"
    )
    prodclass_score: Optional[float] = Field(
        None, description="Сохранённый скор класса производства (первый домен)"
    )
    external_request: Optional[dict[str, Any]] = Field(
        None,
        description=(
            "Запрос во внешний сервис по первому домену (каталоги заменены на счётчики для снижения объёма данных)"
        ),
    )
    external_status: int = Field(
        ..., description="HTTP-статус ответа внешнего сервиса (первый домен)"
    )
    external_response: Optional[dict[str, Any]] = Field(
        None,
        description="Полный ответ внешнего сервиса по первому домену (для отладки)",
    )
    external_response_raw: Optional[Any] = Field(
        None,
        description="Непреобразованный ответ внешнего сервиса по первому домену",
    )
    total_text_length: int = Field(
        ..., description="Суммарная длина текстов, отправленных по всем доменам"
    )
    total_saved_goods: int = Field(
        ..., description="Суммарно сохранено строк в ai_site_goods_types по всем доменам"
    )
    total_saved_equipment: int = Field(
        ..., description="Суммарно сохранено строк в ai_site_equipment по всем доменам"
    )
    domains_processed: list[str] = Field(
        default_factory=list,
        description="Список обработанных доменов (domain_1/domain_2)",
    )
    runs: list[AnalyzeFromInnRun] = Field(
        default_factory=list,
        description="Детальная информация по каждому обработанному домену",
    )


class AnalyzeFromInnError(BaseModel):
    """Описание ошибки выполнения анализа."""

    status: str = Field("error", description="Фиксированное значение 'error'")
    inn: str = Field(..., description="ИНН, по которому выполнялся анализ")
    detail: str = Field(..., description="Причина ошибки")
    payload: Optional[dict[str, Any]] = Field(
        None,
        description="Дополнительные детали, если доступны",
    )
