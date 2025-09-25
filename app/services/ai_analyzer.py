# app/services/ai_analyzer.py
from __future__ import annotations

from typing import Optional

# Здесь можно использовать:
# - app.services.dadata_client.find_party_by_inn
# - app.services.scrape.fetch_and_chunk / to_home_url
# - свои SQL-репозитории для чтения domains/parsing_data итд.


async def analyze_company_by_inn(inn: str) -> dict:
    """
    Верните dict, который потом распакуется в _canonical_response(**result).
    Должны присутствовать ключи:
      - inn: str
      - domain1: Optional[str]
      - domain2: Optional[str]
      - industry: Optional[str]
      - sites: list[str]
      - products: list[AiProduct-like dict]
      - equipment: list[AiEquipment-like dict]
      - utp: Optional[str]
      - letter: Optional[str]
      - note: Optional[str]
    """
    # TODO: реализовать матчинг доменов, извлечение продукции/оборудования и т.д.
    return {
        "inn": inn,
        "domain1": None,
        "domain2": None,
        "industry": None,
        "sites": [],
        "products": [],
        "equipment": [],
        "utp": None,
        "letter": None,
        "note": "not implemented",
    }
