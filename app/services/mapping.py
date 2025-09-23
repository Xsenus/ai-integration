# app/services/mapping.py
from __future__ import annotations
from typing import Optional


def _fio_to_parts(full: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Возвращает: (full, surname, name, patronymic)."""
    if not full:
        return None, None, None, None
    parts = [p for p in full.replace("  ", " ").strip().split(" ") if p]
    sur = name = pat = None
    if len(parts) == 1:
        sur = parts[0]
    elif len(parts) == 2:
        sur, name = parts
    else:
        sur, name, pat = parts[0], parts[1], " ".join(parts[2:])
    return full, sur, name, pat if pat else None


def extract_inn(payload: dict) -> Optional[str]:
    """Извлекает ИНН из payload."""
    if isinstance(payload, dict):
        d = payload.get("data")
        if isinstance(d, dict):
            inn = d.get("inn")
            if inn:
                return str(inn)
        if payload.get("inn"):
            return str(payload["inn"])
    return None


def _normalize_okveds(d: dict) -> list:
    """
    Нормализуем okveds к списку длиной до 7 элементов.
    Поддерживаем варианты:
    - d["okveds"] = {"main":"...", "items":[...]}
    - d["okveds"] = [{"code": "...", "name": "..."}, ...]
    - d["okveds"] = {"62.01": "Разработка ПО", "62.02": "Консультации", ...} (мапа код->название)
    - d["okved"] = "62.01" (тогда оставим main отдельно, а okveds может быть пустым)
    """
    okveds_value = d.get("okveds")

    # Случай 1: словарь с ключом items
    if isinstance(okveds_value, dict) and isinstance(okveds_value.get("items"), list):
        okveds_list = okveds_value.get("items") or []

    # Случай 2: список уже нормализованных записей
    elif isinstance(okveds_value, list):
        okveds_list = okveds_value

    # Случай 3: мапа код->название (нужно сконвертировать в список словарей)
    elif isinstance(okveds_value, dict):
        okveds_list = []
        for k, v in okveds_value.items():
            # пропустим служебные ключи типа "main"
            if k == "main":
                continue
            okveds_list.append({"code": k, "name": v})

    else:
        okveds_list = []

    # Обрезаем до 7
    if isinstance(okveds_list, list):
        okveds_list = okveds_list[:7]
    return okveds_list


def map_summary_from_dadata(payload: dict) -> dict:
    """Нормализует необходимые поля под CompanySummary."""
    d = (payload or {}).get("data") or {}
    name = d.get("name") or {}
    mgmt = d.get("management") or {}
    addr = d.get("address") or {}
    ad = (addr or {}).get("data") or {}

    # основной ОКВЭД — читаем явно и прозрачно
    main_okved = d.get("okved")
    if main_okved is None and isinstance(d.get("okveds"), dict):
        main_okved = (d.get("okveds") or {}).get("main")

    # до 7 ОКВЭДов (разные форматы)
    okveds = _normalize_okveds(d)

    # телефоны/почты
    phones = []
    for p in (d.get("phones") or []):
        if isinstance(p, dict) and p.get("value"):
            phones.append(p["value"])
        elif isinstance(p, str) and p.strip():
            phones.append(p.strip())

    emails = []
    for e in (d.get("emails") or []):
        if isinstance(e, dict) and e.get("value"):
            emails.append(e["value"].strip())
        elif isinstance(e, str) and e.strip():
            emails.append(e.strip())

    full, sur, nm, pat = _fio_to_parts(mgmt.get("name"))

    # finance
    finance = d.get("finance") or {}
    employee_count = finance.get("employee") or finance.get("employees") or d.get("employee_count")
    year = finance.get("year")
    income = finance.get("income")
    revenue = finance.get("revenue")

    res = {
        "inn": d.get("inn"),
        "short_name": name.get("short"),
        "short_name_opf": name.get("short_with_opf"),
        "management_full_name": full,
        "management_surname_n_p": None,
        "management_surname": sur,
        "management_name": nm,
        "management_patronymic": pat,
        "management_post": mgmt.get("post"),
        "branch_count": d.get("branch_count"),
        "address": addr.get("value"),
        "geo_lat": float(ad["geo_lat"]) if ad.get("geo_lat") else None,
        "geo_lon": float(ad["geo_lon"]) if ad.get("geo_lon") else None,
        "status": (d.get("state") or {}).get("status"),
        "employee_count": int(employee_count)
            if isinstance(employee_count, (int, float, str)) and str(employee_count).isdigit()
            else None,
        "main_okved": main_okved,
        "okveds": okveds,
        "year": int(year) if isinstance(year, (int, float, str)) and str(year).isdigit() else None,
        "income": float(income) if isinstance(income, (int, float)) else None,
        "revenue": float(revenue) if isinstance(revenue, (int, float)) else None,
        "smb_type": (d.get("smb") or {}).get("type"),
        "smb_category": (d.get("smb") or {}).get("category"),
        "smb_issue_date": (d.get("smb") or {}).get("issue_date"),
        "phones": phones or None,
        "emails": emails or None,
    }

    # "Фамилия И.О."
    if res["management_surname"]:
        i = (res["management_name"][0] + ".") if res["management_name"] else ""
        o = (res["management_patronymic"][0] + ".") if res["management_patronymic"] else ""
        res["management_surname_n_p"] = f'{res["management_surname"]} {i}{o}'.strip()

    return res
