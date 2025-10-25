from __future__ import annotations

from datetime import datetime
from typing import Sequence

from config import LOCAL_TLDS, LOCAL_DOMAINS_EXTRA
from utils.geo_domains import split_foreign, is_foreign_email


def export_emails_xlsx(path: str, emails: Sequence[str]) -> str:
    """Создаёт xlsx: Все адреса + Локальные + Иностранные + Сводка."""

    import xlsxwriter

    wb = xlsxwriter.Workbook(path)
    all_ws = wb.add_worksheet("Все адреса")
    loc_ws = wb.add_worksheet("Локальные")
    for_ws = wb.add_worksheet("Иностранные")
    sum_ws = wb.add_worksheet("Сводка")

    header = wb.add_format({"bold": True})
    dt = datetime.now().strftime("%Y-%m-%d %H:%M")

    locals_, foreigns = split_foreign(emails)

    # Все адреса + пометка иностранности
    all_ws.write(0, 0, "Email", header)
    all_ws.write(0, 1, "Иностр.", header)
    for row, email in enumerate(emails, start=1):
        all_ws.write(row, 0, email)
        all_ws.write(row, 1, "Да" if is_foreign_email(email) else "Нет")

    # Локальные
    loc_ws.write(0, 0, "Email", header)
    for row, email in enumerate(locals_, start=1):
        loc_ws.write(row, 0, email)

    # Иностранные
    for_ws.write(0, 0, "Email", header)
    for row, email in enumerate(foreigns, start=1):
        for_ws.write(row, 0, email)

    # Сводка/метаданные
    sum_ws.write(0, 0, "Дата", header)
    sum_ws.write(0, 1, dt)
    sum_ws.write(1, 0, "Всего адресов", header)
    sum_ws.write(1, 1, len(emails))
    sum_ws.write(2, 0, "Локальные", header)
    sum_ws.write(2, 1, len(locals_))
    sum_ws.write(3, 0, "Иностранные", header)
    sum_ws.write(3, 1, len(foreigns))
    sum_ws.write(5, 0, "Правило локальности", header)
    sum_ws.write(5, 1, ", ".join(LOCAL_TLDS))
    sum_ws.write(6, 0, "Allow-list доменов", header)
    sum_ws.write(6, 1, ", ".join(sorted(LOCAL_DOMAINS_EXTRA)))

    wb.close()
    return path


__all__ = ["export_emails_xlsx"]
