"""Handlers for periodic sending statistics in the aiogram bot."""  # Документируем назначение модуля

from __future__ import annotations  # Включаем будущее поведение аннотаций для Python 3.10

import logging  # Импортируем logging для диагностики ошибок
from aiogram import Router, types  # Импортируем Router и типы сообщений из aiogram
from aiogram.filters import Command  # Подключаем фильтр для обработки команд

from emailbot.reporting import summarize_period_stats  # Импортируем расчёт статистики отправок
from emailbot.ui.messages import format_period_report  # Импортируем форматирование отчёта по периодам

logger = logging.getLogger(__name__)  # Создаём логгер модуля для отладки

router = Router()  # Инициализируем роутер aiogram


async def _send_period_report(message: types.Message, period: str) -> None:
    """Отправить пользователю текстовый отчёт за указанный период."""  # Объясняем назначение функции

    try:
        stats = summarize_period_stats(period)  # Получаем агрегированную статистику по AUDIT-логам
    except Exception as exc:  # Обрабатываем потенциальные ошибки доступа к данным
        logger.exception("summarize_period_stats failed: %s", exc)  # Фиксируем исключение в журнале
        await message.answer("Не удалось сформировать отчёт 😔")  # Сообщаем пользователю о неудаче
        return  # Завершаем выполнение функции

    report_text = format_period_report(stats)  # Формируем человеко-читаемый отчёт
    await message.answer(report_text)  # Отправляем готовый отчёт пользователю


@router.message(Command("stat_day"))
async def handle_stat_day(message: types.Message) -> None:
    """Ответить на команду /stat_day отчётом за текущий день."""  # Документируем обработчик

    await _send_period_report(message, "day")  # Генерируем и отправляем отчёт за день


@router.message(Command("stat_week"))
async def handle_stat_week(message: types.Message) -> None:
    """Ответить на команду /stat_week отчётом за последние 7 дней."""  # Документируем обработчик

    await _send_period_report(message, "week")  # Генерируем и отправляем отчёт за неделю


@router.message(Command("stat_month"))
async def handle_stat_month(message: types.Message) -> None:
    """Ответить на команду /stat_month отчётом за последние 30 дней."""  # Документируем обработчик

    await _send_period_report(message, "month")  # Генерируем и отправляем отчёт за месяц


@router.message(Command("stat_year"))
async def handle_stat_year(message: types.Message) -> None:
    """Ответить на команду /stat_year отчётом за последние 365 дней."""  # Документируем обработчик

    await _send_period_report(message, "year")  # Генерируем и отправляем отчёт за год


@router.message(lambda message: (message.text or "").strip() == "📈 Отчёты")
async def handle_reports_menu(message: types.Message) -> None:
    """Показать отчёт за день по нажатию кнопки меню «📈 Отчёты»"""  # Описываем обработчик меню

    await _send_period_report(message, "day")  # Генерируем и отправляем отчёт за день
