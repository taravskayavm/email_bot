"""Callback handlers that render paginated examples for the aiogram bot."""

from __future__ import annotations  # Включаем поддержку будущих аннотаций

from aiogram import F, Router  # Импортируем фильтры и роутер из aiogram
from aiogram.filters import Command  # Подключаем фильтр для обработки текстовых команд
from aiogram.exceptions import TelegramBadRequest  # Обрабатываем ошибки при редактировании сообщений
from aiogram.types import CallbackQuery, Message  # Работаем с колбэками и обычными сообщениями Telegram

from emailbot.bot.keyboards import (  # Используем готовые клавиатуры для примеров
    build_examples_entry_kb,  # Клавиатура с кнопкой входа в примеры
    build_examples_paging_kb,  # Клавиатура для пагинации примеров
)
from emailbot.state.example_cache import (  # Получаем функции и контейнеры для хранения примеров
    ExamplesContext,  # Контейнер с данными примеров и пагинаторами
    ListPager,  # Пагинатор для выдачи примеров частями
    get_context,  # Функция получения контекста из кэша
    put_context,  # Функция сохранения контекста в кэш
)
from emailbot.ui.callbacks import (  # Импортируем константы с идентификаторами колбэков
    CB_EXAMPLES_BACK,
    CB_EXAMPLES_INIT,
    CB_EXAMPLES_MORE_COOLDOWN,
    CB_EXAMPLES_MORE_FOREIGN,
)
from emailbot.ui.messages import format_examples_block  # Форматируем текстовые блоки для отображения

router = Router(name="examples")  # Создаём отдельный роутер для колбэков примеров

__all__ = ["router", "store_examples_context"]  # Экспортируем роутер и функцию сохранения контекста


async def _resolve_chat_id(callback: CallbackQuery) -> int | None:
    """Return chat identifier from callback safely."""

    if callback.message and callback.message.chat:  # Убеждаемся, что сообщение присутствует
        return callback.message.chat.id  # Возвращаем идентификатор чата из сообщения
    if callback.from_user and callback.from_user.id:  # В крайнем случае используем ID пользователя
        return callback.from_user.id  # Возвращаем ID пользователя как fallback
    return None  # Если идентификатор недоступен, возвращаем None


def store_examples_context(chat_id: int, ctx: ExamplesContext) -> None:
    """Предоставляем внешним сценариям функцию сохранения контекста примеров."""

    put_context(chat_id, ctx)  # Сохраняем контекст в общем кэше


@router.message(Command("examples_demo"))
async def examples_demo(message: Message) -> None:
    """Демонстрационная команда, создающая тестовый набор примеров."""

    ctx = ExamplesContext(  # Формируем контекст с тестовыми данными
        source_kind="parse",  # Указываем тип источника как парсинг
        source_id="demo",  # Фиксируем условный идентификатор отчёта
        cooldown_total=3,  # Сообщаем количество адресов с кулдауном
        foreign_total=3,  # Сообщаем количество иностранных адресов
        cooldown_pager=ListPager([  # Формируем список примеров для кулдауна
            ("demo1@example.com", "2024-01-10"),  # Первый адрес с датой последнего контакта
            ("demo2@example.com", "2024-02-15"),  # Второй адрес с датой
            ("demo3@example.com", "2024-03-20"),  # Третий адрес с датой
        ]),
        foreign_pager=ListPager([  # Формируем список иностранных адресов
            "foreign1@example.com",  # Первый пример иностранного адреса
            "foreign2@example.com",  # Второй пример иностранного адреса
            "foreign3@example.com",  # Третий пример иностранного адреса
        ]),
    )
    store_examples_context(message.chat.id, ctx)  # Сохраняем контекст для текущего чата
    await message.answer(  # Отправляем сообщение с кнопкой для просмотра примеров
        "Готова демонстрация примеров. Нажмите кнопку ниже.",  # Сообщаем о готовности демо
        reply_markup=build_examples_entry_kb(),  # Прикрепляем клавиатуру для запуска примеров
    )


@router.callback_query(F.data == CB_EXAMPLES_INIT)
async def on_examples_init(callback: CallbackQuery) -> None:
    """Отрисовываем первый набор примеров для пользователя."""

    chat_id = await _resolve_chat_id(callback)  # Определяем, куда отправлять обновление
    if chat_id is None:  # Проверяем доступность идентификатора чата
        await callback.answer("Не удалось определить чат.", show_alert=True)  # Сообщаем об ошибке
        return  # Прекращаем обработку, если нет ID
    ctx = get_context(chat_id)  # Загружаем контекст примеров из кэша
    if not ctx:  # Проверяем, найден ли контекст
        await callback.answer("Нет контекста примеров. Повторите команду.", show_alert=True)  # Сообщаем о необходимости перезапуска
        return  # Завершаем обработку без обновления сообщения
    message = callback.message  # Получаем исходное сообщение для обновления
    if message is None:  # Проверяем, существует ли сообщение
        await callback.answer("Не удалось обновить сообщение.", show_alert=True)  # Сообщаем о невозможности обновления
        return  # Завершаем обработку, если сообщение отсутствует
    text = format_examples_block(  # Формируем текст блока примеров
        cooldown_total=ctx.cooldown_total,  # Передаём общее количество адресов в кулдауне
        cooldown_triplet=ctx.cooldown_pager.next_chunk(),  # Запрашиваем следующую тройку адресов из кулдауна
        foreign_total=ctx.foreign_total,  # Передаём количество иностранных адресов
        foreign_triplet=ctx.foreign_pager.next_chunk(),  # Запрашиваем следующую тройку иностранных адресов
        foreign_title_suffix="отложены" if ctx.source_kind == "send" else "",  # Добавляем уточнение для режима рассылки
    )
    try:  # Пытаемся заменить текст исходного сообщения
        await message.edit_text(text, reply_markup=build_examples_paging_kb())  # Обновляем текст и клавиатуру
    except TelegramBadRequest:  # Обрабатываем случай, когда редактирование недоступно
        await message.answer(text, reply_markup=build_examples_paging_kb())  # Отправляем новое сообщение с тем же содержимым
    await callback.answer()  # Подтверждаем обработку колбэка


@router.callback_query(F.data == CB_EXAMPLES_MORE_COOLDOWN)
async def on_examples_more_cooldown(callback: CallbackQuery) -> None:
    chat_id = await _resolve_chat_id(callback)  # Определяем чат для обновления
    if chat_id is None:  # Проверяем наличие идентификатора чата
        await callback.answer("Не удалось определить чат.", show_alert=True)  # Сообщаем об ошибке
        return  # Завершаем обработку при отсутствии ID
    ctx = get_context(chat_id)  # Достаём контекст примеров
    if not ctx:  # Убеждаемся, что контекст существует
        await callback.answer("Нет контекста примеров. Повторите команду.", show_alert=True)  # Сообщаем о необходимости перезапуска
        return  # Прекращаем обработку, если контекст не найден
    message = callback.message  # Получаем сообщение, которое нужно обновить
    if message is None:  # Проверяем наличие сообщения
        await callback.answer("Не удалось обновить сообщение.", show_alert=True)  # Уведомляем пользователя
        return  # Завершаем обработку, если сообщение отсутствует
    text = format_examples_block(
        cooldown_total=ctx.cooldown_total,  # Общее количество адресов под ограничением
        cooldown_triplet=ctx.cooldown_pager.next_chunk(),  # Следующая тройка адресов из кулдауна
        foreign_total=ctx.foreign_total,  # Количество иностранных адресов
        foreign_triplet=(ctx.foreign_pager.next_chunk() if ctx.foreign_pager.idx else []),  # Обновляем иностранные адреса только при повторных запросах
        foreign_title_suffix="отложены" if ctx.source_kind == "send" else "",  # Уточняем подпись для отчётов отправки
    )
    await message.edit_text(text, reply_markup=build_examples_paging_kb())  # Обновляем текст и клавиатуру
    await callback.answer("Показаны следующие адреса из кулдауна.")  # Подтверждаем действие пользователю


@router.callback_query(F.data == CB_EXAMPLES_MORE_FOREIGN)
async def on_examples_more_foreign(callback: CallbackQuery) -> None:
    chat_id = await _resolve_chat_id(callback)  # Определяем чат, с которым работаем
    if chat_id is None:  # Проверяем наличие идентификатора
        await callback.answer("Не удалось определить чат.", show_alert=True)  # Сообщаем об ошибке определения чата
        return  # Завершаем обработку, если ID отсутствует
    ctx = get_context(chat_id)  # Загружаем контекст примеров
    if not ctx:  # Убеждаемся, что контекст сохранён
        await callback.answer("Нет контекста примеров. Повторите команду.", show_alert=True)  # Просим перезапустить команду
        return  # Прекращаем обработку, если контекст недоступен
    message = callback.message  # Получаем сообщение для обновления
    if message is None:  # Проверяем наличие сообщения
        await callback.answer("Не удалось обновить сообщение.", show_alert=True)  # Сообщаем о проблеме обновления
        return  # Завершаем обработку при отсутствии сообщения
    text = format_examples_block(
        cooldown_total=ctx.cooldown_total,  # Общее количество адресов под ограничением
        cooldown_triplet=(ctx.cooldown_pager.next_chunk() if ctx.cooldown_pager.idx else []),  # Обновляем список кулдауна только при повторных запросах
        foreign_total=ctx.foreign_total,  # Количество иностранных адресов
        foreign_triplet=ctx.foreign_pager.next_chunk(),  # Получаем следующую тройку иностранных адресов
        foreign_title_suffix="отложены" if ctx.source_kind == "send" else "",  # Уточняем подпись для отчётов отправки
    )
    await message.edit_text(text, reply_markup=build_examples_paging_kb())  # Обновляем сообщение новыми примерами
    await callback.answer("Показаны следующие иностранные адреса.")  # Подтверждаем выполненное действие


@router.callback_query(F.data == CB_EXAMPLES_BACK)
async def on_examples_back(callback: CallbackQuery) -> None:
    message = callback.message  # Получаем сообщение, которое необходимо заменить
    if message is None:  # Проверяем, доступно ли сообщение
        await callback.answer("Сообщение недоступно.", show_alert=True)  # Сообщаем о невозможности обновления
        return  # Прекращаем обработку, если сообщение отсутствует
    await message.edit_text(  # Обновляем текст сообщения подсказкой
        "Вернитесь к отчёту: перезапустите команду или восстановите предпросмотр.",
        reply_markup=build_examples_entry_kb(),
    )
    await callback.answer()  # Подтверждаем выполнение команды
