# EBOT-REV-001 — Миграция на aiogram 3.x

Этот каркас вводит точку входа `python -m emailbot.bot` на базе **aiogram 3.x**
и объединяет базовые обработчики `/start` и `/send` в новом `Dispatcher`.

## Запуск

```bash
python -m emailbot.bot
```

Перед запуском создайте файл `.env` и заполните переменные:

- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `EMAIL_ADDRESS` и `EMAIL_PASSWORD` — SMTP-аккаунт для отправки писем.
- Дополнительно можно указать `SMTP_HOST`, `SMTP_PORT`, `SMTP_STARTTLS`.
- `COOLDOWN_DAYS` и `SEND_STATS_PATH` — настройки кулдауна и логов отправки.

Библиотека `python-dotenv` подхватывает `.env` автоматически.

## Обработчики

- `/start` и `/help` показывают клавиатуру направлений из `templates/_labels.json`.
- `/send email@domain.tld | Тема | Текст` валидирует адрес и отправляет письмо через SMTP.

Отправка проходит через общий шлюз `emailbot.aiogram_port.messaging.send_one_email`,
который проверяет кулдаун, логирует события в `utils.send_stats` и уважает настройку
`COOLDOWN_DAYS`.

## Дополнительно

- Для Windows (Anaconda PowerShell) добавлен скрипт `run_bot.ps1`.
- `requirements.delta.txt` содержит подсказку по зависимостям: удалить
  `python-telegram-bot` и добавить `aiogram`/`python-dotenv`, если ещё не сделано.
