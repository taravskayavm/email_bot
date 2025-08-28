# Email Bot — Рассылка HTML-писем через Telegram

## Возможности
- Парсинг email-адресов из PDF и Excel
- Поддержка трёх направлений: спорт, туризм, медицина
- Отправка HTML-писем с форматированием и логотипом
- Проверка, были ли письма отправлены ранее (по IMAP mail.ru)
- Письма отображаются в "Отправленные"
- Управление рассылкой через Telegram

## Установка и запуск

1. Установите зависимости:
```
pip install -r requirements.txt
```

2. Создайте файл `.env` (на основе `.env.example`) и укажите логин/пароль от почты.

3. Запустите бота:
```
python email_bot.py
```
---

## Open-source & Privacy

В репозитории нет персональных данных. Исключены через `.gitignore`:
`.env`, `blocked_emails.txt`, `bot_errors.log`, `sent_log.csv`.

### Quick start
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env  # затем заполните
python email_bot.py
```
>>>>>>> 51d698b (Initial commit)
