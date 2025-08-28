# Email Bot

![CI](https://github.com/taravskayavm/email_bot/actions/workflows/ci.yml/badge.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![Lint: Ruff](https://img.shields.io/badge/lint-ruff-blueviolet.svg)

Telegram-бот для автоматизации рассылки писем:
- парсит email-адреса из PDF и Excel,
- поддерживает ручное подтверждение перед отправкой,
- ведёт историю рассылок (одно письмо не чаще, чем раз в 6 месяцев),
- позволяет управлять группами адресатов и шаблонами писем,
- имеет систему исключений (блок-лист).

## 🚀 Установка и запуск

```bash
git clone https://github.com/taravskayavm/email_bot.git
cd email_bot
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt

cp .env.example .env   # и заполнить своими данными
python email_bot.py
```

## 🛠 Технологии
- Python 3.11
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot)
- Pandas, OpenPyXL
- PyMuPDF
- Pre-commit + Ruff + Black + Flake8 + Mypy
- Pytest + Coverage

## 📦 CI/CD
GitHub Actions проверяет:
- синтаксис и зависимости,
- стиль кода (Ruff, Flake8, Black),
- аннотации типов (Mypy),
- тесты (Pytest с покрытием).

Статус сборки: ![CI](https://github.com/taravskayavm/email_bot/actions/workflows/ci.yml/badge.svg)

---

⚠️ **License: All Rights Reserved**  
Использование, копирование или модификация этого кода возможны только с разрешения автора.
