<# 
    Скрипт для запуска aiogram-версии Telegram-бота Email Bot.
    Предполагается, что:
    - проект находится в папке D:\email_bot;
    - используется Anaconda/Miniconda;
    - существует окружение conda с именем "emailbot";
    - в корне проекта лежит .env с TELEGRAM_BOT_TOKEN и остальными настройками;
    - в модуле emailbot.bot определена точка входа aiogram-бота.
#>

Write-Host "Переход в каталог проекта D:\email_bot..." # Сообщаем пользователю, что переходим в директорию проекта
Set-Location -LiteralPath $PSScriptRoot

Write-Host "Активация окружения conda 'emailbot'..." # Уведомляем о запуске conda-окружения emailbot
conda activate emailbot # Активируем подготовленное conda-окружение

Write-Host "Запуск aiogram-бота (python -m emailbot.bot)..."
python -m emailbot.bot

Write-Host "Процесс бота завершён." # Сообщаем об окончании выполнения скрипта
