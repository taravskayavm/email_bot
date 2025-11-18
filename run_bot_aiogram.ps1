<#
    Сценарий автоматизирует запуск aiogram-бота на Windows и предполагает, что:
    - проект находится в папке D:\email_bot;
    - используется Anaconda/Miniconda;
    - существует окружение conda с именем "emailbot";
    - в корне проекта лежит .env с TELEGRAM_BOT_TOKEN и остальными настройками;
    - в модуле emailbot.bot.__main__ определена точка входа aiogram-бота.
#>

Write-Host "Переход в каталог проекта D:\email_bot..." # Сообщаем пользователю, что переходим в директорию проекта
Set-Location "D:\email_bot" # Переключаемся в каталог, где хранится код проекта

Write-Host "Активация окружения conda 'emailbot'..." # Уведомляем о запуске conda-окружения emailbot
conda activate emailbot # Активируем подготовленное conda-окружение

Write-Host "Запуск aiogram-бота (python -m emailbot.bot)..." # Информируем о старте aiogram-бота через модульную точку входа emailbot.bot.__main__
python -m emailbot.bot # Запускаем основной модуль aiogram-бота, определённый в emailbot.bot.__main__

Write-Host "Процесс бота завершён." # Сообщаем об окончании выполнения скрипта
