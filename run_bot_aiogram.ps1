<# 
    Скрипт для запуска aiogram-версии Telegram-бота Email Bot.
    Предполагается, что:
    - проект находится в папке D:\email_bot;
    - используется Anaconda/Miniconda;
    - существует окружение conda с именем "emailbot";
    - в корне проекта лежит .env с TELEGRAM_BOT_TOKEN и остальными настройками;
    - в модуле bot.aiogram_main определена точка входа aiogram-бота.
#>

Write-Host "Переход в каталог проекта D:\email_bot..." # Сообщаем пользователю, что переходим в директорию проекта
Set-Location "D:\email_bot" # Переключаемся в каталог, где лежит проект

Write-Host "Активация окружения conda 'emailbot'..." # Уведомляем о запуске conda-окружения emailbot
conda activate emailbot # Активируем подготовленное conda-окружение

Write-Host "Запуск aiogram-бота (python -m bot.aiogram_main)..." # Информируем о старте aiogram-бота через модульную точку входа
python -m bot.aiogram_main # Запускаем основной модуль aiogram-бота

Write-Host "Процесс бота завершён." # Сообщаем об окончании выполнения скрипта
