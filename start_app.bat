@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul
echo Запуск приложения Bybit Metrics...
echo.

REM Запускаем ОДИН сервер (API + веб-интерфейс) через server.js.
REM server.js сам раздаёт статику (express.static('.')), поэтому отдельный npx serve не нужен.

echo Проверка свободного порта...
set PORT=9001

REM Ищем первый свободный порт в диапазоне 9001..9100
for /l %%p in (9001, 1, 9100) do (
    netstat -an | findstr LISTENING | findstr :%%p >nul
    if errorlevel 1 (
        set PORT=%%p
        goto port_found
    )
)

:port_found
echo Найден порт: !PORT!

echo 1. Запуск сервера (API + Web) на порту !PORT! (в фоне)...
start /min cmd /c "chcp 65001 >nul && node server.js !PORT!"
timeout /t 2 /nobreak >nul

echo 2. Открытие веб-интерфейса...
start http://localhost:!PORT!/
echo.
echo Готово. Используйте один адрес: http://localhost:!PORT!/