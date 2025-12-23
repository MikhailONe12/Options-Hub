#!/bin/bash

# Set terminal encoding to UTF-8
export LANG=en_US.UTF-8

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Запуск приложения Bybit Metrics..."
echo ""

# Function to check if a port is available
is_port_available() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 1  # Port is in use
    else
        return 0  # Port is available
    fi
}

# Find available ports
echo "Проверка свободных портов..."
# Use fixed ports since server.js has its own port handling logic
API_PORT=9000
WEB_PORT=9001

echo "Будут использованы порты: API сервер - $API_PORT, Веб-интерфейс - $WEB_PORT"
echo ""

# Check if node is installed
if ! command -v node &> /dev/null; then
    echo "Ошибка: Node.js не установлен или не найден в PATH"
    echo "Пожалуйста, установите Node.js с https://nodejs.org/"
    exit 1
fi

# Check if npx is installed
if ! command -v npx &> /dev/null; then
    echo "Ошибка: npx не установлен или не найден в PATH"
    echo "Пожалуйста, установите Node.js с https://nodejs.org/ (включает npm и npx)"
    exit 1
fi

# Start API server in background
echo "1. Запуск API сервера (в фоне)..."
node server.js $API_PORT &
API_PID=$!

# Wait a few seconds for API server to start
sleep 3
echo "API сервер запущен на порту $API_PORT"
echo ""

# Open web interface in default browser
echo "2. Запуск веб-интерфейса..."
open http://localhost:$WEB_PORT

# Start web server with better error handling
# First try to use the local serve package
if ! npx serve -p $WEB_PORT 2>/dev/null; then
    echo "Попытка запуска веб-сервера на порту $WEB_PORT..."
    # Try alternative methods if npx serve fails
    if command -v python3 &> /dev/null; then
        echo "Используется Python для запуска веб-сервера..."
        python3 -m http.server $WEB_PORT
    elif command -v python &> /dev/null; then
        echo "Используется Python для запуска веб-сервера..."
        python -m SimpleHTTPServer $WEB_PORT
    else
        echo "Ошибка: Не удалось запустить веб-сервер на порту $WEB_PORT"
        echo "Установите Node.js или Python для запуска веб-сервера"
        # Kill the API server if web server failed to start
        if [[ -n $API_PID ]]; then
            kill $API_PID 2>/dev/null
        fi
        exit 1
    fi
fi