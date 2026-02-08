"""
🌱 БОТ ФЕРМИ "СМАК ПРИРОДИ" - Replit версия
Работает 24/7 с UptimeRobot
"""

import os
import logging
import asyncio
from threading import Thread
from flask import Flask
from flask_ngrok import run_with_ngrok  # Для локального туннеля

# Импортируем вашего бота
try:
    from bot import main as bot_main, TOKEN
except ImportError:
    # Если не можем импортировать, запускаем напрямую
    import sys
    sys.path.append('.')
    from bot import main as bot_main, TOKEN

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask приложение для пинга
app = Flask(__name__)

@app.route('/')
def home():
    return "🌱 Бот фермы 'Смак природи' работает на Replit! ✅"

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/health')
def health():
    return "OK", 200

def run_flask():
    """Запускает Flask сервер"""
    try:
        # На Replit используем порт 5000
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
    except Exception as e:
        logger.error(f"Ошибка Flask: {e}")

def run_bot():
    """Запускает Telegram бота"""
    try:
        logger.info("🚀 Запуск Telegram бота...")
        asyncio.run(bot_main())
    except Exception as e:
        logger.error(f"Ошибка бота: {e}")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🌱 ЗАПУСК БОТА ФЕРМЫ 'СМАК ПРИРОДИ'")
    logger.info("📱 Telegram бот + Flask сервер")
    logger.info("⏰ UptimeRobot для 24/7 работы")
    logger.info("=" * 60)
    
    # Запускаем Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Запускаем бота в основном потоке
    run_bot()
