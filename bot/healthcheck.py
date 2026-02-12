from flask import Flask
import threading
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def home():
    return "🌱 Бот Бонелет працює!"

@app.route('/health')
def health():
    return "OK", 200

def run_flask():
    """Запуск Flask сервера"""
    port = int(os.getenv("PORT", 8080))
    logger.info(f"🚀 Flask запущено на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def run_bot():
    """Запуск Telegram бота"""
    try:
        import bot
        logger.info("🤖 Запуск Telegram бота...")
        bot.main()
    except Exception as e:
        logger.error(f"❌ Помилка бота: {e}")

if __name__ == "__main__":
    # Запускаємо Flask в окремому потоці
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Запускаємо бота в головному потоці
    run_bot()
