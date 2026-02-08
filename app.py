from flask import Flask, request
import os
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def home():
    return "🌱 Бот фермы 'Смак природи' работает! ✅"

@app.route('/health')
def health():
    """Эндпоинт для проверки здоровья от Render"""
    return "OK", 200

@app.route('/test')
def test():
    """Тестовый эндпоинт"""
    return "🟢 Сервер работает", 200

@app.errorhandler(404)
def not_found(error):
    return "🔍 Страница не найдена", 404

@app.errorhandler(500)
def internal_error(error):
    return "❌ Внутренняя ошибка сервера", 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"🚀 Запуск Flask на порту {port}")
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,
        threaded=True
    )
