from flask import Flask
import threading
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "🌱 Бот фермы работает!"

@app.route('/health')
def health():
    return "OK", 200

def run_flask():
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)

# Запускаем Flask в отдельном потоке
threading.Thread(target=run_flask, daemon=True).start()
