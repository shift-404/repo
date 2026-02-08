import os
import logging
import sys

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

logger.info("=== НАЧАЛО РАБОТЫ БОТА ===")

# Получение токена
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logger.error("❌ Токен не найден! Добавьте BOT_TOKEN в переменные окружения Scalingo")
    exit(1)

logger.info(f"✅ Токен получен: {TOKEN[:4]}...")
logger.info("✅ Все импорты успешны")
logger.info("✅ Бот готов к запуску (но пока без функционала)")

# Просто ждем, чтобы контейнер не завершался сразу
import time
while True:
    logger.info("⏳ Бот работает...")
    time.sleep(10)
