import os
import json
import logging
import sys
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from io import StringIO, BytesIO
import asyncio
import traceback
import time
import requests
import socket

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - ADMIN - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('admin_bot_debug.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("ЗАПУСК АДМІН-БОТА З РОЗШИРЕНОЮ ВІДЛАДКОЮ")
logger.info("=" * 80)

KYIV_TZ = None
try:
    import pytz
    KYIV_TZ = pytz.timezone('Europe/Kyiv')
    logger.info("✅ Бібліотека pytz завантажена, часовий пояс Kyiv")
except ImportError:
    logger.warning("⚠️ Бібліотека pytz не встановлена, використовую UTC")
    KYIV_TZ = None

def get_kyiv_time():
    if KYIV_TZ:
        return datetime.now(KYIV_TZ)
    return datetime.now()

def format_kyiv_time(dt_str):
    if not dt_str:
        return "Н/Д"
    try:
        if isinstance(dt_str, datetime):
            dt = dt_str
        else:
            dt = datetime.strptime(str(dt_str)[:19], '%Y-%m-%d %H:%M:%S')
        if KYIV_TZ and dt.tzinfo is None:
            try:
                dt = pytz.UTC.localize(dt)
                dt = dt.astimezone(KYIV_TZ)
            except:
                pass
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Помилка форматування часу: {e}")
        return str(dt_str)[:16]

# Отримуємо токени з оточення
TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not TOKEN:
    logger.error("❌ ADMIN_BOT_TOKEN не знайдено!")
    sys.exit(1)
else:
    logger.info(f"✅ ADMIN_BOT_TOKEN отримано: {TOKEN[:10]}...")

MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not MAIN_BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не знайдено!")
    sys.exit(1)
else:
    logger.info(f"✅ MAIN_BOT_TOKEN отримано: {MAIN_BOT_TOKEN[:10]}...")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
logger.info(f"✅ ADMIN_PASSWORD конфігуровано: {'так' if ADMIN_PASSWORD else 'ні'}")

ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]
logger.info(f"✅ ADMIN_IDS: {ADMIN_IDS}")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL не знайдено!")
    sys.exit(1)
else:
    logger.info(f"✅ DATABASE_URL отримано: {DATABASE_URL[:20]}...")

def get_db_connection():
    """Підключення до бази даних з детальним логуванням помилок"""
    logger.debug("Спроба підключення до БД...")
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        logger.debug("✅ Підключення до БД успішне")
        return conn
    except Exception as e:
        logger.error(f"❌ Помилка підключення до БД: {e}")
        logger.error(traceback.format_exc())
        return None

def init_database_if_empty():
    """Ініціалізація бази даних з детальним логуванням"""
    logger.info("=" * 60)
    logger.info("🔄 Ініціалізація бази даних...")
    logger.info("=" * 60)
    
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Не вдалося підключитись до БД для ініціалізації")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Створення таблиць
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id BIGINT PRIMARY KEY,
                state TEXT DEFAULT '',
                temp_data TEXT DEFAULT '{}',
                last_section TEXT DEFAULT 'main_menu',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS carts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                product_id INTEGER,
                quantity REAL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_name TEXT,
                username TEXT,
                phone TEXT,
                city TEXT,
                np_department TEXT,
                total REAL,
                status TEXT DEFAULT 'нове',
                order_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER,
                product_name TEXT,
                quantity REAL,
                price_per_unit REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_name TEXT,
                username TEXT,
                text TEXT,
                message_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quick_orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_name TEXT,
                username TEXT,
                phone TEXT,
                product_id INTEGER,
                product_name TEXT,
                quantity REAL,
                contact_method TEXT,
                message TEXT,
                status TEXT DEFAULT 'нове',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT,
                description TEXT,
                unit TEXT DEFAULT 'банка',
                image TEXT,
                image_data BYTEA,
                details TEXT,
                benefits TEXT,
                usage TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_info (
                id INTEGER PRIMARY KEY DEFAULT 1,
                text TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by BIGINT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS welcome_message (
                id INTEGER PRIMARY KEY DEFAULT 1,
                text TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by BIGINT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS faq (
                id SERIAL PRIMARY KEY,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                position INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Додаємо колонки якщо їх немає
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image TEXT')
            logger.info("✅ Колонка image додана до таблиці products")
        except Exception as e:
            logger.error(f"❌ Помилка додавання колонки image: {e}")
        
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image_data BYTEA')
            logger.info("✅ Колонка image_data додана до таблиці products")
        except Exception as e:
            logger.error(f"❌ Помилка додавання колонки image_data: {e}")
        
        # Додаємо нові колонки для benefits та usage
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS benefits TEXT')
            logger.info("✅ Колонка benefits додана до таблиці products")
        except Exception as e:
            logger.error(f"❌ Помилка додавання колонки benefits: {e}")

        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS usage TEXT')
            logger.info("✅ Колонка usage додана до таблиці products")
        except Exception as e:
            logger.error(f"❌ Помилка додавання колонки usage: {e}")
        
        # Додаємо початкові дані для company_info
        cursor.execute("SELECT COUNT(*) FROM company_info")
        company_count = cursor.fetchone()['count']
        
        if company_count == 0:
            company_text = """
<b>🌱 Компанія Бонелет</b>

Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини.

<b>📋 Деталі:</b>
• 👨‍🌾 Працюємо з 2022 року
• 📍 Розташування: Одеська область, с. Великий Дальник
• 📞 Телефон: +380932599103
• 🕒 Графік: ПН-ПТ 9:00-18:00 СБ 10:00-15:00
• 🚚 Доставка: Новою Поштою по всій Україні

<b>🌿 Наша філософія:</b>
• Вирощуємо на власних полях Одещини
• Використовуємо натуральне консервування
• Гарантуємо якість кожного продукту
• Працюємо з любов'ю до природи

<b>🚚 Доставка:</b>
• Новою Поштою по всій Україні
• Самовивіз з Одеської області, с. Великий Дальник
• Терміни доставки: 1-4 дні в залежності від регіону
"""
            cursor.execute('''
                INSERT INTO company_info (id, text) VALUES (1, %s)
            ''', (company_text,))
            logger.info("✅ Додано початкові дані company_info")
        
        # Додаємо початкові дані для welcome_message
        cursor.execute("SELECT COUNT(*) FROM welcome_message")
        welcome_count = cursor.fetchone()['count']
        
        if welcome_count == 0:
            welcome_text = """
<b>🇺🇦 Вітаємо у боті компанії Бонелет! 🌱</b>

Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини:

🥫 Артишок маринований з зернами гірчиці - пікантний, не гострий
🌶️ Артишок маринований з чилі - з нотками гостроти
🍯 Паштет з артишоку - ніжний для бутербродів

<b>🏢 Про нас:</b>
• Працюємо з 2022 року
• Розташування: Одеська область, с. Великий Дальник
• Доставка Новою Поштою по всій Україні

<b>Оберіть опцію з меню 👇</b>
    """
            cursor.execute('''
                INSERT INTO welcome_message (id, text) VALUES (1, %s)
            ''', (welcome_text,))
            logger.info("✅ Додано початкові дані welcome_message")
        
        # Додаємо початкові FAQ
        cursor.execute("SELECT COUNT(*) FROM faq")
        faq_count = cursor.fetchone()['count']
        
        if faq_count == 0:
            faqs = [
                ("Які способи оплати ви приймаєте?", "✅ Готівка при отриманні\n✅ Переказ на карту ПриватБанку\n✅ Оплата через LiqPay", 0),
                ("Які терміни доставки?", "🚚 Київ - 1-2 дні\n🚚 Україна - 2-4 дні\n🚛 Великі партії - 3-5 днів", 1)
            ]
            for question, answer, position in faqs:
                cursor.execute('''
                    INSERT INTO faq (question, answer, position) VALUES (%s, %s, %s)
                ''', (question, answer, position))
            logger.info("✅ Додано початкові FAQ")
        
        conn.commit()
        logger.info("✅ База даних успішно ініціалізована!")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка ініціалізації бази даних: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# ========== ФУНКЦІЇ ДЛЯ РОБОТИ З КОМПАНІЄЮ ==========

def get_company_info() -> str:
    """Отримує текст про компанію з БД"""
    logger.debug("Виклик get_company_info()")
    conn = get_db_connection()
    if not conn:
        return "Помилка отримання даних"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT text FROM company_info WHERE id = 1')
        row = cursor.fetchone()
        return row['text'] if row else "Інформацію не знайдено"
    except Exception as e:
        logger.error(f"Помилка отримання company_info: {e}")
        return "Помилка отримання даних"
    finally:
        conn.close()

def update_company_info(text: str, updated_by: int) -> bool:
    """Оновлює текст про компанію в БД"""
    logger.debug(f"Виклик update_company_info(), updated_by: {updated_by}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE company_info 
            SET text = %s, updated_at = CURRENT_TIMESTAMP, updated_by = %s
            WHERE id = 1
        ''', (text, updated_by))
        conn.commit()
        logger.info(f"✅ Company info оновлено користувачем {updated_by}")
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення company_info: {e}")
        return False
    finally:
        conn.close()

# ========== ФУНКЦІЇ ДЛЯ РОБОТИ З ВІТАЛЬНИМ ПОВІДОМЛЕННЯМ ==========

def get_welcome_message() -> str:
    """Отримує вітальне повідомлення з БД"""
    logger.debug("Виклик get_welcome_message()")
    conn = get_db_connection()
    if not conn:
        return "Помилка отримання даних"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT text FROM welcome_message WHERE id = 1')
        row = cursor.fetchone()
        return row['text'] if row else "Повідомлення не знайдено"
    except Exception as e:
        logger.error(f"Помилка отримання welcome_message: {e}")
        return "Помилка отримання даних"
    finally:
        conn.close()

def update_welcome_message(text: str, updated_by: int) -> bool:
    """Оновлює вітальне повідомлення в БД"""
    logger.debug(f"Виклик update_welcome_message(), updated_by: {updated_by}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE welcome_message 
            SET text = %s, updated_at = CURRENT_TIMESTAMP, updated_by = %s
            WHERE id = 1
        ''', (text, updated_by))
        conn.commit()
        logger.info(f"✅ Welcome message оновлено користувачем {updated_by}")
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення welcome_message: {e}")
        return False
    finally:
        conn.close()

# ========== ФУНКЦІЇ ДЛЯ РОБОТИ З FAQ ==========

def get_all_faqs() -> List[Dict]:
    """Отримує всі FAQ з БД, відсортовані за позицією"""
    logger.debug("Виклик get_all_faqs()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer, position FROM faq ORDER BY position, id')
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        logger.debug(f"Отримано {len(result)} FAQ")
        return result
    except Exception as e:
        logger.error(f"Помилка отримання faq: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_faq_by_id(faq_id: int) -> Optional[Dict]:
    """Отримує FAQ за ID з усіма полями"""
    logger.debug(f"Виклик get_faq_by_id() з ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        logger.error("Не вдалося підключитись до БД")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer, position FROM faq WHERE id = %s', (faq_id,))
        row = cursor.fetchone()
        if row:
            result = {
                'id': row['id'],
                'question': row['question'],
                'answer': row['answer'],
                'position': row['position']
            }
            logger.debug(f"✅ Знайдено FAQ #{faq_id}: {result['question'][:30]}...")
            return result
        logger.warning(f"❌ FAQ з ID {faq_id} не знайдено")
        return None
    except Exception as e:
        logger.error(f"Помилка отримання faq за ID {faq_id}: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def add_faq(question: str, answer: str) -> Optional[int]:
    """Додає новий FAQ, повертає ID"""
    logger.debug(f"Виклик add_faq(), питання: {question[:30]}...")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO faq (question, answer, position) 
            VALUES (%s, %s, COALESCE((SELECT MAX(position) + 1 FROM faq), 0))
            RETURNING id
        ''', (question, answer))
        result = cursor.fetchone()
        conn.commit()
        faq_id = result['id'] if result else None
        logger.info(f"✅ FAQ додано з ID: {faq_id}")
        return faq_id
    except Exception as e:
        logger.error(f"Помилка додавання faq: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def update_faq(faq_id: int, question: str, answer: str) -> bool:
    """Оновлює існуючий FAQ"""
    logger.debug(f"Виклик update_faq() для ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE faq 
            SET question = %s, answer = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        ''', (question, answer, faq_id))
        conn.commit()
        logger.info(f"✅ FAQ #{faq_id} оновлено")
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення faq #{faq_id}: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def delete_faq(faq_id: int) -> bool:
    """Видаляє FAQ"""
    logger.debug(f"Виклик delete_faq() для ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM faq WHERE id = %s', (faq_id,))
        conn.commit()
        logger.info(f"✅ FAQ #{faq_id} видалено")
        return True
    except Exception as e:
        logger.error(f"Помилка видалення faq #{faq_id}: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def move_faq_up(faq_id: int) -> bool:
    """Переміщує FAQ вгору (зменшує position)"""
    logger.debug(f"Виклик move_faq_up() для ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute('SELECT position FROM faq WHERE id = %s', (faq_id,))
        current = cursor.fetchone()
        if not current:
            return False
        
        current_pos = current['position']
        
        cursor.execute('SELECT id, position FROM faq WHERE position < %s ORDER BY position DESC LIMIT 1', (current_pos,))
        previous = cursor.fetchone()
        
        if previous:
            cursor.execute('UPDATE faq SET position = %s WHERE id = %s', (previous['position'], faq_id))
            cursor.execute('UPDATE faq SET position = %s WHERE id = %s', (current_pos, previous['id']))
            conn.commit()
            logger.info(f"✅ FAQ #{faq_id} переміщено вгору")
            return True
        return False
    except Exception as e:
        logger.error(f"Помилка переміщення faq вгору: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def move_faq_down(faq_id: int) -> bool:
    """Переміщує FAQ вниз (збільшує position)"""
    logger.debug(f"Виклик move_faq_down() для ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute('SELECT position FROM faq WHERE id = %s', (faq_id,))
        current = cursor.fetchone()
        if not current:
            return False
        
        current_pos = current['position']
        
        cursor.execute('SELECT id, position FROM faq WHERE position > %s ORDER BY position ASC LIMIT 1', (current_pos,))
        next_faq = cursor.fetchone()
        
        if next_faq:
            cursor.execute('UPDATE faq SET position = %s WHERE id = %s', (next_faq['position'], faq_id))
            cursor.execute('UPDATE faq SET position = %s WHERE id = %s', (current_pos, next_faq['id']))
            conn.commit()
            logger.info(f"✅ FAQ #{faq_id} переміщено вниз")
            return True
        return False
    except Exception as e:
        logger.error(f"Помилка переміщення faq вниз: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# ========== ДОПОМІЖНІ ФУНКЦІЇ ==========

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)
logger.info(f"✅ Директорія для звітів: {REPORTS_DIR}")

admin_sessions = {}
last_password_check = {}
orders_offset = {}
messages_offset = {}
broadcast_in_progress = {}

def is_authenticated(user_id: int) -> bool:
    """Перевіряє чи автентифікований користувач"""
    result = user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"
    logger.debug(f"Перевірка автентифікації для {user_id}: {result}")
    if not result and user_id in admin_sessions:
        logger.debug(f"Сесія користувача {user_id}: {admin_sessions[user_id]}")
    return result

async def download_image_from_url_to_bytes(url: str) -> bytes:
    """Завантажує зображення за URL і повертає як байти"""
    logger.info(f"🌐 Спроба завантажити URL: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, timeout=30, allow_redirects=True, headers=headers)
        response.raise_for_status()
        
        logger.info(f"✅ Зображення завантажено, розмір: {len(response.content)} байт")
        return response.content
        
    except Exception as e:
        logger.error(f"❌ Помилка завантаження зображення: {e}")
        return None

async def download_telegram_file_to_bytes(file_id: str, bot: Bot) -> bytes:
    """Завантажує файл з Telegram і повертає як байти"""
    try:
        file = await bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        logger.info(f"✅ Фото завантажено в пам'ять, розмір: {len(file_bytes)} байт")
        return bytes(file_bytes)
    except Exception as e:
        logger.error(f"❌ Помилка завантаження файлу: {e}")
        return None

async def reset_all_orders():
    """Скидає всі замовлення"""
    logger.warning("⚠️ Викликано reset_all_orders()")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM order_items")
        cursor.execute("DELETE FROM orders")
        cursor.execute("DELETE FROM quick_orders")
        cursor.execute("DELETE FROM carts")
        cursor.execute("DELETE FROM messages")
        
        conn.commit()
        logger.info("✅ Всі замовлення та повідомлення успішно видалено!")
        return True
    except Exception as e:
        logger.error(f"Помилка видалення замовлень: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def safe_get(order, key, default=0):
    """Безпечне отримання значення зі словника"""
    value = order.get(key)
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def get_all_orders(include_quick: bool = True, limit: int = None, offset: int = 0):
    """Отримує всі замовлення"""
    logger.debug(f"Виклик get_all_orders(include_quick={include_quick}, limit={limit}, offset={offset})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        query = '''
            SELECT *, 'regular' as order_type FROM orders 
            ORDER BY created_at DESC
        '''
        if limit:
            query += f' LIMIT {limit} OFFSET {offset}'
        
        cursor.execute(query)
        regular_orders = cursor.fetchall()
        
        all_orders = []
        for row in regular_orders:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            
            cursor.execute('''
                SELECT * FROM order_items 
                WHERE order_id = %s
            ''', (order['order_id'],))
            items = cursor.fetchall()
            
            order_items = []
            for item in items:
                item_dict = dict(item)
                item_dict['created_at'] = format_kyiv_time(item_dict.get('created_at'))
                order_items.append(item_dict)
            
            order['items'] = order_items
            order['display_id'] = order['order_id']
            all_orders.append(order)
        
        if include_quick:
            query = '''
                SELECT *, 'quick' as order_type FROM quick_orders 
                ORDER BY created_at DESC
            '''
            if limit:
                query += f' LIMIT {limit} OFFSET {offset}'
            
            cursor.execute(query)
            quick_orders = cursor.fetchall()
            
            for row in quick_orders:
                order = dict(row)
                order['created_at'] = format_kyiv_time(order.get('created_at'))
                order['order_id'] = order['id']
                order['display_id'] = order['id']
                order['total'] = safe_get(order, 'total', 0)
                order['city'] = order.get('city', 'Н/Д')
                order['np_department'] = order.get('np_department', 'Н/Д')
                all_orders.append(order)
        
        all_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        logger.debug(f"Отримано {len(all_orders)} замовлень")
        return all_orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_recent_orders(hours: int = 1, min_count: int = 3):
    """Отримує останні замовлення"""
    logger.debug(f"Виклик get_recent_orders(hours={hours}, min_count={min_count})")
    all_orders = get_all_orders(include_quick=True)
    
    kyiv_now = get_kyiv_time()
    time_limit = kyiv_now - timedelta(hours=hours)
    
    recent_orders = []
    for order in all_orders:
        try:
            order_time_str = order.get('created_at', '')
            if not order_time_str:
                continue
            order_time = datetime.strptime(str(order_time_str)[:19], '%Y-%m-%d %H:%M:%S')
            if KYIV_TZ:
                try:
                    order_time = KYIV_TZ.localize(order_time)
                except:
                    pass
            if order_time >= time_limit:
                recent_orders.append(order)
        except:
            continue
    
    if len(recent_orders) < min_count:
        additional = all_orders[:min_count]
        for order in additional:
            if order not in recent_orders:
                recent_orders.append(order)
    
    return recent_orders[:min_count]

def get_more_orders(user_id: int, count: int = 5):
    """Отримує наступні замовлення для пагінації"""
    logger.debug(f"Виклик get_more_orders(user_id={user_id}, count={count})")
    if user_id not in orders_offset:
        orders_offset[user_id] = 0
    
    offset = orders_offset[user_id]
    orders = get_all_orders(include_quick=True, limit=count, offset=offset)
    orders_offset[user_id] = offset + len(orders)
    
    return orders

def format_order_text(order: dict) -> str:
    """Форматує текст замовлення для відображення"""
    order_type = "⚡" if order.get('order_type') == 'quick' else "📦"
    order_id = order.get('order_id', order.get('id', 'Н/Д'))
    
    user_name = order.get('user_name', 'Н/Д')
    phone = order.get('phone', 'Н/Д')
    total = safe_get(order, 'total', 0)
    status = order.get('status', 'нове')
    created_at = order.get('created_at', '')
    
    text = f"{order_type} <b>№{order_id}</b> | {created_at[:16] if created_at else 'Н/Д'}\n"
    text += f"👤 Клієнт: {user_name}\n"
    text += f"📞 Телефон: {phone}\n"
    
    if order.get('order_type') == 'quick':
        product_name = order.get('product_name', 'Н/Д')
        text += f"📦 Продукт: {product_name}\n"
        if order.get('message'):
            msg = order.get('message', '')
            text += f"💬 Повідомлення: {msg[:50]}{'...' if len(msg) > 50 else ''}\n"
        text += f"💰 Сума: {total:.2f} грн\n"
    else:
        text += f"💰 Сума: {total:.2f} грн\n"
    
    text += f"📊 Статус: {status}\n"
    return text

def get_orders_by_phone(phone: str):
    """Шукає замовлення за телефоном"""
    logger.debug(f"Виклик get_orders_by_phone(phone={phone})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT *, 'regular' as order_type FROM orders 
            WHERE phone LIKE %s 
            ORDER BY created_at DESC
        ''', (f"%{phone}%",))
        regular_orders = cursor.fetchall()
        
        all_orders = []
        for row in regular_orders:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['display_id'] = order['order_id']
            all_orders.append(order)
        
        cursor.execute('''
            SELECT *, 'quick' as order_type FROM quick_orders 
            WHERE phone LIKE %s 
            ORDER BY created_at DESC
        ''', (f"%{phone}%",))
        quick_orders = cursor.fetchall()
        
        for row in quick_orders:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['order_id'] = order['id']
            order['display_id'] = order['id']
            order['total'] = safe_get(order, 'total', 0)
            all_orders.append(order)
        
        return all_orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень за телефоном: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_new_orders():
    """Отримує нові замовлення"""
    logger.debug("Виклик get_new_orders()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT *, 'regular' as order_type FROM orders 
            WHERE status = 'нове'
            ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['display_id'] = order['order_id']
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання нових замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_quick_orders():
    """Отримує швидкі замовлення"""
    logger.debug("Виклик get_quick_orders()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM quick_orders 
            ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['order_id'] = order['id']
            order['display_id'] = order['id']
            order['total'] = safe_get(order, 'total', 0)
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання швидких замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def update_order_status(order_id: int, status: str, order_type: str = 'regular'):
    """Оновлює статус замовлення"""
    logger.debug(f"Виклик update_order_status(order_id={order_id}, status={status}, order_type={order_type})")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        if order_type == 'regular' or order_type == 'orders':
            cursor.execute('''
                UPDATE orders SET status = %s WHERE order_id = %s
            ''', (status, order_id))
        else:
            cursor.execute('''
                UPDATE quick_orders SET status = %s WHERE id = %s
            ''', (status, order_id))
        
        conn.commit()
        logger.info(f"✅ Статус замовлення #{order_id} оновлено на '{status}'")
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення статусу: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_order_by_id(order_id: int, order_type: str = 'regular'):
    """Отримує замовлення за ID"""
    logger.debug(f"Виклик get_order_by_id(order_id={order_id}, order_type={order_type})")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        if order_type == 'regular' or order_type == 'orders':
            cursor.execute('SELECT * FROM orders WHERE order_id = %s', (order_id,))
            order_row = cursor.fetchone()
            if not order_row:
                logger.warning(f"Замовлення #{order_id} не знайдено")
                return None
            
            order = dict(order_row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            
            cursor.execute('SELECT * FROM order_items WHERE order_id = %s', (order_id,))
            items = cursor.fetchall()
            
            order_items = []
            for item in items:
                item_dict = dict(item)
                item_dict['created_at'] = format_kyiv_time(item_dict.get('created_at'))
                order_items.append(item_dict)
            
            order['items'] = order_items
            order['order_type'] = 'regular'
        else:
            cursor.execute('SELECT * FROM quick_orders WHERE id = %s', (order_id,))
            order_row = cursor.fetchone()
            if not order_row:
                logger.warning(f"Швидке замовлення #{order_id} не знайдено")
                return None
            
            order = dict(order_row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['order_id'] = order['id']
            order['order_type'] = 'quick'
            order['items'] = []
            order['total'] = safe_get(order, 'total', 0)
        
        return order
    except Exception as e:
        logger.error(f"Помилка отримання замовлення: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

async def notify_customer_about_status(user_id: int, order_id: int, status: str):
    """Сповіщає клієнта про зміну статусу замовлення"""
    logger.debug(f"Виклик notify_customer_about_status(user_id={user_id}, order_id={order_id}, status={status})")
    try:
        status_messages = {
            "підтверджено": "✅ Ваше замовлення підтверджено! Ми розпочали його обробку.",
            "упаковано": "📦 Ваше замовлення упаковано та готове до відправки!",
            "відправлено": "🚚 Ваше замовлення відправлено! Очікуйте на повідомлення про прибуття.",
            "прибуло": "📍 Ваше замовлення прибуло у відділення Нової Пошти! Не забудьте отримати його.",
            "скасовано": "❌ На жаль, ваше замовлення було скасовано. Зв'яжіться з нами для деталей."
        }
        
        message = status_messages.get(status, f"📊 Статус вашого замовлення змінено на: {status}")
        
        main_bot = Bot(token=MAIN_BOT_TOKEN)
        
        await main_bot.send_message(
            chat_id=user_id,
            text=f"<b>Замовлення №{order_id}</b>\n\n{message}",
            parse_mode='HTML'
        )
        logger.info(f"✅ Сповіщення про статус #{order_id} відправлено клієнту {user_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка відправки сповіщення клієнту {user_id}: {e}")
        return False

def get_all_messages(limit: int = 50, offset: int = 0):
    """Отримує всі повідомлення"""
    logger.debug(f"Виклик get_all_messages(limit={limit}, offset={offset})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM messages 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
        ''', (limit, offset))
        rows = cursor.fetchall()
        
        messages = []
        for row in rows:
            msg = dict(row)
            msg['created_at'] = format_kyiv_time(msg.get('created_at'))
            messages.append(msg)
        
        return messages
    except Exception as e:
        logger.error(f"Помилка отримання повідомлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_message_by_id(message_id: int):
    """Отримує повідомлення за ID"""
    logger.debug(f"Виклик get_message_by_id(message_id={message_id})")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM messages WHERE id = %s', (message_id,))
        row = cursor.fetchone()
        if row:
            msg = dict(row)
            msg['created_at'] = format_kyiv_time(msg.get('created_at'))
            return msg
        return None
    except Exception as e:
        logger.error(f"Помилка отримання повідомлення: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_recent_messages(hours: int = 24, min_count: int = 5):
    """Отримує останні повідомлення"""
    logger.debug(f"Виклик get_recent_messages(hours={hours}, min_count={min_count})")
    all_messages = get_all_messages(limit=100)
    
    kyiv_now = get_kyiv_time()
    time_limit = kyiv_now - timedelta(hours=hours)
    
    recent_messages = []
    for msg in all_messages:
        try:
            msg_time_str = msg.get('created_at', '')
            if not msg_time_str:
                continue
            msg_time = datetime.strptime(str(msg_time_str)[:19], '%Y-%m-%d %H:%M:%S')
            if KYIV_TZ:
                try:
                    msg_time = KYIV_TZ.localize(msg_time)
                except:
                    pass
            if msg_time >= time_limit:
                recent_messages.append(msg)
        except:
            continue
    
    if len(recent_messages) < min_count:
        additional = all_messages[:min_count]
        for msg in additional:
            if msg not in recent_messages:
                recent_messages.append(msg)
    
    return recent_messages[:min_count]

def get_more_messages(user_id: int, count: int = 5):
    """Отримує наступні повідомлення для пагінації"""
    logger.debug(f"Виклик get_more_messages(user_id={user_id}, count={count})")
    if user_id not in messages_offset:
        messages_offset[user_id] = 0
    
    offset = messages_offset[user_id]
    messages = get_all_messages(limit=count, offset=offset)
    messages_offset[user_id] = offset + len(messages)
    
    return messages

def format_message_text(msg: dict) -> str:
    """Форматує текст повідомлення для відображення"""
    text = f"💬 <b>Повідомлення #{msg['id']}</b>\n\n"
    text += f"👤 <b>Клієнт:</b> {msg['user_name']}\n"
    text += f"📱 <b>Username:</b> @{msg['username']}\n"
    text += f"🆔 <b>ID:</b> {msg['user_id']}\n"
    text += f"📅 <b>Час:</b> {msg['created_at'][:16]}\n"
    text += f"📝 <b>Тип:</b> {msg['message_type']}\n"
    text += f"💬 <b>Текст:</b> {msg['text']}\n"
    return text

def get_messages_by_user(user_id: int):
    """Отримує повідомлення користувача"""
    logger.debug(f"Виклик get_messages_by_user(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM messages 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        
        messages = []
        for row in rows:
            msg = dict(row)
            msg['created_at'] = format_kyiv_time(msg.get('created_at'))
            messages.append(msg)
        
        return messages
    except Exception as e:
        logger.error(f"Помилка отримання повідомлень користувача: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def format_messages_text(messages: list) -> str:
    """Форматує список повідомлень для відображення"""
    if not messages:
        return "💬 Повідомлень поки немає"
    
    text = "💬 <b>ОСТАННІ ПОВІДОМЛЕННЯ</b>\n\n"
    for i, msg in enumerate(messages[:20], 1):
        text += f"<b>{i}. {msg['user_name']}</b> (@{msg['username']})\n"
        text += f"📅 {msg['created_at'][:16]}\n"
        text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
        text += f"🆔 ID: {msg['user_id']}\n"
        text += f"📋 Тип: {msg['message_type']}\n"
        text += f"{'─'*40}\n"
    
    if len(messages) > 20:
        text += f"... та ще {len(messages) - 20} повідомлень"
    
    return text

def generate_messages_file(messages: list) -> bytes:
    """Генерує файл з повідомленнями"""
    output = StringIO()
    output.write("ПОВІДОМЛЕННЯ ВІД КОРИСТУВАЧІВ\n")
    output.write("=" * 80 + "\n")
    output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"Всього повідомлень: {len(messages)}\n")
    output.write("=" * 80 + "\n\n")
    
    for i, msg in enumerate(messages, 1):
        output.write(f"{i}. {msg['user_name']} (@{msg['username']})\n")
        output.write(f"ID: {msg['user_id']}\n")
        output.write(f"Дата: {msg['created_at']}\n")
        output.write(f"Тип: {msg['message_type']}\n")
        output.write(f"Текст: {msg['text']}\n")
        output.write("-" * 40 + "\n")
    
    return output.getvalue().encode('utf-8')

def get_all_users():
    """Отримує всіх користувачів"""
    logger.debug("Виклик get_all_users()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM users 
            ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()
        
        users = []
        for row in rows:
            user = dict(row)
            user['created_at'] = format_kyiv_time(user.get('created_at'))
            users.append(user)
        
        return users
    except Exception as e:
        logger.error(f"Помилка отримання користувачів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_by_phone(phone: str):
    """Шукає користувача за телефоном"""
    logger.debug(f"Виклик get_user_by_phone(phone={phone})")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT user_id, user_name, username FROM orders 
            WHERE phone LIKE %s 
            ORDER BY created_at DESC LIMIT 1
        ''', (f"%{phone}%",))
        order_user = cursor.fetchone()
        
        if order_user:
            user_id = order_user['user_id']
            cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
            user_row = cursor.fetchone()
            if user_row:
                user = dict(user_row)
                user['created_at'] = format_kyiv_time(user.get('created_at'))
                return user
        
        return None
    except Exception as e:
        logger.error(f"Помилка отримання користувача за телефоном: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_user_by_id(user_id: int):
    """Отримує користувача за ID"""
    logger.debug(f"Виклик get_user_by_id(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
        row = cursor.fetchone()
        if row:
            user = dict(row)
            user['created_at'] = format_kyiv_time(user.get('created_at'))
            return user
        return None
    except Exception as e:
        logger.error(f"Помилка отримання користувача: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_user_orders(user_id: int):
    """Отримує замовлення користувача"""
    logger.debug(f"Виклик get_user_orders(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT *, 'regular' as order_type FROM orders 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            
            cursor.execute('''
                SELECT * FROM order_items 
                WHERE order_id = %s
            ''', (order['order_id'],))
            items = cursor.fetchall()
            
            order_items = []
            for item in items:
                item_dict = dict(item)
                item_dict['created_at'] = format_kyiv_time(item_dict.get('created_at'))
                order_items.append(item_dict)
            
            order['items'] = order_items
            order['display_id'] = order['order_id']
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень користувача: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_phones(user_id: int) -> list:
    """Отримує телефони користувача"""
    logger.debug(f"Виклик get_user_phones(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        phones = []
        
        cursor.execute('''
            SELECT DISTINCT phone FROM orders 
            WHERE user_id = %s AND phone IS NOT NULL AND phone != ''
        ''', (user_id,))
        order_phones = cursor.fetchall()
        for row in order_phones:
            if row['phone'] and row['phone'] not in phones:
                phones.append(row['phone'])
        
        cursor.execute('''
            SELECT DISTINCT phone FROM quick_orders 
            WHERE user_id = %s AND phone IS NOT NULL AND phone != ''
        ''', (user_id,))
        quick_phones = cursor.fetchall()
        for row in quick_phones:
            if row['phone'] and row['phone'] not in phones:
                phones.append(row['phone'])
        
        return phones
    except Exception as e:
        logger.error(f"Помилка отримання телефонів користувача: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_messages(user_id: int):
    """Отримує повідомлення користувача"""
    logger.debug(f"Виклик get_user_messages(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM messages 
            WHERE user_id = %s 
            ORDER BY created_at DESC LIMIT 10
        ''', (user_id,))
        rows = cursor.fetchall()
        
        messages = []
        for row in rows:
            msg = dict(row)
            msg['created_at'] = format_kyiv_time(msg.get('created_at'))
            messages.append(msg)
        
        return messages
    except Exception as e:
        logger.error(f"Помилка отримання повідомлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_quick_orders(user_id: int):
    """Отримує швидкі замовлення користувача"""
    logger.debug(f"Виклик get_user_quick_orders(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM quick_orders 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['order_id'] = order['id']
            order['total'] = safe_get(order, 'total', 0)
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання швидких замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_customer_segment(user_data: dict, orders: list) -> str:
    """Визначає сегмент клієнта"""
    if not orders:
        return "🆕 Новий клієнт (без замовлень)"
    
    total_orders = len(orders)
    total_spent = sum(order.get('total', 0) for order in orders)
    
    if orders:
        last_order = max(orders, key=lambda x: x.get('created_at', ''))
        last_order_date_str = last_order.get('created_at', '')
        if last_order_date_str:
            try:
                last_order_date = datetime.strptime(str(last_order_date_str)[:19], '%Y-%m-%d %H:%M:%S')
                if KYIV_TZ:
                    try:
                        last_order_date = KYIV_TZ.localize(last_order_date)
                    except:
                        pass
                days_since_last = (get_kyiv_time() - last_order_date).days
            except:
                days_since_last = 999
        else:
            days_since_last = 999
    else:
        days_since_last = 999
    
    if total_orders >= 5 and total_spent >= 5000:
        return "👑 VIP клієнт"
    elif total_orders >= 3:
        return "⭐ Постійний клієнт"
    elif days_since_last > 90:
        return "💤 Неактивний клієнт"
    elif total_orders == 1:
        return "🆕 Новий клієнт (1 замовлення)"
    else:
        return "📊 Активний клієнт"

def get_all_products():
    """Отримує всі товари з усіма полями"""
    logger.debug("Виклик get_all_products()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, name, price, category, description, unit, image, details, benefits, usage, created_at FROM products ORDER BY id')
        rows = cursor.fetchall()
        
        products = []
        for row in rows:
            product = dict(row)
            if product.get('created_at'):
                product['created_at'] = format_kyiv_time(product.get('created_at'))
            products.append(product)
        logger.debug(f"Отримано {len(products)} товарів")
        return products
    except Exception as e:
        logger.error(f"Помилка отримання товарів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_product_by_id(product_id: int):
    """Отримує товар за ID"""
    logger.debug(f"Виклик get_product_by_id() з ID: {product_id}")
    products = get_all_products()
    for product in products:
        if product["id"] == product_id:
            logger.debug(f"✅ Знайдено товар: {product['name']}")
            return product
    logger.warning(f"❌ Товар з ID {product_id} не знайдено")
    return None

def update_product(product_id: int, **kwargs):
    """Оновлює товар"""
    logger.debug(f"Виклик update_product() для ID: {product_id} з даними: {kwargs.keys()}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f"{key} = %s")
                values.append(value)
            else:
                fields.append(f"{key} = NULL")
        
        if not fields:
            logger.warning(f"Спроба оновити товар #{product_id} без даних")
            return False
        
        values.append(product_id)
        query = f"UPDATE products SET {', '.join(fields)} WHERE id = %s"
        cursor.execute(query, values)
        conn.commit()
        logger.info(f"✅ Товар #{product_id} оновлено")
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, details: str, benefits: str = None, usage: str = None):
    """Додає новий товар"""
    logger.info(f"📦 Спроба додати товар: {name}, ціна: {price}, категорія: {category}")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Не вдалося підключитись до БД")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, details, benefits, usage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, price, category, description, unit, details, benefits, usage))
        
        result = cursor.fetchone()
        product_id = result['id'] if result else None
        conn.commit()
        
        logger.info(f"✅ Товар додано з ID: {product_id}")
        return product_id
    except Exception as e:
        logger.error(f"❌ Помилка додавання товару: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def delete_product(product_id: int):
    """Видаляє товар"""
    logger.debug(f"Виклик delete_product() для ID: {product_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM products WHERE id = %s', (product_id,))
        conn.commit()
        logger.info(f"✅ Товар #{product_id} видалено")
        return True
    except Exception as e:
        logger.error(f"Помилка видалення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_all_admins():
    """Отримує всіх адмінів"""
    logger.debug("Виклик get_all_admins()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, username, added_by, added_at FROM admins')
        rows = cursor.fetchall()
        admins = []
        for row in rows:
            admin = dict(row)
            if admin.get('added_at'):
                admin['added_at'] = format_kyiv_time(admin.get('added_at'))
            admins.append(admin)
        return admins
    except Exception as e:
        logger.error(f"Помилка отримання адмінів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def add_admin(user_id: int, username: str = "", added_by: int = 0):
    """Додає адміна"""
    logger.debug(f"Виклик add_admin(user_id={user_id}, username={username}, added_by={added_by})")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admins (user_id, username, added_by)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                added_by = EXCLUDED.added_by
        ''', (user_id, username, added_by))
        conn.commit()
        logger.info(f"✅ Адмін {user_id} доданий")
        return True
    except Exception as e:
        logger.error(f"Помилка додавання адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def remove_admin(user_id: int):
    """Видаляє адміна"""
    logger.debug(f"Виклик remove_admin(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM admins WHERE user_id = %s', (user_id,))
        conn.commit()
        logger.info(f"✅ Адмін {user_id} видалений")
        return True
    except Exception as e:
        logger.error(f"Помилка видалення адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def is_admin(user_id: int) -> bool:
    """Перевіряє чи є користувач адміністратором в БД"""
    logger.debug(f"Виклик is_admin(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = %s', (user_id,))
        result = cursor.fetchone()
        count = result['count'] if result else 0
        return count > 0
    except Exception as e:
        logger.error(f"Помилка перевірки адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_statistics():
    """Отримує статистику"""
    logger.debug("Виклик get_statistics()")
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        regular_orders = cursor.fetchone()['count'] or 0
        
        cursor.execute("SELECT COUNT(*) FROM quick_orders")
        quick_orders_count = cursor.fetchone()['count'] or 0
        
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()['count'] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()['count'] or 0
        
        cursor.execute("SELECT COALESCE(SUM(total), 0) FROM orders")
        regular_revenue = cursor.fetchone()['coalesce'] or 0
        
        cursor.execute("SELECT COALESCE(SUM(total), 0) FROM quick_orders")
        quick_revenue = cursor.fetchone()['coalesce'] or 0
        
        total_orders = regular_orders + quick_orders_count
        total_revenue = regular_revenue + quick_revenue
        
        avg_check = total_revenue / total_orders if total_orders > 0 else 0
        
        cursor.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
        rows = cursor.fetchall()
        orders_by_status = {row['status']: row['count'] for row in rows}
        
        cursor.execute("SELECT status, COUNT(*) FROM quick_orders GROUP BY status")
        quick_rows = cursor.fetchall()
        for row in quick_rows:
            status = row['status']
            if status in orders_by_status:
                orders_by_status[status] += row['count']
            else:
                orders_by_status[status] = row['count']
        
        cursor.execute('''
            SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(total), 0) FROM orders 
            WHERE created_at >= NOW() - INTERVAL '30 days'
        ''')
        last_30_days_regular = cursor.fetchone()
        
        cursor.execute('''
            SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(total), 0) FROM quick_orders 
            WHERE created_at >= NOW() - INTERVAL '30 days'
        ''')
        last_30_days_quick = cursor.fetchone()
        
        last_30_days_count = (last_30_days_regular['coalesce'] or 0) + (last_30_days_quick['coalesce'] or 0)
        last_30_days_sum = (last_30_days_regular['coalesce_2'] or 0) + (last_30_days_quick['coalesce_2'] or 0)
        
        users = get_all_users()
        segments = {
            "vip": 0,
            "regular": 0,
            "new": 0,
            "inactive": 0,
            "active": 0
        }
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            quick_orders = get_user_quick_orders(user['user_id'])
            all_orders = orders + quick_orders
            segment = get_customer_segment(user, all_orders)
            if "VIP" in segment:
                segments["vip"] += 1
            elif "Постійний" in segment:
                segments["regular"] += 1
            elif "Неактивний" in segment:
                segments["inactive"] += 1
            elif "Новий" in segment:
                segments["new"] += 1
            else:
                segments["active"] += 1
        
        return {
            "total_orders": total_orders,
            "total_users": total_users,
            "total_quick_orders": quick_orders_count,
            "total_messages": total_messages,
            "total_revenue": total_revenue,
            "avg_check": avg_check,
            "orders_by_status": orders_by_status,
            "last_30_days_orders": last_30_days_count,
            "last_30_days_revenue": last_30_days_sum,
            "segments": segments
        }
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        logger.error(traceback.format_exc())
        return {}
    finally:
        conn.close()

# ========== ФУНКЦІЇ ДЛЯ ГЕНЕРАЦІЇ ЗВІТІВ ==========

def generate_orders_report(orders: list, format: str = "txt"):
    """Генерує звіт по замовленнях"""
    logger.debug(f"Генерація звіту по замовленнях, формат: {format}")
    if format == "txt":
        output = StringIO()
        output.write("ЗВІТ ПО ЗАМОВЛЕННЯХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього замовлень: {len(orders)}\n")
        output.write("=" * 80 + "\n\n")
        
        for order in orders:
            order_id = order.get('order_id', order.get('id', 'Н/Д'))
            output.write(f"Номер: {order_id}\n")
            output.write(f"Дата: {order['created_at']}\n")
            output.write(f"Клієнт: {order.get('user_name', 'Н/Д')}\n")
            output.write(f"Телефон: {order.get('phone', 'Н/Д')}\n")
            output.write(f"Username: @{order.get('username', 'Н/Д')}\n")
            output.write(f"Сума: {order.get('total', 0):.2f} грн\n")
            output.write(f"Статус: {order.get('status', 'нове')}\n")
            output.write(f"Тип: {order.get('order_type', 'regular')}\n")
            if order.get('order_type') == 'quick' and order.get('message'):
                output.write(f"Повідомлення: {order.get('message')}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Номер', 'Дата', 'Клієнт', 'Телефон', 'Username', 'Сума', 'Статус', 'Тип', 'Повідомлення'])
        
        for order in orders:
            order_id = order.get('order_id', order.get('id', 'Н/Д'))
            writer.writerow([
                order_id,
                order['created_at'],
                order.get('user_name', 'Н/Д'),
                order.get('phone', 'Н/Д'),
                order.get('username', 'Н/Д'),
                f"{order.get('total', 0):.2f}",
                order.get('status', 'нове'),
                order.get('order_type', 'regular'),
                order.get('message', '')
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_users_report(users: list) -> bytes:
    """Генерує звіт по користувачах"""
    logger.debug(f"Генерація звіту по користувачах, кількість: {len(users)}")
    output = StringIO()
    output.write("ЗВІТ ПО КОРИСТУВАЧАХ\n")
    output.write("=" * 100 + "\n")
    output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"Всього користувачів: {len(users)}\n")
    output.write("=" * 100 + "\n\n")
    
    for user in users:
        user_id = user['user_id']
        orders = get_user_orders(user_id)
        quick_orders = get_user_quick_orders(user_id)
        messages = get_user_messages(user_id)
        all_orders = orders + quick_orders
        phones = get_user_phones(user_id)
        
        segment = get_customer_segment(user, all_orders)
        
        output.write(f"ID: {user_id}\n")
        output.write(f"Ім'я: {user['first_name']} {user['last_name']}\n")
        output.write(f"Username: @{user['username']}\n")
        output.write(f"Дата реєстрації: {user['created_at'][:16]}\n")
        output.write(f"Сегмент: {segment}\n\n")
        
        if phones:
            output.write("📞 ТЕЛЕФОНИ:\n")
            for i, phone in enumerate(phones, 1):
                output.write(f"  {i}. {phone}\n")
            output.write("\n")
        
        output.write("📦 ЗАМОВЛЕННЯ:\n")
        output.write(f"  Всього замовлень: {len(all_orders)}\n")
        
        if all_orders:
            total_spent = sum(o.get('total', 0) for o in orders)
            output.write(f"  Загальна сума: {total_spent:.2f} грн\n")
            if orders:
                output.write(f"  Середній чек: {total_spent/len(orders):.2f} грн\n")
            output.write("\n")
            
            output.write("  Останні замовлення:\n")
            for i, order in enumerate(all_orders[:3], 1):
                order_id = order.get('order_id', order.get('id', 'Н/Д'))
                order_type = "⚡" if order.get('order_type') == 'quick' else "📦"
                created_at = order.get('created_at', '')[:16]
                status = order.get('status', 'нове')
                total = order.get('total', 0)
                phone = order.get('phone', '')
                output.write(f"    {i}. {order_type} №{order_id} | {created_at} | {total:.2f} грн | {status}\n")
                if phone:
                    output.write(f"       Телефон: {phone}\n")
                if order.get('order_type') == 'quick' and order.get('message'):
                    output.write(f"       Повідомлення: {order.get('message')[:100]}\n")
                elif order.get('order_type') == 'regular' and order.get('items'):
                    output.write(f"       Товари:\n")
                    for item in order.get('items', [])[:2]:
                        output.write(f"         • {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} грн\n")
                    if len(order.get('items', [])) > 2:
                        output.write(f"         ... та ще {len(order.get('items', [])) - 2} товарів\n")
        else:
            output.write("  Замовлень немає\n")
        
        if messages:
            output.write(f"\n💬 ПОВІДОМЛЕННЯ: {len(messages)}\n")
            output.write("  Останні повідомлення:\n")
            for i, msg in enumerate(messages[:3], 1):
                created_at = msg.get('created_at', '')[:16]
                text = msg.get('text', '')
                output.write(f"    {i}. {created_at}: {text[:100]}{'...' if len(text) > 100 else ''}\n")
        
        output.write("-" * 100 + "\n\n")
    
    return output.getvalue().encode('utf-8')

def generate_quick_orders_report(orders: list, format: str = "txt"):
    """Генерує звіт по швидких замовленнях"""
    logger.debug(f"Генерація звіту по швидких замовленнях, формат: {format}")
    if format == "txt":
        output = StringIO()
        output.write("ЗВІТ ПО ШВИДКИХ ЗАМОВЛЕННЯХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього замовлень: {len(orders)}\n")
        output.write("=" * 80 + "\n\n")
        
        for order in orders:
            output.write(f"Номер: {order['id']}\n")
            output.write(f"Дата: {order['created_at']}\n")
            output.write(f"Клієнт: {order['user_name']}\n")
            output.write(f"Телефон: {order['phone']}\n")
            output.write(f"Username: @{order['username']}\n")
            output.write(f"Продукт: {order['product_name']}\n")
            output.write(f"Спосіб зв'язку: {order['contact_method']}\n")
            if order.get('message'):
                output.write(f"Повідомлення: {order['message']}\n")
            output.write(f"Статус: {order['status']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Номер', 'Дата', 'Клієнт', 'Телефон', 'Username', 'Продукт', 'Спосіб зв`язку', 'Повідомлення', 'Статус'])
        
        for order in orders:
            writer.writerow([
                order['id'],
                order['created_at'],
                order['user_name'],
                order['phone'],
                order['username'],
                order['product_name'],
                order['contact_method'],
                order.get('message', ''),
                order['status']
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_stats_report(stats: dict, format: str = "txt"):
    """Генерує звіт по статистиці"""
    logger.debug(f"Генерація звіту по статистиці, формат: {format}")
    if format == "txt":
        output = StringIO()
        output.write("СТАТИСТИКА\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write("=" * 80 + "\n\n")
        
        output.write(f"📋 Замовлень: {stats.get('total_orders', 0)}\n")
        output.write(f"💰 Виручка: {stats.get('total_revenue', 0):.2f} грн\n")
        output.write(f"💳 Середній чек: {stats.get('avg_check', 0):.2f} грн\n")
        output.write(f"👥 Клієнтів: {stats.get('total_users', 0)}\n")
        output.write(f"⚡ Швидких замовлень: {stats.get('total_quick_orders', 0)}\n")
        output.write(f"💬 Повідомлень: {stats.get('total_messages', 0)}\n\n")
        
        output.write("📊 Замовлення за останні 30 днів:\n")
        output.write(f"   Кількість: {stats.get('last_30_days_orders', 0)}\n")
        output.write(f"   Сума: {stats.get('last_30_days_revenue', 0):.2f} грн\n\n")
        
        output.write("📊 Статуси замовлень:\n")
        for status, count in stats.get('orders_by_status', {}).items():
            output.write(f"   • {status}: {count}\n")
        
        output.write("\n👥 Сегментація клієнтів:\n")
        segments = stats.get('segments', {})
        output.write(f"   👑 VIP: {segments.get('vip', 0)}\n")
        output.write(f"   ⭐ Постійні: {segments.get('regular', 0)}\n")
        output.write(f"   🆕 Нові: {segments.get('new', 0)}\n")
        output.write(f"   📊 Активні: {segments.get('active', 0)}\n")
        output.write(f"   💤 Неактивні: {segments.get('inactive', 0)}\n")
        
        return output.getvalue().encode('utf-8')

def generate_messages_report(messages: list, format: str = "txt"):
    """Генерує звіт по повідомленнях"""
    logger.debug(f"Генерація звіту по повідомленнях, формат: {format}")
    if format == "txt":
        output = StringIO()
        output.write("ЗВІТ ПО ПОВІДОМЛЕННЯХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього повідомлень: {len(messages)}\n")
        output.write("=" * 80 + "\n\n")
        
        for msg in messages:
            output.write(f"ID: {msg['id']}\n")
            output.write(f"User ID: {msg['user_id']}\n")
            output.write(f"Ім'я: {msg['user_name']}\n")
            output.write(f"Username: @{msg['username']}\n")
            output.write(f"Дата: {msg['created_at']}\n")
            output.write(f"Тип: {msg['message_type']}\n")
            output.write(f"Текст: {msg['text']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID Повідомлення', 'User ID', 'Імя', 'Username', 'Дата', 'Тип', 'Текст'])
        
        for msg in messages:
            writer.writerow([
                msg['id'],
                msg['user_id'],
                msg['user_name'],
                msg['username'],
                msg['created_at'],
                msg['message_type'],
                msg['text']
            ])
        
        return output.getvalue().encode('utf-8-sig')

# ========== ФУНКЦІЇ ДЛЯ СТВОРЕННЯ КЛАВІАТУР ==========

def create_inline_keyboard(buttons: List[List[Dict]]) -> InlineKeyboardMarkup:
    """Створює Inline клавіатуру"""
    keyboard = []
    for row in buttons:
        keyboard_row = []
        for button in row:
            keyboard_row.append(
                InlineKeyboardButton(
                    text=button.get("text", ""),
                    callback_data=button.get("callback_data", "")
                )
            )
        keyboard.append(keyboard_row)
    return InlineKeyboardMarkup(keyboard)

def get_main_menu():
    """Головне меню адмін-панелі"""
    keyboard = [
        [{"text": "📦 Товари", "callback_data": "admin_products"}],
        [{"text": "📋 Замовлення", "callback_data": "admin_orders"}],
        [{"text": "👥 Клієнти", "callback_data": "admin_customers"}],
        [{"text": "💬 Повідомлення", "callback_data": "admin_messages"}],
        [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
        [{"text": "📁 Звіти", "callback_data": "admin_reports"}],
        [{"text": "📢 Розсилки", "callback_data": "admin_broadcast"}],
        [{"text": "👑 Адміни", "callback_data": "admin_manage_admins"}],
        [{"text": "🔄 Скинути замовлення", "callback_data": "admin_reset_orders"}],
        [{"text": "⚙️ Налаштування", "callback_data": "admin_settings"}],
        [{"text": "🏢 Редагувати 'Про компанію'", "callback_data": "admin_edit_company"}],
        [{"text": "👋 Редагувати вітання", "callback_data": "admin_edit_welcome"}],
        [{"text": "❓ Редагувати FAQ", "callback_data": "admin_faq_edit"}],
        [{"text": "🔐 Вийти", "callback_data": "admin_logout"}]
    ]
    return create_inline_keyboard(keyboard)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    """Клавіатура з кнопкою Назад"""
    buttons = [[{"text": "🔙 Назад", "callback_data": f"back_to_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu():
    """Меню керування товарами"""
    keyboard = [
        [{"text": "📋 Список товарів", "callback_data": "admin_product_list"}],
        [{"text": "➕ Додати товар", "callback_data": "admin_product_add"}],
        [{"text": "✏️ Редагувати товар", "callback_data": "admin_product_edit"}],
        [{"text": "🗑 Видалити товар", "callback_data": "admin_product_delete"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_menu():
    """Меню керування замовленнями"""
    keyboard = [
        [{"text": "📋 Останні замовлення", "callback_data": "admin_order_recent"}],
        [{"text": "📋 Всі замовлення", "callback_data": "admin_order_all"}],
        [{"text": "🆕 Нові замовлення", "callback_data": "admin_order_new"}],
        [{"text": "⚡ Швидкі замовлення", "callback_data": "admin_order_quick"}],
        [{"text": "📞 Пошук за телефоном", "callback_data": "admin_order_by_phone"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customers_menu():
    """Меню керування клієнтами"""
    keyboard = [
        [{"text": "📋 Всі клієнти", "callback_data": "admin_customers_all"}],
        [{"text": "🔍 Пошук за телефоном", "callback_data": "admin_customer_search"}],
        [{"text": "👑 VIP клієнти", "callback_data": "admin_customers_vip"}],
        [{"text": "⭐ Постійні клієнти", "callback_data": "admin_customers_regular"}],
        [{"text": "🆕 Нові клієнти", "callback_data": "admin_customers_new"}],
        [{"text": "💤 Неактивні клієнти", "callback_data": "admin_customers_inactive"}],
        [{"text": "📁 Вивантажити клієнтів", "callback_data": "export_customers"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_messages_menu():
    """Меню керування повідомленнями"""
    keyboard = [
        [{"text": "📋 Останні повідомлення", "callback_data": "admin_messages_recent"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "admin_messages_all"}],
        [{"text": "📁 Всі повідомлення файлом", "callback_data": "messages_all_file"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_menu():
    """Меню розсилок"""
    keyboard = [
        [{"text": "📢 Всім клієнтам", "callback_data": "broadcast_all"}],
        [{"text": "👑 VIP клієнтам", "callback_data": "broadcast_vip"}],
        [{"text": "⭐ Постійним клієнтам", "callback_data": "broadcast_regular"}],
        [{"text": "🆕 Новим клієнтам", "callback_data": "broadcast_new"}],
        [{"text": "💤 Неактивним клієнтам", "callback_data": "broadcast_inactive"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_input_back_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура для повернення з розсилки"""
    buttons = [[{"text": "🔙 Назад", "callback_data": "back_to_broadcast"}]]
    return create_inline_keyboard(buttons)

def get_reports_menu():
    """Меню звітів"""
    keyboard = [
        [{"text": "📦 Замовлення (TXT)", "callback_data": "report_orders_txt"}],
        [{"text": "📦 Замовлення (CSV)", "callback_data": "report_orders_csv"}],
        [{"text": "👥 Клієнти (TXT)", "callback_data": "report_users_txt"}],
        [{"text": "👥 Клієнти (CSV)", "callback_data": "report_users_csv"}],
        [{"text": "⚡ Швидкі замовлення (TXT)", "callback_data": "report_quick_txt"}],
        [{"text": "⚡ Швидкі замовлення (CSV)", "callback_data": "report_quick_csv"}],
        [{"text": "💬 Повідомлення (TXT)", "callback_data": "report_messages_txt"}],
        [{"text": "💬 Повідомлення (CSV)", "callback_data": "report_messages_csv"}],
        [{"text": "📊 Статистика (TXT)", "callback_data": "report_stats_txt"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_admins_menu():
    """Меню керування адмінами"""
    keyboard = [
        [{"text": "📋 Список адмінів", "callback_data": "admin_list"}],
        [{"text": "➕ Додати адміна", "callback_data": "admin_add"}],
        [{"text": "🗑 Видалити адміна", "callback_data": "admin_remove"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_settings_menu():
    """Меню налаштувань"""
    keyboard = [
        [{"text": "🔑 Змінити пароль", "callback_data": "admin_settings_password"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_company_edit_menu() -> InlineKeyboardMarkup:
    """Меню редагування інформації про компанію"""
    buttons = [
        [{"text": "✏️ Редагувати текст", "callback_data": "company_edit_text"}],
        [{"text": "👁️ Переглянути поточний", "callback_data": "company_view"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

def get_welcome_edit_menu() -> InlineKeyboardMarkup:
    """Меню редагування вітального повідомлення"""
    buttons = [
        [{"text": "✏️ Редагувати вітання", "callback_data": "welcome_edit_text"}],
        [{"text": "👁️ Переглянути поточне", "callback_data": "welcome_view"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_edit_main_menu() -> InlineKeyboardMarkup:
    """Головне меню редагування FAQ"""
    buttons = [
        [{"text": "📋 Список FAQ для редагування", "callback_data": "faq_edit_list"}],
        [{"text": "➕ Додати нове питання", "callback_data": "faq_edit_add"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_edit_list_keyboard(faqs: List[Dict]) -> InlineKeyboardMarkup:
    """Клавіатура зі списком FAQ для редагування"""
    buttons = []
    for faq in faqs:
        short_q = faq['question'][:40] + "..." if len(faq['question']) > 40 else faq['question']
        buttons.append([{"text": f"❓ {short_q}", "callback_data": f"faq_edit_select_{faq['id']}"}])
    buttons.append([{"text": "➕ Додати нове", "callback_data": "faq_edit_add"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_faq_edit_main"}])
    return create_inline_keyboard(buttons)

def get_faq_edit_actions_keyboard(faq_id: int) -> InlineKeyboardMarkup:
    """Клавіатура дій для конкретного FAQ"""
    buttons = [
        [{"text": "✏️ Редагувати питання", "callback_data": f"faq_edit_question_{faq_id}"}],
        [{"text": "✏️ Редагувати відповідь", "callback_data": f"faq_edit_answer_{faq_id}"}],
        [{"text": "⬆️ Перемістити вгору", "callback_data": f"faq_edit_move_up_{faq_id}"}],
        [{"text": "⬇️ Перемістити вниз", "callback_data": f"faq_edit_move_down_{faq_id}"}],
        [{"text": "❌ Видалити", "callback_data": f"faq_edit_delete_{faq_id}"}],
        [{"text": "🔙 Назад до списку", "callback_data": "faq_edit_list"}]
    ]
    return create_inline_keyboard(buttons)

def get_order_actions_menu(order_id: int, order_type: str = 'regular'):
    """Меню дій із замовленням"""
    keyboard = [
        [{"text": "✅ Підтвердити", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "📦 Упаковано", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "🚚 Відправлено", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "📍 Прибуло", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "❌ Скасувати", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "📝 Відповісти", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_orders"}]
    ]
    return create_inline_keyboard(keyboard)

def get_message_actions_menu(message_id: int, user_id: int):
    """Меню дій із повідомленням"""
    keyboard = [
        [{"text": "📝 Відповісти", "callback_data": f"reply_user_{user_id}"}],
        [{"text": "👤 Профіль клієнта", "callback_data": f"customer_view_{user_id}"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "back_to_messages"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_messages"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customer_actions_menu(user_id: int):
    """Меню дій із клієнтом"""
    keyboard = [
        [{"text": "📋 Історія замовлень", "callback_data": f"customer_orders_{user_id}"}],
        [{"text": "💬 Повідомлення", "callback_data": f"customer_messages_{user_id}"}],
        [{"text": "📢 Надіслати повідомлення", "callback_data": f"customer_message_{user_id}"}],
        [{"text": "👑 Зробити адміном", "callback_data": f"customer_make_admin_{user_id}"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_customers"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_status_keyboard(order_id: int, order_type: str = 'regular'):
    """Клавіатура для зміни статусу замовлення"""
    keyboard = [
        [{"text": "✅ Підтвердити", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "📦 Упаковано", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "🚚 Відправлено", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "📍 Прибуло", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "❌ Скасувати", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "📝 Відповісти", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_orders"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_pagination_keyboard(user_id: int, has_more: bool = True):
    """Клавіатура пагінації для замовлень"""
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 замовлень", "callback_data": "admin_order_more"}])
    buttons.append([{"text": "🔍 Детально", "callback_data": "admin_order_details"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_orders"}])
    return create_inline_keyboard(buttons)

def get_messages_pagination_keyboard(user_id: int, has_more: bool = True):
    """Клавіатура пагінації для повідомлень"""
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 повідомлень", "callback_data": "admin_messages_more"}])
    buttons.append([{"text": "🔍 Детально", "callback_data": "admin_messages_details"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_messages"}])
    return create_inline_keyboard(buttons)

def get_product_image_keyboard(product_id: int, has_image: bool = False) -> InlineKeyboardMarkup:
    """Клавіатура для керування фото товару"""
    buttons = []
    buttons.append([{"text": "🌐 Завантажити за URL", "callback_data": f"edit_product_image_url_{product_id}"}])
    buttons.append([{"text": "📷 Завантажити файл", "callback_data": f"edit_product_image_file_{product_id}"}])
    if has_image:
        buttons.append([{"text": "🗑 Видалити фото", "callback_data": f"delete_product_image_{product_id}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": f"back_to_edit_product_{product_id}"}])
    return create_inline_keyboard(buttons)

def get_edit_field_keyboard(product_id: int, field_name: str, has_delete: bool = True) -> InlineKeyboardMarkup:
    """Клавіатура для редагування поля з кнопкою видалення"""
    buttons = []
    if has_delete:
        buttons.append([{"text": "🗑 Видалити поточне значення", "callback_data": f"delete_field_{field_name}_{product_id}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": f"back_to_edit_product_{product_id}"}])
    return create_inline_keyboard(buttons)

# ========== ОБРОБНИКИ КОМАНД ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник команди /start"""
    user = update.effective_user
    user_id = user.id
    
    logger.info(f"👤 Адмін {user_id} (@{user.username}) викликав /start")
    
    if ADMIN_IDS and user_id in ADMIN_IDS:
        logger.info(f"✅ Адмін {user_id} знайдений в ADMIN_IDS")
        admin_sessions[user_id] = {"state": "waiting_password"}
        await update.message.reply_text("🔐 Вхід в адмін-панель Бонелет\n\nБудь ласка, введіть пароль:")
        return
    
    if is_admin(user_id):
        logger.info(f"✅ Адмін {user_id} знайдений в БД")
        admin_sessions[user_id] = {"state": "waiting_password"}
        await update.message.reply_text("🔐 Вхід в адмін-панель Бонелет\n\nБудь ласка, введіть пароль:")
        return
    
    logger.warning(f"❌ Спроба доступу неавторизованого користувача {user_id}")
    await update.message.reply_text("❌ Доступ заборонено\n\nВи не маєте прав адміністратора.")

async def check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перевіряє пароль при вході"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    logger.info(f"🔐 Спроба входу адміна {user_id}")
    
    if user_id not in admin_sessions or admin_sessions[user_id].get("state") != "waiting_password":
        logger.warning(f"Спроба ввести пароль без сесії від {user_id}")
        return
    
    if text == ADMIN_PASSWORD:
        admin_sessions[user_id] = {"state": "authenticated", "authenticated_at": get_kyiv_time().isoformat()}
        last_password_check[user_id] = get_kyiv_time()
        
        logger.info(f"✅ Адмін {user_id} успішно автентифікований, сесія: {admin_sessions[user_id]}")
        
        if not is_admin(user_id):
            add_admin(user_id, user.username or "", user_id)
            logger.info(f"✅ Нового адміна {user_id} додано до БД")
        
        await update.message.reply_text("✅ Пароль прийнято!\n\nЛаскаво прошу до адмін-панелі.", reply_markup=get_main_menu())
    else:
        await update.message.reply_text("❌ Невірний пароль!\n\nСпробуйте ще раз або напишіть /start")
        logger.warning(f"❌ Невірний пароль від адміна {user_id}")
        admin_sessions.pop(user_id, None)
        
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Примусовий вхід в систему"""
    user = update.effective_user
    user_id = user.id
    
    logger.info(f"👤 Адмін {user_id} викликав /login")
    
    # Очищаємо стару сесію
    admin_sessions.pop(user_id, None)
    last_password_check.pop(user_id, None)
    
    admin_sessions[user_id] = {"state": "waiting_password"}
    await update.message.reply_text("🔐 Будь ласка, введіть пароль:")
    
# ========== ОБРОБНИК НАТИСКАНЬ НА КНОПКИ ==========

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник натискань на кнопки"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        user_id = user.id
        data = query.data
        
        logger.info(f"🖱️ Адмін {user_id} натиснув: {data}")
        
        if not is_authenticated(user_id):
            logger.warning(f"❌ Неавтентифікований адмін {user_id} спробував натиснути {data}")
            await query.edit_message_text("❌ Сесія закінчилась\n\nНапишіть /start для повторного входу")
            return
        
        # ============== ОБРОБКА КНОПОК "НАЗАД" ==============
        
        if data.startswith("back_to_"):
            target = data[8:]
            logger.debug(f"Обробка back_to: {target}")
            
            if target == "faq_edit_main":
                await query.edit_message_text("❓ Редагування FAQ\n\nОберіть дію:", reply_markup=get_faq_edit_main_menu())
                return
            elif target.startswith("edit_product_"):
                try:
                    product_id = int(target.split("_")[2])
                    product = get_product_by_id(product_id)
                    if product:
                        admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
                        keyboard = [
                            [{"text": "📝 Назва", "callback_data": f"edit_field_name_{product_id}"}],
                            [{"text": "💰 Ціна", "callback_data": f"edit_field_price_{product_id}"}],
                            [{"text": "📋 Опис", "callback_data": f"edit_field_desc_{product_id}"}],
                            [{"text": "🏷 Категорія", "callback_data": f"edit_field_cat_{product_id}"}],
                            [{"text": "📷 Фото", "callback_data": f"edit_field_image_{product_id}"}],
                            [{"text": "📏 Одиниці", "callback_data": f"edit_field_unit_{product_id}"}],
                            [{"text": "📊 Деталі", "callback_data": f"edit_field_details_{product_id}"}],
                            [{"text": "🌟 Переваги", "callback_data": f"edit_field_benefits_{product_id}"}],
                            [{"text": "💡 Використання", "callback_data": f"edit_field_usage_{product_id}"}],
                            [{"text": "🔙 Назад", "callback_data": "back_to_products"}]
                        ]
                        await query.edit_message_text(
                            f"✏️ Редагування товару #{product_id}\n\n"
                            f"Назва: {product['name']}\n"
                            f"Ціна: {product['price']} грн\n"
                            f"Одиниці: {product['unit']}\n"
                            f"Переваги: {product.get('benefits', 'Не вказано')[:50]}...\n\n"
                            f"Оберіть поле для редагування:",
                            reply_markup=create_inline_keyboard(keyboard)
                        )
                        return
                except Exception as e:
                    logger.error(f"Помилка обробки back_to_edit_product: {e}")
                
                await query.edit_message_text("📦 Керування товарами\n\nОберіть дію:", reply_markup=get_products_menu())
                return
            elif target == "main":
                await query.edit_message_text("🔐 Адмін-панель Бонелет\n\nОберіть розділ:", reply_markup=get_main_menu())
                return
            elif target == "orders":
                await query.edit_message_text("📋 Керування замовленнями\n\nОберіть тип замовлень:", reply_markup=get_orders_menu())
                return
            elif target == "customers":
                await query.edit_message_text("👥 Керування клієнтами\n\nОберіть дію:", reply_markup=get_customers_menu())
                return
            elif target == "messages":
                await query.edit_message_text("💬 Керування повідомленнями\n\nОберіть дію:", reply_markup=get_messages_menu())
                return
            elif target == "broadcast":
                await query.edit_message_text("📢 Розсилка повідомлень\n\nОберіть цільову аудиторію:", reply_markup=get_broadcast_menu())
                return
            elif target == "products":
                await query.edit_message_text("📦 Керування товарами\n\nОберіть дію:", reply_markup=get_products_menu())
                return
            elif target == "company":
                await query.edit_message_text("🏢 Редагування 'Про компанію'\n\nОберіть дію:", reply_markup=get_company_edit_menu())
                return
            elif target == "welcome":
                await query.edit_message_text("👋 Редагування вітального повідомлення\n\nОберіть дію:", reply_markup=get_welcome_edit_menu())
                return
            else:
                await query.edit_message_text("🔐 Адмін-панель Бонелет\n\nОберіть розділ:", reply_markup=get_main_menu())
                return
        
        # ============== ВИХІД ==============
        
        elif data == "admin_logout":
            admin_sessions.pop(user_id, None)
            last_password_check.pop(user_id, None)
            logger.info(f"🔓 Адмін {user_id} вийшов з системи")
            await query.edit_message_text("🔐 Ви вийшли з адмін-панелі\n\nДля повторного входу напишіть /start")
            return
        
        # ============== СКИДАННЯ ЗАМОВЛЕНЬ ==============
        
        elif data == "admin_reset_orders":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Так, видалити всі замовлення", callback_data="confirm_reset_orders")],
                [InlineKeyboardButton("❌ Ні, скасувати", callback_data="back_to_main")]
            ])
            await query.edit_message_text("⚠️ <b>Ви дійсно хочете видалити ВСІ замовлення та повідомлення?</b>\n\nКлієнти та товари залишаться, але всі замовлення та повідомлення будуть безповоротно видалені.", reply_markup=keyboard, parse_mode='HTML')
            return
        
        elif data == "confirm_reset_orders":
            success = await reset_all_orders()
            if success:
                text = "✅ Всі замовлення та повідомлення успішно видалено!"
            else:
                text = "❌ Помилка при видаленні"
            await query.edit_message_text(text, reply_markup=get_main_menu())
            return
        
        # ============== РЕДАГУВАННЯ КОМПАНІЇ ==============
        
        elif data == "admin_edit_company":
            await query.edit_message_text("🏢 Редагування 'Про компанію'\n\nОберіть дію:", reply_markup=get_company_edit_menu())
            return
        
        elif data == "company_view":
            company_text = get_company_info()
            await query.edit_message_text(
                f"🏢 <b>Поточний текст:</b>\n\n{company_text}",
                reply_markup=get_back_keyboard("company"),
                parse_mode='HTML'
            )
            return
        
        elif data == "company_edit_text":
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_company_text"}
            await query.edit_message_text(
                f"✏️ Редагування тексту 'Про компанію'\n\n"
                f"📋 <b>Поточний текст (скопіюйте його):</b>\n\n{get_company_info()}\n\n"
                f"📝 Надішліть новий текст:",
                reply_markup=get_back_keyboard("company"),
                parse_mode='HTML'
            )
            return
        
        # ============== РЕДАГУВАННЯ ВІТАННЯ ==============
        
        elif data == "admin_edit_welcome":
            await query.edit_message_text("👋 Редагування вітального повідомлення\n\nОберіть дію:", reply_markup=get_welcome_edit_menu())
            return
        
        elif data == "welcome_view":
            welcome_text = get_welcome_message()
            await query.edit_message_text(
                f"👋 <b>Поточне вітальне повідомлення:</b>\n\n{welcome_text}",
                reply_markup=get_back_keyboard("welcome"),
                parse_mode='HTML'
            )
            return
        
        elif data == "welcome_edit_text":
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_welcome_text"}
            await query.edit_message_text(
                f"✏️ Редагування вітального повідомлення\n\n"
                f"📋 <b>Поточний текст (скопіюйте його):</b>\n\n{get_welcome_message()}\n\n"
                f"📝 Надішліть новий текст:",
                reply_markup=get_back_keyboard("welcome"),
                parse_mode='HTML'
            )
            return
        
        # ============== РЕДАГУВАННЯ FAQ ==============
        
        elif data == "admin_faq_edit":
            await query.edit_message_text("❓ Редагування FAQ\n\nОберіть дію:", reply_markup=get_faq_edit_main_menu())
            return
        
        elif data == "faq_edit_list":
            faqs = get_all_faqs()
            if not faqs:
                await query.edit_message_text("❓ FAQ порожній. Додайте нове питання.", reply_markup=get_faq_edit_main_menu())
                return
            
            await query.edit_message_text(
                "❓ <b>Виберіть FAQ для редагування:</b>",
                reply_markup=get_faq_edit_list_keyboard(faqs),
                parse_mode='HTML'
            )
            return
        
        elif data == "faq_edit_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "faq_edit_add_question"}
            await query.edit_message_text(
                "➕ Додавання нового FAQ\n\nВведіть <b>питання</b>:",
                reply_markup=get_back_keyboard("faq_edit_main"),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("faq_edit_select_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                admin_sessions[user_id]["current_faq_id"] = faq_id
                
                text = f"❓ <b>FAQ #{faq_id}</b>\n\n"
                text += f"<b>Питання:</b> {faq['question']}\n\n"
                text += f"<b>Відповідь:</b> {faq['answer']}"
                
                await query.edit_message_text(
                    text,
                    reply_markup=get_faq_edit_actions_keyboard(faq_id),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка вибору FAQ: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_question_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                admin_sessions[user_id] = {
                    "state": "authenticated",
                    "action": f"faq_edit_update_question",
                    "faq_id": faq_id
                }
                
                await query.edit_message_text(
                    f"✏️ Редагування питання FAQ #{faq_id}\n\n"
                    f"📋 <b>Поточне питання:</b>\n{faq['question']}\n\n"
                    f"📝 Введіть нове питання:",
                    reply_markup=get_edit_field_keyboard(faq_id, "faq_question", True),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка редагування питання: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_answer_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                admin_sessions[user_id] = {
                    "state": "authenticated",
                    "action": f"faq_edit_update_answer",
                    "faq_id": faq_id
                }
                
                await query.edit_message_text(
                    f"✏️ Редагування відповіді FAQ #{faq_id}\n\n"
                    f"📋 <b>Поточна відповідь:</b>\n{faq['answer']}\n\n"
                    f"📝 Введіть нову відповідь:",
                    reply_markup=get_edit_field_keyboard(faq_id, "faq_answer", True),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка редагування відповіді: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_move_up_"):
            try:
                faq_id = int(data.split("_")[4])
                if move_faq_up(faq_id):
                    await query.answer("✅ Переміщено вгору")
                else:
                    await query.answer("❌ Вже на початку", show_alert=False)
                
                faqs = get_all_faqs()
                await query.edit_message_text(
                    "❓ <b>Виберіть FAQ для редагування:</b>",
                    reply_markup=get_faq_edit_list_keyboard(faqs),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка переміщення вгору: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_move_down_"):
            try:
                faq_id = int(data.split("_")[4])
                if move_faq_down(faq_id):
                    await query.answer("✅ Переміщено вниз")
                else:
                    await query.answer("❌ Вже в кінці", show_alert=False)
                
                faqs = get_all_faqs()
                await query.edit_message_text(
                    "❓ <b>Виберіть FAQ для редагування:</b>",
                    reply_markup=get_faq_edit_list_keyboard(faqs),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка переміщення вниз: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_delete_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Так, видалити", callback_data=f"faq_edit_confirm_delete_{faq_id}")],
                    [InlineKeyboardButton("❌ Ні, скасувати", callback_data=f"faq_edit_select_{faq_id}")]
                ])
                await query.edit_message_text(
                    f"❓ <b>Видалити FAQ?</b>\n\n"
                    f"<b>Питання:</b> {faq['question']}\n\n"
                    f"Ця дія незворотна.",
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка видалення: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_confirm_delete_"):
            try:
                faq_id = int(data.split("_")[4])
                if delete_faq(faq_id):
                    await query.answer("✅ FAQ видалено")
                    faqs = get_all_faqs()
                    if faqs:
                        await query.edit_message_text(
                            "❓ <b>Виберіть FAQ для редагування:</b>",
                            reply_markup=get_faq_edit_list_keyboard(faqs),
                            parse_mode='HTML'
                        )
                    else:
                        await query.edit_message_text("❓ FAQ порожній", reply_markup=get_faq_edit_main_menu())
                else:
                    await query.edit_message_text("❌ Помилка при видаленні", reply_markup=get_back_keyboard("faq_edit_main"))
            except (IndexError, ValueError) as e:
                logger.error(f"Помилка підтвердження видалення: {e}")
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        # ============== КЕРУВАННЯ ТОВАРАМИ ==============
        
        elif data == "admin_products":
            await query.edit_message_text("📦 Керування товарами\n\nОберіть дію:", reply_markup=get_products_menu())
            return
        
        elif data == "admin_product_list":
            products = get_all_products()
            if not products:
                text = "📦 Список товарів\n\nТоварів не знайдено."
            else:
                text = "📦 Список товарів\n\n"
                for p in products:
                    text += f"ID: {p['id']}\nНазва: {p['name']}\nЦіна: {p['price']} грн/{p['unit']}\nКатегорія: {p['category']}\n{'─'*30}\n"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_products"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_product_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_product_name"}
            await query.edit_message_text("➕ Додавання нового товару\n\nВведіть назву товару:", reply_markup=get_back_keyboard("products"))
            return
        
        elif data == "admin_product_edit":
            products = get_all_products()
            if not products:
                await query.edit_message_text("❌ Товарів не знайдено", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([{"text": f"{p['id']}. {p['name'][:30]}", "callback_data": f"edit_product_{p['id']}"}])
            keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_products"}])
            await query.edit_message_text("✏️ Редагування товару\n\nОберіть товар для редагування:", reply_markup=create_inline_keyboard(keyboard))
            return
        
        # ============== ОБРОБНИКИ ДЛЯ РЕДАГУВАННЯ ПОЛІВ ТОВАРУ ==============
        
        elif data.startswith("delete_field_"):
            parts = data.split("_")
            if len(parts) >= 4:
                field_name = parts[2]
                try:
                    product_id = int(parts[3])
                    logger.info(f"🗑 Видалення поля {field_name} для товару {product_id}")
                    
                    product = get_product_by_id(product_id)
                    if not product:
                        await query.edit_message_text("❌ Товар не знайдено", reply_markup=get_products_menu())
                        return
                    
                    update_data = {}
                    field_mapping = {
                        "name": "name",
                        "price": "price",
                        "desc": "description",
                        "cat": "category",
                        "unit": "unit",
                        "details": "details",
                        "benefits": "benefits",
                        "usage": "usage",
                        "image": "image_data"
                    }
                    
                    db_field = field_mapping.get(field_name)
                    if db_field:
                        update_data[db_field] = None
                        
                        if update_product(product_id, **update_data):
                            await query.edit_message_text(
                                f"✅ Поле товару #{product_id} очищено!",
                                reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                            )
                        else:
                            await query.edit_message_text(
                                f"❌ Помилка при очищенні поля",
                                reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                            )
                    else:
                        await query.edit_message_text("❌ Невідоме поле", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
                        
                except ValueError:
                    await query.edit_message_text("❌ Помилка ID", reply_markup=get_products_menu())
            return
        
        elif data.startswith("delete_product_image_"):
            logger.info(f"🔄 Натиснуто кнопку delete_product_image_, data: {data}")
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"✅ Розпарсено product_id: {product_id}")
            except (IndexError, ValueError) as e:
                logger.error(f"❌ Помилка парсингу ID: {e}")
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
            
            product = get_product_by_id(product_id)
            if not product:
                logger.error(f"❌ Товар з ID {product_id} не знайдено в БД")
                await query.edit_message_text(f"❌ Помилка: товар з ID {product_id} не знайдено", reply_markup=get_products_menu())
                return
            
            if update_product(product_id, image_data=None):
                await query.edit_message_text(
                    f"✅ Фото товару #{product_id} видалено!",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
            else:
                await query.edit_message_text(
                    f"❌ Помилка при видаленні фото",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
            return
        
        elif data.startswith("edit_product_image_url_"):
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"✅ Вибір: завантаження фото за URL для товару {product_id}")
                admin_sessions[user_id] = {
                    "state": "authenticated", 
                    "action": "edit_product_image_url", 
                    "product_id": product_id
                }
                await query.edit_message_text(
                    "🌐 Введіть URL зображення:",
                    reply_markup=get_edit_field_keyboard(product_id, "image", True),
                    parse_mode='HTML'
                )
                return
            except (IndexError, ValueError):
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
        
        elif data.startswith("edit_product_image_file_"):
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"✅ Вибір: завантаження файлу фото для товару {product_id}")
                admin_sessions[user_id] = {
                    "state": "authenticated", 
                    "action": "edit_product_image_file", 
                    "product_id": product_id
                }
                await query.edit_message_text(
                    "📷 Надішліть фото:",
                    reply_markup=get_edit_field_keyboard(product_id, "image", True),
                    parse_mode='HTML'
                )
                return
            except (IndexError, ValueError):
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
        
        elif data.startswith("edit_field_"):
            parts = data.split("_")
            if len(parts) < 4:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_products_menu())
                return
            
            field = parts[2]
            try:
                product_id = int(parts[-1])
            except (IndexError, ValueError):
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
            
            product = get_product_by_id(product_id)
            if not product:
                await query.edit_message_text("❌ Товар не знайдено", reply_markup=get_products_menu())
                return
            
            if field == "image":
                has_image = product.get('image_data') is not None
                admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_image", "product_id": product_id}
                await query.edit_message_text(
                    "📷 Виберіть спосіб завантаження фото:",
                    reply_markup=get_product_image_keyboard(product_id, has_image)
                )
                return
            elif field == "unit":
                admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_unit", "product_id": product_id}
                current_value = product.get('unit', 'Не вказано')
                await query.edit_message_text(
                    f"✏️ Редагування одиниці виміру\n\n📋 <b>Поточне значення:</b> {current_value}\n\n📝 Введіть нову одиницю виміру (наприклад: банка, кг, шт, л):",
                    reply_markup=get_edit_field_keyboard(product_id, "unit", True),
                    parse_mode='HTML'
                )
                return
            elif field == "details":
                admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_details", "product_id": product_id}
                current_value = product.get('details', 'Не вказано')
                await query.edit_message_text(
                    f"✏️ Редагування деталей товару\n\n📋 <b>Поточні деталі:</b>\n{current_value}\n\n📝 Введіть нові деталі:",
                    reply_markup=get_edit_field_keyboard(product_id, "details", True),
                    parse_mode='HTML'
                )
                return
            elif field == "benefits":
                admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_benefits", "product_id": product_id}
                current_value = product.get('benefits', 'Не вказано')
                await query.edit_message_text(
                    f"✏️ Редагування переваг товару\n\n📋 <b>Поточні переваги (кожна з нового рядка):</b>\n{current_value}\n\n📝 Введіть нові переваги (кожна з нового рядка):",
                    reply_markup=get_edit_field_keyboard(product_id, "benefits", True),
                    parse_mode='HTML'
                )
                return
            elif field == "usage":
                admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_usage", "product_id": product_id}
                current_value = product.get('usage', 'Не вказано')
                await query.edit_message_text(
                    f"✏️ Редагування інструкції з використання\n\n📋 <b>Поточна інструкція:</b>\n{current_value}\n\n📝 Введіть нову інструкцію з використання:",
                    reply_markup=get_edit_field_keyboard(product_id, "usage", True),
                    parse_mode='HTML'
                )
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_{field}", "product_id": product_id}
            
            field_names = {
                "name": "назву",
                "price": "ціну",
                "desc": "опис",
                "cat": "категорію"
            }
            
            current_values = {
                "name": product.get('name', 'Не вказано'),
                "price": f"{product.get('price', 0)} грн",
                "desc": product.get('description', 'Не вказано')[:50] + '...' if len(product.get('description', '')) > 50 else product.get('description', 'Не вказано'),
                "cat": product.get('category', 'Не вказано')
            }
            
            current = current_values.get(field, 'Не вказано')
            
            await query.edit_message_text(
                f"✏️ Редагування {field_names.get(field, '')}\n\n📋 <b>Поточне значення:</b> {current}\n\n📝 Введіть нову {field_names.get(field, '')}:",
                reply_markup=get_edit_field_keyboard(product_id, field, True),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("edit_product_"):
            logger.info(f"📝 Натиснуто загальний edit_product_ з data: {data}")
            
            if data.count("_") > 2:
                logger.warning(f"⚠️ Пропускаємо складний callback у загальному обробнику: {data}")
                return
            
            try:
                product_id = int(data.split("_")[2])
                logger.info(f"✅ Розпарсено product_id: {product_id}")
            except (IndexError, ValueError):
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
            
            product = get_product_by_id(product_id)
            if not product:
                await query.edit_message_text("❌ Товар не знайдено", reply_markup=get_products_menu())
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
            keyboard = [
                [{"text": "📝 Назва", "callback_data": f"edit_field_name_{product_id}"}],
                [{"text": "💰 Ціна", "callback_data": f"edit_field_price_{product_id}"}],
                [{"text": "📋 Опис", "callback_data": f"edit_field_desc_{product_id}"}],
                [{"text": "🏷 Категорія", "callback_data": f"edit_field_cat_{product_id}"}],
                [{"text": "📷 Фото", "callback_data": f"edit_field_image_{product_id}"}],
                [{"text": "📏 Одиниці", "callback_data": f"edit_field_unit_{product_id}"}],
                [{"text": "📊 Деталі", "callback_data": f"edit_field_details_{product_id}"}],
                [{"text": "🌟 Переваги", "callback_data": f"edit_field_benefits_{product_id}"}],
                [{"text": "💡 Використання", "callback_data": f"edit_field_usage_{product_id}"}],
                [{"text": "🔙 Назад", "callback_data": "back_to_products"}]
            ]
            await query.edit_message_text(
                f"✏️ Редагування товару #{product_id}\n\n"
                f"Назва: {product['name']}\n"
                f"Ціна: {product['price']} грн\n"
                f"Одиниці: {product['unit']}\n"
                f"Переваги: {product.get('benefits', 'Не вказано')[:50]}...\n\n"
                f"Оберіть поле для редагування:",
                reply_markup=create_inline_keyboard(keyboard)
            )
            return
        
        # ============== ВИДАЛЕННЯ ТОВАРУ ==============
        
        elif data == "admin_product_delete":
            products = get_all_products()
            if not products:
                await query.edit_message_text("❌ Товарів не знайдено", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([{"text": f"❌ {p['id']}. {p['name'][:30]}", "callback_data": f"delete_product_{p['id']}"}])
            keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_products"}])
            await query.edit_message_text("🗑 Видалення товару\n\nОберіть товар для видалення:", reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("delete_product_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_products_menu())
                return
            
            try:
                product_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
            
            keyboard = [
                [{"text": "✅ Так, видалити", "callback_data": f"confirm_delete_{product_id}"}],
                [{"text": "❌ Ні, скасувати", "callback_data": "back_to_products"}]
            ]
            await query.edit_message_text(f"🗑 Підтвердження видалення\n\nВи дійсно хочете видалити товар #{product_id}?", reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("confirm_delete_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_products_menu())
                return
            
            try:
                product_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID товару", reply_markup=get_products_menu())
                return
            
            if delete_product(product_id):
                text = "✅ Товар успішно видалено!"
            else:
                text = "❌ Помилка при видаленні товару"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_products"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        # ============== КЕРУВАННЯ ЗАМОВЛЕННЯМИ ==============
        
        elif data == "admin_orders":
            await query.edit_message_text("📋 Керування замовленнями\n\nОберіть тип замовлень:", reply_markup=get_orders_menu())
            return
        
        elif data == "admin_order_recent":
            recent_orders = get_recent_orders(hours=1, min_count=3)
            if not recent_orders:
                text = "📋 Замовлень за останню годину немає.\n\nПоказую останні замовлення:"
                recent_orders = get_all_orders(include_quick=True, limit=3)
            
            if not recent_orders:
                text = "📋 Замовлень не знайдено."
            else:
                text = "📋 <b>ОСТАННІ ЗАМОВЛЕННЯ</b>\n\n"
                for order in recent_orders:
                    text += format_order_text(order) + f"{'─'*40}\n"
            
            all_orders = get_all_orders(include_quick=True, limit=5, offset=0)
            has_more = len(all_orders) >= 5
            
            await query.edit_message_text(text, reply_markup=get_orders_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_order_more":
            more_orders = get_more_orders(user_id, count=5)
            if not more_orders:
                text = "📋 Більше замовлень не знайдено."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("orders"), parse_mode='HTML')
                return
            
            text = "📋 <b>ЩЕ ЗАМОВЛЕННЯ</b>\n\n"
            for order in more_orders:
                text += format_order_text(order) + f"{'─'*40}\n"
            
            next_orders = get_all_orders(include_quick=True, limit=1, offset=orders_offset.get(user_id, 0))
            has_more = len(next_orders) > 0
            
            await query.edit_message_text(text, reply_markup=get_orders_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_order_all":
            orders = get_all_orders(include_quick=True, limit=10)
            if not orders:
                text = "📋 Всі замовлення\n\nЗамовлень не знайдено."
                keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_orders"}]]
                await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
                return
            
            text = f"📋 Всі замовлення\n\nВсього: {len(get_all_orders(include_quick=True))}\n\n"
            for order in orders[:10]:
                text += format_order_text(order) + f"{'─'*40}\n"
            
            if len(get_all_orders(include_quick=True)) > 10:
                text += f"... та ще більше замовлень"
            
            keyboard = [
                [{"text": "🔍 Детально", "callback_data": "admin_order_details"}],
                [{"text": "🔙 Назад", "callback_data": "back_to_orders"}]
            ]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard), parse_mode='HTML')
            return
        
        elif data == "admin_order_details":
            orders = get_all_orders(include_quick=True, limit=20)
            if not orders:
                await query.edit_message_text("❌ Замовлень не знайдено", reply_markup=get_orders_menu())
                return
            keyboard = []
            for order in orders[:20]:
                order_type = order.get('order_type', 'regular')
                type_prefix = "⚡" if order_type == 'quick' else "📦"
                display_id = order.get('order_id', order.get('id', 'Н/Д'))
                customer_name = order.get('user_name', 'Н/Д')
                total = safe_get(order, 'total', 0)
                keyboard.append([{"text": f"{type_prefix} №{display_id} - {customer_name} - {total:.0f} грн", "callback_data": f"order_view_{display_id}_{order_type}"}])
            keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_orders"}])
            await query.edit_message_text("📋 Детальний перегляд замовлень\n\nОберіть замовлення:", reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_order_new":
            orders = get_new_orders()
            if not orders:
                text = "🆕 Нові замовлення\n\nНових замовлень немає."
            else:
                text = f"🆕 Нові замовлення\n\nВсього: {len(orders)}\n\n"
                for order in orders[:10]:
                    text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
                    text += f"Клієнт: {order['user_name']}\n"
                    text += f"Сума: {order.get('total', 0):.2f} грн\n"
                    text += f"Телефон: {order['phone']}\n"
                    text += f"{'─'*30}\n"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_orders"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_order_quick":
            orders = get_quick_orders()
            if not orders:
                text = "⚡ Швидкі замовлення\n\nШвидких замовлень немає."
            else:
                text = f"⚡ Швидкі замовлення\n\nВсього: {len(orders)}\n\n"
                for order in orders[:10]:
                    text += f"⚡ №{order['id']} | {order['created_at'][:16]}\n"
                    text += f"Клієнт: {order['user_name']}\n"
                    text += f"Телефон: {order['phone']}\n"
                    text += f"Продукт: {order['product_name']}\n"
                    if order.get('message'):
                        text += f"💬 {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    text += f"{'─'*30}\n"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_orders"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_order_by_phone":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_orders_by_phone"}
            await query.edit_message_text("📞 Пошук замовлень за телефоном\n\nВведіть номер телефону клієнта:", reply_markup=get_back_keyboard("orders"))
            return
        
        elif data.startswith("order_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            order = get_order_by_id(order_id, order_type)
            if not order:
                await query.edit_message_text("❌ Замовлення не знайдено", reply_markup=get_orders_menu())
                return
            
            text = f"📋 ЗАМОВЛЕННЯ №{order_id}\n\n"
            text += f"📅 Дата: {order['created_at']}\n"
            text += f"👤 Клієнт: {order['user_name']}\n"
            text += f"📞 Телефон: {order['phone']}\n"
            text += f"📱 Username: @{order['username']}\n"
            
            if order_type == 'regular':
                text += f"🏙️ Місто: {order.get('city', 'Н/Д')}\n"
                text += f"🏣 Відділення: {order.get('np_department', 'Н/Д')}\n"
                text += f"{'─'*30}\n"
                text += "📦 Товари:\n"
                for item in order.get('items', []):
                    text += f"  • {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} грн\n"
            else:
                text += f"📦 Продукт: {order.get('product_name', 'Н/Д')}\n"
                text += f"📞 Спосіб зв'язку: {order.get('contact_method', 'Н/Д')}\n"
                if order.get('message'):
                    text += f"💬 Повідомлення: {order['message']}\n"
            
            text += f"{'─'*30}\n"
            text += f"💰 Сума: {order.get('total', 0):.2f} грн\n"
            text += f"📊 Статус: {order.get('status', 'нове')}\n"
            
            await query.edit_message_text(text, reply_markup=get_order_actions_menu(order_id, order_type), parse_mode='HTML')
            return
        
        elif data.startswith("reply_order_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            order = get_order_by_id(order_id, order_type)
            if not order:
                await query.edit_message_text("❌ Замовлення не знайдено", reply_markup=get_orders_menu())
                return
            
            admin_sessions[user_id] = {
                "state": "authenticated", 
                "action": "reply_to_order",
                "order_id": order_id,
                "order_type": order_type,
                "user_id": order['user_id']
            }
            await query.edit_message_text(
                f"📝 Відповідь на замовлення №{order_id}\n\nВведіть текст повідомлення для клієнта:",
                reply_markup=get_back_keyboard(f"order_view_{order_id}_{order_type}")
            )
            return
        
        elif data.startswith("order_confirm_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "підтверджено", order_type):
                text = f"✅ Замовлення №{order_id} підтверджено!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "підтверджено")
            else:
                text = f"❌ Помилка при підтвердженні замовлення"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"order_view_{order_id}_{order_type}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("order_packed_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "упаковано", order_type):
                text = f"📦 Замовлення №{order_id} упаковано!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "упаковано")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"order_view_{order_id}_{order_type}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("order_shipped_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "відправлено", order_type):
                text = f"🚚 Замовлення №{order_id} відправлено!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "відправлено")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"order_view_{order_id}_{order_type}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("order_arrived_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "прибуло", order_type):
                text = f"📍 Замовлення №{order_id} прибуло у відділення!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "прибуло")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"order_view_{order_id}_{order_type}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("order_cancel_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID замовлення", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "скасовано", order_type):
                text = f"❌ Замовлення №{order_id} скасовано!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "скасовано")
            else:
                text = f"❌ Помилка при скасуванні замовлення"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"order_view_{order_id}_{order_type}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        # ============== КЕРУВАННЯ ПОВІДОМЛЕННЯМИ ==============
        
        elif data == "admin_messages":
            await query.edit_message_text("💬 Керування повідомленнями\n\nОберіть дію:", reply_markup=get_messages_menu())
            return
        
        elif data == "admin_messages_recent":
            recent_messages = get_recent_messages(hours=24, min_count=5)
            if not recent_messages:
                text = "💬 Повідомлень за останню добу немає.\n\nПоказую останні повідомлення:"
                recent_messages = get_all_messages(limit=5)
            
            if not recent_messages:
                text = "💬 Повідомлень не знайдено."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("messages"))
                return
            
            all_messages = get_all_messages(limit=5, offset=0)
            has_more = len(all_messages) >= 5
            
            text = "💬 <b>ОСТАННІ ПОВІДОМЛЕННЯ</b>\n\n"
            for msg in recent_messages:
                text += f"💬 <b>Повідомлення #{msg['id']}</b>\n"
                text += f"👤 Клієнт: {msg['user_name']} (@{msg['username']})\n"
                text += f"📅 Час: {msg['created_at'][:16]}\n"
                text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                text += f"{'─'*40}\n"
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_more":
            more_messages = get_more_messages(user_id, count=5)
            if not more_messages:
                text = "💬 Більше повідомлень не знайдено."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("messages"), parse_mode='HTML')
                return
            
            text = "💬 <b>ЩЕ ПОВІДОМЛЕННЯ</b>\n\n"
            for msg in more_messages:
                text += f"💬 <b>Повідомлення #{msg['id']}</b>\n"
                text += f"👤 Клієнт: {msg['user_name']} (@{msg['username']})\n"
                text += f"📅 Час: {msg['created_at'][:16]}\n"
                text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                text += f"{'─'*40}\n"
            
            next_messages = get_all_messages(limit=1, offset=messages_offset.get(user_id, 0))
            has_more = len(next_messages) > 0
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_all":
            messages = get_all_messages(limit=20)
            if not messages:
                text = "💬 Повідомлень поки немає"
            else:
                text = "💬 <b>ВСІ ПОВІДОМЛЕННЯ</b>\n\n"
                for msg in messages:
                    text += f"💬 <b>Повідомлення #{msg['id']}</b>\n"
                    text += f"👤 Клієнт: {msg['user_name']} (@{msg['username']})\n"
                    text += f"📅 Час: {msg['created_at'][:16]}\n"
                    text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                    text += f"{'─'*40}\n"
            
            all_messages = get_all_messages(limit=5, offset=0)
            has_more = len(all_messages) >= 5
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_details":
            messages = get_all_messages(limit=50)
            if not messages:
                await query.edit_message_text("❌ Повідомлень не знайдено", reply_markup=get_back_keyboard("messages"))
                return
            keyboard = []
            for msg in messages[:20]:
                user_name = msg['user_name']
                msg_id = msg['id']
                created_at = msg['created_at'][:16] if msg['created_at'] else 'Н/Д'
                text_preview = msg['text'][:30] + ('...' if len(msg['text']) > 30 else '')
                keyboard.append([{"text": f"💬 #{msg_id} - {user_name} - {created_at}\n📝 {text_preview}", "callback_data": f"message_view_{msg_id}"}])
            keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_messages"}])
            await query.edit_message_text("📋 Детальний перегляд повідомлень\n\nОберіть повідомлення:", reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("message_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("messages"))
                return
            
            try:
                message_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID повідомлення", reply_markup=get_back_keyboard("messages"))
                return
            
            msg = get_message_by_id(message_id)
            if not msg:
                await query.edit_message_text("❌ Повідомлення не знайдено", reply_markup=get_back_keyboard("messages"))
                return
            
            text = format_message_text(msg)
            await query.edit_message_text(
                text,
                reply_markup=get_message_actions_menu(message_id, msg['user_id']),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("reply_user_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("messages"))
                return
            
            try:
                user_id_to_reply = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID користувача", reply_markup=get_back_keyboard("messages"))
                return
            
            user_data = get_user_by_id(user_id_to_reply)
            
            admin_sessions[user_id] = {
                "state": "authenticated",
                "action": "reply_to_user",
                "customer_id": user_id_to_reply
            }
            await query.edit_message_text(
                f"📝 Відповідь користувачу {user_data['first_name'] if user_data else '#'}{user_id_to_reply}\n\nВведіть текст повідомлення:",
                reply_markup=get_back_keyboard("messages")
            )
            return
        
        elif data == "messages_all_file":
            messages = get_all_messages(limit=1000)
            if not messages:
                await query.edit_message_text("💬 Повідомлень поки немає", reply_markup=get_back_keyboard("messages"))
                return
            file_data = generate_messages_report(messages, "txt")
            await query.message.reply_document(
                document=file_data,
                filename=f"all_messages_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="💬 Всі повідомлення користувачів"
            )
            await query.edit_message_text("✅ Файл з повідомленнями згенеровано!", reply_markup=get_back_keyboard("messages"))
            return
        
        # ============== КЕРУВАННЯ КЛІЄНТАМИ ==============
        
        elif data == "admin_customers":
            await query.edit_message_text("👥 Керування клієнтами\n\nОберіть дію:", reply_markup=get_customers_menu())
            return
        
        elif data == "admin_customers_all":
            users = get_all_users()
            if not users:
                text = "👥 Клієнти\n\nКлієнтів не знайдено."
            else:
                text = f"👥 ВСІ КЛІЄНТИ\n\nВсього: {len(users)}\n\n"
                for user in users[:20]:
                    orders = get_user_orders(user['user_id'])
                    quick_orders = get_user_quick_orders(user['user_id'])
                    all_orders = orders + quick_orders
                    segment = get_customer_segment(user, all_orders)
                    created_at = user.get('created_at', '')
                    text += f"ID: {user['user_id']}\n"
                    text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
                    text += f"Username: @{user['username']}\n"
                    text += f"📊 {segment}\n"
                    text += f"📦 Замовлень: {len(all_orders)}\n"
                    text += f"{'─'*30}\n"
                if len(users) > 20:
                    text += f"... та ще {len(users) - 20} клієнтів"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_customers"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_customers_vip":
            users = get_all_users()
            text = "👑 VIP КЛІЄНТИ\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "VIP" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nІм'я: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\n📦 Замовлень: {len(all_orders)}\n{'─'*30}\n"
            if count == 0:
                text = "👑 VIP клієнтів не знайдено"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_customers"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_customers_regular":
            users = get_all_users()
            text = "⭐ ПОСТІЙНІ КЛІЄНТИ\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "Постійний" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nІм'я: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\n📦 Замовлень: {len(all_orders)}\n{'─'*30}\n"
            if count == 0:
                text = "⭐ Постійних клієнтів не знайдено"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_customers"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_customers_new":
            users = get_all_users()
            text = "🆕 НОВІ КЛІЄНТИ\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "Новий" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nІм'я: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\n📦 Замовлень: {len(all_orders)}\n{'─'*30}\n"
            if count == 0:
                text = "🆕 Нових клієнтів не знайдено"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_customers"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_customers_inactive":
            users = get_all_users()
            text = "💤 НЕАКТИВНІ КЛІЄНТИ\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "Неактивний" in segment:
                    count += 1
                    last_order_date = "Немає"
                    if all_orders:
                        last_order = all_orders[0].get('created_at', '')
                        last_order_date = last_order[:16]
                    text += f"ID: {user['user_id']}\nІм'я: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\nОстаннє замовлення: {last_order_date}\n{'─'*30}\n"
            if count == 0:
                text = "💤 Неактивних клієнтів не знайдено"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_customers"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "export_customers":
            users = get_all_users()
            if not users:
                await query.edit_message_text("❌ Немає клієнтів для експорту", reply_markup=get_customers_menu())
                return
            
            file_data = generate_users_report(users)
            await query.message.reply_document(
                document=file_data,
                filename=f"customers_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="👥 Повний звіт по клієнтах"
            )
            await query.edit_message_text("✅ Файл з клієнтами згенеровано!", reply_markup=get_customers_menu())
            return
        
        elif data == "admin_customer_search":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_customer_by_phone"}
            await query.edit_message_text("🔍 Пошук клієнта за телефоном\n\nВведіть номер телефону:", reply_markup=get_back_keyboard("customers"))
            return
        
        elif data.startswith("customer_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID клієнта", reply_markup=get_back_keyboard("customers"))
                return
            
            user = get_user_by_id(customer_id)
            if not user:
                await query.edit_message_text("❌ Клієнта не знайдено")
                return
            orders = get_user_orders(customer_id)
            quick_orders = get_user_quick_orders(customer_id)
            messages = get_user_messages(customer_id)
            all_orders = orders + quick_orders
            segment = get_customer_segment(user, all_orders)
            
            text = f"👤 ПРОФІЛЬ КЛІЄНТА\n\n"
            text += f"ID: {user['user_id']}\n"
            text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
            text += f"Username: @{user['username']}\n"
            text += f"📅 Реєстрація: {user.get('created_at', 'Н/Д')[:16]}\n"
            text += f"📊 Сегмент: {segment}\n\n"
            
            if all_orders:
                total_spent = sum(o.get('total', 0) for o in orders)
                text += f"📦 Всього замовлень: {len(all_orders)}\n"
                text += f"💰 Загальна сума: {total_spent:.2f} грн\n"
                if orders:
                    text += f"💳 Середній чек: {total_spent/len(orders):.2f} грн\n\n"
                
                text += "🆕 Останнє замовлення:\n"
                last = all_orders[0]
                last_created = last.get('created_at', '')[:16]
                last_id = last.get('order_id', last.get('id', 'Н/Д'))
                text += f"   №{last_id} від {last_created}\n"
                text += f"   Сума: {last.get('total', 0):.2f} грн\n"
                text += f"   Статус: {last.get('status', 'нове')}\n"
            else:
                text += "📦 Замовлень: 0\n"
            
            text += f"\n💬 Повідомлень: {len(messages)}"
            
            await query.edit_message_text(
                text,
                reply_markup=get_customer_actions_menu(customer_id),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("customer_orders_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID клієнта", reply_markup=get_back_keyboard("customers"))
                return
            
            orders = get_user_orders(customer_id)
            quick_orders = get_user_quick_orders(customer_id)
            all_orders = orders + quick_orders
            
            if not all_orders:
                text = "📋 Історія замовлень\n\nУ клієнта немає замовлень."
            else:
                text = f"📋 ІСТОРІЯ ЗАМОВЛЕНЬ\n\nВсього: {len(all_orders)}\n\n"
                for order in all_orders:
                    created_at = order.get('created_at', '')[:16]
                    order_id = order.get('order_id', order.get('id', 'Н/Д'))
                    order_type = "⚡" if order.get('order_type') == 'quick' else "📦"
                    text += f"{order_type} №{order_id} | {created_at}\n"
                    text += f"Сума: {order.get('total', 0):.2f} грн\n"
                    text += f"Статус: {order.get('status', 'нове')}\n"
                    if order.get('order_type') == 'quick' and order.get('message'):
                        text += f"💬 {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    text += f"{'─'*30}\n"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"customer_view_{customer_id}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard), parse_mode='HTML')
            return
        
        elif data.startswith("customer_messages_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID клієнта", reply_markup=get_back_keyboard("customers"))
                return
            
            messages = get_user_messages(customer_id)
            
            if not messages:
                text = "💬 Повідомлення\n\nУ клієнта немає повідомлень."
            else:
                text = f"💬 ПОВІДОМЛЕННЯ КЛІЄНТА\n\n"
                for msg in messages[:10]:
                    created_at = msg.get('created_at', '')[:16]
                    text += f"📅 {created_at}\n"
                    text += f"📝 {msg['text']}\n"
                    text += f"{'─'*30}\n"
            
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"customer_view_{customer_id}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard), parse_mode='HTML')
            return
        
        elif data.startswith("customer_message_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID клієнта", reply_markup=get_back_keyboard("customers"))
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": "send_message_to_customer", "customer_id": customer_id}
            await query.edit_message_text("📢 Надіслати повідомлення клієнту\n\nВведіть текст повідомлення:", reply_markup=get_back_keyboard(f"customer_view_{customer_id}"))
            return
        
        elif data.startswith("customer_make_admin_"):
            parts = data.split("_")
            if len(parts) < 4:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[3])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID клієнта", reply_markup=get_back_keyboard("customers"))
                return
            
            user = get_user_by_id(customer_id)
            if user:
                if add_admin(customer_id, user['username'], user_id):
                    text = f"✅ Користувача {user['first_name']} додано до адмінів!"
                else:
                    text = "❌ Помилка при додаванні адміна"
            else:
                text = "❌ Користувача не знайдено"
            keyboard = [[{"text": "🔙 Назад", "callback_data": f"customer_view_{customer_id}"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard), parse_mode='HTML')
            return
        
        # ============== РОЗСИЛКИ ==============
        
        elif data == "admin_broadcast":
            await query.edit_message_text("📢 Розсилка повідомлень\n\nОберіть цільову аудиторію:", reply_markup=get_broadcast_menu())
            return
        
        elif data.startswith("broadcast_"):
            segment = data.replace("broadcast_", "")
            admin_sessions[user_id] = {"state": "authenticated", "action": "broadcast", "segment": segment}
            await query.edit_message_text(f"📢 Розсилка для сегменту: {segment}\n\nВведіть текст повідомлення для розсилки:", reply_markup=get_broadcast_input_back_keyboard())
            return
        
        # ============== ЗВІТИ ==============
        
        elif data == "admin_reports":
            await query.edit_message_text("📁 Генерація звітів\n\nОберіть тип звіту та формат:", reply_markup=get_reports_menu())
            return
        
        elif data == "report_orders_txt":
            orders = get_all_orders(include_quick=True)
            report_data = generate_orders_report(orders, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="📋 Звіт по замовленнях"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_orders_csv":
            orders = get_all_orders(include_quick=True)
            report_data = generate_orders_report(orders, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="📋 Звіт по замовленнях (CSV)"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_users_txt":
            users = get_all_users()
            report_data = generate_users_report(users)
            await query.message.reply_document(
                document=report_data,
                filename=f"users_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="👥 Звіт по клієнтах"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_users_csv":
            await query.edit_message_text("Функція в розробці, використовуйте TXT формат", reply_markup=get_reports_menu())
            return
        
        elif data == "report_quick_txt":
            orders = get_quick_orders()
            report_data = generate_quick_orders_report(orders, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"quick_orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="⚡ Звіт по швидких замовленнях"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_quick_csv":
            orders = get_quick_orders()
            report_data = generate_quick_orders_report(orders, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"quick_orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="⚡ Звіт по швидких замовленнях (CSV)"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_messages_txt":
            messages = get_all_messages(limit=1000)
            report_data = generate_messages_report(messages, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"messages_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="💬 Звіт по повідомленнях"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_messages_csv":
            messages = get_all_messages(limit=1000)
            report_data = generate_messages_report(messages, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"messages_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="💬 Звіт по повідомленнях (CSV)"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_stats_txt":
            stats = get_statistics()
            report_data = generate_stats_report(stats, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"stats_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="📊 Статистика"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        # ============== КЕРУВАННЯ АДМІНАМИ ==============
        
        elif data == "admin_manage_admins":
            await query.edit_message_text("👑 Керування адміністраторами\n\nОберіть дію:", reply_markup=get_admins_menu())
            return
        
        elif data == "admin_list":
            admins = get_all_admins()
            if not admins:
                text = "📋 Список адмінів\n\nАдмінів не знайдено."
            else:
                text = "📋 СПИСОК АДМІНІСТРАТОРІВ\n\n"
                for admin in admins:
                    added_at = admin.get('added_at', '')[:16]
                    text += f"ID: {admin['user_id']}\nUsername: @{admin['username']}\nДодано: {added_at}\n{'─'*30}\n"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_main"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data == "admin_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_admin"}
            await query.edit_message_text("➕ Додавання адміністратора\n\nВведіть Telegram ID користувача:", reply_markup=get_back_keyboard("main"))
            return
        
        elif data == "admin_remove":
            admins = get_all_admins()
            if not admins:
                await query.edit_message_text("❌ Адмінів не знайдено", reply_markup=get_admins_menu())
                return
            keyboard = []
            for admin in admins:
                if admin['user_id'] != user_id:
                    keyboard.append([{"text": f"❌ {admin['user_id']} - @{admin['username']}", "callback_data": f"remove_admin_{admin['user_id']}"}])
            keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_main"}])
            await query.edit_message_text("🗑 Видалення адміністратора\n\nОберіть адміна для видалення:", reply_markup=create_inline_keyboard(keyboard))
            return
        
        elif data.startswith("remove_admin_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("❌ Помилка формату даних", reply_markup=get_back_keyboard("main"))
                return
            
            try:
                admin_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Помилка: некоректний ID адміністратора", reply_markup=get_back_keyboard("main"))
                return
            
            if admin_id == user_id:
                text = "❌ Не можна видалити самого себе!"
            elif remove_admin(admin_id):
                text = "✅ Адміна успішно видалено!"
            else:
                text = "❌ Помилка при видаленні адміна"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_main"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        # ============== СТАТИСТИКА ==============
        
        elif data == "admin_stats":
            stats = get_statistics()
            text = "📊 СТАТИСТИКА\n\n"
            text += f"📋 Замовлень: {stats.get('total_orders', 0)}\n"
            text += f"💰 Виручка: {stats.get('total_revenue', 0):.2f} грн\n"
            text += f"💳 Середній чек: {stats.get('avg_check', 0):.2f} грн\n"
            text += f"👥 Клієнтів: {stats.get('total_users', 0)}\n"
            text += f"⚡ Швидких замовлень: {stats.get('total_quick_orders', 0)}\n"
            text += f"💬 Повідомлень: {stats.get('total_messages', 0)}\n\n"
            text += "📊 Замовлення за останні 30 днів:\n"
            text += f"   Кількість: {stats.get('last_30_days_orders', 0)}\n"
            text += f"   Сума: {stats.get('last_30_days_revenue', 0):.2f} грн\n\n"
            text += "📊 Статуси замовлень:\n"
            for status, count in stats.get('orders_by_status', {}).items():
                text += f"   • {status}: {count}\n"
            text += "\n👥 Сегментація клієнтів:\n"
            segments = stats.get('segments', {})
            text += f"   👑 VIP: {segments.get('vip', 0)}\n"
            text += f"   ⭐ Постійні: {segments.get('regular', 0)}\n"
            text += f"   🆕 Нові: {segments.get('new', 0)}\n"
            text += f"   📊 Активні: {segments.get('active', 0)}\n"
            text += f"   💤 Неактивні: {segments.get('inactive', 0)}\n"
            keyboard = [[{"text": "🔙 Назад", "callback_data": "back_to_main"}]]
            await query.edit_message_text(text, reply_markup=create_inline_keyboard(keyboard))
            return
        
        # ============== НАЛАШТУВАННЯ ==============
        
        elif data == "admin_settings":
            await query.edit_message_text("⚙️ Налаштування\n\nОберіть розділ:", reply_markup=get_settings_menu())
            return
        
        elif data == "admin_settings_password":
            admin_sessions[user_id] = {"state": "authenticated", "action": "change_password"}
            await query.edit_message_text("🔑 Зміна пароля\n\nВведіть новий пароль:", reply_markup=get_back_keyboard("main"))
            return
        
        else:
            logger.warning(f"⚠️ Невідомий callback: {data}")
            await query.edit_message_text("❌ Невідома команда", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"❌ Помилка в button_handler: {e}")
        logger.error(traceback.format_exc())
        try:
            await query.edit_message_text(
                "❌ Сталася помилка. Повертаємось до головного меню.",
                reply_markup=get_main_menu()
            )
        except:
            pass

# ========== ОБРОБНИК ТЕКСТОВИХ ПОВІДОМЛЕНЬ ==========

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник текстових повідомлень та фото"""
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip() if update.message.text else ""
        
        logger.info(f"📝 Адмін {user_id}: {text[:50] if text else '[Фото]'}...")
        
        if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
            await check_password(update, context)
            return
        
        if not is_authenticated(user_id):
            logger.warning(f"❌ Неавтентифікований адмін {user_id} спробував надіслати повідомлення")
            await update.message.reply_text(
                "❌ Сесія закінчилась\n\nНапишіть /start для повторного входу",
                reply_markup=get_main_menu()
            )
            return
        
        session = admin_sessions.get(user_id, {})
        action = session.get("action")
        logger.info(f"📌 Поточний action: {action}, session: {session}")
        
        # ============== РЕДАГУВАННЯ КОМПАНІЇ ==============
        
        if action == "edit_company_text":
            logger.debug(f"Оновлення тексту компанії: довжина {len(text)}")
            if update_company_info(text, user_id):
                await update.message.reply_text(
                    "✅ Текст 'Про компанію' успішно оновлено!",
                    reply_markup=get_company_edit_menu()
                )
            else:
                await update.message.reply_text(
                    "❌ Помилка при оновленні тексту",
                    reply_markup=get_company_edit_menu()
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== РЕДАГУВАННЯ ВІТАННЯ ==============
        
        if action == "edit_welcome_text":
            logger.debug(f"Оновлення вітального повідомлення: довжина {len(text)}")
            if update_welcome_message(text, user_id):
                await update.message.reply_text(
                    "✅ Вітальне повідомлення успішно оновлено!",
                    reply_markup=get_welcome_edit_menu()
                )
            else:
                await update.message.reply_text(
                    "❌ Помилка при оновленні вітання",
                    reply_markup=get_welcome_edit_menu()
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ДОДАВАННЯ FAQ ==============
        
        elif action == "faq_edit_add_question":
            logger.debug(f"Додавання FAQ: отримано питання: {text[:30]}...")
            admin_sessions[user_id]["faq_question"] = text
            admin_sessions[user_id]["action"] = "faq_edit_add_answer"
            await update.message.reply_text(
                "📝 Введіть <b>відповідь</b> на питання:",
                reply_markup=get_back_keyboard("faq_edit_main"),
                parse_mode='HTML'
            )
            return
        
        elif action == "faq_edit_add_answer":
            question = session.get("faq_question")
            answer = text
            logger.debug(f"Додавання FAQ: питання: {question[:30]}..., відповідь: {answer[:30]}...")
            faq_id = add_faq(question, answer)
            if faq_id:
                await update.message.reply_text(
                    f"✅ FAQ додано! ID: {faq_id}",
                    reply_markup=get_faq_edit_main_menu()
                )
            else:
                await update.message.reply_text(
                    "❌ Помилка при додаванні FAQ",
                    reply_markup=get_faq_edit_main_menu()
                )
            admin_sessions[user_id].pop("action", None)
            if "faq_question" in admin_sessions[user_id]:
                admin_sessions[user_id].pop("faq_question")
            return
        
        # ============== РЕДАГУВАННЯ FAQ ==============
        
        elif action == "faq_edit_update_question":
            faq_id = session.get("faq_id")
            if not faq_id:
                await update.message.reply_text("❌ Помилка: ID FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            faq = get_faq_by_id(faq_id)
            if not faq:
                await update.message.reply_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_faq(faq_id, text, faq['answer']):
                await update.message.reply_text(
                    f"✅ Питання FAQ #{faq_id} оновлено!",
                    reply_markup=get_faq_edit_actions_keyboard(faq_id)
                )
            else:
                await update.message.reply_text(
                    "❌ Помилка при оновленні",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}")
                )
            
            admin_sessions[user_id].pop("action", None)
            admin_sessions[user_id].pop("faq_id", None)
            return
        
        elif action == "faq_edit_update_answer":
            faq_id = session.get("faq_id")
            if not faq_id:
                await update.message.reply_text("❌ Помилка: ID FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            faq = get_faq_by_id(faq_id)
            if not faq:
                await update.message.reply_text("❌ FAQ не знайдено", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_faq(faq_id, faq['question'], text):
                await update.message.reply_text(
                    f"✅ Відповідь FAQ #{faq_id} оновлено!",
                    reply_markup=get_faq_edit_actions_keyboard(faq_id)
                )
            else:
                await update.message.reply_text(
                    "❌ Помилка при оновленні",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}")
                )
            
            admin_sessions[user_id].pop("action", None)
            admin_sessions[user_id].pop("faq_id", None)
            return
        
        # ============== ДОДАВАННЯ ТОВАРУ ==============
        
        elif action == "add_product_name":
            admin_sessions[user_id]["product_name"] = text
            admin_sessions[user_id]["action"] = "add_product_price"
            await update.message.reply_text("Введіть ціну товару (тільки число):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_price":
            try:
                price = float(text.replace(",", "."))
                admin_sessions[user_id]["product_price"] = price
                admin_sessions[user_id]["action"] = "add_product_category"
                await update.message.reply_text("Введіть категорію товару:", reply_markup=get_back_keyboard("products"))
            except ValueError:
                await update.message.reply_text("❌ Невірний формат. Введіть число (наприклад: 250):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_category":
            admin_sessions[user_id]["product_category"] = text
            admin_sessions[user_id]["action"] = "add_product_description"
            await update.message.reply_text("Введіть опис товару:", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_description":
            admin_sessions[user_id]["product_description"] = text
            admin_sessions[user_id]["action"] = "add_product_unit"
            await update.message.reply_text("Введіть одиницю виміру (наприклад: банка, кг, шт):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_unit":
            admin_sessions[user_id]["product_unit"] = text
            admin_sessions[user_id]["action"] = "add_product_details"
            await update.message.reply_text("Введіть деталі товару (об'єм, вага, склад тощо):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_details":
            product_data = {
                "name": session.get("product_name"),
                "price": session.get("product_price"),
                "category": session.get("product_category"),
                "description": session.get("product_description"),
                "unit": session.get("product_unit"),
                "details": text
            }
            
            product_id = add_product(**product_data)
            
            if product_id:
                await update.message.reply_text(
                    f"✅ Товар успішно додано!\n\nID: {product_id}\nНазва: {product_data['name']}\nЦіна: {product_data['price']} грн\nОдиниці: {product_data['unit']}",
                    reply_markup=get_products_menu()
                )
            else:
                await update.message.reply_text("❌ Помилка при додаванні товару", reply_markup=get_products_menu())
            
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== РЕДАГУВАННЯ ТОВАРУ ==============
        
        elif action == "edit_product_unit":
            product_id = session.get("product_id")
            if update_product(product_id, unit=text):
                await update.message.reply_text(f"✅ Одиниці товару #{product_id} оновлено!", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            else:
                await update.message.reply_text("❌ Помилка при оновленні одиниць", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_details":
            product_id = session.get("product_id")
            if update_product(product_id, details=text):
                await update.message.reply_text(f"✅ Деталі товару #{product_id} оновлено!", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            else:
                await update.message.reply_text("❌ Помилка при оновленні деталей", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_benefits":
            product_id = session.get("product_id")
            if update_product(product_id, benefits=text):
                await update.message.reply_text(f"✅ Переваги товару #{product_id} оновлено!", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            else:
                await update.message.reply_text("❌ Помилка при оновленні переваг", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_usage":
            product_id = session.get("product_id")
            if update_product(product_id, usage=text):
                await update.message.reply_text(f"✅ Інструкція з використання товару #{product_id} оновлено!", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            else:
                await update.message.reply_text("❌ Помилка при оновленні інструкції", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_image_url":
            product_id = session.get("product_id")
            logger.info(f"📝 Отримано повідомлення для edit_product_image_url, product_id: {product_id}, текст: {text}")
            
            if not product_id:
                logger.error("❌ product_id не знайдено в сесії!")
                await update.message.reply_text("❌ Помилка: ID товару не знайдено. Спробуйте ще раз.", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            image_bytes = await download_image_from_url_to_bytes(text)
            
            if image_bytes:
                if update_product(product_id, image_data=image_bytes):
                    await update.message.reply_text(
                        f"✅ Фото товару #{product_id} оновлено! (збережено в БД)", 
                        reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                    )
                else:
                    await update.message.reply_text(
                        "❌ Помилка при оновленні фото в базі даних", 
                        reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                    )
            else:
                await update.message.reply_text(
                    "❌ Помилка при завантаженні зображення за URL. Перевірте посилання та спробуйте ще раз.", 
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
            
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_image_file":
            product_id = session.get("product_id")
            logger.info(f"📝 Отримано фото для edit_product_image_file, product_id: {product_id}")
            
            if not product_id:
                logger.error("❌ product_id не знайдено в сесії!")
                await update.message.reply_text(
                    "❌ Помилка: ID товару не знайдено. Спробуйте ще раз.", 
                    reply_markup=get_products_menu()
                )
                admin_sessions[user_id].pop("action", None)
                return
            
            if update.message.photo:
                file_id = update.message.photo[-1].file_id
                logger.info(f"📸 Отримано file_id: {file_id}")
                
                image_bytes = await download_telegram_file_to_bytes(file_id, context.bot)
                
                if image_bytes:
                    if update_product(product_id, image_data=image_bytes):
                        await update.message.reply_text(
                            f"✅ Фото товару #{product_id} оновлено! (збережено в БД)", 
                            reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                        )
                    else:
                        await update.message.reply_text(
                            "❌ Помилка при оновленні фото в базі даних", 
                            reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                        )
                else:
                    await update.message.reply_text(
                        "❌ Помилка при завантаженні фото", 
                        reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                    )
            else:
                await update.message.reply_text(
                    "❌ Будь ласка, надішліть фото", 
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
                return
            
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action.startswith("edit_product_"):
            field = action.replace("edit_product_", "")
            product_id = session.get("product_id")
            
            update_data = {}
            if field == "name":
                update_data["name"] = text
            elif field == "price":
                try:
                    update_data["price"] = float(text.replace(",", "."))
                except ValueError:
                    await update.message.reply_text("❌ Невірний формат. Введіть число:", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
                    return
            elif field == "desc":
                update_data["description"] = text
            elif field == "cat":
                update_data["category"] = text
            else:
                await update.message.reply_text("❌ Невідоме поле для редагування", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_product(product_id, **update_data):
                await update.message.reply_text(f"✅ Товар #{product_id} оновлено!", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            else:
                await update.message.reply_text("❌ Помилка при оновленні товару", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ПОШУК ЗАМОВЛЕНЬ ==============
        
        elif action == "search_orders_by_phone":
            orders = get_orders_by_phone(text)
            if not orders:
                await update.message.reply_text(f"❌ Замовлень за номером {text} не знайдено", reply_markup=get_orders_menu())
            else:
                response = f"📋 Знайдено замовлень: {len(orders)}\n\n"
                for order in orders[:5]:
                    created_at = order.get('created_at', '')[:16]
                    order_id = order.get('order_id', order.get('id', 'Н/Д'))
                    response += f"№{order_id} | {created_at}\n"
                    response += f"Сума: {order.get('total', 0):.2f} грн\n"
                    response += f"Статус: {order.get('status', 'нове')}\n"
                    if order.get('order_type') == 'quick' and order.get('message'):
                        response += f"💬 {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    response += f"{'─'*30}\n"
                keyboard = []
                for order in orders[:10]:
                    order_id = order.get('order_id', order.get('id', 0))
                    order_type = order.get('order_type', 'regular')
                    keyboard.append([{"text": f"📦 №{order_id}", "callback_data": f"order_view_{order_id}_{order_type}"}])
                keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_orders"}])
                await update.message.reply_text(response, reply_markup=create_inline_keyboard(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ПОШУК КЛІЄНТІВ ==============
        
        elif action == "search_customer_by_phone":
            user_data = get_user_by_phone(text)
            if not user_data:
                await update.message.reply_text(f"❌ Клієнта з телефоном {text} не знайдено", reply_markup=get_customers_menu())
            else:
                orders = get_user_orders(user_data['user_id'])
                quick_orders = get_user_quick_orders(user_data['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user_data, all_orders)
                
                response = f"👤 КЛІЄНТ ЗНАЙДЕНИЙ\n\n"
                response += f"ID: {user_data['user_id']}\n"
                response += f"Ім'я: {user_data['first_name']} {user_data['last_name']}\n"
                response += f"Username: @{user_data['username']}\n"
                response += f"📅 Реєстрація: {user_data.get('created_at', '')[:16]}\n"
                response += f"📊 Сегмент: {segment}\n"
                response += f"📦 Замовлень: {len(all_orders)}\n\n"
                
                if all_orders:
                    total = sum(o.get('total', 0) for o in orders)
                    response += f"💰 Загальна сума: {total:.2f} грн"
                
                keyboard = [[{"text": "👤 Переглянути профіль", "callback_data": f"customer_view_{user_data['user_id']}"}]]
                keyboard.append([{"text": "🔙 Назад", "callback_data": "back_to_customers"}])
                
                await update.message.reply_text(response, reply_markup=create_inline_keyboard(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== НАДСИЛАННЯ ПОВІДОМЛЕНЬ ==============
        
        elif action == "send_message_to_customer":
            customer_id = session.get("customer_id")
            try:
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"📢 <b>Повідомлення від адміністратора</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text("✅ Повідомлення надіслано!", reply_markup=get_customer_actions_menu(customer_id))
            except Exception as e:
                await update.message.reply_text(f"❌ Помилка при надсиланні: {e}", reply_markup=get_customer_actions_menu(customer_id))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "reply_to_order":
            customer_id = session.get("user_id")
            order_id = session.get("order_id")
            try:
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"📢 <b>Відповідь на замовлення №{order_id}</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text(
                    f"✅ Відповідь на замовлення №{order_id} надіслано!",
                    reply_markup=get_order_actions_menu(order_id, session.get("order_type", 'regular'))
                )
            except Exception as e:
                await update.message.reply_text(
                    f"❌ Помилка при надсиланні: {e}",
                    reply_markup=get_order_actions_menu(order_id, session.get("order_type", 'regular'))
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "reply_to_user":
            customer_id = session.get("customer_id")
            try:
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"📢 <b>Відповідь адміністратора</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text(
                    "✅ Відповідь надіслано!",
                    reply_markup=get_customer_actions_menu(customer_id)
                )
            except Exception as e:
                await update.message.reply_text(
                    f"❌ Помилка при надсиланні: {e}",
                    reply_markup=get_customer_actions_menu(customer_id)
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== РОЗСИЛКИ ==============
        
        elif action == "broadcast":
            segment = session.get("segment")
            
            await update.message.reply_text(f"📢 Розпочинаю розсилку...")
            
            admin_bot = Bot(token=TOKEN)
            
            if segment == "all":
                sent, failed = await send_broadcast_to_all(admin_bot, text, admin_user_id=user_id)
                segment_name = "ВСІМ користувачам"
            elif segment == "vip":
                sent, failed = await send_broadcast_to_segment(admin_bot, "vip", text, admin_user_id=user_id)
                segment_name = "👑 VIP клієнтам"
            elif segment == "regular":
                sent, failed = await send_broadcast_to_segment(admin_bot, "regular", text, admin_user_id=user_id)
                segment_name = "⭐ Постійним клієнтам"
            elif segment == "new":
                sent, failed = await send_broadcast_to_segment(admin_bot, "new", text, admin_user_id=user_id)
                segment_name = "🆕 Новим клієнтам"
            elif segment == "inactive":
                sent, failed = await send_broadcast_to_segment(admin_bot, "inactive", text, admin_user_id=user_id)
                segment_name = "💤 Неактивним клієнтам"
            else:
                sent, failed = 0, 0
                segment_name = segment
            
            await update.message.reply_text(
                f"✅ <b>Розсилка завершена!</b>\n\n"
                f"📢 Сегмент: {segment_name}\n"
                f"✓ Доставлено: {sent}\n"
                f"✗ Помилок: {failed}\n\n"
                f"<i>Детальний звіт у логах</i>",
                reply_markup=get_broadcast_menu(),
                parse_mode='HTML'
            )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ЗМІНА ПАРОЛЯ ==============
        
        elif action == "change_password":
            global ADMIN_PASSWORD
            ADMIN_PASSWORD = text
            logger.info(f"🔑 Пароль змінено адміном {user_id}")
            await update.message.reply_text("✅ Пароль успішно змінено!", reply_markup=get_settings_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ДОДАВАННЯ АДМІНА ==============
        
        elif action == "add_admin":
            try:
                new_admin_id = int(text)
                if add_admin(new_admin_id, "", user_id):
                    await update.message.reply_text(
                        f"✅ Користувача з ID {new_admin_id} додано до адмінів!\n\n"
                        f"Тепер він може увійти в адмін-бот за паролем.",
                        reply_markup=get_admins_menu()
                    )
                else:
                    await update.message.reply_text("❌ Помилка при додаванні адміна", reply_markup=get_admins_menu())
            except ValueError:
                await update.message.reply_text("❌ Введіть коректний числовий ID", reply_markup=get_admins_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        else:
            logger.warning(f"❌ Невідома команда від адміна {user_id}: {action}")
            await update.message.reply_text("❌ Невідома команда", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"❌ Помилка в message_handler: {e}")
        logger.error(traceback.format_exc())
        await update.message.reply_text(
            "❌ Сталася помилка. Повертаємось до головного меню.",
            reply_markup=get_main_menu()
        )

# ========== ФУНКЦІЇ ДЛЯ РОЗСИЛОК ==========

async def send_broadcast_to_all(admin_bot: Bot, message: str, admin_user_id: int = None):
    """Розсилка всім користувачам"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("Немає користувачів для розсилки")
        if admin_user_id:
            try:
                await admin_bot.send_message(
                    chat_id=admin_user_id,
                    text="⚠️ Немає користувачів для розсилки",
                    parse_mode='HTML'
                )
            except:
                pass
        return 0, 0
    
    if admin_user_id:
        try:
            await admin_bot.send_message(
                chat_id=admin_user_id,
                text=f"📢 <b>Розпочато розсилку ВСІМ користувачам</b>\n\n👥 Всього: {len(users)}",
                parse_mode='HTML'
            )
        except:
            pass
    
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    
    broadcast_in_progress[admin_user_id] = {"total": len(users), "sent": 0, "failed": 0}
    
    for i, user in enumerate(users):
        try:
            await main_bot.send_message(
                chat_id=user['user_id'],
                text=message,
                parse_mode='HTML'
            )
            sent_count += 1
            
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["sent"] = sent_count
            
            if admin_user_id and (i + 1) % 10 == 0:
                try:
                    await admin_bot.send_message(
                        chat_id=admin_user_id,
                        text=f"📢 <b>Прогрес розсилки:</b> {i + 1}/{len(users)} (✓ {sent_count} | ✗ {fail_count})",
                        parse_mode='HTML'
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
        except Exception as e:
            error_str = str(e)
            if "Chat not found" in error_str or "bot was blocked" in error_str:
                logger.warning(f"⚠️ Користувач {user['user_id']} заблокував бота або неактивний")
            else:
                logger.error(f"Помилка відправки користувачу {user['user_id']}: {e}")
            fail_count += 1
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["failed"] = fail_count
    
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

async def send_broadcast_to_segment(admin_bot: Bot, segment: str, message: str, admin_user_id: int = None):
    """Розсилка за сегментом клієнтів"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("Немає користувачів для розсилки")
        return 0, 0
    
    filtered_users = []
    segment_map = {
        "vip": "👑 VIP клієнт",
        "regular": "⭐ Постійний клієнт",
        "new": "🆕 Новий клієнт",
        "inactive": "💤 Неактивний клієнт",
        "active": "📊 Активний клієнт"
    }
    
    for user in users:
        user_orders = get_user_orders(user['user_id'])
        quick_orders = get_user_quick_orders(user['user_id'])
        all_orders = user_orders + quick_orders
        user_segment = get_customer_segment(user, all_orders)
        
        if segment in user_segment or (segment == "new" and "Новий" in user_segment):
            filtered_users.append(user)
    
    if admin_user_id:
        try:
            segment_name = segment_map.get(segment, segment)
            await admin_bot.send_message(
                chat_id=admin_user_id,
                text=f"📢 <b>Розпочато розсилку для {segment_name}</b>\n\n👥 Всього: {len(filtered_users)}",
                parse_mode='HTML'
            )
        except:
            pass
    
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    
    broadcast_in_progress[admin_user_id] = {"total": len(filtered_users), "sent": 0, "failed": 0}
    
    for i, user in enumerate(filtered_users):
        try:
            await main_bot.send_message(
                chat_id=user['user_id'],
                text=message,
                parse_mode='HTML'
            )
            sent_count += 1
            
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["sent"] = sent_count
            
            if admin_user_id and (i + 1) % 10 == 0:
                try:
                    await admin_bot.send_message(
                        chat_id=admin_user_id,
                        text=f"📢 <b>Прогрес розсилки:</b> {i + 1}/{len(filtered_users)} (✓ {sent_count} | ✗ {fail_count})",
                        parse_mode='HTML'
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
        except Exception as e:
            error_str = str(e)
            if "Chat not found" in error_str or "bot was blocked" in error_str:
                logger.warning(f"⚠️ Користувач {user['user_id']} заблокував бота або неактивний")
            else:
                logger.error(f"Помилка відправки користувачу {user['user_id']}: {e}")
            fail_count += 1
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["failed"] = fail_count
    
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

# ========== ОБРОБНИК ПОМИЛОК ==========

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник помилок"""
    try:
        if isinstance(context.error, Conflict):
            logger.error("❌ Конфлікт з іншим екземпляром бота! Переконайтеся, що запущено тільки один екземпляр.")
            # Не викидаємо помилку далі, просто логуємо
            return
        
        logger.error(f"Помилка: {context.error}")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"Помилка в обробнику помилок: {e}")

# ========== ГОЛОВНА ФУНКЦІЯ ==========

def main():
    """Головна функція запуску бота"""
    logger.info("=" * 80)
    logger.info("🚀 ЗАПУСК АДМІН-БОТА БОНЕЛЕТ")
    logger.info("=" * 80)
    
    try:
        conn = get_db_connection()
        if conn:
            logger.info(f"✅ Підключення до бази даних успішне")
            logger.info("🔄 Викликаю init_database_if_empty()...")
            init_result = init_database_if_empty()
            logger.info(f"📊 Результат ініціалізації: {init_result}")
            
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users")
                users_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM orders")
                orders_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM products")
                products_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM quick_orders")
                quick_orders_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM messages")
                messages_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM faq")
                faq_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM admins")
                admins_count = cursor.fetchone()['count']
                
                logger.info(f"📊 Статистика БД:")
                logger.info(f"   • Користувачів: {users_count}")
                logger.info(f"   • Замовлень: {orders_count}")
                logger.info(f"   • Швидких замовлень: {quick_orders_count}")
                logger.info(f"   • Товарів: {products_count}")
                logger.info(f"   • Повідомлень: {messages_count}")
                logger.info(f"   • FAQ: {faq_count}")
                logger.info(f"   • Адмінів: {admins_count}")
                
            except Exception as e:
                logger.error(f"❌ Помилка отримання статистики: {e}")
                logger.error(traceback.format_exc())
            
            conn.close()
        else:
            logger.warning("⚠️ Не вдалося підключитись до БД")
            init_database_if_empty()
        
        application = Application.builder().token(TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        application.add_handler(MessageHandler(filters.PHOTO, message_handler))
        application.add_error_handler(error_handler)
        
        logger.info("✅ Адмін-бот готовий до роботи")
        logger.info("🚀 Запуск polling...")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}")
        logger.error(traceback.format_exc())
        time.sleep(5)

if __name__ == "__main__":
    main()



