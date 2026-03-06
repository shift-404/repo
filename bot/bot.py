import os
import json
import re
import logging
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import asyncio

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logger.error("BOT_TOKEN не знайдено!")
    sys.exit(1)

ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not ADMIN_BOT_TOKEN:
    logger.error("ADMIN_BOT_TOKEN не знайдено!")
    sys.exit(1)

logger.info(f"✅ Токен основного бота отримано: {TOKEN[:4]}...{TOKEN[-4:]}")
logger.info(f"✅ Токен адмін-бота отримано: {ADMIN_BOT_TOKEN[:4]}...{ADMIN_BOT_TOKEN[-4:]}")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL не знайдено!")
    sys.exit(1)

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Помилка підключення до БД: {e}")
        return None

def init_database():
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
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
                unit TEXT DEFAULT 'шт',
                image TEXT,
                image_data BYTEA,
                details TEXT,
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
        
        # ========== ТАБЛИЦІ ДЛЯ КОНТЕНТУ ==========
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
        
        # Додаємо початкові дані для company_info, якщо їх немає
        cursor.execute("SELECT COUNT(*) FROM company_info")
        company_count = cursor.fetchone()['count']
        
        if company_count == 0:
            company_text = """
Компанія Бонелет

Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини.

Деталі:
• Працюємо з 2022 року
• Розташування: Одеська область, с. Великий Дальник
• Телефон: +380932599103
• Графік: ПН-ПТ 9:00-18:00 СБ 10:00-15:00
• Доставка: Новою Поштою по всій Україні

Наша філософія:
• Вирощуємо на власних полях Одещини
• Використовуємо натуральне консервування
• Гарантуємо якість кожного продукту
• Працюємо з любов'ю до природи

Доставка:
• Новою Поштою по всій Україні
• Самовивіз з Одеської області, с. Великий Дальник
• Терміни доставки: 1-4 дні в залежності від регіону
"""
            cursor.execute('''
                INSERT INTO company_info (id, text) VALUES (1, %s)
            ''', (company_text,))
        
        # Додаємо початкові дані для welcome_message, якщо їх немає
        cursor.execute("SELECT COUNT(*) FROM welcome_message")
        welcome_count = cursor.fetchone()['count']
        
        if welcome_count == 0:
            welcome_text = """
Вітаємо у боті компанії Бонелет!

Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини.

Про нас:
• Працюємо з 2022 року
• Розташування: Одеська область, с. Великий Дальник
• Доставка Новою Поштою по всій Україні

Оберіть опцію з меню
    """
            cursor.execute('''
                INSERT INTO welcome_message (id, text) VALUES (1, %s)
            ''', (welcome_text,))
        
        # Додаємо початкові FAQ, якщо їх немає
        cursor.execute("SELECT COUNT(*) FROM faq")
        faq_count = cursor.fetchone()['count']
        
        if faq_count == 0:
            faqs = [
                ("Які способи оплати ви приймаєте?", "Готівка при отриманні\nПереказ на карту ПриватБанку\nОплата через LiqPay", 0),
                ("Які терміни доставки?", "Київ - 1-2 дні\nУкраїна - 2-4 дні\nВеликі партії - 3-5 днів", 1)
            ]
            for question, answer, position in faqs:
                cursor.execute('''
                    INSERT INTO faq (question, answer, position) VALUES (%s, %s, %s)
                ''', (question, answer, position))
        
        conn.commit()
        logger.info("✅ База даних PostgreSQL ініціалізована")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка ініціалізації бази даних: {e}")
        return False
    finally:
        conn.close()

# ========== ФУНКЦІЇ ДЛЯ РОБОТИ З КОНТЕНТОМ ==========

def get_company_info() -> str:
    """Отримує текст про компанію з БД"""
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

def get_welcome_message() -> str:
    """Отримує вітальне повідомлення з БД"""
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

def get_all_faqs() -> List[Dict]:
    """Отримує всі FAQ з БД, відсортовані за позицією"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer, position FROM faq ORDER BY position, id')
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Помилка отримання faq: {e}")
        return []
    finally:
        conn.close()

def get_faq_by_id(faq_id: int) -> Optional[Dict]:
    """Отримує FAQ за ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer FROM faq WHERE id = %s', (faq_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"Помилка отримання faq за ID: {e}")
        return None
    finally:
        conn.close()

# ========== РЕШТА КОДУ ==========

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

ORDERS_LOG = os.path.join(LOGS_DIR, "orders.txt")
USERS_LOG = os.path.join(LOGS_DIR, "users.txt")
MESSAGES_LOG = os.path.join(LOGS_DIR, "messages.txt")
QUICK_ORDERS_LOG = os.path.join(LOGS_DIR, "quick_orders.txt")

def log_order(order_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(ORDERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"ЗАМОВЛЕННЯ #{order_data.get('order_id', 'Н/Д')}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Клієнт: {order_data.get('user_name', 'Н/Д')}\n")
            f.write(f"Телефон: {order_data.get('phone', 'Н/Д')}\n")
            f.write(f"Username: @{order_data.get('username', 'Н/Д')}\n")
            f.write(f"Місто: {order_data.get('city', 'Н/Д')}\n")
            f.write(f"Відділення: {order_data.get('np_department', 'Н/Д')}\n")
            f.write(f"Сума: {order_data.get('total', 0):.2f} грн\n")
            f.write(f"Статус: {order_data.get('status', 'нове')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"Помилка запису замовлення: {e}")

def log_user(user_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(USERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | ID:{user_data.get('user_id')} | {user_data.get('first_name', '')} {user_data.get('last_name', '')} | @{user_data.get('username', '')}\n")
    except Exception as e:
        logger.error(f"Помилка запису користувача: {e}")

def log_message(msg_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(MESSAGES_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'─'*50}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Від: {msg_data.get('user_name', 'Н/Д')} (ID: {msg_data.get('user_id', 'Н/Д')})\n")
            f.write(f"Username: @{msg_data.get('username', 'Н/Д')}\n")
            f.write(f"Тип: {msg_data.get('message_type', 'Н/Д')}\n")
            f.write(f"Текст: {msg_data.get('text', 'Н/Д')}\n")
            f.write(f"{'─'*50}\n")
    except Exception as e:
        logger.error(f"Помилка запису повідомлення: {e}")

def log_quick_order(order_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(QUICK_ORDERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"ШВИДКЕ ЗАМОВЛЕННЯ #{order_data.get('order_id', 'Н/Д')}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Клієнт: {order_data.get('user_name', 'Н/Д')}\n")
            f.write(f"Телефон: {order_data.get('phone', 'Н/Д')}\n")
            f.write(f"Username: @{order_data.get('username', 'Н/Д')}\n")
            f.write(f"Продукт: {order_data.get('product_name', 'Н/Д')}\n")
            f.write(f"Спосіб зв'язку: {order_data.get('contact_method', 'Н/Д')}\n")
            f.write(f"Повідомлення: {order_data.get('message', '')}\n")
            f.write(f"Статус: {order_data.get('status', 'нове')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"Помилка запису швидкого замовлення: {e}")

def check_single_instance():
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', 9999))
        sock.close()
        if result == 0:
            logger.error("⚠️ Другий екземпляр бота вже запущено!")
            return False
        return True
    except Exception as e:
        logger.error(f"⚠️ Помилка перевірки екземпляра: {e}")
        return True

async def notify_admins_about_new_order(order_data: dict):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("Немає адмінів для сповіщення")
            return
        
        order_type = "⚡ ШВИДКЕ" if order_data.get('order_type') == 'quick' else "📦 ЗВИЧАЙНЕ"
        order_id = order_data.get('order_id', order_data.get('id', 'Н/Д'))
        
        message = f"🆕 <b>НОВЕ {order_type} ЗАМОВЛЕННЯ #{order_id}</b>\n\n"
        message += f"👤 <b>Клієнт:</b> {order_data.get('user_name', 'Н/Д')}\n"
        message += f"📞 <b>Телефон:</b> {order_data.get('phone', 'Н/Д')}\n"
        
        if order_data.get('order_type') == 'quick':
            message += f"📦 <b>Продукт:</b> {order_data.get('product_name', 'Н/Д')}\n"
            message += f"💬 <b>Спосіб зв'язку:</b> {order_data.get('contact_method', 'Н/Д')}\n"
            if order_data.get('message'):
                message += f"📝 <b>Повідомлення:</b> {order_data.get('message')}\n"
        else:
            message += f"🏙️ <b>Місто:</b> {order_data.get('city', 'Н/Д')}\n"
            message += f"🏣 <b>Відділення НП:</b> {order_data.get('np_department', 'Н/Д')}\n"
            message += f"💰 <b>Сума:</b> {order_data.get('total', 0):.2f} грн\n"
            
            items_text = ""
            for item in order_data.get('items', []):
                items_text += f"  • {item.get('product_name')} x {item.get('quantity')} = {item.get('price_per_unit', 0) * item.get('quantity', 0):.2f} грн\n"
            if items_text:
                message += f"📦 <b>Товари:</b>\n{items_text}"
        
        message += f"\n🕒 <b>Час:</b> {order_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
        
        admin_bot = Bot(token=ADMIN_BOT_TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML'
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Сповіщення про замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в notify_admins_about_new_order: {e}")

async def notify_admins_about_message(message_data: dict):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("Немає адмінів для сповіщення")
            return
        
        message = f"💬 <b>НОВЕ ПОВІДОМЛЕННЯ</b>\n\n"
        message += f"👤 <b>Клієнт:</b> {message_data.get('user_name', 'Н/Д')}\n"
        message += f"📱 <b>Username:</b> @{message_data.get('username', 'Н/Д')}\n"
        message += f"🆔 <b>User ID:</b> {message_data.get('user_id', 'Н/Д')}\n"
        message += f"📝 <b>Текст:</b> {message_data.get('text', 'Н/Д')}\n"
        message += f"🕒 <b>Час:</b> {message_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
        
        admin_bot = Bot(token=ADMIN_BOT_TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML'
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Сповіщення про повідомлення відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в notify_admins_about_message: {e}")

async def send_combined_quick_order_notification(order_id: int, user_id: int, user_name: str, username: str, product_name: str, message_text: str):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("Немає адмінів для сповіщення")
            return
        
        message = f"🆕 <b>НОВЕ ⚡ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id}</b>\n\n"
        message += f"👤 <b>Клієнт:</b> {user_name}\n"
        message += f"📱 <b>Username:</b> @{username}\n"
        message += f"🆔 <b>User ID:</b> {user_id}\n"
        message += f"📦 <b>Продукт:</b> {product_name}\n"
        message += f"💬 <b>Спосіб зв'язку:</b> chat\n"
        message += f"📝 <b>Повідомлення:</b> {message_text}\n"
        message += f"🕒 <b>Час:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        admin_bot = Bot(token=ADMIN_BOT_TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML'
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Об'єднане сповіщення про швидке замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в send_combined_quick_order_notification: {e}")

class Database:
    
    @staticmethod
    def get_connection():
        return get_db_connection()
    
    @staticmethod
    def save_user(user_id: int, first_name: str = "", last_name: str = "", username: str = ""):
        conn = Database.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO users (user_id, first_name, last_name, username)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    username = EXCLUDED.username
            ''', (user_id, first_name, last_name, username))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка збереження користувача: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def get_user_session(user_id: int) -> Dict:
        conn = Database.get_connection()
        if not conn:
            return {"state": "", "temp_data": {}, "last_section": "main_menu"}
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT state, temp_data, last_section 
                FROM user_sessions 
                WHERE user_id = %s
            ''', (user_id,))
            
            row = cursor.fetchone()
            if row:
                state, temp_data_json, last_section = row['state'], row['temp_data'], row['last_section']
                temp_data = json.loads(temp_data_json) if temp_data_json else {}
                return {"state": state, "temp_data": temp_data, "last_section": last_section}
            return {"state": "", "temp_data": {}, "last_section": "main_menu"}
        except Exception as e:
            logger.error(f"Помилка отримання сесії: {e}")
            return {"state": "", "temp_data": {}, "last_section": "main_menu"}
        finally:
            conn.close()
    
    @staticmethod
    def save_user_session(user_id: int, state: str = "", temp_data: Dict = None, last_section: str = ""):
        conn = Database.get_connection()
        if not conn:
            return
        
        try:
            temp_data_json = json.dumps(temp_data) if temp_data else "{}"
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_sessions (user_id, state, temp_data, last_section, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    temp_data = EXCLUDED.temp_data,
                    last_section = EXCLUDED.last_section,
                    updated_at = CURRENT_TIMESTAMP
            ''', (user_id, state, temp_data_json, last_section))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка збереження сесії: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def clear_user_session(user_id: int):
        conn = Database.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_sessions WHERE user_id = %s', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка очищення сесії: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def add_to_cart(user_id: int, product_id: int, quantity: float) -> bool:
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, quantity FROM carts 
                WHERE user_id = %s AND product_id = %s
            ''', (user_id, product_id))
            
            existing = cursor.fetchone()
            
            if existing:
                cart_id, old_quantity = existing['id'], existing['quantity']
                new_quantity = old_quantity + quantity
                cursor.execute('''
                    UPDATE carts SET quantity = %s, added_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                ''', (new_quantity, cart_id))
            else:
                cursor.execute('''
                    INSERT INTO carts (user_id, product_id, quantity)
                    VALUES (%s, %s, %s)
                ''', (user_id, product_id, quantity))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Помилка додавання в корзину: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_cart_items(user_id: int) -> List[Dict]:
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT id, product_id, quantity FROM carts WHERE user_id = %s', (user_id,))
            rows = cursor.fetchall()
            
            items = []
            for row in rows:
                cart_id, product_id, quantity = row['id'], row['product_id'], row['quantity']
                product = Database.get_product_by_id(product_id)
                if product:
                    items.append({
                        "cart_id": cart_id,
                        "product": product,
                        "quantity": quantity
                    })
            return items
        except Exception as e:
            logger.error(f"Помилка отримання корзини: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def clear_cart(user_id: int):
        conn = Database.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM carts WHERE user_id = %s', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка очищення корзини: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def remove_from_cart(cart_id: int):
        conn = Database.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM carts WHERE id = %s', (cart_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка видалення з корзини: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def create_order(order_data: Dict) -> int:
        conn = Database.get_connection()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO orders (user_id, user_name, username, phone, city, np_department, total, order_type, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING order_id
            ''', (
                order_data.get("user_id"),
                order_data.get("user_name"),
                order_data.get("username"),
                order_data.get("phone"),
                order_data.get("city"),
                order_data.get("np_department"),
                order_data.get("total"),
                order_data.get("order_type"),
                "нове"
            ))
            
            result = cursor.fetchone()
            order_id = result['order_id'] if result else 0
            
            for item in order_data.get("items", []):
                cursor.execute('''
                    INSERT INTO order_items (order_id, product_name, quantity, price_per_unit)
                    VALUES (%s, %s, %s, %s)
                ''', (order_id, item.get("product_name"), item.get("quantity"), item.get("price")))
            
            cursor.execute('DELETE FROM carts WHERE user_id = %s', (order_data.get("user_id"),))
            conn.commit()
            logger.info(f"✅ Замовлення #{order_id} створено успішно")
            return order_id
        except Exception as e:
            logger.error(f"Помилка створення замовлення: {e}")
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def save_message(user_id: int, user_name: str, username: str, text: str, message_type: str):
        conn = Database.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO messages (user_id, user_name, username, text, message_type)
                VALUES (%s, %s, %s, %s, %s)
            ''', (user_id, user_name, username, text, message_type))
            conn.commit()
        except Exception as e:
            logger.error(f"Помилка збереження повідомлення: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def save_quick_order(user_id: int, user_name: str, username: str, product_id: int, 
                        product_name: str, quantity: float, phone: str = None, 
                        contact_method: str = "chat", message: str = None) -> int:
        conn = Database.get_connection()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO quick_orders (user_id, user_name, username, product_id, product_name, 
                                        quantity, phone, contact_method, message, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (user_id, user_name, username, product_id, product_name, quantity, phone, contact_method, message, "нове"))
            
            result = cursor.fetchone()
            order_id = result['id'] if result else 0
            conn.commit()
            logger.info(f"✅ Швидке замовлення #{order_id} збережено")
            return order_id
        except Exception as e:
            logger.error(f"Помилка збереження швидкого замовлення: {e}")
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def get_statistics() -> Dict:
        conn = Database.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM orders")
            total_orders = cursor.fetchone()['count']
            cursor.execute("SELECT COUNT(*) FROM messages")
            total_messages = cursor.fetchone()['count']
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM users")
            total_users = cursor.fetchone()['count']
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM carts")
            active_carts = cursor.fetchone()['count']
            cursor.execute("SELECT COUNT(*) FROM quick_orders")
            quick_orders = cursor.fetchone()['count']
            cursor.execute("SELECT SUM(total) FROM orders")
            total_revenue = cursor.fetchone()['sum'] or 0
            
            return {
                "total_orders": total_orders,
                "total_messages": total_messages,
                "total_users": total_users,
                "active_carts": active_carts,
                "quick_orders": quick_orders,
                "total_revenue": total_revenue
            }
        except Exception as e:
            logger.error(f"Помилка отримання статистики: {e}")
            return {}
        finally:
            conn.close()
    
    @staticmethod
    def get_all_products():
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT id, name, price, category, description, unit, image, details, created_at FROM products ORDER BY id')
            rows = cursor.fetchall()
            
            products = []
            for row in rows:
                product = {
                    "id": row['id'],
                    "name": row['name'],
                    "price": row['price'],
                    "category": row['category'],
                    "description": row['description'],
                    "unit": row['unit'],
                    "image": row['image'],
                    "details": row['details']
                }
                products.append(product)
            return products
        except Exception as e:
            logger.error(f"Помилка отримання товарів: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_product_image(product_id: int):
        """Отримує байти зображення товару з БД"""
        conn = Database.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT image_data FROM products WHERE id = %s', (product_id,))
            row = cursor.fetchone()
            if row and row['image_data']:
                if hasattr(row['image_data'], 'tobytes'):
                    return row['image_data'].tobytes()
                return bytes(row['image_data'])
            return None
        except Exception as e:
            logger.error(f"Помилка отримання зображення товару: {e}")
            return None
        finally:
            conn.close()
    
    @staticmethod
    def get_product_by_id(product_id: int):
        products = Database.get_all_products()
        for product in products:
            if product["id"] == product_id:
                return product
        return None
    
    @staticmethod
    def update_product_image(product_id: int, image_data: bytes) -> bool:
        """Оновлює зображення товару в БД"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE products SET image_data = %s WHERE id = %s', (psycopg2.Binary(image_data), product_id))
            conn.commit()
            logger.info(f"✅ Зображення товару #{product_id} оновлено в БД")
            return True
        except Exception as e:
            logger.error(f"Помилка оновлення зображення товару: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def delete_product_image(product_id: int) -> bool:
        """Видаляє зображення товару з БД"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE products SET image_data = NULL WHERE id = %s', (product_id,))
            conn.commit()
            logger.info(f"✅ Зображення товару #{product_id} видалено з БД")
            return True
        except Exception as e:
            logger.error(f"Помилка видалення зображення товару: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_user_orders(user_id: int) -> List[Dict]:
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM orders 
                WHERE user_id = %s 
                ORDER BY created_at DESC
            ''', (user_id,))
            rows = cursor.fetchall()
            
            orders = []
            for row in rows:
                order = dict(row)
                created_at = order.get('created_at')
                if created_at and hasattr(created_at, 'strftime'):
                    created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    created_at_str = str(created_at) if created_at else 'Н/Д'
                
                orders.append({
                    "order_id": order['order_id'],
                    "user_id": order['user_id'],
                    "user_name": order['user_name'],
                    "username": order['username'],
                    "phone": order['phone'],
                    "city": order['city'],
                    "np_department": order['np_department'],
                    "total": order['total'],
                    "status": order['status'],
                    "order_type": order['order_type'],
                    "created_at": created_at_str
                })
            return orders
        except Exception as e:
            logger.error(f"Помилка отримання замовлень користувача: {e}")
            return []
        finally:
            conn.close()

def get_product_by_id(product_id: int):
    return Database.get_product_by_id(product_id)

def get_products_from_db():
    return Database.get_all_products()

PRODUCTS = get_products_from_db()

def refresh_products():
    global PRODUCTS
    PRODUCTS = get_products_from_db()
    logger.info(f"🔄 Оновлено товари: {len(PRODUCTS)} позицій")

refresh_products()

# ========== КОМАНДИ ДЛЯ АДМІНІВ ==========

async def is_admin_user(user_id: int) -> bool:
    """Перевіряє чи є користувач адміністратором"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = %s', (user_id,))
        count = cursor.fetchone()['count']
        return count > 0
    except Exception as e:
        logger.error(f"Помилка перевірки адміна: {e}")
        return False
    finally:
        conn.close()

async def setphoto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для встановлення фото товару (тільки для адмінів)"""
    user = update.effective_user
    user_id = user.id
    
    # Перевіряємо чи користувач адмін
    if not await is_admin_user(user_id):
        logger.warning(f"❌ Користувач {user_id} спробував використати адмін-команду")
        return  # Ніякої відповіді звичайним користувачам
    
    args = context.args
    if not args:
        await update.message.reply_text("❌ Вкажіть ID товару. Приклад: /setphoto 1")
        return
    
    try:
        product_id = int(args[0])
        product = get_product_by_id(product_id)
        if not product:
            await update.message.reply_text(f"❌ Товар з ID {product_id} не знайдено")
            return
        
        # Зберігаємо в контексті, що цей адмін зараз встановлює фото для цього товару
        context.user_data['setphoto_product_id'] = product_id
        context.user_data['setphoto_mode'] = 'waiting'
        
        await update.message.reply_text(
            f"📸 Встановлення фото для товару #{product_id} - {product['name']}\n\n"
            f"Надішліть фото файлом або введіть URL зображення.\n"
            f"Для скасування введіть /cancel",
            parse_mode='HTML'
        )
    except ValueError:
        await update.message.reply_text("❌ ID товару має бути числом")

async def handle_admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє фото від адміна для встановлення на товар"""
    user = update.effective_user
    user_id = user.id
    
    # Перевіряємо чи є активна сесія встановлення фото
    if 'setphoto_product_id' not in context.user_data:
        return
    
    # Перевіряємо чи користувач адмін
    if not await is_admin_user(user_id):
        logger.warning(f"❌ Користувач {user_id} спробував надіслати фото для товару")
        return
    
    product_id = context.user_data['setphoto_product_id']
    product = get_product_by_id(product_id)
    
    if update.message.photo:
        # Отримуємо файл з найбільшою роздільною здатністю
        file_id = update.message.photo[-1].file_id
        file = await context.bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Зберігаємо в БД
        if Database.update_product_image(product_id, bytes(file_bytes)):
            await update.message.reply_text(
                f"✅ Фото для товару #{product_id} - {product['name']} успішно збережено!",
                reply_markup=get_main_menu()
            )
        else:
            await update.message.reply_text(
                f"❌ Помилка при збереженні фото",
                reply_markup=get_main_menu()
            )
        
        # Очищаємо сесію
        del context.user_data['setphoto_product_id']
        del context.user_data['setphoto_mode']

async def handle_admin_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє URL від адміна для встановлення на товар"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    # Перевіряємо чи є активна сесія встановлення фото
    if 'setphoto_product_id' not in context.user_data:
        return
    
    # Перевіряємо чи користувач адмін
    if not await is_admin_user(user_id):
        logger.warning(f"❌ Користувач {user_id} спробував надіслати URL для товару")
        return
    
    # Перевіряємо чи це схоже на URL
    if not (text.startswith('http://') or text.startswith('https://')):
        await update.message.reply_text("❌ Будь ласка, надішліть правильний URL (починається з http:// або https://)")
        return
    
    product_id = context.user_data['setphoto_product_id']
    product = get_product_by_id(product_id)
    
    # Завантажуємо зображення за URL
    await update.message.reply_text("⏰ Завантажую зображення...")
    
    try:
        import requests
        response = requests.get(text, timeout=30)
        response.raise_for_status()
        
        # Зберігаємо в БД
        if Database.update_product_image(product_id, response.content):
            await update.message.reply_text(
                f"✅ Фото для товару #{product_id} - {product['name']} успішно збережено!",
                reply_markup=get_main_menu()
            )
        else:
            await update.message.reply_text(
                f"❌ Помилка при збереженні фото",
                reply_markup=get_main_menu()
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка завантаження зображення: {e}")
    
    # Очищаємо сесію
    del context.user_data['setphoto_product_id']
    del context.user_data['setphoto_mode']

def create_inline_keyboard(buttons: List[List[Dict]]) -> InlineKeyboardMarkup:
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

def get_main_menu() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "🏢 Про компанію", "callback_data": "company"}],
        [{"text": "📦 Наші продукти", "callback_data": "products"}],
        [{"text": "❓ Часті запитання", "callback_data": "faq"}],
        [
            {"text": "🛒 Моя корзина", "callback_data": "cart"}, 
            {"text": "📋 Мої замовлення", "callback_data": "my_orders"}
        ],
        [{"text": "📞 Зв'язатися з нами", "callback_data": "contact"}]
    ]
    return create_inline_keyboard(buttons)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    buttons = [[{"text": "🔙 Назад", "callback_data": f"back_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu() -> InlineKeyboardMarkup:
    refresh_products()
    buttons = []
    for product in PRODUCTS:
        button_text = f"{product['name']}\n{product['price']} грн/{product['unit']}"
        # Обмежуємо довжину тексту
        if len(button_text) > 60:
            name_part = product['name'][:35] + "..." if len(product['name']) > 35 else product['name']
            button_text = f"{name_part}\n{product['price']} грн/{product['unit']}"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
        buttons.append([{
            "text": button_text,
            "callback_data": f"product_{product['id']}"
        }])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_product_detail_menu(product_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "🛒 Додати в кошик", "callback_data": f"add_to_cart_{product_id}"}],
        [{"text": "⚡ Швидке замовлення", "callback_data": f"quick_order_{product_id}"}],
        [{"text": "🔙 Назад", "callback_data": "back_products"}]
    ]
    return create_inline_keyboard(buttons)

def get_quick_order_menu(product_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "📞 Зателефонуйте мені", "callback_data": f"quick_call_{product_id}"}],
        [{"text": "💬 Напишіть мені в чат", "callback_data": f"quick_chat_{product_id}"}],
        [{"text": "🔙 Назад", "callback_data": f"product_{product_id}"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_menu() -> InlineKeyboardMarkup:
    # Отримуємо свіжі FAQ з БД при кожному запиті
    faqs = get_all_faqs()
    buttons = []
    for faq in faqs:
        short_q = faq['question'][:40] + "..." if len(faq['question']) > 40 else faq['question']
        buttons.append([{
            "text": f"❔ {short_q}",
            "callback_data": f"faq_{faq['id']}"
        }])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_contact_menu() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "📞 Зателефонувати", "callback_data": "call_us"}],
        [{"text": "📍 Наша адреса", "callback_data": "our_address"}],
        [{"text": "💬 Написати нам тут", "callback_data": "write_here"}],
        [{"text": "🔙 Назад", "callback_data": "back_main_menu"}]
    ]
    return create_inline_keyboard(buttons)

def get_cart_menu(cart_items: List) -> InlineKeyboardMarkup:
    buttons = []
    if cart_items:
        buttons.append([{"text": "✅ Оформити замовлення", "callback_data": "checkout_cart"}])
        buttons.append([{"text": "🗑️ Очистити корзину", "callback_data": "clear_cart"}])
        
        for item in cart_items:
            product_name = item["product"]["name"][:20]
            if len(item["product"]["name"]) > 20:
                product_name += "..."
            buttons.append([{
                "text": f"❌ {product_name} ({item['quantity']} {item['product']['unit']})",
                "callback_data": f"remove_from_cart_{item['cart_id']}"
            }])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_order_confirmation_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "✅ Так, продовжити", "callback_data": "confirm_order_yes"}],
        [{"text": "❌ Ні, скасувати", "callback_data": "confirm_order_no"}]
    ]
    return create_inline_keyboard(buttons)

def get_my_orders_menu(orders: List) -> InlineKeyboardMarkup:
    buttons = []
    for order in orders[:5]:
        buttons.append([{
            "text": f"№{order['order_id']} - {order['created_at'][:16]} - {order['total']} грн",
            "callback_data": f"user_order_{order['order_id']}"
        }])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def parse_quantity(text: str) -> Tuple[bool, float, str]:
    text = text.strip().replace(" ", "")
    match = re.search(r'(\d+(?:[.,]\d+)?)', text)
    
    if not match:
        return False, 0, "❌ Будь ласка, введіть число (наприклад: 1, 1.5, 2.3)"
    
    try:
        num_str = match.group(1).replace(",", ".")
        quantity = float(num_str)
        if quantity <= 0:
            return False, 0, "❌ Кількість повинна бути більше 0"
        if quantity > 100:
            return False, 0, "❌ Занадто велика кількість. Максимум 100"
        return True, quantity, ""
    except ValueError:
        return False, 0, "❌ Некоректний формат числа"

def validate_phone(phone: str) -> Tuple[bool, str]:
    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    
    if re.match(r'^(\+38|38)?0\d{9}$', phone):
        if phone.startswith("0"):
            phone = "+38" + phone
        elif phone.startswith("38"):
            phone = "+" + phone
        elif phone.startswith("+380"):
            pass
        else:
            phone = "+380" + phone[1:] if phone.startswith("+") else "+380" + phone
        return True, phone
    return False, phone

def get_welcome_text() -> str:
    # Отримуємо актуальне вітальне повідомлення з БД
    return get_welcome_message()

def get_company_text() -> str:
    # Отримуємо актуальний текст з БД
    return get_company_info()

def get_product_text(product_id: int) -> str:
    refresh_products()
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "❌ Продукт не знайдено"
    
    # Формуємо текст товару ТІЛЬКИ з поля description
    text = f"{product['name']}\n\n"
    text += f"{product['description']}\n\n"
    text += f"Ціна: {product['price']} грн/{product['unit']}"
    
    # БІЛЬШЕ НІЧОГО НЕ ДОДАЄМО!
    # Ніяких details, ніяких додаткових полів
    
    return text

def get_quick_order_text(product_id: int) -> str:
    refresh_products()
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "❌ Продукт не знайдено"
    
    return f"""
Швидке замовлення: {product['name']}

Ціна: {product['price']} грн/{product['unit']}

Як ви бажаєте, щоб ми з вами зв'язалися?

📞 Зателефонуйте мені - ми зателефонуємо вам для уточнення деталей
💬 Напишіть мені в чат - ви можете написати всі деталі тут і ми відповімо

Оберіть зручний для вас спосіб зв'язку
"""

def get_faq_text(faq_id: int) -> str:
    # Отримуємо конкретний FAQ з БД
    conn = get_db_connection()
    if not conn:
        return "❌ Помилка отримання даних"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT question, answer FROM faq WHERE id = %s', (faq_id,))
        row = cursor.fetchone()
        if row:
            return f"""
{row['question']}

{row['answer']}

Маєте інші запитання? Зв'яжіться з нами: +380932599103
            """
        return "❌ Питання не знайдено"
    except Exception as e:
        logger.error(f"Помилка отримання faq за ID: {e}")
        return "❌ Помилка отримання даних"
    finally:
        conn.close()

def get_contact_text() -> str:
    return """
Зв'язок з нами

Ми завжди раді допомогти вам!

Оберіть спосіб зв'язку:
• Телефон - для швидких запитань
• Адреса - для самовивозу
• Написати тут - швидке повідомлення в чаті

Просто напишіть нам повідомлення в цьому чаті
    """

def get_cart_text(cart_items: List[Dict]) -> str:
    if not cart_items:
        return "🛒 Ваша корзина порожня\n\nДодайте товари з каталогу!"
    
    text = "🛒 Ваша корзина\n\n"
    total = 0
    
    for i, item in enumerate(cart_items, 1):
        quantity = item["quantity"]
        product = item["product"]
        item_total = product["price"] * quantity
        text += f"{i}. {product['name']}\n"
        text += f"   Кількість: {quantity} {product['unit']}\n"
        text += f"   Ціна: {product['price']} грн/{product['unit']} × {quantity} = {item_total:.2f} грн\n\n"
        total += item_total
    
    text += f"Всього товарів: {len(cart_items)}\n"
    text += f"Загальна сума: {total:.2f} грн\n\n"
    
    if len(cart_items) >= 3:
        discount = total * 0.05
        discount_total = total - discount
        text += f"Знижка 5% за 3+ банок: -{discount:.2f} грн\n"
        text += f"До сплати: {discount_total:.2f} грн\n\n"
    
    text += "Для оформлення замовлення натисніть кнопку нижче"
    return text

def get_my_orders_text(orders: List[Dict]) -> str:
    if not orders:
        return "📋 У вас ще немає замовлень\n\nЗробіть перше замовлення в розділі 'Наші продукти'!"
    
    text = "📋 Мої замовлення\n\n"
    for order in orders:
        text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
        text += f"Сума: {order['total']:.2f} грн | Статус: {order['status']}\n"
        text += f"{'─'*40}\n"
    return text

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"👤 [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'}: /start")
        
        Database.save_user(user_id, user.first_name, user.last_name or "", user.username or "")
        
        log_user({
            "user_id": user_id,
            "first_name": user.first_name,
            "last_name": user.last_name or "",
            "username": user.username or ""
        })
        
        Database.clear_user_session(user_id)
        welcome = get_welcome_text()
        await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
        Database.save_user_session(user_id, last_section="main_menu")
        
    except Exception as e:
        logger.error(f"❌ Помилка в start: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ℹ️ Допомога: оберіть опцію з меню", reply_markup=get_main_menu())

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    # Очищаємо будь-які активні сесії
    if 'setphoto_product_id' in context.user_data:
        del context.user_data['setphoto_product_id']
        del context.user_data['setphoto_mode']
        await update.message.reply_text("❌ Встановлення фото скасовано", reply_markup=get_main_menu())
    
    Database.clear_user_session(user_id)
    welcome = get_welcome_text()
    await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
    Database.save_user_session(user_id, last_section="main_menu")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        
        chat_id = update.effective_chat.id
        user = query.from_user
        user_id = user.id
        data = query.data
        
        logger.info(f"🖱️ [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'} натиснув: {data}")
        
        Database.save_user(user_id, user.first_name, user.last_name or "", user.username or "")
        
        # Обробка кнопок "Назад"
        if data.startswith("back_"):
            back_target = data[5:]
            if back_target == "main_menu":
                welcome = get_welcome_text()
                try:
                    await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="main_menu")
            elif back_target == "products":
                products_text = "📦 Наші продукти\n\nОберіть продукт для детальної інформації:"
                try:
                    await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="products")
            elif back_target == "faq":
                faq_text = "❓ Часті запитання\n\nОберіть питання для отримання відповіді:"
                try:
                    await query.edit_message_text(faq_text, reply_markup=get_faq_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(faq_text, reply_markup=get_faq_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="faq")
            elif back_target == "contact":
                contact_text = get_contact_text()
                try:
                    await query.edit_message_text(contact_text, reply_markup=get_contact_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(contact_text, reply_markup=get_contact_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="contact")
            elif back_target == "cart":
                cart_items = Database.get_cart_items(user_id)
                cart_text = get_cart_text(cart_items)
                try:
                    await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="cart")
            elif back_target == "my_orders":
                orders = Database.get_user_orders(user_id)
                text = get_my_orders_text(orders)
                try:
                    await query.edit_message_text(text, reply_markup=get_my_orders_menu(orders), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(text, reply_markup=get_my_orders_menu(orders), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="my_orders")
            else:
                welcome = get_welcome_text()
                try:
                    await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="main_menu")
            return
        
        # Основні розділи меню
        elif data == "company":
            company_text = get_company_text()
            await query.edit_message_text(company_text, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="company")
            return
        
        elif data == "products":
            products_text = "📦 Наші продукти\n\nОберіть продукт для детальної інформації:"
            await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
            return
        
        elif data == "faq":
            faq_text = "❓ Часті запитання\n\nОберіть питання для отримання відповіді:"
            await query.edit_message_text(faq_text, reply_markup=get_faq_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="faq")
            return
        
        elif data == "cart":
            cart_items = Database.get_cart_items(user_id)
            cart_text = get_cart_text(cart_items)
            await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="cart")
            return
        
        elif data == "my_orders":
            orders = Database.get_user_orders(user_id)
            text = get_my_orders_text(orders)
            await query.edit_message_text(text, reply_markup=get_my_orders_menu(orders), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="my_orders")
            return
        
        elif data == "contact":
            contact_text = get_contact_text()
            await query.edit_message_text(contact_text, reply_markup=get_contact_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="contact")
            return
        
        elif data in ["call_us", "our_address"]:
            if data == "call_us":
                contact_info = "📞 Телефон для зв'язку:\n\n"
                contact_info += "+380932599103\n\n"
                contact_info += "Графік роботи: Пн-Пт 9:00-18:00, Сб 10:00-15:00"
            else:
                contact_info = "📍 Наша адреса:\n\n"
                contact_info += "Одеська область\n"
                contact_info += "село Великий Дальник\n"
                contact_info += "Самовивіз можливий за попереднім домовленням\n\n"
                contact_info += "Графік самовивозу: Пн-Пт 9:00-18:00, Сб 10:00-15:00"
            
            await query.edit_message_text(contact_info, reply_markup=get_back_keyboard("contact"), parse_mode='HTML')
            return
        
        elif data == "write_here":
            Database.save_user_session(user_id, "waiting_message")
            response = "💬 Написати нам тут\n\n"
            response += "Напишіть ваше повідомлення прямо в цьому чаті:\n\n"
            response += "• Питання про продукти\n"
            response += "• Консультація\n"
            response += "• Пропозиції співпраці\n"
            response += "• Інші питання\n\n"
            response += "Ми відповімо вам найближчим часом!"
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        # ============== ОБРОБНИКИ ТОВАРІВ ==============
        
        elif data.startswith("product_"):
            product_id = int(data.split("_")[1])
            refresh_products()
            product = get_product_by_id(product_id)
            product_text = get_product_text(product_id)
            
            logger.info(f"📦 Відкрито товар #{product_id}")
            
            # Отримуємо зображення з БД
            image_data = Database.get_product_image(product_id)
            
            if image_data:
                try:
                    from io import BytesIO
                    photo = BytesIO(image_data)
                    photo.name = f"product_{product_id}.jpg"
                    logger.info(f"📸 Відправляємо фото з БД для товару #{product_id}")
                    
                    await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo,
                        caption=product_text,
                        parse_mode='HTML',
                        reply_markup=get_product_detail_menu(product_id)
                    )
                    await query.message.delete()
                    Database.save_user_session(user_id, last_section=f"product_{product_id}")
                    return
                except Exception as e:
                    logger.error(f"❌ Помилка відправки фото з БД: {e}")
            
            # Якщо немає фото або помилка, відправляємо тільки текст
            await query.edit_message_text(product_text, reply_markup=get_product_detail_menu(product_id), parse_mode='HTML')
            
            Database.save_user_session(user_id, last_section=f"product_{product_id}")
            return
        
        elif data.startswith("add_to_cart_"):
            product_id = int(data.split("_")[3])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_quantity", temp_data)
            
            response = f"📦 Додавання {product['name']} до кошика\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "📊 Введіть кількість (тільки число):\n\n"
            response += f"Наприклад: 1, 2, 3 (в {product['unit']})"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        # ============== ОБРОБНИКИ ШВИДКОГО ЗАМОВЛЕННЯ ==============
        
        elif data.startswith("quick_order_"):
            product_id = int(data.split("_")[2])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            quick_order_text = get_quick_order_text(product_id)
            
            # Перевіряємо чи повідомлення має медіа (фото)
            if query.message.photo:
                # Якщо це фото, відправляємо нове повідомлення
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=quick_order_text,
                    reply_markup=get_quick_order_menu(product_id),
                    parse_mode='HTML'
                )
                # Видаляємо старе повідомлення з фото
                await query.message.delete()
            else:
                # Якщо звичайне текстове повідомлення - редагуємо
                await query.edit_message_text(
                    quick_order_text, 
                    reply_markup=get_quick_order_menu(product_id), 
                    parse_mode='HTML'
                )
            
            Database.save_user_session(user_id, last_section=f"quick_order_{product_id}")
            return
        
        elif data.startswith("quick_call_"):
            product_id = int(data.split("_")[2])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_phone_for_quick_order", temp_data)
            
            response = f"📞 Зателефонуйте мені: {product['name']}\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "📱 Введіть ваш номер телефону:\n\n"
            response += "Приклад: +380932599103 або 0932599103\n\n"
            response += "Ми зателефонуємо вам для уточнення деталей замовлення!"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        elif data.startswith("quick_chat_"):
            product_id = int(data.split("_")[2])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            order_id = Database.save_quick_order(
                user_id=user_id,
                user_name=user_name,
                username=username,
                product_id=product_id,
                product_name=product['name'],
                quantity=0,
                phone=None,
                contact_method="chat",
                message=None
            )
            
            Database.save_user_session(user_id, "waiting_message_for_quick_order", {"order_id": order_id, "product_name": product['name']})
            
            response = f"💬 Напишіть мені в чат: {product['name']}\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "💬 Просто напишіть ваше повідомлення в цей чат!\n\n"
            response += "Вкажіть:\n"
            response += "• Бажану кількість\n"
            response += "• Контактні дані\n"
            response += "• Бажаний час доставки\n\n"
            response += "Ми відповімо вам найближчим часом для уточнення деталей замовлення!"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            
            logger.info(f"\n{'='*80}")
            logger.info(f"⚡ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id} (ЧАТ - очікування повідомлення):")
            logger.info(f"👤 Клієнт: {user_name}")
            logger.info(f"📦 Продукт: {product['name']}")
            logger.info(f"🆔 User ID: {user_id}")
            logger.info(f"{'='*80}\n")
            return
        
        # ============== ОБРОБНИКИ FAQ ==============
        
        elif data.startswith("faq_"):
            try:
                faq_id = int(data.split("_")[1])
                faq_text = get_faq_text(faq_id)
                await query.edit_message_text(faq_text, reply_markup=get_back_keyboard("faq"), parse_mode='HTML')
            except (IndexError, ValueError):
                await query.edit_message_text("❌ Помилка", reply_markup=get_back_keyboard("faq"))
            return
        
        # ============== ОБРОБНИКИ КОРЗИНИ ==============
        
        elif data.startswith("remove_from_cart_"):
            cart_id = int(data.split("_")[3])
            Database.remove_from_cart(cart_id)
            cart_items = Database.get_cart_items(user_id)
            cart_text = get_cart_text(cart_items)
            await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
            return
        
        elif data == "checkout_cart":
            cart_items = Database.get_cart_items(user_id)
            
            if not cart_items:
                response = "🛒 Ваша корзина порожня\n\n"
                response += "Додайте товари з каталогу перед оформленням замовлення!"
                await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
                return
            
            Database.save_user_session(user_id, "full_order_name", {})
            
            response = "🛒 Оформлення замовлення\n\n"
            response += f"📦 У вашій корзині: {len(cart_items)} товар(ів)\n"
            
            total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
            response += f"💰 Загальна сума: {total:.2f} грн\n\n"
            response += "📝 Введіть ваше ПІБ (повне ім'я):\n\n"
            response += "Наприклад: Іванов Іван Іванович"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        elif data == "clear_cart":
            Database.clear_cart(user_id)
            response = "🗑️ Корзина очищена!\n\n"
            response += "Ваша корзина тепер порожня.\n"
            response += "Додайте товари з каталогу."
            await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        # ============== ОБРОБНИКИ ЗАМОВЛЕНЬ ==============
        
        elif data.startswith("user_order_"):
            order_id = int(data.split("_")[2])
            await query.edit_message_text(
                f"📋 Деталі замовлення #{order_id} (в розробці)",
                reply_markup=get_back_keyboard("my_orders")
            )
            return
        
        # ============== ОБРОБНИКИ ПІДТВЕРДЖЕННЯ ЗАМОВЛЕННЯ ==============
        
        elif data.startswith("confirm_order_"):
            if data == "confirm_order_yes":
                session = Database.get_user_session(user_id)
                temp_data = session["temp_data"]
                
                try:
                    order_id = Database.create_order(temp_data)
                    
                    if order_id > 0:
                        logger.info(f"\n{'='*80}")
                        logger.info(f"✅ НОВЕ ЗАМОВЛЕННЯ #{order_id}:")
                        logger.info(f"👤 Клієнт: {temp_data.get('user_name', '')}")
                        logger.info(f"📞 Телефон: {temp_data.get('phone', '')}")
                        logger.info(f"🏙️ Місто: {temp_data.get('city', '')}")
                        logger.info(f"🏣 НП: {temp_data.get('np_department', '')}")
                        logger.info(f"💰 Сума: {temp_data.get('total', 0):.2f} грн")
                        logger.info(f"🛒 Товарів: {len(temp_data.get('items', []))}")
                        logger.info(f"🆔 User ID: {user_id}")
                        logger.info(f"{'='*80}\n")
                        
                        temp_data["order_id"] = order_id
                        temp_data["status"] = "нове"
                        temp_data["order_type"] = "regular"
                        log_order(temp_data)
                        
                        await notify_admins_about_new_order(temp_data)
                        
                        Database.clear_user_session(user_id)
                        
                        text = f"✅ Замовлення оформлено!\n\n"
                        text += f"🆔 Номер замовлення: #{order_id}\n"
                        text += f"👤 ПІБ: {temp_data.get('user_name', '')}\n"
                        text += f"📱 Телефон: {temp_data.get('phone', '')}\n"
                        text += f"🏙️ Місто: {temp_data.get('city', '')}\n"
                        text += f"🏣 Відділення Нової Пошти: {temp_data.get('np_department', '')}\n"
                        text += f"💰 Сума: {temp_data.get('total', 0):.2f} грн\n\n"
                        text += "📞 Ми зв'яжемось з вами для підтвердження!\n\n"
                        text += "Дякуємо за замовлення!"
                    else:
                        text = "❌ Помилка оформлення замовлення!\n\n"
                        text += "Будь ласка, спробуйте ще раз або зв'яжіться з нами.\n\n"
                        text += "Вибачте за незручності."
                        Database.clear_user_session(user_id)
                except Exception as e:
                    logger.error(f"❌ Помилка при створенні замовлення: {e}")
                    text = "❌ Помилка оформлення замовлення!\n\n"
                    text += "Будь ласка, спробуйте ще раз.\n\n"
                    text += "Вибачте за незручності."
                    Database.clear_user_session(user_id)
            else:
                text = "❌ Замовлення скасовано\n\n"
                text += "Ви можете продовжити покупки.\n"
                text += "Ваша корзина збережена."
                Database.clear_user_session(user_id)
            
            await query.edit_message_text(text, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        else:
            logger.warning(f"⚠️ Невідомий callback: {data}")
            welcome = get_welcome_text()
            await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"❌ Помилка обробки callback: {e}")
        try:
            text = "❌ Сталася помилка\n\n"
            text += "Будь ласка, спробуйте ще раз або використайте /start"
            keyboard = get_main_menu()
            await query.edit_message_text(text, keyboard, parse_mode='HTML')
        except:
            pass

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip()
        
        logger.info(f"👤 [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'}: {text[:50]}...")
        
        Database.save_user(user_id, user.first_name, user.last_name or "", user.username or "")
        
        # Спочатку перевіряємо чи це не команда для адміна
        if text.startswith('/'):
            # Адмін-команди обробляються окремо
            return
        
        # Перевіряємо чи є активна сесія для адміна
        if 'setphoto_product_id' in context.user_data:
            await handle_admin_url(update, context)
            return
        
        # Звичайна обробка повідомлень
        if text == "/start" or text == "/cancel" or text.lower() == "скасувати":
            Database.clear_user_session(user_id)
            welcome = get_welcome_text()
            await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        if text == "/help":
            await update.message.reply_text("ℹ️ Допомога: оберіть опцію з меню", reply_markup=get_main_menu())
            return
        
        session = Database.get_user_session(user_id)
        state = session["state"]
        temp_data = session["temp_data"]
        
        if state == "waiting_quantity":
            product_id = temp_data.get("product_id")
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await update.message.reply_text("❌ Помилка: продукт не знайдено", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            success, quantity, error_msg = parse_quantity(text)
            
            if not success:
                response = f"❌ Невірний формат!\n\n{error_msg}\n\n"
                response += f"Продукт: {product['name']}\n"
                response += f"Ціна: {product['price']} грн/{product['unit']}\n\n"
                response += "📊 Введіть кількість (тільки число):\n"
                response += f"Наприклад: 1, 2, 3 (в {product['unit']})"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            Database.add_to_cart(user_id, product_id, quantity)
            Database.clear_user_session(user_id)
            
            total_price = product["price"] * quantity
            response = f"✅ {product['name']} додано до кошика!\n\n"
            response += f"📊 Кількість: {quantity} {product['unit']}\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n"
            response += f"💵 Сума: {total_price:.2f} грн\n\n"
            
            cart_items = Database.get_cart_items(user_id)
            response += f"🛒 У кошику: {len(cart_items)} товар(ів)\n\n"
            response += "Продовжуйте додавати товари або перейдіть до оформлення замовлення."
            
            await update.message.reply_text(response, parse_mode='HTML')
            
            products_text = "📦 Наші продукти\n\nОберіть продукт для детальної інформації:"
            await update.message.reply_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
            return
        
        elif state == "waiting_message":
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            Database.save_message(user_id, user_name, username, text, "повідомлення з меню")
            
            message_data = {
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "повідомлення з меню",
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            await notify_admins_about_message(message_data)
            
            log_message(message_data)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"💬 НОВЕ ПОВІДОМЛЕННЯ:")
            logger.info(f"👤 Ім'я: {user_name}")
            logger.info(f"📱 Username: {username}")
            logger.info(f"🆔 ID: {user_id}")
            logger.info(f"💬 Текст: {text}")
            logger.info(f"🕒 Час: {datetime.now().isoformat()}")
            logger.info(f"{'='*80}\n")
            
            response = "✅ Повідомлення отримано!\n\n"
            response += "Ми відповімо вам найближчим часом.\n"
            response += "Дякуємо за звернення!"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.clear_user_session(user_id)
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        elif state == "waiting_message_for_quick_order":
            order_id = temp_data.get("order_id")
            product_name = temp_data.get("product_name")
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE quick_orders 
                        SET message = %s 
                        WHERE id = %s
                    ''', (text, order_id))
                    conn.commit()
                except Exception as e:
                    logger.error(f"❌ Помилка оновлення повідомлення: {e}")
                finally:
                    conn.close()
            
            Database.save_message(user_id, user_name, username, text, "швидке замовлення")
            
            await send_combined_quick_order_notification(order_id, user_id, user_name, username, product_name, text)
            
            log_quick_order({
                "order_id": order_id,
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "phone": None,
                "product_name": product_name,
                "contact_method": "chat",
                "message": text,
                "status": "нове"
            })
            
            logger.info(f"\n{'='*80}")
            logger.info(f"✅ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id} - отримано повідомлення:")
            logger.info(f"👤 Клієнт: {user_name}")
            logger.info(f"📱 Username: {username}")
            logger.info(f"📦 Продукт: {product_name}")
            logger.info(f"💬 Повідомлення: {text}")
            logger.info(f"{'='*80}\n")
            
            response = f"✅ Дякуємо! Ваше повідомлення отримано!\n\n"
            response += f"🆔 Номер замовлення: #{order_id}\n"
            response += f"📦 Продукт: {product_name}\n"
            response += f"💬 Ваше повідомлення: {text}\n\n"
            response += "Ми зв'яжемося з вами найближчим часом для уточнення деталей!\n\n"
            response += "Дякуємо за замовлення!"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.clear_user_session(user_id)
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        elif state.startswith("full_order_"):
            if state == "full_order_name":
                temp_data["user_name"] = text
                temp_data["username"] = user.username or "немає"
                Database.save_user_session(user_id, "full_order_phone", temp_data)
                
                response = "📱 Введіть ваш номер телефону:\n\n"
                response += "Приклад: +380932599103 або 0932599103"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_phone":
                phone = text.strip()
                is_valid, formatted_phone = validate_phone(phone)
                
                if not is_valid:
                    response = f"❌ Невірний номер телефону!\n\n"
                    response += "📱 Введіть ваш номер телефону ще раз:\n"
                    response += "Приклад: +380932599103 або 0932599103"
                    await update.message.reply_text(response, parse_mode='HTML')
                    return
                
                temp_data["phone"] = formatted_phone
                Database.save_user_session(user_id, "full_order_city", temp_data)
                
                response = "🏙️ Введіть місто доставки:\n\n"
                response += "Наприклад: Київ, Львів, Одеса"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_city":
                temp_data["city"] = text
                Database.save_user_session(user_id, "full_order_np", temp_data)
                
                response = "🏣 Введіть номер відділення Нової Пошти:\n\n"
                response += "Наприклад: Відділення №25, Поштомат №12345"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_np":
                temp_data["np_department"] = text
                
                cart_items = Database.get_cart_items(user_id)
                total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                
                if len(cart_items) >= 3:
                    total = total * 0.95
                
                temp_data["total"] = total
                temp_data["order_type"] = "повне замовлення"
                temp_data["user_id"] = user_id
                
                order_items = []
                for item in cart_items:
                    order_items.append({
                        "product_name": item["product"]["name"],
                        "quantity": item["quantity"],
                        "price": item["product"]["price"]
                    })
                
                temp_data["items"] = order_items
                Database.save_user_session(user_id, "full_order_confirm", temp_data)
                
                response = "✅ Дані отримано! Перевірте інформацію:\n\n"
                response += f"👤 ПІБ: {temp_data.get('user_name', '')}\n"
                response += f"📱 Телефон: {temp_data.get('phone', '')}\n"
                response += f"🏙️ Місто: {temp_data.get('city', '')}\n"
                response += f"🏣 Відділення Нової Пошти: {text}\n"
                response += f"🛒 Товарів у кошику: {len(cart_items)}\n"
                
                if len(cart_items) >= 3:
                    original_total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                    discount = original_total * 0.05
                    response += f"🎁 Знижка 5% за 3+ банок: -{discount:.2f} грн\n"
                
                response += f"💰 Загальна сума: {total:.2f} грн\n\n"
                response += "Підтвердити замовлення?"
                
                await update.message.reply_text(response, reply_markup=get_order_confirmation_keyboard(), parse_mode='HTML')
                return
        
        elif state == "waiting_phone_for_quick_order":
            phone = text.strip()
            product_id = temp_data.get("product_id")
            
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            if not product:
                await update.message.reply_text("❌ Помилка: продукт не знайдено", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            is_valid, formatted_phone = validate_phone(phone)
            
            if not is_valid:
                response = f"❌ Невірний номер телефону!\n\n"
                response += "📱 Введіть ваш номер телефону ще раз:\n"
                response += "Приклад: +380932599103 або 0932599103"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            order_id = Database.save_quick_order(
                user_id, user_name, username, product_id, product["name"], 
                0, formatted_phone, "call", None
            )
            
            order_data = {
                "id": order_id,
                "order_type": "quick",
                "user_name": user_name,
                "username": username,
                "phone": formatted_phone,
                "product_name": product['name'],
                "contact_method": "call",
                "user_id": user_id,
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            await notify_admins_about_new_order(order_data)
            
            log_quick_order({
                "order_id": order_id,
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "phone": formatted_phone,
                "product_name": product["name"],
                "contact_method": "call",
                "message": None,
                "status": "нове"
            })
            
            logger.info(f"\n{'='*80}")
            logger.info(f"⚡ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id} (ТЕЛЕФОН):")
            logger.info(f"👤 Клієнт: {user_name}")
            logger.info(f"📞 Телефон: {formatted_phone}")
            logger.info(f"📦 Продукт: {product['name']}")
            logger.info(f"💰 Ціна: {product['price']} грн/{product['unit']}")
            logger.info(f"🆔 User ID: {user_id}")
            logger.info(f"📱 Username: {username}")
            logger.info(f"{'='*80}\n")
            
            Database.clear_user_session(user_id)
            
            response = f"✅ Швидке замовлення прийнято!\n\n"
            response += f"🆔 Номер замовлення: #{order_id}\n"
            response += f"📦 Продукт: {product['name']}\n"
            response += f"📞 Ваш телефон: {formatted_phone}\n\n"
            response += "Ми зателефонуємо вам найближчим часом для уточнення деталей!\n\n"
            response += "Дякуємо за замовлення!"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        else:
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            Database.save_message(user_id, user_name, username, text, "повідомлення в чаті")
            
            message_data = {
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "повідомлення в чаті",
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            await notify_admins_about_message(message_data)
            
            log_message(message_data)
            
            response = "✅ Повідомлення отримано!\n\n"
            response += "Ми відповімо вам найближчим часом.\n"
            response += "Дякуємо за звернення!"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"❌ Помилка в message_handler: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.error(f"⚠️ Помилка під час обробки оновлення {update}: {context.error}")
        
        if 'Conflict' in str(context.error):
            logger.warning("🔄 Виявлено конфлікт - можливо запущено дублюючий бот")
            return
        
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Виникла помилка\n\nБудь ласка, спробуйте ще раз або використайте /start",
                    parse_mode='HTML'
                )
            except:
                pass
    except Exception as e:
        logger.error(f"❌ Помилка в обробнику помилок: {e}")

def main():
    try:
        if not check_single_instance():
            logger.error("🚫 Бот вже запущено в іншому процесі! Завершуємо...")
            sys.exit(1)
        
        time.sleep(2)
        
        if not init_database():
            logger.error("❌ Не вдалося ініціалізувати базу даних")
            return
        
        refresh_products()
        
        stats = Database.get_statistics()
        logger.info("=" * 80)
        logger.info("🌱 БОТ КОМПАНІЇ 'БОНЕЛЕТ' ЗАПУЩЕНО")
        logger.info(f"🔑 Токен: {TOKEN[:10]}...")
        logger.info("=" * 80)
        logger.info("📊 Статистика:")
        logger.info(f"• Користувачів: {stats.get('total_users', 0)}")
        logger.info(f"• Замовлень: {stats.get('total_orders', 0)}")
        logger.info(f"• Повідомлень: {stats.get('total_messages', 0)}")
        logger.info(f"• Швидких замовлень: {stats.get('quick_orders', 0)}")
        logger.info(f"• Активних кошиків: {stats.get('active_carts', 0)}")
        logger.info(f"• Продуктів у базі: {len(PRODUCTS)}")
        logger.info(f"• Виручка: {stats.get('total_revenue', 0):.2f} грн")
        logger.info("=" * 80)
        logger.info("🔄 Очікування повідомлень...\n")
        
        application = Application.builder().token(TOKEN).build()
        
        # Звичайні команди
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("cancel", cancel_command))
        
        # Адмін-команди (тільки для адмінів)
        application.add_handler(CommandHandler("setphoto", setphoto_command))
        
        # Обробники
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(MessageHandler(filters.PHOTO, handle_admin_photo))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        
        application.add_error_handler(error_handler)
        
        logger.info("🚀 Запуск polling...")
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            poll_interval=2.0,
            timeout=30,
            read_timeout=30,
            connect_timeout=30,
            pool_timeout=30,
            close_loop=False
        )
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        import traceback
        logger.error(traceback.format_exc())
        time.sleep(10)

if __name__ == "__main__":
    main()

