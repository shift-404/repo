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

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# ==================== НАЛАШТУВАННЯ ЛОГУВАННЯ ====================

logging.basicConfig(
    format='%(asctime)s - ADMIN - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ==================== КИЇВСЬКИЙ ЧАСОВИЙ ПОЯС ====================

KYIV_TZ = None
try:
    import pytz
    KYIV_TZ = pytz.timezone('Europe/Kyiv')
except ImportError:
    logger.warning("⚠️ Бібліотека pytz не встановлена, використовую UTC")
    KYIV_TZ = None

def get_kyiv_time():
    """Повертає поточний час у Києві"""
    if KYIV_TZ:
        return datetime.now(KYIV_TZ)
    return datetime.now()

def format_kyiv_time(dt_str):
    """Форматує час з бази даних у київський час"""
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
    except:
        return str(dt_str)[:16]

# ==================== ЗМІННІ СЕРЕДОВИЩА ====================

TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not TOKEN:
    logger.error("❌ ADMIN_BOT_TOKEN не знайдено!")
    sys.exit(1)

MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not MAIN_BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не знайдено! Додайте BOT_TOKEN в змінні середовища")
    sys.exit(1)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

# ==================== ПІДКЛЮЧЕННЯ ДО POSTGRESQL ====================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL не знайдено! Додайте змінну середовища")
    sys.exit(1)

def get_db_connection():
    """Підключення до PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Помилка підключення до БД: {e}")
        return None

def init_database_if_empty():
    """Ініціалізує базу даних, якщо вона порожня"""
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
                unit TEXT DEFAULT 'банка',
                image TEXT DEFAULT '🥫',
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_name TEXT,
                order_id INTEGER,
                text TEXT,
                rating INTEGER DEFAULT 5,
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
        
        # Додаємо колонку message до quick_orders якщо її немає
        try:
            cursor.execute('ALTER TABLE quick_orders ADD COLUMN IF NOT EXISTS message TEXT')
        except:
            pass
        
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()['count']
        
        if count == 0:
            products = [
                (1, "Артишок маринований з зернами гірчиці", 250, "мариновані артишоки", 
                 "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
                 "банка", "🥫", "Баночка 315 мл, Маса нетто 280 г, Склад: артишок 60%, вода, оцет винний, цукор, сіль, суміш спецій, зерна гірчиці"),
                
                (2, "Артишок маринований з чилі", 250, "мариновані артишоки",
                 "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
                 "банка", "🌶️", "Баночка 315 мл, Маса нетто 280 г, Склад: артишок 60%, вода, олія оливкова, оцет винний, цукор, сіль, суміш спецій, чилі"),
                
                (3, "Паштет з артишоку", 290, "паштети",
                 "Ніжний паштет з артишоку, ідеальний для бутербродів та закусок.",
                 "банка", "🍯", "Баночка 200 г, Маса нетто 200 г, Склад: артишок, вершки, олія оливкова, спеції")
            ]
            
            for product in products:
                cursor.execute('''
                    INSERT INTO products (id, name, price, category, description, unit, image, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                ''', product)
        
        conn.commit()
        logger.info("✅ Таблиці успішно створено/перевірено!")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка створення таблиць: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# Локальна папка для звітів
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

# ==================== СЕСІЇ АДМІНІВ ====================

admin_sessions = {}
last_password_check = {}
orders_offset = {}  # Для пагінації замовлень
messages_offset = {}  # Для пагінації повідомлень
broadcast_in_progress = {}  # Для відстеження прогресу розсилки

def is_authenticated(user_id: int) -> bool:
    """Перевіряє чи авторизований адмін"""
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

# ==================== ФУНКЦІЯ ДЛЯ СПОВІЩЕНЬ АДМІНАМ ====================

async def notify_admins_about_new_order(order_data: dict):
    """Відправляє сповіщення всім адмінам про нове замовлення"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("⚠️ Немає адмінів для сповіщення")
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
        
        message += f"\n🕒 <b>Час:</b> {format_kyiv_time(order_data.get('created_at'))}"
        
        # Клавіатура з кнопками для керування замовленням
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Керувати замовленням", callback_data=f"order_view_{order_id}_{order_data.get('order_type', 'regular')}")],
            [InlineKeyboardButton("📝 Відповісти клієнту", callback_data=f"reply_order_{order_id}_{order_data.get('order_type', 'regular')}")]
        ])
        
        # Створюємо окремого бота для адмін-бота
        admin_bot = Bot(token=TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"❌ Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"✅ Сповіщення про замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"❌ Помилка в notify_admins_about_new_order: {e}")
        logger.error(traceback.format_exc())

async def notify_admins_about_message(message_data: dict):
    """Відправляє сповіщення всім адмінам про нове повідомлення від користувача"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("⚠️ Немає адмінів для сповіщення")
            return
        
        message = f"💬 <b>НОВЕ ПОВІДОМЛЕННЯ</b>\n\n"
        message += f"👤 <b>Клієнт:</b> {message_data.get('user_name', 'Н/Д')}\n"
        message += f"📱 <b>Username:</b> @{message_data.get('username', 'Н/Д')}\n"
        message += f"🆔 <b>User ID:</b> {message_data.get('user_id', 'Н/Д')}\n"
        message += f"📝 <b>Текст:</b> {message_data.get('text', 'Н/Д')}\n"
        message += f"🕒 <b>Час:</b> {format_kyiv_time(message_data.get('created_at'))}"
        
        # Клавіатура з кнопками для відповіді
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Відповісти", callback_data=f"reply_user_{message_data.get('user_id')}")],
            [InlineKeyboardButton("👤 Профіль клієнта", callback_data=f"customer_view_{message_data.get('user_id')}")]
        ])
        
        # Створюємо окремого бота для адмін-бота
        admin_bot = Bot(token=TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"❌ Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"✅ Сповіщення про повідомлення відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"❌ Помилка в notify_admins_about_message: {e}")
        logger.error(traceback.format_exc())

async def send_combined_quick_order_notification(order_id: int, user_id: int, user_name: str, username: str, product_name: str, message_text: str):
    """Відправляє одне об'єднане сповіщення про швидке замовлення з повідомленням"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Не вдалося підключитись до БД для отримання списку адмінів")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("⚠️ Немає адмінів для сповіщення")
            return
        
        # Формуємо ОДНЕ об'єднане повідомлення
        message = f"🆕 <b>НОВЕ ⚡ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id}</b>\n\n"
        message += f"👤 <b>Клієнт:</b> {user_name}\n"
        message += f"📱 <b>Username:</b> @{username}\n"
        message += f"🆔 <b>User ID:</b> {user_id}\n"
        message += f"📦 <b>Продукт:</b> {product_name}\n"
        message += f"💬 <b>Спосіб зв'язку:</b> chat\n"
        message += f"📝 <b>Повідомлення:</b> {message_text}\n"
        message += f"🕒 <b>Час:</b> {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Клавіатура з кнопками для керування замовленням
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Керувати замовленням", callback_data=f"order_view_{order_id}_quick")],
            [InlineKeyboardButton("📝 Відповісти клієнту", callback_data=f"reply_order_{order_id}_quick")]
        ])
        
        # Створюємо окремого бота для адмін-бота
        admin_bot = Bot(token=TOKEN)
        
        sent_count = 0
        for admin in admins:
            try:
                await admin_bot.send_message(
                    chat_id=admin['user_id'],
                    text=message,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"❌ Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"✅ Об'єднане сповіщення про швидке замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"❌ Помилка в send_combined_quick_order_notification: {e}")
        logger.error(traceback.format_exc())

# ==================== ФУНКЦІЇ ДЛЯ ЗАМОВЛЕНЬ ====================

def safe_get(order, key, default=0):
    """Безпечне отримання значення зі словника"""
    if key in order and order[key] is not None:
        return order[key]
    return default

def get_all_orders(include_quick: bool = True, limit: int = None, offset: int = 0):
    """Отримати всі замовлення з БД з пагінацією"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Отримуємо звичайні замовлення
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
            # Отримуємо швидкі замовлення
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
                order['city'] = safe_get(order, 'city', 'Н/Д')
                order['np_department'] = safe_get(order, 'np_department', 'Н/Д')
                all_orders.append(order)
        
        # Сортуємо всі замовлення за датою
        all_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return all_orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_recent_orders(hours: int = 1, min_count: int = 3):
    """Отримує замовлення за останні години, якщо менше min_count - додає ще"""
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
        # Додаємо ще останні замовлення
        additional = all_orders[:min_count]
        for order in additional:
            if order not in recent_orders:
                recent_orders.append(order)
    
    return recent_orders[:min_count]

def get_more_orders(user_id: int, count: int = 5):
    """Отримує наступні замовлення для пагінації"""
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
    
    text = f"{order_type} <b>№{order_id}</b> | {order['created_at'][:16]}\n"
    text += f"👤 Клієнт: {order.get('user_name', 'Н/Д')}\n"
    text += f"📞 Телефон: {order.get('phone', 'Н/Д')}\n"
    
    if order.get('order_type') == 'quick':
        text += f"📦 Продукт: {order.get('product_name', 'Н/Д')}\n"
        if order.get('message'):
            text += f"💬 Повідомлення: {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
        text += f"💰 Сума: {order.get('total', 0):.2f} грн\n"
    else:
        text += f"💰 Сума: {order.get('total', 0):.2f} грн\n"
    
    text += f"📊 Статус: {order.get('status', 'нове')}\n"
    return text

def get_orders_by_phone(phone: str):
    """Отримати замовлення за номером телефону (звичайні та швидкі)"""
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
    """Отримати нові замовлення (статус 'нове')"""
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
    """Отримати швидкі замовлення"""
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
    """Оновити статус замовлення"""
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
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення статусу: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_order_by_id(order_id: int, order_type: str = 'regular'):
    """Отримати замовлення за ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        if order_type == 'regular' or order_type == 'orders':
            cursor.execute('SELECT * FROM orders WHERE order_id = %s', (order_id,))
            order_row = cursor.fetchone()
            if not order_row:
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

# ==================== ФУНКЦІЯ ДЛЯ СПОВІЩЕННЯ КЛІЄНТА ПРО СТАТУС ====================

async def notify_customer_about_status(user_id: int, order_id: int, status: str):
    """Відправляє повідомлення клієнту про зміну статусу замовлення через основного бота"""
    try:
        status_messages = {
            "підтверджено": "✅ Ваше замовлення підтверджено! Ми розпочали його обробку.",
            "упаковано": "📦 Ваше замовлення упаковано та готове до відправки!",
            "відправлено": "🚚 Ваше замовлення відправлено! Очікуйте на повідомлення про прибуття.",
            "прибуло": "📍 Ваше замовлення прибуло у відділення Нової Пошти! Не забудьте отримати його.",
            "скасовано": "❌ На жаль, ваше замовлення було скасовано. Зв'яжіться з нами для деталей."
        }
        
        message = status_messages.get(status, f"📊 Статус вашого замовлення змінено на: {status}")
        
        # Створюємо окремого бота для основного бота
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

# ==================== ФУНКЦІЇ ДЛЯ ПОВІДОМЛЕНЬ ====================

def get_all_messages(limit: int = 50, offset: int = 0):
    """Отримати всі повідомлення від користувачів з пагінацією"""
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
        logger.error(f"❌ Помилка отримання повідомлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_message_by_id(message_id: int):
    """Отримати повідомлення за ID"""
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
        logger.error(f"❌ Помилка отримання повідомлення: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_recent_messages(hours: int = 24, min_count: int = 5):
    """Отримує повідомлення за останні години, якщо менше min_count - додає ще"""
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
        # Додаємо ще останні повідомлення
        additional = all_messages[:min_count]
        for msg in additional:
            if msg not in recent_messages:
                recent_messages.append(msg)
    
    return recent_messages[:min_count]

def get_more_messages(user_id: int, count: int = 5):
    """Отримує наступні повідомлення для пагінації"""
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
    """Отримати повідомлення конкретного користувача"""
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
        logger.error(f"❌ Помилка отримання повідомлень користувача: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def format_messages_text(messages: list) -> str:
    """Форматує повідомлення для відправки"""
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

# ==================== ФУНКЦІЇ ДЛЯ КЛІЄНТІВ ====================

def get_all_users():
    """Отримати всіх користувачів"""
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
    """Отримати користувача за номером телефону"""
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
    """Отримати користувача за ID"""
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
    """Отримати всі замовлення користувача"""
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

def get_user_messages(user_id: int):
    """Отримати повідомлення користувача"""
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
    """Отримати швидкі замовлення користувача"""
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
    """Визначення сегменту клієнта"""
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

# ==================== ФУНКЦІЇ ДЛЯ РОЗСИЛОК ====================

async def send_broadcast_to_all(admin_bot: Bot, message: str, admin_user_id: int = None):
    """Відправка розсилки ВСІМ користувачам через основного бота"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("⚠️ Немає користувачів для розсилки")
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
    
    # Відправляємо повідомлення про початок розсилки адміну
    if admin_user_id:
        try:
            await admin_bot.send_message(
                chat_id=admin_user_id,
                text=f"📢 <b>Розпочато розсилку ВСІМ користувачам</b>\n\n👥 Всього: {len(users)}",
                parse_mode='HTML'
            )
        except:
            pass
    
    # Створюємо окремого бота для основного бота
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    
    # Створюємо словник для збереження прогресу
    broadcast_in_progress[admin_user_id] = {"total": len(users), "sent": 0, "failed": 0}
    
    for i, user in enumerate(users):
        try:
            # Відправляємо через основного бота
            await main_bot.send_message(
                chat_id=user['user_id'],
                text=f"📢 <b>Оголошення</b>\n\n{message}",
                parse_mode='HTML'
            )
            sent_count += 1
            
            # Оновлюємо прогрес
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["sent"] = sent_count
            
            # Відправляємо статус кожні 10 повідомлень
            if admin_user_id and (i + 1) % 10 == 0:
                try:
                    await admin_bot.send_message(
                        chat_id=admin_user_id,
                        text=f"📢 <b>Прогрес розсилки:</b> {i + 1}/{len(users)} (✓ {sent_count} | ✗ {fail_count})",
                        parse_mode='HTML'
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)  # Невелика затримка, щоб не флудити
        except Exception as e:
            error_str = str(e)
            if "Chat not found" in error_str or "bot was blocked" in error_str:
                logger.warning(f"⚠️ Користувач {user['user_id']} заблокував бота або неактивний")
            else:
                logger.error(f"Помилка відправки користувачу {user['user_id']}: {e}")
            fail_count += 1
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["failed"] = fail_count
    
    # Видаляємо інформацію про прогрес після завершення
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

async def send_broadcast_to_segment(admin_bot: Bot, segment: str, message: str, admin_user_id: int = None):
    """Відправка розсилки по сегменту клієнтів через основного бота"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("⚠️ Немає користувачів для розсилки")
        return 0, 0
    
    # Фільтруємо користувачів за сегментом
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
    
    # Створюємо окремого бота для основного бота
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    
    # Створюємо словник для збереження прогресу
    broadcast_in_progress[admin_user_id] = {"total": len(filtered_users), "sent": 0, "failed": 0}
    
    for i, user in enumerate(filtered_users):
        try:
            # Відправляємо через основного бота
            await main_bot.send_message(
                chat_id=user['user_id'],
                text=f"📢 <b>Оголошення</b>\n\n{message}",
                parse_mode='HTML'
            )
            sent_count += 1
            
            # Оновлюємо прогрес
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["sent"] = sent_count
            
            # Відправляємо статус кожні 10 повідомлень
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
    
    # Видаляємо інформацію про прогрес після завершення
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

# ==================== ФУНКЦІЇ ДЛЯ ВІДГУКІВ ====================

async def send_review_request(user_id: int, order_id: int = None):
    """Відправка запиту на відгук через основного бота"""
    text = "⭐ <b>Ваша думка важлива для нас!</b>\n\n"
    text += "Будемо вдячні, якщо ви залишите відгук про наші продукти:\n\n"
    text += "• Якість товару\n"
    text += "• Швидкість доставки\n"
    text += "• Обслуговування\n\n"
    text += "Напишіть ваш відгук прямо в цьому чаті, і ми опублікуємо його на наших сторінках!\n\n"
    text += "<i>Дякуємо, що обираєте Бонелет! 🌱</i>"
    
    if order_id:
        text = f"📦 <b>Замовлення #{order_id}</b>\n\n" + text
    
    try:
        # Створюємо окремого бота для основного бота
        main_bot = Bot(token=MAIN_BOT_TOKEN)
        
        await main_bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode='HTML'
        )
        return True
    except Exception as e:
        logger.error(f"Помилка відправки запиту на відгук: {e}")
        return False

def get_all_reviews(limit: int = None):
    """Отримати всі відгуки"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        if limit:
            cursor.execute('SELECT * FROM reviews ORDER BY created_at DESC LIMIT %s', (limit,))
        else:
            cursor.execute('SELECT * FROM reviews ORDER BY created_at DESC')
        rows = cursor.fetchall()
        
        reviews = []
        for row in rows:
            review = dict(row)
            review['created_at'] = format_kyiv_time(review.get('created_at'))
            reviews.append(review)
        
        return reviews
    except Exception as e:
        logger.error(f"❌ Помилка отримання відгуків: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def format_reviews_text(reviews: list) -> str:
    """Форматує відгуки для відправки"""
    if not reviews:
        return "⭐ Відгуків поки немає"
    
    text = "⭐ <b>ВІДГУКИ КЛІЄНТІВ</b>\n\n"
    for i, review in enumerate(reviews, 1):
        rating_stars = "⭐" * review['rating']
        text += f"<b>{i}. {review['user_name']}</b> {rating_stars}\n"
        text += f"📅 {review['created_at'][:16]}\n"
        text += f"💬 {review['text'][:100]}{'...' if len(review['text']) > 100 else ''}\n"
        if review.get('order_id'):
            text += f"📦 Замовлення №{review['order_id']}\n"
        text += f"{'─'*40}\n"
    
    return text

def generate_reviews_file(reviews: list) -> bytes:
    """Генерує файл з відгуками"""
    output = StringIO()
    output.write("ВІДГУКИ КЛІЄНТІВ\n")
    output.write("=" * 80 + "\n")
    output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"Всього відгуків: {len(reviews)}\n")
    output.write("=" * 80 + "\n\n")
    
    for i, review in enumerate(reviews, 1):
        rating_stars = "⭐" * review['rating']
        output.write(f"{i}. {review['user_name']} {rating_stars}\n")
        output.write(f"Дата: {review['created_at'][:16]}\n")
        output.write(f"Текст: {review['text']}\n")
        if review.get('order_id'):
            output.write(f"Замовлення: №{review['order_id']}\n")
        output.write("-" * 40 + "\n")
    
    return output.getvalue().encode('utf-8')

# ==================== ФУНКЦІЇ ДЛЯ СТАТИСТИКИ ====================

def get_statistics():
    """Отримати статистику"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        
        # Загальна кількість замовлень (звичайні)
        cursor.execute("SELECT COUNT(*) FROM orders")
        regular_orders = cursor.fetchone()['count'] or 0
        
        # Загальна кількість швидких замовлень
        cursor.execute("SELECT COUNT(*) FROM quick_orders")
        quick_orders_count = cursor.fetchone()['count'] or 0
        
        # Загальна кількість користувачів
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()['count'] or 0
        
        # Загальна кількість повідомлень
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()['count'] or 0
        
        # Загальна кількість відгуків
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()['count'] or 0
        
        # Сума замовлень (звичайні)
        cursor.execute("SELECT COALESCE(SUM(total), 0) FROM orders")
        regular_revenue = cursor.fetchone()['coalesce'] or 0
        
        # Сума швидких замовлень
        cursor.execute("SELECT COALESCE(SUM(total), 0) FROM quick_orders")
        quick_revenue = cursor.fetchone()['coalesce'] or 0
        
        total_orders = regular_orders + quick_orders_count
        total_revenue = regular_revenue + quick_revenue
        
        avg_check = total_revenue / total_orders if total_orders > 0 else 0
        
        # Статуси звичайних замовлень
        cursor.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
        rows = cursor.fetchall()
        orders_by_status = {row['status']: row['count'] for row in rows}
        
        # Статуси швидких замовлень
        cursor.execute("SELECT status, COUNT(*) FROM quick_orders GROUP BY status")
        quick_rows = cursor.fetchall()
        for row in quick_rows:
            status = row['status']
            if status in orders_by_status:
                orders_by_status[status] += row['count']
            else:
                orders_by_status[status] = row['count']
        
        # Замовлення за останні 30 днів (звичайні)
        cursor.execute('''
            SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(total), 0) FROM orders 
            WHERE created_at >= NOW() - INTERVAL '30 days'
        ''')
        last_30_days_regular = cursor.fetchone()
        
        # Замовлення за останні 30 днів (швидкі)
        cursor.execute('''
            SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(total), 0) FROM quick_orders 
            WHERE created_at >= NOW() - INTERVAL '30 days'
        ''')
        last_30_days_quick = cursor.fetchone()
        
        last_30_days_count = (last_30_days_regular['coalesce'] or 0) + (last_30_days_quick['coalesce'] or 0)
        last_30_days_sum = (last_30_days_regular['coalesce_2'] or 0) + (last_30_days_quick['coalesce_2'] or 0)
        
        # Сегментація клієнтів
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
            "total_reviews": total_reviews,
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

# ==================== ФУНКЦІЇ ДЛЯ ТОВАРІВ ====================

def get_all_products():
    """Отримати всі товари з БД"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM products ORDER BY id')
        rows = cursor.fetchall()
        
        products = []
        for row in rows:
            product = dict(row)
            if product.get('created_at'):
                product['created_at'] = format_kyiv_time(product.get('created_at'))
            products.append(product)
        return products
    except Exception as e:
        logger.error(f"❌ Помилка отримання товарів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_product_by_id(product_id: int):
    """Отримати товар за ID"""
    products = get_all_products()
    for product in products:
        if product["id"] == product_id:
            return product
    return None

def update_product(product_id: int, **kwargs):
    """Оновлює товар"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            fields.append(f"{key} = %s")
            values.append(value)
        
        values.append(product_id)
        query = f"UPDATE products SET {', '.join(fields)} WHERE id = %s"
        cursor.execute(query, values)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Помилка оновлення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, image: str, details: str):
    """Додає новий товар"""
    logger.info(f"🔄 Спроба додати товар: {name}, ціна: {price}, категорія: {category}")
    
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Не вдалося підключитись до БД")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, image, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, price, category, description, unit, image, details))
        
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
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM products WHERE id = %s', (product_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Помилка видалення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# ==================== ФУНКЦІЇ ДЛЯ АДМІНІВ ====================

def get_all_admins():
    """Отримує список адмінів"""
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
        logger.error(f"❌ Помилка отримання адмінів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def add_admin(user_id: int, username: str = "", added_by: int = 0):
    """Додає нового адміна"""
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
        return True
    except Exception as e:
        logger.error(f"❌ Помилка додавання адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def remove_admin(user_id: int):
    """Видаляє адміна"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM admins WHERE user_id = %s', (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Помилка видалення адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def is_admin(user_id: int) -> bool:
    """Перевіряє чи є користувач адміном"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = %s', (user_id,))
        count = cursor.fetchone()['count']
        return count > 0
    except Exception as e:
        logger.error(f"❌ Помилка перевірки адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# ==================== ФУНКЦІЇ ГЕНЕРАЦІЇ ЗВІТІВ ====================

def generate_orders_report(orders: list, format: str = "txt"):
    """Згенерувати звіт по замовленнях"""
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

def generate_users_report(users: list, format: str = "txt"):
    """Згенерувати звіт по користувачах"""
    if format == "txt":
        output = StringIO()
        output.write("ЗВІТ ПО КОРИСТУВАЧАХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього користувачів: {len(users)}\n")
        output.write("=" * 80 + "\n\n")
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            quick_orders = get_user_quick_orders(user['user_id'])
            all_orders = orders + quick_orders
            segment = get_customer_segment(user, all_orders)
            output.write(f"ID: {user['user_id']}\n")
            output.write(f"Ім'я: {user['first_name']} {user['last_name']}\n")
            output.write(f"Username: @{user['username']}\n")
            output.write(f"Дата реєстрації: {user['created_at'][:16]}\n")
            output.write(f"Сегмент: {segment}\n")
            output.write(f"Замовлень: {len(all_orders)}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID', 'Імя', 'Прізвище', 'Username', 'Дата реєстрації', 'Сегмент', 'Замовлень'])
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            quick_orders = get_user_quick_orders(user['user_id'])
            all_orders = orders + quick_orders
            segment = get_customer_segment(user, all_orders)
            writer.writerow([
                user['user_id'],
                user['first_name'],
                user['last_name'],
                user['username'],
                user['created_at'][:16],
                segment,
                len(all_orders)
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_quick_orders_report(orders: list, format: str = "txt"):
    """Згенерувати звіт по швидких замовленнях"""
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
    """Згенерувати звіт по статистиці"""
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
        output.write(f"💬 Повідомлень: {stats.get('total_messages', 0)}\n")
        output.write(f"⭐ Відгуків: {stats.get('total_reviews', 0)}\n\n")
        
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
    """Згенерувати звіт по повідомленнях"""
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

# ==================== ФУНКЦІЇ КЛАВІАТУР ====================

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

def get_main_menu():
    keyboard = [
        [{"text": "📦 Товари", "callback_data": "admin_products"}],
        [{"text": "📋 Замовлення", "callback_data": "admin_orders"}],
        [{"text": "👥 Клієнти", "callback_data": "admin_customers"}],
        [{"text": "💬 Повідомлення", "callback_data": "admin_messages"}],
        [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
        [{"text": "📁 Звіти", "callback_data": "admin_reports"}],
        [{"text": "📢 Розсилки", "callback_data": "admin_broadcast"}],
        [{"text": "⭐ Відгуки", "callback_data": "admin_reviews"}],
        [{"text": "👑 Адміни", "callback_data": "admin_manage_admins"}],
        [{"text": "⚙️ Налаштування", "callback_data": "admin_settings"}],
        [{"text": "🔐 Вийти", "callback_data": "admin_logout"}]
    ]
    return create_inline_keyboard(keyboard)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    buttons = [[{"text": "🔙 Назад", "callback_data": f"back_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu():
    keyboard = [
        [{"text": "📋 Список товарів", "callback_data": "admin_product_list"}],
        [{"text": "➕ Додати товар", "callback_data": "admin_product_add"}],
        [{"text": "✏️ Редагувати товар", "callback_data": "admin_product_edit"}],
        [{"text": "🗑 Видалити товар", "callback_data": "admin_product_delete"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_menu():
    keyboard = [
        [{"text": "📋 Останні замовлення", "callback_data": "admin_order_recent"}],
        [{"text": "📋 Всі замовлення", "callback_data": "admin_order_all"}],
        [{"text": "🆕 Нові замовлення", "callback_data": "admin_order_new"}],
        [{"text": "⚡ Швидкі замовлення", "callback_data": "admin_order_quick"}],
        [{"text": "📞 Пошук за телефоном", "callback_data": "admin_order_by_phone"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customers_menu():
    keyboard = [
        [{"text": "📋 Всі клієнти", "callback_data": "admin_customers_all"}],
        [{"text": "🔍 Пошук за телефоном", "callback_data": "admin_customer_search"}],
        [{"text": "👑 VIP клієнти", "callback_data": "admin_customers_vip"}],
        [{"text": "⭐ Постійні клієнти", "callback_data": "admin_customers_regular"}],
        [{"text": "🆕 Нові клієнти", "callback_data": "admin_customers_new"}],
        [{"text": "💤 Неактивні клієнти", "callback_data": "admin_customers_inactive"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_messages_menu():
    keyboard = [
        [{"text": "📋 Останні повідомлення", "callback_data": "admin_messages_recent"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "admin_messages_all"}],
        [{"text": "📁 Всі повідомлення файлом", "callback_data": "messages_all_file"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_menu():
    keyboard = [
        [{"text": "📢 Всім клієнтам", "callback_data": "broadcast_all"}],
        [{"text": "👑 VIP клієнтам", "callback_data": "broadcast_vip"}],
        [{"text": "⭐ Постійним клієнтам", "callback_data": "broadcast_regular"}],
        [{"text": "🆕 Новим клієнтам", "callback_data": "broadcast_new"}],
        [{"text": "💤 Неактивним клієнтам", "callback_data": "broadcast_inactive"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_reviews_menu():
    keyboard = [
        [{"text": "📋 Останні відгуки", "callback_data": "recent_reviews"}],
        [{"text": "🔢 Вказати кількість", "callback_data": "reviews_count"}],
        [{"text": "📁 Всі відгуки файлом", "callback_data": "reviews_all_file"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_reports_menu():
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
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_admins_menu():
    keyboard = [
        [{"text": "📋 Список адмінів", "callback_data": "admin_list"}],
        [{"text": "➕ Додати адміна", "callback_data": "admin_add"}],
        [{"text": "🗑 Видалити адміна", "callback_data": "admin_remove"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_settings_menu():
    keyboard = [
        [{"text": "🔑 Змінити пароль", "callback_data": "admin_settings_password"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_actions_menu(order_id: int, order_type: str = 'regular'):
    keyboard = [
        [{"text": "✅ Підтвердити", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "📦 Упаковано", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "🚚 Відправлено", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "📍 Прибуло", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "❌ Скасувати", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "⭐ Запитати відгук", "callback_data": f"order_review_{order_id}_{order_type}"}],
        [{"text": "📝 Відповісти", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "🔙 Назад", "callback_data": "admin_order_all"}]
    ]
    return create_inline_keyboard(keyboard)

def get_message_actions_menu(message_id: int, user_id: int):
    keyboard = [
        [{"text": "📝 Відповісти", "callback_data": f"reply_user_{user_id}"}],
        [{"text": "👤 Профіль клієнта", "callback_data": f"customer_view_{user_id}"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "admin_messages_all"}],
        [{"text": "🔙 Назад", "callback_data": "admin_messages"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customer_actions_menu(user_id: int):
    keyboard = [
        [{"text": "📋 Історія замовлень", "callback_data": f"customer_orders_{user_id}"}],
        [{"text": "💬 Повідомлення", "callback_data": f"customer_messages_{user_id}"}],
        [{"text": "📢 Надіслати повідомлення", "callback_data": f"customer_message_{user_id}"}],
        [{"text": "⭐ Запитати відгук", "callback_data": f"customer_review_{user_id}"}],
        [{"text": "👑 Зробити адміном", "callback_data": f"customer_make_admin_{user_id}"}],
        [{"text": "🔙 Назад", "callback_data": "admin_customers"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_status_keyboard(order_id: int, order_type: str = 'regular'):
    keyboard = [
        [{"text": "✅ Підтвердити", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "📦 Упаковано", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "🚚 Відправлено", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "📍 Прибуло", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "❌ Скасувати", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "⭐ Запитати відгук", "callback_data": f"order_review_{order_id}_{order_type}"}],
        [{"text": "📝 Відповісти", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "🔙 Назад", "callback_data": "admin_order_all"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_pagination_keyboard(user_id: int, has_more: bool = True):
    """Клавіатура для пагінації замовлень"""
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 замовлень", "callback_data": "admin_order_more"}])
    buttons.append([{"text": "🔙 Назад до меню", "callback_data": "admin_orders"}])
    return create_inline_keyboard(buttons)

def get_messages_pagination_keyboard(user_id: int, has_more: bool = True):
    """Клавіатура для пагінації повідомлень"""
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 повідомлень", "callback_data": "admin_messages_more"}])
    buttons.append([{"text": "🔙 Назад до меню", "callback_data": "admin_messages"}])
    return create_inline_keyboard(buttons)

def get_reply_keyboard(order_id: int = None, user_id: int = None):
    """Клавіатура для відповіді"""
    buttons = []
    if order_id:
        buttons.append([{"text": "📦 Переглянути замовлення", "callback_data": f"order_view_{order_id}"}])
    if user_id:
        buttons.append([{"text": "👤 Профіль клієнта", "callback_data": f"customer_view_{user_id}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "admin_back_main"}])
    return create_inline_keyboard(buttons)

def get_reviews_back_keyboard() -> InlineKeyboardMarkup:
    buttons = [[{"text": "🔙 Назад до відгуків", "callback_data": "admin_reviews"}]]
    return create_inline_keyboard(buttons)

def get_messages_back_keyboard() -> InlineKeyboardMarkup:
    buttons = [[{"text": "🔙 Назад до повідомлень", "callback_data": "admin_messages"}]]
    return create_inline_keyboard(buttons)

# ==================== ПЕРЕВІРКА АВТОРИЗАЦІЇ ====================

def is_authenticated(user_id: int) -> bool:
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

# ==================== ОБРОБНИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Доступ заборонено\n\nВи не маєте прав адміністратора.")
        return
    
    admin_sessions[user_id] = {"state": "waiting_password"}
    await update.message.reply_text("🔐 Вхід в адмін-панель Бонелет\n\nБудь ласка, введіть пароль:")

async def check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    if user_id not in admin_sessions or admin_sessions[user_id].get("state") != "waiting_password":
        return
    
    if text == ADMIN_PASSWORD:
        admin_sessions[user_id] = {"state": "authenticated", "authenticated_at": get_kyiv_time().isoformat()}
        last_password_check[user_id] = get_kyiv_time()
        
        if not is_admin(user_id):
            add_admin(user_id, user.username or "", user_id)
        
        await update.message.reply_text("✅ Пароль прийнято!\n\nЛаскаво просимо до адмін-панелі.", reply_markup=get_main_menu())
    else:
        await update.message.reply_text("❌ Невірний пароль!\n\nСпробуйте ще раз або напишіть /start")
        admin_sessions.pop(user_id, None)

# ==================== ОБРОБНИКИ КНОПОК ====================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        user_id = user.id
        data = query.data
        
        logger.info(f"🖱️ Адмін {user_id} натиснув: {data}")
        
        if not is_authenticated(user_id):
            await query.edit_message_text("❌ Сесія закінчилась\n\nНапишіть /start для повторного входу")
            return
        
        # Скидаємо offset при новому запиті
        if data == "admin_orders" or data == "admin_back_main" or data == "admin_messages":
            orders_offset.pop(user_id, None)
            messages_offset.pop(user_id, None)
        
        # ===== ГОЛОВНЕ МЕНЮ =====
        if data == "admin_back_main":
            await query.edit_message_text("🔐 Адмін-панель Бонелет\n\nОберіть розділ:", reply_markup=get_main_menu())
            return
        
        elif data == "admin_logout":
            admin_sessions.pop(user_id, None)
            last_password_check.pop(user_id, None)
            await query.edit_message_text("🔐 Ви вийшли з адмін-панелі\n\nДля повторного входу напишіть /start")
            return
        
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
                    text += f"ID: {p['id']}\nНазва: {p['name']}\nЦіна: {p['price']} грн\nКатегорія: {p['category']}\n{'─'*30}\n"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_product_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_product_name"}
            await query.edit_message_text("➕ Додавання нового товару\n\nВведіть назву товару:", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif data == "admin_product_edit":
            products = get_all_products()
            if not products:
                await query.edit_message_text("❌ Товарів не знайдено", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([InlineKeyboardButton(f"{p['id']}. {p['name'][:30]}", callback_data=f"edit_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_products")])
            await query.edit_message_text("✏️ Редагування товару\n\nОберіть товар для редагування:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("edit_product_"):
            product_id = int(data.split("_")[2])
            product = get_product_by_id(product_id)
            if not product:
                await query.edit_message_text("❌ Товар не знайдено", reply_markup=get_products_menu())
                return
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
            keyboard = [
                [InlineKeyboardButton("📝 Назва", callback_data=f"edit_field_name_{product_id}")],
                [InlineKeyboardButton("💰 Ціна", callback_data=f"edit_field_price_{product_id}")],
                [InlineKeyboardButton("📋 Опис", callback_data=f"edit_field_desc_{product_id}")],
                [InlineKeyboardButton("🏷 Категорія", callback_data=f"edit_field_cat_{product_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_product_edit")]
            ]
            await query.edit_message_text(
                f"✏️ Редагування товару #{product_id}\n\nНазва: {product['name']}\nЦіна: {product['price']} грн\n\nОберіть поле для редагування:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        elif data.startswith("edit_field_"):
            parts = data.split("_")
            field = parts[2]
            product_id = int(parts[3])
            admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_{field}", "product_id": product_id}
            field_names = {"name": "назву", "price": "ціну", "desc": "опис", "cat": "категорію"}
            await query.edit_message_text(f"✏️ Введіть нову {field_names.get(field, '')}:", reply_markup=get_back_keyboard(f"edit_product_{product_id}"))
            return
        
        elif data == "admin_product_delete":
            products = get_all_products()
            if not products:
                await query.edit_message_text("❌ Товарів не знайдено", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([InlineKeyboardButton(f"❌ {p['id']}. {p['name'][:30]}", callback_data=f"delete_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_products")])
            await query.edit_message_text("🗑 Видалення товару\n\nОберіть товар для видалення:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("delete_product_"):
            product_id = int(data.split("_")[2])
            keyboard = [
                [InlineKeyboardButton("✅ Так, видалити", callback_data=f"confirm_delete_{product_id}")],
                [InlineKeyboardButton("❌ Ні, скасувати", callback_data="admin_products")]
            ]
            await query.edit_message_text(f"🗑 Підтвердження видалення\n\nВи дійсно хочете видалити товар #{product_id}?", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("confirm_delete_"):
            product_id = int(data.split("_")[2])
            if delete_product(product_id):
                text = "✅ Товар успішно видалено!"
            else:
                text = "❌ Помилка при видаленні товару"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ===== ЗАМОВЛЕННЯ =====
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
                await query.edit_message_text(text, reply_markup=get_back_keyboard("admin_orders"), parse_mode='HTML')
                return
            
            text = "📋 <b>ЩЕ ЗАМОВЛЕННЯ</b>\n\n"
            for order in more_orders:
                text += format_order_text(order) + f"{'─'*40}\n"
            
            # Перевіряємо чи є ще замовлення
            next_orders = get_all_orders(include_quick=True, limit=1, offset=orders_offset.get(user_id, 0))
            has_more = len(next_orders) > 0
            
            await query.edit_message_text(text, reply_markup=get_orders_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_order_all":
            orders = get_all_orders(include_quick=True, limit=10)
            if not orders:
                text = "📋 Всі замовлення\n\nЗамовлень не знайдено."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            text = f"📋 Всі замовлення\n\nВсього: {len(get_all_orders(include_quick=True))}\n\n"
            for order in orders[:10]:
                text += format_order_text(order) + f"{'─'*40}\n"
            
            if len(get_all_orders(include_quick=True)) > 10:
                text += f"... та ще більше замовлень"
            
            keyboard = [
                [InlineKeyboardButton("🔍 Детально", callback_data="admin_order_details")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
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
                keyboard.append([InlineKeyboardButton(
                    f"{type_prefix} №{display_id} - {customer_name} - {order.get('total', 0):.0f} грн", 
                    callback_data=f"order_view_{display_id}_{order_type}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")])
            await query.edit_message_text("📋 Детальний перегляд замовлень\n\nОберіть замовлення:", reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_order_by_phone":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_orders_by_phone"}
            await query.edit_message_text("📞 Пошук замовлень за телефоном\n\nВведіть номер телефону клієнта:", reply_markup=get_back_keyboard("admin_orders"))
            return
        
        elif data.startswith("order_view_"):
            parts = data.split("_")
            order_id = int(parts[2])
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
            
            await query.edit_message_text(text, reply_markup=get_order_status_keyboard(order_id, order_type), parse_mode='HTML')
            return
        
        elif data.startswith("reply_order_"):
            parts = data.split("_")
            order_id = int(parts[2])
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
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "підтверджено", order_type):
                text = f"✅ Замовлення №{order_id} підтверджено!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "підтверджено")
            else:
                text = f"❌ Помилка при підтвердженні замовлення"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_packed_"):
            parts = data.split("_")
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "упаковано", order_type):
                text = f"📦 Замовлення №{order_id} упаковано!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "упаковано")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_shipped_"):
            parts = data.split("_")
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "відправлено", order_type):
                text = f"🚚 Замовлення №{order_id} відправлено!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "відправлено")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_arrived_"):
            parts = data.split("_")
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "прибуло", order_type):
                text = f"📍 Замовлення №{order_id} прибуло у відділення!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "прибуло")
            else:
                text = f"❌ Помилка при оновленні статусу"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_cancel_"):
            parts = data.split("_")
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "скасовано", order_type):
                text = f"❌ Замовлення №{order_id} скасовано!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "скасовано")
            else:
                text = f"❌ Помилка при скасуванні замовлення"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_review_"):
            parts = data.split("_")
            order_id = int(parts[2])
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            order = get_order_by_id(order_id, order_type)
            if order and order['user_id']:
                if await send_review_request(order['user_id'], order_id):
                    text = f"✅ Запит на відгук для замовлення №{order_id} надіслано!"
                else:
                    text = f"❌ Помилка при надсиланні запиту"
            else:
                text = "❌ Користувача не знайдено"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ===== ПОВІДОМЛЕННЯ =====
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
            else:
                text = "💬 <b>ОСТАННІ ПОВІДОМЛЕННЯ</b>\n\n"
                for msg in recent_messages:
                    text += f"💬 <b>Повідомлення #{msg['id']}</b>\n"
                    text += f"👤 Клієнт: {msg['user_name']} (@{msg['username']})\n"
                    text += f"📅 Час: {msg['created_at'][:16]}\n"
                    text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                    text += f"{'─'*40}\n"
            
            all_messages = get_all_messages(limit=5, offset=0)
            has_more = len(all_messages) >= 5
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_more":
            more_messages = get_more_messages(user_id, count=5)
            if not more_messages:
                text = "💬 Більше повідомлень не знайдено."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("admin_messages"), parse_mode='HTML')
                return
            
            text = "💬 <b>ЩЕ ПОВІДОМЛЕННЯ</b>\n\n"
            for msg in more_messages:
                text += f"💬 <b>Повідомлення #{msg['id']}</b>\n"
                text += f"👤 Клієнт: {msg['user_name']} (@{msg['username']})\n"
                text += f"📅 Час: {msg['created_at'][:16]}\n"
                text += f"📝 {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                text += f"{'─'*40}\n"
            
            # Перевіряємо чи є ще повідомлення
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_messages")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
        
        elif data.startswith("message_view_"):
            message_id = int(data.split("_")[2])
            msg = get_message_by_id(message_id)
            if not msg:
                await query.edit_message_text("❌ Повідомлення не знайдено", reply_markup=get_messages_menu())
                return
            
            text = format_message_text(msg)
            await query.edit_message_text(
                text,
                reply_markup=get_message_actions_menu(message_id, msg['user_id']),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("reply_user_"):
            user_id_to_reply = int(data.split("_")[2])
            user_data = get_user_by_id(user_id_to_reply)
            
            admin_sessions[user_id] = {
                "state": "authenticated",
                "action": "reply_to_user",
                "customer_id": user_id_to_reply
            }
            await query.edit_message_text(
                f"📝 Відповідь користувачу {user_data['first_name'] if user_data else '#'}{user_id_to_reply}\n\nВведіть текст повідомлення:",
                reply_markup=get_back_keyboard("admin_messages")
            )
            return
        
        elif data == "messages_all_file":
            messages = get_all_messages(limit=1000)
            if not messages:
                await query.edit_message_text("💬 Повідомлень поки немає", reply_markup=get_messages_back_keyboard())
                return
            file_data = generate_messages_report(messages, "txt")
            await query.message.reply_document(
                document=file_data,
                filename=f"all_messages_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="💬 Всі повідомлення користувачів"
            )
            await query.edit_message_text("✅ Файл з повідомленнями згенеровано!", reply_markup=get_messages_back_keyboard())
            return
        
        # ===== КЛІЄНТИ =====
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_customer_search":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_customer_by_phone"}
            await query.edit_message_text("🔍 Пошук клієнта за телефоном\n\nВведіть номер телефону:", reply_markup=get_back_keyboard("admin_customers"))
            return
        
        elif data.startswith("customer_view_"):
            customer_id = int(data.split("_")[2])
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
                reply_markup=get_customer_actions_menu(customer_id)
            )
            return
        
        elif data.startswith("customer_orders_"):
            customer_id = int(data.split("_")[2])
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
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("customer_messages_"):
            customer_id = int(data.split("_")[2])
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
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("customer_message_"):
            customer_id = int(data.split("_")[2])
            admin_sessions[user_id] = {"state": "authenticated", "action": "send_message_to_customer", "customer_id": customer_id}
            await query.edit_message_text("📢 Надіслати повідомлення клієнту\n\nВведіть текст повідомлення:", reply_markup=get_back_keyboard(f"customer_view_{customer_id}"))
            return
        
        elif data.startswith("customer_review_"):
            customer_id = int(data.split("_")[2])
            if await send_review_request(customer_id):
                text = "✅ Запит на відгук надіслано!"
            else:
                text = "❌ Помилка при надсиланні запиту"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("customer_make_admin_"):
            customer_id = int(data.split("_")[3])
            user = get_user_by_id(customer_id)
            if user:
                if add_admin(customer_id, user['username'], user_id):
                    text = f"✅ Користувача {user['first_name']} додано до адмінів!"
                else:
                    text = "❌ Помилка при додаванні адміна"
            else:
                text = "❌ Користувача не знайдено"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ===== РОЗСИЛКИ =====
        elif data == "admin_broadcast":
            await query.edit_message_text("📢 Розсилка повідомлень\n\nОберіть цільову аудиторію:", reply_markup=get_broadcast_menu())
            return
        
        elif data.startswith("broadcast_"):
            segment = data.replace("broadcast_", "")
            admin_sessions[user_id] = {"state": "authenticated", "action": "broadcast", "segment": segment}
            await query.edit_message_text(f"📢 Розсилка для сегменту: {segment}\n\nВведіть текст повідомлення для розсилки:", reply_markup=get_back_keyboard("admin_broadcast"))
            return
        
        # ===== ВІДГУКИ =====
        elif data == "admin_reviews":
            await query.edit_message_text("⭐ Керування відгуками\n\nОберіть дію:", reply_markup=get_reviews_menu())
            return
        
        elif data == "recent_reviews":
            reviews = get_all_reviews(limit=10)
            if not reviews:
                text = "⭐ Відгуків поки немає"
            else:
                text = format_reviews_text(reviews)
            await query.edit_message_text(text, reply_markup=get_reviews_back_keyboard(), parse_mode='HTML')
            return
        
        elif data == "reviews_count":
            admin_sessions[user_id] = {"state": "authenticated", "action": "reviews_count"}
            await query.edit_message_text("🔢 Введіть кількість останніх відгуків для перегляду:", reply_markup=get_back_keyboard("admin_reviews"))
            return
        
        elif data == "reviews_all_file":
            reviews = get_all_reviews()
            if not reviews:
                await query.edit_message_text("⭐ Відгуків поки немає", reply_markup=get_reviews_back_keyboard())
                return
            file_data = generate_reviews_file(reviews)
            await query.message.reply_document(
                document=file_data,
                filename=f"all_reviews_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="⭐ Всі відгуки клієнтів"
            )
            await query.edit_message_text("✅ Файл з відгуками згенеровано!", reply_markup=get_reviews_back_keyboard())
            return
        
        # ===== ЗВІТИ =====
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
            report_data = generate_users_report(users, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"users_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="👥 Звіт по клієнтах"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_users_csv":
            users = get_all_users()
            report_data = generate_users_report(users, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"users_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="👥 Звіт по клієнтах (CSV)"
            )
            await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
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
        
        # ===== АДМІНИ =====
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_admin"}
            await query.edit_message_text("➕ Додавання адміністратора\n\nВведіть Telegram ID користувача:", reply_markup=get_back_keyboard("admin_manage_admins"))
            return
        
        elif data == "admin_remove":
            admins = get_all_admins()
            if not admins:
                await query.edit_message_text("❌ Адмінів не знайдено", reply_markup=get_admins_menu())
                return
            keyboard = []
            for admin in admins:
                if admin['user_id'] != user_id:
                    keyboard.append([InlineKeyboardButton(f"❌ {admin['user_id']} - @{admin['username']}", callback_data=f"remove_admin_{admin['user_id']}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")])
            await query.edit_message_text("🗑 Видалення адміністратора\n\nОберіть адміна для видалення:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("remove_admin_"):
            admin_id = int(data.split("_")[2])
            if admin_id == user_id:
                text = "❌ Не можна видалити самого себе!"
            elif remove_admin(admin_id):
                text = "✅ Адміна успішно видалено!"
            else:
                text = "❌ Помилка при видаленні адміна"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ===== СТАТИСТИКА =====
        elif data == "admin_stats":
            stats = get_statistics()
            text = "📊 СТАТИСТИКА\n\n"
            text += f"📋 Замовлень: {stats.get('total_orders', 0)}\n"
            text += f"💰 Виручка: {stats.get('total_revenue', 0):.2f} грн\n"
            text += f"💳 Середній чек: {stats.get('avg_check', 0):.2f} грн\n"
            text += f"👥 Клієнтів: {stats.get('total_users', 0)}\n"
            text += f"⚡ Швидких замовлень: {stats.get('total_quick_orders', 0)}\n"
            text += f"💬 Повідомлень: {stats.get('total_messages', 0)}\n"
            text += f"⭐ Відгуків: {stats.get('total_reviews', 0)}\n\n"
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ===== НАЛАШТУВАННЯ =====
        elif data == "admin_settings":
            await query.edit_message_text("⚙️ Налаштування\n\nОберіть розділ:", reply_markup=get_settings_menu())
            return
        
        elif data == "admin_settings_password":
            admin_sessions[user_id] = {"state": "authenticated", "action": "change_password"}
            await query.edit_message_text("🔑 Зміна пароля\n\nВведіть новий пароль:", reply_markup=get_back_keyboard("admin_settings"))
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

# ==================== ОБРОБНИК ТЕКСТОВИХ ПОВІДОМЛЕНЬ ====================

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip()
        
        logger.info(f"📝 Адмін {user_id}: {text[:50]}...")
        
        if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
            await check_password(update, context)
            return
        
        if not is_authenticated(user_id):
            return
        
        session = admin_sessions.get(user_id, {})
        action = session.get("action")
        
        if action == "add_product_name":
            admin_sessions[user_id]["product_name"] = text
            admin_sessions[user_id]["action"] = "add_product_price"
            await update.message.reply_text("Введіть ціну товару (тільки число):", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_price":
            try:
                price = float(text.replace(",", "."))
                admin_sessions[user_id]["product_price"] = price
                admin_sessions[user_id]["action"] = "add_product_category"
                await update.message.reply_text("Введіть категорію товару:", reply_markup=get_back_keyboard("admin_products"))
            except ValueError:
                await update.message.reply_text("❌ Невірний формат. Введіть число (наприклад: 250):", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_category":
            admin_sessions[user_id]["product_category"] = text
            admin_sessions[user_id]["action"] = "add_product_description"
            await update.message.reply_text("Введіть опис товару:", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_description":
            admin_sessions[user_id]["product_description"] = text
            admin_sessions[user_id]["action"] = "add_product_unit"
            await update.message.reply_text("Введіть одиницю виміру (наприклад: банка, кг, шт):", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_unit":
            admin_sessions[user_id]["product_unit"] = text
            admin_sessions[user_id]["action"] = "add_product_image"
            await update.message.reply_text("Введіть емодзі для товару (наприклад: 🥫, 🌶️, 🍯):", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_image":
            admin_sessions[user_id]["product_image"] = text
            admin_sessions[user_id]["action"] = "add_product_details"
            await update.message.reply_text("Введіть деталі товару (об'єм, вага, склад тощо):", reply_markup=get_back_keyboard("admin_products"))
            return
        
        elif action == "add_product_details":
            product_data = {
                "name": session.get("product_name"),
                "price": session.get("product_price"),
                "category": session.get("product_category"),
                "description": session.get("product_description"),
                "unit": session.get("product_unit"),
                "image": session.get("product_image"),
                "details": text
            }
            
            product_id = add_product(**product_data)
            
            if product_id:
                await update.message.reply_text(
                    f"✅ Товар успішно додано!\n\nID: {product_id}\nНазва: {product_data['name']}\nЦіна: {product_data['price']} грн",
                    reply_markup=get_products_menu()
                )
            else:
                await update.message.reply_text("❌ Помилка при додаванні товару", reply_markup=get_products_menu())
            
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
            
            if update_product(product_id, **update_data):
                await update.message.reply_text(f"✅ Товар #{product_id} оновлено!", reply_markup=get_products_menu())
            else:
                await update.message.reply_text("❌ Помилка при оновленні товару", reply_markup=get_products_menu())
            
            admin_sessions[user_id].pop("action", None)
            return
        
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
                    keyboard.append([InlineKeyboardButton(f"📦 №{order_id}", callback_data=f"order_view_{order_id}_{order_type}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")])
                await update.message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
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
                
                keyboard = [[InlineKeyboardButton("👤 Переглянути профіль", callback_data=f"customer_view_{user_data['user_id']}")]]
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")])
                
                await update.message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "send_message_to_customer":
            customer_id = session.get("customer_id")
            try:
                # Створюємо окремого бота для основного бота
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
                # Створюємо окремого бота для основного бота
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"📢 <b>Відповідь на замовлення №{order_id}</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text(
                    f"✅ Відповідь на замовлення №{order_id} надіслано!",
                    reply_markup=get_order_status_keyboard(order_id, session.get("order_type", 'regular'))
                )
            except Exception as e:
                await update.message.reply_text(
                    f"❌ Помилка при надсиланні: {e}",
                    reply_markup=get_order_status_keyboard(order_id, session.get("order_type", 'regular'))
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "reply_to_user":
            customer_id = session.get("customer_id")
            try:
                # Створюємо окремого бота для основного бота
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
        
        elif action == "broadcast":
            segment = session.get("segment")
            
            await update.message.reply_text(f"📢 Розпочинаю розсилку...")
            
            # Створюємо окремого бота для адмін-бота (для відправки статусу адміну)
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
        
        elif action == "change_password":
            global ADMIN_PASSWORD
            ADMIN_PASSWORD = text
            await update.message.reply_text("✅ Пароль успішно змінено!", reply_markup=get_settings_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "reviews_count":
            try:
                count = int(text)
                if count <= 0:
                    raise ValueError
                reviews = get_all_reviews(limit=count)
                if count > 20:
                    file_data = generate_reviews_file(reviews)
                    await update.message.reply_document(
                        document=file_data,
                        filename=f"reviews_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                        caption=f"⭐ {count} останніх відгуків"
                    )
                else:
                    if reviews:
                        text_response = format_reviews_text(reviews)
                    else:
                        text_response = "⭐ Відгуків поки немає"
                    await update.message.reply_text(text_response, reply_markup=get_reviews_back_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Введіть коректне число більше 0", reply_markup=get_reviews_back_keyboard())
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "add_admin":
            try:
                new_admin_id = int(text)
                new_user = get_user_by_id(new_admin_id)
                if new_user:
                    if add_admin(new_admin_id, new_user['username'], user_id):
                        await update.message.reply_text(f"✅ Користувача {new_user['first_name']} додано до адмінів!", reply_markup=get_admins_menu())
                    else:
                        await update.message.reply_text("❌ Помилка при додаванні адміна", reply_markup=get_admins_menu())
                else:
                    await update.message.reply_text("❌ Користувача з таким ID не знайдено в базі\n\nСпочатку користувач має написати основному боту /start", reply_markup=get_admins_menu())
            except ValueError:
                await update.message.reply_text("❌ Введіть коректний числовий ID", reply_markup=get_admins_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        else:
            await update.message.reply_text("❌ Невідома команда", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"❌ Помилка в message_handler: {e}")
        logger.error(traceback.format_exc())
        await update.message.reply_text(
            "❌ Сталася помилка. Повертаємось до головного меню.",
            reply_markup=get_main_menu()
        )

# ==================== ОСНОВНА ФУНКЦІЯ ====================

def main():
    logger.info("🚀 Запуск адмін-бота Бонелет...")
    
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
                cursor.execute("SELECT COUNT(*) FROM reviews")
                reviews_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM quick_orders")
                quick_orders_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM messages")
                messages_count = cursor.fetchone()['count']
                
                logger.info(f"📊 Статистика БД: {users_count} користувачів, {orders_count} замовлень, {quick_orders_count} швидких замовлень, {products_count} товарів, {messages_count} повідомлень, {reviews_count} відгуків")
                
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
        
        logger.info("✅ Адмін-бот готовий до роботи")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}")
        logger.error(traceback.format_exc())
        time.sleep(5)

if __name__ == "__main__":
    main()
