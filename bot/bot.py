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


from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot, InputMediaPhoto
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

logging.basicConfig(
    format='%(asctime)s - ADMIN - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

KYIV_TZ = None
try:
    import pytz
    KYIV_TZ = pytz.timezone('Europe/Kyiv')
except ImportError:
    logger.warning("Бібліотека pytz не встановлена, використовую UTC")
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
    except:
        return str(dt_str)[:16]

TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not TOKEN:
    logger.error("ADMIN_BOT_TOKEN не знайдено!")
    sys.exit(1)

MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not MAIN_BOT_TOKEN:
    logger.error("BOT_TOKEN не знайдено!")
    sys.exit(1)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL не знайдено!")
    sys.exit(1)

# Тимчасова папка для завантаження (буде очищатися)
TEMP_IMAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_images")
os.makedirs(TEMP_IMAGE_DIR, exist_ok=True)
print(f"📁 Тимчасова папка для зображень: {TEMP_IMAGE_DIR}")

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"Помилка підключення до БД: {e}")
        return None

def init_database_if_empty():
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
                image_path TEXT,
                image_file_id TEXT,
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
        
        try:
            cursor.execute('ALTER TABLE quick_orders ADD COLUMN IF NOT EXISTS message TEXT')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image_path TEXT')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image_file_id TEXT')
        except:
            pass
        
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()['count']
        
        if count == 0:
            products = [
                (1, "Артишок маринований з зернами гірчиці", 250, "мариновані артишоки", 
                 "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
                 "банка", "🥫", None, None, "Баночка 315 мл, Маса нетто 280 г, Склад: артишок 60%, вода, оцет винний, цукор, сіль, суміш спецій, зерна гірчиці"),
                
                (2, "Артишок маринований з чилі", 250, "мариновані артишоки",
                 "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
                 "банка", "🌶️", None, None, "Баночка 315 мл, Маса нетто 280 г, Склад: артишок 60%, вода, олія оливкова, оцет винний, цукор, сіль, суміш спецій, чилі"),
                
                (3, "Паштет з артишоку", 290, "паштети",
                 "Ніжний паштет з артишоку, ідеальний для бутербродів та закусок.",
                 "банка", "🍯", None, None, "Баночка 200 г, Маса нетто 200 г, Склад: артишок, вершки, олія оливкова, спеції")
            ]
            
            for product in products:
                cursor.execute('''
                    INSERT INTO products (id, name, price, category, description, unit, image, image_path, image_file_id, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                ''', product)
        
        conn.commit()
        logger.info("Таблиці успішно створено/перевірено!")
        return True
    except Exception as e:
        logger.error(f"Помилка створення таблиць: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

admin_sessions = {}
last_password_check = {}
orders_offset = {}
messages_offset = {}
broadcast_in_progress = {}

def is_authenticated(user_id: int) -> bool:
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

async def download_telegram_file(file_id: str, bot: Bot) -> str:
    """Тимчасово завантажує файл для отримання file_id (не для зберігання)"""
    try:
        file = await bot.get_file(file_id)
        file_path = os.path.join(TEMP_IMAGE_DIR, f"{file_id}.jpg")
        await file.download_to_drive(file_path)
        return file_path
    except Exception as e:
        logger.error(f"Помилка завантаження файлу: {e}")
        return None

async def download_image_from_url(url: str) -> tuple:
    """Завантажує зображення за URL і повертає тимчасовий шлях до файлу"""
    logger.info(f"🌐 Спроба завантажити URL: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, timeout=30, allow_redirects=True, headers=headers)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '')
        logger.info(f"📦 Отримано content-type: {content_type}")
        
        if not content_type.startswith('image/'):
            # Перевіряємо сигнатуру файлу
            if response.content[:4] in [b'\xff\xd8\xff\xe0', b'\xff\xd8\xff\xe1', b'\x89PNG', b'GIF8']:
                logger.info("📸 Файл схожий на зображення за сигнатурою")
            else:
                logger.error(f"❌ URL не містить зображення: {content_type}")
                return None, None
        
        filename = f"url_image_{int(time.time())}.jpg"
        file_path = os.path.join(TEMP_IMAGE_DIR, filename)
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"✅ Зображення завантажено тимчасово: {file_path}")
        return file_path, None
    except Exception as e:
        logger.error(f"❌ Помилка завантаження зображення за URL {url}: {e}")
        return None, None

async def reset_all_orders():
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
        logger.info("Всі замовлення та повідомлення успішно видалено!")
        return True
    except Exception as e:
        logger.error(f"Помилка видалення замовлень: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

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
        
        message += f"\n🕒 <b>Час:</b> {format_kyiv_time(order_data.get('created_at'))}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Керувати замовленням", callback_data=f"order_view_{order_id}_{order_data.get('order_type', 'regular')}")],
            [InlineKeyboardButton("📝 Відповісти клієнту", callback_data=f"reply_order_{order_id}_{order_data.get('order_type', 'regular')}")]
        ])
        
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
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Сповіщення про замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в notify_admins_about_new_order: {e}")
        logger.error(traceback.format_exc())

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
        message += f"🕒 <b>Час:</b> {format_kyiv_time(message_data.get('created_at'))}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Відповісти", callback_data=f"reply_user_{message_data.get('user_id')}")],
            [InlineKeyboardButton("👤 Профіль клієнта", callback_data=f"customer_view_{message_data.get('user_id')}")]
        ])
        
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
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Сповіщення про повідомлення відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в notify_admins_about_message: {e}")
        logger.error(traceback.format_exc())

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
        message += f"🕒 <b>Час:</b> {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Керувати замовленням", callback_data=f"order_view_{order_id}_quick")],
            [InlineKeyboardButton("📝 Відповісти клієнту", callback_data=f"reply_order_{order_id}_quick")]
        ])
        
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
                logger.error(f"Помилка відправки сповіщення адміну {admin['user_id']}: {e}")
        
        logger.info(f"Об'єднане сповіщення про швидке замовлення #{order_id} відправлено {sent_count} адмінам")
        
    except Exception as e:
        logger.error(f"Помилка в send_combined_quick_order_notification: {e}")
        logger.error(traceback.format_exc())

def safe_get(order, key, default=0):
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
        
        return all_orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_recent_orders(hours: int = 1, min_count: int = 3):
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
    if user_id not in orders_offset:
        orders_offset[user_id] = 0
    
    offset = orders_offset[user_id]
    orders = get_all_orders(include_quick=True, limit=count, offset=offset)
    orders_offset[user_id] = offset + len(orders)
    
    return orders

def format_order_text(order: dict) -> str:
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

async def notify_customer_about_status(user_id: int, order_id: int, status: str):
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
        logger.info(f"Сповіщення про статус #{order_id} відправлено клієнту {user_id}")
        return True
    except Exception as e:
        logger.error(f"Помилка відправки сповіщення клієнту {user_id}: {e}")
        return False

def get_all_messages(limit: int = 50, offset: int = 0):
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
    if user_id not in messages_offset:
        messages_offset[user_id] = 0
    
    offset = messages_offset[user_id]
    messages = get_all_messages(limit=count, offset=offset)
    messages_offset[user_id] = offset + len(messages)
    
    return messages

def format_message_text(msg: dict) -> str:
    text = f"💬 <b>Повідомлення #{msg['id']}</b>\n\n"
    text += f"👤 <b>Клієнт:</b> {msg['user_name']}\n"
    text += f"📱 <b>Username:</b> @{msg['username']}\n"
    text += f"🆔 <b>ID:</b> {msg['user_id']}\n"
    text += f"📅 <b>Час:</b> {msg['created_at'][:16]}\n"
    text += f"📝 <b>Тип:</b> {msg['message_type']}\n"
    text += f"💬 <b>Текст:</b> {msg['text']}\n"
    return text

def get_messages_by_user(user_id: int):
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
        logger.error(f"Помилка отримання товарів: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_product_by_id(product_id: int):
    logger.info(f"🔍 get_product_by_id викликано з ID: {product_id}")
    products = get_all_products()
    logger.info(f"📦 Отримано товарів з БД: {len(products)}")
    for product in products:
        if product["id"] == product_id:
            logger.info(f"✅ Знайдено товар: {product['name']}")
            return product
    logger.warning(f"❌ Товар з ID {product_id} не знайдено в БД")
    return None

def update_product(product_id: int, **kwargs):
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
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, image: str, image_path: str, image_file_id: str, details: str):
    logger.info(f"Спроба додати товар: {name}, ціна: {price}, категорія: {category}")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Не вдалося підключитись до БД")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, image, image_path, image_file_id, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, price, category, description, unit, image, image_path, image_file_id, details))
        
        result = cursor.fetchone()
        product_id = result['id'] if result else None
        conn.commit()
        
        logger.info(f"Товар додано з ID: {product_id}")
        return product_id
    except Exception as e:
        logger.error(f"Помилка додавання товару: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()
        
def delete_product(product_id: int):
    product = get_product_by_id(product_id)
    if product and product.get('image_path'):
        try:
            if os.path.exists(product['image_path']):
                os.remove(product['image_path'])
                logger.info(f"Видалено файл зображення: {product['image_path']}")
        except Exception as e:
            logger.error(f"Помилка видалення файлу зображення: {e}")
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM products WHERE id = %s', (product_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Помилка видалення товару: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_all_admins():
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
        logger.error(f"Помилка додавання адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def remove_admin(user_id: int):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM admins WHERE user_id = %s', (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Помилка видалення адміна: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def is_admin(user_id: int) -> bool:
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
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def generate_orders_report(orders: list, format: str = "txt"):
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

def get_statistics():
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
        [{"text": "👑 Адміни", "callback_data": "admin_manage_admins"}],
        [{"text": "🔄 Скинути замовлення", "callback_data": "admin_reset_orders"}],
        [{"text": "⚙️ Налаштування", "callback_data": "admin_settings"}],
        [{"text": "🔐 Вийти", "callback_data": "admin_logout"}]
    ]
    return create_inline_keyboard(keyboard)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    buttons = [[{"text": "🔙 Назад", "callback_data": f"back_to_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu():
    keyboard = [
        [{"text": "📋 Список товарів", "callback_data": "admin_product_list"}],
        [{"text": "➕ Додати товар", "callback_data": "admin_product_add"}],
        [{"text": "✏️ Редагувати товар", "callback_data": "admin_product_edit"}],
        [{"text": "🗑 Видалити товар", "callback_data": "admin_product_delete"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_menu():
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
    keyboard = [
        [{"text": "📋 Останні повідомлення", "callback_data": "admin_messages_recent"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "admin_messages_all"}],
        [{"text": "📁 Всі повідомлення файлом", "callback_data": "messages_all_file"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_menu():
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
    buttons = [[{"text": "🔙 Назад", "callback_data": "back_to_broadcast"}]]
    return create_inline_keyboard(buttons)

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
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_admins_menu():
    keyboard = [
        [{"text": "📋 Список адмінів", "callback_data": "admin_list"}],
        [{"text": "➕ Додати адміна", "callback_data": "admin_add"}],
        [{"text": "🗑 Видалити адміна", "callback_data": "admin_remove"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_settings_menu():
    keyboard = [
        [{"text": "🔑 Змінити пароль", "callback_data": "admin_settings_password"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_actions_menu(order_id: int, order_type: str = 'regular'):
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
    keyboard = [
        [{"text": "📝 Відповісти", "callback_data": f"reply_user_{user_id}"}],
        [{"text": "👤 Профіль клієнта", "callback_data": f"customer_view_{user_id}"}],
        [{"text": "📋 Всі повідомлення", "callback_data": "back_to_messages"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_messages"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customer_actions_menu(user_id: int):
    keyboard = [
        [{"text": "📋 Історія замовлень", "callback_data": f"customer_orders_{user_id}"}],
        [{"text": "💬 Повідомлення", "callback_data": f"customer_messages_{user_id}"}],
        [{"text": "📢 Надіслати повідомлення", "callback_data": f"customer_message_{user_id}"}],
        [{"text": "👑 Зробити адміном", "callback_data": f"customer_make_admin_{user_id}"}],
        [{"text": "🔙 Назад", "callback_data": "back_to_customers"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_status_keyboard(order_id: int, order_type: str = 'regular'):
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
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 замовлень", "callback_data": "admin_order_more"}])
    buttons.append([{"text": "🔍 Детально", "callback_data": "admin_order_details"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_orders"}])
    return create_inline_keyboard(buttons)

def get_messages_pagination_keyboard(user_id: int, has_more: bool = True):
    buttons = []
    if has_more:
        buttons.append([{"text": "📋 Ще 5 повідомлень", "callback_data": "admin_messages_more"}])
    buttons.append([{"text": "🔍 Детально", "callback_data": "admin_messages_details"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_messages"}])
    return create_inline_keyboard(buttons)

def get_product_image_keyboard(product_id: int, has_image: bool = False) -> InlineKeyboardMarkup:
    buttons = []
    buttons.append([{"text": "🌐 Завантажити за URL", "callback_data": f"edit_product_image_url_{product_id}"}])
    buttons.append([{"text": "📷 Завантажити файл", "callback_data": f"edit_product_image_file_{product_id}"}])
    if has_image:
        buttons.append([{"text": "🗑 Видалити фото", "callback_data": f"delete_product_image_{product_id}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": f"back_to_edit_product_{product_id}"}])
    return create_inline_keyboard(buttons)

def is_authenticated(user_id: int) -> bool:
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

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
        
        # Обробка кнопок "Назад"
        if data.startswith("back_to_"):
            target = data[8:]
            
            if target.startswith("edit_product_"):
                try:
                    product_id = int(target.split("_")[2])
                    product = get_product_by_id(product_id)
                    if product:
                        admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
                        keyboard = [
                            [InlineKeyboardButton("📝 Назва", callback_data=f"edit_field_name_{product_id}")],
                            [InlineKeyboardButton("💰 Ціна", callback_data=f"edit_field_price_{product_id}")],
                            [InlineKeyboardButton("📋 Опис", callback_data=f"edit_field_desc_{product_id}")],
                            [InlineKeyboardButton("🏷 Категорія", callback_data=f"edit_field_cat_{product_id}")],
                            [InlineKeyboardButton("📷 Фото", callback_data=f"edit_field_image_{product_id}")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")]
                        ]
                        await query.edit_message_text(
                            f"✏️ Редагування товару #{product_id}\n\nНазва: {product['name']}\nЦіна: {product['price']} грн\n\nОберіть поле для редагування:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                        return
                except:
                    pass
                
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
            else:
                await query.edit_message_text("🔐 Адмін-панель Бонелет\n\nОберіть розділ:", reply_markup=get_main_menu())
                return
        
        elif data == "admin_logout":
            admin_sessions.pop(user_id, None)
            last_password_check.pop(user_id, None)
            await query.edit_message_text("🔐 Ви вийшли з адмін-панелі\n\nДля повторного входу напишіть /start")
            return
        
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
                keyboard.append([InlineKeyboardButton(f"{p['id']}. {p['name'][:30]}", callback_data=f"edit_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")])
            await query.edit_message_text("✏️ Редагування товару\n\nОберіть товар для редагування:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # СПОЧАТКУ специфічні обробники для фото
        elif data.startswith("edit_product_image_url_"):
            logger.info(f"🔄 Натиснуто кнопку edit_product_image_url_, data: {data}")
            parts = data.split("_")
            logger.info(f"Розбито на частини: {parts}")
            try:
                product_id = int(parts[-1])
                logger.info(f"✅ Розпарсено product_id: {product_id}")
            except (IndexError, ValueError) as e:
                logger.error(f"❌ Помилка парсингу ID: {e}")
                await query.edit_message_text("❌ Помилка: некоректний ID товару (помилка парсингу)", reply_markup=get_products_menu())
                return

            # Перевіряємо, чи товар існує
            product = get_product_by_id(product_id)
            if not product:
                logger.error(f"❌ Товар з ID {product_id} не знайдено в БД")
                await query.edit_message_text(f"❌ Помилка: товар з ID {product_id} не знайдено", reply_markup=get_products_menu())
                return

            # Зберігаємо стан
            admin_sessions[user_id] = {
                "state": "authenticated",
                "action": "edit_product_image_url",
                "product_id": product_id
            }
            logger.info(f"✅ Стан збережено в admin_sessions[{user_id}]: {admin_sessions[user_id]}")

            await query.edit_message_text(
                "🌐 Введіть URL зображення:",
                reply_markup=get_back_keyboard(f"edit_product_{product_id}")
            )
            return
        
        elif data.startswith("edit_product_image_file_"):
            logger.info(f"🔄 Натиснуто кнопку edit_product_image_file_, data: {data}")
            parts = data.split("_")
            logger.info(f"Розбито на частини: {parts}")
            try:
                product_id = int(parts[-1])
                logger.info(f"✅ Розпарсено product_id: {product_id}")
            except (IndexError, ValueError) as e:
                logger.error(f"❌ Помилка парсингу ID: {e}")
                await query.edit_message_text("❌ Помилка: некоректний ID товару (помилка парсингу)", reply_markup=get_products_menu())
                return

            product = get_product_by_id(product_id)
            if not product:
                logger.error(f"❌ Товар з ID {product_id} не знайдено в БД")
                await query.edit_message_text(f"❌ Помилка: товар з ID {product_id} не знайдено", reply_markup=get_products_menu())
                return

            admin_sessions[user_id] = {
                "state": "authenticated",
                "action": "edit_product_image_file",
                "product_id": product_id
            }
            logger.info(f"✅ Стан збережено в admin_sessions[{user_id}]: {admin_sessions[user_id]}")

            await query.edit_message_text(
                "📷 Надішліть фото товару:",
                reply_markup=get_back_keyboard(f"edit_product_{product_id}")
            )
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
            
            # Видаляємо старе фото, якщо воно було збережене локально
            if product and product.get('image_path'):
                try:
                    if os.path.exists(product['image_path']):
                        os.remove(product['image_path'])
                        logger.info(f"Видалено файл зображення: {product['image_path']}")
                except Exception as e:
                    logger.error(f"Помилка видалення файлу: {e}")
            
            # Оновлюємо товар в БД - видаляємо обидва поля
            if update_product(product_id, image_path=None, image_file_id=None):
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
        
        # ТІЛЬКИ ПІСЛЯ специфічних обробників йде загальний edit_product_
        elif data.startswith("edit_product_"):
            logger.info(f"📝 Натиснуто загальний edit_product_ з data: {data}")
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
                [InlineKeyboardButton("📝 Назва", callback_data=f"edit_field_name_{product_id}")],
                [InlineKeyboardButton("💰 Ціна", callback_data=f"edit_field_price_{product_id}")],
                [InlineKeyboardButton("📋 Опис", callback_data=f"edit_field_desc_{product_id}")],
                [InlineKeyboardButton("🏷 Категорія", callback_data=f"edit_field_cat_{product_id}")],
                [InlineKeyboardButton("📷 Фото", callback_data=f"edit_field_image_{product_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")]
            ]
            await query.edit_message_text(
                f"✏️ Редагування товару #{product_id}\n\nНазва: {product['name']}\nЦіна: {product['price']} грн\n\nОберіть поле для редагування:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        elif data == "admin_product_delete":
            products = get_all_products()
            if not products:
                await query.edit_message_text("❌ Товарів не знайдено", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([InlineKeyboardButton(f"❌ {p['id']}. {p['name'][:30]}", callback_data=f"delete_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")])
            await query.edit_message_text("🗑 Видалення товару\n\nОберіть товар для видалення:", reply_markup=InlineKeyboardMarkup(keyboard))
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
                [InlineKeyboardButton("✅ Так, видалити", callback_data=f"confirm_delete_{product_id}")],
                [InlineKeyboardButton("❌ Ні, скасувати", callback_data="back_to_products")]
            ]
            await query.edit_message_text(f"🗑 Підтвердження видалення\n\nВи дійсно хочете видалити товар #{product_id}?", reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ... решта обробників (замовлення, клієнти, тощо) залишаються без змін ...
        # (я їх не включаю сюди для економії місця, але вони мають бути в коді)
        
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

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip() if update.message.text else ""
        
        logger.info(f"📝 Адмін {user_id}: {text[:50] if text else '[Фото]'}...")
        
        if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
            await check_password(update, context)
            return
        
        if not is_authenticated(user_id):
            return
        
        session = admin_sessions.get(user_id, {})
        action = session.get("action")
        logger.info(f"📌 Поточний action: {action}, session: {session}")
        
        if action == "add_product_name":
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
            admin_sessions[user_id]["action"] = "add_product_image"
            await update.message.reply_text("Введіть емодзі для товару (наприклад: 🥫, 🌶️, 🍯):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_image":
            admin_sessions[user_id]["product_image"] = text
            admin_sessions[user_id]["action"] = "add_product_image_upload"
            await update.message.reply_text("📷 Надішліть фото товару (або введіть 'пропустити'):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_image_upload":
            if update.message.photo:
                file_id = update.message.photo[-1].file_id
                # Тимчасово завантажуємо файл, щоб отримати file_id (не зберігаємо)
                image_path = await download_telegram_file(file_id, context.bot)
                if image_path:
                    # Видаляємо тимчасовий файл
                    try:
                        os.remove(image_path)
                    except:
                        pass
                
                admin_sessions[user_id]["product_image_path"] = None
                admin_sessions[user_id]["product_image_file_id"] = file_id
                admin_sessions[user_id]["action"] = "add_product_details"
                await update.message.reply_text("Введіть деталі товару (об'єм, вага, склад тощо):", reply_markup=get_back_keyboard("products"))
            elif text.lower() == "пропустити" or text == "-":
                admin_sessions[user_id]["product_image_path"] = None
                admin_sessions[user_id]["product_image_file_id"] = None
                admin_sessions[user_id]["action"] = "add_product_details"
                await update.message.reply_text("Введіть деталі товару (об'єм, вага, склад тощо):", reply_markup=get_back_keyboard("products"))
            else:
                await update.message.reply_text("❌ Будь ласка, надішліть фото або введіть 'пропустити'")
            return
        
        elif action == "add_product_details":
            product_data = {
                "name": session.get("product_name"),
                "price": session.get("product_price"),
                "category": session.get("product_category"),
                "description": session.get("product_description"),
                "unit": session.get("product_unit"),
                "image": session.get("product_image"),
                "image_path": None,  # Не зберігаємо локальні файли
                "image_file_id": session.get("product_image_file_id"),
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
        
        elif action == "edit_product_image_url":
            product_id = session.get("product_id")
            logger.info(f"📝 Отримано повідомлення для edit_product_image_url, product_id з сесії: {product_id}, текст: {text}")
            
            if not product_id:
                logger.error("❌ product_id не знайдено в сесії!")
                await update.message.reply_text("❌ Помилка: ID товару не знайдено. Спробуйте ще раз.", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            # Завантажуємо зображення за URL (тимчасово)
            logger.info(f"🌐 Завантаження з URL: {text}")
            image_path, _ = await download_image_from_url(text)
            
            if image_path:
                logger.info(f"✅ Фото завантажено тимчасово: {image_path}")
                
                # Відправляємо фото в Telegram, щоб отримати file_id
                try:
                    with open(image_path, 'rb') as photo:
                        sent_message = await context.bot.send_photo(
                            chat_id=user_id,
                            photo=photo,
                            caption="Тимчасове фото для отримання file_id"
                        )
                    
                    # Отримуємо file_id з відправленого фото
                    if sent_message and sent_message.photo:
                        file_id = sent_message.photo[-1].file_id
                        logger.info(f"✅ Отримано file_id: {file_id}")
                        
                        # Видаляємо повідомлення з фото
                        await context.bot.delete_message(chat_id=user_id, message_id=sent_message.message_id)
                        
                        # Видаляємо старий image_file_id/image_path, зберігаємо новий file_id
                        if update_product(product_id, image_file_id=file_id, image_path=None):
                            await update.message.reply_text(f"✅ Фото товару #{product_id} оновлено за URL! (збережено file_id)", reply_markup=get_products_menu())
                        else:
                            await update.message.reply_text("❌ Помилка при оновленні фото в базі даних", reply_markup=get_products_menu())
                    else:
                        logger.error("❌ Не вдалося отримати file_id з відправленого фото")
                        await update.message.reply_text("❌ Помилка при отриманні file_id", reply_markup=get_products_menu())
                
                except Exception as e:
                    logger.error(f"❌ Помилка при відправці фото в Telegram: {e}")
                    await update.message.reply_text("❌ Помилка при обробці фото", reply_markup=get_products_menu())
                finally:
                    # Видаляємо тимчасовий файл
                    try:
                        if os.path.exists(image_path):
                            os.remove(image_path)
                            logger.info(f"🗑 Видалено тимчасовий файл: {image_path}")
                    except Exception as e:
                        logger.error(f"Помилка видалення тимчасового файлу: {e}")
            else:
                logger.error(f"❌ Не вдалося завантажити зображення за URL: {text}")
                await update.message.reply_text("❌ Помилка при завантаженні зображення за URL. Перевірте посилання та спробуйте ще раз.", reply_markup=get_products_menu())
            
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_image_file":
            product_id = session.get("product_id")
            logger.info(f"📝 Отримано фото для edit_product_image_file, product_id з сесії: {product_id}")
            
            if not product_id:
                logger.error("❌ product_id не знайдено в сесії!")
                await update.message.reply_text("❌ Помилка: ID товару не знайдено. Спробуйте ще раз.", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            if update.message.photo:
                file_id = update.message.photo[-1].file_id
                logger.info(f"📸 Отримано file_id: {file_id}")
                
                # Тимчасово завантажуємо файл (не зберігаємо)
                image_path = await download_telegram_file(file_id, context.bot)
                if image_path:
                    try:
                        os.remove(image_path)
                        logger.info(f"🗑 Видалено тимчасовий файл")
                    except:
                        pass
                
                # Оновлюємо товар в БД - зберігаємо ТІЛЬКИ file_id
                if update_product(product_id, image_file_id=file_id, image_path=None):
                    await update.message.reply_text(f"✅ Фото товару #{product_id} оновлено! (збережено file_id)", reply_markup=get_products_menu())
                else:
                    await update.message.reply_text("❌ Помилка при оновленні фото в базі даних", reply_markup=get_products_menu())
            else:
                await update.message.reply_text("❌ Будь ласка, надішліть фото", reply_markup=get_back_keyboard("products"))
                return
            
            admin_sessions[user_id].pop("action", None)
            return
        
        # ... решта обробників (редагування полів, пошук, тощо) залишаються без змін ...
        
        else:
            await update.message.reply_text("❌ Невідома команда", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"❌ Помилка в message_handler: {e}")
        logger.error(traceback.format_exc())
        await update.message.reply_text(
            "❌ Сталася помилка. Повертаємось до головного меню.",
            reply_markup=get_main_menu()
        )

# ... інші функції (send_broadcast_to_all, send_broadcast_to_segment, main) залишаються без змін ...

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
                cursor.execute("SELECT COUNT(*) FROM quick_orders")
                quick_orders_count = cursor.fetchone()['count']
                cursor.execute("SELECT COUNT(*) FROM messages")
                messages_count = cursor.fetchone()['count']
                
                logger.info(f"📊 Статистика БД: {users_count} користувачів, {orders_count} замовлень, {quick_orders_count} швидких замовлень, {products_count} товарів, {messages_count} повідомлень")
                
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
        
        logger.info("✅ Адмін-бот готовий до роботи")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}")
        logger.error(traceback.format_exc())
        time.sleep(5)

if __name__ == "__main__":
    main()

