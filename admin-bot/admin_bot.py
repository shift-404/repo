import os
import json
import sqlite3
import logging
import sys
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from io import StringIO, BytesIO
import asyncio

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
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

# ==================== ЗМІННІ СЕРЕДОВИЩА ====================

TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not TOKEN:
    logger.error("❌ ADMIN_BOT_TOKEN не знайдено!")
    sys.exit(1)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

# ==================== ШЛЯХИ ДО ФАЙЛІВ ====================

# ВАЖЛИВО: Використовуємо спільну теку Railway Volume
DB_PATH = "/app/data/farm_bot.db"

# Локальна папка для звітів
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

# ==================== СЕСІЇ АДМІНІВ ====================

admin_sessions = {}

# ==================== ФУНКЦІЇ ДЛЯ РОБОТИ З БД ====================

def get_db_connection():
    """Підключення до бази даних основного бота"""
    try:
        # Переконуємось, що папка існує
        os.makedirs("/app/data", exist_ok=True)
        
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Помилка підключення до БД: {e}")
        return None

# ==================== ФУНКЦІЇ ДЛЯ ЗАМОВЛЕНЬ ====================

def get_all_orders():
    """Отримати всі замовлення з БД"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM orders 
            ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            
            # Отримуємо товари для замовлення
            cursor.execute('''
                SELECT * FROM order_items 
                WHERE order_id = ?
            ''', (order['order_id'],))
            items = cursor.fetchall()
            order['items'] = [dict(item) for item in items]
            
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень: {e}")
        return []
    finally:
        conn.close()

def get_orders_by_phone(phone: str):
    """Отримати замовлення за номером телефону"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM orders 
            WHERE phone LIKE ? 
            ORDER BY created_at DESC
        ''', (f"%{phone}%",))
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            cursor.execute('''
                SELECT * FROM order_items 
                WHERE order_id = ?
            ''', (order['order_id'],))
            items = cursor.fetchall()
            order['items'] = [dict(item) for item in items]
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень за телефоном: {e}")
        return []
    finally:
        conn.close()

def get_new_orders():
    """Отримати нові замовлення"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM orders 
            WHERE status = 'нове'
            ORDER BY created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання нових замовлень: {e}")
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
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання швидких замовлень: {e}")
        return []
    finally:
        conn.close()

def update_order_status(order_id: int, status: str):
    """Оновити статус замовлення"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE orders SET status = ? WHERE order_id = ?
        ''', (status, order_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Помилка оновлення статусу: {e}")
        return False
    finally:
        conn.close()

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
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання користувачів: {e}")
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
        # Спочатку шукаємо в замовленнях
        cursor.execute('''
            SELECT DISTINCT user_id, user_name, username FROM orders 
            WHERE phone LIKE ? 
            ORDER BY created_at DESC LIMIT 1
        ''', (f"%{phone}%",))
        order_user = cursor.fetchone()
        
        if order_user:
            user_id = order_user[0]
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            return dict(cursor.fetchone())
        
        return None
    except Exception as e:
        logger.error(f"Помилка отримання користувача за телефоном: {e}")
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
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"Помилка отримання користувача: {e}")
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
            SELECT * FROM orders 
            WHERE user_id = ? 
            ORDER BY created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            order = dict(row)
            cursor.execute('''
                SELECT * FROM order_items 
                WHERE order_id = ?
            ''', (order['order_id'],))
            items = cursor.fetchall()
            order['items'] = [dict(item) for item in items]
            orders.append(order)
        
        return orders
    except Exception as e:
        logger.error(f"Помилка отримання замовлень користувача: {e}")
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
            WHERE user_id = ? 
            ORDER BY created_at DESC LIMIT 10
        ''', (user_id,))
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання повідомлень: {e}")
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
            WHERE user_id = ? 
            ORDER BY created_at DESC
        ''', (user_id,))
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання швидких замовлень: {e}")
        return []
    finally:
        conn.close()

def get_customer_segment(user_data: dict, orders: list) -> str:
    """Визначення сегменту клієнта"""
    if not orders:
        return "🆕 Новий клієнт (без замовлень)"
    
    total_orders = len(orders)
    total_spent = sum(order['total'] for order in orders)
    last_order = max(orders, key=lambda x: x['created_at'])
    last_order_date = datetime.strptime(last_order['created_at'][:19], '%Y-%m-%d %H:%M:%S')
    days_since_last = (datetime.now() - last_order_date).days
    
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

async def send_broadcast_to_segment(context: ContextTypes.DEFAULT_TYPE, segment: str, message: str):
    """Відправка розсилки по сегменту клієнтів"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    for user in users:
        user_orders = get_user_orders(user['user_id'])
        user_segment = get_customer_segment(user, user_orders)
        
        if segment == "all" or segment in user_segment:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=f"📢 <b>Оголошення</b>\n\n{message}",
                    parse_mode='HTML'
                )
                sent_count += 1
                await asyncio.sleep(0.05)  # Щоб уникнути лімітів Telegram
            except Exception as e:
                logger.error(f"Помилка відправки користувачу {user['user_id']}: {e}")
                fail_count += 1
    
    return sent_count, fail_count

# ==================== ФУНКЦІЇ ДЛЯ ВІДГУКІВ ====================

async def send_review_request(context: ContextTypes.DEFAULT_TYPE, user_id: int, order_id: int = None):
    """Відправка запиту на відгук"""
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
        await context.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode='HTML'
        )
        return True
    except Exception as e:
        logger.error(f"Помилка відправки запиту на відгук: {e}")
        return False

# ==================== ФУНКЦІЇ ДЛЯ СТАТИСТИКИ ====================

def get_statistics():
    """Отримати статистику"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        
        # Загальна кількість
        cursor.execute("SELECT COUNT(*) FROM orders")
        total_orders = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM quick_orders")
        total_quick_orders = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]
        
        # Сума замовлень
        cursor.execute("SELECT SUM(total) FROM orders")
        total_revenue = cursor.fetchone()[0] or 0
        
        # Середній чек
        avg_check = total_revenue / total_orders if total_orders > 0 else 0
        
        # Замовлення за статусами
        cursor.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
        orders_by_status = dict(cursor.fetchall())
        
        # Замовлення за останні 30 днів
        cursor.execute('''
            SELECT COUNT(*), SUM(total) FROM orders 
            WHERE created_at >= datetime('now', '-30 days')
        ''')
        last_30_days = cursor.fetchone()
        
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
            segment = get_customer_segment(user, orders)
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
            "total_quick_orders": total_quick_orders,
            "total_messages": total_messages,
            "total_reviews": total_reviews,
            "total_revenue": total_revenue,
            "avg_check": avg_check,
            "orders_by_status": orders_by_status,
            "last_30_days_orders": last_30_days[0] or 0,
            "last_30_days_revenue": last_30_days[1] or 0,
            "segments": segments
        }
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
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
            products.append({
                "id": row[0],
                "name": row[1],
                "price": row[2],
                "category": row[3],
                "description": row[4],
                "unit": row[5],
                "image": row[6],
                "details": row[7]
            })
        return products
    except Exception as e:
        logger.error(f"❌ Ошибка получения товаров: {e}")
        return []
    finally:
        conn.close()

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
            fields.append(f"{key} = ?")
            values.append(value)
        
        values.append(product_id)
        query = f"UPDATE products SET {', '.join(fields)} WHERE id = ?"
        cursor.execute(query, values)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка обновления товара: {e}")
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, image: str, details: str):
    """Додає новий товар"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, image, details)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (name, price, category, description, unit, image, details))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"❌ Ошибка добавления товара: {e}")
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
        cursor.execute('DELETE FROM products WHERE id = ?', (product_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка удаления товара: {e}")
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
            admins.append({
                "user_id": row[0],
                "username": row[1],
                "added_by": row[2],
                "added_at": row[3]
            })
        return admins
    except Exception as e:
        logger.error(f"❌ Ошибка получения админов: {e}")
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
            INSERT OR REPLACE INTO admins (user_id, username, added_by)
            VALUES (?, ?, ?)
        ''', (user_id, username, added_by))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка добавления админа: {e}")
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
        cursor.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка удаления админа: {e}")
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
        cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = ?', (user_id,))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"❌ Ошибка проверки админа: {e}")
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
        output.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього замовлень: {len(orders)}\n")
        output.write("=" * 80 + "\n\n")
        
        for order in orders:
            output.write(f"Номер: {order['order_id']}\n")
            output.write(f"Дата: {order['created_at']}\n")
            output.write(f"Клієнт: {order['user_name']}\n")
            output.write(f"Телефон: {order['phone']}\n")
            output.write(f"Username: @{order['username']}\n")
            output.write(f"Місто: {order['city']}\n")
            output.write(f"Відділення: {order['np_department']}\n")
            output.write(f"Сума: {order['total']:.2f} грн\n")
            output.write(f"Статус: {order['status']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Номер', 'Дата', 'Клієнт', 'Телефон', 'Username', 'Місто', 'Відділення', 'Сума', 'Статус'])
        
        for order in orders:
            writer.writerow([
                order['order_id'],
                order['created_at'],
                order['user_name'],
                order['phone'],
                order['username'],
                order['city'],
                order['np_department'],
                f"{order['total']:.2f}",
                order['status']
            ])
        
        return output.getvalue().encode('utf-8-sig')

# ==================== ФУНКЦІЇ КЛАВІАТУР ====================

def create_inline_keyboard(buttons: List[List[Dict]]) -> InlineKeyboardMarkup:
    """Створює inline клавіатуру"""
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
        [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
        [{"text": "📁 Звіти", "callback_data": "admin_reports"}],
        [{"text": "📢 Розсилки", "callback_data": "admin_broadcast"}],
        [{"text": "👑 Адміни", "callback_data": "admin_manage_admins"}],
        [{"text": "⚙️ Налаштування", "callback_data": "admin_settings"}],
        [{"text": "🔐 Вийти", "callback_data": "admin_logout"}]
    ]
    return create_inline_keyboard(keyboard)

def get_products_menu():
    """Меню керування товарами"""
    keyboard = [
        [{"text": "📋 Список товарів", "callback_data": "admin_product_list"}],
        [{"text": "➕ Додати товар", "callback_data": "admin_product_add"}],
        [{"text": "✏️ Редагувати товар", "callback_data": "admin_product_edit"}],
        [{"text": "🗑 Видалити товар", "callback_data": "admin_product_delete"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_menu():
    """Меню керування замовленнями"""
    keyboard = [
        [{"text": "📋 Всі замовлення", "callback_data": "admin_order_all"}],
        [{"text": "🆕 Нові замовлення", "callback_data": "admin_order_new"}],
        [{"text": "⚡ Швидкі замовлення", "callback_data": "admin_order_quick"}],
        [{"text": "📞 Пошук за телефоном", "callback_data": "admin_order_by_phone"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
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
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
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
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_reports_menu():
    """Меню звітів"""
    keyboard = [
        [{"text": "📦 Замовлення (TXT)", "callback_data": "report_orders_txt"}],
        [{"text": "📦 Замовлення (CSV)", "callback_data": "report_orders_csv"}],
        [{"text": "👥 Клієнти (TXT)", "callback_data": "report_users_txt"}],
        [{"text": "👥 Клієнти (CSV)", "callback_data": "report_users_csv"}],
        [{"text": "⚡ Швидкі замовлення (TXT)", "callback_data": "report_quick_txt"}],
        [{"text": "⚡ Швидкі замовлення (CSV)", "callback_data": "report_quick_csv"}],
        [{"text": "📊 Статистика (TXT)", "callback_data": "report_stats_txt"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_admins_menu():
    """Меню керування адмінами"""
    keyboard = [
        [{"text": "📋 Список адмінів", "callback_data": "admin_list"}],
        [{"text": "➕ Додати адміна", "callback_data": "admin_add"}],
        [{"text": "🗑 Видалити адміна", "callback_data": "admin_remove"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_settings_menu():
    """Меню налаштувань"""
    keyboard = [
        [{"text": "🔑 Змінити пароль", "callback_data": "admin_settings_password"}],
        [{"text": "🔙 Назад", "callback_data": "admin_back_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_actions_menu(order_id: int):
    """Меню дій із замовленням"""
    keyboard = [
        [{"text": "✅ Підтвердити", "callback_data": f"order_confirm_{order_id}"}],
        [{"text": "📦 Упаковано", "callback_data": f"order_packed_{order_id}"}],
        [{"text": "🚚 Відправлено", "callback_data": f"order_shipped_{order_id}"}],
        [{"text": "📍 Прибуло", "callback_data": f"order_arrived_{order_id}"}],
        [{"text": "❌ Скасувати", "callback_data": f"order_cancel_{order_id}"}],
        [{"text": "⭐ Запитати відгук", "callback_data": f"order_review_{order_id}"}],
        [{"text": "🔙 Назад", "callback_data": "admin_order_all"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customer_actions_menu(user_id: int):
    """Меню дій з клієнтом"""
    keyboard = [
        [{"text": "📋 Історія замовлень", "callback_data": f"customer_orders_{user_id}"}],
        [{"text": "💬 Повідомлення", "callback_data": f"customer_messages_{user_id}"}],
        [{"text": "📢 Надіслати повідомлення", "callback_data": f"customer_message_{user_id}"}],
        [{"text": "⭐ Запитати відгук", "callback_data": f"customer_review_{user_id}"}],
        [{"text": "👑 Зробити адміном", "callback_data": f"customer_make_admin_{user_id}"}],
        [{"text": "🔙 Назад", "callback_data": "admin_customers"}]
    ]
    return create_inline_keyboard(keyboard)

# ==================== ПЕРЕВІРКА АВТОРИЗАЦІЇ ====================

def is_authenticated(user_id: int) -> bool:
    """Перевіряє чи авторизований адмін"""
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

# ==================== ОБРОБНИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник команди /start - запит пароля"""
    user = update.effective_user
    user_id = user.id
    
    # Перевірка чи є ID в списку дозволених
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await update.message.reply_text(
            "❌ Доступ заборонено\n\n"
            "Ви не маєте прав адміністратора."
        )
        return
    
    # Запит пароля
    admin_sessions[user_id] = {"state": "waiting_password"}
    
    await update.message.reply_text(
        "🔐 Вхід в адмін-панель Бонелет\n\n"
        "Будь ласка, введіть пароль:"
    )

async def check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перевірка введеного пароля"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    if user_id not in admin_sessions or admin_sessions[user_id].get("state") != "waiting_password":
        return
    
    if text == ADMIN_PASSWORD:
        admin_sessions[user_id] = {
            "state": "authenticated", 
            "authenticated_at": datetime.now().isoformat()
        }
        
        # Перевіряємо чи є в списку адмінів
        if not is_admin(user_id):
            add_admin(user_id, user.username or "", user_id)
        
        await update.message.reply_text(
            "✅ Пароль прийнято!\n\n"
            "Ласкаво просимо до адмін-панелі.",
            reply_markup=get_main_menu()
        )
    else:
        await update.message.reply_text(
            "❌ Невірний пароль!\n\n"
            "Спробуйте ще раз або напишіть /start"
        )
        admin_sessions.pop(user_id, None)

# ==================== ОБРОБНИКИ КНОПОК ====================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник натискань кнопок"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_id = user.id
    data = query.data
    
    # Перевірка авторизації
    if not is_authenticated(user_id):
        await query.edit_message_text(
            "❌ Сесія закінчилась\n\n"
            "Напишіть /start для повторного входу"
        )
        return
    
    # ===== ГОЛОВНЕ МЕНЮ =====
    if data == "admin_back_main":
        await query.edit_message_text(
            "🔐 Адмін-панель Бонелет\n\n"
            "Оберіть розділ:",
            reply_markup=get_main_menu()
        )
    
    elif data == "admin_logout":
        admin_sessions.pop(user_id, None)
        await query.edit_message_text(
            "🔐 Ви вийшли з адмін-панелі\n\n"
            "Для повторного входу напишіть /start"
        )
    
    # ===== ТОВАРИ =====
    elif data == "admin_products":
        await query.edit_message_text(
            "📦 Керування товарами\n\n"
            "Оберіть дію:",
            reply_markup=get_products_menu()
        )
    
    elif data == "admin_product_list":
        products = get_all_products()
        if not products:
            text = "📦 Список товарів\n\nТоварів не знайдено."
        else:
            text = "📦 Список товарів\n\n"
            for p in products:
                text += f"ID: {p['id']}\n"
                text += f"Назва: {p['name']}\n"
                text += f"Ціна: {p['price']} грн\n"
                text += f"Категорія: {p['category']}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_products")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_product_add":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "add_product_name"
        }
        await query.edit_message_text(
            "➕ Додавання нового товару\n\n"
            "Введіть назву товару:"
        )
    
    elif data == "admin_product_edit":
        products = get_all_products()
        keyboard = []
        for p in products[:20]:
            keyboard.append([InlineKeyboardButton(
                f"{p['id']}. {p['name'][:30]}", 
                callback_data=f"edit_product_{p['id']}"
            )])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_products")])
        
        await query.edit_message_text(
            "✏️ Редагування товару\n\n"
            "Оберіть товар для редагування:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("edit_product_"):
        product_id = int(data.split("_")[2])
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "edit_product_field",
            "product_id": product_id
        }
        
        keyboard = [
            [InlineKeyboardButton("📝 Назва", callback_data=f"edit_field_name_{product_id}")],
            [InlineKeyboardButton("💰 Ціна", callback_data=f"edit_field_price_{product_id}")],
            [InlineKeyboardButton("📋 Опис", callback_data=f"edit_field_desc_{product_id}")],
            [InlineKeyboardButton("🏷 Категорія", callback_data=f"edit_field_cat_{product_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_product_edit")]
        ]
        
        await query.edit_message_text(
            f"✏️ Редагування товару #{product_id}\n\n"
            "Оберіть поле для редагування:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("edit_field_"):
        parts = data.split("_")
        field = parts[2]
        product_id = int(parts[3])
        
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": f"edit_product_{field}",
            "product_id": product_id
        }
        
        field_names = {
            "name": "назву",
            "price": "ціну",
            "desc": "опис",
            "cat": "категорію"
        }
        
        await query.edit_message_text(
            f"✏️ Введіть нову {field_names.get(field, '')}:"
        )
    
    elif data == "admin_product_delete":
        products = get_all_products()
        keyboard = []
        for p in products[:20]:
            keyboard.append([InlineKeyboardButton(
                f"❌ {p['id']}. {p['name'][:30]}", 
                callback_data=f"delete_product_{p['id']}"
            )])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_products")])
        
        await query.edit_message_text(
            "🗑 Видалення товару\n\n"
            "Оберіть товар для видалення:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("delete_product_"):
        product_id = int(data.split("_")[2])
        
        keyboard = [
            [InlineKeyboardButton("✅ Так, видалити", callback_data=f"confirm_delete_{product_id}")],
            [InlineKeyboardButton("❌ Ні, скасувати", callback_data="admin_products")]
        ]
        
        await query.edit_message_text(
            f"🗑 Підтвердження видалення\n\n"
            f"Ви дійсно хочете видалити товар #{product_id}?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("confirm_delete_"):
        product_id = int(data.split("_")[2])
        if delete_product(product_id):
            text = "✅ Товар успішно видалено!"
        else:
            text = "❌ Помилка при видаленні товару"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_products")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ===== ЗАМОВЛЕННЯ =====
    elif data == "admin_orders":
        await query.edit_message_text(
            "📋 Керування замовленнями\n\n"
            "Оберіть тип замовлень:",
            reply_markup=get_orders_menu()
        )
    
    elif data == "admin_order_all":
        orders = get_all_orders()
        if not orders:
            text = "📋 Всі замовлення\n\nЗамовлень не знайдено."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        text = f"📋 Всі замовлення\n\nВсього: {len(orders)}\n\n"
        
        for order in orders[:10]:
            text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
            text += f"Клієнт: {order['user_name']}\n"
            text += f"Телефон: {order['phone']}\n"
            text += f"Сума: {order['total']:.2f} грн\n"
            text += f"Статус: {order['status']}\n"
            text += f"{'─'*30}\n"
        
        if len(orders) > 10:
            text += f"... та ще {len(orders) - 10} замовлень\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Детально", callback_data="admin_order_details")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_details":
        orders = get_all_orders()
        keyboard = []
        for order in orders[:20]:
            keyboard.append([InlineKeyboardButton(
                f"№{order['order_id']} - {order['user_name']} - {order['total']} грн",
                callback_data=f"order_view_{order['order_id']}"
            )])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")])
        
        await query.edit_message_text(
            "📋 Детальний перегляд замовлень\n\n"
            "Оберіть замовлення:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "admin_order_new":
        orders = get_new_orders()
        if not orders:
            text = "🆕 Нові замовлення\n\nНових замовлень немає."
        else:
            text = f"🆕 Нові замовлення\n\nВсього: {len(orders)}\n\n"
            for order in orders[:10]:
                text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
                text += f"Клієнт: {order['user_name']}\n"
                text += f"Сума: {order['total']:.2f} грн\n"
                text += f"Телефон: {order['phone']}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_quick":
        orders = get_quick_orders()
        if not orders:
            text = "⚡ Швидкі замовлення\n\nШвидких замовлень немає."
        else:
            text = f"⚡ Швидкі замовлення\n\nВсього: {len(orders)}\n\n"
            for order in orders[:10]:
                text += f"№{order['id']} | {order['created_at'][:16]}\n"
                text += f"Клієнт: {order['user_name']}\n"
                text += f"Телефон: {order['phone']}\n"
                text += f"Продукт: {order['product_name']}\n"
                text += f"Спосіб: {order['contact_method']}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_by_phone":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "search_orders_by_phone"
        }
        await query.edit_message_text(
            "📞 Пошук замовлень за телефоном\n\n"
            "Введіть номер телефону клієнта:"
        )
    
    elif data.startswith("order_view_"):
        order_id = int(data.split("_")[2])
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
            order = dict(cursor.fetchone())
            
            cursor.execute("SELECT * FROM order_items WHERE order_id = ?", (order_id,))
            items = cursor.fetchall()
            conn.close()
            
            text = f"📋 ЗАМОВЛЕННЯ №{order_id}\n\n"
            text += f"📅 Дата: {order['created_at']}\n"
            text += f"👤 Клієнт: {order['user_name']}\n"
            text += f"📞 Телефон: {order['phone']}\n"
            text += f"📱 Username: @{order['username']}\n"
            text += f"🏙️ Місто: {order['city']}\n"
            text += f"🏣 Відділення: {order['np_department']}\n"
            text += f"{'─'*30}\n"
            text += "📦 Товари:\n"
            for item in items:
                text += f"  • {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} грн\n"
            text += f"{'─'*30}\n"
            text += f"💰 Сума: {order['total']:.2f} грн\n"
            text += f"📊 Статус: {order['status']}\n"
            
            await query.edit_message_text(
                text,
                reply_markup=get_order_actions_menu(order_id)
            )
    
    elif data.startswith("order_confirm_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "підтверджено"):
            text = f"✅ Замовлення №{order_id} підтверджено!"
            
            # Відправляємо повідомлення клієнту
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
                user_id = cursor.fetchone()[0]
                conn.close()
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"✅ <b>Замовлення №{order_id} підтверджено!</b>\n\n"
                             f"Ваше замовлення прийнято в роботу. Ми повідомимо вас про зміну статусу.",
                        parse_mode='HTML'
                    )
                except:
                    pass
        else:
            text = f"❌ Помилка при підтвердженні замовлення"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_packed_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "упаковано"):
            text = f"📦 Замовлення №{order_id} упаковано!"
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
                user_id = cursor.fetchone()[0]
                conn.close()
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"📦 <b>Замовлення №{order_id} упаковано!</b>\n\n"
                             f"Ваше замовлення готове до відправки. Очікуйте на номер для відстеження.",
                        parse_mode='HTML'
                    )
                except:
                    pass
        else:
            text = f"❌ Помилка при оновленні статусу"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_shipped_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "відправлено"):
            text = f"🚚 Замовлення №{order_id} відправлено!"
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
                user_id = cursor.fetchone()[0]
                conn.close()
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🚚 <b>Замовлення №{order_id} відправлено!</b>\n\n"
                             f"Ваше замовлення вже в дорозі. Очікуйте на повідомлення про прибуття.",
                        parse_mode='HTML'
                    )
                except:
                    pass
        else:
            text = f"❌ Помилка при оновленні статусу"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_arrived_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "прибуло"):
            text = f"📍 Замовлення №{order_id} прибуло у відділення!"
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
                user_id = cursor.fetchone()[0]
                conn.close()
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"📍 <b>Замовлення №{order_id} прибуло!</b>\n\n"
                             f"Ваше замовлення вже чекає на вас у відділенні Нової Пошти. "
                             f"Не забудьте отримати його!",
                        parse_mode='HTML'
                    )
                except:
                    pass
        else:
            text = f"❌ Помилка при оновленні статусу"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_cancel_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "скасовано"):
            text = f"❌ Замовлення №{order_id} скасовано!"
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
                user_id = cursor.fetchone()[0]
                conn.close()
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"❌ <b>Замовлення №{order_id} скасовано</b>\n\n"
                             f"Якщо у вас виникли питання, зв'яжіться з нами: @support",
                        parse_mode='HTML'
                    )
                except:
                    pass
        else:
            text = f"❌ Помилка при скасуванні замовлення"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_review_"):
        order_id = int(data.split("_")[2])
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
            user_id = cursor.fetchone()[0]
            conn.close()
            
            if await send_review_request(context, user_id, order_id):
                text = f"✅ Запит на відгук для замовлення №{order_id} надіслано!"
            else:
                text = f"❌ Помилка при надсиланні запиту"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"order_view_{order_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ===== КЛІЄНТИ =====
    elif data == "admin_customers":
        await query.edit_message_text(
            "👥 Керування клієнтами\n\n"
            "Оберіть дію:",
            reply_markup=get_customers_menu()
        )
    
    elif data == "admin_customers_all":
        users = get_all_users()
        text = f"👥 ВСІ КЛІЄНТИ\n\nВсього: {len(users)}\n\n"
        
        for user in users[:20]:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            text += f"ID: {user['user_id']}\n"
            text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
            text += f"Username: @{user['username']}\n"
            text += f"📊 {segment}\n"
            text += f"📦 Замовлень: {len(orders)}\n"
            text += f"{'─'*30}\n"
        
        if len(users) > 20:
            text += f"... та ще {len(users) - 20} клієнтів"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_customers_vip":
        users = get_all_users()
        text = "👑 VIP КЛІЄНТИ\n\n"
        count = 0
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            if "VIP" in segment:
                count += 1
                text += f"ID: {user['user_id']}\n"
                text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
                text += f"Username: @{user['username']}\n"
                text += f"📦 Замовлень: {len(orders)}\n"
                text += f"{'─'*30}\n"
        
        text = f"👑 VIP КЛІЄНТИ\n\nЗнайдено: {count}\n\n" + text if count > 0 else "👑 VIP клієнтів не знайдено"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_customers_regular":
        users = get_all_users()
        text = "⭐ ПОСТІЙНІ КЛІЄНТИ\n\n"
        count = 0
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            if "Постійний" in segment:
                count += 1
                text += f"ID: {user['user_id']}\n"
                text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
                text += f"Username: @{user['username']}\n"
                text += f"📦 Замовлень: {len(orders)}\n"
                text += f"{'─'*30}\n"
        
        text = f"⭐ ПОСТІЙНІ КЛІЄНТИ\n\nЗнайдено: {count}\n\n" + text if count > 0 else "⭐ Постійних клієнтів не знайдено"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_customers_new":
        users = get_all_users()
        text = "🆕 НОВІ КЛІЄНТИ\n\n"
        count = 0
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            if "Новий" in segment:
                count += 1
                text += f"ID: {user['user_id']}\n"
                text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
                text += f"Username: @{user['username']}\n"
                text += f"📦 Замовлень: {len(orders)}\n"
                text += f"{'─'*30}\n"
        
        text = f"🆕 НОВІ КЛІЄНТИ\n\nЗнайдено: {count}\n\n" + text if count > 0 else "🆕 Нових клієнтів не знайдено"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_customers_inactive":
        users = get_all_users()
        text = "💤 НЕАКТИВНІ КЛІЄНТИ\n\n"
        count = 0
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            if "Неактивний" in segment:
                count += 1
                text += f"ID: {user['user_id']}\n"
                text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
                text += f"Username: @{user['username']}\n"
                text += f"Останнє замовлення: {orders[0]['created_at'][:16] if orders else 'Немає'}\n"
                text += f"{'─'*30}\n"
        
        text = f"💤 НЕАКТИВНІ КЛІЄНТИ\n\nЗнайдено: {count}\n\n" + text if count > 0 else "💤 Неактивних клієнтів не знайдено"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_customer_search":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "search_customer_by_phone"
        }
        await query.edit_message_text(
            "🔍 Пошук клієнта за телефоном\n\n"
            "Введіть номер телефону:"
        )
    
    elif data.startswith("customer_view_"):
        customer_id = int(data.split("_")[2])
        user = get_user_by_id(customer_id)
        if not user:
            await query.edit_message_text("❌ Клієнта не знайдено")
            return
        
        orders = get_user_orders(customer_id)
        messages = get_user_messages(customer_id)
        quick_orders = get_user_quick_orders(customer_id)
        segment = get_customer_segment(user, orders)
        
        text = f"👤 ПРОФІЛЬ КЛІЄНТА\n\n"
        text += f"ID: {user['user_id']}\n"
        text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
        text += f"Username: @{user['username']}\n"
        text += f"📅 Реєстрація: {user['created_at'][:16]}\n"
        text += f"📊 Сегмент: {segment}\n\n"
        
        if orders:
            total_spent = sum(o['total'] for o in orders)
            text += f"📦 Всього замовлень: {len(orders)}\n"
            text += f"💰 Загальна сума: {total_spent:.2f} грн\n"
            text += f"💳 Середній чек: {total_spent/len(orders):.2f} грн\n\n"
            
            text += "🆕 Останнє замовлення:\n"
            last = orders[0]
            text += f"   №{last['order_id']} від {last['created_at'][:16]}\n"
            text += f"   Сума: {last['total']:.2f} грн\n"
            text += f"   Статус: {last['status']}\n"
        else:
            text += "📦 Замовлень: 0\n"
        
        text += f"\n💬 Повідомлень: {len(messages)}\n"
        text += f"⚡ Швидких замовлень: {len(quick_orders)}"
        
        await query.edit_message_text(
            text,
            reply_markup=get_customer_actions_menu(customer_id)
        )
    
    elif data.startswith("customer_orders_"):
        customer_id = int(data.split("_")[2])
        orders = get_user_orders(customer_id)
        
        if not orders:
            text = "📋 Історія замовлень\n\nУ клієнта немає замовлень."
        else:
            text = f"📋 ІСТОРІЯ ЗАМОВЛЕНЬ\n\nВсього: {len(orders)}\n\n"
            for order in orders:
                text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
                text += f"Сума: {order['total']:.2f} грн\n"
                text += f"Статус: {order['status']}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("customer_messages_"):
        customer_id = int(data.split("_")[2])
        messages = get_user_messages(customer_id)
        
        if not messages:
            text = "💬 Повідомлення\n\nУ клієнта немає повідомлень."
        else:
            text = f"💬 ОСТАННІ ПОВІДОМЛЕННЯ\n\n"
            for msg in messages[:10]:
                text += f"📅 {msg['created_at'][:16]}\n"
                text += f"📝 {msg['text'][:100]}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("customer_message_"):
        customer_id = int(data.split("_")[2])
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "send_message_to_customer",
            "customer_id": customer_id
        }
        await query.edit_message_text(
            "📢 Надіслати повідомлення клієнту\n\n"
            "Введіть текст повідомлення:"
        )
    
    elif data.startswith("customer_review_"):
        customer_id = int(data.split("_")[2])
        if await send_review_request(context, customer_id):
            text = "✅ Запит на відгук надіслано!"
        else:
            text = "❌ Помилка при надсиланні запиту"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"customer_view_{customer_id}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
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
    
    # ===== РОЗСИЛКИ =====
    elif data == "admin_broadcast":
        await query.edit_message_text(
            "📢 Розсилка повідомлень\n\n"
            "Оберіть цільову аудиторію:",
            reply_markup=get_broadcast_menu()
        )
    
    elif data.startswith("broadcast_"):
        segment = data.replace("broadcast_", "")
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "broadcast",
            "segment": segment
        }
        await query.edit_message_text(
            f"📢 Розсилка для сегменту: {segment}\n\n"
            f"Введіть текст повідомлення для розсилки:"
        )
    
    # ===== ЗВІТИ =====
    elif data == "admin_reports":
        await query.edit_message_text(
            "📁 Генерація звітів\n\n"
            "Оберіть тип звіту та формат:",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_orders_txt":
        orders = get_all_orders()
        report_data = generate_orders_report(orders, "txt")
        await query.message.reply_document(
            document=report_data,
            filename=f"orders_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="📋 Звіт по замовленнях"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_orders_csv":
        orders = get_all_orders()
        report_data = generate_orders_report(orders, "csv")
        await query.message.reply_document(
            document=report_data,
            filename=f"orders_report_{datetime.now().strftime('%Y%m%d')}.csv",
            caption="📋 Звіт по замовленнях (CSV)"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_users_txt":
        users = get_all_users()
        output = StringIO()
        output.write("ЗВІТ ПО КЛІЄНТАХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього клієнтів: {len(users)}\n")
        output.write("=" * 80 + "\n\n")
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            output.write(f"ID: {user['user_id']}\n")
            output.write(f"Ім'я: {user['first_name']} {user['last_name']}\n")
            output.write(f"Username: @{user['username']}\n")
            output.write(f"Дата реєстрації: {user['created_at'][:16]}\n")
            output.write(f"Сегмент: {segment}\n")
            output.write(f"Замовлень: {len(orders)}\n")
            output.write("-" * 40 + "\n")
        
        await query.message.reply_document(
            document=output.getvalue().encode('utf-8'),
            filename=f"users_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="👥 Звіт по клієнтах"
        )
        await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
    
    elif data == "report_users_csv":
        users = get_all_users()
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID', 'Імя', 'Прізвище', 'Username', 'Дата реєстрації', 'Сегмент', 'Замовлень'])
        
        for user in users:
            orders = get_user_orders(user['user_id'])
            segment = get_customer_segment(user, orders)
            writer.writerow([
                user['user_id'],
                user['first_name'],
                user['last_name'],
                user['username'],
                user['created_at'][:16],
                segment,
                len(orders)
            ])
        
        await query.message.reply_document(
            document=output.getvalue().encode('utf-8-sig'),
            filename=f"users_report_{datetime.now().strftime('%Y%m%d')}.csv",
            caption="👥 Звіт по клієнтах (CSV)"
        )
        await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
    
    elif data == "report_quick_txt":
        orders = get_quick_orders()
        output = StringIO()
        output.write("ЗВІТ ПО ШВИДКИХ ЗАМОВЛЕННЯХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
            output.write(f"Статус: {order['status']}\n")
            output.write("-" * 40 + "\n")
        
        await query.message.reply_document(
            document=output.getvalue().encode('utf-8'),
            filename=f"quick_orders_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="⚡ Звіт по швидких замовленнях"
        )
        await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
    
    elif data == "report_quick_csv":
        orders = get_quick_orders()
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Номер', 'Дата', 'Клієнт', 'Телефон', 'Username', 'Продукт', 'Спосіб зв`язку', 'Статус'])
        
        for order in orders:
            writer.writerow([
                order['id'],
                order['created_at'],
                order['user_name'],
                order['phone'],
                order['username'],
                order['product_name'],
                order['contact_method'],
                order['status']
            ])
        
        await query.message.reply_document(
            document=output.getvalue().encode('utf-8-sig'),
            filename=f"quick_orders_report_{datetime.now().strftime('%Y%m%d')}.csv",
            caption="⚡ Звіт по швидких замовленнях (CSV)"
        )
        await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
    
    elif data == "report_stats_txt":
        stats = get_statistics()
        output = StringIO()
        output.write("СТАТИСТИКА\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
        
        await query.message.reply_document(
            document=output.getvalue().encode('utf-8'),
            filename=f"stats_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="📊 Статистика"
        )
        await query.edit_message_text("✅ Звіт згенеровано!", reply_markup=get_reports_menu())
    
    # ===== АДМІНИ =====
    elif data == "admin_manage_admins":
        await query.edit_message_text(
            "👑 Керування адміністраторами\n\n"
            "Оберіть дію:",
            reply_markup=get_admins_menu()
        )
    
    elif data == "admin_list":
        admins = get_all_admins()
        if not admins:
            text = "📋 Список адмінів\n\nАдмінів не знайдено."
        else:
            text = "📋 СПИСОК АДМІНІСТРАТОРІВ\n\n"
            for admin in admins:
                text += f"ID: {admin['user_id']}\n"
                text += f"Username: @{admin['username']}\n"
                text += f"Додано: {admin['added_at'][:16]}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_add":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "add_admin"
        }
        await query.edit_message_text(
            "➕ Додавання адміністратора\n\n"
            "Введіть Telegram ID користувача:"
        )
    
    elif data == "admin_remove":
        admins = get_all_admins()
        keyboard = []
        for admin in admins:
            if admin['user_id'] != user_id:  # Не можна видалити себе
                keyboard.append([InlineKeyboardButton(
                    f"❌ {admin['user_id']} - @{admin['username']}", 
                    callback_data=f"remove_admin_{admin['user_id']}"
                )])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")])
        
        await query.edit_message_text(
            "🗑 Видалення адміністратора\n\n"
            "Оберіть адміна для видалення:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
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
    
    # ===== НАЛАШТУВАННЯ =====
    elif data == "admin_settings":
        await query.edit_message_text(
            "⚙️ Налаштування\n\n"
            "Оберіть розділ:",
            reply_markup=get_settings_menu()
        )
    
    elif data == "admin_settings_password":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "change_password"
        }
        await query.edit_message_text(
            "🔑 Зміна пароля\n\n"
            "Введіть новий пароль:"
        )

# ==================== ОБРОБНИК ТЕКСТОВИХ ПОВІДОМЛЕНЬ ====================

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник текстових повідомлень"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    # Перевірка на пароль
    if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
        await check_password(update, context)
        return
    
    # Перевірка авторизації
    if not is_authenticated(user_id):
        return
    
    session = admin_sessions.get(user_id, {})
    action = session.get("action")
    
    # Додавання товару
    if action == "add_product_name":
        admin_sessions[user_id]["product_name"] = text
        admin_sessions[user_id]["action"] = "add_product_price"
        await update.message.reply_text("Введіть ціну товару (тільки число):")
    
    elif action == "add_product_price":
        try:
            price = float(text.replace(",", "."))
            admin_sessions[user_id]["product_price"] = price
            admin_sessions[user_id]["action"] = "add_product_category"
            await update.message.reply_text("Введіть категорію товару:")
        except ValueError:
            await update.message.reply_text("❌ Невірний формат. Введіть число (наприклад: 250):")
    
    elif action == "add_product_category":
        admin_sessions[user_id]["product_category"] = text
        admin_sessions[user_id]["action"] = "add_product_description"
        await update.message.reply_text("Введіть опис товару:")
    
    elif action == "add_product_description":
        admin_sessions[user_id]["product_description"] = text
        admin_sessions[user_id]["action"] = "add_product_unit"
        await update.message.reply_text("Введіть одиницю виміру (наприклад: банка, кг, шт):")
    
    elif action == "add_product_unit":
        admin_sessions[user_id]["product_unit"] = text
        admin_sessions[user_id]["action"] = "add_product_image"
        await update.message.reply_text("Введіть емодзі для товару (наприклад: 🥫, 🌶️, 🍯):")
    
    elif action == "add_product_image":
        admin_sessions[user_id]["product_image"] = text
        admin_sessions[user_id]["action"] = "add_product_details"
        await update.message.reply_text("Введіть деталі товару (об'єм, вага, склад тощо):")
    
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
                f"✅ Товар успішно додано!\n\n"
                f"ID: {product_id}\n"
                f"Назва: {product_data['name']}\n"
                f"Ціна: {product_data['price']} грн",
                reply_markup=get_products_menu()
            )
        else:
            await update.message.reply_text(
                "❌ Помилка при додаванні товару",
                reply_markup=get_products_menu()
            )
        
        admin_sessions[user_id].pop("action", None)
    
    # Редагування товару
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
                await update.message.reply_text("❌ Невірний формат. Введіть число:")
                return
        elif field == "desc":
            update_data["description"] = text
        elif field == "cat":
            update_data["category"] = text
        
        if update_product(product_id, **update_data):
            await update.message.reply_text(
                f"✅ Товар #{product_id} оновлено!",
                reply_markup=get_products_menu()
            )
        else:
            await update.message.reply_text(
                "❌ Помилка при оновленні товару",
                reply_markup=get_products_menu()
            )
        
        admin_sessions[user_id].pop("action", None)
    
    # Пошук замовлень за телефоном
    elif action == "search_orders_by_phone":
        orders = get_orders_by_phone(text)
        
        if not orders:
            await update.message.reply_text(
                f"❌ Замовлень за номером {text} не знайдено",
                reply_markup=get_orders_menu()
            )
        else:
            response = f"📋 Знайдено замовлень: {len(orders)}\n\n"
            for order in orders[:5]:
                response += f"№{order['order_id']} | {order['created_at'][:16]}\n"
                response += f"Сума: {order['total']:.2f} грн\n"
                response += f"Статус: {order['status']}\n"
                response += f"{'─'*30}\n"
            
            keyboard = []
            for order in orders[:10]:
                keyboard.append([InlineKeyboardButton(
                    f"📦 №{order['order_id']}",
                    callback_data=f"order_view_{order['order_id']}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")])
            
            await update.message.reply_text(
                response,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        admin_sessions[user_id].pop("action", None)
    
    # Пошук клієнта за телефоном
    elif action == "search_customer_by_phone":
        user_data = get_user_by_phone(text)
        
        if not user_data:
            await update.message.reply_text(
                f"❌ Клієнта з телефоном {text} не знайдено",
                reply_markup=get_customers_menu()
            )
        else:
            orders = get_user_orders(user_data['user_id'])
            segment = get_customer_segment(user_data, orders)
            
            response = f"👤 КЛІЄНТ ЗНАЙДЕНИЙ\n\n"
            response += f"ID: {user_data['user_id']}\n"
            response += f"Ім'я: {user_data['first_name']} {user_data['last_name']}\n"
            response += f"Username: @{user_data['username']}\n"
            response += f"📊 Сегмент: {segment}\n"
            response += f"📦 Замовлень: {len(orders)}\n\n"
            
            if orders:
                total = sum(o['total'] for o in orders)
                response += f"💰 Загальна сума: {total:.2f} грн"
            
            keyboard = [[InlineKeyboardButton(
                "👤 Переглянути профіль",
                callback_data=f"customer_view_{user_data['user_id']}"
            )]]
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_customers")])
            
            await update.message.reply_text(
                response,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        admin_sessions[user_id].pop("action", None)
    
    # Надіслати повідомлення клієнту
    elif action == "send_message_to_customer":
        customer_id = session.get("customer_id")
        
        try:
            await context.bot.send_message(
                chat_id=customer_id,
                text=f"📢 <b>Повідомлення від адміністратора</b>\n\n{text}",
                parse_mode='HTML'
            )
            await update.message.reply_text(
                "✅ Повідомлення надіслано!",
                reply_markup=get_customer_actions_menu(customer_id)
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Помилка при надсиланні: {e}",
                reply_markup=get_customer_actions_menu(customer_id)
            )
        
        admin_sessions[user_id].pop("action", None)
    
    # Розсилка
    elif action == "broadcast":
        segment = session.get("segment")
        
        await update.message.reply_text(f"📢 Починаю розсилку для сегменту: {segment}...")
        
        sent, failed = await send_broadcast_to_segment(context, segment, text)
        
        await update.message.reply_text(
            f"✅ Розсилка завершена!\n\n"
            f"✓ Доставлено: {sent}\n"
            f"✗ Помилок: {failed}",
            reply_markup=get_broadcast_menu()
        )
        
        admin_sessions[user_id].pop("action", None)
    
    # Зміна пароля
    elif action == "change_password":
        global ADMIN_PASSWORD
        ADMIN_PASSWORD = text
        await update.message.reply_text(
            "✅ Пароль успішно змінено!",
            reply_markup=get_settings_menu()
        )
        admin_sessions[user_id].pop("action", None)
    
    # Додавання адміна
    elif action == "add_admin":
        try:
            new_admin_id = int(text)
            new_user = get_user_by_id(new_admin_id)
            
            if new_user:
                if add_admin(new_admin_id, new_user['username'], user_id):
                    await update.message.reply_text(
                        f"✅ Користувача {new_user['first_name']} додано до адмінів!",
                        reply_markup=get_admins_menu()
                    )
                else:
                    await update.message.reply_text(
                        "❌ Помилка при додаванні адміна",
                        reply_markup=get_admins_menu()
                    )
            else:
                await update.message.reply_text(
                    "❌ Користувача з таким ID не знайдено в базі\n\n"
                    "Спочатку користувач має написати основному боту /start",
                    reply_markup=get_admins_menu()
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Введіть коректний числовий ID",
                reply_markup=get_admins_menu()
            )
        
        admin_sessions[user_id].pop("action", None)

# ==================== ОСНОВНА ФУНКЦІЯ ====================

def init_database_if_empty():
    """Ініціалізує базу даних, якщо вона порожня"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Перевіряємо чи є таблиці
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        if cursor.fetchone():
            logger.info("✅ Таблиці вже існують")
            return True
        
        logger.info("🔄 База даних порожня, створюємо таблиці...")
        
        # Таблица користувачів
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица сесій
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id INTEGER PRIMARY KEY,
                state TEXT DEFAULT '',
                temp_data TEXT DEFAULT '{}',
                last_section TEXT DEFAULT 'main_menu',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица кошиків
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS carts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_id INTEGER,
                quantity REAL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица замовлень
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
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
        
        # Таблица елементів замовлень
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                product_name TEXT,
                quantity REAL,
                price_per_unit REAL
            )
        ''')
        
        # Таблица повідомлень
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                user_name TEXT,
                username TEXT,
                text TEXT,
                message_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица швидких замовлень
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quick_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                user_name TEXT,
                username TEXT,
                phone TEXT,
                product_id INTEGER,
                product_name TEXT,
                quantity REAL,
                contact_method TEXT,
                status TEXT DEFAULT 'нове',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица товарів
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        
        # Таблица відгуків
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                user_name TEXT,
                order_id INTEGER,
                text TEXT,
                rating INTEGER DEFAULT 5,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица адмінів
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Додаємо базові товари
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
        
        cursor.executemany('''
            INSERT OR IGNORE INTO products (id, name, price, category, description, unit, image, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', products)
        
        conn.commit()
        logger.info("✅ Таблиці успішно створено!")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка створення таблиць: {e}")
        return False
    finally:
        conn.close()

def main():
    """Запуск адмін-бота"""
    logger.info("🚀 Запуск адмін-бота Бонелет...")
    
    # Перевіряємо підключення до БД
    conn = get_db_connection()
    if conn:
        logger.info(f"✅ Підключення до бази даних успішне: {DB_PATH}")
        
        # Ініціалізуємо БД якщо вона порожня
        init_database_if_empty()
        
        # Перевіряємо чи є таблиця admins
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        
        # Перевіряємо чи є дані в БД
        try:
            cursor.execute("SELECT COUNT(*) FROM users")
            users_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM orders")
            orders_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM products")
            products_count = cursor.fetchone()[0]
            
            logger.info(f"📊 Статистика БД: {users_count} користувачів, {orders_count} замовлень, {products_count} товарів")
        except Exception as e:
            logger.error(f"❌ Помилка отримання статистики: {e}")
        
        conn.close()
    else:
        logger.warning("⚠️ Не вдалося підключитись до БД основного бота")
        # Створюємо БД якщо її немає
        init_database_if_empty()
    
    # Створюємо додаток
    application = Application.builder().token(TOKEN).build()
    
    # Додаємо обробники
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    logger.info("✅ Адмін-бот готовий до роботи")
    application.run_polling(drop_pending_updates=True)

