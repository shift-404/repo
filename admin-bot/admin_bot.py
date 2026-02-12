import os
import json
import sqlite3
import logging
import sys
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from io import StringIO, BytesIO

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "bot", "farm_bot.db")  # Спільна БД з основним ботом
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

# Створюємо папку для звітів, якщо її немає
os.makedirs(REPORTS_DIR, exist_ok=True)

# ==================== ФАЙЛИ ДЛЯ ЛОГУВАННЯ ====================

ORDERS_LOG = os.path.join(REPORTS_DIR, "orders.txt")
USERS_LOG = os.path.join(REPORTS_DIR, "users.txt")
MESSAGES_LOG = os.path.join(REPORTS_DIR, "messages.txt")
QUICK_ORDERS_LOG = os.path.join(REPORTS_DIR, "quick_orders.txt")

# ==================== СЕСІЇ АДМІНІВ ====================

admin_sessions = {}

# ==================== ФУНКЦІЇ ЛОГУВАННЯ ====================

def log_order(order_data: dict):
    """Записує замовлення у текстовий файл"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(ORDERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"ЗАМОВЛЕННЯ #{order_data.get('order_id', 'Н/Д')}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Клієнт: {order_data.get('user_name', 'Н/Д')}\n")
            f.write(f"Телефон: {order_data.get('phone', 'Н/Д')}\n")
            f.write(f"Username: {order_data.get('username', 'Н/Д')}\n")
            f.write(f"User ID: {order_data.get('user_id', 'Н/Д')}\n")
            f.write(f"Місто: {order_data.get('city', 'Н/Д')}\n")
            f.write(f"Відділення НП: {order_data.get('np_department', 'Н/Д')}\n")
            f.write(f"Сума: {order_data.get('total', 0):.2f} грн\n")
            f.write(f"Товари:\n")
            for item in order_data.get('items', []):
                f.write(f"  - {item.get('product_name')} x {item.get('quantity')} = {item.get('price') * item.get('quantity'):.2f} грн\n")
            f.write(f"{'='*80}\n")
    except Exception as e:
        logger.error(f"Помилка запису замовлення: {e}")

def log_user(user_data: dict):
    """Записує користувача у текстовий файл"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(USERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | ID:{user_data.get('user_id')} | {user_data.get('first_name')} {user_data.get('last_name')} | @{user_data.get('username')}\n")
    except Exception as e:
        logger.error(f"Помилка запису користувача: {e}")

def log_message(message_data: dict):
    """Записує повідомлення у текстовий файл"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(MESSAGES_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'─'*60}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Від: {message_data.get('user_name')} (ID: {message_data.get('user_id')})\n")
            f.write(f"Username: @{message_data.get('username')}\n")
            f.write(f"Тип: {message_data.get('message_type')}\n")
            f.write(f"Текст: {message_data.get('text')}\n")
            f.write(f"{'─'*60}\n")
    except Exception as e:
        logger.error(f"Помилка запису повідомлення: {e}")

def log_quick_order(order_data: dict):
    """Записує швидке замовлення у текстовий файл"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(QUICK_ORDERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"ШВИДКЕ ЗАМОВЛЕННЯ #{order_data.get('order_id', 'Н/Д')}\n")
            f.write(f"Час: {timestamp}\n")
            f.write(f"Клієнт: {order_data.get('user_name', 'Н/Д')}\n")
            f.write(f"Телефон: {order_data.get('phone', 'Н/Д')}\n")
            f.write(f"Username: {order_data.get('username', 'Н/Д')}\n")
            f.write(f"User ID: {order_data.get('user_id', 'Н/Д')}\n")
            f.write(f"Продукт: {order_data.get('product_name', 'Н/Д')}\n")
            f.write(f"Спосіб зв'язку: {order_data.get('contact_method', 'Н/Д')}\n")
            f.write(f"{'='*80}\n")
    except Exception as e:
        logger.error(f"Помилка запису швидкого замовлення: {e}")

# ==================== ФУНКЦІЇ ДЛЯ РОБОТИ З БД ====================

def get_db_connection():
    """Підключення до бази даних основного бота"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Помилка підключення до БД: {e}")
        return None

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

def get_all_products():
    """Отримати всі товари"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM products 
            ORDER BY id
        ''')
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка отримання товарів: {e}")
        return []
    finally:
        conn.close()

def update_product(product_id: int, **kwargs):
    """Оновити товар"""
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
        logger.error(f"Помилка оновлення товару: {e}")
        return False
    finally:
        conn.close()

def delete_product(product_id: int):
    """Видалити товар"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM products WHERE id = ?", (product_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Помилка видалення товару: {e}")
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, image: str, details: str):
    """Додати новий товар"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, image, details)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (name, price, category, description, unit, image, details))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Помилка додавання товару: {e}")
        return False
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
        
        # Сума замовлень
        cursor.execute("SELECT SUM(total) FROM orders")
        total_revenue = cursor.fetchone()[0] or 0
        
        # Замовлення за статусами
        cursor.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
        orders_by_status = dict(cursor.fetchall())
        
        return {
            "total_orders": total_orders,
            "total_users": total_users,
            "total_quick_orders": total_quick_orders,
            "total_messages": total_messages,
            "total_revenue": total_revenue,
            "orders_by_status": orders_by_status
        }
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        return {}
    finally:
        conn.close()

# ==================== ФУНКЦІЇ ГЕНЕРАЦІЇ ЗВІТІВ ====================

def generate_orders_report(format: str = "txt"):
    """Згенерувати звіт по замовленнях"""
    orders = get_all_orders()
    
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
            output.write(f"Товари:\n")
            for item in order.get('items', []):
                output.write(f"  - {item['product_name']} x {item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} грн\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Номер', 'Дата', 'Клієнт', 'Телефон', 'Username', 'Місто', 'Відділення', 'Сума', 'Статус', 'Товари'])
        
        for order in orders:
            items_str = "; ".join([f"{item['product_name']} x{item['quantity']}" for item in order.get('items', [])])
            writer.writerow([
                order['order_id'],
                order['created_at'],
                order['user_name'],
                order['phone'],
                order['username'],
                order['city'],
                order['np_department'],
                f"{order['total']:.2f}",
                order['status'],
                items_str
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_users_report(format: str = "txt"):
    """Згенерувати звіт по користувачах"""
    users = get_all_users()
    
    if format == "txt":
        output = StringIO()
        output.write("ЗВІТ ПО КОРИСТУВАЧАХ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Всього користувачів: {len(users)}\n")
        output.write("=" * 80 + "\n\n")
        
        for user in users:
            output.write(f"ID: {user['user_id']}\n")
            output.write(f"Ім'я: {user['first_name']} {user['last_name']}\n")
            output.write(f"Username: @{user['username']}\n")
            output.write(f"Дата реєстрації: {user['created_at']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['User ID', 'Імя', 'Прізвище', 'Username', 'Дата реєстрації'])
        
        for user in users:
            writer.writerow([
                user['user_id'],
                user['first_name'],
                user['last_name'],
                user['username'],
                user['created_at']
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_quick_orders_report(format: str = "txt"):
    """Згенерувати звіт по швидких замовленнях"""
    orders = get_quick_orders()
    
    if format == "txt":
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
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
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
        
        return output.getvalue().encode('utf-8-sig')

# ==================== ФУНКЦІЇ КЛАВІАТУР ====================

def get_main_menu():
    """Головне меню адмін-панелі"""
    keyboard = [
        [InlineKeyboardButton("📦 Товари", callback_data="admin_products")],
        [InlineKeyboardButton("📋 Замовлення", callback_data="admin_orders")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("👥 Користувачі", callback_data="admin_users")],
        [InlineKeyboardButton("📁 Звіти", callback_data="admin_reports")],
        [InlineKeyboardButton("⚙️ Налаштування", callback_data="admin_settings")],
        [InlineKeyboardButton("🔐 Вийти", callback_data="admin_logout")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_products_menu():
    """Меню керування товарами"""
    keyboard = [
        [InlineKeyboardButton("📋 Список товарів", callback_data="admin_product_list")],
        [InlineKeyboardButton("➕ Додати товар", callback_data="admin_product_add")],
        [InlineKeyboardButton("✏️ Редагувати товар", callback_data="admin_product_edit")],
        [InlineKeyboardButton("🗑 Видалити товар", callback_data="admin_product_delete")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_orders_menu():
    """Меню керування замовленнями"""
    keyboard = [
        [InlineKeyboardButton("📋 Всі замовлення", callback_data="admin_order_all")],
        [InlineKeyboardButton("🆕 Нові замовлення", callback_data="admin_order_new")],
        [InlineKeyboardButton("⚡ Швидкі замовлення", callback_data="admin_order_quick")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_reports_menu():
    """Меню звітів"""
    keyboard = [
        [InlineKeyboardButton("📦 Замовлення (TXT)", callback_data="report_orders_txt")],
        [InlineKeyboardButton("📦 Замовлення (CSV)", callback_data="report_orders_csv")],
        [InlineKeyboardButton("👥 Користувачі (TXT)", callback_data="report_users_txt")],
        [InlineKeyboardButton("👥 Користувачі (CSV)", callback_data="report_users_csv")],
        [InlineKeyboardButton("⚡ Швидкі замовлення (TXT)", callback_data="report_quick_txt")],
        [InlineKeyboardButton("⚡ Швидкі замовлення (CSV)", callback_data="report_quick_csv")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_settings_menu():
    """Меню налаштувань"""
    keyboard = [
        [InlineKeyboardButton("🔑 Змінити пароль", callback_data="admin_settings_password")],
        [InlineKeyboardButton("📢 Розсилка", callback_data="admin_settings_broadcast")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_order_actions_menu(order_id: int):
    """Меню дій із замовленням"""
    keyboard = [
        [InlineKeyboardButton("✅ Підтвердити", callback_data=f"order_confirm_{order_id}")],
        [InlineKeyboardButton("📦 Відправлено", callback_data=f"order_shipped_{order_id}")],
        [InlineKeyboardButton("❌ Скасувати", callback_data=f"order_cancel_{order_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]
    ]
    return InlineKeyboardMarkup(keyboard)

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
            text = "📦 Список товарів\n\n"
            text += "Товарів не знайдено."
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
        for p in products[:10]:  # Показуємо перші 10
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
        for p in products[:10]:
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
            text = "📋 Всі замовлення\n\n"
            text += "Замовлень не знайдено."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        text = f"📋 Всі замовлення\n\n"
        text += f"Всього: {len(orders)}\n\n"
        
        for order in orders[:5]:  # Показуємо перші 5
            text += f"№{order['order_id']} | {order['created_at'][:10]}\n"
            text += f"Клієнт: {order['user_name']}\n"
            text += f"Сума: {order['total']:.2f} грн\n"
            text += f"Статус: {order['status']}\n"
            text += f"{'─'*30}\n"
        
        if len(orders) > 5:
            text += f"... та ще {len(orders) - 5} замовлень\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Детально", callback_data="admin_order_details")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_details":
        orders = get_all_orders()
        keyboard = []
        for order in orders[:10]:
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
            text += f"Дата: {order['created_at']}\n"
            text += f"Клієнт: {order['user_name']}\n"
            text += f"Телефон: {order['phone']}\n"
            text += f"Username: @{order['username']}\n"
            text += f"Місто: {order['city']}\n"
            text += f"Відділення: {order['np_department']}\n"
            text += f"{'─'*30}\n"
            text += "Товари:\n"
            for item in items:
                text += f"  • {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} грн\n"
            text += f"{'─'*30}\n"
            text += f"Сума: {order['total']:.2f} грн\n"
            text += f"Статус: {order['status']}\n"
            
            await query.edit_message_text(
                text,
                reply_markup=get_order_actions_menu(order_id)
            )
    
    elif data.startswith("order_confirm_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "підтверджено"):
            text = f"✅ Замовлення №{order_id} підтверджено!"
        else:
            text = f"❌ Помилка при підтвердженні замовлення"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_shipped_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "відправлено"):
            text = f"📦 Замовлення №{order_id} відправлено!"
        else:
            text = f"❌ Помилка при оновленні статусу"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("order_cancel_"):
        order_id = int(data.split("_")[2])
        if update_order_status(order_id, "скасовано"):
            text = f"❌ Замовлення №{order_id} скасовано!"
        else:
            text = f"❌ Помилка при скасуванні замовлення"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_order_all")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_new":
        orders = get_new_orders()
        if not orders:
            text = "🆕 Нові замовлення\n\n"
            text += "Нових замовлень немає."
        else:
            text = f"🆕 Нові замовлення\n\n"
            text += f"Всього: {len(orders)}\n\n"
            for order in orders:
                text += f"№{order['order_id']} | {order['created_at'][:16]}\n"
                text += f"Клієнт: {order['user_name']}\n"
                text += f"Сума: {order['total']:.2f} грн\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "admin_order_quick":
        orders = get_quick_orders()
        if not orders:
            text = "⚡ Швидкі замовлення\n\n"
            text += "Швидких замовлень немає."
        else:
            text = f"⚡ Швидкі замовлення\n\n"
            text += f"Всього: {len(orders)}\n\n"
            for order in orders[:10]:
                text += f"№{order['id']} | {order['created_at'][:16]}\n"
                text += f"Клієнт: {order['user_name']}\n"
                text += f"Телефон: {order['phone']}\n"
                text += f"Продукт: {order['product_name']}\n"
                text += f"{'─'*30}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_orders")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ===== СТАТИСТИКА =====
    elif data == "admin_stats":
        stats = get_statistics()
        
        text = "📊 СТАТИСТИКА\n\n"
        text += f"📋 Замовлень: {stats.get('total_orders', 0)}\n"
        text += f"💰 Виручка: {stats.get('total_revenue', 0):.2f} грн\n"
        text += f"⚡ Швидких замовлень: {stats.get('total_quick_orders', 0)}\n"
        text += f"👥 Користувачів: {stats.get('total_users', 0)}\n"
        text += f"💬 Повідомлень: {stats.get('total_messages', 0)}\n\n"
        
        orders_by_status = stats.get('orders_by_status', {})
        if orders_by_status:
            text += "Статуси замовлень:\n"
            for status, count in orders_by_status.items():
                text += f"  • {status}: {count}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ===== КОРИСТУВАЧІ =====
    elif data == "admin_users":
        users = get_all_users()
        
        text = "👥 КОРИСТУВАЧІ\n\n"
        text += f"Всього: {len(users)}\n\n"
        
        for user in users[:10]:
            text += f"ID: {user['user_id']}\n"
            text += f"Ім'я: {user['first_name']} {user['last_name']}\n"
            text += f"Username: @{user['username']}\n"
            text += f"Дата: {user['created_at'][:16]}\n"
            text += f"{'─'*30}\n"
        
        if len(users) > 10:
            text += f"... та ще {len(users) - 10} користувачів"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ===== ЗВІТИ =====
    elif data == "admin_reports":
        await query.edit_message_text(
            "📁 Генерація звітів\n\n"
            "Оберіть тип звіту та формат:",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_orders_txt":
        report_data = generate_orders_report("txt")
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
        report_data = generate_orders_report("csv")
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
        report_data = generate_users_report("txt")
        await query.message.reply_document(
            document=report_data,
            filename=f"users_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="👥 Звіт по користувачах"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_users_csv":
        report_data = generate_users_report("csv")
        await query.message.reply_document(
            document=report_data,
            filename=f"users_report_{datetime.now().strftime('%Y%m%d')}.csv",
            caption="👥 Звіт по користувачах (CSV)"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_quick_txt":
        report_data = generate_quick_orders_report("txt")
        await query.message.reply_document(
            document=report_data,
            filename=f"quick_orders_report_{datetime.now().strftime('%Y%m%d')}.txt",
            caption="⚡ Звіт по швидких замовленнях"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
    elif data == "report_quick_csv":
        report_data = generate_quick_orders_report("csv")
        await query.message.reply_document(
            document=report_data,
            filename=f"quick_orders_report_{datetime.now().strftime('%Y%m%d')}.csv",
            caption="⚡ Звіт по швидких замовленнях (CSV)"
        )
        await query.edit_message_text(
            "✅ Звіт згенеровано!",
            reply_markup=get_reports_menu()
        )
    
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
    
    elif data == "admin_settings_broadcast":
        admin_sessions[user_id] = {
            "state": "authenticated",
            "action": "broadcast_message"
        }
        await query.edit_message_text(
            "📢 Розсилка повідомлень\n\n"
            "Введіть текст для розсилки:"
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
    
    # Зміна пароля
    elif action == "change_password":
        global ADMIN_PASSWORD
        ADMIN_PASSWORD = text
        await update.message.reply_text(
            "✅ Пароль успішно змінено!",
            reply_markup=get_settings_menu()
        )
        admin_sessions[user_id].pop("action", None)
    
    # Розсилка
    elif action == "broadcast_message":
        users = get_all_users()
        success_count = 0
        fail_count = 0
        
        await update.message.reply_text(f"📢 Починаю розсилку для {len(users)} користувачів...")
        
        for user_data in users:
            try:
                await context.bot.send_message(
                    chat_id=user_data['user_id'],
                    text=f"📢 ОГОЛОШЕННЯ\n\n{text}"
                )
                success_count += 1
            except:
                fail_count += 1
        
        await update.message.reply_text(
            f"✅ Розсилка завершена!\n\n"
            f"✓ Доставлено: {success_count}\n"
            f"✗ Помилок: {fail_count}",
            reply_markup=get_settings_menu()
        )
        
        admin_sessions[user_id].pop("action", None)

# ==================== ОСНОВНА ФУНКЦІЯ ====================

def main():
    """Запуск адмін-бота"""
    logger.info("🚀 Запуск адмін-бота Бонелет...")
    
    # Перевіряємо підключення до БД
    conn = get_db_connection()
    if conn:
        logger.info("✅ Підключення до бази даних успішне")
        conn.close()
    else:
        logger.warning("⚠️ Не вдалося підключитись до БД основного бота")
    
    # Створюємо додаток
    application = Application.builder().token(TOKEN).build()
    
    # Додаємо обробники
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    logger.info("✅ Адмін-бот готовий до роботи")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
