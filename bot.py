import os
import json
import sqlite3
import re
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ← ВАЖНО! Импорты telegram ДОЛЖНЫ быть ПОСЛЕ базовых импортов
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackContext
)

# ==================== НАСТРОЙКА ЛОГГИРОВАНИЯ ====================

# СНАЧАЛА создаем логгер
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==================== ПОЛУЧЕНИЕ ТОКЕНА ====================

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logger.error("❌ Токен не найден! Добавьте BOT_TOKEN в переменные окружения Scalingo")
    exit(1)

logger.info(f"✅ Токен получен: {TOKEN[:4]}...{TOKEN[-4:]}")

# ==================== ЗАЩИТА ОТ ДУБЛИРОВАНИЯ ====================

def check_single_instance():
    """Проверяет, что запущен только один экземпляр бота"""
    import socket
    try:
        # Пытаемся занять порт для проверки
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', 9999))
        sock.close()
        
        if result == 0:
            logger.error("⚠️ Другой экземпляр бота уже запущен!")
            return False
        return True
    except Exception as e:
        logger.error(f"⚠️ Ошибка проверки экземпляра: {e}")
        return True

# ==================== БАЗА ДАННЫХ ====================

def init_database():
    """Инициализация базы данных"""
    try:
        conn = sqlite3.connect('farm_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        
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
        
        conn.commit()
        conn.close()
        logger.info("✅ База данных инициализирована")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        return False

class Database:
    """Клас для роботи з базою даних"""
    
    @staticmethod
    def get_connection():
        """Повертає з'єднання з базою даних"""
        return sqlite3.connect('farm_bot.db', timeout=20, check_same_thread=False)
    
    @staticmethod
    def save_user(user_id: int, first_name: str = "", last_name: str = "", username: str = ""):
        """Зберігає або оновлює користувача"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, first_name, last_name, username)
                VALUES (?, ?, ?, ?)
            ''', (user_id, first_name, last_name, username))
            
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения пользователя: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def get_user_session(user_id: int) -> Dict:
        """Отримує сесію користувача"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT state, temp_data, last_section 
                FROM user_sessions 
                WHERE user_id = ?
            ''', (user_id,))
            
            row = cursor.fetchone()
            
            if row:
                state, temp_data_json, last_section = row
                temp_data = json.loads(temp_data_json) if temp_data_json else {}
                return {
                    "state": state,
                    "temp_data": temp_data,
                    "last_section": last_section
                }
            return {"state": "", "temp_data": {}, "last_section": "main_menu"}
        except Exception as e:
            logger.error(f"❌ Ошибка получения сессии: {e}")
            return {"state": "", "temp_data": {}, "last_section": "main_menu"}
        finally:
            conn.close()
    
    @staticmethod
    def save_user_session(user_id: int, state: str = "", temp_data: Dict = None, last_section: str = ""):
        """Зберігає сесію користувача"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            temp_data_json = json.dumps(temp_data) if temp_data else "{}"
            
            cursor.execute('''
                INSERT OR REPLACE INTO user_sessions (user_id, state, temp_data, last_section, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, state, temp_data_json, last_section))
            
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сессии: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def clear_user_session(user_id: int):
        """Очищає сесію користувача"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM user_sessions WHERE user_id = ?', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка очистки сессии: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def add_to_cart(user_id: int, product_id: int, quantity: float) -> bool:
        """Додає товар до кошика"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            # Проверяем, есть ли уже товар в корзине
            cursor.execute('''
                SELECT id, quantity FROM carts 
                WHERE user_id = ? AND product_id = ?
            ''', (user_id, product_id))
            
            existing = cursor.fetchone()
            
            if existing:
                cart_id, old_quantity = existing
                new_quantity = old_quantity + quantity
                cursor.execute('''
                    UPDATE carts SET quantity = ?, added_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (new_quantity, cart_id))
            else:
                cursor.execute('''
                    INSERT INTO carts (user_id, product_id, quantity)
                    VALUES (?, ?, ?)
                ''', (user_id, product_id, quantity))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления в корзину: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_cart_items(user_id: int) -> List[Dict]:
        """Отримує товари з кошика"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT id, product_id, quantity FROM carts WHERE user_id = ?', (user_id,))
            rows = cursor.fetchall()
            
            items = []
            for row in rows:
                cart_id, product_id, quantity = row
                product = next((p for p in PRODUCTS if p["id"] == product_id), None)
                if product:
                    items.append({
                        "cart_id": cart_id,
                        "product": product,
                        "quantity": quantity
                    })
            
            return items
        except Exception as e:
            logger.error(f"❌ Ошибка получения корзины: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def clear_cart(user_id: int):
        """Очищає кошик"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM carts WHERE user_id = ?', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка очистки корзины: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def remove_from_cart(cart_id: int):
        """Видаляє товар з кошика"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM carts WHERE id = ?', (cart_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка удаления из корзины: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def create_order(order_data: Dict) -> int:
        """Створює замовлення"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            # Используем транзакцию для избежания блокировок
            cursor.execute('BEGIN TRANSACTION')
            
            cursor.execute('''
                INSERT INTO orders (user_id, user_name, username, phone, city, np_department, total, order_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                order_data.get("user_id"),
                order_data.get("user_name"),
                order_data.get("username"),
                order_data.get("phone"),
                order_data.get("city"),
                order_data.get("np_department"),
                order_data.get("total"),
                order_data.get("order_type")
            ))
            
            order_id = cursor.lastrowid
            
            # Добавляем товары в заказ
            for item in order_data.get("items", []):
                cursor.execute('''
                    INSERT INTO order_items (order_id, product_name, quantity, price_per_unit)
                    VALUES (?, ?, ?, ?)
                ''', (
                    order_id,
                    item.get("product_name"),
                    item.get("quantity"),
                    item.get("price")
                ))
            
            # Очищаем корзину
            cursor.execute('DELETE FROM carts WHERE user_id = ?', (order_data.get("user_id"),))
            
            conn.commit()
            logger.info(f"✅ Заказ #{order_id} создан успешно")
            return order_id
        except Exception as e:
            logger.error(f"❌ Ошибка создания заказа: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def save_message(user_id: int, user_name: str, username: str, text: str, message_type: str):
        """Зберігає повідомлення"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO messages (user_id, user_name, username, text, message_type)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, user_name, username, text, message_type))
            
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сообщения: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def save_quick_order(user_id: int, user_name: str, username: str, product_id: int, 
                        product_name: str, quantity: float, phone: str = None, 
                        contact_method: str = "chat") -> int:
        """Зберігає швидке замовлення"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO quick_orders (user_id, user_name, username, product_id, product_name, 
                                        quantity, phone, contact_method)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, user_name, username, product_id, product_name, quantity, phone, contact_method))
            
            order_id = cursor.lastrowid
            conn.commit()
            logger.info(f"✅ Быстрый заказ #{order_id} сохранен")
            return order_id
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения быстрого заказа: {e}")
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def get_statistics() -> Dict:
        """Повертає статистику"""
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM orders')
            total_orders = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM messages')
            total_messages = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users')
            total_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM carts')
            active_carts = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM quick_orders')
            quick_orders = cursor.fetchone()[0]
            
            return {
                "total_orders": total_orders,
                "total_messages": total_messages,
                "total_users": total_users,
                "active_carts": active_carts,
                "quick_orders": quick_orders
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
        finally:
            conn.close()

# ==================== ДАНІ ПРОДУКТІВ ====================

PRODUCTS = [
    {
        "id": 1,
        "name": "Артишок маринований з зернами гірчиці",
        "category": "мариновані артишоки",
        "description": "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
        "price": 250,
        "unit": "банка",
        "image": "🥫",
        "details": {
            "volume": "Баночка 315 мл",
            "weight": "Маса нетто 280 г",
            "composition": "артишок 60%, вода, оцет винний, цукор, сіль, суміш спецій, зерна гірчиці",
            "availability": "є в наявності"
        }
    },
    {
        "id": 2,
        "name": "Артишок маринований з чилі",
        "category": "мариновані артишоки",
        "description": "Артишок вирощений та замаринований на Одещині, пікантний, не гострий.",
        "price": 250,
        "unit": "банка",
        "image": "🌶️",
        "details": {
            "volume": "Баночка 315 мл",
            "weight": "Маса нетто 280 г",
            "composition": "артишок 60%, вода, олія оливкова, оцет винний, цукор, сіль, суміш спецій, чилі",
            "availability": "є в наявності"
        }
    },
    {
        "id": 3,
        "name": "Паштет з артишоку",
        "category": "паштети",
        "description": "Ніжний паштет з артишоку, ідеальний для бутербродів та закусок.",
        "price": 290,
        "unit": "банка",
        "image": "🍯",
        "details": {
            "volume": "Баночка 200 г",
            "weight": "Маса нетто 200 г",
            "composition": "артишок, вершки, олія оливкова, спеції",
            "availability": "є в наявності"
        }
    }
]

FAQS = [
    {
        "question": "Які способи оплати ви приймаєте?",
        "answer": "✅ Готівка при отриманні\n✅ Переказ на карту ПриватБанку\n✅ Оплата через LiqPay"
    },
    {
        "question": "Які терміни доставки?",
        "answer": "🚚 Київ - 1-2 дні\n🚚 Україна - 2-4 дні\n🚛 Великі партії - 3-5 днів"
    },
    {
        "question": "Чи є гарантія якості?",
        "answer": "⭐ Всі продукти вирощені на Одещині\n⭐ Без штучних добавок\n⭐ Натуральне консервування\n⭐ Щоденний контроль якості"
    },
    {
        "question": "Як зберігати продукти?",
        "answer": "❄️ Мариновані артишоки - у холодильнику після відкриття\n🌡️ Паштети - у холодильнику після відкриття\n📦 Герметично закриті банки - при кімнатній температурі"
    },
    {
        "question": "Чи є знижки?",
        "answer": "🎁 При замовленні від 3 банок - знижка 5%\n🎁 Постійним клієнтам - знижка 10%\n🎁 При самовивозі з Великого Дальника - додаткова знижка 5%"
    },
    {
        "question": "Чи є доставка по всій Україні?",
        "answer": "✅ Так, доставляємо Новою Поштою по всій Україні\n🏪 Можливий самовивіз з Одеської області, с. Великий Дальник"
    },
    {
        "question": "Як оформити замовлення?",
        "answer": "🛒 Додайте товари в кошик → оформіть замовлення\n⚡ Або використайте швидке замовлення\n📞 Або зателефонуйте нам: +380932599103"
    }
]

COMPANY_INFO = {
    "name": "🌱 Компанія Бонелет",
    "description": "Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини.",
    "details": [
        "👨‍🌾 Працюємо з 2022 року",
        "📍 Розташування: Одеська область, с. Великий Дальник",
        "📞 Телефон: +380932599103",
        "🕒 Графік: ПН-ПТ 9:00-18:00 СБ 10:00-15:00",
        "🚚 Доставка: Новою Поштою по всій Україні"
    ]
}

# ==================== ГЕНЕРАТОРИ КЛАВІАТУР ====================

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

def get_main_menu() -> InlineKeyboardMarkup:
    """Головне меню"""
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
    """Повертає кнопку 'Назад'"""
    buttons = [[{"text": "🔙 Назад", "callback_data": f"back_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu() -> InlineKeyboardMarkup:
    """Меню продуктів"""
    buttons = []
    
    for product in PRODUCTS:
        buttons.append([{
            "text": f"{product['image']} {product['name']} - {product['price']} грн/{product['unit']}",
            "callback_data": f"product_{product['id']}"
        }])
    
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_product_detail_menu(product_id: int) -> InlineKeyboardMarkup:
    """Меню деталей продукту"""
    buttons = [
        [{"text": "🛒 Додати в кошик", "callback_data": f"add_to_cart_{product_id}"}],
        [{"text": "⚡ Швидке замовлення", "callback_data": f"quick_order_{product_id}"}],
        [{"text": "🔙 Назад", "callback_data": "back_products"}]
    ]
    return create_inline_keyboard(buttons)

def get_quick_order_menu(product_id: int) -> InlineKeyboardMarkup:
    """Меню швидкого замовлення"""
    buttons = [
        [{"text": "📞 Зателефонуйте мені", "callback_data": f"quick_call_{product_id}"}],
        [{"text": "💬 Напишіть мені в чат", "callback_data": f"quick_chat_{product_id}"}],
        [{"text": "🔙 Назад", "callback_data": f"product_{product_id}"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_menu() -> InlineKeyboardMarkup:
    """Меню FAQ"""
    buttons = []
    
    for i, faq in enumerate(FAQS, 1):
        buttons.append([{
            "text": f"❔ {faq['question'][:40]}...",
            "callback_data": f"faq_{i}"
        }])
    
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_contact_menu() -> InlineKeyboardMarkup:
    """Меню контактів"""
    buttons = [
        [{"text": "📞 Зателефонувати", "callback_data": "call_us"}],
        [{"text": "📧 Написати email", "callback_data": "email_us"}],
        [{"text": "📍 Наша адреса", "callback_data": "our_address"}],
        [{"text": "💬 Написати нам тут", "callback_data": "write_here"}],
        [{"text": "🔙 Назад", "callback_data": "back_main_menu"}]
    ]
    return create_inline_keyboard(buttons)

def get_cart_menu(cart_items: List) -> InlineKeyboardMarkup:
    """Меню корзини"""
    buttons = []
    
    if cart_items:
        buttons.append([{"text": "✅ Оформити замовлення", "callback_data": "checkout_cart"}])
        buttons.append([{"text": "🗑️ Очистити корзину", "callback_data": "clear_cart"}])
        
        for item in cart_items:
            product_name = item["product"]["name"][:20]
            if len(item["product"]["name"]) > 20:
                product_name += "..."
            
            buttons.append([{
                "text": f"❌ {product_name} ({item['quantity']}{item['product']['unit']})",
                "callback_data": f"remove_from_cart_{item['cart_id']}"
            }])
    
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_order_confirmation_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура підтвердження замовлення"""
    buttons = [
        [{"text": "✅ Так, продовжити", "callback_data": "confirm_order_yes"}],
        [{"text": "❌ Ні, скасувати", "callback_data": "confirm_order_no"}]
    ]
    return create_inline_keyboard(buttons)

# ==================== УТІЛІТИ ДЛЯ ВАЛІДАЦІЇ ====================

def parse_quantity(text: str) -> Tuple[bool, float, str]:
    """Парсить кількість"""
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
    """Валідує телефон"""
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

# ==================== ГЕНЕРАТОРИ ТЕКСТУ ====================

def get_welcome_text() -> str:
    return """
<b>🇺🇦 Вітаємо у боті компанії Бонелет! 🌱</b>

Ми спеціалізуємося на вирощуванні овочів та фруктів на полях Одещини:

🥫 <b>Артишок маринований з зернами гірчиці</b> - пікантний, не гострий
🌶️ <b>Артишок маринований з чилі</b> - з нотками гостроти
🍯 <b>Паштет з артишоку</b> - ніжний для бутербродів

<b>🏢 Про нас:</b>
• Працюємо з 2022 року
• Розташування: Одеська область, с. Великий Дальник
• Доставка Новою Поштою по всій Україні

<b>Оберіть опцію з меню 👇</b>
    """

def get_company_text() -> str:
    text = f"""
<b>{COMPANY_INFO['name']}</b>

{COMPANY_INFO['description']}

<b>📋 Деталі:</b>
"""
    for detail in COMPANY_INFO['details']:
        text += f"• {detail}\n"
    
    text += "\n<b>🌿 Наша філософія:</b>\n"
    text += "• Вирощуємо на власних полях Одещини\n"
    text += "• Використовуємо натуральне консервування\n"
    text += "• Гарантуємо якість кожного продукту\n"
    text += "• Працюємо з любов'ю до природи\n"
    
    text += "\n<b>🚚 Доставка:</b>\n"
    text += "• Новою Поштою по всій Україні\n"
    text += "• Самовивіз з Одеської області, с. Великий Дальник\n"
    text += "• Терміни доставки: 1-4 дні в залежності від регіону\n"
    
    return text

def get_product_text(product_id: int) -> str:
    """Текст продукту"""
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "❌ Продукт не знайдено"
    
    return f"""
<b>{product['image']} {product['name']}</b>

📝 <i>{product['description']}</i>

💰 <b>Ціна:</b> {product['price']} грн/{product['unit']}
🏷️ <b>Категорія:</b> {product['category']}
📦 <b>Наявність:</b> {product['details']['availability']}

<b>📊 Характеристики:</b>
• {product['details']['volume']}
• {product['details']['weight']}

<b>🍽️ Склад:</b>
{product['details']['composition']}

<b>🌟 Переваги:</b>
• Вирощений на Одещині
• Натуральне консервування
• Без штучних добавок
• Висока якість

<b>💡 Як використовувати:</b>
Ідеально підходить як закуска, до салатів, м'ясних страв та як самостійна страва.
    """

def get_quick_order_text(product_id: int) -> str:
    """Текст швидкого замовлення"""
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "❌ Продукт не знайдено"
    
    return f"""
<b>⚡ Швидке замовлення: {product['image']} {product['name']}</b>

💬 <b>Як ви бажаєте, щоб ми з вами зв'язалися?</b>

📞 <b>Зателефонуйте мені</b> - ми зателефонуємо вам для уточнення деталей
💬 <b>Напишіть мені в чат</b> - ви можете написати всі деталі тут і ми відповімо

<i>Оберіть зручний для вас спосіб зв'язку 👇</i>
    """

def get_faq_text(faq_id: int) -> str:
    """Текст FAQ"""
    if 0 <= faq_id - 1 < len(FAQS):
        faq = FAQS[faq_id - 1]
        return f"""
<b>❔ {faq['question']}</b>

{faq['answer']}

<i>📞 Маєте інші запитання? Зв'яжіться з нами: +380932599103</i>
        """
    return "❌ Питання не знайдено"

def get_contact_text() -> str:
    return """
<b>📞 Зв'язок з нами</b>

Ми завжди раді допомогти вам!

<b>Оберіть спосіб зв'язку:</b>
• <b>Телефон</b> - для швидких запитань
• <b>Email</b> - для детальних консультацій
• <b>Адреса</b> - для самовивозу
• <b>Написати тут</b> - швидке повідомлення в чаті

<i>Просто напишіть нам повідомлення в цьому чаті 👇</i>
    """

def get_cart_text(cart_items: List[Dict]) -> str:
    """Текст корзини"""
    if not cart_items:
        return "🛒 <b>Ваша корзина порожня</b>\n\nДодайте товари з каталогу!"
    
    text = "🛒 <b>Ваша корзина</b>\n\n"
    
    total = 0
    for i, item in enumerate(cart_items, 1):
        quantity = item["quantity"]
        product = item["product"]
        item_total = product["price"] * quantity
        
        text += f"<b>{i}. {product['name']}</b>\n"
        text += f"   📊 Кількість: <b>{quantity} {product['unit']}</b>\n"
        text += f"   💰 Ціна: {product['price']} грн/{product['unit']} × {quantity} = <b>{item_total:.2f} грн</b>\n\n"
        
        total += item_total
    
    text += f"<b>📊 Всього товарів:</b> {len(cart_items)}\n"
    text += f"<b>💰 Загальна сума:</b> <b>{total:.2f} грн</b>\n\n"
    
    # Додаємо інформацію про знижки
    if len(cart_items) >= 3:
        discount = total * 0.05
        discount_total = total - discount
        text += f"🎁 <b>Знижка 5% за 3+ банок:</b> -{discount:.2f} грн\n"
        text += f"💵 <b>До сплати:</b> <b>{discount_total:.2f} грн</b>\n\n"
    
    text += "<i>Для оформлення замовлення натисніть кнопку нижче</i>"
    
    return text

# ==================== TELEGRAM HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    try:
        chat_id = update.effective_chat.id
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"👤 [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'}: /start")
        
        # Сохраняем пользователя
        Database.save_user(
            user_id,
            user.first_name,
            user.last_name or "",
            user.username or ""
        )
        
        # Очищаем сессию
        Database.clear_user_session(user_id)
        
        welcome = get_welcome_text()
        await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
        Database.save_user_session(user_id, last_section="main_menu")
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА В start: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    await update.message.reply_text("ℹ️ Допомога: оберіть опцію з меню", reply_markup=get_main_menu())

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /cancel"""
    user = update.effective_user
    user_id = user.id
    
    Database.clear_user_session(user_id)
    welcome = get_welcome_text()
    await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
    Database.save_user_session(user_id, last_section="main_menu")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик inline кнопок"""
    try:
        query = update.callback_query
        await query.answer()
        
        callback_id = query.id
        message = query.message
        chat_id = message.chat.id
        message_id = message.message_id
        data = query.data
        user = query.from_user
        user_id = user.id
        
        logger.info(f"🖱️ [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'} натиснув: {data}")
        
        # Сохраняем пользователя
        Database.save_user(
            user_id,
            user.first_name,
            user.last_name or "",
            user.username or ""
        )
        
        # Обробка кнопок "Назад"
        if data.startswith("back_"):
            back_target = data[5:]
            
            if back_target == "main_menu":
                welcome = get_welcome_text()
                await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="main_menu")
            
            elif back_target == "products":
                products_text = "📦 <b>Наші продукти</b>\n\nОберіть продукт для детальної інформації:"
                await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="products")
            
            elif back_target == "faq":
                faq_text = "❓ <b>Часті запитання</b>\n\nОберіть питання для отримання відповіді:"
                await query.edit_message_text(faq_text, reply_markup=get_faq_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="faq")
            
            elif back_target == "contact":
                contact_text = get_contact_text()
                await query.edit_message_text(contact_text, reply_markup=get_contact_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="contact")
            
            elif back_target == "cart":
                cart_items = Database.get_cart_items(user_id)
                cart_text = get_cart_text(cart_items)
                await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="cart")
            
            else:
                welcome = get_welcome_text()
                await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="main_menu")
        
        # Головное меню
        elif data == "company":
            company_text = get_company_text()
            await query.edit_message_text(company_text, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="company")
        
        elif data == "products":
            products_text = "📦 <b>Наші продукти</b>\n\nОберіть продукт для детальної інформації:"
            await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
        
        elif data.startswith("product_"):
            product_id = int(data.split("_")[1])
            product_text = get_product_text(product_id)
            await query.edit_message_text(product_text, reply_markup=get_product_detail_menu(product_id), parse_mode='HTML')
            Database.save_user_session(user_id, last_section=f"product_{product_id}")
        
        elif data.startswith("add_to_cart_"):
            product_id = int(data.split("_")[3])
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            # Сохраняем сессию
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_quantity", temp_data)
            
            # Запрос количества
            response = f"📦 <b>Додавання {product['name']} до кошика</b>\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "📊 <b>Введіть кількість (тільки число):</b>\n\n"
            response += f"<i>Наприклад: 1, 2, 3 (в {product['unit']})</i>"
            
            await context.bot.send_message(chat_id, response, parse_mode='HTML')
        
        elif data.startswith("quick_order_"):
            product_id = int(data.split("_")[2])
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            # Показываем меню выбора способа связи
            quick_order_text = get_quick_order_text(product_id)
            await query.edit_message_text(quick_order_text, reply_markup=get_quick_order_menu(product_id), parse_mode='HTML')
        
        elif data.startswith("quick_call_"):
            product_id = int(data.split("_")[2])
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            # Сохраняем сессию для запроса телефона
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_phone_for_quick_order", temp_data)
            
            # Запрос телефона
            response = f"📞 <b>Зателефонуйте мені: {product['name']}</b>\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "📱 <b>Введіть ваш номер телефону:</b>\n\n"
            response += "<i>Приклад: +380932599103 або 0932599103</i>\n\n"
            response += "<b>Ми зателефонуємо вам для уточнення деталей замовлення!</b>"
            
            await context.bot.send_message(chat_id, response, parse_mode='HTML')
        
        elif data.startswith("quick_chat_"):
            product_id = int(data.split("_")[2])
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            response = f"💬 <b>Напишіть мені в чат: {product['name']}</b>\n\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n\n"
            response += "💬 <b>Просто напишіть ваше повідомлення в цей чат!</b>\n\n"
            response += "Вкажіть:\n"
            response += "• Бажану кількість\n"
            response += "• Контактні дані\n"
            response += "• Бажаний час доставки\n\n"
            response += "<b>Ми відповімо вам найближчим часом для уточнення деталей замовлення!</b>"
            
            await context.bot.send_message(chat_id, response, parse_mode='HTML')
            
            # Логируем в консоль
            user_session = Database.get_user_session(user_id)
            user_name = f"User_{user_id}"
            
            logger.info(f"\n{'='*80}")
            logger.info(f"⚡ ШВИДКЕ ЗАМОВЛЕННЯ (ЧАТ):")
            logger.info(f"👤 Клієнт: {user_name}")
            logger.info(f"📦 Продукт: {product['name']}")
            logger.info(f"💰 Ціна: {product['price']} грн/{product['unit']}")
            logger.info(f"🆔 User ID: {user_id}")
            logger.info(f"💬 Контакт: Чат Telegram")
            logger.info(f"{'='*80}\n")
            
            Database.clear_user_session(user_id)
        
        elif data == "faq":
            faq_text = "❓ <b>Часті запитання</b>\n\nОберіть питання для отримання відповіді:"
            await query.edit_message_text(faq_text, reply_markup=get_faq_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="faq")
        
        elif data.startswith("faq_"):
            faq_id = int(data.split("_")[1])
            faq_text = get_faq_text(faq_id)
            await query.edit_message_text(faq_text, reply_markup=get_back_keyboard("faq"), parse_mode='HTML')
        
        elif data == "cart":
            cart_items = Database.get_cart_items(user_id)
            cart_text = get_cart_text(cart_items)
            await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="cart")
        
        elif data.startswith("remove_from_cart_"):
            cart_id = int(data.split("_")[3])
            Database.remove_from_cart(cart_id)
            
            # Обновляем корзину
            cart_items = Database.get_cart_items(user_id)
            cart_text = get_cart_text(cart_items)
            await query.edit_message_text(cart_text, reply_markup=get_cart_menu(cart_items), parse_mode='HTML')
        
        elif data == "checkout_cart":
            cart_items = Database.get_cart_items(user_id)
            
            if not cart_items:
                response = "🛒 <b>Ваша корзина порожня</b>\n\n"
                response += "Додайте товари з каталогу перед оформленням замовлення!"
                await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
                return
            
            # Начинаем оформление
            Database.save_user_session(user_id, "full_order_name", {})
            
            # Запрос ФИО
            response = "🛒 <b>Оформлення замовлення</b>\n\n"
            response += f"📦 У вашій корзині: <b>{len(cart_items)} товар(ів)</b>\n"
            
            total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
            response += f"💰 Загальна сума: <b>{total:.2f} грн</b>\n\n"
            response += "📝 <b>Введіть ваше ПІБ (повне ім'я):</b>\n\n"
            response += "<i>Наприклад: Іванов Іван Іванович</i>"
            
            await context.bot.send_message(chat_id, response, parse_mode='HTML')
        
        elif data == "clear_cart":
            Database.clear_cart(user_id)
            
            response = "🗑️ <b>Корзина очищена!</b>\n\n"
            response += "Ваша корзина тепер порожня.\n"
            response += "<i>Додайте товари з каталогу.</i>"
            
            await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
        
        elif data == "my_orders":
            text = "📋 <b>Мої замовлення</b>\n\n"
            text += "Функція перегляду замовлень знаходиться в розробці.\n"
            text += "<i>Зв'яжіться з нами для отримання інформації про ваші замовлення.</i>"
            
            await query.edit_message_text(text, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="my_orders")
        
        elif data == "contact":
            contact_text = get_contact_text()
            await query.edit_message_text(contact_text, reply_markup=get_contact_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="contact")
        
        elif data == "write_here":
            Database.save_user_session(user_id, "waiting_message")
            
            response = "💬 <b>Написати нам тут</b>\n\n"
            response += "Напишіть ваше повідомлення прямо в цьому чаті:\n\n"
            response += "• Питання про продукти\n"
            response += "• Консультація\n"
            response += "• Пропозиції співпраці\n"
            response += "• Інші питання\n\n"
            response += "<i>Ми відповімо вам найближчим часом!</i>"
            
            await context.bot.send_message(chat_id, response, parse_mode='HTML')
        
        elif data in ["call_us", "email_us", "our_address"]:
            if data == "call_us":
                contact_info = "📞 <b>Телефон для зв'язку:</b>\n\n"
                contact_info += "✅ <code>+380932599103</code>\n\n"
                contact_info += "<i>Графік роботи: Пн-Пт 9:00-18:00, Сб 10:00-15:00</i>"
            
            elif data == "email_us":
                contact_info = "📧 <b>Email для листування:</b>\n\n"
                contact_info += "Напишіть нам повідомлення в цьому чаті, і ми надамо email для подальшого листування.\n\n"
                contact_info += "<i>Відповідаємо протягом 24 годин</i>"
            
            else:  # our_address
                contact_info = "📍 <b>Наша адреса:</b>\n\n"
                contact_info += "🏠 Одеська область\n"
                contact_info += "📌 село Великий Дальник\n"
                contact_info += "🚗 <b>Самовивіз можливий за попереднім домовленням</b>\n\n"
                contact_info += "<i>Графік самовивозу: Пн-Пт 9:00-18:00, Сб 10:00-15:00</i>"
            
            await query.edit_message_text(contact_info, reply_markup=get_back_keyboard("contact"), parse_mode='HTML')
        
        elif data.startswith("confirm_order_"):
            if data == "confirm_order_yes":
                # Получаем данные
                session = Database.get_user_session(user_id)
                temp_data = session["temp_data"]
                
                try:
                    # Создаем заказ
                    order_id = Database.create_order(temp_data)
                    
                    if order_id > 0:
                        # Логируем
                        logger.info(f"\n{'='*80}")
                        logger.info(f"✅ НОВИЙ ЗАМОВЛЕННЯ #{order_id}:")
                        logger.info(f"👤 Клієнт: {temp_data.get('user_name', '')}")
                        logger.info(f"📞 Телефон: {temp_data.get('phone', '')}")
                        logger.info(f"🏙️ Місто: {temp_data.get('city', '')}")
                        logger.info(f"🏣 НП: {temp_data.get('np_department', '')}")
                        logger.info(f"💰 Сума: {temp_data.get('total', 0):.2f} грн")
                        logger.info(f"🛒 Товарів: {len(temp_data.get('items', []))}")
                        logger.info(f"🆔 User ID: {user_id}")
                        logger.info(f"{'='*80}\n")
                        
                        # Очищаем сессию
                        Database.clear_user_session(user_id)
                        
                        # Отправляем подтверждение
                        text = f"✅ <b>Замовлення оформлено!</b>\n\n"
                        text += f"🆔 Номер замовлення: <b>#{order_id}</b>\n"
                        text += f"👤 ПІБ: <b>{temp_data.get('user_name', '')}</b>\n"
                        text += f"📱 Телефон: <b>{temp_data.get('phone', '')}</b>\n"
                        text += f"🏙️ Місто: <b>{temp_data.get('city', '')}</b>\n"
                        text += f"🏣 Відділення Нової Пошти: <b>{temp_data.get('np_department', '')}</b>\n"
                        text += f"💰 Сума: <b>{temp_data.get('total', 0):.2f} грн</b>\n\n"
                        text += "📞 <b>Ми зв'яжемось з вами для підтвердження!</b>\n\n"
                        text += "<i>Дякуємо за замовлення! 🌱</i>"
                    else:
                        text = "❌ <b>Помилка оформлення замовлення!</b>\n\n"
                        text += "Будь ласка, спробуйте ще раз або зв'яжіться з нами.\n\n"
                        text += "<i>Вибачте за незручності.</i>"
                        Database.clear_user_session(user_id)
                except Exception as e:
                    logger.error(f"❌ Ошибка при создании заказа: {e}")
                    text = "❌ <b>Помилка оформлення замовлення!</b>\n\n"
                    text += "Будь ласка, спробуйте ще раз.\n\n"
                    text += "<i>Вибачте за незручності.</i>"
                    Database.clear_user_session(user_id)
                
            else:
                text = "❌ <b>Замовлення скасовано</b>\n\n"
                text += "Ви можете продовжити покупки.\n"
                text += "<i>Ваша корзина збережена.</i>"
                Database.clear_user_session(user_id)
            
            await query.edit_message_text(text, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
        
        else:
            logger.warning(f"⚠️ Невідомий callback: {data}")
            welcome = get_welcome_text()
            await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"❌ Ошибка обработки callback: {e}")
        try:
            text = "❌ <b>Сталася помилка</b>\n\n"
            text += "Будь ласка, спробуйте ще раз або використайте /start"
            keyboard = get_main_menu()
            await query.edit_message_text(text, keyboard, parse_mode='HTML')
        except:
            pass

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    try:
        chat_id = update.effective_chat.id
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip()
        
        logger.info(f"👤 [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'}: {text}")
        
        # Сохраняем пользователя
        Database.save_user(
            user_id,
            user.first_name,
            user.last_name or "",
            user.username or ""
        )
        
        # Команды /start и /cancel
        if text == "/start" or text == "/cancel" or text.lower() == "скасувати":
            Database.clear_user_session(user_id)
            welcome = get_welcome_text()
            await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        # Команда /help
        if text == "/help":
            await update.message.reply_text("ℹ️ Допомога: оберіть опцію з меню", reply_markup=get_main_menu())
            return
        
        # Получаем состояние пользователя
        session = Database.get_user_session(user_id)
        state = session["state"]
        temp_data = session["temp_data"]
        
        # Обработка состояний
        if state == "waiting_quantity":
            product_id = temp_data.get("product_id")
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await update.message.reply_text("❌ Помилка: продукт не знайдено", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            # Парсим количество
            success, quantity, error_msg = parse_quantity(text)
            
            if not success:
                response = f"❌ <b>Невірний формат!</b>\n\n{error_msg}\n\n"
                response += f"<b>Продукт:</b> {product['name']}\n"
                response += f"<b>Ціна:</b> {product['price']} грн/{product['unit']}\n\n"
                response += "📊 <b>Введіть кількість (тільки число):</b>\n"
                response += f"<i>Наприклад: 1, 2, 3 (в {product['unit']})</i>"
                
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            # Добавляем в корзину
            Database.add_to_cart(user_id, product_id, quantity)
            
            # Очищаем сессию
            Database.clear_user_session(user_id)
            
            # Показываем подтверждение
            total_price = product["price"] * quantity
            response = f"✅ <b>{product['name']}</b> додано до кошика!\n\n"
            response += f"📊 Кількість: <b>{quantity} {product['unit']}</b>\n"
            response += f"💰 Ціна: {product['price']} грн/{product['unit']}\n"
            response += f"💵 Сума: <b>{total_price:.2f} грн</b>\n\n"
            
            cart_items = Database.get_cart_items(user_id)
            response += f"🛒 У кошику: <b>{len(cart_items)} товар(ів)</b>\n\n"
            response += "<i>Продовжуйте додавати товари або перейдіть до оформлення замовлення.</i>"
            
            await update.message.reply_text(response, parse_mode='HTML')
            
            # Показываем продукты
            products_text = "📦 <b>Наші продукти</b>\n\nОберіть продукт для детальної інформації:"
            await update.message.reply_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
        
        elif state == "waiting_message":
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            # Сохраняем сообщение
            Database.save_message(user_id, user_name, username, text, "повідомлення з меню")
            
            # Логируем
            logger.info(f"\n{'='*80}")
            logger.info(f"💬 НОВЕ ПОВІДОМЛЕННЯ:")
            logger.info(f"👤 Ім'я: {user_name}")
            logger.info(f"📱 Username: {username}")
            logger.info(f"🆔 ID: {user_id}")
            logger.info(f"💬 Текст: {text}")
            logger.info(f"🕒 Час: {datetime.now().isoformat()}")
            logger.info(f"{'='*80}\n")
            
            # Отвечаем
            response = "✅ <b>Повідомлення отримано!</b>\n\n"
            response += "Ми відповімо вам найближчим часом.\n"
            response += "<i>Дякуємо за звернення! 🌱</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.clear_user_session(user_id)
            Database.save_user_session(user_id, last_section="main_menu")
        
        elif state.startswith("full_order_"):
            if state == "full_order_name":
                temp_data["user_name"] = text
                temp_data["username"] = user.username or "немає"
                Database.save_user_session(user_id, "full_order_phone", temp_data)
                
                response = "📱 <b>Введіть ваш номер телефону:</b>\n\n"
                response += "<i>Приклад: +380932599103 або 0932599103</i>"
                await update.message.reply_text(response, parse_mode='HTML')
            
            elif state == "full_order_phone":
                # Валидация телефона
                phone = text.strip()
                is_valid, formatted_phone = validate_phone(phone)
                
                if not is_valid:
                    response = f"❌ <b>Невірний номер телефону!</b>\n\n"
                    response += "📱 <b>Введіть ваш номер телефону ще раз:</b>\n"
                    response += "<i>Приклад: +380932599103 або 0932599103</i>"
                    
                    await update.message.reply_text(response, parse_mode='HTML')
                    return
                
                temp_data["phone"] = formatted_phone
                Database.save_user_session(user_id, "full_order_city", temp_data)
                
                response = "🏙️ <b>Введіть місто доставки:</b>\n\n"
                response += "<i>Наприклад: Київ, Львів, Одеса</i>"
                await update.message.reply_text(response, parse_mode='HTML')
            
            elif state == "full_order_city":
                temp_data["city"] = text
                Database.save_user_session(user_id, "full_order_np", temp_data)
                
                response = "🏣 <b>Введіть номер відділення Нової Пошти:</b>\n\n"
                response += "<i>Наприклад: Відділення №25, Поштомат №12345</i>"
                await update.message.reply_text(response, parse_mode='HTML')
            
            elif state == "full_order_np":
                temp_data["np_department"] = text
                
                # Рассчитываем сумму
                cart_items = Database.get_cart_items(user_id)
                total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                
                # Применяем скидку если 3+ банок
                if len(cart_items) >= 3:
                    total = total * 0.95  # 5% скидка
                
                temp_data["total"] = total
                temp_data["order_type"] = "повне замовлення"
                temp_data["user_id"] = user_id
                
                # Подготавливаем товары
                order_items = []
                for item in cart_items:
                    order_items.append({
                        "product_name": item["product"]["name"],
                        "quantity": item["quantity"],
                        "price": item["product"]["price"]
                    })
                
                temp_data["items"] = order_items
                
                # Сохраняем
                Database.save_user_session(user_id, "full_order_confirm", temp_data)
                
                # Показываем подтверждение
                response = "✅ <b>Дані отримано! Перевірте інформацію:</b>\n\n"
                response += f"👤 <b>ПІБ:</b> {temp_data.get('user_name', '')}\n"
                response += f"📱 <b>Телефон:</b> {temp_data.get('phone', '')}\n"
                response += f"🏙️ <b>Місто:</b> {temp_data.get('city', '')}\n"
                response += f"🏣 <b>Відділення Нової Пошти:</b> {text}\n"
                response += f"🛒 <b>Товарів у кошику:</b> {len(cart_items)}\n"
                
                if len(cart_items) >= 3:
                    original_total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                    discount = original_total * 0.05
                    response += f"🎁 <b>Знижка 5% за 3+ банок:</b> -{discount:.2f} грн\n"
                
                response += f"💰 <b>Загальна сума:</b> {total:.2f} грн\n\n"
                response += "<b>Підтвердити замовлення?</b>"
                
                await update.message.reply_text(response, reply_markup=get_order_confirmation_keyboard(), parse_mode='HTML')
        
        elif state == "waiting_phone_for_quick_order":
            phone = text.strip()
            product_id = temp_data.get("product_id")
            
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            if not product:
                await update.message.reply_text("❌ Помилка: продукт не знайдено", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            # Валидация
            is_valid, formatted_phone = validate_phone(phone)
            
            if not is_valid:
                response = f"❌ <b>Невірний номер телефону!</b>\n\n"
                response += "📱 <b>Введіть ваш номер телефону ще раз:</b>\n"
                response += "<i>Приклад: +380932599103 або 0932599103</i>"
                
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            # Сохраняем быстрый заказ
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            order_id = Database.save_quick_order(
                user_id, user_name, username, product_id, product["name"], 
                0, formatted_phone, "call"
            )
            
            # Логируем
            logger.info(f"\n{'='*80}")
            logger.info(f"⚡ ШВИДКЕ ЗАМОВЛЕННЯ #{order_id} (ТЕЛЕФОН):")
            logger.info(f"👤 Клієнт: {user_name}")
            logger.info(f"📞 Телефон: {formatted_phone}")
            logger.info(f"📦 Продукт: {product['name']}")
            logger.info(f"💰 Ціна: {product['price']} грн/{product['unit']}")
            logger.info(f"🆔 User ID: {user_id}")
            logger.info(f"📱 Username: {username}")
            logger.info(f"{'='*80}\n")
            
            # Очищаем сессию
            Database.clear_user_session(user_id)
            
            # Отвечаем
            response = f"✅ <b>Швидке замовлення прийнято!</b>\n\n"
            response += f"🆔 <b>Номер замовлення:</b> #{order_id}\n"
            response += f"📦 <b>Продукт:</b> {product['name']}\n"
            response += f"📞 <b>Ваш телефон:</b> {formatted_phone}\n\n"
            response += "<b>Ми зателефонуємо вам найближчим часом для уточнення деталей!</b>\n\n"
            response += "<i>Дякуємо за замовлення! 🌱</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
        
        else:
            # Обычное сообщение
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            # Сохраняем сообщение
            Database.save_message(user_id, user_name, username, text, "повідомлення в чаті")
            
            # Отвечаем
            response = "✅ <b>Повідомлення отримано!</b>\n\n"
            response += "Ми відповімо вам найближчим часом.\n"
            response += "<i>Дякуємо за звернення! 🌱</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"❌ ОШИБКА В message_handler: {e}")

# ==================== ОБРАБОТЧИК ОШИБОК ====================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    try:
        logger.error(f"⚠️ Ошибка во время обработки обновления {update}: {context.error}")
        
        if 'Conflict' in str(context.error):
            logger.warning("🔄 Обнаружен конфликт - возможно запущен дублирующий бот")
            # Не пытаемся отправлять сообщение, чтобы не усугублять проблему
            return
        
        # Для других ошибок можно уведомить пользователя
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ <b>Виникла помилка</b>\n\nБудь ласка, спробуйте ще раз або використайте /start",
                    parse_mode='HTML'
                )
            except:
                pass
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок: {e}")

# ==================== ЗАПУСК БОТА ====================

def main():
    """Основная функция запуска бота"""
    try:
        # Проверяем, не запущен ли уже бот
        if not check_single_instance():
            logger.error("🚫 Бот уже запущен в другом процессе! Завершаем...")
            sys.exit(1)
        
        # Добавляем задержку для предотвращения конфликтов при перезапуске
        time.sleep(2)
        
        # Инициализируем базу данных
        if not init_database():
            logger.error("❌ Не удалось инициализировать базу данных")
            return
        
        # Логируем статистику
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
        logger.info("=" * 80)
        logger.info("🔄 Очікування повідомлень...\n")
        
        # Создаем приложение с обработчиком ошибок
        application = Application.builder().token(TOKEN).build()
        
        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("cancel", cancel_command))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        
        # Добавляем обработчик ошибок
        application.add_error_handler(error_handler)
        
        # Запускаем бота с параметрами для избежания конфликтов
        logger.info("🚀 Запуск polling...")
        application.run_polling(
            drop_pending_updates=True,      # Игнорировать старые сообщения
            allowed_updates=Update.ALL_TYPES,
            poll_interval=2.0,              # Интервал опроса
            timeout=30,                     # Таймаут запроса
            read_timeout=30,                # Таймаут чтения
            connect_timeout=30,             # Таймаут подключения
            pool_timeout=30,                # Таймаут пула
            close_loop=False                # Не закрывать event loop
        )
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(f"Тип ошибки: {type(e)}")
        import traceback
        logger.error(f"Трейсбэк: {traceback.format_exc()}")
        
        # Ждем перед повторной попыткой (если будет перезапуск)
        time.sleep(10)

if __name__ == "__main__":
    main()

