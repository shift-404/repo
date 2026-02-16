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
    logger.error("❌ Токен не найден! Добавьте BOT_TOKEN в переменные окружения")
    exit(1)

logger.info(f"✅ Токен получен: {TOKEN[:4]}...{TOKEN[-4:]}")

# ==================== ПІДКЛЮЧЕННЯ ДО POSTGRESQL ====================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL не знайдено! Додайте змінну середовища")
    exit(1)

def get_db_connection():
    """Підключення до PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Помилка підключення до БД: {e}")
        return None

# ==================== ІНІЦІАЛІЗАЦІЯ БАЗИ ДАНИХ ====================

def init_database():
    """Створення таблиць в PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Таблиця користувачів
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблиця сесій
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id BIGINT PRIMARY KEY,
                state TEXT DEFAULT '',
                temp_data TEXT DEFAULT '{}',
                last_section TEXT DEFAULT 'main_menu',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблиця кошиків
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS carts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                product_id INTEGER,
                quantity REAL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблиця замовлень
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
        
        # Таблиця елементів замовлень
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER,
                product_name TEXT,
                quantity REAL,
                price_per_unit REAL
            )
        ''')
        
        # Таблиця повідомлень
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
        
        # Таблиця швидких замовлень
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
                status TEXT DEFAULT 'нове',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблиця товарів
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
        
        # Таблиця відгуків
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
        
        # Таблиця адмінів
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Перевіряємо чи є товари, якщо ні - додаємо базові
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()['count']
        
        if count == 0:
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
            
            for product in products:
                cursor.execute('''
                    INSERT INTO products (id, name, price, category, description, unit, image, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                ''', product)
        
        conn.commit()
        logger.info("✅ База даних PostgreSQL ініціалізована")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка ініціалізації бази даних: {e}")
        return False
    finally:
        conn.close()

# ==================== ШЛЯХИ ДЛЯ ЛОГІВ ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

ORDERS_LOG = os.path.join(LOGS_DIR, "orders.txt")
USERS_LOG = os.path.join(LOGS_DIR, "users.txt")
MESSAGES_LOG = os.path.join(LOGS_DIR, "messages.txt")
QUICK_ORDERS_LOG = os.path.join(LOGS_DIR, "quick_orders.txt")

# ==================== ФУНКЦІЇ ЛОГУВАННЯ ====================

def log_order(order_data: dict):
    """Запис замовлення у файл"""
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
            f.write(f"Товари:\n")
            for item in order_data.get('items', []):
                f.write(f"  - {item.get('product_name')} x {item.get('quantity')} = {item.get('price', 0) * item.get('quantity', 0):.2f} грн\n")
            f.write(f"Статус: {order_data.get('status', 'нове')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"Помилка запису замовлення: {e}")

def log_user(user_data: dict):
    """Запис користувача у файл"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(USERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | ID:{user_data.get('user_id')} | {user_data.get('first_name', '')} {user_data.get('last_name', '')} | @{user_data.get('username', '')}\n")
    except Exception as e:
        logger.error(f"Помилка запису користувача: {e}")

def log_message(msg_data: dict):
    """Запис повідомлення у файл"""
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
    """Запис швидкого замовлення у файл"""
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
            f.write(f"Статус: {order_data.get('status', 'нове')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"Помилка запису швидкого замовлення: {e}")

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

# ==================== КЛАС DATABASE (ПЕРЕРОБЛЕНИЙ ПІД POSTGRESQL) ====================

class Database:
    """Клас для роботи з PostgreSQL"""
    
    @staticmethod
    def get_connection():
        """Повертає з'єднання з базою даних"""
        return get_db_connection()
    
    @staticmethod
    def save_user(user_id: int, first_name: str = "", last_name: str = "", username: str = ""):
        """Зберігає або оновлює користувача"""
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
            logger.error(f"❌ Ошибка сохранения пользователя: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def get_user_session(user_id: int) -> Dict:
        """Отримує сесію користувача"""
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
            logger.error(f"❌ Ошибка сохранения сессии: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def clear_user_session(user_id: int):
        """Очищає сесію користувача"""
        conn = Database.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_sessions WHERE user_id = %s', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка очистки сессии: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def add_to_cart(user_id: int, product_id: int, quantity: float) -> bool:
        """Додає товар до кошика"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            # Проверяем, есть ли уже товар в корзине
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
            logger.error(f"❌ Ошибка добавления в корзину: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_cart_items(user_id: int) -> List[Dict]:
        """Отримує товари з кошика"""
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
            logger.error(f"❌ Ошибка получения корзины: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def clear_cart(user_id: int):
        """Очищає кошик"""
        conn = Database.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM carts WHERE user_id = %s', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка очистки корзины: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def remove_from_cart(cart_id: int):
        """Видаляє товар з кошика"""
        conn = Database.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM carts WHERE id = %s', (cart_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка удаления из корзины: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def create_order(order_data: Dict) -> int:
        """Створює замовлення"""
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
            
            # Добавляем товары в заказ
            for item in order_data.get("items", []):
                cursor.execute('''
                    INSERT INTO order_items (order_id, product_name, quantity, price_per_unit)
                    VALUES (%s, %s, %s, %s)
                ''', (
                    order_id,
                    item.get("product_name"),
                    item.get("quantity"),
                    item.get("price")
                ))
            
            # Очищаем корзину
            cursor.execute('DELETE FROM carts WHERE user_id = %s', (order_data.get("user_id"),))
            
            conn.commit()
            logger.info(f"✅ Заказ #{order_id} создан успешно")
            return order_id
        except Exception as e:
            logger.error(f"❌ Ошибка создания заказа: {e}")
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def save_message(user_id: int, user_name: str, username: str, text: str, message_type: str):
        """Зберігає повідомлення"""
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
            logger.error(f"❌ Ошибка сохранения сообщения: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def save_quick_order(user_id: int, user_name: str, username: str, product_id: int, 
                        product_name: str, quantity: float, phone: str = None, 
                        contact_method: str = "chat") -> int:
        """Зберігає швидке замовлення"""
        conn = Database.get_connection()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO quick_orders (user_id, user_name, username, product_id, product_name, 
                                        quantity, phone, contact_method, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (user_id, user_name, username, product_id, product_name, quantity, phone, contact_method, "нове"))
            
            result = cursor.fetchone()
            order_id = result['id'] if result else 0
            
            conn.commit()
            logger.info(f"✅ Быстрый заказ #{order_id} сохранен")
            return order_id
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения быстрого заказа: {e}")
            return 0
        finally:
            conn.close()
    
    @staticmethod
    def save_review(user_id: int, user_name: str, order_id: int, text: str, rating: int = 5):
        """Зберігає відгук"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO reviews (user_id, user_name, order_id, text, rating)
                VALUES (%s, %s, %s, %s, %s)
            ''', (user_id, user_name, order_id, text, rating))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения отзыва: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_statistics() -> Dict:
        """Повертає статистику"""
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
            
            cursor.execute("SELECT COUNT(*) FROM reviews")
            total_reviews = cursor.fetchone()['count']
            
            return {
                "total_orders": total_orders,
                "total_messages": total_messages,
                "total_users": total_users,
                "active_carts": active_carts,
                "quick_orders": quick_orders,
                "total_revenue": total_revenue,
                "total_reviews": total_reviews
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
        finally:
            conn.close()
    
    @staticmethod
    def get_all_products():
        """Отримує всі товари з БД"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM products ORDER BY id')
            rows = cursor.fetchall()
            
            products = []
            for row in rows:
                products.append({
                    "id": row['id'],
                    "name": row['name'],
                    "price": row['price'],
                    "category": row['category'],
                    "description": row['description'],
                    "unit": row['unit'],
                    "image": row['image'],
                    "details": row['details']
                })
            return products
        except Exception as e:
            logger.error(f"❌ Ошибка получения товаров: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_product_by_id(product_id: int):
        """Отримує товар за ID"""
        products = Database.get_all_products()
        for product in products:
            if product["id"] == product_id:
                return product
        return None
    
    @staticmethod
    def update_product(product_id: int, **kwargs):
        """Оновлює товар"""
        conn = Database.get_connection()
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
            logger.error(f"❌ Ошибка обновления товара: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def add_product(name: str, price: float, category: str, description: str, unit: str, image: str, details: str):
        """Додає новий товар"""
        conn = Database.get_connection()
        if not conn:
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
            return product_id
        except Exception as e:
            logger.error(f"❌ Ошибка добавления товара: {e}")
            return None
        finally:
            conn.close()
    
    @staticmethod
    def delete_product(product_id: int):
        """Видаляє товар"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM products WHERE id = %s', (product_id,))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления товара: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_all_orders():
        """Отримує всі замовлення"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM orders ORDER BY created_at DESC')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_all_users():
        """Отримує всіх користувачів"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users ORDER BY created_at DESC')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователей: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_all_admins():
        """Отримує всіх адмінів"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM admins')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения админов: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def add_admin(user_id: int, username: str = "", added_by: int = 0):
        """Додає адміна"""
        conn = Database.get_connection()
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
            logger.error(f"❌ Ошибка добавления админа: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def remove_admin(user_id: int):
        """Видаляє адміна"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM admins WHERE user_id = %s', (user_id,))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления админа: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def is_admin(user_id: int) -> bool:
        """Перевіряє чи є користувач адміном"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = %s', (user_id,))
            count = cursor.fetchone()['count']
            return count > 0
        except Exception as e:
            logger.error(f"❌ Ошибка проверки админа: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_order_by_id(order_id: int):
        """Отримує замовлення за ID"""
        conn = Database.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM orders WHERE order_id = %s', (order_id,))
            order = cursor.fetchone()
            
            if order:
                cursor.execute('SELECT * FROM order_items WHERE order_id = %s', (order_id,))
                items = cursor.fetchall()
                order['items'] = items
            
            return order
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа: {e}")
            return None
        finally:
            conn.close()
    
    @staticmethod
    def get_orders_by_phone(phone: str):
        """Отримує замовлення за номером телефону"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM orders 
                WHERE phone LIKE %s 
                ORDER BY created_at DESC
            ''', (f"%{phone}%",))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов по телефону: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_new_orders():
        """Отримує нові замовлення"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM orders 
                WHERE status = 'нове'
                ORDER BY created_at DESC
            ''')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения новых заказов: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_quick_orders():
        """Отримує швидкі замовлення"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM quick_orders 
                ORDER BY created_at DESC
            ''')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения быстрых заказов: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def update_order_status(order_id: int, status: str):
        """Оновлює статус замовлення"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE orders SET status = %s WHERE order_id = %s
            ''', (status, order_id))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_user_by_id(user_id: int):
        """Отримує користувача за ID"""
        conn = Database.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователя: {e}")
            return None
        finally:
            conn.close()
    
    @staticmethod
    def get_user_orders(user_id: int):
        """Отримує замовлення користувача"""
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
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов пользователя: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_user_messages(user_id: int):
        """Отримує повідомлення користувача"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM messages 
                WHERE user_id = %s 
                ORDER BY created_at DESC LIMIT 10
            ''', (user_id,))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения сообщений: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_user_quick_orders(user_id: int):
        """Отримує швидкі замовлення користувача"""
        conn = Database.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM quick_orders 
                WHERE user_id = %s 
                ORDER BY created_at DESC
            ''', (user_id,))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения быстрых заказов: {e}")
            return []
        finally:
            conn.close()

# ==================== ДАНІ ПРОДУКТІВ ====================

def get_products_from_db():
    """Отримує товари з БД для сумісності зі старим кодом"""
    return Database.get_all_products()

# Глобальна змінна для доступу до товарів
PRODUCTS = get_products_from_db()

# Оновлюємо PRODUCTS кожного разу при запуску
def refresh_products():
    global PRODUCTS
    PRODUCTS = get_products_from_db()
    logger.info(f"🔄 Оновлено товари: {len(PRODUCTS)} позицій")

# Викликаємо при старті
refresh_products()

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
    },
    {
        "question": "Як залишити відгук?",
        "answer": "⭐ Ви можете залишити відгук про наші продукти просто написавши його в чат після отримання замовлення. Ми будемо дуже вдячні!"
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
            {"text": "⭐ Мій відгук", "callback_data": "my_review"}
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
    refresh_products()  # Оновлюємо товари перед показом
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
        [{"text": "⭐ Залишити відгук", "callback_data": "leave_review"}],
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
                "text": f"❌ {product_name} ({item['quantity']} {item['product']['unit']})",
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

def get_review_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура для відгуку"""
    buttons = [
        [{"text": "⭐ 5 зірок", "callback_data": "review_5"}],
        [{"text": "⭐ 4 зірки", "callback_data": "review_4"}],
        [{"text": "⭐ 3 зірки", "callback_data": "review_3"}],
        [{"text": "⭐ 2 зірки", "callback_data": "review_2"}],
        [{"text": "⭐ 1 зірка", "callback_data": "review_1"}],
        [{"text": "🔙 Назад", "callback_data": "back_main_menu"}]
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
    refresh_products()  # Оновлюємо товари
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "❌ Продукт не знайдено"
    
    return f"""
<b>{product['image']} {product['name']}</b>

📝 <i>{product['description']}</i>

💰 <b>Ціна:</b> {product['price']} грн/{product['unit']}
🏷️ <b>Категорія:</b> {product['category']}
📦 <b>Наявність:</b> Є в наявності

<b>📊 Характеристики:</b>
• {product['details']}

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
    refresh_products()
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

<b>⭐ Залишити відгук</b> - поділіться враженнями про наші продукти

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

def get_review_text() -> str:
    """Текст для відгуку"""
    return """
⭐ <b>Залишити відгук</b>

Ми будемо дуже вдячні, якщо ви поділитесь своїми враженнями про наші продукти!

<b>Оберіть оцінку:</b>
• 5 ⭐ - Чудово
• 4 ⭐ - Добре
• 3 ⭐ - Нормально
• 2 ⭐ - Погано
• 1 ⭐ - Жахливо

Після вибору оцінки напишіть ваш відгук текстом.
    """

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
        
        # Логуємо користувача
        log_user({
            "user_id": user_id,
            "first_name": user.first_name,
            "last_name": user.last_name or "",
            "username": user.username or ""
        })
        
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
            refresh_products()
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
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("❌ Продукт не знайдено", reply_markup=get_back_keyboard("products"))
                return
            
            # Показываем меню выбора способа связи
            quick_order_text = get_quick_order_text(product_id)
            await query.edit_message_text(quick_order_text, reply_markup=get_quick_order_menu(product_id), parse_mode='HTML')
        
        elif data.startswith("quick_call_"):
            product_id = int(data.split("_")[2])
            refresh_products()
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
            refresh_products()
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
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            
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
        
        elif data == "my_review":
            review_text = get_review_text()
            await query.edit_message_text(review_text, reply_markup=get_review_keyboard(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="review")
        
        elif data.startswith("review_"):
            if data == "review_5":
                rating = 5
            elif data == "review_4":
                rating = 4
            elif data == "review_3":
                rating = 3
            elif data == "review_2":
                rating = 2
            elif data == "review_1":
                rating = 1
            else:
                rating = 5
            
            Database.save_user_session(user_id, f"waiting_review_text", {"rating": rating})
            
            await query.edit_message_text(
                f"⭐ <b>Ваша оцінка: {rating} зірок</b>\n\n"
                f"Напишіть текст вашого відгуку:",
                parse_mode='HTML'
            )
        
        elif data == "leave_review":
            review_text = get_review_text()
            await query.edit_message_text(review_text, reply_markup=get_review_keyboard(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="review")
        
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
                        
                        # Логуємо замовлення у файл
                        temp_data["order_id"] = order_id
                        temp_data["status"] = "нове"
                        log_order(temp_data)
                        
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
                        text += "<i>Дякуємо за замовлення! 🌱</i>\n\n"
                        text += "⭐ Після отримання замовлення ви зможете залишити відгук у меню 'Мій відгук'"
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
        
        logger.info(f"👤 [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'Користувач'}: {text[:50]}...")
        
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
            refresh_products()
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
            
            # Логуємо повідомлення
            log_message({
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "повідомлення з меню"
            })
            
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
            
            refresh_products()
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
            
            # Логуємо швидке замовлення
            log_quick_order({
                "order_id": order_id,
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "phone": formatted_phone,
                "product_name": product["name"],
                "contact_method": "call",
                "status": "нове"
            })
            
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
        
        elif state == "waiting_review_text":
            rating = temp_data.get("rating", 5)
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            
            # Зберігаємо відгук
            if Database.save_review(user_id, user_name, 0, text, rating):
                response = f"✅ <b>Дякуємо за ваш відгук!</b>\n\n"
                response += f"⭐ Ваша оцінка: {rating}/5\n\n"
                response += f"Ваш відгук: \"{text}\"\n\n"
                response += "<i>Ми цінуємо вашу думку!</i>"
                
                # Логуємо відгук
                logger.info(f"\n{'='*80}")
                logger.info(f"⭐ НОВИЙ ВІДГУК:")
                logger.info(f"👤 Клієнт: {user_name}")
                logger.info(f"⭐ Оцінка: {rating}/5")
                logger.info(f"💬 Текст: {text}")
                logger.info(f"{'='*80}\n")
            else:
                response = "❌ <b>Помилка при збереженні відгуку</b>\n\nСпробуйте пізніше."
            
            Database.clear_user_session(user_id)
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
        
        else:
            # Обычное сообщение
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'немає'
            
            # Сохраняем сообщение
            Database.save_message(user_id, user_name, username, text, "повідомлення в чаті")
            
            # Логуємо повідомлення
            log_message({
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "повідомлення в чаті"
            })
            
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
        
        # Оновлюємо товари
        refresh_products()
        
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
        logger.info(f"• Виручка: {stats.get('total_revenue', 0):.2f} грн")
        logger.info(f"• Відгуків: {stats.get('total_reviews', 0)}")
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
            connect_timeout=30,              # Таймаут подключения
            pool_timeout=30,                  # Таймаут пула
            close_loop=False                  # Не закрывать event loop
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
