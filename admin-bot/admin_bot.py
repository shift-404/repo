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

# ÐÐ°Ð»Ð°ÑˆÑ‚ÑƒÐ²Ð°Ð½Ð½Ñ Ð»Ð¾Ð³ÑƒÐ²Ð°Ð½Ð½Ñ
logging.basicConfig(
    format='%(asctime)s - ADMIN - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('admin_bot_debug.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Ð”Ð¾Ð´Ð°Ñ‚ÐºÐ¾Ð²Ðµ Ð»Ð¾Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð´Ð»Ñ Ð²Ñ–Ð´Ð»Ð°Ð´ÐºÐ¸
debug_logger = logging.getLogger('debug')
debug_logger.setLevel(logging.DEBUG)

logger.info("=" * 80)
logger.info("Ð—ÐÐŸÐ£Ð¡Ðš ÐÐ”ÐœÐ†Ð-Ð‘ÐžÐ¢Ð Ð— Ð ÐžÐ—Ð¨Ð˜Ð Ð•ÐÐžÐ® Ð’Ð†Ð”Ð›ÐÐ”ÐšÐžÐ®")
logger.info("=" * 80)

KYIV_TZ = None
try:
    import pytz
    KYIV_TZ = pytz.timezone('Europe/Kyiv')
    logger.info("âœ… Ð‘Ñ–Ð±Ð»Ñ–Ð¾Ñ‚ÐµÐºÐ° pytz Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð°, Ñ‡Ð°ÑÐ¾Ð²Ð¸Ð¹ Ð¿Ð¾ÑÑ Kyiv")
except ImportError:
    logger.warning("âš ï¸ Ð‘Ñ–Ð±Ð»Ñ–Ð¾Ñ‚ÐµÐºÐ° pytz Ð½Ðµ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð°, Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð¾Ð²ÑƒÑŽ UTC")
    KYIV_TZ = None

def get_kyiv_time():
    if KYIV_TZ:
        return datetime.now(KYIV_TZ)
    return datetime.now()

def format_kyiv_time(dt_str):
    if not dt_str:
        return "Ð/Ð”"
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‡Ð°ÑÑƒ: {e}")
        return str(dt_str)[:16]

# Ð”Ñ–Ð°Ð³Ð½Ð¾ÑÑ‚Ð¸ÐºÐ°
logger.info("ðŸ“‚ ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð° Ð¿Ð°Ð¿ÐºÐ°: %s", os.getcwd())
logger.info("ðŸ“„ Ð¤Ð°Ð¹Ð»Ð¸ Ð² Ð¿Ð°Ð¿Ñ†Ñ–: %s", os.listdir('.'))

# ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ð½Ð°ÑÐ²Ð½Ñ–ÑÑ‚ÑŒ requirements.txt
if os.path.exists('requirements.txt'):
    logger.info("âœ… requirements.txt Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
    with open('requirements.txt', 'r') as f:
        content = f.read()
        logger.info("ðŸ“„ Ð’Ð¼Ñ–ÑÑ‚ requirements.txt:\n%s", content)
else:
    logger.error("âŒ requirements.txt ÐÐ• Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")

# ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ requests Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾
try:
    import requests
    logger.info("âœ… requests Ð²Ð¶Ðµ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾, Ð²ÐµÑ€ÑÑ–Ñ: %s", requests.__version__)
except ImportError:
    logger.error("âŒ requests Ð½Ðµ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾")
    
    import subprocess
    logger.info("ðŸ“¦ Ð’ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÑŽÑŽ requests...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    logger.info("âœ… requests Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾")
    import requests

def check_single_instance():
    """ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ” Ñ‡Ð¸ Ð½Ðµ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾ Ñ–Ð½ÑˆÐ¸Ð¹ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€ Ð±Ð¾Ñ‚Ð°"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', 9999))
        sock.close()
        if result == 0:
            logger.error("âš ï¸ Ð”Ñ€ÑƒÐ³Ð¸Ð¹ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€ Ð±Ð¾Ñ‚Ð° Ð²Ð¶Ðµ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾!")
            return False
        return True
    except Exception as e:
        logger.error(f"âš ï¸ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÐ¸ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€Ð°: {e}")
        return True

# ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ Ñ‚Ð¾ÐºÐµÐ½Ð¸ Ð· Ð¾Ñ‚Ð¾Ñ‡ÐµÐ½Ð½Ñ
TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not TOKEN:
    logger.error("âŒ ADMIN_BOT_TOKEN Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)
else:
    logger.info(f"âœ… ADMIN_BOT_TOKEN Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾: {TOKEN[:10]}...")

MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not MAIN_BOT_TOKEN:
    logger.error("âŒ BOT_TOKEN Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)
else:
    logger.info(f"âœ… MAIN_BOT_TOKEN Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾: {MAIN_BOT_TOKEN[:10]}...")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
logger.info(f"âœ… ADMIN_PASSWORD ÐºÐ¾Ð½Ñ„Ñ–Ð³ÑƒÑ€Ð¾Ð²Ð°Ð½Ð¾: {'Ñ‚Ð°Ðº' if ADMIN_PASSWORD else 'Ð½Ñ–'}")

ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]
logger.info(f"âœ… ADMIN_IDS: {ADMIN_IDS}")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("âŒ DATABASE_URL Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)
else:
    logger.info(f"âœ… DATABASE_URL Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾: {DATABASE_URL[:20]}...")

def get_db_connection():
    """ÐŸÑ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ… Ð· Ð´ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¸Ð¼ Ð»Ð¾Ð³ÑƒÐ²Ð°Ð½Ð½ÑÐ¼ Ð¿Ð¾Ð¼Ð¸Ð»Ð¾Ðº"""
    logger.debug("Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð‘Ð”...")
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        logger.debug("âœ… ÐŸÑ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð‘Ð” ÑƒÑÐ¿Ñ–ÑˆÐ½Ðµ")
        return conn
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð‘Ð”: {e}")
        logger.error(traceback.format_exc())
        return None

def init_database_if_empty():
    """Ð†Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ… Ð· Ð´ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¸Ð¼ Ð»Ð¾Ð³ÑƒÐ²Ð°Ð½Ð½ÑÐ¼"""
    logger.info("=" * 60)
    logger.info("ðŸ”„ Ð†Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ…...")
    logger.info("=" * 60)
    
    conn = get_db_connection()
    if not conn:
        logger.error("âŒ ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð” Ð´Ð»Ñ Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ—")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Ð¡Ñ‚Ð²Ð¾Ñ€ÐµÐ½Ð½Ñ Ñ‚Ð°Ð±Ð»Ð¸Ñ†ÑŒ
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
                status TEXT DEFAULT 'Ð½Ð¾Ð²Ðµ',
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
                status TEXT DEFAULT 'Ð½Ð¾Ð²Ðµ',
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
                unit TEXT DEFAULT 'Ð±Ð°Ð½ÐºÐ°',
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
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ ÐºÐ¾Ð»Ð¾Ð½ÐºÐ¸ ÑÐºÑ‰Ð¾ Ñ—Ñ… Ð½ÐµÐ¼Ð°Ñ”
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image TEXT')
            logger.info("âœ… ÐšÐ¾Ð»Ð¾Ð½ÐºÐ° image Ð´Ð¾Ð´Ð°Ð½Ð° Ð´Ð¾ Ñ‚Ð°Ð±Ð»Ð¸Ñ†Ñ– products")
        except Exception as e:
            logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ ÐºÐ¾Ð»Ð¾Ð½ÐºÐ¸ image: {e}")
        
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image_data BYTEA')
            logger.info("âœ… ÐšÐ¾Ð»Ð¾Ð½ÐºÐ° image_data Ð´Ð¾Ð´Ð°Ð½Ð° Ð´Ð¾ Ñ‚Ð°Ð±Ð»Ð¸Ñ†Ñ– products")
        except Exception as e:
            logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ ÐºÐ¾Ð»Ð¾Ð½ÐºÐ¸ image_data: {e}")
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– Ð´Ð»Ñ company_info
        cursor.execute("SELECT COUNT(*) FROM company_info")
        company_count = cursor.fetchone()['count']
        
        if company_count == 0:
            company_text = """
<b>ðŸŒ± ÐšÐ¾Ð¼Ð¿Ð°Ð½Ñ–Ñ Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚</b>

ÐœÐ¸ ÑÐ¿ÐµÑ†Ñ–Ð°Ð»Ñ–Ð·ÑƒÑ”Ð¼Ð¾ÑÑ Ð½Ð° Ð²Ð¸Ñ€Ð¾Ñ‰ÑƒÐ²Ð°Ð½Ð½Ñ– Ð¾Ð²Ð¾Ñ‡Ñ–Ð² Ñ‚Ð° Ñ„Ñ€ÑƒÐºÑ‚Ñ–Ð² Ð½Ð° Ð¿Ð¾Ð»ÑÑ… ÐžÐ´ÐµÑ‰Ð¸Ð½Ð¸.

<b>ðŸ“‹ Ð”ÐµÑ‚Ð°Ð»Ñ–:</b>
â€¢ ðŸ‘¨â€ðŸŒ¾ ÐŸÑ€Ð°Ñ†ÑŽÑ”Ð¼Ð¾ Ð· 2022 Ñ€Ð¾ÐºÑƒ
â€¢ ðŸ“ Ð Ð¾Ð·Ñ‚Ð°ÑˆÑƒÐ²Ð°Ð½Ð½Ñ: ÐžÐ´ÐµÑÑŒÐºÐ° Ð¾Ð±Ð»Ð°ÑÑ‚ÑŒ, Ñ. Ð’ÐµÐ»Ð¸ÐºÐ¸Ð¹ Ð”Ð°Ð»ÑŒÐ½Ð¸Ðº
â€¢ ðŸ“ž Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: +380932599103
â€¢ ðŸ•’ Ð“Ñ€Ð°Ñ„Ñ–Ðº: ÐŸÐ-ÐŸÐ¢ 9:00-18:00 Ð¡Ð‘ 10:00-15:00
â€¢ ðŸšš Ð”Ð¾ÑÑ‚Ð°Ð²ÐºÐ°: ÐÐ¾Ð²Ð¾ÑŽ ÐŸÐ¾ÑˆÑ‚Ð¾ÑŽ Ð¿Ð¾ Ð²ÑÑ–Ð¹ Ð£ÐºÑ€Ð°Ñ—Ð½Ñ–

<b>ðŸŒ¿ ÐÐ°ÑˆÐ° Ñ„Ñ–Ð»Ð¾ÑÐ¾Ñ„Ñ–Ñ:</b>
â€¢ Ð’Ð¸Ñ€Ð¾Ñ‰ÑƒÑ”Ð¼Ð¾ Ð½Ð° Ð²Ð»Ð°ÑÐ½Ð¸Ñ… Ð¿Ð¾Ð»ÑÑ… ÐžÐ´ÐµÑ‰Ð¸Ð½Ð¸
â€¢ Ð’Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð¾Ð²ÑƒÑ”Ð¼Ð¾ Ð½Ð°Ñ‚ÑƒÑ€Ð°Ð»ÑŒÐ½Ðµ ÐºÐ¾Ð½ÑÐµÑ€Ð²ÑƒÐ²Ð°Ð½Ð½Ñ
â€¢ Ð“Ð°Ñ€Ð°Ð½Ñ‚ÑƒÑ”Ð¼Ð¾ ÑÐºÑ–ÑÑ‚ÑŒ ÐºÐ¾Ð¶Ð½Ð¾Ð³Ð¾ Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ñƒ
â€¢ ÐŸÑ€Ð°Ñ†ÑŽÑ”Ð¼Ð¾ Ð· Ð»ÑŽÐ±Ð¾Ð²'ÑŽ Ð´Ð¾ Ð¿Ñ€Ð¸Ñ€Ð¾Ð´Ð¸

<b>ðŸšš Ð”Ð¾ÑÑ‚Ð°Ð²ÐºÐ°:</b>
â€¢ ÐÐ¾Ð²Ð¾ÑŽ ÐŸÐ¾ÑˆÑ‚Ð¾ÑŽ Ð¿Ð¾ Ð²ÑÑ–Ð¹ Ð£ÐºÑ€Ð°Ñ—Ð½Ñ–
â€¢ Ð¡Ð°Ð¼Ð¾Ð²Ð¸Ð²Ñ–Ð· Ð· ÐžÐ´ÐµÑÑŒÐºÐ¾Ñ— Ð¾Ð±Ð»Ð°ÑÑ‚Ñ–, Ñ. Ð’ÐµÐ»Ð¸ÐºÐ¸Ð¹ Ð”Ð°Ð»ÑŒÐ½Ð¸Ðº
â€¢ Ð¢ÐµÑ€Ð¼Ñ–Ð½Ð¸ Ð´Ð¾ÑÑ‚Ð°Ð²ÐºÐ¸: 1-4 Ð´Ð½Ñ– Ð² Ð·Ð°Ð»ÐµÐ¶Ð½Ð¾ÑÑ‚Ñ– Ð²Ñ–Ð´ Ñ€ÐµÐ³Ñ–Ð¾Ð½Ñƒ
"""
            cursor.execute('''
                INSERT INTO company_info (id, text) VALUES (1, %s)
            ''', (company_text,))
            logger.info("âœ… Ð”Ð¾Ð´Ð°Ð½Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– company_info")
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– Ð´Ð»Ñ welcome_message
        cursor.execute("SELECT COUNT(*) FROM welcome_message")
        welcome_count = cursor.fetchone()['count']
        
        if welcome_count == 0:
            welcome_text = """
<b>ðŸ‡ºðŸ‡¦ Ð’Ñ–Ñ‚Ð°Ñ”Ð¼Ð¾ Ñƒ Ð±Ð¾Ñ‚Ñ– ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–Ñ— Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚! ðŸŒ±</b>

ÐœÐ¸ ÑÐ¿ÐµÑ†Ñ–Ð°Ð»Ñ–Ð·ÑƒÑ”Ð¼Ð¾ÑÑ Ð½Ð° Ð²Ð¸Ñ€Ð¾Ñ‰ÑƒÐ²Ð°Ð½Ð½Ñ– Ð¾Ð²Ð¾Ñ‡Ñ–Ð² Ñ‚Ð° Ñ„Ñ€ÑƒÐºÑ‚Ñ–Ð² Ð½Ð° Ð¿Ð¾Ð»ÑÑ… ÐžÐ´ÐµÑ‰Ð¸Ð½Ð¸:

ðŸ¥« ÐÑ€Ñ‚Ð¸ÑˆÐ¾Ðº Ð¼Ð°Ñ€Ð¸Ð½Ð¾Ð²Ð°Ð½Ð¸Ð¹ Ð· Ð·ÐµÑ€Ð½Ð°Ð¼Ð¸ Ð³Ñ–Ñ€Ñ‡Ð¸Ñ†Ñ– - Ð¿Ñ–ÐºÐ°Ð½Ñ‚Ð½Ð¸Ð¹, Ð½Ðµ Ð³Ð¾ÑÑ‚Ñ€Ð¸Ð¹
ðŸŒ¶ï¸ ÐÑ€Ñ‚Ð¸ÑˆÐ¾Ðº Ð¼Ð°Ñ€Ð¸Ð½Ð¾Ð²Ð°Ð½Ð¸Ð¹ Ð· Ñ‡Ð¸Ð»Ñ– - Ð· Ð½Ð¾Ñ‚ÐºÐ°Ð¼Ð¸ Ð³Ð¾ÑÑ‚Ñ€Ð¾Ñ‚Ð¸
ðŸ¯ ÐŸÐ°ÑˆÑ‚ÐµÑ‚ Ð· Ð°Ñ€Ñ‚Ð¸ÑˆÐ¾ÐºÑƒ - Ð½Ñ–Ð¶Ð½Ð¸Ð¹ Ð´Ð»Ñ Ð±ÑƒÑ‚ÐµÑ€Ð±Ñ€Ð¾Ð´Ñ–Ð²

<b>ðŸ¢ ÐŸÑ€Ð¾ Ð½Ð°Ñ:</b>
â€¢ ÐŸÑ€Ð°Ñ†ÑŽÑ”Ð¼Ð¾ Ð· 2022 Ñ€Ð¾ÐºÑƒ
â€¢ Ð Ð¾Ð·Ñ‚Ð°ÑˆÑƒÐ²Ð°Ð½Ð½Ñ: ÐžÐ´ÐµÑÑŒÐºÐ° Ð¾Ð±Ð»Ð°ÑÑ‚ÑŒ, Ñ. Ð’ÐµÐ»Ð¸ÐºÐ¸Ð¹ Ð”Ð°Ð»ÑŒÐ½Ð¸Ðº
â€¢ Ð”Ð¾ÑÑ‚Ð°Ð²ÐºÐ° ÐÐ¾Ð²Ð¾ÑŽ ÐŸÐ¾ÑˆÑ‚Ð¾ÑŽ Ð¿Ð¾ Ð²ÑÑ–Ð¹ Ð£ÐºÑ€Ð°Ñ—Ð½Ñ–

<b>ÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¾Ð¿Ñ†Ñ–ÑŽ Ð· Ð¼ÐµÐ½ÑŽ ðŸ‘‡</b>
    """
            cursor.execute('''
                INSERT INTO welcome_message (id, text) VALUES (1, %s)
            ''', (welcome_text,))
            logger.info("âœ… Ð”Ð¾Ð´Ð°Ð½Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– welcome_message")
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– FAQ
        cursor.execute("SELECT COUNT(*) FROM faq")
        faq_count = cursor.fetchone()['count']
        
        if faq_count == 0:
            faqs = [
                ("Ð¯ÐºÑ– ÑÐ¿Ð¾ÑÐ¾Ð±Ð¸ Ð¾Ð¿Ð»Ð°Ñ‚Ð¸ Ð²Ð¸ Ð¿Ñ€Ð¸Ð¹Ð¼Ð°Ñ”Ñ‚Ðµ?", "âœ… Ð“Ð¾Ñ‚Ñ–Ð²ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ–\nâœ… ÐŸÐµÑ€ÐµÐºÐ°Ð· Ð½Ð° ÐºÐ°Ñ€Ñ‚Ñƒ ÐŸÑ€Ð¸Ð²Ð°Ñ‚Ð‘Ð°Ð½ÐºÑƒ\nâœ… ÐžÐ¿Ð»Ð°Ñ‚Ð° Ñ‡ÐµÑ€ÐµÐ· LiqPay", 0),
                ("Ð¯ÐºÑ– Ñ‚ÐµÑ€Ð¼Ñ–Ð½Ð¸ Ð´Ð¾ÑÑ‚Ð°Ð²ÐºÐ¸?", "ðŸšš ÐšÐ¸Ñ—Ð² - 1-2 Ð´Ð½Ñ–\nðŸšš Ð£ÐºÑ€Ð°Ñ—Ð½Ð° - 2-4 Ð´Ð½Ñ–\nðŸš› Ð’ÐµÐ»Ð¸ÐºÑ– Ð¿Ð°Ñ€Ñ‚Ñ–Ñ— - 3-5 Ð´Ð½Ñ–Ð²", 1)
            ]
            for question, answer, position in faqs:
                cursor.execute('''
                    INSERT INTO faq (question, answer, position) VALUES (%s, %s, %s)
                ''', (question, answer, position))
            logger.info("âœ… Ð”Ð¾Ð´Ð°Ð½Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– FAQ")
        
        conn.commit()
        logger.info("âœ… Ð‘Ð°Ð·Ð° Ð´Ð°Ð½Ð¸Ñ… ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð¾Ð²Ð°Ð½Ð°!")
        return True
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ— Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ…: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

# ========== Ð¤Ð£ÐÐšÐ¦Ð†Ð‡ Ð”Ð›Ð¯ Ð ÐžÐ‘ÐžÐ¢Ð˜ Ð— ÐšÐžÐœÐŸÐÐÐ†Ð„Ð® ==========

def get_company_info() -> str:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ñ‚ÐµÐºÑÑ‚ Ð¿Ñ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ Ð· Ð‘Ð”"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_company_info()")
    conn = get_db_connection()
    if not conn:
        return "ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT text FROM company_info WHERE id = 1')
        row = cursor.fetchone()
        return row['text'] if row else "Ð†Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–ÑŽ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ company_info: {e}")
        return "ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    finally:
        conn.close()

def update_company_info(text: str, updated_by: int) -> bool:
    """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” Ñ‚ÐµÐºÑÑ‚ Ð¿Ñ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ Ð² Ð‘Ð”"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº update_company_info(), updated_by: {updated_by}")
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
        logger.info(f"âœ… Company info Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ÐµÐ¼ {updated_by}")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ company_info: {e}")
        return False
    finally:
        conn.close()

# ========== Ð¤Ð£ÐÐšÐ¦Ð†Ð‡ Ð”Ð›Ð¯ Ð ÐžÐ‘ÐžÐ¢Ð˜ Ð— Ð’Ð†Ð¢ÐÐ›Ð¬ÐÐ˜Ðœ ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯Ðœ ==========

def get_welcome_message() -> str:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ð‘Ð”"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_welcome_message()")
    conn = get_db_connection()
    if not conn:
        return "ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT text FROM welcome_message WHERE id = 1')
        row = cursor.fetchone()
        return row['text'] if row else "ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ welcome_message: {e}")
        return "ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    finally:
        conn.close()

def update_welcome_message(text: str, updated_by: int) -> bool:
    """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ð‘Ð”"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº update_welcome_message(), updated_by: {updated_by}")
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
        logger.info(f"âœ… Welcome message Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ÐµÐ¼ {updated_by}")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ welcome_message: {e}")
        return False
    finally:
        conn.close()

# ========== Ð¤Ð£ÐÐšÐ¦Ð†Ð‡ Ð”Ð›Ð¯ Ð ÐžÐ‘ÐžÐ¢Ð˜ Ð— FAQ ==========

def get_all_faqs() -> List[Dict]:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ– FAQ Ð· Ð‘Ð”, Ð²Ñ–Ð´ÑÐ¾Ñ€Ñ‚Ð¾Ð²Ð°Ð½Ñ– Ð·Ð° Ð¿Ð¾Ð·Ð¸Ñ†Ñ–Ñ”ÑŽ"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_faqs()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer, position FROM faq ORDER BY position, id')
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        logger.debug(f"ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ {len(result)} FAQ")
        return result
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ faq: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_faq_by_id(faq_id: int) -> Optional[Dict]:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” FAQ Ð·Ð° ID Ð· ÑƒÑÑ–Ð¼Ð° Ð¿Ð¾Ð»ÑÐ¼Ð¸"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_faq_by_id() Ð· ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        logger.error("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð”")
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
            logger.debug(f"âœ… Ð—Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ FAQ #{faq_id}: {result['question'][:30]}...")
            return result
        logger.warning(f"âŒ FAQ Ð· ID {faq_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
        return None
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ faq Ð·Ð° ID {faq_id}: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def add_faq(question: str, answer: str) -> Optional[int]:
    """Ð”Ð¾Ð´Ð°Ñ” Ð½Ð¾Ð²Ð¸Ð¹ FAQ, Ð¿Ð¾Ð²ÐµÑ€Ñ‚Ð°Ñ” ID"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº add_faq(), Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ: {question[:30]}...")
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
        logger.info(f"âœ… FAQ Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð· ID: {faq_id}")
        return faq_id
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ faq: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def update_faq(faq_id: int, question: str, answer: str) -> bool:
    """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” Ñ–ÑÐ½ÑƒÑŽÑ‡Ð¸Ð¹ FAQ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº update_faq() Ð´Ð»Ñ ID: {faq_id}")
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
        logger.info(f"âœ… FAQ #{faq_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ faq #{faq_id}: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def delete_faq(faq_id: int) -> bool:
    """Ð’Ð¸Ð´Ð°Ð»ÑÑ” FAQ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº delete_faq() Ð´Ð»Ñ ID: {faq_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM faq WHERE id = %s', (faq_id,))
        conn.commit()
        logger.info(f"âœ… FAQ #{faq_id} Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ faq #{faq_id}: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def move_faq_up(faq_id: int) -> bool:
    """ÐŸÐµÑ€ÐµÐ¼Ñ–Ñ‰ÑƒÑ” FAQ Ð²Ð³Ð¾Ñ€Ñƒ (Ð·Ð¼ÐµÐ½ÑˆÑƒÑ” position)"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº move_faq_up() Ð´Ð»Ñ ID: {faq_id}")
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
            logger.info(f"âœ… FAQ #{faq_id} Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð¾ Ð²Ð³Ð¾Ñ€Ñƒ")
            return True
        return False
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð½Ñ faq Ð²Ð³Ð¾Ñ€Ñƒ: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def move_faq_down(faq_id: int) -> bool:
    """ÐŸÐµÑ€ÐµÐ¼Ñ–Ñ‰ÑƒÑ” FAQ Ð²Ð½Ð¸Ð· (Ð·Ð±Ñ–Ð»ÑŒÑˆÑƒÑ” position)"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº move_faq_down() Ð´Ð»Ñ ID: {faq_id}")
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
            logger.info(f"âœ… FAQ #{faq_id} Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð¾ Ð²Ð½Ð¸Ð·")
            return True
        return False
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð½Ñ faq Ð²Ð½Ð¸Ð·: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)
logger.info(f"âœ… Ð”Ð¸Ñ€ÐµÐºÑ‚Ð¾Ñ€Ñ–Ñ Ð´Ð»Ñ Ð·Ð²Ñ–Ñ‚Ñ–Ð²: {REPORTS_DIR}")

admin_sessions = {}
last_password_check = {}
orders_offset = {}
messages_offset = {}
broadcast_in_progress = {}

def is_authenticated(user_id: int) -> bool:
    """ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ” Ñ‡Ð¸ Ð°Ð²Ñ‚ÐµÐ½Ñ‚Ð¸Ñ„Ñ–ÐºÐ¾Ð²Ð°Ð½Ð¸Ð¹ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡"""
    result = user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"
    logger.debug(f"ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÐ° Ð°Ð²Ñ‚ÐµÐ½Ñ‚Ð¸Ñ„Ñ–ÐºÐ°Ñ†Ñ–Ñ— Ð´Ð»Ñ {user_id}: {result}")
    if not result and user_id in admin_sessions:
        logger.debug(f"Ð¡ÐµÑÑ–Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° {user_id}: {admin_sessions[user_id]}")
    return result

async def download_image_from_url_to_bytes(url: str) -> bytes:
    """Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑ” Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð° URL Ñ– Ð¿Ð¾Ð²ÐµÑ€Ñ‚Ð°Ñ” ÑÐº Ð±Ð°Ð¹Ñ‚Ð¸"""
    logger.info(f"ðŸŒ Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶Ð¸Ñ‚Ð¸ URL: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, timeout=30, allow_redirects=True, headers=headers)
        response.raise_for_status()
        
        logger.info(f"âœ… Ð—Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð¾, Ñ€Ð¾Ð·Ð¼Ñ–Ñ€: {len(response.content)} Ð±Ð°Ð¹Ñ‚")
        return response.content
        
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ: {e}")
        return None

async def download_telegram_file_to_bytes(file_id: str, bot: Bot) -> bytes:
    """Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑ” Ñ„Ð°Ð¹Ð» Ð· Telegram Ñ– Ð¿Ð¾Ð²ÐµÑ€Ñ‚Ð°Ñ” ÑÐº Ð±Ð°Ð¹Ñ‚Ð¸"""
    try:
        file = await bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        logger.info(f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð¾ Ð² Ð¿Ð°Ð¼'ÑÑ‚ÑŒ, Ñ€Ð¾Ð·Ð¼Ñ–Ñ€: {len(file_bytes)} Ð±Ð°Ð¹Ñ‚")
        return bytes(file_bytes)
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ñ„Ð°Ð¹Ð»Ñƒ: {e}")
        return None

async def reset_all_orders():
    """Ð¡ÐºÐ¸Ð´Ð°Ñ” Ð²ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.warning("âš ï¸ Ð’Ð¸ÐºÐ»Ð¸ÐºÐ°Ð½Ð¾ reset_all_orders()")
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
        logger.info("âœ… Ð’ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚Ð° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾!")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

async def notify_admins_about_new_order(order_data: dict):
    """Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð¿Ñ€Ð¾ Ð½Ð¾Ð²Ðµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº notify_admins_about_new_order() Ð· Ð´Ð°Ð½Ð¸Ð¼Ð¸: {order_data.get('order_id')}")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð” Ð´Ð»Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÐ¿Ð¸ÑÐºÑƒ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("ÐÐµÐ¼Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð´Ð»Ñ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ")
            return
        
        order_type = "âš¡ Ð¨Ð’Ð˜Ð”ÐšÐ•" if order_data.get('order_type') == 'quick' else "ðŸ“¦ Ð—Ð’Ð˜Ð§ÐÐ™ÐÐ•"
        order_id = order_data.get('order_id', order_data.get('id', 'Ð/Ð”'))
        
        message = f"ðŸ†• <b>ÐÐžÐ’Ð• {order_type} Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id}</b>\n\n"
        message += f"ðŸ‘¤ <b>ÐšÐ»Ñ–Ñ”Ð½Ñ‚:</b> {order_data.get('user_name', 'Ð/Ð”')}\n"
        message += f"ðŸ“ž <b>Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½:</b> {order_data.get('phone', 'Ð/Ð”')}\n"
        
        if order_data.get('order_type') == 'quick':
            message += f"ðŸ“¦ <b>ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚:</b> {order_data.get('product_name', 'Ð/Ð”')}\n"
            message += f"ðŸ’¬ <b>Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ:</b> {order_data.get('contact_method', 'Ð/Ð”')}\n"
            if order_data.get('message'):
                message += f"ðŸ“ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:</b> {order_data.get('message')}\n"
        else:
            message += f"ðŸ™ï¸ <b>ÐœÑ–ÑÑ‚Ð¾:</b> {order_data.get('city', 'Ð/Ð”')}\n"
            message += f"ðŸ£ <b>Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ ÐÐŸ:</b> {order_data.get('np_department', 'Ð/Ð”')}\n"
            message += f"ðŸ’° <b>Ð¡ÑƒÐ¼Ð°:</b> {order_data.get('total', 0):.2f} Ð³Ñ€Ð½\n"
            
            items_text = ""
            for item in order_data.get('items', []):
                items_text += f"  â€¢ {item.get('product_name')} x {item.get('quantity')} = {item.get('price_per_unit', 0) * item.get('quantity', 0):.2f} Ð³Ñ€Ð½\n"
            if items_text:
                message += f"ðŸ“¦ <b>Ð¢Ð¾Ð²Ð°Ñ€Ð¸:</b>\n{items_text}"
        
        message += f"\nðŸ•’ <b>Ð§Ð°Ñ:</b> {format_kyiv_time(order_data.get('created_at'))}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("ðŸ“‹ ÐšÐµÑ€ÑƒÐ²Ð°Ñ‚Ð¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼", callback_data=f"order_view_{order_id}_{order_data.get('order_type', 'regular')}")],
            [InlineKeyboardButton("ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñƒ", callback_data=f"reply_order_{order_id}_{order_data.get('order_type', 'regular')}")]
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² notify_admins_about_new_order: {e}")
        logger.error(traceback.format_exc())

async def notify_admins_about_message(message_data: dict):
    """Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð¿Ñ€Ð¾ Ð½Ð¾Ð²Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº notify_admins_about_message() Ð²Ñ–Ð´ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° {message_data.get('user_id')}")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð” Ð´Ð»Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÐ¿Ð¸ÑÐºÑƒ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("ÐÐµÐ¼Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð´Ð»Ñ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ")
            return
        
        message = f"ðŸ’¬ <b>ÐÐžÐ’Ð• ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯</b>\n\n"
        message += f"ðŸ‘¤ <b>ÐšÐ»Ñ–Ñ”Ð½Ñ‚:</b> {message_data.get('user_name', 'Ð/Ð”')}\n"
        message += f"ðŸ“± <b>Username:</b> @{message_data.get('username', 'Ð/Ð”')}\n"
        message += f"ðŸ†” <b>User ID:</b> {message_data.get('user_id', 'Ð/Ð”')}\n"
        message += f"ðŸ“ <b>Ð¢ÐµÐºÑÑ‚:</b> {message_data.get('text', 'Ð/Ð”')}\n"
        message += f"ðŸ•’ <b>Ð§Ð°Ñ:</b> {format_kyiv_time(message_data.get('created_at'))}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸", callback_data=f"reply_user_{message_data.get('user_id')}")],
            [InlineKeyboardButton("ðŸ‘¤ ÐŸÑ€Ð¾Ñ„Ñ–Ð»ÑŒ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", callback_data=f"customer_view_{message_data.get('user_id')}")]
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² notify_admins_about_message: {e}")
        logger.error(traceback.format_exc())

async def send_combined_quick_order_notification(order_id: int, user_id: int, user_name: str, username: str, product_name: str, message_text: str):
    """Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð¿Ñ€Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð· Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº send_combined_quick_order_notification() Ð´Ð»Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id}")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð” Ð´Ð»Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÐ¿Ð¸ÑÐºÑƒ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²")
            return
        
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admins")
        admins = cursor.fetchall()
        conn.close()
        
        if not admins:
            logger.warning("ÐÐµÐ¼Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð² Ð´Ð»Ñ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ")
            return
        
        message = f"ðŸ†• <b>ÐÐžÐ’Ð• âš¡ Ð¨Ð’Ð˜Ð”ÐšÐ• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id}</b>\n\n"
        message += f"ðŸ‘¤ <b>ÐšÐ»Ñ–Ñ”Ð½Ñ‚:</b> {user_name}\n"
        message += f"ðŸ“± <b>Username:</b> @{username}\n"
        message += f"ðŸ†” <b>User ID:</b> {user_id}\n"
        message += f"ðŸ“¦ <b>ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚:</b> {product_name}\n"
        message += f"ðŸ’¬ <b>Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ:</b> chat\n"
        message += f"ðŸ“ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:</b> {message_text}\n"
        message += f"ðŸ•’ <b>Ð§Ð°Ñ:</b> {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("ðŸ“‹ ÐšÐµÑ€ÑƒÐ²Ð°Ñ‚Ð¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼", callback_data=f"order_view_{order_id}_quick")],
            [InlineKeyboardButton("ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñƒ", callback_data=f"reply_order_{order_id}_quick")]
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"ÐžÐ±'Ñ”Ð´Ð½Ð°Ð½Ðµ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² send_combined_quick_order_notification: {e}")
        logger.error(traceback.format_exc())

def safe_get(order, key, default=0):
    """Ð‘ÐµÐ·Ð¿ÐµÑ‡Ð½Ðµ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð½Ñ Ð·Ñ– ÑÐ»Ð¾Ð²Ð½Ð¸ÐºÐ°"""
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
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_orders(include_quick={include_quick}, limit={limit}, offset={offset})")
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
                order['city'] = order.get('city', 'Ð/Ð”')
                order['np_department'] = order.get('np_department', 'Ð/Ð”')
                all_orders.append(order)
        
        all_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        logger.debug(f"ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ {len(all_orders)} Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ")
        return all_orders
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_recent_orders(hours: int = 1, min_count: int = 3):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_recent_orders(hours={hours}, min_count={min_count})")
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
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð½Ð°ÑÑ‚ÑƒÐ¿Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ Ð¿Ð°Ð³Ñ–Ð½Ð°Ñ†Ñ–Ñ—"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_more_orders(user_id={user_id}, count={count})")
    if user_id not in orders_offset:
        orders_offset[user_id] = 0
    
    offset = orders_offset[user_id]
    orders = get_all_orders(include_quick=True, limit=count, offset=offset)
    orders_offset[user_id] = offset + len(orders)
    
    return orders

def format_order_text(order: dict) -> str:
    """Ð¤Ð¾Ñ€Ð¼Ð°Ñ‚ÑƒÑ” Ñ‚ÐµÐºÑÑ‚ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ Ð²Ñ–Ð´Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ"""
    order_type = "âš¡" if order.get('order_type') == 'quick' else "ðŸ“¦"
    order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
    
    user_name = order.get('user_name', 'Ð/Ð”')
    phone = order.get('phone', 'Ð/Ð”')
    total = safe_get(order, 'total', 0)
    status = order.get('status', 'Ð½Ð¾Ð²Ðµ')
    created_at = order.get('created_at', '')
    
    text = f"{order_type} <b>â„–{order_id}</b> | {created_at[:16] if created_at else 'Ð/Ð”'}\n"
    text += f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {user_name}\n"
    text += f"ðŸ“ž Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {phone}\n"
    
    if order.get('order_type') == 'quick':
        product_name = order.get('product_name', 'Ð/Ð”')
        text += f"ðŸ“¦ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {product_name}\n"
        if order.get('message'):
            msg = order.get('message', '')
            text += f"ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {msg[:50]}{'...' if len(msg) > 50 else ''}\n"
        text += f"ðŸ’° Ð¡ÑƒÐ¼Ð°: {total:.2f} Ð³Ñ€Ð½\n"
    else:
        text += f"ðŸ’° Ð¡ÑƒÐ¼Ð°: {total:.2f} Ð³Ñ€Ð½\n"
    
    text += f"ðŸ“Š Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {status}\n"
    return text

def get_orders_by_phone(phone: str):
    """Ð¨ÑƒÐºÐ°Ñ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_orders_by_phone(phone={phone})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_new_orders():
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð½Ð¾Ð²Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_new_orders()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT *, 'regular' as order_type FROM orders 
            WHERE status = 'Ð½Ð¾Ð²Ðµ'
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð½Ð¾Ð²Ð¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_quick_orders():
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” ÑˆÐ²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_quick_orders()")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def update_order_status(order_id: int, status: str, order_type: str = 'regular'):
    """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” ÑÑ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº update_order_status(order_id={order_id}, status={status}, order_type={order_type})")
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
        logger.info(f"âœ… Ð¡Ñ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾ Ð½Ð° '{status}'")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_order_by_id(order_id: int, order_type: str = 'regular'):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð° ID"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_order_by_id(order_id={order_id}, order_type={order_type})")
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        if order_type == 'regular' or order_type == 'orders':
            cursor.execute('SELECT * FROM orders WHERE order_id = %s', (order_id,))
            order_row = cursor.fetchone()
            if not order_row:
                logger.warning(f"Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
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
                logger.warning(f"Ð¨Ð²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
                return None
            
            order = dict(order_row)
            order['created_at'] = format_kyiv_time(order.get('created_at'))
            order['order_id'] = order['id']
            order['order_type'] = 'quick'
            order['items'] = []
            order['total'] = safe_get(order, 'total', 0)
        
        return order
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

async def notify_customer_about_status(user_id: int, order_id: int, status: str):
    """Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰Ð°Ñ” ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð¿Ñ€Ð¾ Ð·Ð¼Ñ–Ð½Ñƒ ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº notify_customer_about_status(user_id={user_id}, order_id={order_id}, status={status})")
    try:
        status_messages = {
            "Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð¾": "âœ… Ð’Ð°ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð¾! ÐœÐ¸ Ñ€Ð¾Ð·Ð¿Ð¾Ñ‡Ð°Ð»Ð¸ Ð¹Ð¾Ð³Ð¾ Ð¾Ð±Ñ€Ð¾Ð±ÐºÑƒ.",
            "ÑƒÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾": "ðŸ“¦ Ð’Ð°ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ ÑƒÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾ Ñ‚Ð° Ð³Ð¾Ñ‚Ð¾Ð²Ðµ Ð´Ð¾ Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸!",
            "Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾": "ðŸšš Ð’Ð°ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾! ÐžÑ‡Ñ–ÐºÑƒÐ¹Ñ‚Ðµ Ð½Ð° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ Ð¿Ñ€Ð¸Ð±ÑƒÑ‚Ñ‚Ñ.",
            "Ð¿Ñ€Ð¸Ð±ÑƒÐ»Ð¾": "ðŸ“ Ð’Ð°ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¸Ð±ÑƒÐ»Ð¾ Ñƒ Ð²Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ ÐÐ¾Ð²Ð¾Ñ— ÐŸÐ¾ÑˆÑ‚Ð¸! ÐÐµ Ð·Ð°Ð±ÑƒÐ´ÑŒÑ‚Ðµ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ñ‚Ð¸ Ð¹Ð¾Ð³Ð¾.",
            "ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾": "âŒ ÐÐ° Ð¶Ð°Ð»ÑŒ, Ð²Ð°ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð±ÑƒÐ»Ð¾ ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾. Ð—Ð²'ÑÐ¶Ñ–Ñ‚ÑŒÑÑ Ð· Ð½Ð°Ð¼Ð¸ Ð´Ð»Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹."
        }
        
        message = status_messages.get(status, f"ðŸ“Š Ð¡Ñ‚Ð°Ñ‚ÑƒÑ Ð²Ð°ÑˆÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð¼Ñ–Ð½ÐµÐ½Ð¾ Ð½Ð°: {status}")
        
        main_bot = Bot(token=MAIN_BOT_TOKEN)
        
        await main_bot.send_message(
            chat_id=user_id,
            text=f"<b>Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id}</b>\n\n{message}",
            parse_mode='HTML'
        )
        logger.info(f"âœ… Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ ÑÑ‚Ð°Ñ‚ÑƒÑ #{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñƒ {user_id}")
        return True
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñƒ {user_id}: {e}")
        return False

def get_all_messages(limit: int = 50, offset: int = 0):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_messages(limit={limit}, offset={offset})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_message_by_id(message_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð° ID"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_message_by_id(message_id={message_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_recent_messages(hours: int = 24, min_count: int = 5):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_recent_messages(hours={hours}, min_count={min_count})")
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
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð½Ð°ÑÑ‚ÑƒÐ¿Ð½Ñ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ Ð¿Ð°Ð³Ñ–Ð½Ð°Ñ†Ñ–Ñ—"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_more_messages(user_id={user_id}, count={count})")
    if user_id not in messages_offset:
        messages_offset[user_id] = 0
    
    offset = messages_offset[user_id]
    messages = get_all_messages(limit=count, offset=offset)
    messages_offset[user_id] = offset + len(messages)
    
    return messages

def format_message_text(msg: dict) -> str:
    """Ð¤Ð¾Ñ€Ð¼Ð°Ñ‚ÑƒÑ” Ñ‚ÐµÐºÑÑ‚ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ Ð²Ñ–Ð´Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ"""
    text = f"ðŸ’¬ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ #{msg['id']}</b>\n\n"
    text += f"ðŸ‘¤ <b>ÐšÐ»Ñ–Ñ”Ð½Ñ‚:</b> {msg['user_name']}\n"
    text += f"ðŸ“± <b>Username:</b> @{msg['username']}\n"
    text += f"ðŸ†” <b>ID:</b> {msg['user_id']}\n"
    text += f"ðŸ“… <b>Ð§Ð°Ñ:</b> {msg['created_at'][:16]}\n"
    text += f"ðŸ“ <b>Ð¢Ð¸Ð¿:</b> {msg['message_type']}\n"
    text += f"ðŸ’¬ <b>Ð¢ÐµÐºÑÑ‚:</b> {msg['text']}\n"
    return text

def get_messages_by_user(user_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_messages_by_user(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def format_messages_text(messages: list) -> str:
    """Ð¤Ð¾Ñ€Ð¼Ð°Ñ‚ÑƒÑ” ÑÐ¿Ð¸ÑÐ¾Ðº Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð´Ð»Ñ Ð²Ñ–Ð´Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ"""
    if not messages:
        return "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð¿Ð¾ÐºÐ¸ Ð½ÐµÐ¼Ð°Ñ”"
    
    text = "ðŸ’¬ <b>ÐžÐ¡Ð¢ÐÐÐÐ† ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯</b>\n\n"
    for i, msg in enumerate(messages[:20], 1):
        text += f"<b>{i}. {msg['user_name']}</b> (@{msg['username']})\n"
        text += f"ðŸ“… {msg['created_at'][:16]}\n"
        text += f"ðŸ“ {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
        text += f"ðŸ†” ID: {msg['user_id']}\n"
        text += f"ðŸ“‹ Ð¢Ð¸Ð¿: {msg['message_type']}\n"
        text += f"{'â”€'*40}\n"
    
    if len(messages) > 20:
        text += f"... Ñ‚Ð° Ñ‰Ðµ {len(messages) - 20} Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ"
    
    return text

def generate_messages_file(messages: list) -> bytes:
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ñ„Ð°Ð¹Ð» Ð· Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼Ð¸"""
    output = StringIO()
    output.write("ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯ Ð’Ð†Ð” ÐšÐžÐ Ð˜Ð¡Ð¢Ð£Ð’ÐÐ§Ð†Ð’\n")
    output.write("=" * 80 + "\n")
    output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"Ð’ÑÑŒÐ¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {len(messages)}\n")
    output.write("=" * 80 + "\n\n")
    
    for i, msg in enumerate(messages, 1):
        output.write(f"{i}. {msg['user_name']} (@{msg['username']})\n")
        output.write(f"ID: {msg['user_id']}\n")
        output.write(f"Ð”Ð°Ñ‚Ð°: {msg['created_at']}\n")
        output.write(f"Ð¢Ð¸Ð¿: {msg['message_type']}\n")
        output.write(f"Ð¢ÐµÐºÑÑ‚: {msg['text']}\n")
        output.write("-" * 40 + "\n")
    
    return output.getvalue().encode('utf-8')

def get_all_users():
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ–Ñ… ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_users()")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_by_phone(phone: str):
    """Ð¨ÑƒÐºÐ°Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_by_phone(phone={phone})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_user_by_id(user_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° Ð·Ð° ID"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_by_id(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def get_user_orders(user_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_orders(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_phones(user_id: int) -> list:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_phones(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñ–Ð² ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_messages(user_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_messages(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_user_quick_orders(user_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” ÑˆÐ²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_user_quick_orders(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_customer_segment(user_data: dict, orders: list) -> str:
    """Ð’Ð¸Ð·Ð½Ð°Ñ‡Ð°Ñ” ÑÐµÐ³Ð¼ÐµÐ½Ñ‚ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°"""
    if not orders:
        return "ðŸ†• ÐÐ¾Ð²Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚ (Ð±ÐµÐ· Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ)"
    
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
        return "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚"
    elif total_orders >= 3:
        return "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚"
    elif days_since_last > 90:
        return "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚"
    elif total_orders == 1:
        return "ðŸ†• ÐÐ¾Ð²Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚ (1 Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ)"
    else:
        return "ðŸ“Š ÐÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚"

def get_all_products():
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ– Ñ‚Ð¾Ð²Ð°Ñ€Ð¸"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_products()")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, name, price, category, description, unit, image, details, created_at FROM products ORDER BY id')
        rows = cursor.fetchall()
        
        products = []
        for row in rows:
            product = dict(row)
            if product.get('created_at'):
                product['created_at'] = format_kyiv_time(product.get('created_at'))
            products.append(product)
        logger.debug(f"ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ {len(products)} Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²")
        return products
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_product_by_id(product_id: int):
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ñ‚Ð¾Ð²Ð°Ñ€ Ð·Ð° ID"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº get_product_by_id() Ð· ID: {product_id}")
    products = get_all_products()
    for product in products:
        if product["id"] == product_id:
            logger.debug(f"âœ… Ð—Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€: {product['name']}")
            return product
    logger.warning(f"âŒ Ð¢Ð¾Ð²Ð°Ñ€ Ð· ID {product_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
    return None

def update_product(product_id: int, **kwargs):
    """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” Ñ‚Ð¾Ð²Ð°Ñ€"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº update_product() Ð´Ð»Ñ ID: {product_id} Ð· Ð´Ð°Ð½Ð¸Ð¼Ð¸: {kwargs.keys()}")
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
            logger.warning(f"Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð¾Ð½Ð¾Ð²Ð¸Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€ #{product_id} Ð±ÐµÐ· Ð´Ð°Ð½Ð¸Ñ…")
            return False
        
        values.append(product_id)
        query = f"UPDATE products SET {', '.join(fields)} WHERE id = %s"
        cursor.execute(query, values)
        conn.commit()
        logger.info(f"âœ… Ð¢Ð¾Ð²Ð°Ñ€ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def add_product(name: str, price: float, category: str, description: str, unit: str, details: str):
    """Ð”Ð¾Ð´Ð°Ñ” Ð½Ð¾Ð²Ð¸Ð¹ Ñ‚Ð¾Ð²Ð°Ñ€"""
    logger.info(f"ðŸ“¦ Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð´Ð¾Ð´Ð°Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€: {name}, Ñ†Ñ–Ð½Ð°: {price}, ÐºÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–Ñ: {category}")
    
    conn = get_db_connection()
    if not conn:
        logger.error("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð”")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (name, price, category, description, unit, details)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, price, category, description, unit, details))
        
        result = cursor.fetchone()
        product_id = result['id'] if result else None
        conn.commit()
        
        logger.info(f"âœ… Ð¢Ð¾Ð²Ð°Ñ€ Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð· ID: {product_id}")
        return product_id
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        conn.close()

def delete_product(product_id: int):
    """Ð’Ð¸Ð´Ð°Ð»ÑÑ” Ñ‚Ð¾Ð²Ð°Ñ€"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº delete_product() Ð´Ð»Ñ ID: {product_id}")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM products WHERE id = %s', (product_id,))
        conn.commit()
        logger.info(f"âœ… Ð¢Ð¾Ð²Ð°Ñ€ #{product_id} Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def get_all_admins():
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ–Ñ… Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_all_admins()")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def add_admin(user_id: int, username: str = "", added_by: int = 0):
    """Ð”Ð¾Ð´Ð°Ñ” Ð°Ð´Ð¼Ñ–Ð½Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº add_admin(user_id={user_id}, username={username}, added_by={added_by})")
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
        logger.info(f"âœ… ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð´Ð¾Ð´Ð°Ð½Ð¸Ð¹")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ð°: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def remove_admin(user_id: int):
    """Ð’Ð¸Ð´Ð°Ð»ÑÑ” Ð°Ð´Ð¼Ñ–Ð½Ð°"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº remove_admin(user_id={user_id})")
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM admins WHERE user_id = %s', (user_id,))
        conn.commit()
        logger.info(f"âœ… ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¸Ð¹")
        return True
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ð°: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

def is_admin(user_id: int) -> bool:
    """ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ” Ñ‡Ð¸ Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð¾Ð¼ Ð² Ð‘Ð”"""
    logger.debug(f"Ð’Ð¸ÐºÐ»Ð¸Ðº is_admin(user_id={user_id})")
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÐ¸ Ð°Ð´Ð¼Ñ–Ð½Ð°: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð½Ð¸Ðº ÐºÐ¾Ð¼Ð°Ð½Ð´Ð¸ /start"""
    user = update.effective_user
    user_id = user.id
    
    logger.info(f"ðŸ‘¤ ÐÐ´Ð¼Ñ–Ð½ {user_id} (@{user.username}) Ð²Ð¸ÐºÐ»Ð¸ÐºÐ°Ð² /start")
    
    # Ð¡Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÑƒ Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡ÐµÑ€ÐµÐ· ADMIN_IDS (Ð¿ÐµÑ€ÑˆÑ– Ð°Ð´Ð¼Ñ–Ð½Ð¸)
    if ADMIN_IDS and user_id in ADMIN_IDS:
        logger.info(f"âœ… ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¸Ð¹ Ð² ADMIN_IDS")
        admin_sessions[user_id] = {"state": "waiting_password"}
        await update.message.reply_text("ðŸ” Ð’Ñ…Ñ–Ð´ Ð² Ð°Ð´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»ÑŒ Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚\n\nÐ‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, Ð²Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ:")
        return
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡ÐµÑ€ÐµÐ· Ð±Ð°Ð·Ñƒ Ð´Ð°Ð½Ð¸Ñ…
    if is_admin(user_id):
        logger.info(f"âœ… ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¸Ð¹ Ð² Ð‘Ð”")
        admin_sessions[user_id] = {"state": "waiting_password"}
        await update.message.reply_text("ðŸ” Ð’Ñ…Ñ–Ð´ Ð² Ð°Ð´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»ÑŒ Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚\n\nÐ‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, Ð²Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ:")
        return
    
    # Ð¯ÐºÑ‰Ð¾ Ð½Ðµ Ð¿Ñ€Ð¾Ð¹ÑˆÐ¾Ð² Ð¶Ð¾Ð´Ð½Ñƒ Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÑƒ - Ð´Ð¾ÑÑ‚ÑƒÐ¿ Ð·Ð°Ð±Ð¾Ñ€Ð¾Ð½ÐµÐ½Ð¾
    logger.warning(f"âŒ Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð´Ð¾ÑÑ‚ÑƒÐ¿Ñƒ Ð½ÐµÐ°Ð²Ñ‚Ð¾Ñ€Ð¸Ð·Ð¾Ð²Ð°Ð½Ð¾Ð³Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° {user_id}")
    await update.message.reply_text("âŒ Ð”Ð¾ÑÑ‚ÑƒÐ¿ Ð·Ð°Ð±Ð¾Ñ€Ð¾Ð½ÐµÐ½Ð¾\n\nÐ’Ð¸ Ð½Ðµ Ð¼Ð°Ñ”Ñ‚Ðµ Ð¿Ñ€Ð°Ð² Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°.")

async def check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ” Ð¿Ð°Ñ€Ð¾Ð»ÑŒ Ð¿Ñ€Ð¸ Ð²Ñ…Ð¾Ð´Ñ–"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    logger.info(f"ðŸ” Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð²Ñ…Ð¾Ð´Ñƒ Ð°Ð´Ð¼Ñ–Ð½Ð° {user_id}")
    
    if user_id not in admin_sessions or admin_sessions[user_id].get("state") != "waiting_password":
        logger.warning(f"Ð¡Ð¿Ñ€Ð¾Ð±Ð° Ð²Ð²ÐµÑÑ‚Ð¸ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ Ð±ÐµÐ· ÑÐµÑÑ–Ñ— Ð²Ñ–Ð´ {user_id}")
        return
    
    if text == ADMIN_PASSWORD:
        admin_sessions[user_id] = {"state": "authenticated", "authenticated_at": get_kyiv_time().isoformat()}
        last_password_check[user_id] = get_kyiv_time()
        
        logger.info(f"âœ… ÐÐ´Ð¼Ñ–Ð½ {user_id} ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð°Ð²Ñ‚ÐµÐ½Ñ‚Ð¸Ñ„Ñ–ÐºÐ¾Ð²Ð°Ð½Ð¸Ð¹, ÑÐµÑÑ–Ñ: {admin_sessions[user_id]}")
        
        if not is_admin(user_id):
            add_admin(user_id, user.username or "", user_id)
            logger.info(f"âœ… ÐÐ¾Ð²Ð¾Ð³Ð¾ Ð°Ð´Ð¼Ñ–Ð½Ð° {user_id} Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð´Ð¾ Ð‘Ð”")
        
        await update.message.reply_text("âœ… ÐŸÐ°Ñ€Ð¾Ð»ÑŒ Ð¿Ñ€Ð¸Ð¹Ð½ÑÑ‚Ð¾!\n\nÐ›Ð°ÑÐºÐ°Ð²Ð¾ Ð¿Ñ€Ð¾ÑˆÑƒ Ð´Ð¾ Ð°Ð´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»Ñ–.", reply_markup=get_main_menu())
    else:
        await update.message.reply_text("âŒ ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ!\n\nÐ¡Ð¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð· Ð°Ð±Ð¾ Ð½Ð°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ /start")
        logger.warning(f"âŒ ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ Ð²Ñ–Ð´ Ð°Ð´Ð¼Ñ–Ð½Ð° {user_id}")
        admin_sessions.pop(user_id, None)

def generate_orders_report(orders: list, format: str = "txt"):
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…"""
    logger.debug(f"Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñƒ Ð¿Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…, Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚: {format}")
    if format == "txt":
        output = StringIO()
        output.write("Ð—Ð’Ð†Ð¢ ÐŸÐž Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯Ð¥\n")
        output.write("=" * 80 + "\n")
        output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Ð’ÑÑŒÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(orders)}\n")
        output.write("=" * 80 + "\n\n")
        
        for order in orders:
            order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
            output.write(f"ÐÐ¾Ð¼ÐµÑ€: {order_id}\n")
            output.write(f"Ð”Ð°Ñ‚Ð°: {order['created_at']}\n")
            output.write(f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order.get('user_name', 'Ð/Ð”')}\n")
            output.write(f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order.get('phone', 'Ð/Ð”')}\n")
            output.write(f"Username: @{order.get('username', 'Ð/Ð”')}\n")
            output.write(f"Ð¡ÑƒÐ¼Ð°: {order.get('total', 0):.2f} Ð³Ñ€Ð½\n")
            output.write(f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order.get('status', 'Ð½Ð¾Ð²Ðµ')}\n")
            output.write(f"Ð¢Ð¸Ð¿: {order.get('order_type', 'regular')}\n")
            if order.get('order_type') == 'quick' and order.get('message'):
                output.write(f"ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {order.get('message')}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ÐÐ¾Ð¼ÐµÑ€', 'Ð”Ð°Ñ‚Ð°', 'ÐšÐ»Ñ–Ñ”Ð½Ñ‚', 'Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½', 'Username', 'Ð¡ÑƒÐ¼Ð°', 'Ð¡Ñ‚Ð°Ñ‚ÑƒÑ', 'Ð¢Ð¸Ð¿', 'ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ'])
        
        for order in orders:
            order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
            writer.writerow([
                order_id,
                order['created_at'],
                order.get('user_name', 'Ð/Ð”'),
                order.get('phone', 'Ð/Ð”'),
                order.get('username', 'Ð/Ð”'),
                f"{order.get('total', 0):.2f}",
                order.get('status', 'Ð½Ð¾Ð²Ðµ'),
                order.get('order_type', 'regular'),
                order.get('message', '')
            ])
        
        return output.getvalue().encode('utf-8-sig')

def generate_users_report(users: list) -> bytes:
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ñ…"""
    logger.debug(f"Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñƒ Ð¿Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ñ…, ÐºÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ: {len(users)}")
    output = StringIO()
    output.write("Ð—Ð’Ð†Ð¢ ÐŸÐž ÐšÐžÐ Ð˜Ð¡Ð¢Ð£Ð’ÐÐ§ÐÐ¥\n")
    output.write("=" * 100 + "\n")
    output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"Ð’ÑÑŒÐ¾Ð³Ð¾ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²: {len(users)}\n")
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
        output.write(f"Ð†Ð¼'Ñ: {user['first_name']} {user['last_name']}\n")
        output.write(f"Username: @{user['username']}\n")
        output.write(f"Ð”Ð°Ñ‚Ð° Ñ€ÐµÑ”ÑÑ‚Ñ€Ð°Ñ†Ñ–Ñ—: {user['created_at'][:16]}\n")
        output.write(f"Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚: {segment}\n\n")
        
        if phones:
            output.write("ðŸ“ž Ð¢Ð•Ð›Ð•Ð¤ÐžÐÐ˜:\n")
            for i, phone in enumerate(phones, 1):
                output.write(f"  {i}. {phone}\n")
            output.write("\n")
        
        output.write("ðŸ“¦ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯:\n")
        output.write(f"  Ð’ÑÑŒÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n")
        
        if all_orders:
            total_spent = sum(o.get('total', 0) for o in orders)
            output.write(f"  Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°: {total_spent:.2f} Ð³Ñ€Ð½\n")
            if orders:
                output.write(f"  Ð¡ÐµÑ€ÐµÐ´Ð½Ñ–Ð¹ Ñ‡ÐµÐº: {total_spent/len(orders):.2f} Ð³Ñ€Ð½\n")
            output.write("\n")
            
            output.write("  ÐžÑÑ‚Ð°Ð½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:\n")
            for i, order in enumerate(all_orders[:3], 1):
                order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
                order_type = "âš¡" if order.get('order_type') == 'quick' else "ðŸ“¦"
                created_at = order.get('created_at', '')[:16]
                status = order.get('status', 'Ð½Ð¾Ð²Ðµ')
                total = order.get('total', 0)
                phone = order.get('phone', '')
                output.write(f"    {i}. {order_type} â„–{order_id} | {created_at} | {total:.2f} Ð³Ñ€Ð½ | {status}\n")
                if phone:
                    output.write(f"       Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {phone}\n")
                if order.get('order_type') == 'quick' and order.get('message'):
                    output.write(f"       ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {order.get('message')[:100]}\n")
                elif order.get('order_type') == 'regular' and order.get('items'):
                    output.write(f"       Ð¢Ð¾Ð²Ð°Ñ€Ð¸:\n")
                    for item in order.get('items', [])[:2]:
                        output.write(f"         â€¢ {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} Ð³Ñ€Ð½\n")
                    if len(order.get('items', [])) > 2:
                        output.write(f"         ... Ñ‚Ð° Ñ‰Ðµ {len(order.get('items', [])) - 2} Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²\n")
        else:
            output.write("  Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½ÐµÐ¼Ð°Ñ”\n")
        
        if messages:
            output.write(f"\nðŸ’¬ ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯: {len(messages)}\n")
            output.write("  ÐžÑÑ‚Ð°Ð½Ð½Ñ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:\n")
            for i, msg in enumerate(messages[:3], 1):
                created_at = msg.get('created_at', '')[:16]
                text = msg.get('text', '')
                output.write(f"    {i}. {created_at}: {text[:100]}{'...' if len(text) > 100 else ''}\n")
        
        output.write("-" * 100 + "\n\n")
    
    return output.getvalue().encode('utf-8')

def generate_quick_orders_report(orders: list, format: str = "txt"):
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…"""
    logger.debug(f"Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñƒ Ð¿Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…, Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚: {format}")
    if format == "txt":
        output = StringIO()
        output.write("Ð—Ð’Ð†Ð¢ ÐŸÐž Ð¨Ð’Ð˜Ð”ÐšÐ˜Ð¥ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯Ð¥\n")
        output.write("=" * 80 + "\n")
        output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Ð’ÑÑŒÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(orders)}\n")
        output.write("=" * 80 + "\n\n")
        
        for order in orders:
            output.write(f"ÐÐ¾Ð¼ÐµÑ€: {order['id']}\n")
            output.write(f"Ð”Ð°Ñ‚Ð°: {order['created_at']}\n")
            output.write(f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order['user_name']}\n")
            output.write(f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order['phone']}\n")
            output.write(f"Username: @{order['username']}\n")
            output.write(f"ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {order['product_name']}\n")
            output.write(f"Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ: {order['contact_method']}\n")
            if order.get('message'):
                output.write(f"ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {order['message']}\n")
            output.write(f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order['status']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ÐÐ¾Ð¼ÐµÑ€', 'Ð”Ð°Ñ‚Ð°', 'ÐšÐ»Ñ–Ñ”Ð½Ñ‚', 'Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½', 'Username', 'ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚', 'Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²`ÑÐ·ÐºÑƒ', 'ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ', 'Ð¡Ñ‚Ð°Ñ‚ÑƒÑ'])
        
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
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸Ñ†Ñ–"""
    logger.debug(f"Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñƒ Ð¿Ð¾ ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸Ñ†Ñ–, Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚: {format}")
    if format == "txt":
        output = StringIO()
        output.write("Ð¡Ð¢ÐÐ¢Ð˜Ð¡Ð¢Ð˜ÐšÐ\n")
        output.write("=" * 80 + "\n")
        output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write("=" * 80 + "\n\n")
        
        output.write(f"ðŸ“‹ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('total_orders', 0)}\n")
        output.write(f"ðŸ’° Ð’Ð¸Ñ€ÑƒÑ‡ÐºÐ°: {stats.get('total_revenue', 0):.2f} Ð³Ñ€Ð½\n")
        output.write(f"ðŸ’³ Ð¡ÐµÑ€ÐµÐ´Ð½Ñ–Ð¹ Ñ‡ÐµÐº: {stats.get('avg_check', 0):.2f} Ð³Ñ€Ð½\n")
        output.write(f"ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²: {stats.get('total_users', 0)}\n")
        output.write(f"âš¡ Ð¨Ð²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('total_quick_orders', 0)}\n")
        output.write(f"ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {stats.get('total_messages', 0)}\n\n")
        
        output.write("ðŸ“Š Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð° Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– 30 Ð´Ð½Ñ–Ð²:\n")
        output.write(f"   ÐšÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ: {stats.get('last_30_days_orders', 0)}\n")
        output.write(f"   Ð¡ÑƒÐ¼Ð°: {stats.get('last_30_days_revenue', 0):.2f} Ð³Ñ€Ð½\n\n")
        
        output.write("ðŸ“Š Ð¡Ñ‚Ð°Ñ‚ÑƒÑÐ¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ:\n")
        for status, count in stats.get('orders_by_status', {}).items():
            output.write(f"   â€¢ {status}: {count}\n")
        
        output.write("\nðŸ‘¥ Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚Ð°Ñ†Ñ–Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²:\n")
        segments = stats.get('segments', {})
        output.write(f"   ðŸ‘‘ VIP: {segments.get('vip', 0)}\n")
        output.write(f"   â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ñ–: {segments.get('regular', 0)}\n")
        output.write(f"   ðŸ†• ÐÐ¾Ð²Ñ–: {segments.get('new', 0)}\n")
        output.write(f"   ðŸ“Š ÐÐºÑ‚Ð¸Ð²Ð½Ñ–: {segments.get('active', 0)}\n")
        output.write(f"   ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ñ–: {segments.get('inactive', 0)}\n")
        
        return output.getvalue().encode('utf-8')

def generate_messages_report(messages: list, format: str = "txt"):
    """Ð“ÐµÐ½ÐµÑ€ÑƒÑ” Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÑ…"""
    logger.debug(f"Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñƒ Ð¿Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÑ…, Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚: {format}")
    if format == "txt":
        output = StringIO()
        output.write("Ð—Ð’Ð†Ð¢ ÐŸÐž ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯Ð¥\n")
        output.write("=" * 80 + "\n")
        output.write(f"Ð”Ð°Ñ‚Ð°: {get_kyiv_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Ð’ÑÑŒÐ¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {len(messages)}\n")
        output.write("=" * 80 + "\n\n")
        
        for msg in messages:
            output.write(f"ID: {msg['id']}\n")
            output.write(f"User ID: {msg['user_id']}\n")
            output.write(f"Ð†Ð¼'Ñ: {msg['user_name']}\n")
            output.write(f"Username: @{msg['username']}\n")
            output.write(f"Ð”Ð°Ñ‚Ð°: {msg['created_at']}\n")
            output.write(f"Ð¢Ð¸Ð¿: {msg['message_type']}\n")
            output.write(f"Ð¢ÐµÐºÑÑ‚: {msg['text']}\n")
            output.write("-" * 40 + "\n")
        
        return output.getvalue().encode('utf-8')
    
    elif format == "csv":
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ', 'User ID', 'Ð†Ð¼Ñ', 'Username', 'Ð”Ð°Ñ‚Ð°', 'Ð¢Ð¸Ð¿', 'Ð¢ÐµÐºÑÑ‚'])
        
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
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÑƒ"""
    logger.debug("Ð’Ð¸ÐºÐ»Ð¸Ðº get_statistics()")
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
            elif "ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¹" in segment:
                segments["regular"] += 1
            elif "ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹" in segment:
                segments["inactive"] += 1
            elif "ÐÐ¾Ð²Ð¸Ð¹" in segment:
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
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ¸: {e}")
        logger.error(traceback.format_exc())
        return {}
    finally:
        conn.close()

def create_inline_keyboard(buttons: List[List[Dict]]) -> InlineKeyboardMarkup:
    """Ð¡Ñ‚Ð²Ð¾Ñ€ÑŽÑ” Inline ÐºÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ñƒ"""
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
    """Ð“Ð¾Ð»Ð¾Ð²Ð½Ðµ Ð¼ÐµÐ½ÑŽ Ð°Ð´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»Ñ–"""
    keyboard = [
        [{"text": "ðŸ“¦ Ð¢Ð¾Ð²Ð°Ñ€Ð¸", "callback_data": "admin_products"}],
        [{"text": "ðŸ“‹ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_orders"}],
        [{"text": "ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers"}],
        [{"text": "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_messages"}],
        [{"text": "ðŸ“Š Ð¡Ñ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ°", "callback_data": "admin_stats"}],
        [{"text": "ðŸ“ Ð—Ð²Ñ–Ñ‚Ð¸", "callback_data": "admin_reports"}],
        [{"text": "ðŸ“¢ Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸", "callback_data": "admin_broadcast"}],
        [{"text": "ðŸ‘‘ ÐÐ´Ð¼Ñ–Ð½Ð¸", "callback_data": "admin_manage_admins"}],
        [{"text": "ðŸ”„ Ð¡ÐºÐ¸Ð½ÑƒÑ‚Ð¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_reset_orders"}],
        [{"text": "âš™ï¸ ÐÐ°Ð»Ð°ÑˆÑ‚ÑƒÐ²Ð°Ð½Ð½Ñ", "callback_data": "admin_settings"}],
        [{"text": "ðŸ¢ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ 'ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ'", "callback_data": "admin_edit_company"}],
        [{"text": "ðŸ‘‹ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ð²Ñ–Ñ‚Ð°Ð½Ð½Ñ", "callback_data": "admin_edit_welcome"}],
        [{"text": "â“ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ FAQ", "callback_data": "admin_faq_edit"}],
        [{"text": "ðŸ” Ð’Ð¸Ð¹Ñ‚Ð¸", "callback_data": "admin_logout"}]
    ]
    return create_inline_keyboard(keyboard)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð· ÐºÐ½Ð¾Ð¿ÐºÐ¾ÑŽ ÐÐ°Ð·Ð°Ð´"""
    buttons = [[{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": f"back_to_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu():
    """ÐœÐµÐ½ÑŽ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ð°Ð¼Ð¸"""
    keyboard = [
        [{"text": "ðŸ“‹ Ð¡Ð¿Ð¸ÑÐ¾Ðº Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²", "callback_data": "admin_product_list"}],
        [{"text": "âž• Ð”Ð¾Ð´Ð°Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€", "callback_data": "admin_product_add"}],
        [{"text": "âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€", "callback_data": "admin_product_edit"}],
        [{"text": "ðŸ—‘ Ð’Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€", "callback_data": "admin_product_delete"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_menu():
    """ÐœÐµÐ½ÑŽ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼Ð¸"""
    keyboard = [
        [{"text": "ðŸ“‹ ÐžÑÑ‚Ð°Ð½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_order_recent"}],
        [{"text": "ðŸ“‹ Ð’ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_order_all"}],
        [{"text": "ðŸ†• ÐÐ¾Ð²Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_order_new"}],
        [{"text": "âš¡ Ð¨Ð²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_order_quick"}],
        [{"text": "ðŸ“ž ÐŸÐ¾ÑˆÑƒÐº Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼", "callback_data": "admin_order_by_phone"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customers_menu():
    """ÐœÐµÐ½ÑŽ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼Ð¸"""
    keyboard = [
        [{"text": "ðŸ“‹ Ð’ÑÑ– ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers_all"}],
        [{"text": "ðŸ” ÐŸÐ¾ÑˆÑƒÐº Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼", "callback_data": "admin_customer_search"}],
        [{"text": "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers_vip"}],
        [{"text": "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ñ– ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers_regular"}],
        [{"text": "ðŸ†• ÐÐ¾Ð²Ñ– ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers_new"}],
        [{"text": "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ñ– ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¸", "callback_data": "admin_customers_inactive"}],
        [{"text": "ðŸ“ Ð’Ð¸Ð²Ð°Ð½Ñ‚Ð°Ð¶Ð¸Ñ‚Ð¸ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²", "callback_data": "export_customers"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_messages_menu():
    """ÐœÐµÐ½ÑŽ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼Ð¸"""
    keyboard = [
        [{"text": "ðŸ“‹ ÐžÑÑ‚Ð°Ð½Ð½Ñ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_messages_recent"}],
        [{"text": "ðŸ“‹ Ð’ÑÑ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": "admin_messages_all"}],
        [{"text": "ðŸ“ Ð’ÑÑ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ñ„Ð°Ð¹Ð»Ð¾Ð¼", "callback_data": "messages_all_file"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_menu():
    """ÐœÐµÐ½ÑŽ Ñ€Ð¾Ð·ÑÐ¸Ð»Ð¾Ðº"""
    keyboard = [
        [{"text": "ðŸ“¢ Ð’ÑÑ–Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼", "callback_data": "broadcast_all"}],
        [{"text": "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼", "callback_data": "broadcast_vip"}],
        [{"text": "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼", "callback_data": "broadcast_regular"}],
        [{"text": "ðŸ†• ÐÐ¾Ð²Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼", "callback_data": "broadcast_new"}],
        [{"text": "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼", "callback_data": "broadcast_inactive"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_broadcast_input_back_keyboard() -> InlineKeyboardMarkup:
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð´Ð»Ñ Ð¿Ð¾Ð²ÐµÑ€Ð½ÐµÐ½Ð½Ñ Ð· Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸"""
    buttons = [[{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_broadcast"}]]
    return create_inline_keyboard(buttons)

def get_reports_menu():
    """ÐœÐµÐ½ÑŽ Ð·Ð²Ñ–Ñ‚Ñ–Ð²"""
    keyboard = [
        [{"text": "ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ (TXT)", "callback_data": "report_orders_txt"}],
        [{"text": "ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ (CSV)", "callback_data": "report_orders_csv"}],
        [{"text": "ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð¸ (TXT)", "callback_data": "report_users_txt"}],
        [{"text": "ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð¸ (CSV)", "callback_data": "report_users_csv"}],
        [{"text": "âš¡ Ð¨Ð²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ (TXT)", "callback_data": "report_quick_txt"}],
        [{"text": "âš¡ Ð¨Ð²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ (CSV)", "callback_data": "report_quick_csv"}],
        [{"text": "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ (TXT)", "callback_data": "report_messages_txt"}],
        [{"text": "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ (CSV)", "callback_data": "report_messages_csv"}],
        [{"text": "ðŸ“Š Ð¡Ñ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ° (TXT)", "callback_data": "report_stats_txt"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_admins_menu():
    """ÐœÐµÐ½ÑŽ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼Ð¸"""
    keyboard = [
        [{"text": "ðŸ“‹ Ð¡Ð¿Ð¸ÑÐ¾Ðº Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²", "callback_data": "admin_list"}],
        [{"text": "âž• Ð”Ð¾Ð´Ð°Ñ‚Ð¸ Ð°Ð´Ð¼Ñ–Ð½Ð°", "callback_data": "admin_add"}],
        [{"text": "ðŸ—‘ Ð’Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ð°Ð´Ð¼Ñ–Ð½Ð°", "callback_data": "admin_remove"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

def get_settings_menu():
    """ÐœÐµÐ½ÑŽ Ð½Ð°Ð»Ð°ÑˆÑ‚ÑƒÐ²Ð°Ð½ÑŒ"""
    keyboard = [
        [{"text": "ðŸ”‘ Ð—Ð¼Ñ–Ð½Ð¸Ñ‚Ð¸ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ", "callback_data": "admin_settings_password"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(keyboard)

# ========== ÐœÐ•ÐÐ® Ð”Ð›Ð¯ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ ÐšÐžÐœÐŸÐÐÐ†Ð‡ ==========

def get_company_edit_menu() -> InlineKeyboardMarkup:
    """ÐœÐµÐ½ÑŽ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ñ–Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–Ñ— Ð¿Ñ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ"""
    buttons = [
        [{"text": "âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ñ‚ÐµÐºÑÑ‚", "callback_data": "company_edit_text"}],
        [{"text": "ðŸ‘ï¸ ÐŸÐµÑ€ÐµÐ³Ð»ÑÐ½ÑƒÑ‚Ð¸ Ð¿Ð¾Ñ‚Ð¾Ñ‡Ð½Ð¸Ð¹", "callback_data": "company_view"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

# ========== ÐœÐ•ÐÐ® Ð”Ð›Ð¯ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ Ð’Ð†Ð¢ÐÐÐÐ¯ ==========

def get_welcome_edit_menu() -> InlineKeyboardMarkup:
    """ÐœÐµÐ½ÑŽ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ð¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ"""
    buttons = [
        [{"text": "âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ð²Ñ–Ñ‚Ð°Ð½Ð½Ñ", "callback_data": "welcome_edit_text"}],
        [{"text": "ðŸ‘ï¸ ÐŸÐµÑ€ÐµÐ³Ð»ÑÐ½ÑƒÑ‚Ð¸ Ð¿Ð¾Ñ‚Ð¾Ñ‡Ð½Ðµ", "callback_data": "welcome_view"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

# ========== ÐÐžÐ’Ð• ÐœÐ•ÐÐ® Ð”Ð›Ð¯ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ FAQ ==========

def get_faq_edit_main_menu() -> InlineKeyboardMarkup:
    """Ð“Ð¾Ð»Ð¾Ð²Ð½Ðµ Ð¼ÐµÐ½ÑŽ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ FAQ"""
    buttons = [
        [{"text": "ðŸ“‹ Ð¡Ð¿Ð¸ÑÐ¾Ðº FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ", "callback_data": "faq_edit_list"}],
        [{"text": "âž• Ð”Ð¾Ð´Ð°Ñ‚Ð¸ Ð½Ð¾Ð²Ðµ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ", "callback_data": "faq_edit_add"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_main"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_edit_list_keyboard(faqs: List[Dict]) -> InlineKeyboardMarkup:
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð·Ñ– ÑÐ¿Ð¸ÑÐºÐ¾Ð¼ FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ"""
    buttons = []
    for faq in faqs:
        # Ð¡ÐºÐ¾Ñ€Ð¾Ñ‡ÑƒÑ”Ð¼Ð¾ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ Ð´Ð»Ñ ÐºÐ½Ð¾Ð¿ÐºÐ¸
        short_q = faq['question'][:40] + "..." if len(faq['question']) > 40 else faq['question']
        buttons.append([{"text": f"â“ {short_q}", "callback_data": f"faq_edit_select_{faq['id']}"}])
    buttons.append([{"text": "âž• Ð”Ð¾Ð´Ð°Ñ‚Ð¸ Ð½Ð¾Ð²Ðµ", "callback_data": "faq_edit_add"}])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_faq_edit_main"}])
    return create_inline_keyboard(buttons)

def get_faq_edit_actions_keyboard(faq_id: int) -> InlineKeyboardMarkup:
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð´Ñ–Ð¹ Ð´Ð»Ñ ÐºÐ¾Ð½ÐºÑ€ÐµÑ‚Ð½Ð¾Ð³Ð¾ FAQ"""
    buttons = [
        [{"text": "âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ", "callback_data": f"faq_edit_question_{faq_id}"}],
        [{"text": "âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ñ‚Ð¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ", "callback_data": f"faq_edit_answer_{faq_id}"}],
        [{"text": "â¬†ï¸ ÐŸÐµÑ€ÐµÐ¼Ñ–ÑÑ‚Ð¸Ñ‚Ð¸ Ð²Ð³Ð¾Ñ€Ñƒ", "callback_data": f"faq_edit_move_up_{faq_id}"}],
        [{"text": "â¬‡ï¸ ÐŸÐµÑ€ÐµÐ¼Ñ–ÑÑ‚Ð¸Ñ‚Ð¸ Ð²Ð½Ð¸Ð·", "callback_data": f"faq_edit_move_down_{faq_id}"}],
        [{"text": "âŒ Ð’Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸", "callback_data": f"faq_edit_delete_{faq_id}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´ Ð´Ð¾ ÑÐ¿Ð¸ÑÐºÑƒ", "callback_data": "faq_edit_list"}]
    ]
    return create_inline_keyboard(buttons)

def get_order_actions_menu(order_id: int, order_type: str = 'regular'):
    """ÐœÐµÐ½ÑŽ Ð´Ñ–Ð¹ Ñ–Ð· Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼"""
    keyboard = [
        [{"text": "âœ… ÐŸÑ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¸Ñ‚Ð¸", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "ðŸ“¦ Ð£Ð¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "ðŸšš Ð’Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "ðŸ“ ÐŸÑ€Ð¸Ð±ÑƒÐ»Ð¾", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "âŒ Ð¡ÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_orders"}]
    ]
    return create_inline_keyboard(keyboard)

def get_message_actions_menu(message_id: int, user_id: int):
    """ÐœÐµÐ½ÑŽ Ð´Ñ–Ð¹ Ñ–Ð· Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼"""
    keyboard = [
        [{"text": "ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸", "callback_data": f"reply_user_{user_id}"}],
        [{"text": "ðŸ‘¤ ÐŸÑ€Ð¾Ñ„Ñ–Ð»ÑŒ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", "callback_data": f"customer_view_{user_id}"}],
        [{"text": "ðŸ“‹ Ð’ÑÑ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": "back_to_messages"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_messages"}]
    ]
    return create_inline_keyboard(keyboard)

def get_customer_actions_menu(user_id: int):
    """ÐœÐµÐ½ÑŽ Ð´Ñ–Ð¹ Ñ–Ð· ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð¾Ð¼"""
    keyboard = [
        [{"text": "ðŸ“‹ Ð†ÑÑ‚Ð¾Ñ€Ñ–Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ", "callback_data": f"customer_orders_{user_id}"}],
        [{"text": "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": f"customer_messages_{user_id}"}],
        [{"text": "ðŸ“¢ ÐÐ°Ð´Ñ–ÑÐ»Ð°Ñ‚Ð¸ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", "callback_data": f"customer_message_{user_id}"}],
        [{"text": "ðŸ‘‘ Ð—Ñ€Ð¾Ð±Ð¸Ñ‚Ð¸ Ð°Ð´Ð¼Ñ–Ð½Ð¾Ð¼", "callback_data": f"customer_make_admin_{user_id}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_customers"}]
    ]
    return create_inline_keyboard(keyboard)

def get_order_status_keyboard(order_id: int, order_type: str = 'regular'):
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð´Ð»Ñ Ð·Ð¼Ñ–Ð½Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"""
    keyboard = [
        [{"text": "âœ… ÐŸÑ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¸Ñ‚Ð¸", "callback_data": f"order_confirm_{order_id}_{order_type}"}],
        [{"text": "ðŸ“¦ Ð£Ð¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾", "callback_data": f"order_packed_{order_id}_{order_type}"}],
        [{"text": "ðŸšš Ð’Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾", "callback_data": f"order_shipped_{order_id}_{order_type}"}],
        [{"text": "ðŸ“ ÐŸÑ€Ð¸Ð±ÑƒÐ»Ð¾", "callback_data": f"order_arrived_{order_id}_{order_type}"}],
        [{"text": "âŒ Ð¡ÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", "callback_data": f"order_cancel_{order_id}_{order_type}"}],
        [{"text": "ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–ÑÑ‚Ð¸", "callback_data": f"reply_order_{order_id}_{order_type}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_orders"}]
    ]
    return create_inline_keyboard(keyboard)

def get_orders_pagination_keyboard(user_id: int, has_more: bool = True):
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð¿Ð°Ð³Ñ–Ð½Ð°Ñ†Ñ–Ñ— Ð´Ð»Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ"""
    buttons = []
    if has_more:
        buttons.append([{"text": "ðŸ“‹ Ð©Ðµ 5 Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ", "callback_data": "admin_order_more"}])
    buttons.append([{"text": "ðŸ” Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾", "callback_data": "admin_order_details"}])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_orders"}])
    return create_inline_keyboard(buttons)

def get_messages_pagination_keyboard(user_id: int, has_more: bool = True):
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð¿Ð°Ð³Ñ–Ð½Ð°Ñ†Ñ–Ñ— Ð´Ð»Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ"""
    buttons = []
    if has_more:
        buttons.append([{"text": "ðŸ“‹ Ð©Ðµ 5 Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ", "callback_data": "admin_messages_more"}])
    buttons.append([{"text": "ðŸ” Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾", "callback_data": "admin_messages_details"}])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_to_messages"}])
    return create_inline_keyboard(buttons)

def get_product_image_keyboard(product_id: int, has_image: bool = False) -> InlineKeyboardMarkup:
    """ÐšÐ»Ð°Ð²Ñ–Ð°Ñ‚ÑƒÑ€Ð° Ð´Ð»Ñ ÐºÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ"""
    buttons = []
    buttons.append([{"text": "ðŸŒ Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶Ð¸Ñ‚Ð¸ Ð·Ð° URL", "callback_data": f"edit_product_image_url_{product_id}"}])
    buttons.append([{"text": "ðŸ“· Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶Ð¸Ñ‚Ð¸ Ñ„Ð°Ð¹Ð»", "callback_data": f"edit_product_image_file_{product_id}"}])
    if has_image:
        buttons.append([{"text": "ðŸ—‘ Ð’Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ñ„Ð¾Ñ‚Ð¾", "callback_data": f"delete_product_image_{product_id}"}])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": f"back_to_edit_product_{product_id}"}])
    return create_inline_keyboard(buttons)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð½Ð¸Ðº Ð½Ð°Ñ‚Ð¸ÑÐºÐ°Ð½ÑŒ Ð½Ð° ÐºÐ½Ð¾Ð¿ÐºÐ¸"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        user_id = user.id
        data = query.data
        
        logger.info(f"ðŸ–±ï¸ ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð½Ð°Ñ‚Ð¸ÑÐ½ÑƒÐ²: {data}")
        logger.debug(f"Ð¡Ñ‚Ð°Ð½ ÑÐµÑÑ–Ñ—: admin_sessions[{user_id}] = {admin_sessions.get(user_id)}")
        
        if not is_authenticated(user_id):
            logger.warning(f"âŒ ÐÐµÐ°Ð²Ñ‚ÐµÐ½Ñ‚Ð¸Ñ„Ñ–ÐºÐ¾Ð²Ð°Ð½Ð¸Ð¹ Ð°Ð´Ð¼Ñ–Ð½ {user_id} ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ²Ð°Ð² Ð½Ð°Ñ‚Ð¸ÑÐ½ÑƒÑ‚Ð¸ {data}")
            logger.debug(f"Ð’ÑÑ– ÑÐµÑÑ–Ñ—: {admin_sessions}")
            await query.edit_message_text("âŒ Ð¡ÐµÑÑ–Ñ Ð·Ð°ÐºÑ–Ð½Ñ‡Ð¸Ð»Ð°ÑÑŒ\n\nÐÐ°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ /start Ð´Ð»Ñ Ð¿Ð¾Ð²Ñ‚Ð¾Ñ€Ð½Ð¾Ð³Ð¾ Ð²Ñ…Ð¾Ð´Ñƒ")
            return
        
        # ============== 1. Ð¡ÐŸÐ•Ð¦Ð˜Ð¤Ð†Ð§ÐÐ† ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ ==============
        
        # ÐžÐ±Ñ€Ð¾Ð±ÐºÐ° ÐºÐ½Ð¾Ð¿Ð¾Ðº "ÐÐ°Ð·Ð°Ð´"
        if data.startswith("back_to_"):
            target = data[8:]
            logger.debug(f"ÐžÐ±Ñ€Ð¾Ð±ÐºÐ° back_to: {target}")
            
            if target == "faq_edit_main":
                await query.edit_message_text("â“ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ FAQ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_faq_edit_main_menu())
                return
            elif target.startswith("edit_product_"):
                try:
                    product_id = int(target.split("_")[2])
                    product = get_product_by_id(product_id)
                    if product:
                        admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
                        keyboard = [
                            [InlineKeyboardButton("ðŸ“ ÐÐ°Ð·Ð²Ð°", callback_data=f"edit_field_name_{product_id}")],
                            [InlineKeyboardButton("ðŸ’° Ð¦Ñ–Ð½Ð°", callback_data=f"edit_field_price_{product_id}")],
                            [InlineKeyboardButton("ðŸ“‹ ÐžÐ¿Ð¸Ñ", callback_data=f"edit_field_desc_{product_id}")],
                            [InlineKeyboardButton("ðŸ· ÐšÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–Ñ", callback_data=f"edit_field_cat_{product_id}")],
                            [InlineKeyboardButton("ðŸ“· Ð¤Ð¾Ñ‚Ð¾", callback_data=f"edit_field_image_{product_id}")],
                            [InlineKeyboardButton("ðŸ“ ÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ–", callback_data=f"edit_field_unit_{product_id}")],
                            [InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")]
                        ]
                        await query.edit_message_text(
                            f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id}\n\n"
                            f"ÐÐ°Ð·Ð²Ð°: {product['name']}\n"
                            f"Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½\n"
                            f"ÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ–: {product['unit']}\n\n"
                            f"ÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ð¾Ð»Ðµ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                        return
                except Exception as e:
                    logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð±Ñ€Ð¾Ð±ÐºÐ¸ back_to_edit_product: {e}")
                
                await query.edit_message_text("ðŸ“¦ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_products_menu())
                return
            elif target == "main":
                await query.edit_message_text("ðŸ” ÐÐ´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»ÑŒ Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ€Ð¾Ð·Ð´Ñ–Ð»:", reply_markup=get_main_menu())
                return
            elif target == "orders":
                await query.edit_message_text("ðŸ“‹ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ‚Ð¸Ð¿ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ:", reply_markup=get_orders_menu())
                return
            elif target == "customers":
                await query.edit_message_text("ðŸ‘¥ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_customers_menu())
                return
            elif target == "messages":
                await query.edit_message_text("ðŸ’¬ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_messages_menu())
                return
            elif target == "broadcast":
                await query.edit_message_text("ðŸ“¢ Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ†Ñ–Ð»ÑŒÐ¾Ð²Ñƒ Ð°ÑƒÐ´Ð¸Ñ‚Ð¾Ñ€Ñ–ÑŽ:", reply_markup=get_broadcast_menu())
                return
            elif target == "products":
                await query.edit_message_text("ðŸ“¦ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_products_menu())
                return
            elif target == "company":
                await query.edit_message_text("ðŸ¢ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ 'ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ'\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_company_edit_menu())
                return
            elif target == "welcome":
                await query.edit_message_text("ðŸ‘‹ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ð¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_welcome_edit_menu())
                return
            else:
                await query.edit_message_text("ðŸ” ÐÐ´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»ÑŒ Ð‘Ð¾Ð½ÐµÐ»ÐµÑ‚\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ€Ð¾Ð·Ð´Ñ–Ð»:", reply_markup=get_main_menu())
                return
        
        elif data == "admin_logout":
            admin_sessions.pop(user_id, None)
            last_password_check.pop(user_id, None)
            logger.info(f"ðŸ”“ ÐÐ´Ð¼Ñ–Ð½ {user_id} Ð²Ð¸Ð¹ÑˆÐ¾Ð² Ð· ÑÐ¸ÑÑ‚ÐµÐ¼Ð¸")
            await query.edit_message_text("ðŸ” Ð’Ð¸ Ð²Ð¸Ð¹ÑˆÐ»Ð¸ Ð· Ð°Ð´Ð¼Ñ–Ð½-Ð¿Ð°Ð½ÐµÐ»Ñ–\n\nÐ”Ð»Ñ Ð¿Ð¾Ð²Ñ‚Ð¾Ñ€Ð½Ð¾Ð³Ð¾ Ð²Ñ…Ð¾Ð´Ñƒ Ð½Ð°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ /start")
            return
        
        elif data == "admin_reset_orders":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("âœ… Ð¢Ð°Ðº, Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ð²ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", callback_data="confirm_reset_orders")],
                [InlineKeyboardButton("âŒ ÐÑ–, ÑÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", callback_data="back_to_main")]
            ])
            await query.edit_message_text("âš ï¸ <b>Ð’Ð¸ Ð´Ñ–Ð¹ÑÐ½Ð¾ Ñ…Ð¾Ñ‡ÐµÑ‚Ðµ Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ð’Ð¡Ð† Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚Ð° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ?</b>\n\nÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð¸ Ñ‚Ð° Ñ‚Ð¾Ð²Ð°Ñ€Ð¸ Ð·Ð°Ð»Ð¸ÑˆÐ°Ñ‚ÑŒÑÑ, Ð°Ð»Ðµ Ð²ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚Ð° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð±ÑƒÐ´ÑƒÑ‚ÑŒ Ð±ÐµÐ·Ð¿Ð¾Ð²Ð¾Ñ€Ð¾Ñ‚Ð½Ð¾ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ñ–.", reply_markup=keyboard, parse_mode='HTML')
            return
        
        elif data == "confirm_reset_orders":
            success = await reset_all_orders()
            if success:
                text = "âœ… Ð’ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚Ð° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾!"
            else:
                text = "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ–"
            await query.edit_message_text(text, reply_markup=get_main_menu())
            return
        
        # ========== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ ÐšÐžÐœÐŸÐÐÐ†Ð‡ ==========
        
        elif data == "admin_edit_company":
            await query.edit_message_text("ðŸ¢ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ 'ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ'\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_company_edit_menu())
            return
        
        elif data == "company_view":
            company_text = get_company_info()
            await query.edit_message_text(
                f"ðŸ¢ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚:</b>\n\n{company_text}",
                reply_markup=get_back_keyboard("company"),
                parse_mode='HTML'
            )
            return
        
        elif data == "company_edit_text":
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_company_text"}
            await query.edit_message_text(
                f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚ÐµÐºÑÑ‚Ñƒ 'ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ'\n\n"
                f"ðŸ“‹ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚ (ÑÐºÐ¾Ð¿Ñ–ÑŽÐ¹Ñ‚Ðµ Ð¹Ð¾Ð³Ð¾):</b>\n\n{get_company_info()}\n\n"
                f"ðŸ“ ÐÐ°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚:",
                reply_markup=get_back_keyboard("company"),
                parse_mode='HTML'
            )
            return
        
        # ========== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ Ð’Ð†Ð¢ÐÐÐÐ¯ ==========
        
        elif data == "admin_edit_welcome":
            await query.edit_message_text("ðŸ‘‹ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ð¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_welcome_edit_menu())
            return
        
        elif data == "welcome_view":
            welcome_text = get_welcome_message()
            await query.edit_message_text(
                f"ðŸ‘‹ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ðµ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:</b>\n\n{welcome_text}",
                reply_markup=get_back_keyboard("welcome"),
                parse_mode='HTML'
            )
            return
        
        elif data == "welcome_edit_text":
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_welcome_text"}
            await query.edit_message_text(
                f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ð¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ\n\n"
                f"ðŸ“‹ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚ (ÑÐºÐ¾Ð¿Ñ–ÑŽÐ¹Ñ‚Ðµ Ð¹Ð¾Ð³Ð¾):</b>\n\n{get_welcome_message()}\n\n"
                f"ðŸ“ ÐÐ°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚:",
                reply_markup=get_back_keyboard("welcome"),
                parse_mode='HTML'
            )
            return
        
        # ========== ÐÐžÐ’Ð† ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ FAQ ==========
        
        elif data == "admin_faq_edit":
            await query.edit_message_text("â“ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ FAQ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_faq_edit_main_menu())
            return
        
        elif data == "faq_edit_list":
            faqs = get_all_faqs()
            if not faqs:
                await query.edit_message_text("â“ FAQ Ð¿Ð¾Ñ€Ð¾Ð¶Ð½Ñ–Ð¹. Ð”Ð¾Ð´Ð°Ð¹Ñ‚Ðµ Ð½Ð¾Ð²Ðµ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ.", reply_markup=get_faq_edit_main_menu())
                return
            
            await query.edit_message_text(
                "â“ <b>Ð’Ð¸Ð±ÐµÑ€Ñ–Ñ‚ÑŒ FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:</b>",
                reply_markup=get_faq_edit_list_keyboard(faqs),
                parse_mode='HTML'
            )
            return
        
        elif data == "faq_edit_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "faq_edit_add_question"}
            await query.edit_message_text(
                "âž• Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ð½Ð¾Ð²Ð¾Ð³Ð¾ FAQ\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ <b>Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ</b>:",
                reply_markup=get_back_keyboard("faq_edit_main"),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("faq_edit_select_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                # Ð—Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ ID Ð² ÑÐµÑÑ–Ñ—
                admin_sessions[user_id]["current_faq_id"] = faq_id
                
                text = f"â“ <b>FAQ #{faq_id}</b>\n\n"
                text += f"<b>ÐŸÐ¸Ñ‚Ð°Ð½Ð½Ñ:</b> {faq['question']}\n\n"
                text += f"<b>Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ:</b> {faq['answer']}"
                
                await query.edit_message_text(
                    text,
                    reply_markup=get_faq_edit_actions_keyboard(faq_id),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð±Ð¾Ñ€Ñƒ FAQ: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_question_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                admin_sessions[user_id] = {
                    "state": "authenticated",
                    "action": f"faq_edit_update_question",
                    "faq_id": faq_id
                }
                
                await query.edit_message_text(
                    f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ FAQ #{faq_id}\n\n"
                    f"ðŸ“‹ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ðµ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ:</b>\n{faq['question']}\n\n"
                    f"ðŸ“ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ðµ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ:",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}"),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_answer_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                admin_sessions[user_id] = {
                    "state": "authenticated",
                    "action": f"faq_edit_update_answer",
                    "faq_id": faq_id
                }
                
                await query.edit_message_text(
                    f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´Ñ– FAQ #{faq_id}\n\n"
                    f"ðŸ“‹ <b>ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð° Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ:</b>\n{faq['answer']}\n\n"
                    f"ðŸ“ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ñƒ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ:",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}"),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´Ñ–: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_move_up_"):
            try:
                faq_id = int(data.split("_")[4])
                if move_faq_up(faq_id):
                    await query.answer("âœ… ÐŸÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð¾ Ð²Ð³Ð¾Ñ€Ñƒ")
                else:
                    await query.answer("âŒ Ð’Ð¶Ðµ Ð½Ð° Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÑƒ", show_alert=False)
                
                # ÐŸÐ¾Ð²ÐµÑ€Ñ‚Ð°Ñ”Ð¼Ð¾ÑÑŒ Ð´Ð¾ ÑÐ¿Ð¸ÑÐºÑƒ
                faqs = get_all_faqs()
                await query.edit_message_text(
                    "â“ <b>Ð’Ð¸Ð±ÐµÑ€Ñ–Ñ‚ÑŒ FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:</b>",
                    reply_markup=get_faq_edit_list_keyboard(faqs),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð²Ð³Ð¾Ñ€Ñƒ: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_move_down_"):
            try:
                faq_id = int(data.split("_")[4])
                if move_faq_down(faq_id):
                    await query.answer("âœ… ÐŸÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð¾ Ð²Ð½Ð¸Ð·")
                else:
                    await query.answer("âŒ Ð’Ð¶Ðµ Ð² ÐºÑ–Ð½Ñ†Ñ–", show_alert=False)
                
                # ÐŸÐ¾Ð²ÐµÑ€Ñ‚Ð°Ñ”Ð¼Ð¾ÑÑŒ Ð´Ð¾ ÑÐ¿Ð¸ÑÐºÑƒ
                faqs = get_all_faqs()
                await query.edit_message_text(
                    "â“ <b>Ð’Ð¸Ð±ÐµÑ€Ñ–Ñ‚ÑŒ FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:</b>",
                    reply_markup=get_faq_edit_list_keyboard(faqs),
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ¼Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð²Ð½Ð¸Ð·: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_delete_"):
            try:
                faq_id = int(data.split("_")[3])
                faq = get_faq_by_id(faq_id)
                if not faq:
                    await query.edit_message_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                    return
                
                # ÐŸÑ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð½Ñ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("âœ… Ð¢Ð°Ðº, Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸", callback_data=f"faq_edit_confirm_delete_{faq_id}")],
                    [InlineKeyboardButton("âŒ ÐÑ–, ÑÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", callback_data=f"faq_edit_select_{faq_id}")]
                ])
                await query.edit_message_text(
                    f"â“ <b>Ð’Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ FAQ?</b>\n\n"
                    f"<b>ÐŸÐ¸Ñ‚Ð°Ð½Ð½Ñ:</b> {faq['question']}\n\n"
                    f"Ð¦Ñ Ð´Ñ–Ñ Ð½ÐµÐ·Ð²Ð¾Ñ€Ð¾Ñ‚Ð½Ð°.",
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        elif data.startswith("faq_edit_confirm_delete_"):
            try:
                faq_id = int(data.split("_")[4])
                if delete_faq(faq_id):
                    await query.answer("âœ… FAQ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾")
                    faqs = get_all_faqs()
                    if faqs:
                        await query.edit_message_text(
                            "â“ <b>Ð’Ð¸Ð±ÐµÑ€Ñ–Ñ‚ÑŒ FAQ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:</b>",
                            reply_markup=get_faq_edit_list_keyboard(faqs),
                            parse_mode='HTML'
                        )
                    else:
                        await query.edit_message_text("â“ FAQ Ð¿Ð¾Ñ€Ð¾Ð¶Ð½Ñ–Ð¹", reply_markup=get_faq_edit_main_menu())
                else:
                    await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ–", reply_markup=get_back_keyboard("faq_edit_main"))
            except (IndexError, ValueError) as e:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð½Ñ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq_edit_main"))
            return
        
        # ========== Ð†ÐÐ¨Ð† ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ ==========
        
        elif data == "admin_products":
            await query.edit_message_text("ðŸ“¦ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_products_menu())
            return
        
        elif data == "admin_product_list":
            products = get_all_products()
            if not products:
                text = "ðŸ“¦ Ð¡Ð¿Ð¸ÑÐ¾Ðº Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²\n\nÐ¢Ð¾Ð²Ð°Ñ€Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
            else:
                text = "ðŸ“¦ Ð¡Ð¿Ð¸ÑÐ¾Ðº Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²\n\n"
                for p in products:
                    text += f"ID: {p['id']}\nÐÐ°Ð·Ð²Ð°: {p['name']}\nÐ¦Ñ–Ð½Ð°: {p['price']} Ð³Ñ€Ð½/{p['unit']}\nÐšÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–Ñ: {p['category']}\n{'â”€'*30}\n"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_product_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_product_name"}
            await query.edit_message_text("âž• Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ð½Ð¾Ð²Ð¾Ð³Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð°Ð·Ð²Ñƒ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ:", reply_markup=get_back_keyboard("products"))
            return
        
        elif data == "admin_product_edit":
            products = get_all_products()
            if not products:
                await query.edit_message_text("âŒ Ð¢Ð¾Ð²Ð°Ñ€Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([InlineKeyboardButton(f"{p['id']}. {p['name'][:30]}", callback_data=f"edit_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")])
            await query.edit_message_text("âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ‚Ð¾Ð²Ð°Ñ€ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ Ð¤ÐžÐ¢Ðž ==============
        
        elif data.startswith("delete_product_image_"):
            logger.info(f"ðŸ”„ ÐÐ°Ñ‚Ð¸ÑÐ½ÑƒÑ‚Ð¾ ÐºÐ½Ð¾Ð¿ÐºÑƒ delete_product_image_, data: {data}")
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"âœ… Ð Ð¾Ð·Ð¿Ð°Ñ€ÑÐµÐ½Ð¾ product_id: {product_id}")
            except (IndexError, ValueError) as e:
                logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ð°Ñ€ÑÐ¸Ð½Ð³Ñƒ ID: {e}")
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
            
            product = get_product_by_id(product_id)
            if not product:
                logger.error(f"âŒ Ð¢Ð¾Ð²Ð°Ñ€ Ð· ID {product_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð² Ð‘Ð”")
                await query.edit_message_text(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ñ‚Ð¾Ð²Ð°Ñ€ Ð· ID {product_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_products_menu())
                return
            
            if update_product(product_id, image_data=None):
                await query.edit_message_text(
                    f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾!",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
            else:
                await query.edit_message_text(
                    f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
            return
        
        elif data.startswith("edit_product_image_url_"):
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"âœ… Ð’Ð¸Ð±Ñ–Ñ€: Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾ Ð·Ð° URL Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ {product_id}")
                admin_sessions[user_id] = {
                    "state": "authenticated", 
                    "action": "edit_product_image_url", 
                    "product_id": product_id
                }
                await query.edit_message_text(
                    "ðŸŒ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ URL Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ:",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
                return
            except (IndexError, ValueError):
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
        
        elif data.startswith("edit_product_image_file_"):
            try:
                product_id = int(data.split("_")[-1])
                logger.info(f"âœ… Ð’Ð¸Ð±Ñ–Ñ€: Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ñ„Ð°Ð¹Ð»Ñƒ Ñ„Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ {product_id}")
                admin_sessions[user_id] = {
                    "state": "authenticated", 
                    "action": "edit_product_image_file", 
                    "product_id": product_id
                }
                await query.edit_message_text(
                    "ðŸ“· ÐÐ°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ñ„Ð¾Ñ‚Ð¾:",
                    reply_markup=get_back_keyboard(f"edit_product_{product_id}")
                )
                return
            except (IndexError, ValueError):
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
        
        elif data.startswith("edit_field_"):
            parts = data.split("_")
            if len(parts) < 4:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_products_menu())
                return
            
            field = parts[2]
            try:
                product_id = int(parts[-1])
            except (IndexError, ValueError):
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
            
            if field == "image":
                product = get_product_by_id(product_id)
                has_image = product and product.get('image_data') is not None
                admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_image", "product_id": product_id}
                await query.edit_message_text(
                    "ðŸ“· Ð’Ð¸Ð±ÐµÑ€Ñ–Ñ‚ÑŒ ÑÐ¿Ð¾ÑÑ–Ð± Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾:",
                    reply_markup=get_product_image_keyboard(product_id, has_image)
                )
                return
            elif field == "unit":
                admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_unit", "product_id": product_id}
                await query.edit_message_text(
                    f"âœï¸ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ñƒ Ð¾Ð´Ð¸Ð½Ð¸Ñ†ÑŽ Ð²Ð¸Ð¼Ñ–Ñ€Ñƒ (Ð½Ð°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: Ð±Ð°Ð½ÐºÐ°, ÐºÐ³, ÑˆÑ‚, Ð»):",
                    reply_markup=get_back_keyboard("products")
                )
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": f"edit_product_{field}", "product_id": product_id}
            field_names = {"name": "Ð½Ð°Ð·Ð²Ñƒ", "price": "Ñ†Ñ–Ð½Ñƒ", "desc": "Ð¾Ð¿Ð¸Ñ", "cat": "ÐºÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–ÑŽ"}
            await query.edit_message_text(f"âœï¸ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ñƒ {field_names.get(field, '')}:", reply_markup=get_back_keyboard("products"))
            return
        
        elif data.startswith("edit_product_"):
            logger.info(f"ðŸ“ ÐÐ°Ñ‚Ð¸ÑÐ½ÑƒÑ‚Ð¾ Ð·Ð°Ð³Ð°Ð»ÑŒÐ½Ð¸Ð¹ edit_product_ Ð· data: {data}")
            
            if data.count("_") > 2:
                logger.warning(f"âš ï¸ ÐŸÑ€Ð¾Ð¿ÑƒÑÐºÐ°Ñ”Ð¼Ð¾ ÑÐºÐ»Ð°Ð´Ð½Ð¸Ð¹ callback Ñƒ Ð·Ð°Ð³Ð°Ð»ÑŒÐ½Ð¾Ð¼Ñƒ Ð¾Ð±Ñ€Ð¾Ð±Ð½Ð¸ÐºÑƒ: {data}")
                return
            
            try:
                product_id = int(data.split("_")[2])
                logger.info(f"âœ… Ð Ð¾Ð·Ð¿Ð°Ñ€ÑÐµÐ½Ð¾ product_id: {product_id}")
            except (IndexError, ValueError):
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
            
            product = get_product_by_id(product_id)
            if not product:
                await query.edit_message_text("âŒ Ð¢Ð¾Ð²Ð°Ñ€ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_products_menu())
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": "edit_product_field", "product_id": product_id}
            keyboard = [
                [InlineKeyboardButton("ðŸ“ ÐÐ°Ð·Ð²Ð°", callback_data=f"edit_field_name_{product_id}")],
                [InlineKeyboardButton("ðŸ’° Ð¦Ñ–Ð½Ð°", callback_data=f"edit_field_price_{product_id}")],
                [InlineKeyboardButton("ðŸ“‹ ÐžÐ¿Ð¸Ñ", callback_data=f"edit_field_desc_{product_id}")],
                [InlineKeyboardButton("ðŸ· ÐšÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–Ñ", callback_data=f"edit_field_cat_{product_id}")],
                [InlineKeyboardButton("ðŸ“· Ð¤Ð¾Ñ‚Ð¾", callback_data=f"edit_field_image_{product_id}")],
                [InlineKeyboardButton("ðŸ“ ÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ–", callback_data=f"edit_field_unit_{product_id}")],
                [InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")]
            ]
            await query.edit_message_text(
                f"âœï¸ Ð ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id}\n\n"
                f"ÐÐ°Ð·Ð²Ð°: {product['name']}\n"
                f"Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½\n"
                f"ÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ–: {product['unit']}\n\n"
                f"ÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ð¾Ð»Ðµ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        elif data == "admin_product_delete":
            products = get_all_products()
            if not products:
                await query.edit_message_text("âŒ Ð¢Ð¾Ð²Ð°Ñ€Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_products_menu())
                return
            keyboard = []
            for p in products[:20]:
                keyboard.append([InlineKeyboardButton(f"âŒ {p['id']}. {p['name'][:30]}", callback_data=f"delete_product_{p['id']}")])
            keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")])
            await query.edit_message_text("ðŸ—‘ Ð’Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ‚Ð¾Ð²Ð°Ñ€ Ð´Ð»Ñ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("delete_product_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_products_menu())
                return
            
            try:
                product_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
            
            keyboard = [
                [InlineKeyboardButton("âœ… Ð¢Ð°Ðº, Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸", callback_data=f"confirm_delete_{product_id}")],
                [InlineKeyboardButton("âŒ ÐÑ–, ÑÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", callback_data="back_to_products")]
            ]
            await query.edit_message_text(f"ðŸ—‘ ÐŸÑ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð½Ñ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ\n\nÐ’Ð¸ Ð´Ñ–Ð¹ÑÐ½Ð¾ Ñ…Ð¾Ñ‡ÐµÑ‚Ðµ Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€ #{product_id}?", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("confirm_delete_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_products_menu())
                return
            
            try:
                product_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
                return
            
            if delete_product(product_id):
                text = "âœ… Ð¢Ð¾Ð²Ð°Ñ€ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾!"
            else:
                text = "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ– Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_products")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_orders":
            await query.edit_message_text("ðŸ“‹ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ‚Ð¸Ð¿ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ:", reply_markup=get_orders_menu())
            return
        
        elif data == "admin_order_recent":
            recent_orders = get_recent_orders(hours=1, min_count=3)
            if not recent_orders:
                text = "ðŸ“‹ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð·Ð° Ð¾ÑÑ‚Ð°Ð½Ð½ÑŽ Ð³Ð¾Ð´Ð¸Ð½Ñƒ Ð½ÐµÐ¼Ð°Ñ”.\n\nÐŸÐ¾ÐºÐ°Ð·ÑƒÑŽ Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:"
                recent_orders = get_all_orders(include_quick=True, limit=3)
            
            if not recent_orders:
                text = "ðŸ“‹ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
            else:
                text = "ðŸ“‹ <b>ÐžÐ¡Ð¢ÐÐÐÐ† Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯</b>\n\n"
                for order in recent_orders:
                    text += format_order_text(order) + f"{'â”€'*40}\n"
            
            all_orders = get_all_orders(include_quick=True, limit=5, offset=0)
            has_more = len(all_orders) >= 5
            
            await query.edit_message_text(text, reply_markup=get_orders_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_order_more":
            more_orders = get_more_orders(user_id, count=5)
            if not more_orders:
                text = "ðŸ“‹ Ð‘Ñ–Ð»ÑŒÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("orders"), parse_mode='HTML')
                return
            
            text = "ðŸ“‹ <b>Ð©Ð• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯</b>\n\n"
            for order in more_orders:
                text += format_order_text(order) + f"{'â”€'*40}\n"
            
            next_orders = get_all_orders(include_quick=True, limit=1, offset=orders_offset.get(user_id, 0))
            has_more = len(next_orders) > 0
            
            await query.edit_message_text(text, reply_markup=get_orders_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_order_all":
            orders = get_all_orders(include_quick=True, limit=10)
            if not orders:
                text = "ðŸ“‹ Ð’ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐ—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
                keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            text = f"ðŸ“‹ Ð’ÑÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐ’ÑÑŒÐ¾Ð³Ð¾: {len(get_all_orders(include_quick=True))}\n\n"
            for order in orders[:10]:
                text += format_order_text(order) + f"{'â”€'*40}\n"
            
            if len(get_all_orders(include_quick=True)) > 10:
                text += f"... Ñ‚Ð° Ñ‰Ðµ Ð±Ñ–Ð»ÑŒÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ"
            
            keyboard = [
                [InlineKeyboardButton("ðŸ” Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾", callback_data="admin_order_details")],
                [InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
        
        elif data == "admin_order_details":
            orders = get_all_orders(include_quick=True, limit=20)
            if not orders:
                await query.edit_message_text("âŒ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_orders_menu())
                return
            keyboard = []
            for order in orders[:20]:
                order_type = order.get('order_type', 'regular')
                type_prefix = "âš¡" if order_type == 'quick' else "ðŸ“¦"
                display_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
                customer_name = order.get('user_name', 'Ð/Ð”')
                total = safe_get(order, 'total', 0)
                keyboard.append([InlineKeyboardButton(
                    f"{type_prefix} â„–{display_id} - {customer_name} - {total:.0f} Ð³Ñ€Ð½", 
                    callback_data=f"order_view_{display_id}_{order_type}"
                )])
            keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")])
            await query.edit_message_text("ðŸ“‹ Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¸Ð¹ Ð¿ÐµÑ€ÐµÐ³Ð»ÑÐ´ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_order_new":
            orders = get_new_orders()
            if not orders:
                text = "ðŸ†• ÐÐ¾Ð²Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐÐ¾Ð²Ð¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½ÐµÐ¼Ð°Ñ”."
            else:
                text = f"ðŸ†• ÐÐ¾Ð²Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐ’ÑÑŒÐ¾Ð³Ð¾: {len(orders)}\n\n"
                for order in orders[:10]:
                    text += f"â„–{order['order_id']} | {order['created_at'][:16]}\n"
                    text += f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order['user_name']}\n"
                    text += f"Ð¡ÑƒÐ¼Ð°: {order.get('total', 0):.2f} Ð³Ñ€Ð½\n"
                    text += f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order['phone']}\n"
                    text += f"{'â”€'*30}\n"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_order_quick":
            orders = get_quick_orders()
            if not orders:
                text = "âš¡ Ð¨Ð²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐ¨Ð²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð½ÐµÐ¼Ð°Ñ”."
            else:
                text = f"âš¡ Ð¨Ð²Ð¸Ð´ÐºÑ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ\n\nÐ’ÑÑŒÐ¾Ð³Ð¾: {len(orders)}\n\n"
                for order in orders[:10]:
                    text += f"âš¡ â„–{order['id']} | {order['created_at'][:16]}\n"
                    text += f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order['user_name']}\n"
                    text += f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order['phone']}\n"
                    text += f"ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {order['product_name']}\n"
                    if order.get('message'):
                        text += f"ðŸ’¬ {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    text += f"{'â”€'*30}\n"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_order_by_phone":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_orders_by_phone"}
            await query.edit_message_text("ðŸ“ž ÐŸÐ¾ÑˆÑƒÐº Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°:", reply_markup=get_back_keyboard("orders"))
            return
        
        elif data.startswith("order_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            order = get_order_by_id(order_id, order_type)
            if not order:
                await query.edit_message_text("âŒ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_orders_menu())
                return
            
            text = f"ðŸ“‹ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ â„–{order_id}\n\n"
            text += f"ðŸ“… Ð”Ð°Ñ‚Ð°: {order['created_at']}\n"
            text += f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order['user_name']}\n"
            text += f"ðŸ“ž Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order['phone']}\n"
            text += f"ðŸ“± Username: @{order['username']}\n"
            
            if order_type == 'regular':
                text += f"ðŸ™ï¸ ÐœÑ–ÑÑ‚Ð¾: {order.get('city', 'Ð/Ð”')}\n"
                text += f"ðŸ£ Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ: {order.get('np_department', 'Ð/Ð”')}\n"
                text += f"{'â”€'*30}\n"
                text += "ðŸ“¦ Ð¢Ð¾Ð²Ð°Ñ€Ð¸:\n"
                for item in order.get('items', []):
                    text += f"  â€¢ {item['product_name']} x{item['quantity']} = {item['price_per_unit'] * item['quantity']:.2f} Ð³Ñ€Ð½\n"
            else:
                text += f"ðŸ“¦ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {order.get('product_name', 'Ð/Ð”')}\n"
                text += f"ðŸ“ž Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ: {order.get('contact_method', 'Ð/Ð”')}\n"
                if order.get('message'):
                    text += f"ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {order['message']}\n"
            
            text += f"{'â”€'*30}\n"
            text += f"ðŸ’° Ð¡ÑƒÐ¼Ð°: {order.get('total', 0):.2f} Ð³Ñ€Ð½\n"
            text += f"ðŸ“Š Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order.get('status', 'Ð½Ð¾Ð²Ðµ')}\n"
            
            await query.edit_message_text(text, reply_markup=get_order_actions_menu(order_id, order_type), parse_mode='HTML')
            return
        
        elif data.startswith("reply_order_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            order = get_order_by_id(order_id, order_type)
            if not order:
                await query.edit_message_text("âŒ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_orders_menu())
                return
            
            admin_sessions[user_id] = {
                "state": "authenticated", 
                "action": "reply_to_order",
                "order_id": order_id,
                "order_type": order_type,
                "user_id": order['user_id']
            }
            await query.edit_message_text(
                f"ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ Ð½Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id}\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‚ÐµÐºÑÑ‚ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°:",
                reply_markup=get_back_keyboard(f"order_view_{order_id}_{order_type}")
            )
            return
        
        elif data.startswith("order_confirm_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð¾", order_type):
                text = f"âœ… Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð¾!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð¾")
            else:
                text = f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_packed_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "ÑƒÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾", order_type):
                text = f"ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} ÑƒÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "ÑƒÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð¾")
            else:
                text = f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_shipped_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾", order_type):
                text = f"ðŸšš Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾")
            else:
                text = f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_arrived_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "Ð¿Ñ€Ð¸Ð±ÑƒÐ»Ð¾", order_type):
                text = f"ðŸ“ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} Ð¿Ñ€Ð¸Ð±ÑƒÐ»Ð¾ Ñƒ Ð²Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "Ð¿Ñ€Ð¸Ð±ÑƒÐ»Ð¾")
            else:
                text = f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– ÑÑ‚Ð°Ñ‚ÑƒÑÑƒ"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("order_cancel_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_orders_menu())
                return
            
            try:
                order_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", reply_markup=get_orders_menu())
                return
            
            order_type = parts[3] if len(parts) > 3 else 'regular'
            
            if update_order_status(order_id, "ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾", order_type):
                text = f"âŒ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾!"
                
                order = get_order_by_id(order_id, order_type)
                if order and order['user_id']:
                    await notify_customer_about_status(order['user_id'], order_id, "ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾")
            else:
                text = f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ ÑÐºÐ°ÑÑƒÐ²Ð°Ð½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"order_view_{order_id}_{order_type}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_messages":
            await query.edit_message_text("ðŸ’¬ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_messages_menu())
            return
        
        elif data == "admin_messages_recent":
            recent_messages = get_recent_messages(hours=24, min_count=5)
            if not recent_messages:
                text = "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð·Ð° Ð¾ÑÑ‚Ð°Ð½Ð½ÑŽ Ð´Ð¾Ð±Ñƒ Ð½ÐµÐ¼Ð°Ñ”.\n\nÐŸÐ¾ÐºÐ°Ð·ÑƒÑŽ Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:"
                recent_messages = get_all_messages(limit=5)
            
            if not recent_messages:
                text = "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("messages"))
                return
            
            all_messages = get_all_messages(limit=5, offset=0)
            has_more = len(all_messages) >= 5
            
            text = "ðŸ’¬ <b>ÐžÐ¡Ð¢ÐÐÐÐ† ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯</b>\n\n"
            for msg in recent_messages:
                text += f"ðŸ’¬ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ #{msg['id']}</b>\n"
                text += f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {msg['user_name']} (@{msg['username']})\n"
                text += f"ðŸ“… Ð§Ð°Ñ: {msg['created_at'][:16]}\n"
                text += f"ðŸ“ {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                text += f"{'â”€'*40}\n"
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_more":
            more_messages = get_more_messages(user_id, count=5)
            if not more_messages:
                text = "ðŸ’¬ Ð‘Ñ–Ð»ÑŒÑˆÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
                await query.edit_message_text(text, reply_markup=get_back_keyboard("messages"), parse_mode='HTML')
                return
            
            text = "ðŸ’¬ <b>Ð©Ð• ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯</b>\n\n"
            for msg in more_messages:
                text += f"ðŸ’¬ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ #{msg['id']}</b>\n"
                text += f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {msg['user_name']} (@{msg['username']})\n"
                text += f"ðŸ“… Ð§Ð°Ñ: {msg['created_at'][:16]}\n"
                text += f"ðŸ“ {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                text += f"{'â”€'*40}\n"
            
            next_messages = get_all_messages(limit=1, offset=messages_offset.get(user_id, 0))
            has_more = len(next_messages) > 0
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_all":
            messages = get_all_messages(limit=20)
            if not messages:
                text = "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð¿Ð¾ÐºÐ¸ Ð½ÐµÐ¼Ð°Ñ”"
            else:
                text = "ðŸ’¬ <b>Ð’Ð¡Ð† ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯</b>\n\n"
                for msg in messages:
                    text += f"ðŸ’¬ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ #{msg['id']}</b>\n"
                    text += f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {msg['user_name']} (@{msg['username']})\n"
                    text += f"ðŸ“… Ð§Ð°Ñ: {msg['created_at'][:16]}\n"
                    text += f"ðŸ“ {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n"
                    text += f"{'â”€'*40}\n"
            
            all_messages = get_all_messages(limit=5, offset=0)
            has_more = len(all_messages) >= 5
            
            await query.edit_message_text(text, reply_markup=get_messages_pagination_keyboard(user_id, has_more), parse_mode='HTML')
            return
        
        elif data == "admin_messages_details":
            messages = get_all_messages(limit=50)
            if not messages:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("messages"))
                return
            keyboard = []
            for msg in messages[:20]:
                user_name = msg['user_name']
                msg_id = msg['id']
                created_at = msg['created_at'][:16] if msg['created_at'] else 'Ð/Ð”'
                text_preview = msg['text'][:30] + ('...' if len(msg['text']) > 30 else '')
                keyboard.append([InlineKeyboardButton(
                    f"ðŸ’¬ #{msg_id} - {user_name} - {created_at}\nðŸ“ {text_preview}", 
                    callback_data=f"message_view_{msg_id}"
                )])
            keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_messages")])
            await query.edit_message_text("ðŸ“‹ Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¸Ð¹ Ð¿ÐµÑ€ÐµÐ³Ð»ÑÐ´ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("message_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("messages"))
                return
            
            try:
                message_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ", reply_markup=get_back_keyboard("messages"))
                return
            
            msg = get_message_by_id(message_id)
            if not msg:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("messages"))
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
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("messages"))
                return
            
            try:
                user_id_to_reply = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°", reply_markup=get_back_keyboard("messages"))
                return
            
            user_data = get_user_by_id(user_id_to_reply)
            
            admin_sessions[user_id] = {
                "state": "authenticated",
                "action": "reply_to_user",
                "customer_id": user_id_to_reply
            }
            await query.edit_message_text(
                f"ðŸ“ Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñƒ {user_data['first_name'] if user_data else '#'}{user_id_to_reply}\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‚ÐµÐºÑÑ‚ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:",
                reply_markup=get_back_keyboard("messages")
            )
            return
        
        elif data == "messages_all_file":
            messages = get_all_messages(limit=1000)
            if not messages:
                await query.edit_message_text("ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ð¿Ð¾ÐºÐ¸ Ð½ÐµÐ¼Ð°Ñ”", reply_markup=get_back_keyboard("messages"))
                return
            file_data = generate_messages_report(messages, "txt")
            await query.message.reply_document(
                document=file_data,
                filename=f"all_messages_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ’¬ Ð’ÑÑ– Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²"
            )
            await query.edit_message_text("âœ… Ð¤Ð°Ð¹Ð» Ð· Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÐ¼Ð¸ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_back_keyboard("messages"))
            return
        
        elif data == "admin_customers":
            await query.edit_message_text("ðŸ‘¥ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_customers_menu())
            return
        
        elif data == "admin_customers_all":
            users = get_all_users()
            if not users:
                text = "ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð¸\n\nÐšÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
            else:
                text = f"ðŸ‘¥ Ð’Ð¡Ð† ÐšÐ›Ð†Ð„ÐÐ¢Ð˜\n\nÐ’ÑÑŒÐ¾Ð³Ð¾: {len(users)}\n\n"
                for user in users[:20]:
                    orders = get_user_orders(user['user_id'])
                    quick_orders = get_user_quick_orders(user['user_id'])
                    all_orders = orders + quick_orders
                    segment = get_customer_segment(user, all_orders)
                    created_at = user.get('created_at', '')
                    text += f"ID: {user['user_id']}\n"
                    text += f"Ð†Ð¼'Ñ: {user['first_name']} {user['last_name']}\n"
                    text += f"Username: @{user['username']}\n"
                    text += f"ðŸ“Š {segment}\n"
                    text += f"ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n"
                    text += f"{'â”€'*30}\n"
                if len(users) > 20:
                    text += f"... Ñ‚Ð° Ñ‰Ðµ {len(users) - 20} ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_customers_vip":
            users = get_all_users()
            text = "ðŸ‘‘ VIP ÐšÐ›Ð†Ð„ÐÐ¢Ð˜\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "VIP" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nÐ†Ð¼'Ñ: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\nðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n{'â”€'*30}\n"
            if count == 0:
                text = "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_customers_regular":
            users = get_all_users()
            text = "â­ ÐŸÐžÐ¡Ð¢Ð†Ð™ÐÐ† ÐšÐ›Ð†Ð„ÐÐ¢Ð˜\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¹" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nÐ†Ð¼'Ñ: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\nðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n{'â”€'*30}\n"
            if count == 0:
                text = "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ñ… ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_customers_new":
            users = get_all_users()
            text = "ðŸ†• ÐÐžÐ’Ð† ÐšÐ›Ð†Ð„ÐÐ¢Ð˜\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "ÐÐ¾Ð²Ð¸Ð¹" in segment:
                    count += 1
                    text += f"ID: {user['user_id']}\nÐ†Ð¼'Ñ: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\nðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n{'â”€'*30}\n"
            if count == 0:
                text = "ðŸ†• ÐÐ¾Ð²Ð¸Ñ… ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_customers_inactive":
            users = get_all_users()
            text = "ðŸ’¤ ÐÐ•ÐÐšÐ¢Ð˜Ð’ÐÐ† ÐšÐ›Ð†Ð„ÐÐ¢Ð˜\n\n"
            count = 0
            for user in users:
                orders = get_user_orders(user['user_id'])
                quick_orders = get_user_quick_orders(user['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user, all_orders)
                if "ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹" in segment:
                    count += 1
                    last_order_date = "ÐÐµÐ¼Ð°Ñ”"
                    if all_orders:
                        last_order = all_orders[0].get('created_at', '')
                        last_order_date = last_order[:16]
                    text += f"ID: {user['user_id']}\nÐ†Ð¼'Ñ: {user['first_name']} {user['last_name']}\nUsername: @{user['username']}\nÐžÑÑ‚Ð°Ð½Ð½Ñ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {last_order_date}\n{'â”€'*30}\n"
            if count == 0:
                text = "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ñ… ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "export_customers":
            users = get_all_users()
            if not users:
                await query.edit_message_text("âŒ ÐÐµÐ¼Ð°Ñ” ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð² Ð´Ð»Ñ ÐµÐºÑÐ¿Ð¾Ñ€Ñ‚Ñƒ", reply_markup=get_customers_menu())
                return
            
            file_data = generate_users_report(users)
            await query.message.reply_document(
                document=file_data,
                filename=f"customers_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ‘¥ ÐŸÐ¾Ð²Ð½Ð¸Ð¹ Ð·Ð²Ñ–Ñ‚ Ð¿Ð¾ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ñ…"
            )
            await query.edit_message_text("âœ… Ð¤Ð°Ð¹Ð» Ð· ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼Ð¸ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_customers_menu())
            return
        
        elif data == "admin_customer_search":
            admin_sessions[user_id] = {"state": "authenticated", "action": "search_customer_by_phone"}
            await query.edit_message_text("ðŸ” ÐŸÐ¾ÑˆÑƒÐº ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð·Ð° Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ:", reply_markup=get_back_keyboard("customers"))
            return
        
        elif data.startswith("customer_view_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", reply_markup=get_back_keyboard("customers"))
                return
            
            user = get_user_by_id(customer_id)
            if not user:
                await query.edit_message_text("âŒ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
                return
            orders = get_user_orders(customer_id)
            quick_orders = get_user_quick_orders(customer_id)
            messages = get_user_messages(customer_id)
            all_orders = orders + quick_orders
            segment = get_customer_segment(user, all_orders)
            
            text = f"ðŸ‘¤ ÐŸÐ ÐžÐ¤Ð†Ð›Ð¬ ÐšÐ›Ð†Ð„ÐÐ¢Ð\n\n"
            text += f"ID: {user['user_id']}\n"
            text += f"Ð†Ð¼'Ñ: {user['first_name']} {user['last_name']}\n"
            text += f"Username: @{user['username']}\n"
            text += f"ðŸ“… Ð ÐµÑ”ÑÑ‚Ñ€Ð°Ñ†Ñ–Ñ: {user.get('created_at', 'Ð/Ð”')[:16]}\n"
            text += f"ðŸ“Š Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚: {segment}\n\n"
            
            if all_orders:
                total_spent = sum(o.get('total', 0) for o in orders)
                text += f"ðŸ“¦ Ð’ÑÑŒÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n"
                text += f"ðŸ’° Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°: {total_spent:.2f} Ð³Ñ€Ð½\n"
                if orders:
                    text += f"ðŸ’³ Ð¡ÐµÑ€ÐµÐ´Ð½Ñ–Ð¹ Ñ‡ÐµÐº: {total_spent/len(orders):.2f} Ð³Ñ€Ð½\n\n"
                
                text += "ðŸ†• ÐžÑÑ‚Ð°Ð½Ð½Ñ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:\n"
                last = all_orders[0]
                last_created = last.get('created_at', '')[:16]
                last_id = last.get('order_id', last.get('id', 'Ð/Ð”'))
                text += f"   â„–{last_id} Ð²Ñ–Ð´ {last_created}\n"
                text += f"   Ð¡ÑƒÐ¼Ð°: {last.get('total', 0):.2f} Ð³Ñ€Ð½\n"
                text += f"   Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {last.get('status', 'Ð½Ð¾Ð²Ðµ')}\n"
            else:
                text += "ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: 0\n"
            
            text += f"\nðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {len(messages)}"
            
            await query.edit_message_text(
                text,
                reply_markup=get_customer_actions_menu(customer_id),
                parse_mode='HTML'
            )
            return
        
        elif data.startswith("customer_orders_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", reply_markup=get_back_keyboard("customers"))
                return
            
            orders = get_user_orders(customer_id)
            quick_orders = get_user_quick_orders(customer_id)
            all_orders = orders + quick_orders
            
            if not all_orders:
                text = "ðŸ“‹ Ð†ÑÑ‚Ð¾Ñ€Ñ–Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ\n\nÐ£ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð½ÐµÐ¼Ð°Ñ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ."
            else:
                text = f"ðŸ“‹ Ð†Ð¡Ð¢ÐžÐ Ð†Ð¯ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐ¬\n\nÐ’ÑÑŒÐ¾Ð³Ð¾: {len(all_orders)}\n\n"
                for order in all_orders:
                    created_at = order.get('created_at', '')[:16]
                    order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
                    order_type = "âš¡" if order.get('order_type') == 'quick' else "ðŸ“¦"
                    text += f"{order_type} â„–{order_id} | {created_at}\n"
                    text += f"Ð¡ÑƒÐ¼Ð°: {order.get('total', 0):.2f} Ð³Ñ€Ð½\n"
                    text += f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order.get('status', 'Ð½Ð¾Ð²Ðµ')}\n"
                    if order.get('order_type') == 'quick' and order.get('message'):
                        text += f"ðŸ’¬ {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    text += f"{'â”€'*30}\n"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
        
        elif data.startswith("customer_messages_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", reply_markup=get_back_keyboard("customers"))
                return
            
            messages = get_user_messages(customer_id)
            
            if not messages:
                text = "ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ\n\nÐ£ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð½ÐµÐ¼Ð°Ñ” Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ."
            else:
                text = f"ðŸ’¬ ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯ ÐšÐ›Ð†Ð„ÐÐ¢Ð\n\n"
                for msg in messages[:10]:
                    created_at = msg.get('created_at', '')[:16]
                    text += f"ðŸ“… {created_at}\n"
                    text += f"ðŸ“ {msg['text']}\n"
                    text += f"{'â”€'*30}\n"
            
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
        
        elif data.startswith("customer_message_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", reply_markup=get_back_keyboard("customers"))
                return
            
            admin_sessions[user_id] = {"state": "authenticated", "action": "send_message_to_customer", "customer_id": customer_id}
            await query.edit_message_text("ðŸ“¢ ÐÐ°Ð´Ñ–ÑÐ»Ð°Ñ‚Ð¸ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñƒ\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‚ÐµÐºÑÑ‚ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:", reply_markup=get_back_keyboard(f"customer_view_{customer_id}"))
            return
        
        elif data.startswith("customer_make_admin_"):
            parts = data.split("_")
            if len(parts) < 4:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("customers"))
                return
            
            try:
                customer_id = int(parts[3])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°", reply_markup=get_back_keyboard("customers"))
                return
            
            user = get_user_by_id(customer_id)
            if user:
                if add_admin(customer_id, user['username'], user_id):
                    text = f"âœ… ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° {user['first_name']} Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð´Ð¾ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²!"
                else:
                    text = "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ– Ð°Ð´Ð¼Ñ–Ð½Ð°"
            else:
                text = "âŒ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data=f"customer_view_{customer_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
        
        elif data == "admin_broadcast":
            await query.edit_message_text("ðŸ“¢ Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ†Ñ–Ð»ÑŒÐ¾Ð²Ñƒ Ð°ÑƒÐ´Ð¸Ñ‚Ð¾Ñ€Ñ–ÑŽ:", reply_markup=get_broadcast_menu())
            return
        
        elif data.startswith("broadcast_"):
            segment = data.replace("broadcast_", "")
            admin_sessions[user_id] = {"state": "authenticated", "action": "broadcast", "segment": segment}
            await query.edit_message_text(f"ðŸ“¢ Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð´Ð»Ñ ÑÐµÐ³Ð¼ÐµÐ½Ñ‚Ñƒ: {segment}\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‚ÐµÐºÑÑ‚ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸:", reply_markup=get_broadcast_input_back_keyboard())
            return
        
        elif data == "admin_reports":
            await query.edit_message_text("ðŸ“ Ð“ÐµÐ½ÐµÑ€Ð°Ñ†Ñ–Ñ Ð·Ð²Ñ–Ñ‚Ñ–Ð²\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ‚Ð¸Ð¿ Ð·Ð²Ñ–Ñ‚Ñƒ Ñ‚Ð° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚:", reply_markup=get_reports_menu())
            return
        
        elif data == "report_orders_txt":
            orders = get_all_orders(include_quick=True)
            report_data = generate_orders_report(orders, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ“‹ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_orders_csv":
            orders = get_all_orders(include_quick=True)
            report_data = generate_orders_report(orders, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="ðŸ“‹ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ… (CSV)"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_users_txt":
            users = get_all_users()
            report_data = generate_users_report(users)
            await query.message.reply_document(
                document=report_data,
                filename=f"users_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ‘¥ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ñ…"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_users_csv":
            await query.edit_message_text("Ð¤ÑƒÐ½ÐºÑ†Ñ–Ñ Ð² Ñ€Ð¾Ð·Ñ€Ð¾Ð±Ñ†Ñ–, Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð¾Ð²ÑƒÐ¹Ñ‚Ðµ TXT Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚", reply_markup=get_reports_menu())
            return
        
        elif data == "report_quick_txt":
            orders = get_quick_orders()
            report_data = generate_quick_orders_report(orders, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"quick_orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="âš¡ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ…"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_quick_csv":
            orders = get_quick_orders()
            report_data = generate_quick_orders_report(orders, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"quick_orders_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="âš¡ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÑ… (CSV)"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_messages_txt":
            messages = get_all_messages(limit=1000)
            report_data = generate_messages_report(messages, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"messages_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ’¬ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÑ…"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_messages_csv":
            messages = get_all_messages(limit=1000)
            report_data = generate_messages_report(messages, "csv")
            await query.message.reply_document(
                document=report_data,
                filename=f"messages_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="ðŸ’¬ Ð—Ð²Ñ–Ñ‚ Ð¿Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½ÑÑ… (CSV)"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "report_stats_txt":
            stats = get_statistics()
            report_data = generate_stats_report(stats, "txt")
            await query.message.reply_document(
                document=report_data,
                filename=f"stats_report_{get_kyiv_time().strftime('%Y%m%d_%H%M%S')}.txt",
                caption="ðŸ“Š Ð¡Ñ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ°"
            )
            await query.edit_message_text("âœ… Ð—Ð²Ñ–Ñ‚ Ð·Ð³ÐµÐ½ÐµÑ€Ð¾Ð²Ð°Ð½Ð¾!", reply_markup=get_reports_menu())
            return
        
        elif data == "admin_manage_admins":
            await query.edit_message_text("ðŸ‘‘ ÐšÐµÑ€ÑƒÐ²Ð°Ð½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°Ð¼Ð¸\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð´Ñ–ÑŽ:", reply_markup=get_admins_menu())
            return
        
        elif data == "admin_list":
            admins = get_all_admins()
            if not admins:
                text = "ðŸ“‹ Ð¡Ð¿Ð¸ÑÐ¾Ðº Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²\n\nÐÐ´Ð¼Ñ–Ð½Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
            else:
                text = "ðŸ“‹ Ð¡ÐŸÐ˜Ð¡ÐžÐš ÐÐ”ÐœÐ†ÐÐ†Ð¡Ð¢Ð ÐÐ¢ÐžÐ Ð†Ð’\n\n"
                for admin in admins:
                    added_at = admin.get('added_at', '')[:16]
                    text += f"ID: {admin['user_id']}\nUsername: @{admin['username']}\nÐ”Ð¾Ð´Ð°Ð½Ð¾: {added_at}\n{'â”€'*30}\n"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_main")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_add":
            admin_sessions[user_id] = {"state": "authenticated", "action": "add_admin"}
            await query.edit_message_text("âž• Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Telegram ID ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°:", reply_markup=get_back_keyboard("main"))
            return
        
        elif data == "admin_remove":
            admins = get_all_admins()
            if not admins:
                await query.edit_message_text("âŒ ÐÐ´Ð¼Ñ–Ð½Ñ–Ð² Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_admins_menu())
                return
            keyboard = []
            for admin in admins:
                if admin['user_id'] != user_id:
                    keyboard.append([InlineKeyboardButton(f"âŒ {admin['user_id']} - @{admin['username']}", callback_data=f"remove_admin_{admin['user_id']}")])
            keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_main")])
            await query.edit_message_text("ðŸ—‘ Ð’Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð°Ð´Ð¼Ñ–Ð½Ð° Ð´Ð»Ñ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data.startswith("remove_admin_"):
            parts = data.split("_")
            if len(parts) < 3:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ Ð´Ð°Ð½Ð¸Ñ…", reply_markup=get_back_keyboard("main"))
                return
            
            try:
                admin_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð½ÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ ID Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°", reply_markup=get_back_keyboard("main"))
                return
            
            if admin_id == user_id:
                text = "âŒ ÐÐµ Ð¼Ð¾Ð¶Ð½Ð° Ð²Ð¸Ð´Ð°Ð»Ð¸Ñ‚Ð¸ ÑÐ°Ð¼Ð¾Ð³Ð¾ ÑÐµÐ±Ðµ!"
            elif remove_admin(admin_id):
                text = "âœ… ÐÐ´Ð¼Ñ–Ð½Ð° ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾!"
            else:
                text = "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ– Ð°Ð´Ð¼Ñ–Ð½Ð°"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_main")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_stats":
            stats = get_statistics()
            text = "ðŸ“Š Ð¡Ð¢ÐÐ¢Ð˜Ð¡Ð¢Ð˜ÐšÐ\n\n"
            text += f"ðŸ“‹ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('total_orders', 0)}\n"
            text += f"ðŸ’° Ð’Ð¸Ñ€ÑƒÑ‡ÐºÐ°: {stats.get('total_revenue', 0):.2f} Ð³Ñ€Ð½\n"
            text += f"ðŸ’³ Ð¡ÐµÑ€ÐµÐ´Ð½Ñ–Ð¹ Ñ‡ÐµÐº: {stats.get('avg_check', 0):.2f} Ð³Ñ€Ð½\n"
            text += f"ðŸ‘¥ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²: {stats.get('total_users', 0)}\n"
            text += f"âš¡ Ð¨Ð²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('total_quick_orders', 0)}\n"
            text += f"ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {stats.get('total_messages', 0)}\n\n"
            text += "ðŸ“Š Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð° Ð¾ÑÑ‚Ð°Ð½Ð½Ñ– 30 Ð´Ð½Ñ–Ð²:\n"
            text += f"   ÐšÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ: {stats.get('last_30_days_orders', 0)}\n"
            text += f"   Ð¡ÑƒÐ¼Ð°: {stats.get('last_30_days_revenue', 0):.2f} Ð³Ñ€Ð½\n\n"
            text += "ðŸ“Š Ð¡Ñ‚Ð°Ñ‚ÑƒÑÐ¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ:\n"
            for status, count in stats.get('orders_by_status', {}).items():
                text += f"   â€¢ {status}: {count}\n"
            text += "\nðŸ‘¥ Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚Ð°Ñ†Ñ–Ñ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²:\n"
            segments = stats.get('segments', {})
            text += f"   ðŸ‘‘ VIP: {segments.get('vip', 0)}\n"
            text += f"   â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ñ–: {segments.get('regular', 0)}\n"
            text += f"   ðŸ†• ÐÐ¾Ð²Ñ–: {segments.get('new', 0)}\n"
            text += f"   ðŸ“Š ÐÐºÑ‚Ð¸Ð²Ð½Ñ–: {segments.get('active', 0)}\n"
            text += f"   ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ñ–: {segments.get('inactive', 0)}\n"
            keyboard = [[InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_main")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        elif data == "admin_settings":
            await query.edit_message_text("âš™ï¸ ÐÐ°Ð»Ð°ÑˆÑ‚ÑƒÐ²Ð°Ð½Ð½Ñ\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ñ€Ð¾Ð·Ð´Ñ–Ð»:", reply_markup=get_settings_menu())
            return
        
        elif data == "admin_settings_password":
            admin_sessions[user_id] = {"state": "authenticated", "action": "change_password"}
            await query.edit_message_text("ðŸ”‘ Ð—Ð¼Ñ–Ð½Ð° Ð¿Ð°Ñ€Ð¾Ð»Ñ\n\nÐ’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð²Ð¸Ð¹ Ð¿Ð°Ñ€Ð¾Ð»ÑŒ:", reply_markup=get_back_keyboard("main"))
            return
        
        else:
            logger.warning(f"âš ï¸ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ð¸Ð¹ callback: {data}")
            await query.edit_message_text("âŒ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ð° ÐºÐ¾Ð¼Ð°Ð½Ð´Ð°", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² button_handler: {e}")
        logger.error(traceback.format_exc())
        try:
            await query.edit_message_text(
                "âŒ Ð¡Ñ‚Ð°Ð»Ð°ÑÑ Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°. ÐŸÐ¾Ð²ÐµÑ€Ñ‚Ð°Ñ”Ð¼Ð¾ÑÑŒ Ð´Ð¾ Ð³Ð¾Ð»Ð¾Ð²Ð½Ð¾Ð³Ð¾ Ð¼ÐµÐ½ÑŽ.",
                reply_markup=get_main_menu()
            )
        except:
            pass

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð½Ð¸Ðº Ñ‚ÐµÐºÑÑ‚Ð¾Ð²Ð¸Ñ… Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ Ñ‚Ð° Ñ„Ð¾Ñ‚Ð¾"""
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip() if update.message.text else ""
        
        logger.info(f"ðŸ“ ÐÐ´Ð¼Ñ–Ð½ {user_id}: {text[:50] if text else '[Ð¤Ð¾Ñ‚Ð¾]'}...")
        
        if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
            await check_password(update, context)
            return
        
        if not is_authenticated(user_id):
            return
        
        session = admin_sessions.get(user_id, {})
        action = session.get("action")
        logger.info(f"ðŸ“Œ ÐŸÐ¾Ñ‚Ð¾Ñ‡Ð½Ð¸Ð¹ action: {action}, session: {session}")
        
        # ========== ÐžÐ‘Ð ÐžÐ‘ÐšÐ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ ÐšÐžÐœÐŸÐÐÐ†Ð‡ ==========
        
        if action == "edit_company_text":
            logger.debug(f"ÐžÐ½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ‚ÐµÐºÑÑ‚Ñƒ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–Ñ—: Ð´Ð¾Ð²Ð¶Ð¸Ð½Ð° {len(text)}")
            if update_company_info(text, user_id):
                await update.message.reply_text(
                    "âœ… Ð¢ÐµÐºÑÑ‚ 'ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ' ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!",
                    reply_markup=get_company_edit_menu()
                )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ñ‚ÐµÐºÑÑ‚Ñƒ",
                    reply_markup=get_company_edit_menu()
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ========== ÐžÐ‘Ð ÐžÐ‘ÐšÐ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ Ð’Ð†Ð¢ÐÐÐÐ¯ ==========
        
        if action == "edit_welcome_text":
            logger.debug(f"ÐžÐ½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ð¾Ð³Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: Ð´Ð¾Ð²Ð¶Ð¸Ð½Ð° {len(text)}")
            if update_welcome_message(text, user_id):
                await update.message.reply_text(
                    "âœ… Ð’Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!",
                    reply_markup=get_welcome_edit_menu()
                )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ð²Ñ–Ñ‚Ð°Ð½Ð½Ñ",
                    reply_markup=get_welcome_edit_menu()
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        # ========== ÐÐžÐ’Ð† ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ Ð”ÐžÐ”ÐÐ’ÐÐÐÐ¯ Ð¢Ð Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ FAQ ==========
        
        elif action == "faq_edit_add_question":
            logger.debug(f"Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ FAQ: Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ: {text[:30]}...")
            admin_sessions[user_id]["faq_question"] = text
            admin_sessions[user_id]["action"] = "faq_edit_add_answer"
            await update.message.reply_text(
                "ðŸ“ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ <b>Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ</b> Ð½Ð° Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ:",
                reply_markup=get_back_keyboard("faq_edit_main"),
                parse_mode='HTML'
            )
            return
        
        elif action == "faq_edit_add_answer":
            question = session.get("faq_question")
            answer = text
            logger.debug(f"Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ FAQ: Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ: {question[:30]}..., Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ: {answer[:30]}...")
            faq_id = add_faq(question, answer)
            if faq_id:
                await update.message.reply_text(
                    f"âœ… FAQ Ð´Ð¾Ð´Ð°Ð½Ð¾! ID: {faq_id}",
                    reply_markup=get_faq_edit_main_menu()
                )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ– FAQ",
                    reply_markup=get_faq_edit_main_menu()
                )
            admin_sessions[user_id].pop("action", None)
            if "faq_question" in admin_sessions[user_id]:
                admin_sessions[user_id].pop("faq_question")
            return
        
        elif action == "faq_edit_update_question":
            faq_id = session.get("faq_id")
            if not faq_id:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: ID FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            faq = get_faq_by_id(faq_id)
            if not faq:
                await update.message.reply_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_faq(faq_id, text, faq['answer']):
                await update.message.reply_text(
                    f"âœ… ÐŸÐ¸Ñ‚Ð°Ð½Ð½Ñ FAQ #{faq_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!",
                    reply_markup=get_faq_edit_actions_keyboard(faq_id)
                )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ–",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}")
                )
            
            admin_sessions[user_id].pop("action", None)
            admin_sessions[user_id].pop("faq_id", None)
            return
        
        elif action == "faq_edit_update_answer":
            faq_id = session.get("faq_id")
            if not faq_id:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: ID FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            faq = get_faq_by_id(faq_id)
            if not faq:
                await update.message.reply_text("âŒ FAQ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("faq_edit_main"))
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_faq(faq_id, faq['question'], text):
                await update.message.reply_text(
                    f"âœ… Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ FAQ #{faq_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!",
                    reply_markup=get_faq_edit_actions_keyboard(faq_id)
                )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ–",
                    reply_markup=get_back_keyboard(f"faq_edit_select_{faq_id}")
                )
            
            admin_sessions[user_id].pop("action", None)
            admin_sessions[user_id].pop("faq_id", None)
            return
        
        # ========== ÐžÐ‘Ð ÐžÐ‘ÐšÐ Ð”ÐžÐ”ÐÐ’ÐÐÐÐ¯ Ð¢ÐžÐ’ÐÐ Ð£ ==========
        
        elif action == "add_product_name":
            admin_sessions[user_id]["product_name"] = text
            admin_sessions[user_id]["action"] = "add_product_price"
            await update.message.reply_text("Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ†Ñ–Ð½Ñƒ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ (Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ñ‡Ð¸ÑÐ»Ð¾):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_price":
            try:
                price = float(text.replace(",", "."))
                admin_sessions[user_id]["product_price"] = price
                admin_sessions[user_id]["action"] = "add_product_category"
                await update.message.reply_text("Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ ÐºÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–ÑŽ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ:", reply_markup=get_back_keyboard("products"))
            except ValueError:
                await update.message.reply_text("âŒ ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚. Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‡Ð¸ÑÐ»Ð¾ (Ð½Ð°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: 250):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_category":
            admin_sessions[user_id]["product_category"] = text
            admin_sessions[user_id]["action"] = "add_product_description"
            await update.message.reply_text("Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð¾Ð¿Ð¸Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ:", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_description":
            admin_sessions[user_id]["product_description"] = text
            admin_sessions[user_id]["action"] = "add_product_unit"
            await update.message.reply_text("Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð¾Ð´Ð¸Ð½Ð¸Ñ†ÑŽ Ð²Ð¸Ð¼Ñ–Ñ€Ñƒ (Ð½Ð°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: Ð±Ð°Ð½ÐºÐ°, ÐºÐ³, ÑˆÑ‚):", reply_markup=get_back_keyboard("products"))
            return
        
        elif action == "add_product_unit":
            admin_sessions[user_id]["product_unit"] = text
            admin_sessions[user_id]["action"] = "add_product_details"
            await update.message.reply_text("Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð´ÐµÑ‚Ð°Ð»Ñ– Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ (Ð¾Ð±'Ñ”Ð¼, Ð²Ð°Ð³Ð°, ÑÐºÐ»Ð°Ð´ Ñ‚Ð¾Ñ‰Ð¾):", reply_markup=get_back_keyboard("products"))
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
                    f"âœ… Ð¢Ð¾Ð²Ð°Ñ€ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð´Ð¾Ð´Ð°Ð½Ð¾!\n\nID: {product_id}\nÐÐ°Ð·Ð²Ð°: {product_data['name']}\nÐ¦Ñ–Ð½Ð°: {product_data['price']} Ð³Ñ€Ð½\nÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ–: {product_data['unit']}",
                    reply_markup=get_products_menu()
                )
            else:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ– Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
            
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð”Ð›Ð¯ Ð Ð•Ð”ÐÐ“Ð£Ð’ÐÐÐÐ¯ Ð¢ÐžÐ’ÐÐ Ð£ ==============
        
        elif action == "edit_product_unit":
            product_id = session.get("product_id")
            if update_product(product_id, unit=text):
                await update.message.reply_text(f"âœ… ÐžÐ´Ð¸Ð½Ð¸Ñ†Ñ– Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!", reply_markup=get_products_menu())
            else:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ð¾Ð´Ð¸Ð½Ð¸Ñ†ÑŒ", reply_markup=get_products_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_image_url":
            product_id = session.get("product_id")
            logger.info(f"ðŸ“ ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð´Ð»Ñ edit_product_image_url, product_id: {product_id}, Ñ‚ÐµÐºÑÑ‚: {text}")
            
            if not product_id:
                logger.error("âŒ product_id Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð² ÑÐµÑÑ–Ñ—!")
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾. Ð¡Ð¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð·.", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            # Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑ”Ð¼Ð¾ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð° URL
            image_bytes = await download_image_from_url_to_bytes(text)
            
            if image_bytes:
                # ÐžÐ½Ð¾Ð²Ð»ÑŽÑ”Ð¼Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€ Ð² Ð‘Ð” - Ð·Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ Ð±Ð°Ð¹Ñ‚Ð¸
                if update_product(product_id, image_data=image_bytes):
                    await update.message.reply_text(
                        f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾! (Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð¾ Ð² Ð‘Ð”)", 
                        reply_markup=get_products_menu()
                    )
                else:
                    await update.message.reply_text(
                        "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾ Ð² Ð±Ð°Ð·Ñ– Ð´Ð°Ð½Ð¸Ñ…", 
                        reply_markup=get_products_menu()
                    )
            else:
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ– Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð° URL. ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€Ñ‚Ðµ Ð¿Ð¾ÑÐ¸Ð»Ð°Ð½Ð½Ñ Ñ‚Ð° ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð·.", 
                    reply_markup=get_products_menu()
                )
            
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "edit_product_image_file":
            product_id = session.get("product_id")
            logger.info(f"ðŸ“ ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ Ñ„Ð¾Ñ‚Ð¾ Ð´Ð»Ñ edit_product_image_file, product_id: {product_id}")
            
            if not product_id:
                logger.error("âŒ product_id Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð² ÑÐµÑÑ–Ñ—!")
                await update.message.reply_text(
                    "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾. Ð¡Ð¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð·.", 
                    reply_markup=get_products_menu()
                )
                admin_sessions[user_id].pop("action", None)
                return
            
            if update.message.photo:
                file_id = update.message.photo[-1].file_id
                logger.info(f"ðŸ“¸ ÐžÑ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ file_id: {file_id}")
                
                # Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑ”Ð¼Ð¾ Ñ„Ð¾Ñ‚Ð¾ Ð² Ð¿Ð°Ð¼'ÑÑ‚ÑŒ ÑÐº Ð±Ð°Ð¹Ñ‚Ð¸
                image_bytes = await download_telegram_file_to_bytes(file_id, context.bot)
                
                if image_bytes:
                    # ÐžÐ½Ð¾Ð²Ð»ÑŽÑ”Ð¼Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€ Ð² Ð‘Ð” - Ð·Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ Ð±Ð°Ð¹Ñ‚Ð¸
                    if update_product(product_id, image_data=image_bytes):
                        await update.message.reply_text(
                            f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾! (Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð¾ Ð² Ð‘Ð”)", 
                            reply_markup=get_products_menu()
                        )
                    else:
                        await update.message.reply_text(
                            "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾ Ð² Ð±Ð°Ð·Ñ– Ð´Ð°Ð½Ð¸Ñ…", 
                            reply_markup=get_products_menu()
                        )
                else:
                    await update.message.reply_text(
                        "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾", 
                        reply_markup=get_products_menu()
                    )
            else:
                await update.message.reply_text(
                    "âŒ Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, Ð½Ð°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ñ„Ð¾Ñ‚Ð¾", 
                    reply_markup=get_back_keyboard("products")
                )
                return
            
            admin_sessions[user_id].pop("action", None)
            return
        
        # ============== Ð†ÐÐ¨Ð† ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ ==============
        
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
                    await update.message.reply_text("âŒ ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚. Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‡Ð¸ÑÐ»Ð¾:", reply_markup=get_back_keyboard("products"))
                    return
            elif field == "desc":
                update_data["description"] = text
            elif field == "cat":
                update_data["category"] = text
            else:
                await update.message.reply_text("âŒ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ðµ Ð¿Ð¾Ð»Ðµ Ð´Ð»Ñ Ñ€ÐµÐ´Ð°Ð³ÑƒÐ²Ð°Ð½Ð½Ñ", reply_markup=get_products_menu())
                admin_sessions[user_id].pop("action", None)
                return
            
            if update_product(product_id, **update_data):
                await update.message.reply_text(f"âœ… Ð¢Ð¾Ð²Ð°Ñ€ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾!", reply_markup=get_products_menu())
            else:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ– Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ", reply_markup=get_products_menu())
            
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "search_orders_by_phone":
            orders = get_orders_by_phone(text)
            if not orders:
                await update.message.reply_text(f"âŒ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ Ð·Ð° Ð½Ð¾Ð¼ÐµÑ€Ð¾Ð¼ {text} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_orders_menu())
            else:
                response = f"ðŸ“‹ Ð—Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(orders)}\n\n"
                for order in orders[:5]:
                    created_at = order.get('created_at', '')[:16]
                    order_id = order.get('order_id', order.get('id', 'Ð/Ð”'))
                    response += f"â„–{order_id} | {created_at}\n"
                    response += f"Ð¡ÑƒÐ¼Ð°: {order.get('total', 0):.2f} Ð³Ñ€Ð½\n"
                    response += f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order.get('status', 'Ð½Ð¾Ð²Ðµ')}\n"
                    if order.get('order_type') == 'quick' and order.get('message'):
                        response += f"ðŸ’¬ {order['message'][:50]}{'...' if len(order['message']) > 50 else ''}\n"
                    response += f"{'â”€'*30}\n"
                keyboard = []
                for order in orders[:10]:
                    order_id = order.get('order_id', order.get('id', 0))
                    order_type = order.get('order_type', 'regular')
                    keyboard.append([InlineKeyboardButton(f"ðŸ“¦ â„–{order_id}", callback_data=f"order_view_{order_id}_{order_type}")])
                keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_orders")])
                await update.message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "search_customer_by_phone":
            user_data = get_user_by_phone(text)
            if not user_data:
                await update.message.reply_text(f"âŒ ÐšÐ»Ñ–Ñ”Ð½Ñ‚Ð° Ð· Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ð¾Ð¼ {text} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_customers_menu())
            else:
                orders = get_user_orders(user_data['user_id'])
                quick_orders = get_user_quick_orders(user_data['user_id'])
                all_orders = orders + quick_orders
                segment = get_customer_segment(user_data, all_orders)
                
                response = f"ðŸ‘¤ ÐšÐ›Ð†Ð„ÐÐ¢ Ð—ÐÐÐ™Ð”Ð•ÐÐ˜Ð™\n\n"
                response += f"ID: {user_data['user_id']}\n"
                response += f"Ð†Ð¼'Ñ: {user_data['first_name']} {user_data['last_name']}\n"
                response += f"Username: @{user_data['username']}\n"
                response += f"ðŸ“… Ð ÐµÑ”ÑÑ‚Ñ€Ð°Ñ†Ñ–Ñ: {user_data.get('created_at', '')[:16]}\n"
                response += f"ðŸ“Š Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚: {segment}\n"
                response += f"ðŸ“¦ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {len(all_orders)}\n\n"
                
                if all_orders:
                    total = sum(o.get('total', 0) for o in orders)
                    response += f"ðŸ’° Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°: {total:.2f} Ð³Ñ€Ð½"
                
                keyboard = [[InlineKeyboardButton("ðŸ‘¤ ÐŸÐµÑ€ÐµÐ³Ð»ÑÐ½ÑƒÑ‚Ð¸ Ð¿Ñ€Ð¾Ñ„Ñ–Ð»ÑŒ", callback_data=f"customer_view_{user_data['user_id']}")]]
                keyboard.append([InlineKeyboardButton("ðŸ”™ ÐÐ°Ð·Ð°Ð´", callback_data="back_to_customers")])
                
                await update.message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "send_message_to_customer":
            customer_id = session.get("customer_id")
            try:
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"ðŸ“¢ <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð²Ñ–Ð´ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text("âœ… ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð½Ð°Ð´Ñ–ÑÐ»Ð°Ð½Ð¾!", reply_markup=get_customer_actions_menu(customer_id))
            except Exception as e:
                await update.message.reply_text(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð½Ð°Ð´ÑÐ¸Ð»Ð°Ð½Ð½Ñ–: {e}", reply_markup=get_customer_actions_menu(customer_id))
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "reply_to_order":
            customer_id = session.get("user_id")
            order_id = session.get("order_id")
            try:
                main_bot = Bot(token=MAIN_BOT_TOKEN)
                
                await main_bot.send_message(
                    chat_id=customer_id,
                    text=f"ðŸ“¢ <b>Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ Ð½Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id}</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text(
                    f"âœ… Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ Ð½Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ â„–{order_id} Ð½Ð°Ð´Ñ–ÑÐ»Ð°Ð½Ð¾!",
                    reply_markup=get_order_actions_menu(order_id, session.get("order_type", 'regular'))
                )
            except Exception as e:
                await update.message.reply_text(
                    f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð½Ð°Ð´ÑÐ¸Ð»Ð°Ð½Ð½Ñ–: {e}",
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
                    text=f"ðŸ“¢ <b>Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°</b>\n\n{text}",
                    parse_mode='HTML'
                )
                await update.message.reply_text(
                    "âœ… Ð’Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´ÑŒ Ð½Ð°Ð´Ñ–ÑÐ»Ð°Ð½Ð¾!",
                    reply_markup=get_customer_actions_menu(customer_id)
                )
            except Exception as e:
                await update.message.reply_text(
                    f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð½Ð°Ð´ÑÐ¸Ð»Ð°Ð½Ð½Ñ–: {e}",
                    reply_markup=get_customer_actions_menu(customer_id)
                )
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "broadcast":
            segment = session.get("segment")
            
            await update.message.reply_text(f"ðŸ“¢ Ð Ð¾Ð·Ð¿Ð¾Ñ‡Ð¸Ð½Ð°ÑŽ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÑƒ...")
            
            admin_bot = Bot(token=TOKEN)
            
            if segment == "all":
                sent, failed = await send_broadcast_to_all(admin_bot, text, admin_user_id=user_id)
                segment_name = "Ð’Ð¡Ð†Ðœ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ð¼"
            elif segment == "vip":
                sent, failed = await send_broadcast_to_segment(admin_bot, "vip", text, admin_user_id=user_id)
                segment_name = "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼"
            elif segment == "regular":
                sent, failed = await send_broadcast_to_segment(admin_bot, "regular", text, admin_user_id=user_id)
                segment_name = "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼"
            elif segment == "new":
                sent, failed = await send_broadcast_to_segment(admin_bot, "new", text, admin_user_id=user_id)
                segment_name = "ðŸ†• ÐÐ¾Ð²Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼"
            elif segment == "inactive":
                sent, failed = await send_broadcast_to_segment(admin_bot, "inactive", text, admin_user_id=user_id)
                segment_name = "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ð°Ð¼"
            else:
                sent, failed = 0, 0
                segment_name = segment
            
            await update.message.reply_text(
                f"âœ… <b>Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð·Ð°Ð²ÐµÑ€ÑˆÐµÐ½Ð°!</b>\n\n"
                f"ðŸ“¢ Ð¡ÐµÐ³Ð¼ÐµÐ½Ñ‚: {segment_name}\n"
                f"âœ“ Ð”Ð¾ÑÑ‚Ð°Ð²Ð»ÐµÐ½Ð¾: {sent}\n"
                f"âœ— ÐŸÐ¾Ð¼Ð¸Ð»Ð¾Ðº: {failed}\n\n"
                f"<i>Ð”ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¸Ð¹ Ð·Ð²Ñ–Ñ‚ Ñƒ Ð»Ð¾Ð³Ð°Ñ…</i>",
                reply_markup=get_broadcast_menu(),
                parse_mode='HTML'
            )
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "change_password":
            global ADMIN_PASSWORD
            ADMIN_PASSWORD = text
            logger.info(f"ðŸ”‘ ÐŸÐ°Ñ€Ð¾Ð»ÑŒ Ð·Ð¼Ñ–Ð½ÐµÐ½Ð¾ Ð°Ð´Ð¼Ñ–Ð½Ð¾Ð¼ {user_id}")
            await update.message.reply_text("âœ… ÐŸÐ°Ñ€Ð¾Ð»ÑŒ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð·Ð¼Ñ–Ð½ÐµÐ½Ð¾!", reply_markup=get_settings_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        elif action == "add_admin":
            try:
                new_admin_id = int(text)
                # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð°Ð´Ð¼Ñ–Ð½Ð° Ð² Ð‘Ð”
                if add_admin(new_admin_id, "", user_id):
                    await update.message.reply_text(
                        f"âœ… ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð° Ð· ID {new_admin_id} Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð´Ð¾ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²!\n\n"
                        f"Ð¢ÐµÐ¿ÐµÑ€ Ð²Ñ–Ð½ Ð¼Ð¾Ð¶Ðµ ÑƒÐ²Ñ–Ð¹Ñ‚Ð¸ Ð² Ð°Ð´Ð¼Ñ–Ð½-Ð±Ð¾Ñ‚ Ð·Ð° Ð¿Ð°Ñ€Ð¾Ð»ÐµÐ¼.",
                        reply_markup=get_admins_menu()
                    )
                else:
                    await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ– Ð°Ð´Ð¼Ñ–Ð½Ð°", reply_markup=get_admins_menu())
            except ValueError:
                await update.message.reply_text("âŒ Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ ÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ Ñ‡Ð¸ÑÐ»Ð¾Ð²Ð¸Ð¹ ID", reply_markup=get_admins_menu())
            admin_sessions[user_id].pop("action", None)
            return
        
        else:
            logger.warning(f"âŒ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ð° ÐºÐ¾Ð¼Ð°Ð½Ð´Ð° Ð²Ñ–Ð´ Ð°Ð´Ð¼Ñ–Ð½Ð° {user_id}: {action}")
            await update.message.reply_text("âŒ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ð° ÐºÐ¾Ð¼Ð°Ð½Ð´Ð°", reply_markup=get_main_menu())
            
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² message_handler: {e}")
        logger.error(traceback.format_exc())
        await update.message.reply_text(
            "âŒ Ð¡Ñ‚Ð°Ð»Ð°ÑÑ Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°. ÐŸÐ¾Ð²ÐµÑ€Ñ‚Ð°Ñ”Ð¼Ð¾ÑÑŒ Ð´Ð¾ Ð³Ð¾Ð»Ð¾Ð²Ð½Ð¾Ð³Ð¾ Ð¼ÐµÐ½ÑŽ.",
            reply_markup=get_main_menu()
        )

async def send_broadcast_to_all(admin_bot: Bot, message: str, admin_user_id: int = None):
    """Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð²ÑÑ–Ð¼ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ð¼"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("ÐÐµÐ¼Ð°Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð² Ð´Ð»Ñ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸")
        if admin_user_id:
            try:
                await admin_bot.send_message(
                    chat_id=admin_user_id,
                    text="âš ï¸ ÐÐµÐ¼Ð°Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð² Ð´Ð»Ñ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸",
                    parse_mode='HTML'
                )
            except:
                pass
        return 0, 0
    
    if admin_user_id:
        try:
            await admin_bot.send_message(
                chat_id=admin_user_id,
                text=f"ðŸ“¢ <b>Ð Ð¾Ð·Ð¿Ð¾Ñ‡Ð°Ñ‚Ð¾ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÑƒ Ð’Ð¡Ð†Ðœ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ð¼</b>\n\nðŸ‘¥ Ð’ÑÑŒÐ¾Ð³Ð¾: {len(users)}",
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
                        text=f"ðŸ“¢ <b>ÐŸÑ€Ð¾Ð³Ñ€ÐµÑ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸:</b> {i + 1}/{len(users)} (âœ“ {sent_count} | âœ— {fail_count})",
                        parse_mode='HTML'
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
        except Exception as e:
            error_str = str(e)
            if "Chat not found" in error_str or "bot was blocked" in error_str:
                logger.warning(f"âš ï¸ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ {user['user_id']} Ð·Ð°Ð±Ð»Ð¾ÐºÑƒÐ²Ð°Ð² Ð±Ð¾Ñ‚Ð° Ð°Ð±Ð¾ Ð½ÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹")
            else:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñƒ {user['user_id']}: {e}")
            fail_count += 1
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["failed"] = fail_count
    
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

async def send_broadcast_to_segment(admin_bot: Bot, segment: str, message: str, admin_user_id: int = None):
    """Ð Ð¾Ð·ÑÐ¸Ð»ÐºÐ° Ð·Ð° ÑÐµÐ³Ð¼ÐµÐ½Ñ‚Ð¾Ð¼ ÐºÐ»Ñ–Ñ”Ð½Ñ‚Ñ–Ð²"""
    users = get_all_users()
    sent_count = 0
    fail_count = 0
    
    if not users:
        logger.warning("ÐÐµÐ¼Ð°Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð² Ð´Ð»Ñ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸")
        return 0, 0
    
    filtered_users = []
    segment_map = {
        "vip": "ðŸ‘‘ VIP ÐºÐ»Ñ–Ñ”Ð½Ñ‚",
        "regular": "â­ ÐŸÐ¾ÑÑ‚Ñ–Ð¹Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚",
        "new": "ðŸ†• ÐÐ¾Ð²Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚",
        "inactive": "ðŸ’¤ ÐÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚",
        "active": "ðŸ“Š ÐÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹ ÐºÐ»Ñ–Ñ”Ð½Ñ‚"
    }
    
    for user in users:
        user_orders = get_user_orders(user['user_id'])
        quick_orders = get_user_quick_orders(user['user_id'])
        all_orders = user_orders + quick_orders
        user_segment = get_customer_segment(user, all_orders)
        
        if segment in user_segment or (segment == "new" and "ÐÐ¾Ð²Ð¸Ð¹" in user_segment):
            filtered_users.append(user)
    
    if admin_user_id:
        try:
            segment_name = segment_map.get(segment, segment)
            await admin_bot.send_message(
                chat_id=admin_user_id,
                text=f"ðŸ“¢ <b>Ð Ð¾Ð·Ð¿Ð¾Ñ‡Ð°Ñ‚Ð¾ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÑƒ Ð´Ð»Ñ {segment_name}</b>\n\nðŸ‘¥ Ð’ÑÑŒÐ¾Ð³Ð¾: {len(filtered_users)}",
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
                        text=f"ðŸ“¢ <b>ÐŸÑ€Ð¾Ð³Ñ€ÐµÑ Ñ€Ð¾Ð·ÑÐ¸Ð»ÐºÐ¸:</b> {i + 1}/{len(filtered_users)} (âœ“ {sent_count} | âœ— {fail_count})",
                        parse_mode='HTML'
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
        except Exception as e:
            error_str = str(e)
            if "Chat not found" in error_str or "bot was blocked" in error_str:
                logger.warning(f"âš ï¸ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ {user['user_id']} Ð·Ð°Ð±Ð»Ð¾ÐºÑƒÐ²Ð°Ð² Ð±Ð¾Ñ‚Ð° Ð°Ð±Ð¾ Ð½ÐµÐ°ÐºÑ‚Ð¸Ð²Ð½Ð¸Ð¹")
            else:
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñƒ {user['user_id']}: {e}")
            fail_count += 1
            if admin_user_id and admin_user_id in broadcast_in_progress:
                broadcast_in_progress[admin_user_id]["failed"] = fail_count
    
    if admin_user_id and admin_user_id in broadcast_in_progress:
        del broadcast_in_progress[admin_user_id]
    
    return sent_count, fail_count

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð½Ð¸Ðº Ð¿Ð¾Ð¼Ð¸Ð»Ð¾Ðº"""
    try:
        if isinstance(context.error, Conflict):
            logger.error("âŒ ÐšÐ¾Ð½Ñ„Ð»Ñ–ÐºÑ‚ Ð· Ñ–Ð½ÑˆÐ¸Ð¼ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€Ð¾Ð¼ Ð±Ð¾Ñ‚Ð°! ÐŸÐµÑ€ÐµÐºÐ¾Ð½Ð°Ð¹Ñ‚ÐµÑÑ, Ñ‰Ð¾ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾ Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ð¾Ð´Ð¸Ð½ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€.")
            return
        
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: {context.error}")
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² Ð¾Ð±Ñ€Ð¾Ð±Ð½Ð¸ÐºÑƒ Ð¿Ð¾Ð¼Ð¸Ð»Ð¾Ðº: {e}")

def main():
    """Ð“Ð¾Ð»Ð¾Ð²Ð½Ð° Ñ„ÑƒÐ½ÐºÑ†Ñ–Ñ Ð·Ð°Ð¿ÑƒÑÐºÑƒ Ð±Ð¾Ñ‚Ð°"""
    logger.info("=" * 80)
    logger.info("ðŸš€ Ð—ÐÐŸÐ£Ð¡Ðš ÐÐ”ÐœÐ†Ð-Ð‘ÐžÐ¢Ð Ð‘ÐžÐÐ•Ð›Ð•Ð¢")
    logger.info("=" * 80)
    
    # Ð¢Ð¸Ð¼Ñ‡Ð°ÑÐ¾Ð²Ð¾ Ð²Ð¸Ð¼Ð¸ÐºÐ°Ñ”Ð¼Ð¾ Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÑƒ Ð½Ð° ÑƒÐ½Ñ–ÐºÐ°Ð»ÑŒÐ½Ð¸Ð¹ ÐµÐºÐ·ÐµÐ¼Ð¿Ð»ÑÑ€ Ñ‡ÐµÑ€ÐµÐ· ÐºÐ¾Ð½Ñ„Ð»Ñ–ÐºÑ‚Ð¸ Ð½Ð° Railway
    # if not check_single_instance():
    #     logger.error("ðŸš« Ð‘Ð¾Ñ‚ Ð²Ð¶Ðµ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾ Ð² Ñ–Ð½ÑˆÐ¾Ð¼Ñƒ Ð¿Ñ€Ð¾Ñ†ÐµÑÑ–! Ð—Ð°Ð²ÐµÑ€ÑˆÑƒÑ”Ð¼Ð¾...")
    #     sys.exit(1)
    
    try:
        conn = get_db_connection()
        if conn:
            logger.info(f"âœ… ÐŸÑ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ… ÑƒÑÐ¿Ñ–ÑˆÐ½Ðµ")
            logger.info("ðŸ”„ Ð’Ð¸ÐºÐ»Ð¸ÐºÐ°ÑŽ init_database_if_empty()...")
            init_result = init_database_if_empty()
            logger.info(f"ðŸ“Š Ð ÐµÐ·ÑƒÐ»ÑŒÑ‚Ð°Ñ‚ Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ—: {init_result}")
            
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
                
                logger.info(f"ðŸ“Š Ð¡Ñ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ° Ð‘Ð”:")
                logger.info(f"   â€¢ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²: {users_count}")
                logger.info(f"   â€¢ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {orders_count}")
                logger.info(f"   â€¢ Ð¨Ð²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {quick_orders_count}")
                logger.info(f"   â€¢ Ð¢Ð¾Ð²Ð°Ñ€Ñ–Ð²: {products_count}")
                logger.info(f"   â€¢ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {messages_count}")
                logger.info(f"   â€¢ FAQ: {faq_count}")
                logger.info(f"   â€¢ ÐÐ´Ð¼Ñ–Ð½Ñ–Ð²: {admins_count}")
                
            except Exception as e:
                logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ¸: {e}")
                logger.error(traceback.format_exc())
            
            conn.close()
        else:
            logger.warning("âš ï¸ ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡Ð¸Ñ‚Ð¸ÑÑŒ Ð´Ð¾ Ð‘Ð”")
            init_database_if_empty()
        
        application = Application.builder().token(TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        application.add_handler(MessageHandler(filters.PHOTO, message_handler))
        application.add_error_handler(error_handler)
        
        logger.info("âœ… ÐÐ´Ð¼Ñ–Ð½-Ð±Ð¾Ñ‚ Ð³Ð¾Ñ‚Ð¾Ð²Ð¸Ð¹ Ð´Ð¾ Ñ€Ð¾Ð±Ð¾Ñ‚Ð¸")
        logger.info("ðŸš€ Ð—Ð°Ð¿ÑƒÑÐº polling...")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"âŒ ÐšÑ€Ð¸Ñ‚Ð¸Ñ‡Ð½Ð° Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°: {e}")
        logger.error(traceback.format_exc())
        time.sleep(5)

if __name__ == "__main__":
    main()
