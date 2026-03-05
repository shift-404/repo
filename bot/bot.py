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
    logger.error("BOT_TOKEN Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)

ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN")
if not ADMIN_BOT_TOKEN:
    logger.error("ADMIN_BOT_TOKEN Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)

logger.info(f"âœ… Ð¢Ð¾ÐºÐµÐ½ Ð¾ÑÐ½Ð¾Ð²Ð½Ð¾Ð³Ð¾ Ð±Ð¾Ñ‚Ð° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾: {TOKEN[:4]}...{TOKEN[-4:]}")
logger.info(f"âœ… Ð¢Ð¾ÐºÐµÐ½ Ð°Ð´Ð¼Ñ–Ð½-Ð±Ð¾Ñ‚Ð° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾: {ADMIN_BOT_TOKEN[:4]}...{ADMIN_BOT_TOKEN[-4:]}")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾!")
    sys.exit(1)

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ–Ð´ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ñ Ð´Ð¾ Ð‘Ð”: {e}")
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
        
        # ========== Ð¢ÐÐ‘Ð›Ð˜Ð¦Ð† Ð”Ð›Ð¯ ÐšÐžÐÐ¢Ð•ÐÐ¢Ð£ ==========
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
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ ÐºÐ¾Ð»Ð¾Ð½ÐºÑƒ image ÑÐºÑ‰Ð¾ Ñ—Ñ— Ð½ÐµÐ¼Ð°Ñ”
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image TEXT')
            logger.info("âœ… ÐšÐ¾Ð»Ð¾Ð½ÐºÐ° image Ð´Ð¾Ð´Ð°Ð½Ð° Ð´Ð¾ Ñ‚Ð°Ð±Ð»Ð¸Ñ†Ñ– products")
        except Exception as e:
            logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ ÐºÐ¾Ð»Ð¾Ð½ÐºÐ¸ image: {e}")
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ ÐºÐ¾Ð»Ð¾Ð½ÐºÑƒ image_data ÑÐºÑ‰Ð¾ Ñ—Ñ— Ð½ÐµÐ¼Ð°Ñ”
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS image_data BYTEA')
            logger.info("âœ… ÐšÐ¾Ð»Ð¾Ð½ÐºÐ° image_data Ð´Ð¾Ð´Ð°Ð½Ð° Ð´Ð¾ Ñ‚Ð°Ð±Ð»Ð¸Ñ†Ñ– products")
        except Exception as e:
            logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ ÐºÐ¾Ð»Ð¾Ð½ÐºÐ¸ image_data: {e}")
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– Ð´Ð»Ñ company_info, ÑÐºÑ‰Ð¾ Ñ—Ñ… Ð½ÐµÐ¼Ð°Ñ”
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
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– Ð´Ð°Ð½Ñ– Ð´Ð»Ñ welcome_message, ÑÐºÑ‰Ð¾ Ñ—Ñ… Ð½ÐµÐ¼Ð°Ñ”
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
        
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÐ¾Ð²Ñ– FAQ, ÑÐºÑ‰Ð¾ Ñ—Ñ… Ð½ÐµÐ¼Ð°Ñ”
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
        
        conn.commit()
        logger.info("âœ… Ð‘Ð°Ð·Ð° Ð´Ð°Ð½Ð¸Ñ… PostgreSQL Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð¾Ð²Ð°Ð½Ð°")
        return True
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·Ð°Ñ†Ñ–Ñ— Ð±Ð°Ð·Ð¸ Ð´Ð°Ð½Ð¸Ñ…: {e}")
        return False
    finally:
        conn.close()

# ========== Ð¤Ð£ÐÐšÐ¦Ð†Ð‡ Ð”Ð›Ð¯ Ð ÐžÐ‘ÐžÐ¢Ð˜ Ð— ÐšÐžÐÐ¢Ð•ÐÐ¢ÐžÐœ ==========

def get_company_info() -> str:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ñ‚ÐµÐºÑÑ‚ Ð¿Ñ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ Ð· Ð‘Ð”"""
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

def get_welcome_message() -> str:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ð‘Ð”"""
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

def get_all_faqs() -> List[Dict]:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð²ÑÑ– FAQ Ð· Ð‘Ð”, Ð²Ñ–Ð´ÑÐ¾Ñ€Ñ‚Ð¾Ð²Ð°Ð½Ñ– Ð·Ð° Ð¿Ð¾Ð·Ð¸Ñ†Ñ–Ñ”ÑŽ"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer, position FROM faq ORDER BY position, id')
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ faq: {e}")
        return []
    finally:
        conn.close()

def get_faq_by_id(faq_id: int) -> Optional[Dict]:
    """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” FAQ Ð·Ð° ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, question, answer FROM faq WHERE id = %s', (faq_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ faq Ð·Ð° ID: {e}")
        return None
    finally:
        conn.close()

# ========== Ð Ð•Ð¨Ð¢Ð ÐšÐžÐ”Ð£ ==========

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
            f.write(f"Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_data.get('order_id', 'Ð/Ð”')}\n")
            f.write(f"Ð§Ð°Ñ: {timestamp}\n")
            f.write(f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order_data.get('user_name', 'Ð/Ð”')}\n")
            f.write(f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order_data.get('phone', 'Ð/Ð”')}\n")
            f.write(f"Username: @{order_data.get('username', 'Ð/Ð”')}\n")
            f.write(f"ÐœÑ–ÑÑ‚Ð¾: {order_data.get('city', 'Ð/Ð”')}\n")
            f.write(f"Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ: {order_data.get('np_department', 'Ð/Ð”')}\n")
            f.write(f"Ð¡ÑƒÐ¼Ð°: {order_data.get('total', 0):.2f} Ð³Ñ€Ð½\n")
            f.write(f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order_data.get('status', 'Ð½Ð¾Ð²Ðµ')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑƒ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")

def log_user(user_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(USERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | ID:{user_data.get('user_id')} | {user_data.get('first_name', '')} {user_data.get('last_name', '')} | @{user_data.get('username', '')}\n")
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑƒ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")

def log_message(msg_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(MESSAGES_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'â”€'*50}\n")
            f.write(f"Ð§Ð°Ñ: {timestamp}\n")
            f.write(f"Ð’Ñ–Ð´: {msg_data.get('user_name', 'Ð/Ð”')} (ID: {msg_data.get('user_id', 'Ð/Ð”')})\n")
            f.write(f"Username: @{msg_data.get('username', 'Ð/Ð”')}\n")
            f.write(f"Ð¢Ð¸Ð¿: {msg_data.get('message_type', 'Ð/Ð”')}\n")
            f.write(f"Ð¢ÐµÐºÑÑ‚: {msg_data.get('text', 'Ð/Ð”')}\n")
            f.write(f"{'â”€'*50}\n")
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑƒ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {e}")

def log_quick_order(order_data: dict):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(QUICK_ORDERS_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"Ð¨Ð’Ð˜Ð”ÐšÐ• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_data.get('order_id', 'Ð/Ð”')}\n")
            f.write(f"Ð§Ð°Ñ: {timestamp}\n")
            f.write(f"ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {order_data.get('user_name', 'Ð/Ð”')}\n")
            f.write(f"Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {order_data.get('phone', 'Ð/Ð”')}\n")
            f.write(f"Username: @{order_data.get('username', 'Ð/Ð”')}\n")
            f.write(f"ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {order_data.get('product_name', 'Ð/Ð”')}\n")
            f.write(f"Ð¡Ð¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ: {order_data.get('contact_method', 'Ð/Ð”')}\n")
            f.write(f"ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {order_data.get('message', '')}\n")
            f.write(f"Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order_data.get('status', 'Ð½Ð¾Ð²Ðµ')}\n")
            f.write(f"{'='*60}\n\n")
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑƒ ÑˆÐ²Ð¸Ð´ÐºÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")

def check_single_instance():
    import socket
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

async def notify_admins_about_new_order(order_data: dict):
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
        
        message += f"\nðŸ•’ <b>Ð§Ð°Ñ:</b> {order_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
        
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² notify_admins_about_new_order: {e}")

async def notify_admins_about_message(message_data: dict):
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
        message += f"ðŸ•’ <b>Ð§Ð°Ñ:</b> {message_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
        
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"Ð¡Ð¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² notify_admins_about_message: {e}")

async def send_combined_quick_order_notification(order_id: int, user_id: int, user_name: str, username: str, product_name: str, message_text: str):
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
        message += f"ðŸ•’ <b>Ð§Ð°Ñ:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
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
                logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð°Ð´Ð¼Ñ–Ð½Ñƒ {admin['user_id']}: {e}")
        
        logger.info(f"ÐžÐ±'Ñ”Ð´Ð½Ð°Ð½Ðµ ÑÐ¿Ð¾Ð²Ñ–Ñ‰ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¾ ÑˆÐ²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾ {sent_count} Ð°Ð´Ð¼Ñ–Ð½Ð°Ð¼")
        
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² send_combined_quick_order_notification: {e}")

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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÐµÑÑ–Ñ—: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ ÑÐµÑÑ–Ñ—: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‡Ð¸Ñ‰ÐµÐ½Ð½Ñ ÑÐµÑÑ–Ñ—: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð´Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ Ð² ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñƒ: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð¸: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‡Ð¸Ñ‰ÐµÐ½Ð½Ñ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð¸: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ð· ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð¸: {e}")
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
                "Ð½Ð¾Ð²Ðµ"
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
            logger.info(f"âœ… Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} ÑÑ‚Ð²Ð¾Ñ€ÐµÐ½Ð¾ ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾")
            return order_id
        except Exception as e:
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° ÑÑ‚Ð²Ð¾Ñ€ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {e}")
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
            ''', (user_id, user_name, username, product_id, product_name, quantity, phone, contact_method, message, "Ð½Ð¾Ð²Ðµ"))
            
            result = cursor.fetchone()
            order_id = result['id'] if result else 0
            conn.commit()
            logger.info(f"âœ… Ð¨Ð²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð¾")
            return order_id
        except Exception as e:
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ ÑˆÐ²Ð¸Ð´ÐºÐ¾Ð³Ð¾ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ ÑÑ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ¸: {e}")
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²: {e}")
            return []
        finally:
            conn.close()
    
    @staticmethod
    def get_product_image(product_id: int):
        """ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ” Ð±Ð°Ð¹Ñ‚Ð¸ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð· Ð‘Ð”"""
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
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
        """ÐžÐ½Ð¾Ð²Ð»ÑŽÑ” Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð² Ð‘Ð”"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE products SET image_data = %s WHERE id = %s', (psycopg2.Binary(image_data), product_id))
            conn.commit()
            logger.info(f"âœ… Ð—Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾ Ð² Ð‘Ð”")
            return True
        except Exception as e:
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
            return False
        finally:
            conn.close()
    
    @staticmethod
    def delete_product_image(product_id: int) -> bool:
        """Ð’Ð¸Ð´Ð°Ð»ÑÑ” Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð· Ð‘Ð”"""
        conn = Database.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE products SET image_data = NULL WHERE id = %s', (product_id,))
            conn.commit()
            logger.info(f"âœ… Ð—Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð¾ Ð· Ð‘Ð”")
            return True
        except Exception as e:
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ð¸Ð´Ð°Ð»ÐµÐ½Ð½Ñ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ: {e}")
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
                    created_at_str = str(created_at) if created_at else 'Ð/Ð”'
                
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
            logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°: {e}")
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
    logger.info(f"ðŸ”„ ÐžÐ½Ð¾Ð²Ð»ÐµÐ½Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ð¸: {len(PRODUCTS)} Ð¿Ð¾Ð·Ð¸Ñ†Ñ–Ð¹")

refresh_products()

# ========== ÐšÐžÐœÐÐÐ”Ð˜ Ð”Ð›Ð¯ ÐÐ”ÐœÐ†ÐÐ†Ð’ ==========

async def is_admin_user(user_id: int) -> bool:
    """ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ” Ñ‡Ð¸ Ñ” ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ Ð°Ð´Ð¼Ñ–Ð½Ñ–ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð¾Ð¼"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM admins WHERE user_id = %s', (user_id,))
        count = cursor.fetchone()['count']
        return count > 0
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÐºÐ¸ Ð°Ð´Ð¼Ñ–Ð½Ð°: {e}")
        return False
    finally:
        conn.close()

async def setphoto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐšÐ¾Ð¼Ð°Ð½Ð´Ð° Ð´Ð»Ñ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ (Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ð´Ð»Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²)"""
    user = update.effective_user
    user_id = user.id
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ Ð°Ð´Ð¼Ñ–Ð½
    if not await is_admin_user(user_id):
        logger.warning(f"âŒ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ {user_id} ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ²Ð°Ð² Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð°Ñ‚Ð¸ Ð°Ð´Ð¼Ñ–Ð½-ÐºÐ¾Ð¼Ð°Ð½Ð´Ñƒ")
        return  # ÐÑ–ÑÐºÐ¾Ñ— Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´Ñ– Ð·Ð²Ð¸Ñ‡Ð°Ð¹Ð½Ð¸Ð¼ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ð°Ð¼
    
    args = context.args
    if not args:
        await update.message.reply_text("âŒ Ð’ÐºÐ°Ð¶Ñ–Ñ‚ÑŒ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ. ÐŸÑ€Ð¸ÐºÐ»Ð°Ð´: /setphoto 1")
        return
    
    try:
        product_id = int(args[0])
        product = get_product_by_id(product_id)
        if not product:
            await update.message.reply_text(f"âŒ Ð¢Ð¾Ð²Ð°Ñ€ Ð· ID {product_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾")
            return
        
        # Ð—Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ Ð² ÐºÐ¾Ð½Ñ‚ÐµÐºÑÑ‚Ñ–, Ñ‰Ð¾ Ñ†ÐµÐ¹ Ð°Ð´Ð¼Ñ–Ð½ Ð·Ð°Ñ€Ð°Ð· Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÑŽÑ” Ñ„Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ†ÑŒÐ¾Ð³Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ
        context.user_data['setphoto_product_id'] = product_id
        context.user_data['setphoto_mode'] = 'waiting'
        
        await update.message.reply_text(
            f"ðŸ“¸ Ð’ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} - {product['name']}\n\n"
            f"ÐÐ°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ñ„Ð¾Ñ‚Ð¾ Ñ„Ð°Ð¹Ð»Ð¾Ð¼ Ð°Ð±Ð¾ Ð²Ð²ÐµÐ´Ñ–Ñ‚ÑŒ URL Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ.\n"
            f"Ð”Ð»Ñ ÑÐºÐ°ÑÑƒÐ²Ð°Ð½Ð½Ñ Ð²Ð²ÐµÐ´Ñ–Ñ‚ÑŒ /cancel",
            parse_mode='HTML'
        )
    except ValueError:
        await update.message.reply_text("âŒ ID Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ Ð¼Ð°Ñ” Ð±ÑƒÑ‚Ð¸ Ñ‡Ð¸ÑÐ»Ð¾Ð¼")

async def handle_admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð»ÑÑ” Ñ„Ð¾Ñ‚Ð¾ Ð²Ñ–Ð´ Ð°Ð´Ð¼Ñ–Ð½Ð° Ð´Ð»Ñ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð½Ð° Ñ‚Ð¾Ð²Ð°Ñ€"""
    user = update.effective_user
    user_id = user.id
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ñ” Ð°ÐºÑ‚Ð¸Ð²Ð½Ð° ÑÐµÑÑ–Ñ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾
    if 'setphoto_product_id' not in context.user_data:
        return
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ Ð°Ð´Ð¼Ñ–Ð½
    if not await is_admin_user(user_id):
        logger.warning(f"âŒ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ {user_id} ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ²Ð°Ð² Ð½Ð°Ð´Ñ–ÑÐ»Ð°Ñ‚Ð¸ Ñ„Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ")
        return
    
    product_id = context.user_data['setphoto_product_id']
    product = get_product_by_id(product_id)
    
    if update.message.photo:
        # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ Ñ„Ð°Ð¹Ð» Ð· Ð½Ð°Ð¹Ð±Ñ–Ð»ÑŒÑˆÐ¾ÑŽ Ñ€Ð¾Ð·Ð´Ñ–Ð»ÑŒÐ½Ð¾ÑŽ Ð·Ð´Ð°Ñ‚Ð½Ñ–ÑÑ‚ÑŽ
        file_id = update.message.photo[-1].file_id
        file = await context.bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Ð—Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ Ð² Ð‘Ð”
        if Database.update_product_image(product_id, bytes(file_bytes)):
            await update.message.reply_text(
                f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} - {product['name']} ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð¾!",
                reply_markup=get_main_menu()
            )
        else:
            await update.message.reply_text(
                f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾",
                reply_markup=get_main_menu()
            )
        
        # ÐžÑ‡Ð¸Ñ‰Ð°Ñ”Ð¼Ð¾ ÑÐµÑÑ–ÑŽ
        del context.user_data['setphoto_product_id']
        del context.user_data['setphoto_mode']

async def handle_admin_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ÐžÐ±Ñ€Ð¾Ð±Ð»ÑÑ” URL Ð²Ñ–Ð´ Ð°Ð´Ð¼Ñ–Ð½Ð° Ð´Ð»Ñ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð½Ð° Ñ‚Ð¾Ð²Ð°Ñ€"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ñ” Ð°ÐºÑ‚Ð¸Ð²Ð½Ð° ÑÐµÑÑ–Ñ Ð²ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾
    if 'setphoto_product_id' not in context.user_data:
        return
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ ÐºÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ Ð°Ð´Ð¼Ñ–Ð½
    if not await is_admin_user(user_id):
        logger.warning(f"âŒ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡ {user_id} ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ²Ð°Ð² Ð½Ð°Ð´Ñ–ÑÐ»Ð°Ñ‚Ð¸ URL Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ")
        return
    
    # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ñ†Ðµ ÑÑ…Ð¾Ð¶Ðµ Ð½Ð° URL
    if not (text.startswith('http://') or text.startswith('https://')):
        await update.message.reply_text("âŒ Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, Ð½Ð°Ð´Ñ–ÑˆÐ»Ñ–Ñ‚ÑŒ Ð¿Ñ€Ð°Ð²Ð¸Ð»ÑŒÐ½Ð¸Ð¹ URL (Ð¿Ð¾Ñ‡Ð¸Ð½Ð°Ñ”Ñ‚ÑŒÑÑ Ð· http:// Ð°Ð±Ð¾ https://)")
        return
    
    product_id = context.user_data['setphoto_product_id']
    product = get_product_by_id(product_id)
    
    # Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑ”Ð¼Ð¾ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð° URL
    await update.message.reply_text("â° Ð—Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÑƒÑŽ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ...")
    
    try:
        import requests
        response = requests.get(text, timeout=30)
        response.raise_for_status()
        
        # Ð—Ð±ÐµÑ€Ñ–Ð³Ð°Ñ”Ð¼Ð¾ Ð² Ð‘Ð”
        if Database.update_product_image(product_id, response.content):
            await update.message.reply_text(
                f"âœ… Ð¤Ð¾Ñ‚Ð¾ Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id} - {product['name']} ÑƒÑÐ¿Ñ–ÑˆÐ½Ð¾ Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð¾!",
                reply_markup=get_main_menu()
            )
        else:
            await update.message.reply_text(
                f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð½Ñ– Ñ„Ð¾Ñ‚Ð¾",
                reply_markup=get_main_menu()
            )
    except Exception as e:
        await update.message.reply_text(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð²Ð°Ð½Ñ‚Ð°Ð¶ÐµÐ½Ð½Ñ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ: {e}")
    
    # ÐžÑ‡Ð¸Ñ‰Ð°Ñ”Ð¼Ð¾ ÑÐµÑÑ–ÑŽ
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
        [{"text": "ðŸ¢ ÐŸÑ€Ð¾ ÐºÐ¾Ð¼Ð¿Ð°Ð½Ñ–ÑŽ", "callback_data": "company"}],
        [{"text": "ðŸ“¦ ÐÐ°ÑˆÑ– Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸", "callback_data": "products"}],
        [{"text": "â“ Ð§Ð°ÑÑ‚Ñ– Ð·Ð°Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ", "callback_data": "faq"}],
        [
            {"text": "ðŸ›’ ÐœÐ¾Ñ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð°", "callback_data": "cart"}, 
            {"text": "ðŸ“‹ ÐœÐ¾Ñ— Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "my_orders"}
        ],
        [{"text": "ðŸ“ž Ð—Ð²'ÑÐ·Ð°Ñ‚Ð¸ÑÑ Ð· Ð½Ð°Ð¼Ð¸", "callback_data": "contact"}]
    ]
    return create_inline_keyboard(buttons)

def get_back_keyboard(back_to: str) -> InlineKeyboardMarkup:
    buttons = [[{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": f"back_{back_to}"}]]
    return create_inline_keyboard(buttons)

def get_products_menu() -> InlineKeyboardMarkup:
    refresh_products()
    buttons = []
    for product in PRODUCTS:
        # Ð”Ð¾Ð´Ð°Ñ”Ð¼Ð¾ ÐµÐ¼Ð¾Ð´Ð·Ñ– Ð¿ÐµÑ€ÐµÐ´ Ð½Ð°Ð·Ð²Ð¾ÑŽ
        emoji = product.get('image', 'ðŸ¥«')  # Ð¯ÐºÑ‰Ð¾ Ð½ÐµÐ¼Ð°Ñ” ÐµÐ¼Ð¾Ð´Ð·Ñ–, Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð¾Ð²ÑƒÑ”Ð¼Ð¾ ÑÑ‚Ð°Ð½Ð´Ð°Ñ€Ñ‚Ð½Ðµ
        button_text = f"{emoji} {product['name']}\n{product['price']} Ð³Ñ€Ð½/{product['unit']}"
        # ÐžÐ±Ð¼ÐµÐ¶ÑƒÑ”Ð¼Ð¾ Ð´Ð¾Ð²Ð¶Ð¸Ð½Ñƒ Ñ‚ÐµÐºÑÑ‚Ñƒ
        if len(button_text) > 60:
            name_part = product['name'][:35] + "..." if len(product['name']) > 35 else product['name']
            button_text = f"{emoji} {name_part}\n{product['price']} Ð³Ñ€Ð½/{product['unit']}"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
        buttons.append([{
            "text": button_text,
            "callback_data": f"product_{product['id']}"
        }])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_product_detail_menu(product_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "ðŸ›’ Ð”Ð¾Ð´Ð°Ñ‚Ð¸ Ð² ÐºÐ¾ÑˆÐ¸Ðº", "callback_data": f"add_to_cart_{product_id}"}],
        [{"text": "âš¡ Ð¨Ð²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": f"quick_order_{product_id}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_products"}]
    ]
    return create_inline_keyboard(buttons)

def get_quick_order_menu(product_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "ðŸ“ž Ð—Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÐ¹Ñ‚Ðµ Ð¼ÐµÐ½Ñ–", "callback_data": f"quick_call_{product_id}"}],
        [{"text": "ðŸ’¬ ÐÐ°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð¼ÐµÐ½Ñ– Ð² Ñ‡Ð°Ñ‚", "callback_data": f"quick_chat_{product_id}"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": f"product_{product_id}"}]
    ]
    return create_inline_keyboard(buttons)

def get_faq_menu() -> InlineKeyboardMarkup:
    # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ ÑÐ²Ñ–Ð¶Ñ– FAQ Ð· Ð‘Ð” Ð¿Ñ€Ð¸ ÐºÐ¾Ð¶Ð½Ð¾Ð¼Ñƒ Ð·Ð°Ð¿Ð¸Ñ‚Ñ–
    faqs = get_all_faqs()
    buttons = []
    for faq in faqs:
        short_q = faq['question'][:40] + "..." if len(faq['question']) > 40 else faq['question']
        buttons.append([{
            "text": f"â” {short_q}",
            "callback_data": f"faq_{faq['id']}"
        }])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_contact_menu() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "ðŸ“ž Ð—Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÐ²Ð°Ñ‚Ð¸", "callback_data": "call_us"}],
        [{"text": "ðŸ“ ÐÐ°ÑˆÐ° Ð°Ð´Ñ€ÐµÑÐ°", "callback_data": "our_address"}],
        [{"text": "ðŸ’¬ ÐÐ°Ð¿Ð¸ÑÐ°Ñ‚Ð¸ Ð½Ð°Ð¼ Ñ‚ÑƒÑ‚", "callback_data": "write_here"}],
        [{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_main_menu"}]
    ]
    return create_inline_keyboard(buttons)

def get_cart_menu(cart_items: List) -> InlineKeyboardMarkup:
    buttons = []
    if cart_items:
        buttons.append([{"text": "âœ… ÐžÑ„Ð¾Ñ€Ð¼Ð¸Ñ‚Ð¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ", "callback_data": "checkout_cart"}])
        buttons.append([{"text": "ðŸ—‘ï¸ ÐžÑ‡Ð¸ÑÑ‚Ð¸Ñ‚Ð¸ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñƒ", "callback_data": "clear_cart"}])
        
        for item in cart_items:
            product_name = item["product"]["name"][:20]
            if len(item["product"]["name"]) > 20:
                product_name += "..."
            buttons.append([{
                "text": f"âŒ {product_name} ({item['quantity']} {item['product']['unit']})",
                "callback_data": f"remove_from_cart_{item['cart_id']}"
            }])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def get_order_confirmation_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [{"text": "âœ… Ð¢Ð°Ðº, Ð¿Ñ€Ð¾Ð´Ð¾Ð²Ð¶Ð¸Ñ‚Ð¸", "callback_data": "confirm_order_yes"}],
        [{"text": "âŒ ÐÑ–, ÑÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸", "callback_data": "confirm_order_no"}]
    ]
    return create_inline_keyboard(buttons)

def get_my_orders_menu(orders: List) -> InlineKeyboardMarkup:
    buttons = []
    for order in orders[:5]:
        buttons.append([{
            "text": f"â„–{order['order_id']} - {order['created_at'][:16]} - {order['total']} Ð³Ñ€Ð½",
            "callback_data": f"user_order_{order['order_id']}"
        }])
    buttons.append([{"text": "ðŸ”™ ÐÐ°Ð·Ð°Ð´", "callback_data": "back_main_menu"}])
    return create_inline_keyboard(buttons)

def parse_quantity(text: str) -> Tuple[bool, float, str]:
    text = text.strip().replace(" ", "")
    match = re.search(r'(\d+(?:[.,]\d+)?)', text)
    
    if not match:
        return False, 0, "âŒ Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, Ð²Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ñ‡Ð¸ÑÐ»Ð¾ (Ð½Ð°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: 1, 1.5, 2.3)"
    
    try:
        num_str = match.group(1).replace(",", ".")
        quantity = float(num_str)
        if quantity <= 0:
            return False, 0, "âŒ ÐšÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ Ð¿Ð¾Ð²Ð¸Ð½Ð½Ð° Ð±ÑƒÑ‚Ð¸ Ð±Ñ–Ð»ÑŒÑˆÐµ 0"
        if quantity > 100:
            return False, 0, "âŒ Ð—Ð°Ð½Ð°Ð´Ñ‚Ð¾ Ð²ÐµÐ»Ð¸ÐºÐ° ÐºÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ. ÐœÐ°ÐºÑÐ¸Ð¼ÑƒÐ¼ 100"
        return True, quantity, ""
    except ValueError:
        return False, 0, "âŒ ÐÐµÐºÐ¾Ñ€ÐµÐºÑ‚Ð½Ð¸Ð¹ Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚ Ñ‡Ð¸ÑÐ»Ð°"

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
    # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ Ð°ÐºÑ‚ÑƒÐ°Ð»ÑŒÐ½Ðµ Ð²Ñ–Ñ‚Ð°Ð»ÑŒÐ½Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ð‘Ð”
    return get_welcome_message()

def get_company_text() -> str:
    # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ Ð°ÐºÑ‚ÑƒÐ°Ð»ÑŒÐ½Ð¸Ð¹ Ñ‚ÐµÐºÑÑ‚ Ð· Ð‘Ð”
    return get_company_info()

def get_product_text(product_id: int) -> str:
    refresh_products()
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
    
    emoji = product.get('image', 'ðŸ¥«')
    text = f"""
<b>{emoji} {product['name']}</b>

ðŸ“ <i>{product['description']}</i>

ðŸ’° <b>Ð¦Ñ–Ð½Ð°:</b> {product['price']} Ð³Ñ€Ð½/{product['unit']}
ðŸ·ï¸ <b>ÐšÐ°Ñ‚ÐµÐ³Ð¾Ñ€Ñ–Ñ:</b> {product['category']}
ðŸ“¦ <b>ÐÐ°ÑÐ²Ð½Ñ–ÑÑ‚ÑŒ:</b> Ð„ Ð² Ð½Ð°ÑÐ²Ð½Ð¾ÑÑ‚Ñ–

<b>ðŸ“Š Ð¥Ð°Ñ€Ð°ÐºÑ‚ÐµÑ€Ð¸ÑÑ‚Ð¸ÐºÐ¸:</b>
â€¢ {product['details']}

<b>ðŸŒŸ ÐŸÐµÑ€ÐµÐ²Ð°Ð³Ð¸:</b>
â€¢ Ð’Ð¸Ñ€Ð¾Ñ‰ÐµÐ½Ð¸Ð¹ Ð½Ð° ÐžÐ´ÐµÑ‰Ð¸Ð½Ñ–
â€¢ ÐÐ°Ñ‚ÑƒÑ€Ð°Ð»ÑŒÐ½Ðµ ÐºÐ¾Ð½ÑÐµÑ€Ð²ÑƒÐ²Ð°Ð½Ð½Ñ
â€¢ Ð‘ÐµÐ· ÑˆÑ‚ÑƒÑ‡Ð½Ð¸Ñ… Ð´Ð¾Ð±Ð°Ð²Ð¾Ðº
â€¢ Ð’Ð¸ÑÐ¾ÐºÐ° ÑÐºÑ–ÑÑ‚ÑŒ

<b>ðŸ’¡ Ð¯Ðº Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð¾Ð²ÑƒÐ²Ð°Ñ‚Ð¸:</b>
Ð†Ð´ÐµÐ°Ð»ÑŒÐ½Ð¾ Ð¿Ñ–Ð´Ñ…Ð¾Ð´Ð¸Ñ‚ÑŒ ÑÐº Ð·Ð°ÐºÑƒÑÐºÐ°, Ð´Ð¾ ÑÐ°Ð»Ð°Ñ‚Ñ–Ð², Ð¼'ÑÑÐ½Ð¸Ñ… ÑÑ‚Ñ€Ð°Ð² Ñ‚Ð° ÑÐº ÑÐ°Ð¼Ð¾ÑÑ‚Ñ–Ð¹Ð½Ð° ÑÑ‚Ñ€Ð°Ð²Ð°.
"""
    return text

def get_quick_order_text(product_id: int) -> str:
    refresh_products()
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    if not product:
        return "âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
    
    emoji = product.get('image', 'ðŸ¥«')
    return f"""
<b>âš¡ Ð¨Ð²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {emoji} {product['name']}</b>

ðŸ’° <b>Ð¦Ñ–Ð½Ð°:</b> {product['price']} Ð³Ñ€Ð½/{product['unit']}

ðŸ’¬ <b>Ð¯Ðº Ð²Ð¸ Ð±Ð°Ð¶Ð°Ñ”Ñ‚Ðµ, Ñ‰Ð¾Ð± Ð¼Ð¸ Ð· Ð²Ð°Ð¼Ð¸ Ð·Ð²'ÑÐ·Ð°Ð»Ð¸ÑÑ?</b>

ðŸ“ž <b>Ð—Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÐ¹Ñ‚Ðµ Ð¼ÐµÐ½Ñ–</b> - Ð¼Ð¸ Ð·Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÑ”Ð¼Ð¾ Ð²Ð°Ð¼ Ð´Ð»Ñ ÑƒÑ‚Ð¾Ñ‡Ð½ÐµÐ½Ð½Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹
ðŸ’¬ <b>ÐÐ°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð¼ÐµÐ½Ñ– Ð² Ñ‡Ð°Ñ‚</b> - Ð²Ð¸ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð½Ð°Ð¿Ð¸ÑÐ°Ñ‚Ð¸ Ð²ÑÑ– Ð´ÐµÑ‚Ð°Ð»Ñ– Ñ‚ÑƒÑ‚ Ñ– Ð¼Ð¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð¼Ð¾

<i>ÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð·Ñ€ÑƒÑ‡Ð½Ð¸Ð¹ Ð´Ð»Ñ Ð²Ð°Ñ ÑÐ¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ ðŸ‘‡</i>
"""

def get_faq_text(faq_id: int) -> str:
    # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ ÐºÐ¾Ð½ÐºÑ€ÐµÑ‚Ð½Ð¸Ð¹ FAQ Ð· Ð‘Ð”
    conn = get_db_connection()
    if not conn:
        return "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT question, answer FROM faq WHERE id = %s', (faq_id,))
        row = cursor.fetchone()
        if row:
            return f"""
<b>â” {row['question']}</b>

{row['answer']}

<i>ðŸ“ž ÐœÐ°Ñ”Ñ‚Ðµ Ñ–Ð½ÑˆÑ– Ð·Ð°Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ? Ð—Ð²'ÑÐ¶Ñ–Ñ‚ÑŒÑÑ Ð· Ð½Ð°Ð¼Ð¸: +380932599103</i>
            """
        return "âŒ ÐŸÐ¸Ñ‚Ð°Ð½Ð½Ñ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾"
    except Exception as e:
        logger.error(f"ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ faq Ð·Ð° ID: {e}")
        return "âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð´Ð°Ð½Ð¸Ñ…"
    finally:
        conn.close()

def get_contact_text() -> str:
    return """
<b>ðŸ“ž Ð—Ð²'ÑÐ·Ð¾Ðº Ð· Ð½Ð°Ð¼Ð¸</b>

ÐœÐ¸ Ð·Ð°Ð²Ð¶Ð´Ð¸ Ñ€Ð°Ð´Ñ– Ð´Ð¾Ð¿Ð¾Ð¼Ð¾Ð³Ñ‚Ð¸ Ð²Ð°Ð¼!

<b>ÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ ÑÐ¿Ð¾ÑÑ–Ð± Ð·Ð²'ÑÐ·ÐºÑƒ:</b>
â€¢ <b>Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½</b> - Ð´Ð»Ñ ÑˆÐ²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¿Ð¸Ñ‚Ð°Ð½ÑŒ
â€¢ <b>ÐÐ´Ñ€ÐµÑÐ°</b> - Ð´Ð»Ñ ÑÐ°Ð¼Ð¾Ð²Ð¸Ð²Ð¾Ð·Ñƒ
â€¢ <b>ÐÐ°Ð¿Ð¸ÑÐ°Ñ‚Ð¸ Ñ‚ÑƒÑ‚</b> - ÑˆÐ²Ð¸Ð´ÐºÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ñ‡Ð°Ñ‚Ñ–

<i>ÐŸÑ€Ð¾ÑÑ‚Ð¾ Ð½Ð°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð½Ð°Ð¼ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ñ†ÑŒÐ¾Ð¼Ñƒ Ñ‡Ð°Ñ‚Ñ– ðŸ‘‡</i>
    """

def get_cart_text(cart_items: List[Dict]) -> str:
    if not cart_items:
        return "ðŸ›’ <b>Ð’Ð°ÑˆÐ° ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð° Ð¿Ð¾Ñ€Ð¾Ð¶Ð½Ñ</b>\n\nÐ”Ð¾Ð´Ð°Ð¹Ñ‚Ðµ Ñ‚Ð¾Ð²Ð°Ñ€Ð¸ Ð· ÐºÐ°Ñ‚Ð°Ð»Ð¾Ð³Ñƒ!"
    
    text = "ðŸ›’ <b>Ð’Ð°ÑˆÐ° ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð°</b>\n\n"
    total = 0
    
    for i, item in enumerate(cart_items, 1):
        quantity = item["quantity"]
        product = item["product"]
        item_total = product["price"] * quantity
        text += f"<b>{i}. {product['name']}</b>\n"
        text += f"   ðŸ“Š ÐšÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ: <b>{quantity} {product['unit']}</b>\n"
        text += f"   ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']} Ã— {quantity} = <b>{item_total:.2f} Ð³Ñ€Ð½</b>\n\n"
        total += item_total
    
    text += f"<b>ðŸ“Š Ð’ÑÑŒÐ¾Ð³Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€Ñ–Ð²:</b> {len(cart_items)}\n"
    text += f"<b>ðŸ’° Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°:</b> <b>{total:.2f} Ð³Ñ€Ð½</b>\n\n"
    
    if len(cart_items) >= 3:
        discount = total * 0.05
        discount_total = total - discount
        text += f"ðŸŽ <b>Ð—Ð½Ð¸Ð¶ÐºÐ° 5% Ð·Ð° 3+ Ð±Ð°Ð½Ð¾Ðº:</b> -{discount:.2f} Ð³Ñ€Ð½\n"
        text += f"ðŸ’µ <b>Ð”Ð¾ ÑÐ¿Ð»Ð°Ñ‚Ð¸:</b> <b>{discount_total:.2f} Ð³Ñ€Ð½</b>\n\n"
    
    text += "<i>Ð”Ð»Ñ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð½Ð°Ñ‚Ð¸ÑÐ½Ñ–Ñ‚ÑŒ ÐºÐ½Ð¾Ð¿ÐºÑƒ Ð½Ð¸Ð¶Ñ‡Ðµ</i>"
    return text

def get_my_orders_text(orders: List[Dict]) -> str:
    if not orders:
        return "ðŸ“‹ <b>Ð£ Ð²Ð°Ñ Ñ‰Ðµ Ð½ÐµÐ¼Ð°Ñ” Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ</b>\n\nÐ—Ñ€Ð¾Ð±Ñ–Ñ‚ÑŒ Ð¿ÐµÑ€ÑˆÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð² Ñ€Ð¾Ð·Ð´Ñ–Ð»Ñ– 'ÐÐ°ÑˆÑ– Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸'!"
    
    text = "ðŸ“‹ <b>ÐœÐ¾Ñ— Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ</b>\n\n"
    for order in orders:
        text += f"â„–{order['order_id']} | {order['created_at'][:16]}\n"
        text += f"Ð¡ÑƒÐ¼Ð°: {order['total']:.2f} Ð³Ñ€Ð½ | Ð¡Ñ‚Ð°Ñ‚ÑƒÑ: {order['status']}\n"
        text += f"{'â”€'*40}\n"
    return text

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"ðŸ‘¤ [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡'}: /start")
        
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
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² start: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("â„¹ï¸ Ð”Ð¾Ð¿Ð¾Ð¼Ð¾Ð³Ð°: Ð¾Ð±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¾Ð¿Ñ†Ñ–ÑŽ Ð· Ð¼ÐµÐ½ÑŽ", reply_markup=get_main_menu())

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    # ÐžÑ‡Ð¸Ñ‰Ð°Ñ”Ð¼Ð¾ Ð±ÑƒÐ´ÑŒ-ÑÐºÑ– Ð°ÐºÑ‚Ð¸Ð²Ð½Ñ– ÑÐµÑÑ–Ñ—
    if 'setphoto_product_id' in context.user_data:
        del context.user_data['setphoto_product_id']
        del context.user_data['setphoto_mode']
        await update.message.reply_text("âŒ Ð’ÑÑ‚Ð°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ñ„Ð¾Ñ‚Ð¾ ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾", reply_markup=get_main_menu())
    
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
        
        logger.info(f"ðŸ–±ï¸ [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡'} Ð½Ð°Ñ‚Ð¸ÑÐ½ÑƒÐ²: {data}")
        
        Database.save_user(user_id, user.first_name, user.last_name or "", user.username or "")
        
        # ÐžÐ±Ñ€Ð¾Ð±ÐºÐ° ÐºÐ½Ð¾Ð¿Ð¾Ðº "ÐÐ°Ð·Ð°Ð´"
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
                products_text = "ðŸ“¦ <b>ÐÐ°ÑˆÑ– Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸</b>\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚ Ð´Ð»Ñ Ð´ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾Ñ— Ñ–Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–Ñ—:"
                try:
                    await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
                except Exception:
                    await query.message.reply_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
                Database.save_user_session(user_id, last_section="products")
            elif back_target == "faq":
                faq_text = "â“ <b>Ð§Ð°ÑÑ‚Ñ– Ð·Ð°Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ</b>\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ Ð´Ð»Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´Ñ–:"
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
        
        # ÐžÑÐ½Ð¾Ð²Ð½Ñ– Ñ€Ð¾Ð·Ð´Ñ–Ð»Ð¸ Ð¼ÐµÐ½ÑŽ
        elif data == "company":
            company_text = get_company_text()
            await query.edit_message_text(company_text, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="company")
            return
        
        elif data == "products":
            products_text = "ðŸ“¦ <b>ÐÐ°ÑˆÑ– Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸</b>\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚ Ð´Ð»Ñ Ð´ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾Ñ— Ñ–Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–Ñ—:"
            await query.edit_message_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
            return
        
        elif data == "faq":
            faq_text = "â“ <b>Ð§Ð°ÑÑ‚Ñ– Ð·Ð°Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ</b>\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ Ð´Ð»Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð½Ñ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð´Ñ–:"
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
                contact_info = "ðŸ“ž <b>Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½ Ð´Ð»Ñ Ð·Ð²'ÑÐ·ÐºÑƒ:</b>\n\n"
                contact_info += "âœ… <code>+380932599103</code>\n\n"
                contact_info += "<i>Ð“Ñ€Ð°Ñ„Ñ–Ðº Ñ€Ð¾Ð±Ð¾Ñ‚Ð¸: ÐŸÐ½-ÐŸÑ‚ 9:00-18:00, Ð¡Ð± 10:00-15:00</i>"
            else:
                contact_info = "ðŸ“ <b>ÐÐ°ÑˆÐ° Ð°Ð´Ñ€ÐµÑÐ°:</b>\n\n"
                contact_info += "ðŸ  ÐžÐ´ÐµÑÑŒÐºÐ° Ð¾Ð±Ð»Ð°ÑÑ‚ÑŒ\n"
                contact_info += "ðŸ“Œ ÑÐµÐ»Ð¾ Ð’ÐµÐ»Ð¸ÐºÐ¸Ð¹ Ð”Ð°Ð»ÑŒÐ½Ð¸Ðº\n"
                contact_info += "ðŸš— <b>Ð¡Ð°Ð¼Ð¾Ð²Ð¸Ð²Ñ–Ð· Ð¼Ð¾Ð¶Ð»Ð¸Ð²Ð¸Ð¹ Ð·Ð° Ð¿Ð¾Ð¿ÐµÑ€ÐµÐ´Ð½Ñ–Ð¼ Ð´Ð¾Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐ¼</b>\n\n"
                contact_info += "<i>Ð“Ñ€Ð°Ñ„Ñ–Ðº ÑÐ°Ð¼Ð¾Ð²Ð¸Ð²Ð¾Ð·Ñƒ: ÐŸÐ½-ÐŸÑ‚ 9:00-18:00, Ð¡Ð± 10:00-15:00</i>"
            
            await query.edit_message_text(contact_info, reply_markup=get_back_keyboard("contact"), parse_mode='HTML')
            return
        
        elif data == "write_here":
            Database.save_user_session(user_id, "waiting_message")
            response = "ðŸ’¬ <b>ÐÐ°Ð¿Ð¸ÑÐ°Ñ‚Ð¸ Ð½Ð°Ð¼ Ñ‚ÑƒÑ‚</b>\n\n"
            response += "ÐÐ°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð²Ð°ÑˆÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¿Ñ€ÑÐ¼Ð¾ Ð² Ñ†ÑŒÐ¾Ð¼Ñƒ Ñ‡Ð°Ñ‚Ñ–:\n\n"
            response += "â€¢ ÐŸÐ¸Ñ‚Ð°Ð½Ð½Ñ Ð¿Ñ€Ð¾ Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸\n"
            response += "â€¢ ÐšÐ¾Ð½ÑÑƒÐ»ÑŒÑ‚Ð°Ñ†Ñ–Ñ\n"
            response += "â€¢ ÐŸÑ€Ð¾Ð¿Ð¾Ð·Ð¸Ñ†Ñ–Ñ— ÑÐ¿Ñ–Ð²Ð¿Ñ€Ð°Ñ†Ñ–\n"
            response += "â€¢ Ð†Ð½ÑˆÑ– Ð¿Ð¸Ñ‚Ð°Ð½Ð½Ñ\n\n"
            response += "<i>ÐœÐ¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð¼Ð¾ Ð²Ð°Ð¼ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼!</i>"
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð¢ÐžÐ’ÐÐ Ð†Ð’ ==============
        
        elif data.startswith("product_"):
            product_id = int(data.split("_")[1])
            refresh_products()
            product = get_product_by_id(product_id)
            product_text = get_product_text(product_id)
            
            logger.info(f"ðŸ“¦ Ð’Ñ–Ð´ÐºÑ€Ð¸Ñ‚Ð¾ Ñ‚Ð¾Ð²Ð°Ñ€ #{product_id}")
            
            # ÐžÑ‚Ñ€Ð¸Ð¼ÑƒÑ”Ð¼Ð¾ Ð·Ð¾Ð±Ñ€Ð°Ð¶ÐµÐ½Ð½Ñ Ð· Ð‘Ð”
            image_data = Database.get_product_image(product_id)
            
            if image_data:
                try:
                    from io import BytesIO
                    photo = BytesIO(image_data)
                    photo.name = f"product_{product_id}.jpg"
                    logger.info(f"ðŸ“¸ Ð’Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÑÑ”Ð¼Ð¾ Ñ„Ð¾Ñ‚Ð¾ Ð· Ð‘Ð” Ð´Ð»Ñ Ñ‚Ð¾Ð²Ð°Ñ€Ñƒ #{product_id}")
                    
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
                    logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²ÐºÐ¸ Ñ„Ð¾Ñ‚Ð¾ Ð· Ð‘Ð”: {e}")
            
            # Ð¯ÐºÑ‰Ð¾ Ð½ÐµÐ¼Ð°Ñ” Ñ„Ð¾Ñ‚Ð¾ Ð°Ð±Ð¾ Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°, Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÑÑ”Ð¼Ð¾ Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ñ‚ÐµÐºÑÑ‚
            await query.edit_message_text(product_text, reply_markup=get_product_detail_menu(product_id), parse_mode='HTML')
            
            Database.save_user_session(user_id, last_section=f"product_{product_id}")
            return
        
        elif data.startswith("add_to_cart_"):
            product_id = int(data.split("_")[3])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("products"))
                return
            
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_quantity", temp_data)
            
            response = f"ðŸ“¦ <b>Ð”Ð¾Ð´Ð°Ð²Ð°Ð½Ð½Ñ {product['name']} Ð´Ð¾ ÐºÐ¾ÑˆÐ¸ÐºÐ°</b>\n\n"
            response += f"ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']}\n\n"
            response += "ðŸ“Š <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ ÐºÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ (Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ñ‡Ð¸ÑÐ»Ð¾):</b>\n\n"
            response += f"<i>ÐÐ°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: 1, 2, 3 (Ð² {product['unit']})</i>"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð¨Ð’Ð˜Ð”ÐšÐžÐ“Ðž Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ ==============
        
        elif data.startswith("quick_order_"):
            product_id = int(data.split("_")[2])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("products"))
                return
            
            quick_order_text = get_quick_order_text(product_id)
            
            # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¼Ð°Ñ” Ð¼ÐµÐ´Ñ–Ð° (Ñ„Ð¾Ñ‚Ð¾)
            if query.message.photo:
                # Ð¯ÐºÑ‰Ð¾ Ñ†Ðµ Ñ„Ð¾Ñ‚Ð¾, Ð²Ñ–Ð´Ð¿Ñ€Ð°Ð²Ð»ÑÑ”Ð¼Ð¾ Ð½Ð¾Ð²Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=quick_order_text,
                    reply_markup=get_quick_order_menu(product_id),
                    parse_mode='HTML'
                )
                # Ð’Ð¸Ð´Ð°Ð»ÑÑ”Ð¼Ð¾ ÑÑ‚Ð°Ñ€Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ñ„Ð¾Ñ‚Ð¾
                await query.message.delete()
            else:
                # Ð¯ÐºÑ‰Ð¾ Ð·Ð²Ð¸Ñ‡Ð°Ð¹Ð½Ðµ Ñ‚ÐµÐºÑÑ‚Ð¾Ð²Ðµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ - Ñ€ÐµÐ´Ð°Ð³ÑƒÑ”Ð¼Ð¾
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
                await query.edit_message_text("âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("products"))
                return
            
            temp_data = {"product_id": product_id}
            Database.save_user_session(user_id, "waiting_phone_for_quick_order", temp_data)
            
            response = f"ðŸ“ž <b>Ð—Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÐ¹Ñ‚Ðµ Ð¼ÐµÐ½Ñ–: {product['name']}</b>\n\n"
            response += f"ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']}\n\n"
            response += "ðŸ“± <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð²Ð°Ñˆ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ:</b>\n\n"
            response += "<i>ÐŸÑ€Ð¸ÐºÐ»Ð°Ð´: +380932599103 Ð°Ð±Ð¾ 0932599103</i>\n\n"
            response += "<b>ÐœÐ¸ Ð·Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÑ”Ð¼Ð¾ Ð²Ð°Ð¼ Ð´Ð»Ñ ÑƒÑ‚Ð¾Ñ‡Ð½ÐµÐ½Ð½Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ!</b>"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        elif data.startswith("quick_chat_"):
            product_id = int(data.split("_")[2])
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await query.edit_message_text("âŒ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_back_keyboard("products"))
                return
            
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'Ð½ÐµÐ¼Ð°Ñ”'
            
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
            
            response = f"ðŸ’¬ <b>ÐÐ°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð¼ÐµÐ½Ñ– Ð² Ñ‡Ð°Ñ‚: {product['name']}</b>\n\n"
            response += f"ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']}\n\n"
            response += "ðŸ’¬ <b>ÐŸÑ€Ð¾ÑÑ‚Ð¾ Ð½Ð°Ð¿Ð¸ÑˆÑ–Ñ‚ÑŒ Ð²Ð°ÑˆÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ñ†ÐµÐ¹ Ñ‡Ð°Ñ‚!</b>\n\n"
            response += "Ð’ÐºÐ°Ð¶Ñ–Ñ‚ÑŒ:\n"
            response += "â€¢ Ð‘Ð°Ð¶Ð°Ð½Ñƒ ÐºÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ\n"
            response += "â€¢ ÐšÐ¾Ð½Ñ‚Ð°ÐºÑ‚Ð½Ñ– Ð´Ð°Ð½Ñ–\n"
            response += "â€¢ Ð‘Ð°Ð¶Ð°Ð½Ð¸Ð¹ Ñ‡Ð°Ñ Ð´Ð¾ÑÑ‚Ð°Ð²ÐºÐ¸\n\n"
            response += "<b>ÐœÐ¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð¼Ð¾ Ð²Ð°Ð¼ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼ Ð´Ð»Ñ ÑƒÑ‚Ð¾Ñ‡Ð½ÐµÐ½Ð½Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ!</b>"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            
            logger.info(f"\n{'='*80}")
            logger.info(f"âš¡ Ð¨Ð’Ð˜Ð”ÐšÐ• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id} (Ð§ÐÐ¢ - Ð¾Ñ‡Ñ–ÐºÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ):")
            logger.info(f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {user_name}")
            logger.info(f"ðŸ“¦ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {product['name']}")
            logger.info(f"ðŸ†” User ID: {user_id}")
            logger.info(f"{'='*80}\n")
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ FAQ ==============
        
        elif data.startswith("faq_"):
            try:
                faq_id = int(data.split("_")[1])
                faq_text = get_faq_text(faq_id)
                await query.edit_message_text(faq_text, reply_markup=get_back_keyboard("faq"), parse_mode='HTML')
            except (IndexError, ValueError):
                await query.edit_message_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°", reply_markup=get_back_keyboard("faq"))
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ ÐšÐžÐ Ð—Ð˜ÐÐ˜ ==============
        
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
                response = "ðŸ›’ <b>Ð’Ð°ÑˆÐ° ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð° Ð¿Ð¾Ñ€Ð¾Ð¶Ð½Ñ</b>\n\n"
                response += "Ð”Ð¾Ð´Ð°Ð¹Ñ‚Ðµ Ñ‚Ð¾Ð²Ð°Ñ€Ð¸ Ð· ÐºÐ°Ñ‚Ð°Ð»Ð¾Ð³Ñƒ Ð¿ÐµÑ€ÐµÐ´ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½ÑÐ¼ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ!"
                await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
                return
            
            Database.save_user_session(user_id, "full_order_name", {})
            
            response = "ðŸ›’ <b>ÐžÑ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ</b>\n\n"
            response += f"ðŸ“¦ Ð£ Ð²Ð°ÑˆÑ–Ð¹ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñ–: <b>{len(cart_items)} Ñ‚Ð¾Ð²Ð°Ñ€(Ñ–Ð²)</b>\n"
            
            total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
            response += f"ðŸ’° Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°: <b>{total:.2f} Ð³Ñ€Ð½</b>\n\n"
            response += "ðŸ“ <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð²Ð°ÑˆÐµ ÐŸÐ†Ð‘ (Ð¿Ð¾Ð²Ð½Ðµ Ñ–Ð¼'Ñ):</b>\n\n"
            response += "<i>ÐÐ°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: Ð†Ð²Ð°Ð½Ð¾Ð² Ð†Ð²Ð°Ð½ Ð†Ð²Ð°Ð½Ð¾Ð²Ð¸Ñ‡</i>"
            
            await context.bot.send_message(chat_id=chat_id, text=response, parse_mode='HTML')
            return
        
        elif data == "clear_cart":
            Database.clear_cart(user_id)
            response = "ðŸ—‘ï¸ <b>ÐšÐ¾Ñ€Ð·Ð¸Ð½Ð° Ð¾Ñ‡Ð¸Ñ‰ÐµÐ½Ð°!</b>\n\n"
            response += "Ð’Ð°ÑˆÐ° ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð° Ñ‚ÐµÐ¿ÐµÑ€ Ð¿Ð¾Ñ€Ð¾Ð¶Ð½Ñ.\n"
            response += "<i>Ð”Ð¾Ð´Ð°Ð¹Ñ‚Ðµ Ñ‚Ð¾Ð²Ð°Ñ€Ð¸ Ð· ÐºÐ°Ñ‚Ð°Ð»Ð¾Ð³Ñƒ.</i>"
            await query.edit_message_text(response, reply_markup=get_back_keyboard("main_menu"), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐ¬ ==============
        
        elif data.startswith("user_order_"):
            order_id = int(data.split("_")[2])
            await query.edit_message_text(
                f"ðŸ“‹ Ð”ÐµÑ‚Ð°Ð»Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ #{order_id} (Ð² Ñ€Ð¾Ð·Ñ€Ð¾Ð±Ñ†Ñ–)",
                reply_markup=get_back_keyboard("my_orders")
            )
            return
        
        # ============== ÐžÐ‘Ð ÐžÐ‘ÐÐ˜ÐšÐ˜ ÐŸÐ†Ð”Ð¢Ð’Ð•Ð Ð”Ð–Ð•ÐÐÐ¯ Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ ==============
        
        elif data.startswith("confirm_order_"):
            if data == "confirm_order_yes":
                session = Database.get_user_session(user_id)
                temp_data = session["temp_data"]
                
                try:
                    order_id = Database.create_order(temp_data)
                    
                    if order_id > 0:
                        logger.info(f"\n{'='*80}")
                        logger.info(f"âœ… ÐÐžÐ’Ð• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id}:")
                        logger.info(f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {temp_data.get('user_name', '')}")
                        logger.info(f"ðŸ“ž Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {temp_data.get('phone', '')}")
                        logger.info(f"ðŸ™ï¸ ÐœÑ–ÑÑ‚Ð¾: {temp_data.get('city', '')}")
                        logger.info(f"ðŸ£ ÐÐŸ: {temp_data.get('np_department', '')}")
                        logger.info(f"ðŸ’° Ð¡ÑƒÐ¼Ð°: {temp_data.get('total', 0):.2f} Ð³Ñ€Ð½")
                        logger.info(f"ðŸ›’ Ð¢Ð¾Ð²Ð°Ñ€Ñ–Ð²: {len(temp_data.get('items', []))}")
                        logger.info(f"ðŸ†” User ID: {user_id}")
                        logger.info(f"{'='*80}\n")
                        
                        temp_data["order_id"] = order_id
                        temp_data["status"] = "Ð½Ð¾Ð²Ðµ"
                        temp_data["order_type"] = "regular"
                        log_order(temp_data)
                        
                        await notify_admins_about_new_order(temp_data)
                        
                        Database.clear_user_session(user_id)
                        
                        text = f"âœ… <b>Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð¾!</b>\n\n"
                        text += f"ðŸ†” ÐÐ¾Ð¼ÐµÑ€ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: <b>#{order_id}</b>\n"
                        text += f"ðŸ‘¤ ÐŸÐ†Ð‘: <b>{temp_data.get('user_name', '')}</b>\n"
                        text += f"ðŸ“± Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: <b>{temp_data.get('phone', '')}</b>\n"
                        text += f"ðŸ™ï¸ ÐœÑ–ÑÑ‚Ð¾: <b>{temp_data.get('city', '')}</b>\n"
                        text += f"ðŸ£ Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ ÐÐ¾Ð²Ð¾Ñ— ÐŸÐ¾ÑˆÑ‚Ð¸: <b>{temp_data.get('np_department', '')}</b>\n"
                        text += f"ðŸ’° Ð¡ÑƒÐ¼Ð°: <b>{temp_data.get('total', 0):.2f} Ð³Ñ€Ð½</b>\n\n"
                        text += "ðŸ“ž <b>ÐœÐ¸ Ð·Ð²'ÑÐ¶ÐµÐ¼Ð¾ÑÑŒ Ð· Ð²Ð°Ð¼Ð¸ Ð´Ð»Ñ Ð¿Ñ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¶ÐµÐ½Ð½Ñ!</b>\n\n"
                        text += "<i>Ð”ÑÐºÑƒÑ”Ð¼Ð¾ Ð·Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ! ðŸŒ±</i>"
                    else:
                        text = "âŒ <b>ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ!</b>\n\n"
                        text += "Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð· Ð°Ð±Ð¾ Ð·Ð²'ÑÐ¶Ñ–Ñ‚ÑŒÑÑ Ð· Ð½Ð°Ð¼Ð¸.\n\n"
                        text += "<i>Ð’Ð¸Ð±Ð°Ñ‡Ñ‚Ðµ Ð·Ð° Ð½ÐµÐ·Ñ€ÑƒÑ‡Ð½Ð¾ÑÑ‚Ñ–.</i>"
                        Database.clear_user_session(user_id)
                except Exception as e:
                    logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ€Ð¸ ÑÑ‚Ð²Ð¾Ñ€ÐµÐ½Ð½Ñ– Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ: {e}")
                    text = "âŒ <b>ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ!</b>\n\n"
                    text += "Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð·.\n\n"
                    text += "<i>Ð’Ð¸Ð±Ð°Ñ‡Ñ‚Ðµ Ð·Ð° Ð½ÐµÐ·Ñ€ÑƒÑ‡Ð½Ð¾ÑÑ‚Ñ–.</i>"
                    Database.clear_user_session(user_id)
            else:
                text = "âŒ <b>Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾</b>\n\n"
                text += "Ð’Ð¸ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð¿Ñ€Ð¾Ð´Ð¾Ð²Ð¶Ð¸Ñ‚Ð¸ Ð¿Ð¾ÐºÑƒÐ¿ÐºÐ¸.\n"
                text += "<i>Ð’Ð°ÑˆÐ° ÐºÐ¾Ñ€Ð·Ð¸Ð½Ð° Ð·Ð±ÐµÑ€ÐµÐ¶ÐµÐ½Ð°.</i>"
                Database.clear_user_session(user_id)
            
            await query.edit_message_text(text, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        else:
            logger.warning(f"âš ï¸ ÐÐµÐ²Ñ–Ð´Ð¾Ð¼Ð¸Ð¹ callback: {data}")
            welcome = get_welcome_text()
            await query.edit_message_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð±Ñ€Ð¾Ð±ÐºÐ¸ callback: {e}")
        try:
            text = "âŒ <b>Ð¡Ñ‚Ð°Ð»Ð°ÑÑ Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°</b>\n\n"
            text += "Ð‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð· Ð°Ð±Ð¾ Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð°Ð¹Ñ‚Ðµ /start"
            keyboard = get_main_menu()
            await query.edit_message_text(text, keyboard, parse_mode='HTML')
        except:
            pass

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        text = update.message.text.strip()
        
        logger.info(f"ðŸ‘¤ [{datetime.now().strftime('%H:%M:%S')}] {user.first_name or 'ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡'}: {text[:50]}...")
        
        Database.save_user(user_id, user.first_name, user.last_name or "", user.username or "")
        
        # Ð¡Ð¿Ð¾Ñ‡Ð°Ñ‚ÐºÑƒ Ð¿ÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ñ†Ðµ Ð½Ðµ ÐºÐ¾Ð¼Ð°Ð½Ð´Ð° Ð´Ð»Ñ Ð°Ð´Ð¼Ñ–Ð½Ð°
        if text.startswith('/'):
            # ÐÐ´Ð¼Ñ–Ð½-ÐºÐ¾Ð¼Ð°Ð½Ð´Ð¸ Ð¾Ð±Ñ€Ð¾Ð±Ð»ÑÑŽÑ‚ÑŒÑÑ Ð¾ÐºÑ€ÐµÐ¼Ð¾
            return
        
        # ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€ÑÑ”Ð¼Ð¾ Ñ‡Ð¸ Ñ” Ð°ÐºÑ‚Ð¸Ð²Ð½Ð° ÑÐµÑÑ–Ñ Ð´Ð»Ñ Ð°Ð´Ð¼Ñ–Ð½Ð°
        if 'setphoto_product_id' in context.user_data:
            await handle_admin_url(update, context)
            return
        
        # Ð—Ð²Ð¸Ñ‡Ð°Ð¹Ð½Ð° Ð¾Ð±Ñ€Ð¾Ð±ÐºÐ° Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ
        if text == "/start" or text == "/cancel" or text.lower() == "ÑÐºÐ°ÑÑƒÐ²Ð°Ñ‚Ð¸":
            Database.clear_user_session(user_id)
            welcome = get_welcome_text()
            await update.message.reply_text(welcome, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        if text == "/help":
            await update.message.reply_text("â„¹ï¸ Ð”Ð¾Ð¿Ð¾Ð¼Ð¾Ð³Ð°: Ð¾Ð±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¾Ð¿Ñ†Ñ–ÑŽ Ð· Ð¼ÐµÐ½ÑŽ", reply_markup=get_main_menu())
            return
        
        session = Database.get_user_session(user_id)
        state = session["state"]
        temp_data = session["temp_data"]
        
        if state == "waiting_quantity":
            product_id = temp_data.get("product_id")
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            
            if not product:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            success, quantity, error_msg = parse_quantity(text)
            
            if not success:
                response = f"âŒ <b>ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚!</b>\n\n{error_msg}\n\n"
                response += f"<b>ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚:</b> {product['name']}\n"
                response += f"<b>Ð¦Ñ–Ð½Ð°:</b> {product['price']} Ð³Ñ€Ð½/{product['unit']}\n\n"
                response += "ðŸ“Š <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ ÐºÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ (Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ñ‡Ð¸ÑÐ»Ð¾):</b>\n"
                response += f"<i>ÐÐ°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: 1, 2, 3 (Ð² {product['unit']})</i>"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            Database.add_to_cart(user_id, product_id, quantity)
            Database.clear_user_session(user_id)
            
            total_price = product["price"] * quantity
            response = f"âœ… <b>{product['name']}</b> Ð´Ð¾Ð´Ð°Ð½Ð¾ Ð´Ð¾ ÐºÐ¾ÑˆÐ¸ÐºÐ°!\n\n"
            response += f"ðŸ“Š ÐšÑ–Ð»ÑŒÐºÑ–ÑÑ‚ÑŒ: <b>{quantity} {product['unit']}</b>\n"
            response += f"ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']}\n"
            response += f"ðŸ’µ Ð¡ÑƒÐ¼Ð°: <b>{total_price:.2f} Ð³Ñ€Ð½</b>\n\n"
            
            cart_items = Database.get_cart_items(user_id)
            response += f"ðŸ›’ Ð£ ÐºÐ¾ÑˆÐ¸ÐºÑƒ: <b>{len(cart_items)} Ñ‚Ð¾Ð²Ð°Ñ€(Ñ–Ð²)</b>\n\n"
            response += "<i>ÐŸÑ€Ð¾Ð´Ð¾Ð²Ð¶ÑƒÐ¹Ñ‚Ðµ Ð´Ð¾Ð´Ð°Ð²Ð°Ñ‚Ð¸ Ñ‚Ð¾Ð²Ð°Ñ€Ð¸ Ð°Ð±Ð¾ Ð¿ÐµÑ€ÐµÐ¹Ð´Ñ–Ñ‚ÑŒ Ð´Ð¾ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð½Ñ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ.</i>"
            
            await update.message.reply_text(response, parse_mode='HTML')
            
            products_text = "ðŸ“¦ <b>ÐÐ°ÑˆÑ– Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚Ð¸</b>\n\nÐžÐ±ÐµÑ€Ñ–Ñ‚ÑŒ Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚ Ð´Ð»Ñ Ð´ÐµÑ‚Ð°Ð»ÑŒÐ½Ð¾Ñ— Ñ–Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–Ñ—:"
            await update.message.reply_text(products_text, reply_markup=get_products_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="products")
            return
        
        elif state == "waiting_message":
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'Ð½ÐµÐ¼Ð°Ñ”'
            
            Database.save_message(user_id, user_name, username, text, "Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ð¼ÐµÐ½ÑŽ")
            
            message_data = {
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð· Ð¼ÐµÐ½ÑŽ",
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            await notify_admins_about_message(message_data)
            
            log_message(message_data)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"ðŸ’¬ ÐÐžÐ’Ð• ÐŸÐžÐ’Ð†Ð”ÐžÐœÐ›Ð•ÐÐÐ¯:")
            logger.info(f"ðŸ‘¤ Ð†Ð¼'Ñ: {user_name}")
            logger.info(f"ðŸ“± Username: {username}")
            logger.info(f"ðŸ†” ID: {user_id}")
            logger.info(f"ðŸ’¬ Ð¢ÐµÐºÑÑ‚: {text}")
            logger.info(f"ðŸ•’ Ð§Ð°Ñ: {datetime.now().isoformat()}")
            logger.info(f"{'='*80}\n")
            
            response = "âœ… <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾!</b>\n\n"
            response += "ÐœÐ¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð¼Ð¾ Ð²Ð°Ð¼ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼.\n"
            response += "<i>Ð”ÑÐºÑƒÑ”Ð¼Ð¾ Ð·Ð° Ð·Ð²ÐµÑ€Ð½ÐµÐ½Ð½Ñ! ðŸŒ±</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.clear_user_session(user_id)
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        elif state == "waiting_message_for_quick_order":
            order_id = temp_data.get("order_id")
            product_name = temp_data.get("product_name")
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'Ð½ÐµÐ¼Ð°Ñ”'
            
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
                    logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {e}")
                finally:
                    conn.close()
            
            Database.save_message(user_id, user_name, username, text, "ÑˆÐ²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ")
            
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
                "status": "Ð½Ð¾Ð²Ðµ"
            })
            
            logger.info(f"\n{'='*80}")
            logger.info(f"âœ… Ð¨Ð’Ð˜Ð”ÐšÐ• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id} - Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:")
            logger.info(f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {user_name}")
            logger.info(f"ðŸ“± Username: {username}")
            logger.info(f"ðŸ“¦ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {product_name}")
            logger.info(f"ðŸ’¬ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ: {text}")
            logger.info(f"{'='*80}\n")
            
            response = f"âœ… <b>Ð”ÑÐºÑƒÑ”Ð¼Ð¾! Ð’Ð°ÑˆÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾!</b>\n\n"
            response += f"ðŸ†” <b>ÐÐ¾Ð¼ÐµÑ€ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:</b> #{order_id}\n"
            response += f"ðŸ“¦ <b>ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚:</b> {product_name}\n"
            response += f"ðŸ’¬ <b>Ð’Ð°ÑˆÐµ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ:</b> {text}\n\n"
            response += "<b>ÐœÐ¸ Ð·Ð²'ÑÐ¶ÐµÐ¼Ð¾ÑÑ Ð· Ð²Ð°Ð¼Ð¸ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼ Ð´Ð»Ñ ÑƒÑ‚Ð¾Ñ‡Ð½ÐµÐ½Ð½Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹!</b>\n\n"
            response += "<i>Ð”ÑÐºÑƒÑ”Ð¼Ð¾ Ð·Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ! ðŸŒ±</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.clear_user_session(user_id)
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        elif state.startswith("full_order_"):
            if state == "full_order_name":
                temp_data["user_name"] = text
                temp_data["username"] = user.username or "Ð½ÐµÐ¼Ð°Ñ”"
                Database.save_user_session(user_id, "full_order_phone", temp_data)
                
                response = "ðŸ“± <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð²Ð°Ñˆ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ:</b>\n\n"
                response += "<i>ÐŸÑ€Ð¸ÐºÐ»Ð°Ð´: +380932599103 Ð°Ð±Ð¾ 0932599103</i>"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_phone":
                phone = text.strip()
                is_valid, formatted_phone = validate_phone(phone)
                
                if not is_valid:
                    response = f"âŒ <b>ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ!</b>\n\n"
                    response += "ðŸ“± <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð²Ð°Ñˆ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ Ñ‰Ðµ Ñ€Ð°Ð·:</b>\n"
                    response += "<i>ÐŸÑ€Ð¸ÐºÐ»Ð°Ð´: +380932599103 Ð°Ð±Ð¾ 0932599103</i>"
                    await update.message.reply_text(response, parse_mode='HTML')
                    return
                
                temp_data["phone"] = formatted_phone
                Database.save_user_session(user_id, "full_order_city", temp_data)
                
                response = "ðŸ™ï¸ <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð¼Ñ–ÑÑ‚Ð¾ Ð´Ð¾ÑÑ‚Ð°Ð²ÐºÐ¸:</b>\n\n"
                response += "<i>ÐÐ°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: ÐšÐ¸Ñ—Ð², Ð›ÑŒÐ²Ñ–Ð², ÐžÐ´ÐµÑÐ°</i>"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_city":
                temp_data["city"] = text
                Database.save_user_session(user_id, "full_order_np", temp_data)
                
                response = "ðŸ£ <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð½Ð¾Ð¼ÐµÑ€ Ð²Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ ÐÐ¾Ð²Ð¾Ñ— ÐŸÐ¾ÑˆÑ‚Ð¸:</b>\n\n"
                response += "<i>ÐÐ°Ð¿Ñ€Ð¸ÐºÐ»Ð°Ð´: Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ â„–25, ÐŸÐ¾ÑˆÑ‚Ð¾Ð¼Ð°Ñ‚ â„–12345</i>"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            elif state == "full_order_np":
                temp_data["np_department"] = text
                
                cart_items = Database.get_cart_items(user_id)
                total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                
                if len(cart_items) >= 3:
                    total = total * 0.95
                
                temp_data["total"] = total
                temp_data["order_type"] = "Ð¿Ð¾Ð²Ð½Ðµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ"
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
                
                response = "âœ… <b>Ð”Ð°Ð½Ñ– Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾! ÐŸÐµÑ€ÐµÐ²Ñ–Ñ€Ñ‚Ðµ Ñ–Ð½Ñ„Ð¾Ñ€Ð¼Ð°Ñ†Ñ–ÑŽ:</b>\n\n"
                response += f"ðŸ‘¤ <b>ÐŸÐ†Ð‘:</b> {temp_data.get('user_name', '')}\n"
                response += f"ðŸ“± <b>Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½:</b> {temp_data.get('phone', '')}\n"
                response += f"ðŸ™ï¸ <b>ÐœÑ–ÑÑ‚Ð¾:</b> {temp_data.get('city', '')}\n"
                response += f"ðŸ£ <b>Ð’Ñ–Ð´Ð´Ñ–Ð»ÐµÐ½Ð½Ñ ÐÐ¾Ð²Ð¾Ñ— ÐŸÐ¾ÑˆÑ‚Ð¸:</b> {text}\n"
                response += f"ðŸ›’ <b>Ð¢Ð¾Ð²Ð°Ñ€Ñ–Ð² Ñƒ ÐºÐ¾ÑˆÐ¸ÐºÑƒ:</b> {len(cart_items)}\n"
                
                if len(cart_items) >= 3:
                    original_total = sum(item["product"]["price"] * item["quantity"] for item in cart_items)
                    discount = original_total * 0.05
                    response += f"ðŸŽ <b>Ð—Ð½Ð¸Ð¶ÐºÐ° 5% Ð·Ð° 3+ Ð±Ð°Ð½Ð¾Ðº:</b> -{discount:.2f} Ð³Ñ€Ð½\n"
                
                response += f"ðŸ’° <b>Ð—Ð°Ð³Ð°Ð»ÑŒÐ½Ð° ÑÑƒÐ¼Ð°:</b> {total:.2f} Ð³Ñ€Ð½\n\n"
                response += "<b>ÐŸÑ–Ð´Ñ‚Ð²ÐµÑ€Ð´Ð¸Ñ‚Ð¸ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ?</b>"
                
                await update.message.reply_text(response, reply_markup=get_order_confirmation_keyboard(), parse_mode='HTML')
                return
        
        elif state == "waiting_phone_for_quick_order":
            phone = text.strip()
            product_id = temp_data.get("product_id")
            
            refresh_products()
            product = next((p for p in PRODUCTS if p["id"] == product_id), None)
            if not product:
                await update.message.reply_text("âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ°: Ð¿Ñ€Ð¾Ð´ÑƒÐºÑ‚ Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾", reply_markup=get_main_menu())
                Database.clear_user_session(user_id)
                return
            
            is_valid, formatted_phone = validate_phone(phone)
            
            if not is_valid:
                response = f"âŒ <b>ÐÐµÐ²Ñ–Ñ€Ð½Ð¸Ð¹ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ!</b>\n\n"
                response += "ðŸ“± <b>Ð’Ð²ÐµÐ´Ñ–Ñ‚ÑŒ Ð²Ð°Ñˆ Ð½Ð¾Ð¼ÐµÑ€ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½Ñƒ Ñ‰Ðµ Ñ€Ð°Ð·:</b>\n"
                response += "<i>ÐŸÑ€Ð¸ÐºÐ»Ð°Ð´: +380932599103 Ð°Ð±Ð¾ 0932599103</i>"
                await update.message.reply_text(response, parse_mode='HTML')
                return
            
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'Ð½ÐµÐ¼Ð°Ñ”'
            
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
                "status": "Ð½Ð¾Ð²Ðµ"
            })
            
            logger.info(f"\n{'='*80}")
            logger.info(f"âš¡ Ð¨Ð’Ð˜Ð”ÐšÐ• Ð—ÐÐœÐžÐ’Ð›Ð•ÐÐÐ¯ #{order_id} (Ð¢Ð•Ð›Ð•Ð¤ÐžÐ):")
            logger.info(f"ðŸ‘¤ ÐšÐ»Ñ–Ñ”Ð½Ñ‚: {user_name}")
            logger.info(f"ðŸ“ž Ð¢ÐµÐ»ÐµÑ„Ð¾Ð½: {formatted_phone}")
            logger.info(f"ðŸ“¦ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚: {product['name']}")
            logger.info(f"ðŸ’° Ð¦Ñ–Ð½Ð°: {product['price']} Ð³Ñ€Ð½/{product['unit']}")
            logger.info(f"ðŸ†” User ID: {user_id}")
            logger.info(f"ðŸ“± Username: {username}")
            logger.info(f"{'='*80}\n")
            
            Database.clear_user_session(user_id)
            
            response = f"âœ… <b>Ð¨Ð²Ð¸Ð´ÐºÐµ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ Ð¿Ñ€Ð¸Ð¹Ð½ÑÑ‚Ð¾!</b>\n\n"
            response += f"ðŸ†” <b>ÐÐ¾Ð¼ÐµÑ€ Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ:</b> #{order_id}\n"
            response += f"ðŸ“¦ <b>ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚:</b> {product['name']}\n"
            response += f"ðŸ“ž <b>Ð’Ð°Ñˆ Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½:</b> {formatted_phone}\n\n"
            response += "<b>ÐœÐ¸ Ð·Ð°Ñ‚ÐµÐ»ÐµÑ„Ð¾Ð½ÑƒÑ”Ð¼Ð¾ Ð²Ð°Ð¼ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼ Ð´Ð»Ñ ÑƒÑ‚Ð¾Ñ‡Ð½ÐµÐ½Ð½Ñ Ð´ÐµÑ‚Ð°Ð»ÐµÐ¹!</b>\n\n"
            response += "<i>Ð”ÑÐºÑƒÑ”Ð¼Ð¾ Ð·Ð° Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½Ð½Ñ! ðŸŒ±</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            return
        
        else:
            user_name = f"{user.first_name or ''} {user.last_name or ''}"
            username = user.username or 'Ð½ÐµÐ¼Ð°Ñ”'
            
            Database.save_message(user_id, user_name, username, text, "Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ñ‡Ð°Ñ‚Ñ–")
            
            message_data = {
                "user_id": user_id,
                "user_name": user_name,
                "username": username,
                "text": text,
                "message_type": "Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð² Ñ‡Ð°Ñ‚Ñ–",
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            await notify_admins_about_message(message_data)
            
            log_message(message_data)
            
            response = "âœ… <b>ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½Ð½Ñ Ð¾Ñ‚Ñ€Ð¸Ð¼Ð°Ð½Ð¾!</b>\n\n"
            response += "ÐœÐ¸ Ð²Ñ–Ð´Ð¿Ð¾Ð²Ñ–Ð¼Ð¾ Ð²Ð°Ð¼ Ð½Ð°Ð¹Ð±Ð»Ð¸Ð¶Ñ‡Ð¸Ð¼ Ñ‡Ð°ÑÐ¾Ð¼.\n"
            response += "<i>Ð”ÑÐºÑƒÑ”Ð¼Ð¾ Ð·Ð° Ð·Ð²ÐµÑ€Ð½ÐµÐ½Ð½Ñ! ðŸŒ±</i>"
            
            await update.message.reply_text(response, reply_markup=get_main_menu(), parse_mode='HTML')
            Database.save_user_session(user_id, last_section="main_menu")
            
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² message_handler: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.error(f"âš ï¸ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð¿Ñ–Ð´ Ñ‡Ð°Ñ Ð¾Ð±Ñ€Ð¾Ð±ÐºÐ¸ Ð¾Ð½Ð¾Ð²Ð»ÐµÐ½Ð½Ñ {update}: {context.error}")
        
        if 'Conflict' in str(context.error):
            logger.warning("ðŸ”„ Ð’Ð¸ÑÐ²Ð»ÐµÐ½Ð¾ ÐºÐ¾Ð½Ñ„Ð»Ñ–ÐºÑ‚ - Ð¼Ð¾Ð¶Ð»Ð¸Ð²Ð¾ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾ Ð´ÑƒÐ±Ð»ÑŽÑŽÑ‡Ð¸Ð¹ Ð±Ð¾Ñ‚")
            return
        
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="âŒ <b>Ð’Ð¸Ð½Ð¸ÐºÐ»Ð° Ð¿Ð¾Ð¼Ð¸Ð»ÐºÐ°</b>\n\nÐ‘ÑƒÐ´ÑŒ Ð»Ð°ÑÐºÐ°, ÑÐ¿Ñ€Ð¾Ð±ÑƒÐ¹Ñ‚Ðµ Ñ‰Ðµ Ñ€Ð°Ð· Ð°Ð±Ð¾ Ð²Ð¸ÐºÐ¾Ñ€Ð¸ÑÑ‚Ð°Ð¹Ñ‚Ðµ /start",
                    parse_mode='HTML'
                )
            except:
                pass
    except Exception as e:
        logger.error(f"âŒ ÐŸÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð² Ð¾Ð±Ñ€Ð¾Ð±Ð½Ð¸ÐºÑƒ Ð¿Ð¾Ð¼Ð¸Ð»Ð¾Ðº: {e}")

def main():
    try:
        if not check_single_instance():
            logger.error("ðŸš« Ð‘Ð¾Ñ‚ Ð²Ð¶Ðµ Ð·Ð°Ð¿ÑƒÑ‰ÐµÐ½Ð¾ Ð² Ñ–Ð½ÑˆÐ¾Ð¼Ñƒ Ð¿Ñ€Ð¾Ñ†ÐµÑÑ–! Ð—Ð°Ð²ÐµÑ€ÑˆÑƒÑ”Ð¼Ð¾...")
            sys.exit(1)
        
        time.sleep(2)
        
        if not init_database():
            logger.error("âŒ ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ñ–Ð½Ñ–Ñ†Ñ–Ð°Ð»Ñ–Ð·ÑƒÐ²Ð°Ñ‚Ð¸ Ð±Ð°Ð·Ñƒ Ð´Ð°Ð½Ð¸Ñ…")
            return
        
        refresh_products()
        
        stats = Database.get_statistics()
        logger.info("=" * 80)
        logger.info("ðŸŒ± Ð‘ÐžÐ¢ ÐšÐžÐœÐŸÐÐÐ†Ð‡ 'Ð‘ÐžÐÐ•Ð›Ð•Ð¢' Ð—ÐÐŸÐ£Ð©Ð•ÐÐž")
        logger.info(f"ðŸ”‘ Ð¢Ð¾ÐºÐµÐ½: {TOKEN[:10]}...")
        logger.info("=" * 80)
        logger.info("ðŸ“Š Ð¡Ñ‚Ð°Ñ‚Ð¸ÑÑ‚Ð¸ÐºÐ°:")
        logger.info(f"â€¢ ÐšÐ¾Ñ€Ð¸ÑÑ‚ÑƒÐ²Ð°Ñ‡Ñ–Ð²: {stats.get('total_users', 0)}")
        logger.info(f"â€¢ Ð—Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('total_orders', 0)}")
        logger.info(f"â€¢ ÐŸÐ¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ: {stats.get('total_messages', 0)}")
        logger.info(f"â€¢ Ð¨Ð²Ð¸Ð´ÐºÐ¸Ñ… Ð·Ð°Ð¼Ð¾Ð²Ð»ÐµÐ½ÑŒ: {stats.get('quick_orders', 0)}")
        logger.info(f"â€¢ ÐÐºÑ‚Ð¸Ð²Ð½Ð¸Ñ… ÐºÐ¾ÑˆÐ¸ÐºÑ–Ð²: {stats.get('active_carts', 0)}")
        logger.info(f"â€¢ ÐŸÑ€Ð¾Ð´ÑƒÐºÑ‚Ñ–Ð² Ñƒ Ð±Ð°Ð·Ñ–: {len(PRODUCTS)}")
        logger.info(f"â€¢ Ð’Ð¸Ñ€ÑƒÑ‡ÐºÐ°: {stats.get('total_revenue', 0):.2f} Ð³Ñ€Ð½")
        logger.info("=" * 80)
        logger.info("ðŸ”„ ÐžÑ‡Ñ–ÐºÑƒÐ²Ð°Ð½Ð½Ñ Ð¿Ð¾Ð²Ñ–Ð´Ð¾Ð¼Ð»ÐµÐ½ÑŒ...\n")
        
        application = Application.builder().token(TOKEN).build()
        
        # Ð—Ð²Ð¸Ñ‡Ð°Ð¹Ð½Ñ– ÐºÐ¾Ð¼Ð°Ð½Ð´Ð¸
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("cancel", cancel_command))
        
        # ÐÐ´Ð¼Ñ–Ð½-ÐºÐ¾Ð¼Ð°Ð½Ð´Ð¸ (Ñ‚Ñ–Ð»ÑŒÐºÐ¸ Ð´Ð»Ñ Ð°Ð´Ð¼Ñ–Ð½Ñ–Ð²)
        application.add_handler(CommandHandler("setphoto", setphoto_command))
        
        # ÐžÐ±Ñ€Ð¾Ð±Ð½Ð¸ÐºÐ¸
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(MessageHandler(filters.PHOTO, handle_admin_photo))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        
        application.add_error_handler(error_handler)
        
        logger.info("ðŸš€ Ð—Ð°Ð¿ÑƒÑÐº polling...")
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
        logger.error(f"âŒ ÐšÐ Ð˜Ð¢Ð˜Ð§ÐÐ ÐŸÐžÐœÐ˜Ð›ÐšÐ: {e}")
        import traceback
        logger.error(traceback.format_exc())
        time.sleep(10)

if __name__ == "__main__":
    main()
