import os
import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ========== НАЛАШТУВАННЯ ==========
logging.basicConfig(
    format='%(asctime)s - ADMIN - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("ADMIN_BOT_TOKEN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")  # Змініть пізніше
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

# Словник для зберігання сесій адмінів (в пам'яті)
admin_sessions = {}

# ========== ГОЛОВНЕ МЕНЮ АДМІНА ==========
async def admin_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головне меню адмін-панелі"""
    keyboard = [
        [InlineKeyboardButton("📦 Товари", callback_data="admin_products")],
        [InlineKeyboardButton("📋 Замовлення", callback_data="admin_orders")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("👥 Користувачі", callback_data="admin_users")],
        [InlineKeyboardButton("⚙️ Налаштування", callback_data="admin_settings")],
        [InlineKeyboardButton("🔐 Вийти", callback_data="admin_logout")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "🔐 <b>Адмін-панель Бонелет</b>\n\n"
    text += "Вітаю в системі керування!\n"
    text += "Оберіть розділ для роботи:"
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')

# ========== ОБРОБНИК КОМАНДИ /START ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запит пароля при старті"""
    user = update.effective_user
    user_id = user.id
    
    # Перевірка чи є ID в списку дозволених
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await update.message.reply_text(
            "❌ <b>Доступ заборонено</b>\n\n"
            "Ви не маєте прав адміністратора.",
            parse_mode='HTML'
        )
        return
    
    # Запит пароля
    admin_sessions[user_id] = {"state": "waiting_password"}
    
    await update.message.reply_text(
        "🔐 <b>Вхід в адмін-панель</b>\n\n"
        "Будь ласка, введіть пароль:",
        parse_mode='HTML'
    )

# ========== ПЕРЕВІРКА ПАРОЛЯ ==========
async def check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перевірка введеного пароля"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    
    # Перевіряємо чи чекаємо на пароль
    if user_id not in admin_sessions or admin_sessions[user_id].get("state") != "waiting_password":
        return
    
    if text == ADMIN_PASSWORD:
        # Пароль вірний
        admin_sessions[user_id] = {"state": "authenticated", "authenticated_at": datetime.now()}
        
        await update.message.reply_text(
            "✅ <b>Пароль прийнято!</b>\n\n"
            "Ласкаво просимо до адмін-панелі.",
            parse_mode='HTML'
        )
        await admin_main_menu(update, context)
    else:
        # Невірний пароль
        await update.message.reply_text(
            "❌ <b>Невірний пароль!</b>\n\n"
            "Спробуйте ще раз або напишіть /start",
            parse_mode='HTML'
        )
        admin_sessions.pop(user_id, None)

# ========== ПЕРЕВІРКА АВТОРИЗАЦІЇ ==========
def is_authenticated(user_id: int) -> bool:
    """Перевіряє чи авторизований адмін"""
    return user_id in admin_sessions and admin_sessions[user_id].get("state") == "authenticated"

# ========== ОБРОБНИК КНОПОК ==========
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка натискань кнопок"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_id = user.id
    data = query.data
    
    # Перевірка авторизації
    if not is_authenticated(user_id):
        await query.edit_message_text(
            "❌ <b>Сесія закінчилась</b>\n\n"
            "Напишіть /start для повторного входу",
            parse_mode='HTML'
        )
        return
    
    # ===== РОЗДІЛ ТОВАРІВ =====
    if data == "admin_products":
        keyboard = [
            [InlineKeyboardButton("📋 Список товарів", callback_data="admin_product_list")],
            [InlineKeyboardButton("➕ Додати товар", callback_data="admin_product_add")],
            [InlineKeyboardButton("✏️ Редагувати товар", callback_data="admin_product_edit")],
            [InlineKeyboardButton("🗑 Видалити товар", callback_data="admin_product_delete")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
        ]
        text = "📦 <b>Керування товарами</b>\n\nОберіть дію:"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== РОЗДІЛ ЗАМОВЛЕНЬ =====
    elif data == "admin_orders":
        keyboard = [
            [InlineKeyboardButton("📋 Всі замовлення", callback_data="admin_order_all")],
            [InlineKeyboardButton("🆕 Нові замовлення", callback_data="admin_order_new")],
            [InlineKeyboardButton("⚡ Швидкі замовлення", callback_data="admin_order_quick")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
        ]
        text = "📋 <b>Керування замовленнями</b>\n\nОберіть тип замовлень:"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== СТАТИСТИКА =====
    elif data == "admin_stats":
        text = "📊 <b>Статистика</b>\n\n"
        text += "Тут буде статистика з основного бота\n\n"
        text += "<i>Функція в розробці</i>"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== КОРИСТУВАЧІ =====
    elif data == "admin_users":
        text = "👥 <b>Користувачі</b>\n\n"
        text += "Тут буде список користувачів\n\n"
        text += "<i>Функція в розробці</i>"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== НАЛАШТУВАННЯ =====
    elif data == "admin_settings":
        keyboard = [
            [InlineKeyboardButton("🔑 Змінити пароль", callback_data="admin_settings_password")],
            [InlineKeyboardButton("📢 Розсилка", callback_data="admin_settings_broadcast")],
            [InlineKeyboardButton("⚙️ Інші налаштування", callback_data="admin_settings_other")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]
        ]
        text = "⚙️ <b>Налаштування</b>\n\nОберіть розділ:"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== ВИХІД =====
    elif data == "admin_logout":
        admin_sessions.pop(user_id, None)
        await query.edit_message_text(
            "🔐 <b>Ви вийшли з адмін-панелі</b>\n\n"
            "Для повторного входу напишіть /start",
            parse_mode='HTML'
        )
    
    # ===== НАЗАД ДО ГОЛОВНОГО МЕНЮ =====
    elif data == "admin_back_main":
        keyboard = [
            [InlineKeyboardButton("📦 Товари", callback_data="admin_products")],
            [InlineKeyboardButton("📋 Замовлення", callback_data="admin_orders")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("👥 Користувачі", callback_data="admin_users")],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="admin_settings")],
            [InlineKeyboardButton("🔐 Вийти", callback_data="admin_logout")]
        ]
        text = "🔐 <b>Адмін-панель Бонелет</b>\n\nОберіть розділ:"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # ===== ТИМЧАСОВІ ЗАГОТОВКИ =====
    elif data in ["admin_product_list", "admin_product_add", "admin_product_edit", 
                  "admin_product_delete", "admin_order_all", "admin_order_new",
                  "admin_order_quick", "admin_settings_password", "admin_settings_broadcast",
                  "admin_settings_other"]:
        text = f"🛠 <b>Функція в розробці</b>\n\n"
        text += f"Callback: <code>{data}</code>\n\n"
        text += "Незабаром тут з'явиться повноцінний функціонал."
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

# ========== ОБРОБНИК ТЕКСТОВИХ ПОВІДОМЛЕНЬ ==========
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка всіх текстових повідомлень"""
    user = update.effective_user
    user_id = user.id
    
    # Якщо чекаємо на пароль
    if user_id in admin_sessions and admin_sessions[user_id].get("state") == "waiting_password":
        await check_password(update, context)
    else:
        # Ігноруємо інші повідомлення
        pass

# ========== ЗАПУСК БОТА ==========
def main():
    """Запуск адмін-бота"""
    if not TOKEN:
        logger.error("❌ ADMIN_BOT_TOKEN не знайдено!")
        return
    
    # Створюємо додаток
    application = Application.builder().token(TOKEN).build()
    
    # Додаємо обробники
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    logger.info("🚀 Адмін-бот запущено!")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    from datetime import datetime
    main()
