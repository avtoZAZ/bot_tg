# bot.py - Telegram Video Bot Premium Edition
# Автор: avtoZAZ
# Дата: 2025-11-11 21:05:45 UTC
# Версія: 3.2.0 (локальна перевірка скасування замість глобальної)

import asyncio
import logging
import random
import string
import time
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Callable, Awaitable
from collections import defaultdict

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, TelegramObject,
    LabeledPrice, PreCheckoutQuery, InputFile
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import async_sessionmaker

# Локальні імпорти
from config import (
    BOT_TOKEN, ADMIN_ID, MAX_REQUESTS_PER_MINUTE, BLOCK_TIME,
    BLOCKED_USERS, ADS_ENABLED, AD_FREQUENCY, ADS_LIST, ADS_ONLY_FREE_USERS,
    CACHE_ENABLED, RECOMMENDATIONS_ENABLED, RECOMMENDATIONS_LIMIT,
    REFERRAL_ENABLED, PROMO_CODES_ENABLED, SERIES_NOTIFICATIONS_ENABLED,
    RATINGS_ENABLED, ANALYTICS_ENABLED, SAVE_SEARCH_QUERIES, TRACK_AD_CLICKS,
    ALLOWED_DOMAINS, VALIDATE_AD_URLS, LOG_SENSITIVE_DATA, PREMIUM_PLANS,
    TOP_WEEKLY_LIMIT, LOGS_DIR
)

from database import (
    init_db, async_session_maker, User, Video, ViewHistory, Favorite,
    Rating, SeriesSubscription, PromoCode, SearchQuery, AdClick, Payment,
    get_or_create_user, add_to_history, get_user_history, toggle_favorite,
    is_favorite, get_user_favorites, rate_video, subscribe_to_series,
    get_series_subscribers, log_search_query, log_ad_click
)

from cache import video_cache, recommendations_cache, top_videos_cache, cache_cleanup_task
from recommendations import get_recommendations, get_top_weekly
from referral import referral_system
from promo import promo_manager
from analytics import analytics

from security import (
    rate_limiter, input_validator, spam_protection, security_logger,
    RateLimiter, InputValidator, SpamProtection, SecurityLogger
)

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOGS_DIR}/bot_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Окремі логери
admin_logger = logging.getLogger('admin')
admin_handler = logging.FileHandler(f'{LOGS_DIR}/admin_actions.log', encoding='utf-8')
admin_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
admin_logger.addHandler(admin_handler)
admin_logger.setLevel(logging.INFO)

premium_logger = logging.getLogger('premium')
premium_handler = logging.FileHandler(f'{LOGS_DIR}/premium.log', encoding='utf-8')
premium_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
premium_logger.addHandler(premium_handler)
premium_logger.setLevel(logging.INFO)

# Глобальні змінні
user_video_count = defaultdict(int)
MAX_VIDEO_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB

# Кеш мов користувачів
user_languages_cache = {}

# ==================== ТЕКСТИ ДЛЯ ВСІХ МОВ ====================

TEXTS = {
    "en": {
        "welcome_message": "Welcome! Send me a code to get your video.",
        "admin_menu_info": "Admin menu is available.",
        "upload_video_button": "📹 Upload Video",
        "add_admin_button": "👤 Add Admin",
        "stats_button": "📊 Statistics",
        "delete_video_button": "🗑️ Delete Video",
        "edit_video_button": "✏️ Edit Metadata",
        "list_videos_button": "📝 All Videos",
        "list_admins_button": "👥 Admins List",
        "export_db_button": "📤 Export DB",
        "list_series_button": "📺 Series List",
        "analytics_button": "📊 Analytics",
        "broadcast_button": "📢 Broadcast",
        "promo_button": "🎫 Promo Codes",
        "back_button": "◀️ Back",
        "cancel_button": "❌ Cancel",
        "premium_button": "⭐ Premium",
        "my_subscription_button": "👤 My Subscription",
        "search_button": "🔍 Search",
        "history_button": "📜 History",
        "favorites_button": "⭐ Favorites",
        "browse_button": "🗂️ Browse",
        "referral_button": "👥 Invite Friends",
        "top_week_button": "🔥 Top Week",
        "add_to_favorites": "⭐ Add to Favorites",
        "remove_from_favorites": "❌ Remove from Favorites",
        "added_to_favorites": "✅ Added to favorites!",
        "removed_from_favorites": "❌ Removed from favorites!",
        "rate_video": "⭐ Rate this video",
        "thanks_for_rating": "✅ Thanks for your rating!",
        "subscribe_to_series": "🔔 Subscribe to series",
        "unsubscribe_from_series": "🔕 Unsubscribe",
        "subscribed_to_series": "✅ You subscribed to {series}!",
        "unsubscribed_from_series": "❌ You unsubscribed from {series}",
        "new_episode_notification": "🎬 New episode available!\n\n📺 {series}\nS{season}E{episode}\n\nCode: <code>{code}</code>",
        "recommendations_header": "🎬 <b>You may also like:</b>\n\n",
        "top_weekly_header": "🔥 <b>Top 5 This Week</b>\n\n",
        "history_header": "📜 <b>Your Watch History</b>\n\n",
        "history_empty": "Your history is empty. Watch some videos!",
        "favorites_header": "⭐ <b>Your Favorites</b>\n\n",
        "favorites_empty": "No favorites yet. Add some videos!",
        "referral_info": """👥 <b>Invite Friends</b>

Your referral link:
<code>{link}</code>

🎁 Rewards:
• Friend joins: +{bonus_days} days premium
• Friend buys premium: +{payment_bonus_days} days premium

📊 Your stats:
• Total referrals: {total}
• Premium referrals: {premium}
• Bonus days earned: {bonus_earned}""",
        "browse_categories": "🗂️ <b>Browse by Category</b>\n\nSelect category:",
        "category_movies": "🎬 Movies",
        "category_series": "📺 Series",
        "category_anime": "🎌 Anime",
        "promo_activate": "🎫 Activate Promo Code",
        "enter_promo_code": "Enter promo code:",
        "promo_activated": "✅ Promo code activated!",
        "create_promo_button": "➕ Create Promo",
        "list_promos_button": "📋 List Promos",
        "broadcast_all": "📢 All Users",
        "broadcast_free": "🆓 Free Users Only",
        "broadcast_premium": "⭐ Premium Only",
        "broadcast_language": "🌍 By Language",
        "enter_broadcast_message": "Enter broadcast message:",
        "broadcast_started": "📢 Broadcast started!\n\nTarget: {target} users",
        "broadcast_completed": """✅ Broadcast completed!

📊 Results:
• Sent: {success}
• Failed: {failed}
• Blocked bot: {blocked}
• Total: {total}""",
        "premium_required": "⭐ This is a PREMIUM video!\n\n💎 Subscribe to Premium to watch this content.",
        "subscribe_button": "💳 Subscribe",
        "premium_info": """⭐ <b>Premium Subscription</b>

🎬 Access to exclusive premium content
📺 Unlimited premium series
✨ No ads
🚀 Priority support

Choose your plan (payment in Telegram Stars ⭐):""",
        "premium_status_active": """✅ <b>Your Premium Status</b>

Status: ⭐ ACTIVE
Plan: {plan}
Valid until: {expires}
Days left: {days_left}

✨ You enjoy ad-free viewing!""",
        "premium_status_inactive": """❌ <b>Your Premium Status</b>

Status: FREE
You don't have an active subscription.

💎 Subscribe to Premium to unlock:
• Exclusive premium content
• Ad-free experience
• Priority support""",
        "premium_purchased": "🎉 Congratulations! Premium activated!\n\n⭐ Benefits:\n• Access to all premium content\n• No more ads!\n• Priority support\n\nValid until: {expires}",
        "is_premium_question": "Is this a premium video? (Premium only)",
        "yes_premium": "⭐ Yes, premium",
        "no_free": "✅ No, free",
        "premium_users_button": "⭐ Premium Users",
        "code_not_found": "❌ Code not found.",
        "rate_limit": "⏱️ Too fast! Wait a moment.",
        "blocked_message": "⛔ You are temporarily blocked due to suspicious activity.",
        "send_video_prompt": "Send me the video file (max 2GB):",
        "video_too_large": "❌ Video is too large! Max size: 2GB",
        "select_video_language": "Select video language:",
        "select_video_type": "Select video type:",
        "lang_english": "🇬🇧 English",
        "lang_russian": "🇷🇺 Russian",
        "lang_chinese": "🇨🇳 Chinese",
        "lang_spanish": "🇪🇸 Spanish",
        "lang_hindi": "🇮🇳 Hindi",
        "type_movie": "🎬 Movie",
        "type_series": "📺 Series",
        "type_anime": "🎌 Anime",
        "is_series_question": "Is this a series/anime with episodes?",
        "yes_series": "✅ Yes, series",
        "no_single": "❌ No, single video",
        "enter_series_name": "Enter series name (letters and numbers only):",
        "enter_season_number": "Enter season number:",
        "enter_episode_number": "Enter episode number:",
        "enter_title": "Enter title (or /skip):",
        "enter_year": "Enter year (or /skip):",
        "enter_genre": "Enter genre (or /skip):",
        "enter_description": "Enter description (or /skip):",
        "send_poster": "📸 Send poster image (or /skip):",
        "enter_custom_code_prompt": "Enter code or /random:",
        "code_exists_error": "❌ Code exists. Try another.",
        "code_invalid_error": "❌ Code must be numbers only.",
        "series_name_invalid": "❌ Name must be letters/numbers only.",
        "number_invalid": "❌ Enter valid number.",
        "video_upload_success": "✅ Video uploaded!\nCode: <code>{code}</code>",
        "series_upload_success": "✅ Episode uploaded!\nCode: <code>{code}</code>\nSeries: {series}\nS{season}E{episode}",
        "series_list_header": "📺 <b>{series}</b>\n\nEpisodes:",
        "episode_info": "\n\n📺 <b>{title}</b>\nS{season}E{episode}",
        "movie_info": "\n\n🎬 <b>{title}</b>",
        "year_info": "\n📅 {year}",
        "genre_info": "\n🎭 {genre}",
        "description_info": "\n📝 {description}",
        "views_info": "\n👁️ Views: {views}",
        "rating_info": "\n⭐ Rating: {rating}/5 ({count} votes)",
        "premium_badge": "\n⭐ PREMIUM",
        "prev_episode": "⬅️ Previous",
        "next_episode": "➡️ Next",
        "back_to_list": "📋 List",
        "enter_search_query": "Enter search query (title, genre, year):",
        "search_results": "🔍 <b>Search Results:</b>\n\n",
        "no_results": "❌ No results found.",
        "action_cancelled": "✅ Action cancelled. Returning to main menu...",
        "no_active_actions": "ℹ️ No active actions to cancel.",
        "stats_message": """📊 <b>Bot Statistics</b>

👥 Users: {users}
🎬 Videos: {videos}
📺 Series: {series}
👮 Admins: {admins}
👁️ Total views: {total_views}
⭐ Premium users: {premium_users}
💰 Revenue: {revenue} ⭐
📅 Last update: {last_update}""",
    },
    "ru": {
        "welcome_message": "Добро пожаловать! Отправьте код для видео.",
        "admin_menu_info": "Меню администратора доступно.",
        "upload_video_button": "📹 Загрузить видео",
        "add_admin_button": "👤 Добавить админа",
        "stats_button": "📊 Статистика",
        "delete_video_button": "🗑️ Удалить видео",
        "edit_video_button": "✏️ Редактировать",
        "list_videos_button": "📝 Все видео",
        "list_admins_button": "👥 Список админов",
        "export_db_button": "📤 Экспорт БД",
        "list_series_button": "📺 Список сериалов",
        "analytics_button": "📊 Аналитика",
        "broadcast_button": "📢 Рассылка",
        "promo_button": "🎫 Промокоды",
        "back_button": "◀️ Назад",
        "cancel_button": "❌ Отменить",
        "premium_button": "⭐ Премиум",
        "my_subscription_button": "👤 Моя подписка",
        "search_button": "🔍 Поиск",
        "history_button": "📜 История",
        "favorites_button": "⭐ Избранное",
        "browse_button": "🗂️ Каталог",
        "referral_button": "👥 Пригласить друзей",
        "top_week_button": "🔥 Топ недели",
        "add_to_favorites": "⭐ Добавить в избранное",
        "remove_from_favorites": "❌ Удалить из избранного",
        "added_to_favorites": "✅ Добавлено в избранное!",
        "removed_from_favorites": "❌ Удалено из избранного!",
        "rate_video": "⭐ Оценить видео",
        "thanks_for_rating": "✅ Спасибо за оценку!",
        "subscribe_to_series": "🔔 Подписаться на сериал",
        "unsubscribe_from_series": "🔕 Отписаться",
        "subscribed_to_series": "✅ Вы подписались на {series}!",
        "unsubscribed_from_series": "❌ Вы отписались от {series}",
        "new_episode_notification": "🎬 Доступен новый эпизод!\n\n📺 {series}\nS{season}E{episode}\n\nКод: <code>{code}</code>",
        "recommendations_header": "🎬 <b>Вам также может понравиться:</b>\n\n",
        "top_weekly_header": "🔥 <b>Топ-5 за неделю</b>\n\n",
        "history_header": "📜 <b>История просмотров</b>\n\n",
        "history_empty": "История пуста. Посмотрите что-нибудь!",
        "favorites_header": "⭐ <b>Избранное</b>\n\n",
        "favorites_empty": "Избранного пока нет. Добавьте видео!",
        "referral_info": """👥 <b>Пригласить друзей</b>

Ваша реферальная ссылка:
<code>{link}</code>

🎁 Награды:
• Друг присоединился: +{bonus_days} дней премиума
• Друг купил премиум: +{payment_bonus_days} дней премиума

📊 Ваша статистика:
• Всего рефералов: {total}
• Премиум рефералов: {premium}
• Заработано дней: {bonus_earned}""",
        "browse_categories": "🗂️ <b>Каталог</b>\n\nВыберите категорию:",
        "category_movies": "🎬 Фильмы",
        "category_series": "📺 Сериалы",
        "category_anime": "🎌 Аниме",
        "promo_activate": "🎫 Активировать промокод",
        "enter_promo_code": "Введите промокод:",
        "promo_activated": "✅ Промокод активирован!",
        "create_promo_button": "➕ Создать промокод",
        "list_promos_button": "📋 Список промокодов",
        "broadcast_all": "📢 Всем пользователям",
        "broadcast_free": "🆓 Только бесплатным",
        "broadcast_premium": "⭐ Только премиум",
        "broadcast_language": "🌍 По языку",
        "enter_broadcast_message": "Введите сообщение для рассылки:",
        "broadcast_started": "📢 Рассылка начата!\n\nЦель: {target} пользователей",
        "broadcast_completed": """✅ Рассылка завершена!

📊 Результаты:
• Отправлено: {success}
• Ошибок: {failed}
• Заблокировали бота: {blocked}
• Всего: {total}""",
        "premium_required": "⭐ Это ПРЕМИУМ видео!\n\n💎 Оформите подписку Premium для просмотра этого контента.",
        "subscribe_button": "💳 Подписаться",
        "premium_info": """⭐ <b>Премиум подписка</b>

🎬 Доступ к эксклюзивному премиум контенту
📺 Безлимитные премиум сериалы
✨ Без рекламы
🚀 Приоритетная поддержка

Выберите тариф (оплата в Telegram Stars ⭐):""",
        "premium_status_active": """✅ <b>Ваш Премиум Статус</b>

Статус: ⭐ АКТИВЕН
Тариф: {plan}
Действует до: {expires}
Осталось дней: {days_left}

✨ Вы смотрите без рекламы!""",
        "premium_status_inactive": """❌ <b>Ваш Премиум Статус</b>

Статус: БЕСПЛАТНЫЙ
У вас нет активной подписки.

💎 Оформите Премиум и получите:
• Эксклюзивный премиум контент
• Просмотр без рекламы
• Приоритетную поддержку""",
        "premium_purchased": "🎉 Поздравляем! Премиум активирован!\n\n⭐ Преимущества:\n• Доступ ко всему премиум контенту\n• Никакой рекламы!\n• Приоритетная поддержка\n\nДействует до: {expires}",
        "is_premium_question": "Это премиум видео? (Только для Premium)",
        "yes_premium": "⭐ Да, премиум",
        "no_free": "✅ Нет, бесплатное",
        "premium_users_button": "⭐ Премиум пользователи",
        "code_not_found": "❌ Код не найден.",
        "rate_limit": "⏱️ Слишком быстро! Подождите немного.",
        "blocked_message": "⛔ Вы временно заблокированы из-за подозрительной активности.",
        "send_video_prompt": "Отправьте видеофайл (макс 2ГБ):",
        "video_too_large": "❌ Видео слишком большое! Макс размер: 2ГБ",
        "select_video_language": "Выберите язык видео:",
        "select_video_type": "Выберите тип видео:",
        "lang_english": "🇬🇧 Английский",
        "lang_russian": "🇷🇺 Русский",
        "lang_chinese": "🇨🇳 Китайский",
        "lang_spanish": "🇪🇸 Испанский",
        "lang_hindi": "🇮🇳 Хинди",
        "type_movie": "🎬 Фильм",
        "type_series": "📺 Сериал",
        "type_anime": "🎌 Аниме",
        "is_series_question": "Это сериал/аниме с эпизодами?",
        "yes_series": "✅ Да, сериал",
        "no_single": "❌ Нет, одно видео",
        "enter_series_name": "Введите название (буквы и цифры):",
        "enter_season_number": "Введите номер сезона:",
        "enter_episode_number": "Введите номер эпизода:",
        "enter_title": "Введите название (или /skip):",
        "enter_year": "Введите год (или /skip):",
        "enter_genre": "Введите жанр (или /skip):",
        "enter_description": "Введите описание (или /skip):",
        "send_poster": "📸 Отправьте постер (или /skip):",
        "enter_custom_code_prompt": "Введите код или /random:",
        "code_exists_error": "❌ Код существует. Попробуйте другой.",
        "code_invalid_error": "❌ Код должен быть из цифр.",
        "series_name_invalid": "❌ Название только буквы/цифры.",
        "number_invalid": "❌ Введите число.",
        "video_upload_success": "✅ Видео загружено!\nКод: <code>{code}</code>",
        "series_upload_success": "✅ Эпизод загружен!\nКод: <code>{code}</code>\nСериал: {series}\nS{season}E{episode}",
        "series_list_header": "📺 <b>{series}</b>\n\nЭпизоды:",
        "episode_info": "\n\n📺 <b>{title}</b>\nS{season}E{episode}",
        "movie_info": "\n\n🎬 <b>{title}</b>",
        "year_info": "\n📅 {year}",
        "genre_info": "\n🎭 {genre}",
        "description_info": "\n📝 {description}",
        "views_info": "\n👁️ Просмотров: {views}",
        "rating_info": "\n⭐ Рейтинг: {rating}/5 ({count} голосов)",
        "premium_badge": "\n⭐ ПРЕМИУМ",
        "prev_episode": "⬅️ Предыдущая",
        "next_episode": "➡️ Следующая",
        "back_to_list": "📋 Список",
        "enter_search_query": "Введите запрос (название, жанр, год):",
        "search_results": "🔍 <b>Результаты поиска:</b>\n\n",
        "no_results": "❌ Ничего не найдено.",
        "action_cancelled": "✅ Действие отменено. Возвращение в главное меню...",
        "no_active_actions": "ℹ️ Нет активных действий для отмены.",
        "stats_message": """📊 <b>Статистика бота</b>

👥 Пользователей: {users}
🎬 Видео: {videos}
📺 Сериалов: {series}
👮 Админов: {admins}
👁️ Всего просмотров: {total_views}
⭐ Премиум пользователей: {premium_users}
💰 Доход: {revenue} ⭐
📅 Последнее обновление: {last_update}""",
    },
    "uk": {
        "welcome_message": "Ласкаво просимо! Надішліть код для отримання відео.",
        "admin_menu_info": "Меню адміністратора доступне.",
        "upload_video_button": "📹 Завантажити відео",
        "add_admin_button": "👤 Додати адміна",
        "stats_button": "📊 Статистика",
        "delete_video_button": "🗑️ Видалити відео",
        "edit_video_button": "✏️ Редагувати",
        "list_videos_button": "📝 Всі відео",
        "list_admins_button": "👥 Список адмінів",
        "export_db_button": "📤 Експорт БД",
        "list_series_button": "📺 Список серіалів",
        "analytics_button": "📊 Аналітика",
        "broadcast_button": "📢 Розсилка",
        "promo_button": "🎫 Промокоди",
        "back_button": "◀️ Назад",
        "cancel_button": "❌ Скасувати",
        "premium_button": "⭐ Преміум",
        "my_subscription_button": "👤 Моя підписка",
        "search_button": "🔍 Пошук",
        "history_button": "📜 Історія",
        "favorites_button": "⭐ Улюблене",
        "browse_button": "🗂️ Каталог",
        "referral_button": "👥 Запросити друзів",
        "top_week_button": "🔥 Топ тижня",
        "add_to_favorites": "⭐ Додати в улюблене",
        "remove_from_favorites": "❌ Видалити з улюбленого",
        "added_to_favorites": "✅ Додано в улюблене!",
        "removed_from_favorites": "❌ Видалено з улюбленого!",
        "rate_video": "⭐ Оцінити відео",
        "thanks_for_rating": "✅ Дякуємо за оцінку!",
        "subscribe_to_series": "🔔 Підписатися на серіал",
        "unsubscribe_from_series": "🔕 Відписатися",
        "subscribed_to_series": "✅ Ви підписалися на {series}!",
        "unsubscribed_from_series": "❌ Ви відписалися від {series}",
        "new_episode_notification": "🎬 Доступний новий епізод!\n\n📺 {series}\nS{season}E{episode}\n\nКод: <code>{code}</code>",
        "recommendations_header": "🎬 <b>Вам також може сподобатися:</b>\n\n",
        "top_weekly_header": "🔥 <b>Топ-5 за тиждень</b>\n\n",
        "history_header": "📜 <b>Історія переглядів</b>\n\n",
        "history_empty": "Історія порожня. Подивіться щось!",
        "favorites_header": "⭐ <b>Улюблене</b>\n\n",
        "favorites_empty": "Улюбленого поки немає. Додайте відео!",
        "referral_info": """👥 <b>Запросити друзів</b>

Ваше реферальне посилання:
<code>{link}</code>

🎁 Нагороди:
• Друг приєднався: +{bonus_days} днів преміуму
• Друг купив преміум: +{payment_bonus_days} днів преміуму

📊 Ваша статистика:
• Всього рефералів: {total}
• Преміум рефералів: {premium}
• Зароблено днів: {bonus_earned}""",
        "browse_categories": "🗂️ <b>Каталог</b>\n\nОберіть категорію:",
        "category_movies": "🎬 Фільми",
        "category_series": "📺 Серіали",
        "category_anime": "🎌 Аніме",
        "promo_activate": "🎫 Активувати промокод",
        "enter_promo_code": "Введіть промокод:",
        "promo_activated": "✅ Промокод активовано!",
        "create_promo_button": "➕ Створити промокод",
        "list_promos_button": "📋 Список промокодів",
        "broadcast_all": "📢 Всім користувачам",
        "broadcast_free": "🆓 Тільки безкоштовним",
        "broadcast_premium": "⭐ Тільки преміум",
        "broadcast_language": "🌍 За мовою",
        "enter_broadcast_message": "Введіть повідомлення для розсилки:",
        "broadcast_started": "📢 Розсилка почата!\n\nЦіль: {target} користувачів",
        "broadcast_completed": """✅ Розсилка завершена!

📊 Результати:
• Надіслано: {success}
• Помилок: {failed}
• Заблокували бота: {blocked}
• Всього: {total}""",
        "premium_required": "⭐ Це ПРЕМІУМ відео!\n\n💎 Оформіть підписку Premium для перегляду цього контенту.",
        "subscribe_button": "💳 Підписатися",
        "premium_info": """⭐ <b>Преміум підписка</b>

🎬 Доступ до ексклюзивного преміум контенту
📺 Безлімітні преміум серіали
✨ Без реклами
🚀 Пріоритетна підтримка

Оберіть тариф (оплата в Telegram Stars ⭐):""",
        "premium_status_active": """✅ <b>Ваш Преміум Статус</b>

Статус: ⭐ АКТИВНИЙ
Тариф: {plan}
Діє до: {expires}
Залишилось днів: {days_left}

✨ Ви дивитесь без реклами!""",
        "premium_status_inactive": """❌ <b>Ваш Преміум Статус</b>

Статус: БЕЗКОШТОВНИЙ
У вас немає активної підписки.

💎 Оформіть Преміум і отримайте:
• Ексклюзивний преміум контент
• Перегляд без реклами
• Пріоритетну підтримку""",
        "premium_purchased": "🎉 Вітаємо! Преміум активовано!\n\n⭐ Переваги:\n• Доступ до всього преміум контенту\n• Без реклами!\n• Пріоритетна підтримка\n\nДіє до: {expires}",
        "is_premium_question": "Це преміум відео? (Тільки для Premium)",
        "yes_premium": "⭐ Так, преміум",
        "no_free": "✅ Ні, безкоштовне",
        "premium_users_button": "⭐ Преміум користувачі",
        "code_not_found": "❌ Код не знайдено.",
        "rate_limit": "⏱️ Занадто швидко! Зачекайте трохи.",
        "blocked_message": "⛔ Вас тимчасово заблоковано через підозрілу активність.",
        "send_video_prompt": "Надішліть відеофайл (макс 2ГБ):",
        "video_too_large": "❌ Відео занадто велике! Макс розмір: 2ГБ",
        "select_video_language": "Оберіть мову відео:",
        "select_video_type": "Оберіть тип відео:",
        "lang_english": "🇬🇧 Англійська",
        "lang_russian": "🇷🇺 Російська",
        "lang_chinese": "🇨🇳 Китайська",
        "lang_spanish": "🇪🇸 Іспанська",
        "lang_hindi": "🇮🇳 Хінді",
        "type_movie": "🎬 Фільм",
        "type_series": "📺 Серіал",
        "type_anime": "🎌 Аніме",
        "is_series_question": "Це серіал/аніме з епізодами?",
        "yes_series": "✅ Так, серіал",
        "no_single": "❌ Ні, одне відео",
        "enter_series_name": "Введіть назву (літери та цифри):",
        "enter_season_number": "Введіть номер сезону:",
        "enter_episode_number": "Введіть номер епізоду:",
        "enter_title": "Введіть назву (або /skip):",
        "enter_year": "Введіть рік (або /skip):",
        "enter_genre": "Введіть жанр (або /skip):",
        "enter_description": "Введіть опис (або /skip):",
        "send_poster": "📸 Надішліть постер (або /skip):",
        "enter_custom_code_prompt": "Введіть код або /random:",
        "code_exists_error": "❌ Код існує. Спробуйте інший.",
        "code_invalid_error": "❌ Код має бути з цифр.",
        "series_name_invalid": "❌ Назва тільки літери/цифри.",
        "number_invalid": "❌ Введіть число.",
        "video_upload_success": "✅ Відео завантажено!\nКод: <code>{code}</code>",
        "series_upload_success": "✅ Епізод завантажено!\nКод: <code>{code}</code>\nСеріал: {series}\nS{season}E{episode}",
        "series_list_header": "📺 <b>{series}</b>\n\nЕпізоди:",
        "episode_info": "\n\n📺 <b>{title}</b>\nS{season}E{episode}",
        "movie_info": "\n\n🎬 <b>{title}</b>",
        "year_info": "\n📅 {year}",
        "genre_info": "\n🎭 {genre}",
        "description_info": "\n📝 {description}",
        "views_info": "\n👁️ Переглядів: {views}",
        "rating_info": "\n⭐ Рейтинг: {rating}/5 ({count} голосів)",
        "premium_badge": "\n⭐ ПРЕМІУМ",
        "prev_episode": "⬅️ Попередня",
        "next_episode": "➡️ Наступна",
        "back_to_list": "📋 Список",
        "enter_search_query": "Введіть запит (назва, жанр, рік):",
        "search_results": "🔍 <b>Результати пошуку:</b>\n\n",
        "no_results": "❌ Нічого не знайдено.",
        "action_cancelled": "✅ Дію скасовано. Повернення до головного меню...",
        "no_active_actions": "ℹ️ Немає активних дій для скасування.",
        "stats_message": """📊 <b>Статистика бота</b>

👥 Користувачів: {users}
🎬 Відео: {videos}
📺 Серіалів: {series}
👮 Адмінів: {admins}
👁️ Всього переглядів: {total_views}
⭐ Преміум користувачів: {premium_users}
💰 Дохід: {revenue} ⭐
📅 Останнє оновлення: {last_update}""",
    }
}

# ==================== СТАНИ ====================

class UserState(StatesGroup):
    waiting_for_code = State()
    waiting_for_search = State()
    waiting_for_video = State()
    waiting_for_admin_id = State()
    waiting_for_video_language = State()
    waiting_for_video_type = State()
    waiting_for_series_choice = State()
    waiting_for_series_name = State()
    waiting_for_season = State()
    waiting_for_episode = State()
    waiting_for_title = State()
    waiting_for_year = State()
    waiting_for_genre = State()
    waiting_for_description = State()
    waiting_for_custom_code = State()
    waiting_for_poster = State()
    waiting_for_delete_code = State()
    waiting_for_edit_code = State()
    waiting_for_edit_field = State()
    waiting_for_edit_value = State()
    waiting_for_premium_choice = State()
    waiting_for_grant_premium_user = State()
    waiting_for_grant_premium_plan = State()  # ✅ НОВИЙ СТАН
    waiting_for_promo_code = State()
    waiting_for_broadcast_message = State()
    waiting_for_rating = State()

# ==================== ДОПОМІЖНІ ФУНКЦІЇ ====================

def get_text(user_id: int, key: str, **kwargs) -> str:
    """Отримати текст на мові користувача (з кешем)"""
    # Використати кеш (оновлюється при виборі мови)
    lang = user_languages_cache.get(user_id, "en")
    
    text = TEXTS.get(lang, TEXTS["en"]).get(key, TEXTS["en"].get(key, ""))
    return text.format(**kwargs) if kwargs else text

def check_rate_limit(user_id: int) -> bool:
    """Перевірка rate limit"""
    if not rate_limiter.check_rate_limit(user_id, MAX_REQUESTS_PER_MINUTE, 60):
        logger.warning(f"Rate limit exceeded for user {user_id}")
        return False
    return True

async def is_premium_user(user_id: int) -> bool:
    """Перевірити чи користувач має активний преміум"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user or not user.is_premium:
            return False
        
        if user.premium_expires and user.premium_expires < datetime.utcnow():
            # Преміум закінчився
            user.is_premium = False
            await session.commit()
            return False
        
        return True

async def is_admin(user_id: int) -> bool:
    """Перевірити чи користувач адмін"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(User.is_admin).where(User.telegram_id == user_id)
        )
        is_admin_flag = result.scalar_one_or_none()
        return is_admin_flag or False

def log_admin_action(user_id: int, action: str, details: str = ""):
    """Логування дій адміністратора"""
    if LOG_SENSITIVE_DATA:
        admin_logger.info(f"Admin {user_id} - {action} - {details}")
    else:
        admin_logger.info(f"Admin *** - {action}")

async def show_ad(message: Message, user_id: int):
    """Показати рекламу з урахуванням мови та затримкою"""
    if not ADS_ENABLED:
        return
    
    # Перевірка чи показувати рекламу
    if ADS_ONLY_FREE_USERS:
        if await is_premium_user(user_id) or await is_admin(user_id):
            return
    
    user_video_count[user_id] += 1
    
    if user_video_count[user_id] % AD_FREQUENCY != 0:
        logger.info(f"Ad skipped for user {user_id} (count: {user_video_count[user_id]})")
        return
    
    # Отримати мову користувача
    async with async_session_maker() as session:
        result = await session.execute(
            select(User.language).where(User.telegram_id == user_id)
        )
        user_lang = result.scalar_one_or_none() or "en"
    
    ads_for_lang = ADS_LIST.get(user_lang, ADS_LIST.get("en", []))
    
    if not ads_for_lang:
        return
    
    ad = random.choice(ads_for_lang)
    
    try:
        # Валідація URL
        if VALIDATE_AD_URLS and ad.get("button_url"):
            if not input_validator.validate_url(ad["button_url"], ALLOWED_DOMAINS):
                security_logger.log_security_event("INVALID_AD_URL", ad["button_url"])
                return
        
        keyboard = None
        
        if ad.get("button_text") and ad.get("button_url"):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=ad["button_text"], url=ad["button_url"])]
            ])
        elif ad.get("button_text"):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=ad["button_text"], callback_data="show_premium_plans")]
            ])
        
        ad_text = input_validator.sanitize_html(ad["text"])
        
        # Таймер 5 секунд
        def get_timer_text(seconds: int) -> str:
            progress = "▓" * (5 - seconds) + "░" * seconds
            if user_lang == "uk":
                return f"[{progress}] {seconds} сек..."
            elif user_lang == "ru":
                return f"[{progress}] {seconds} сек..."
            else:
                return f"[{progress}] {seconds} sec..."
        
        ad_text_with_timer = f"{ad_text}\n\n{get_timer_text(5)}"
        
        # Відправити рекламу
        if ad.get("photo"):
            ad_message = await bot.send_photo(
                message.chat.id,
                photo=ad["photo"],
                caption=ad_text_with_timer,
                reply_markup=keyboard
            )
        else:
            ad_message = await message.answer(
                ad_text_with_timer,
                reply_markup=keyboard
            )
        
        # Зворотній відлік
        for seconds_left in range(4, 0, -1):
            await asyncio.sleep(1)
            
            timer_text = get_timer_text(seconds_left)
            updated_text = f"{ad_text}\n\n{timer_text}"
            
            try:
                if ad.get("photo"):
                    await ad_message.edit_caption(
                        caption=updated_text,
                        reply_markup=keyboard
                    )
                else:
                    await ad_message.edit_text(
                        updated_text,
                        reply_markup=keyboard
                    )
            except:
                pass
        
        await asyncio.sleep(1)
        
        # Логування показу реклами
        if TRACK_AD_CLICKS:
            ad_id = f"{user_lang}_{ads_for_lang.index(ad)}"
            await log_ad_click(user_id, ad_id, clicked=False)
        
        logger.info(f"Ad shown to user {user_id} (lang: {user_lang})")
        
    except Exception as e:
        logger.error(f"Error showing ad: {e}")

async def check_cancel(message: Message, state: FSMContext, user_id: int) -> bool:
    """Перевірити чи користувач натиснув скасувати. Повертає True якщо скасовано."""
    if message.text.strip() in ["❌ Скасувати", "❌ Отменить", "❌ Cancel", "/cancel", "/back"]:
        current_state = await state.get_state()
        await state.clear()
        
        lang = user_languages_cache.get(user_id, "uk")
        
        if lang == "uk":
            msg = "✅ Дію скасовано. Повернення до головного меню..."
        elif lang == "ru":
            msg = "✅ Действие отменено. Возвращение в главное меню..."
        else:
            msg = "✅ Action cancelled. Returning to main menu..."
        
        if await is_admin(user_id):
            await message.answer(msg, reply_markup=get_admin_keyboard(user_id))
        else:
            await message.answer(msg, reply_markup=get_user_keyboard(user_id))
        
        await state.set_state(UserState.waiting_for_code)
        logger.info(f"User {user_id} cancelled action from state: {current_state}")
        return True
    
    return False

# ==================== КЛАВІАТУРИ ====================

def get_language_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура вибору мови"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="English 🇬🇧", callback_data="lang_en")],
        [InlineKeyboardButton(text="Русский 🇷🇺", callback_data="lang_ru")],
        [InlineKeyboardButton(text="Українська 🇺🇦", callback_data="lang_uk")],
    ])

def get_user_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Клавіатура користувача"""
    return ReplyKeyboardMarkup(keyboard=[
        [
            KeyboardButton(text=get_text(user_id, "search_button")),
            KeyboardButton(text=get_text(user_id, "browse_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "history_button")),
            KeyboardButton(text=get_text(user_id, "favorites_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "top_week_button")),
            KeyboardButton(text=get_text(user_id, "referral_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "premium_button")),
            KeyboardButton(text=get_text(user_id, "my_subscription_button"))
        ],
    ], resize_keyboard=True)

def get_admin_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Клавіатура адміністратора"""
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=get_text(user_id, "upload_video_button"))],
        [
            KeyboardButton(text=get_text(user_id, "stats_button")),
            KeyboardButton(text=get_text(user_id, "analytics_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "delete_video_button")),
            KeyboardButton(text=get_text(user_id, "edit_video_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "list_videos_button")),
            KeyboardButton(text=get_text(user_id, "list_series_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "broadcast_button")),
            KeyboardButton(text=get_text(user_id, "promo_button"))
        ],
        [
            KeyboardButton(text=get_text(user_id, "list_admins_button")),
            KeyboardButton(text=get_text(user_id, "add_admin_button"))
        ],
        [
            KeyboardButton(text="⭐ Видати преміум"),
            KeyboardButton(text=get_text(user_id, "export_db_button"))
        ],
    ], resize_keyboard=True)

def get_premium_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавіатура тарифів преміуму"""
    buttons = []
    for plan_id, plan_info in PREMIUM_PLANS.items():
        price_stars = plan_info["price_stars"]
        # Використовуємо англійську назву для простоти
        buttons.append([InlineKeyboardButton(
            text=f"{plan_info['emoji']} {plan_info['name_en']} - {price_stars} ⭐",
            callback_data=f"buy_premium_{plan_id}"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_browse_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавіатура каталогу"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=get_text(user_id, "category_movies"),
            callback_data="browse_movies"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "category_series"),
            callback_data="browse_series"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "category_anime"),
            callback_data="browse_anime"
        )],
    ])

def get_rating_keyboard(video_code: str) -> InlineKeyboardMarkup:
    """Клавіатура рейтингу (1-5 зірок)"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐", callback_data=f"rate_{video_code}_1"),
            InlineKeyboardButton(text="⭐⭐", callback_data=f"rate_{video_code}_2"),
            InlineKeyboardButton(text="⭐⭐⭐", callback_data=f"rate_{video_code}_3"),
            InlineKeyboardButton(text="⭐⭐⭐⭐", callback_data=f"rate_{video_code}_4"),
            InlineKeyboardButton(text="⭐⭐⭐⭐⭐", callback_data=f"rate_{video_code}_5"),
        ]
    ])

async def get_video_action_keyboard(user_id: int, video_code: str, series_name: str = None) -> InlineKeyboardMarkup:
    """Клавіатура дій з відео (улюблене, рейтинг, підписка)"""
    buttons = []
    
    # Перевірити чи в улюбленому
    is_fav = await is_favorite(user_id, video_code)
    
    if is_fav:
        fav_button = InlineKeyboardButton(
            text=get_text(user_id, "remove_from_favorites"),
            callback_data=f"fav_{video_code}"
        )
    else:
        fav_button = InlineKeyboardButton(
            text=get_text(user_id, "add_to_favorites"),
            callback_data=f"fav_{video_code}"
        )
    
    buttons.append([fav_button])
    
    # Рейтинг
    if RATINGS_ENABLED:
        buttons.append([InlineKeyboardButton(
            text=get_text(user_id, "rate_video"),
            callback_data=f"rate_{video_code}_prompt"
        )])
    
    # Підписка на серіал
    if SERIES_NOTIFICATIONS_ENABLED and series_name:
        buttons.append([InlineKeyboardButton(
            text=get_text(user_id, "subscribe_to_series"),
            callback_data=f"sub_{series_name}"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_broadcast_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавіатура розсилки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=get_text(user_id, "broadcast_all"),
            callback_data="broadcast_all"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "broadcast_free"),
            callback_data="broadcast_free"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "broadcast_premium"),
            callback_data="broadcast_premium"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "broadcast_language"),
            callback_data="broadcast_lang"
        )],
    ])

def get_promo_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавіатура промокодів (адмін)"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=get_text(user_id, "create_promo_button"),
            callback_data="promo_create"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "list_promos_button"),
            callback_data="promo_list"
        )],
        [InlineKeyboardButton(
            text=get_text(user_id, "promo_activate"),
            callback_data="promo_activate"
        )],
    ])

def get_cancel_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Клавіатура зі скасуванням"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=get_text(user_id, "cancel_button"))]],
        resize_keyboard=True
    )

# ==================== MIDDLEWARE ====================

class BanCheckMiddleware(BaseMiddleware):
    """Перевірка бану перед усіма обробниками"""
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user_id = None
        if hasattr(event, 'from_user') and event.from_user:
            user_id = event.from_user.id
        
        if user_id:
            # Постійний бан
            if user_id in BLOCKED_USERS:
                return
            
            # Тимчасовий бан
            if rate_limiter.is_blocked(user_id):
                return
        
        return await handler(event, data)

# ==================== ІНІЦІАЛІЗАЦІЯ ====================

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=storage)

# Додати middleware
dp.message.middleware(BanCheckMiddleware())
dp.callback_query.middleware(BanCheckMiddleware())
# Створити broadcast manager
from broadcast import BroadcastManager
# Инициализация менеджера рассылок (фикс падений из-за None)
broadcast_manager = BroadcastManager(bot)
# Необязательно: синхронизируем глобальную ссылку модуля broadcast
import broadcast as _broadcast
_broadcast.broadcast_manager = broadcast_manager
logger.info("✅ BroadcastManager initialized")

# ==================== КОМАНДА /CANCEL ====================

@dp.message(Command("cancel"))
async def handle_cancel_command(message: Message, state: FSMContext):
    """Команда скасування поточної дії"""
    user_id = message.from_user.id
    current_state = await state.get_state()
    
    if current_state:
        await state.clear()
        
        lang = user_languages_cache.get(user_id, "uk")
        
        if lang == "uk":
            msg = "✅ Поточну дію скасовано."
        elif lang == "ru":
            msg = "✅ Текущее действие отменено."
        else:
            msg = "✅ Current action cancelled."
        
        if await is_admin(user_id):
            await message.answer(msg, reply_markup=get_admin_keyboard(user_id))
        else:
            await message.answer(msg, reply_markup=get_user_keyboard(user_id))
        
        await state.set_state(UserState.waiting_for_code)
        logger.info(f"User {user_id} used /cancel from state: {current_state}")
    else:
        lang = user_languages_cache.get(user_id, "uk")
        
        if lang == "uk":
            msg = "ℹ️ Немає активних дій для скасування."
        elif lang == "ru":
            msg = "ℹ️ Нет активных действий для отмены."
        else:
            msg = "ℹ️ No active actions to cancel."
        
        await message.answer(msg)

# ==================== ОБРОБНИК /START ====================

@dp.message(Command("start"))
async def handle_start(message: Message, state: FSMContext):
    """Обробник команди /start з реферальною системою"""
    user_id = message.from_user.id
    
    # Перевірка блокування
    if user_id in BLOCKED_USERS:
        security_logger.log_suspicious_activity(user_id, "BLOCKED_USER_START_ATTEMPT")
        return
    
    await state.clear()
    
    # Створити або отримати користувача
    user = await get_or_create_user(
        telegram_id=user_id,
        username=message.from_user.username,
        first_name=message.from_user.first_name
    )
    
    # Додати мову в кеш
    user_languages_cache[user_id] = user.language
    
    # Перевірити реферальний код
    if REFERRAL_ENABLED and message.text and len(message.text.split()) > 1:
        referral_code = message.text.split()[1]
        
        # Якщо користувач новий і є реферальний код
        if not user.referred_by:
            success, msg = await referral_system.apply_referral(user_id, referral_code)
            if success:
                await message.answer(msg)
    
    # Вибір мови якщо не вибрана
    if not user.language or user.language == "en":
        await message.answer(
            "Please select your language: / Выберите язык: / Оберіть мову:",
            reply_markup=get_language_keyboard()
        )
    else:
        # Показати меню
        if user.is_admin:
            await message.answer(
                get_text(user_id, "welcome_message") + "\n\n" + get_text(user_id, "admin_menu_info"),
                reply_markup=get_admin_keyboard(user_id)
            )
        else:
            await message.answer(
                get_text(user_id, "welcome_message"),
                reply_markup=get_user_keyboard(user_id)
            )
        
        await state.set_state(UserState.waiting_for_code)
    
    logger.info(f"User {user_id} started the bot")

@dp.callback_query(F.data.startswith("lang_"))
async def handle_language_selection(callback: CallbackQuery, state: FSMContext):
    """Обробник вибору мови"""
    lang_code = callback.data.split("_")[1]
    user_id = callback.from_user.id
    
    # Оновити мову в БД
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if user:
            user.language = lang_code
            await session.commit()
    
    # Оновити кеш
    user_languages_cache[user_id] = lang_code
    
    await callback.message.delete()
    
    # Перевірити чи адмін
    is_admin_user = await is_admin(user_id)
    
    if is_admin_user:
        await callback.message.answer(
            get_text(user_id, "welcome_message") + "\n\n" + get_text(user_id, "admin_menu_info"),
            reply_markup=get_admin_keyboard(user_id)
        )
    else:
        await callback.message.answer(
            get_text(user_id, "welcome_message"),
            reply_markup=get_user_keyboard(user_id)
        )
    
    await state.set_state(UserState.waiting_for_code)
    logger.info(f"User {user_id} selected language: {lang_code}")

# ==================== ОБРОБНИК ТЕКСТОВИХ ПОВІДОМЛЕНЬ ====================

@dp.message(UserState.waiting_for_code, F.text)
async def handle_text_input(message: Message, state: FSMContext):
    """Обробник текстового вводу (коди, кнопки, команди)"""
    user_id = message.from_user.id
    
    # Rate limit
    if not check_rate_limit(user_id):
        await message.answer(get_text(user_id, "rate_limit"))
        return
    
    # Спам-захист
    if spam_protection.is_spam(user_id, message.text):
        security_logger.log_suspicious_activity(user_id, "SPAM_DETECTED", message.text[:50])
        rate_limiter.block_user(user_id, BLOCK_TIME)
        return
    
    text = input_validator.sanitize_html(message.text.strip())
    
    # ============ КНОПКА "НАЗАД" ============
    if text in ["◀️ Назад", "◀️ Back", "◀️ Вернуться", "/back"]:
        await state.clear()
        
        # Повернутися до головного меню
        if await is_admin(user_id):
            await message.answer(get_text(user_id, "action_cancelled"), reply_markup=get_admin_keyboard(user_id))
        else:
            await message.answer(get_text(user_id, "action_cancelled"), reply_markup=get_user_keyboard(user_id))
        
        await state.set_state(UserState.waiting_for_code)
        return
    
    # ============ АДМІНСЬКІ КОМАНДИ (ПЕРЕВІРЯЄМО СПОЧАТКУ!) ============
    
    is_admin_user = await is_admin(user_id)
    
    if is_admin_user:
        # Завантаження відео
        if text in ["📹 Завантажити відео", "📹 Загрузить видео", "📹 Upload Video"]:
            await message.answer(
                get_text(user_id, "send_video_prompt"),
                reply_markup=get_cancel_keyboard(user_id)
            )
            await state.set_state(UserState.waiting_for_video)
            return
        
        # Статистика
        if text in ["📊 Статистика", "📊 Statistics"]:
            await show_statistics(message, user_id)
            return
        
        # Аналітика
        if text in ["📊 Аналітика", "📊 Analytics"]:
            await show_analytics(message, user_id)
            return
        
        # Видалити відео
        if text in ["🗑️ Видалити відео", "🗑️ Удалить видео", "🗑️ Delete Video"]:
            await message.answer(
                "🗑️ Введіть код відео для видалення:",
                reply_markup=get_cancel_keyboard(user_id)
            )
            await state.set_state(UserState.waiting_for_delete_code)
            return
        
        # Редагувати
        if text in ["✏️ Редагувати", "✏️ Редактировать", "✏️ Edit Metadata"]:
            await message.answer(
                "✏️ Введіть код відео для редагування:",
                reply_markup=get_cancel_keyboard(user_id)
            )
            await state.set_state(UserState.waiting_for_edit_code)
            return
        
        # Список відео
        if text in ["📝 Всі відео", "📝 Все видео", "📝 All Videos"]:
            await show_all_videos(message, user_id, 1)
            return
        
        # Список серіалів
        if text in ["📺 Список серіалів", "📺 Список сериалов", "📺 Series List"]:
            await show_all_series(message, user_id)
            return
        
        # Розсилка
        if text in ["📢 Розсилка", "📢 Рассылка", "📢 Broadcast"]:
            await message.answer(
                "📢 Оберіть тип розсилки:",
                reply_markup=get_broadcast_keyboard(user_id)
            )
            return
        
        # Промокоди
        if text in ["🎫 Промокоди", "🎫 Promo Codes"]:
            await message.answer(
                "🎫 Промокоди:",
                reply_markup=get_promo_keyboard(user_id)
            )
            return
        
        # Додати адміна
        if text in ["👤 Додати адміна", "👤 Добавить админа", "👤 Add Admin"]:
            await message.answer(
                "👤 Введіть Telegram ID користувача для надання прав адміна:",
                reply_markup=get_cancel_keyboard(user_id)
            )
            await state.set_state(UserState.waiting_for_admin_id)
            return
        
        # Список адмінів
        if text in ["👥 Список адмінів", "👥 Список админов", "👥 Admins List"]:
            await handle_list_admins(message)
            return
        
        # Видати преміум
        if text in ["⭐ Видати преміум", "⭐ Выдать премиум", "⭐ Grant Premium"]:
            await message.answer(
                "⭐ Введіть Telegram ID користувача для видачі преміуму:",
                reply_markup=get_cancel_keyboard(user_id)
            )
            await state.set_state(UserState.waiting_for_grant_premium_user)
            return
        
        # Експорт БД
        if text in ["📤 Експорт БД", "📤 Экспорт БД", "📤 Export DB"]:
            await handle_export_db(message)
            return
    
    # ============ ОБРОБКА КНОПОК КОРИСТУВАЧА ============
    
    # Пошук
    if text in ["🔍 Пошук", "🔍 Поиск", "🔍 Search"]:
        await message.answer(
            get_text(user_id, "enter_search_query"),
            reply_markup=get_cancel_keyboard(user_id)
        )
        await state.set_state(UserState.waiting_for_search)
        return
    
    # Історія
    if text in ["📜 Історія", "📜 История", "📜 History"]:
        await show_user_history(message, user_id)
        return
    
    # Улюблене
    if text in ["⭐ Улюблене", "⭐ Избранное", "⭐ Favorites"]:
        await show_user_favorites(message, user_id)
        return
    
    # Каталог
    if text in ["🗂️ Каталог", "🗂️ Browse"]:
        await message.answer(
            get_text(user_id, "browse_categories"),
            reply_markup=get_browse_keyboard(user_id)
        )
        return
    
    # Топ тижня
    if text in ["🔥 Топ тижня", "🔥 Топ недели", "🔥 Top Week"]:
        await show_top_weekly(message, user_id)
        return
    
    # Реферальна система
    if text in ["👥 Запросити друзів", "👥 Пригласить друзей", "👥 Invite Friends"]:
        await show_referral_info(message, user_id)
        return
    
    # Premium
    if text in ["⭐ Преміум", "⭐ Премиум", "⭐ Premium"]:
        await show_premium_plans(message, user_id)
        return
    
    # Моя підписка
    if text in ["👤 Моя підписка", "👤 Моя подписка", "👤 My Subscription"]:
        await show_subscription_status(message, user_id)
        return
    
    # ============ ПЕРЕВІРКА КОДУ ВІДЕО (В КІНЦІ!) ============
    
    # Валідація коду
    if not input_validator.validate_code(text, max_length=15):
        security_logger.log_suspicious_activity(user_id, "INVALID_CODE_FORMAT", text)
        await message.answer(get_text(user_id, "code_not_found"))
        return
    
    # Спроба отримати відео з кешу
    if CACHE_ENABLED:
        cached_video = await video_cache.get(f"video_{text}")
        if cached_video:
            logger.info(f"Video {text} loaded from cache for user {user_id}")
            await send_video_to_user(message, user_id, cached_video)
            return
    
    # Пошук відео в БД
    async with async_session_maker() as session:
        result = await session.execute(
            select(Video).where(Video.code == text)
        )
        video = result.scalar_one_or_none()
        
        if not video:
            await message.answer(get_text(user_id, "code_not_found"))
            return
        
        # Оновити лічильник переглядів
        video.views_count += 1
        await session.commit()
        
        # Зберегти в кеш
        if CACHE_ENABLED:
            await video_cache.set(f"video_{text}", video)
        
        # Відправити відео користувачу
        await send_video_to_user(message, user_id, video)

# ==================== ФУНКЦІЇ ПОКАЗУ ДАНИХ ============

async def show_user_history(message: Message, user_id: int):
    """Показати історію перегляду користувача"""
    history_videos = await get_user_history(user_id, limit=10)
    
    if not history_videos:
        await message.answer(get_text(user_id, "history_empty"))
        return
    
    msg = get_text(user_id, "history_header")
    
    for video in history_videos:
        premium_mark = " ⭐" if video.is_premium else ""
        
        if video.is_series:
            msg += f"📺 S{video.season}E{video.episode} {video.title or 'Untitled'}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"<code>{video.code}</code>{premium_mark}\n\n"
    
    await message.answer(msg)

async def show_user_favorites(message: Message, user_id: int):
    """Показати улюблені відео користувача"""
    favorites = await get_user_favorites(user_id)
    
    if not favorites:
        await message.answer(get_text(user_id, "favorites_empty"))
        return
    
    msg = get_text(user_id, "favorites_header")
    
    for video in favorites:
        premium_mark = " ⭐" if video.is_premium else ""
        rating_mark = f" ({round(video.avg_rating, 1)}⭐)" if video.ratings_count > 0 else ""
        
        if video.is_series:
            msg += f"📺 S{video.season}E{video.episode} {video.title or 'Untitled'}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"<code>{video.code}</code>{premium_mark}{rating_mark}\n\n"
    
    await message.answer(msg)

async def show_top_weekly(message: Message, user_id: int):
    """Показати топ-5 за тиждень"""
    top_videos = await get_top_weekly(limit=TOP_WEEKLY_LIMIT)
    
    if not top_videos:
        await message.answer("❌ Поки немає даних.")
        return
    
    msg = get_text(user_id, "top_weekly_header")
    
    for i, video in enumerate(top_videos, 1):
        premium_mark = " ⭐" if video.is_premium else ""
        rating_mark = f" ({round(video.avg_rating, 1)}⭐)" if video.ratings_count > 0 else ""
        
        msg += f"{i}. "
        
        if video.is_series:
            msg += f"📺 {video.title or 'Untitled'} S{video.season}E{video.episode}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"   <code>{video.code}</code> (👁️ {video.views_count}){premium_mark}{rating_mark}\n\n"
    
    await message.answer(msg)

async def show_recommendations(message: Message, user_id: int, video_code: str):
    """Показати рекомендації після перегляду"""
    recommendations = await get_recommendations(video_code, user_id, limit=RECOMMENDATIONS_LIMIT)
    
    if not recommendations:
        return
    
    msg = get_text(user_id, "recommendations_header")
    
    for video in recommendations:
        premium_mark = " ⭐" if video.is_premium else ""
        
        if video.is_series:
            msg += f"📺 {video.title or 'Untitled'} S{video.season}E{video.episode}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"<code>{video.code}</code>{premium_mark}\n\n"
    
    await message.answer(msg)

async def show_referral_info(message: Message, user_id: int):
    """Показати реферальну інформацію"""
    if not REFERRAL_ENABLED:
        return
    
    # Згенерувати реферальний код якщо немає
    referral_code = await referral_system.generate_referral_code(user_id)
    
    # Статистика
    stats = await referral_system.get_referral_stats(user_id)
    
    # Реферальне посилання
    bot_info = await bot.get_me()
    referral_link = f"https://t.me/{bot_info.username}?start={referral_code}"
    
    msg = get_text(user_id, "referral_info",
        link=referral_link,
        bonus_days=referral_system.REFERRAL_BONUS_DAYS,
        payment_bonus_days=referral_system.REFERRAL_PAYMENT_BONUS_DAYS,
        total=stats["total_referrals"],
        premium=stats["premium_referrals"],
        bonus_earned=stats["bonus_days_earned"]
    )
    
    await message.answer(msg)

async def show_premium_plans(message: Message, user_id: int):
    """Показати тарифи преміуму"""
    await message.answer(
        get_text(user_id, "premium_info"),
        reply_markup=get_premium_keyboard(user_id)
    )
    logger.info(f"User {user_id} viewed premium plans")

async def show_subscription_status(message: Message, user_id: int):
    """Показати статус підписки користувача"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return
        
        if user.is_premium and user.premium_expires and user.premium_expires > datetime.utcnow():
            days_left = (user.premium_expires - datetime.utcnow()).days
            
            # Назва плану
            plan_name = user.premium_plan or "Unknown"
            if plan_name in PREMIUM_PLANS:
                lang = user.language or "en"
                if lang == "uk":
                    plan_name = PREMIUM_PLANS[plan_name]["name_uk"]
                elif lang == "ru":
                    plan_name = PREMIUM_PLANS[plan_name]["name_ru"]
                else:
                    plan_name = PREMIUM_PLANS[plan_name]["name_en"]
            
            msg = get_text(user_id, "premium_status_active",
                plan=plan_name,
                expires=user.premium_expires.strftime("%Y-%m-%d %H:%M"),
                days_left=days_left
            )
            
            await message.answer(msg)
        else:
            msg = get_text(user_id, "premium_status_inactive")
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=get_text(user_id, "subscribe_button"),
                    callback_data="show_premium_plans"
                )]
            ])
            
            await message.answer(msg, reply_markup=kb)

async def show_statistics(message: Message, user_id: int):
    """Показати статистику бота (адмін)"""
    async with async_session_maker() as session:
        # Підрахунок користувачів
        result = await session.execute(select(func.count(User.id)))
        users_count = result.scalar() or 0
        
        # Підрахунок відео
        result = await session.execute(select(func.count(Video.id)))
        videos_count = result.scalar() or 0
        
        # Підрахунок серіалів
        result = await session.execute(
            select(func.count(func.distinct(Video.series_name)))
            .where(Video.is_series == True)
        )
        series_count = result.scalar() or 0
        
        # Підрахунок адмінів
        result = await session.execute(
            select(func.count(User.id)).where(User.is_admin == True)
        )
        admins_count = result.scalar() or 0
        
        # Загальна кількість переглядів
        result = await session.execute(select(func.sum(Video.views_count)))
        total_views = result.scalar() or 0
        
        # Преміум користувачів
        result = await session.execute(
            select(func.count(User.id))
            .where(
                and_(
                    User.is_premium == True,
                    User.premium_expires > datetime.utcnow()
                )
            )
        )
        premium_users = result.scalar() or 0
        
        # Дохід
        result = await session.execute(
            select(func.sum(Payment.amount))
            .where(Payment.currency == "XTR")
        )
        revenue = result.scalar() or 0
    
    msg = get_text(user_id, "stats_message",
        users=users_count,
        videos=videos_count,
        series=series_count,
        admins=admins_count,
        total_views=total_views,
        premium_users=premium_users,
        revenue=revenue,
        last_update=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    await message.answer(msg)
    log_admin_action(user_id, "STATS_VIEWED")

async def show_analytics(message: Message, user_id: int):
    """Показати поглиблену аналітику (адмін)"""
    if not ANALYTICS_ENABLED:
        await message.answer("❌ Аналітика вимкнена.")
        return
    
    # Retention
    retention = await analytics.get_retention_report(days=30)
    
    # Аналітика реклами
    ad_stats = await analytics.get_ad_analytics()
    
    # Пошукова аналітика
    search_stats = await analytics.get_search_analytics()
    
    # Дохід
    revenue_stats = await analytics.get_revenue_analytics()
    
    # Контент
    content_stats = await analytics.get_content_analytics()
    
    msg = f"""📊 <b>Поглиблена Аналітика</b>

👥 <b>Утримання користувачів (30 днів)</b>
• Нові: {retention['new_users']}
• Активні: {retention['active_users']}
• Retention: {retention['retention_rate']}%

📢 <b>Аналітика реклами</b>
• Показів всього: {ad_stats['total_shows']}
• Кліків всього: {ad_stats['total_clicks']}
• CTR: {ad_stats['overall_ctr']}%
• CTR за тиждень: {ad_stats['weekly_ctr']}%

🔍 <b>Пошукова аналітика</b>
• Всього пошуків: {search_stats['total_searches']}
• За тиждень: {search_stats['weekly_searches']}
• Без результатів: {search_stats['failed_percentage']}%

💰 <b>Дохід</b>
• Всього: {revenue_stats['total_revenue']} ⭐
• За місяць: {revenue_stats['monthly_revenue']} ⭐
• Платежів: {revenue_stats['total_payments']}
• Середній чек: {revenue_stats['avg_payment']} ⭐

🎬 <b>Контент</b>
• Всього відео: {content_stats['total_videos']}
• Безкоштовних: {content_stats['premium_breakdown']['free']}
• Преміум: {content_stats['premium_breakdown']['premium']}
"""
    
    # Топ пошукових запитів
    if search_stats['popular_queries']:
        msg += "\n🔥 <b>Топ-5 пошукових запитів:</b>\n"
        for query in search_stats['popular_queries'][:5]:
            msg += f"• {query['query']} ({query['count']})\n"
    
    await message.answer(msg)
    log_admin_action(user_id, "ANALYTICS_VIEWED")

async def show_all_videos(message: Message, user_id: int, page: int = 1):
    """Показати список всіх відео з пагінацією"""
    per_page = 10
    
    async with async_session_maker() as session:
        # Загальна кількість
        result = await session.execute(select(func.count(Video.id)))
        total_videos = result.scalar() or 0
        
        if total_videos == 0:
            await message.answer("❌ Відео поки немає.")
            return
        
        total_pages = (total_videos + per_page - 1) // per_page
        offset = (page - 1) * per_page
        
        # Отримати відео для сторінки
        result = await session.execute(
            select(Video)
            .order_by(Video.created_at.desc())
            .limit(per_page)
            .offset(offset)
        )
        videos = result.scalars().all()
    
    msg = f"📝 <b>Всі відео</b> (Стор {page}/{total_pages})\n\n"
    
    for video in videos:
        premium_mark = " ⭐" if video.is_premium else ""
        
        if video.is_series:
            msg += f"📺 S{video.season}E{video.episode} {video.title or 'Untitled'}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"<code>{video.code}</code> (👁️ {video.views_count}){premium_mark}\n\n"
    
    # Пагінація
    buttons = []
    nav_buttons = []
    
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"videos_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"videos_page_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await message.answer(msg, reply_markup=kb)

async def show_all_series(message: Message, user_id: int):
    """Показати список всіх серіалів"""
    async with async_session_maker() as session:
        # Отримати унікальні серіали
        result = await session.execute(
            select(Video.series_name, func.count(Video.id).label('episodes_count'))
            .where(Video.is_series == True)
            .group_by(Video.series_name)
            .order_by(Video.series_name)
        )
        series_list = result.all()
    
    if not series_list:
        await message.answer("❌ Серіалів поки немає.")
        return
    
    msg = "📺 <b>Усі серіали</b>\n\n"
    
    for series_name, episodes_count in series_list:
        msg += f"📺 <b>{series_name}</b>\n"
        msg += f"   Епізодів: {episodes_count}\n"
        msg += f"   Мастер-код: <code>{series_name}</code>\n\n"
    
    await message.answer(msg)

# ==================== ОБРОБНИК ПОШУКУ ====================

@dp.message(UserState.waiting_for_search, F.text)
async def handle_search(message: Message, state: FSMContext):
    """Обробник пошуку з аналітикою"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    # Rate limit
    if not check_rate_limit(user_id):
        await message.answer(get_text(user_id, "rate_limit"))
        return
    
    query = input_validator.sanitize_html(message.text.strip().lower())
    
    async with async_session_maker() as session:
        # Пошук по відео
        result = await session.execute(
            select(Video)
            .where(
                or_(
                    Video.title.ilike(f"%{query}%"),
                    Video.genre.ilike(f"%{query}%"),
                    Video.year.ilike(f"%{query}%"),
                    Video.description.ilike(f"%{query}%")
                )
            )
            .order_by(Video.avg_rating.desc(), Video.views_count.desc())
            .limit(20)
        )
        videos = result.scalars().all()
        
        # Фільтрація по доступу до преміуму
        is_premium = await is_premium_user(user_id)
        is_admin_user = await is_admin(user_id)
        
        accessible_videos = []
        for video in videos:
            if video.is_premium:
                if is_premium or is_admin_user:
                    accessible_videos.append(video)
            else:
                accessible_videos.append(video)
        
        results_count = len(accessible_videos)
        
        # Зберегти пошуковий запит
        if SAVE_SEARCH_QUERIES:
            await log_search_query(user_id, query, results_count)
    
    if not accessible_videos:
        await message.answer(get_text(user_id, "no_results"))
        await state.set_state(UserState.waiting_for_code)
        return
    
    msg = get_text(user_id, "search_results")
    
    for video in accessible_videos:
        premium_mark = " ⭐" if video.is_premium else ""
        rating_mark = f" ({round(video.avg_rating, 1)}⭐)" if video.ratings_count > 0 else ""
        
        if video.is_series:
            msg += f"📺 S{video.season}E{video.episode} {video.title or 'Untitled'}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        if video.genre:
            msg += f"🎭 {video.genre}"
        if video.year:
            msg += f" | 📅 {video.year}"
        
        msg += f"\n<code>{video.code}</code> (👁️ {video.views_count}){premium_mark}{rating_mark}\n\n"
    
    if len(accessible_videos) == 20:
        msg += "\n... та можливо ще більше результатів"
    
    await message.answer(msg)
    logger.info(f"User {user_id} searched: '{query}' - {results_count} results")
    
    await state.set_state(UserState.waiting_for_code)

# ==================== ЗАВАНТАЖЕННЯ ВІДЕО (АДМІН) ====================

@dp.message(UserState.waiting_for_video, F.video)
async def handle_video_upload(message: Message, state: FSMContext):
    """Завантаження відео (тільки адміни)"""
    user_id = message.from_user.id
    
    if not await is_admin(user_id):
        await message.answer("❌ Тільки адміністратори можуть завантажувати відео!")
        security_logger.log_suspicious_activity(user_id, "UNAUTHORIZED_VIDEO_UPLOAD_ATTEMPT")
        await state.set_state(UserState.waiting_for_code)
        return
    
    if message.video.file_size > MAX_VIDEO_SIZE:
        size_mb = message.video.file_size / (1024 * 1024)
        await message.answer(get_text(user_id, "video_too_large"))
        await message.answer(f"📊 Розмір: {size_mb:.1f} МБ\nМаксимум: 2048 МБ")
        return
    
    file_id = message.video.file_id
    file_size_mb = message.video.file_size / (1024 * 1024)
    
    await state.update_data(video_file_id=file_id)
    await message.answer(f"✅ Відео прийнято ({file_size_mb:.1f} МБ)")
    
    # Вибір мови відео
    await message.answer(
        get_text(user_id, "select_video_language"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=get_text(user_id, "lang_english"), callback_data="vlang_1")],
            [InlineKeyboardButton(text=get_text(user_id, "lang_russian"), callback_data="vlang_2")],
            [InlineKeyboardButton(text=get_text(user_id, "lang_chinese"), callback_data="vlang_3")],
            [InlineKeyboardButton(text=get_text(user_id, "lang_spanish"), callback_data="vlang_4")],
            [InlineKeyboardButton(text=get_text(user_id, "lang_hindi"), callback_data="vlang_5")],
        ])
    )
    await state.set_state(UserState.waiting_for_video_language)
    
    log_admin_action(user_id, "VIDEO_UPLOAD_STARTED", f"Size: {file_size_mb:.1f} MB")

@dp.callback_query(F.data.startswith("vlang_"), UserState.waiting_for_video_language)
async def handle_video_language(callback: CallbackQuery, state: FSMContext):
    """Вибір мови відео"""
    lang_digit = callback.data.split("_")[1]
    await state.update_data(video_language=int(lang_digit))
    
    # Вибір типу відео
    await callback.message.edit_text(
        get_text(callback.from_user.id, "select_video_type"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=get_text(callback.from_user.id, "type_movie"), callback_data="vtype_1")],
            [InlineKeyboardButton(text=get_text(callback.from_user.id, "type_series"), callback_data="vtype_2")],
            [InlineKeyboardButton(text=get_text(callback.from_user.id, "type_anime"), callback_data="vtype_3")],
        ])
    )
    await state.set_state(UserState.waiting_for_video_type)

@dp.callback_query(F.data.startswith("vtype_"), UserState.waiting_for_video_type)
async def handle_video_type(callback: CallbackQuery, state: FSMContext):
    """Вибір типу відео"""
    type_digit = callback.data.split("_")[1]
    await state.update_data(video_type=int(type_digit))
    
    # Питання про преміум
    await callback.message.edit_text(
        get_text(callback.from_user.id, "is_premium_question"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=get_text(callback.from_user.id, "yes_premium"), callback_data="videopremium_yes")],
            [InlineKeyboardButton(text=get_text(callback.from_user.id, "no_free"), callback_data="videopremium_no")],
        ])
    )
    await state.set_state(UserState.waiting_for_premium_choice)

@dp.callback_query(F.data.startswith("videopremium_"), UserState.waiting_for_premium_choice)
async def handle_video_premium_choice(callback: CallbackQuery, state: FSMContext):
    """Вибір преміум статусу"""
    choice = callback.data.split("_")[1]
    is_premium = (choice == "yes")
    
    await state.update_data(is_premium=is_premium)
    
    user_data = await state.get_data()
    video_type = user_data.get("video_type", 1)
    
    # Якщо серіал або аніме - запитати про епізоди
    if video_type in [2, 3]:
        await callback.message.edit_text(
            get_text(callback.from_user.id, "is_series_question"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=get_text(callback.from_user.id, "yes_series"), callback_data="series_yes")],
                [InlineKeyboardButton(text=get_text(callback.from_user.id, "no_single"), callback_data="series_no")],
            ])
        )
        await state.set_state(UserState.waiting_for_series_choice)
    else:
        await callback.message.delete()
        await callback.message.answer(
            get_text(callback.from_user.id, "enter_title"),
            reply_markup=get_cancel_keyboard(callback.from_user.id)
        )
        await state.set_state(UserState.waiting_for_title)

@dp.callback_query(F.data.startswith("series_"), UserState.waiting_for_series_choice)
async def handle_series_choice(callback: CallbackQuery, state: FSMContext):
    """Вибір чи це серіал"""
    choice = callback.data.split("_")[1]
    
    if choice == "yes":
        await state.update_data(is_series=True)
        await callback.message.delete()
        await callback.message.answer(
            get_text(callback.from_user.id, "enter_series_name"),
            reply_markup=get_cancel_keyboard(callback.from_user.id)
        )
        await state.set_state(UserState.waiting_for_series_name)
    else:
        await state.update_data(is_series=False)
        await callback.message.delete()
        await callback.message.answer(
            get_text(callback.from_user.id, "enter_title"),
            reply_markup=get_cancel_keyboard(callback.from_user.id)
        )
        await state.set_state(UserState.waiting_for_title)

@dp.message(UserState.waiting_for_series_name, F.text)
async def handle_series_name(message: Message, state: FSMContext):
    """Введення назви серіалу"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    series_name = input_validator.sanitize_html(message.text.strip().lower())
    
    if not re.match(r'^[a-z0-9]+$', series_name):
        await message.answer(get_text(user_id, "series_name_invalid"))
        return
    
    await state.update_data(series_name=series_name)
    await message.answer(
        get_text(user_id, "enter_season_number"),
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_season)

@dp.message(UserState.waiting_for_season, F.text)
async def handle_season(message: Message, state: FSMContext):
    """Введення номера сезону"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    try:
        season = int(message.text.strip())
        if season < 1:
            raise ValueError
        await state.update_data(season=season)
        
        await message.answer(
            get_text(user_id, "enter_episode_number"),
            reply_markup=get_cancel_keyboard(user_id)
        )
        await state.set_state(UserState.waiting_for_episode)
    except ValueError:
        await message.answer(get_text(user_id, "number_invalid"))

@dp.message(UserState.waiting_for_episode, F.text)
async def handle_episode(message: Message, state: FSMContext):
    """Введення номера епізоду"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    try:
        episode = int(message.text.strip())
        if episode < 1:
            raise ValueError
        await state.update_data(episode=episode)
        
        await message.answer(
            get_text(user_id, "enter_title"),
            reply_markup=get_cancel_keyboard(user_id)
        )
        await state.set_state(UserState.waiting_for_title)
    except ValueError:
        await message.answer(get_text(user_id, "number_invalid"))

@dp.message(UserState.waiting_for_title, F.text)
async def handle_title(message: Message, state: FSMContext):
    """Введення назви"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    title = input_validator.sanitize_html(message.text.strip()) if message.text.strip() != "/skip" else ""
    await state.update_data(title=title)
    
    await message.answer(
        get_text(user_id, "enter_year"),
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_year)

@dp.message(UserState.waiting_for_year, F.text)
async def handle_year(message: Message, state: FSMContext):
    """Введення року"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    year = input_validator.sanitize_html(message.text.strip()) if message.text.strip() != "/skip" else ""
    await state.update_data(year=year)
    
    await message.answer(
        get_text(user_id, "enter_genre"),
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_genre)

@dp.message(UserState.waiting_for_genre, F.text)
async def handle_genre(message: Message, state: FSMContext):
    """Введення жанру"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    genre = input_validator.sanitize_html(message.text.strip()) if message.text.strip() != "/skip" else ""
    await state.update_data(genre=genre)
    
    await message.answer(
        get_text(user_id, "enter_description"),
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_description)

@dp.message(UserState.waiting_for_description, F.text)
async def handle_description(message: Message, state: FSMContext):
    """Введення опису"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    description = input_validator.sanitize_html(message.text.strip()) if message.text.strip() != "/skip" else ""
    await state.update_data(description=description)
    
    await message.answer(
        get_text(user_id, "send_poster"),
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_poster)

@dp.message(UserState.waiting_for_poster, F.photo)
async def handle_poster_upload(message: Message, state: FSMContext):
    """Завантаження постера"""
    photo_id = message.photo[-1].file_id
    await state.update_data(poster_file_id=photo_id)
    await message.answer("✅ Постер додано!")
    
    await message.answer(
        get_text(message.from_user.id, "enter_custom_code_prompt"),
        reply_markup=get_cancel_keyboard(message.from_user.id)
    )
    await state.set_state(UserState.waiting_for_custom_code)

@dp.message(UserState.waiting_for_poster, F.text)
async def handle_skip_poster(message: Message, state: FSMContext):
    """Пропустити постер"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    if message.text.strip() == "/skip":
        await message.answer(
            get_text(message.from_user.id, "enter_custom_code_prompt"),
            reply_markup=get_cancel_keyboard(user_id)
        )
        await state.set_state(UserState.waiting_for_custom_code)

@dp.message(UserState.waiting_for_custom_code, F.text)
async def handle_custom_code(message: Message, state: FSMContext):
    """Введення коду або генерація випадкового"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    user_data = await state.get_data()
    is_series = user_data.get("is_series", False)
    
    # Генерація або валідація коду
    if text == "/random":
        new_code = ''.join(random.choices(string.digits, k=6))
        
        # Перевірка унікальності
        async with async_session_maker() as session:
            while True:
                result = await session.execute(
                    select(Video).where(Video.code == new_code)
                )
                if not result.scalar_one_or_none():
                    break
                new_code = ''.join(random.choices(string.digits, k=6))
    else:
        # Валідація коду
        if not input_validator.validate_code(text, max_length=15):
            await message.answer(get_text(user_id, "code_invalid_error"))
            security_logger.log_suspicious_activity(user_id, "INVALID_CODE_INPUT", text)
            return
        
        new_code = text
        
        # Перевірка чи код існує
        async with async_session_maker() as session:
            result = await session.execute(
                select(Video).where(Video.code == new_code)
            )
            if result.scalar_one_or_none():
                await message.answer(get_text(user_id, "code_exists_error"))
                return
    
    # Створити відео в БД
    async with async_session_maker() as session:
        video = Video(
            code=new_code,
            file_id=user_data.get("video_file_id"),
            title=user_data.get("title", ""),
            year=user_data.get("year", ""),
            genre=user_data.get("genre", ""),
            description=user_data.get("description", ""),
            poster_file_id=user_data.get("poster_file_id"),
            is_series=is_series,
            series_name=user_data.get("series_name"),
            season=user_data.get("season"),
            episode=user_data.get("episode"),
            is_premium=user_data.get("is_premium", False),
            language=user_data.get("video_language", 1),
            video_type=user_data.get("video_type", 1),
            uploaded_by=user_id,
            views_count=0
        )
        
        session.add(video)
        await session.commit()
    
    # Повідомлення про успіх
    if is_series:
        series_name = user_data.get("series_name")
        season = user_data.get("season")
        episode = user_data.get("episode")
        
        premium_mark = " ⭐ ПРЕМІУМ" if user_data.get("is_premium") else ""
        
        await message.answer(
            get_text(user_id, "series_upload_success",
                code=new_code, series=series_name, season=season, episode=episode) + premium_mark,
            reply_markup=get_admin_keyboard(user_id)
        )
        
        # Сповіщення підписників серіалу
        if SERIES_NOTIFICATIONS_ENABLED:
            subscribers = await get_series_subscribers(series_name)
            
            if subscribers:
                notification_text = get_text(user_id, "new_episode_notification",
                    series=series_name,
                    season=season,
                    episode=episode,
                    code=new_code
                )
                
                stats = await broadcast_manager.send_to_series_subscribers(
                    series_name=series_name,
                    text=notification_text
                )
                
                await message.answer(f"📢 Підписників сповіщено: {stats['success']}/{stats['total']}")
        
        log_admin_action(user_id, "SERIES_EPISODE_UPLOADED",
            f"{series_name} S{season}E{episode} - {new_code} (Premium: {user_data.get('is_premium')})")
    else:
        premium_mark = " ⭐ ПРЕМІУМ" if user_data.get("is_premium") else ""
        
        await message.answer(
            get_text(user_id, "video_upload_success", code=new_code) + premium_mark,
            reply_markup=get_admin_keyboard(user_id)
        )
        
        log_admin_action(user_id, "VIDEO_UPLOADED",
            f"{user_data.get('title', 'Untitled')} - {new_code} (Premium: {user_data.get('is_premium')})")
    
    await state.clear()
    await state.set_state(UserState.waiting_for_code)

# ==================== CALLBACK ОБРОБНИКИ ====================

@dp.callback_query(F.data == "show_premium_plans")
async def handle_show_premium_plans(callback: CallbackQuery):
    """Показати тарифи преміуму"""
    await callback.message.edit_text(
        get_text(callback.from_user.id, "premium_info"),
        reply_markup=get_premium_keyboard(callback.from_user.id)
    )

@dp.callback_query(F.data.startswith("buy_premium_"))
async def handle_buy_premium(callback: CallbackQuery):
    """Обробник покупки преміуму"""
    plan = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    
    plan_info = PREMIUM_PLANS[plan]
    
    # Отримати мову
    async with async_session_maker() as session:
        result = await session.execute(
            select(User.language).where(User.telegram_id == user_id)
        )
        lang = result.scalar_one_or_none() or "en"
    
    if lang == "uk":
        title = plan_info["name_uk"]
        description = f"Преміум підписка на {plan_info['duration_days']} днів"
    elif lang == "ru":
        title = plan_info["name_ru"]
        description = f"Премиум подписка на {plan_info['duration_days']} дней"
    else:
        title = plan_info["name_en"]
        description = f"Premium subscription for {plan_info['duration_days']} days"
    
    prices = [LabeledPrice(label=title, amount=plan_info["price_stars"])]
    
    try:
        await bot.send_invoice(
            chat_id=callback.message.chat.id,
            title=title,
            description=description,
            payload=f"premium_{plan}_{user_id}_{int(time.time())}",
            provider_token="",
            currency="XTR",
            prices=prices,
        )
        
        await callback.answer("⭐ Рахунок надіслано!")
        logger.info(f"Invoice sent to user {user_id} for plan {plan}")
        
    except Exception as e:
        logger.error(f"Error sending invoice: {e}")
        await callback.answer("❌ Помилка при створенні рахунку", show_alert=True)

@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    """Обробка передоплати"""
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    logger.info(f"Pre-checkout query from user {pre_checkout_query.from_user.id}")

@dp.message(F.successful_payment)
async def process_successful_payment(message: Message):
    """Обробка успішного платежу"""
    user_id = message.from_user.id
    payment_info = message.successful_payment
    
    payload_parts = payment_info.invoice_payload.split("_")
    plan = payload_parts[1]
    
    logger.info(f"Payment received: User {user_id}, Plan {plan}, Amount {payment_info.total_amount} Stars")
    
    # Активувати преміум
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if user:
            duration_days = PREMIUM_PLANS[plan]["duration_days"]
            
            if user.is_premium and user.premium_expires and user.premium_expires > datetime.utcnow():
                # Продовжити існуючий
                user.premium_expires += timedelta(days=duration_days)
            else:
                # Новий преміум
                user.is_premium = True
                user.premium_expires = datetime.utcnow() + timedelta(days=duration_days)
                user.premium_plan = plan
            
            expires = user.premium_expires
            
            # Зберегти платіж
            payment = Payment(
                user_id=user_id,
                plan=plan,
                amount=payment_info.total_amount,
                currency="XTR",
                telegram_payment_charge_id=payment_info.telegram_payment_charge_id,
                status="completed"
            )
            session.add(payment)
            
            await session.commit()
            
            # Нагородити реферера
            if REFERRAL_ENABLED:
                await referral_system.reward_referrer_on_payment(user_id)
            
            await message.answer(
                get_text(user_id, "premium_purchased",
                    expires=expires.strftime("%Y-%m-%d %H:%M"))
            )
            
            premium_logger.info(f"Payment completed: User {user_id}, Plan {plan}, Amount {payment_info.total_amount} Stars")

@dp.callback_query(F.data.startswith("fav_"))
async def handle_favorite_toggle(callback: CallbackQuery):
    """Додати/видалити з улюбленого"""
    video_code = callback.data.split("_", 1)[1]
    user_id = callback.from_user.id
    
    added = await toggle_favorite(user_id, video_code)
    
    if added:
        await callback.answer(get_text(user_id, "added_to_favorites"))
    else:
        await callback.answer(get_text(user_id, "removed_from_favorites"))
    
    logger.info(f"User {user_id} toggled favorite for video {video_code}: {added}")

@dp.callback_query(F.data.startswith("rate_"))
async def handle_rating(callback: CallbackQuery):
    """Обробник рейтингу"""
    parts = callback.data.split("_")
    
    if parts[-1] == "prompt":
        # Показати клавіатуру рейтингу
        video_code = parts[1]
        await callback.message.answer(
            get_text(callback.from_user.id, "rate_video"),
            reply_markup=get_rating_keyboard(video_code)
        )
    else:
        # Зберегти рейтинг
        video_code = parts[1]
        stars = int(parts[2])
        user_id = callback.from_user.id
        
        await rate_video(user_id, video_code, stars)
        await callback.answer(get_text(user_id, "thanks_for_rating"))
        
        logger.info(f"User {user_id} rated video {video_code} with {stars} stars")

@dp.callback_query(F.data.startswith("sub_"))
async def handle_series_subscription(callback: CallbackQuery):
    """Підписка на серіал"""
    series_name = callback.data.split("_", 1)[1]
    user_id = callback.from_user.id
    
    subscribed = await subscribe_to_series(user_id, series_name)
    
    if subscribed:
        await callback.answer(get_text(user_id, "subscribed_to_series", series=series_name))
    else:
        await callback.answer(get_text(user_id, "unsubscribed_from_series", series=series_name))
    
    logger.info(f"User {user_id} subscription to {series_name}: {subscribed}")

@dp.callback_query(F.data.startswith("browse_"))
async def handle_browse_category(callback: CallbackQuery):
    """Перегляд по категоріях"""
    category = callback.data.split("_")[1]
    user_id = callback.from_user.id
    
    # Визначити тип відео
    video_type_map = {
        "movies": 1,
        "series": 2,
        "anime": 3
    }
    
    video_type = video_type_map.get(category, 1)
    
    async with async_session_maker() as session:
        result = await session.execute(
            select(Video)
            .where(Video.video_type == video_type)
            .order_by(Video.avg_rating.desc(), Video.views_count.desc())
            .limit(20)
        )
        videos = result.scalars().all()
    
    if not videos:
        await callback.answer("❌ Немає відео в цій категорії")
        return
    
    category_names = {
        "movies": "🎬 Фільми",
        "series": "📺 Серіали",
        "anime": "🎌 Аніме"
    }
    
    msg = f"<b>{category_names[category]}</b>\n\n"
    
    for video in videos:
        premium_mark = " ⭐" if video.is_premium else ""
        rating_mark = f" ({round(video.avg_rating, 1)}⭐)" if video.ratings_count > 0 else ""
        
        if video.is_series:
            msg += f"📺 {video.title or 'Untitled'} S{video.season}E{video.episode}\n"
        else:
            msg += f"🎬 {video.title or 'Untitled'}\n"
        
        msg += f"<code>{video.code}</code>{premium_mark}{rating_mark}\n\n"
    
    await callback.message.edit_text(msg)

@dp.callback_query(F.data.startswith("broadcast_"))
async def handle_broadcast_type(callback: CallbackQuery, state: FSMContext):
    """Обробник вибору типу розсилки"""
    broadcast_type = callback.data.split("_")[1]
    user_id = callback.from_user.id
    
    if not await is_admin(user_id):
        await callback.answer("❌ Тільки для адміністраторів")
        return
    
    await state.update_data(broadcast_type=broadcast_type)
    await callback.message.edit_text(get_text(user_id, "enter_broadcast_message"))
    await state.set_state(UserState.waiting_for_broadcast_message)

# ==================== ОБРОБНИК РОЗСИЛКИ ====================

@dp.message(UserState.waiting_for_broadcast_message, F.text)
async def handle_broadcast_message(message: Message, state: FSMContext):
    """Обробник тексту розсилки"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    if not await is_admin(user_id):
        return
    
    user_data = await state.get_data()
    broadcast_type = user_data.get("broadcast_type", "all")
    
    filter_premium = None
    if broadcast_type == "free":
        filter_premium = False
    elif broadcast_type == "premium":
        filter_premium = True
    
    async with async_session_maker() as session:
        query = select(func.count(User.id))
        if filter_premium is not None:
            query = query.where(User.is_premium == filter_premium)
        result = await session.execute(query)
        target_count = result.scalar() or 0
    
    await message.answer(get_text(user_id, "broadcast_started", target=target_count))
    
    stats = await broadcast_manager.send_broadcast(text=text, filter_premium=filter_premium)
    
    await message.answer(get_text(user_id, "broadcast_completed",
        success=stats["success"], failed=stats["failed"], blocked=stats["blocked"], total=stats["total"]))
    
    log_admin_action(user_id, "BROADCAST_SENT", f"Type: {broadcast_type}, Stats: {stats}")
    
    await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    await state.set_state(UserState.waiting_for_code)

@dp.callback_query(F.data.startswith("promo_"))
async def handle_promo_actions(callback: CallbackQuery, state: FSMContext):
    """Обробник дій з промокодами"""
    action = callback.data.split("_")[1]
    user_id = callback.from_user.id
    
    if action == "activate":
        await callback.message.edit_text(get_text(user_id, "enter_promo_code"))
        await state.set_state(UserState.waiting_for_promo_code)
    
    elif action == "create":
        if not await is_admin(user_id):
            await callback.answer("❌ Тільки для адміністраторів")
            return
        
        # Створити промокод
        success, code = await promo_manager.generate_promo(
            duration_days=7,
            max_uses=100,
            created_by=user_id,
            expires_in_days=30
        )
        
        if success:
            await callback.message.edit_text(
                f"✅ Промокод створено!\n\n"
                f"Код: <code>{code}</code>\n"
                f"Тривалість: 7 днів\n"
                f"Макс використань: 100\n"
                f"Діє до: {(datetime.utcnow() + timedelta(days=30)).strftime('%Y-%m-%d')}"
            )
            log_admin_action(user_id, "PROMO_CREATED", code)
        else:
            await callback.message.edit_text(f"❌ Помилка: {code}")
    
    elif action == "list":
        if not await is_admin(user_id):
            await callback.answer("❌ Тільки для адміністраторів")
            return
        
        promos = await promo_manager.list_active_promos()
        
        if not promos:
            await callback.message.edit_text("❌ Активних промокодів немає")
            return
        
        msg = "🎫 <b>Активні промокоди</b>\n\n"
        
        for promo in promos:
            msg += f"<code>{promo['code']}</code>\n"
            msg += f"  Тривалість: {promo['duration_days']} днів\n"
            msg += f"  Використано: {promo['uses']}\n"
            msg += f"  Діє до: {promo['expires']}\n\n"
        
        await callback.message.edit_text(msg)

# ==================== ОБРОБНИК ПРОМОКОДІВ ====================

@dp.message(UserState.waiting_for_promo_code, F.text)
async def handle_promo_activation(message: Message, state: FSMContext):
    """Активація промокоду"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    success, msg = await promo_manager.activate_promo(user_id, text)
    await message.answer(msg)
    
    if success:
        logger.info(f"User {user_id} activated promo code: {text}")
    
    if await is_admin(user_id):
        await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    else:
        await message.answer("✅ Готово!", reply_markup=get_user_keyboard(user_id))
    
    await state.set_state(UserState.waiting_for_code)

@dp.callback_query(F.data.startswith("videos_page_"))
async def handle_videos_pagination(callback: CallbackQuery):
    """Пагінація списку відео"""
    page = int(callback.data.split("_")[-1])
    await callback.message.delete()
    await show_all_videos(callback.message, callback.from_user.id, page)

@dp.callback_query(F.data == "noop")
async def handle_noop(callback: CallbackQuery):
    """Порожня дія"""
    await callback.answer()

# ==================== ЗАХИСТ ВІД МЕДІА-СПАМУ ====================

@dp.message(F.sticker)
async def handle_sticker_spam(message: Message):
    """Захист від стікерів"""
    user_id = message.from_user.id
    
    if user_id in BLOCKED_USERS:
        return
    
    if spam_protection.is_spam(user_id, media_type="sticker"):
        security_logger.log_suspicious_activity(user_id, "STICKER_SPAM_DETECTED")
        rate_limiter.block_user(user_id, BLOCK_TIME)
        return

@dp.message(F.animation)
async def handle_gif_spam(message: Message):
    """Захист від GIF"""
    user_id = message.from_user.id
    
    if user_id in BLOCKED_USERS:
        return
    
    if spam_protection.is_spam(user_id, media_type="gif"):
        security_logger.log_suspicious_activity(user_id, "GIF_SPAM_DETECTED")
        rate_limiter.block_user(user_id, BLOCK_TIME)
        return

@dp.message(F.photo)  # ✅ ВИПРАВЛЕНО
async def handle_photo_spam(message: Message, state: FSMContext):
    """Захист від фото (крім адмінів у потрібному стані)"""
    user_id = message.from_user.id
    current_state = await state.get_state()
    
    # Дозволити фото при завантаженні постера
    if current_state == UserState.waiting_for_poster:
        return
    
    # Адміни можуть відправляти фото
    if await is_admin(user_id):
        return
    
    if user_id in BLOCKED_USERS:
        return
    
    if spam_protection.is_spam(user_id, media_type="photo"):
        security_logger.log_suspicious_activity(user_id, "PHOTO_SPAM_DETECTED")
        rate_limiter.block_user(user_id, BLOCK_TIME)
        return
# ==================== ОБРОБНИК НЕВІДОМИХ ПОВІДОМЛЕНЬ ====================

@dp.message(F.text)
async def handle_unhandled_messages(message: Message, state: FSMContext):
    """Обробник невідомих повідомлень"""
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer(
            "👋 Welcome! Please select your language:\n"
            "👋 Добро пожаловать! Выберите язык:\n"
            "👋 Ласкаво просимо! Оберіть мову:",
            reply_markup=get_language_keyboard()
        )

@dp.message()
async def handle_any_other_message(message: Message):
    """Fallback обробник"""
    await message.answer(
        "Please use /start\nИспользуйте /start\nВикористовуйте /start"
    )

# ==================== ДОДАТКОВІ АДМІНСЬКІ ОБРОБНИКИ ====================

async def send_video_to_user(message: Message, user_id: int, video: Video):
    """Відправити відео користувачу з усіма перевірками"""
    
    # Перевірка преміум доступу
    if video.is_premium:
        if not await is_premium_user(user_id) and not await is_admin(user_id):
            await message.answer(
                get_text(user_id, "premium_required"),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text=get_text(user_id, "subscribe_button"),
                        callback_data="show_premium_plans"
                    )]
                ])
            )
            logger.info(f"User {user_id} tried to access premium video {video.code}")
            return
    
    # Додати в історію
    await add_to_history(user_id, video.code)
    
    # Сформувати інформацію про відео
    msg_text = ""
    
    if video.title:
        if video.is_series:
            msg_text = get_text(user_id, "episode_info",
                title=video.title,
                season=video.season or "?",
                episode=video.episode or "?"
            )
        else:
            msg_text = get_text(user_id, "movie_info", title=video.title)
    
    if video.year:
        msg_text += get_text(user_id, "year_info", year=video.year)
    if video.genre:
        msg_text += get_text(user_id, "genre_info", genre=video.genre)
    if video.description:
        msg_text += get_text(user_id, "description_info", description=video.description)
    
    msg_text += get_text(user_id, "views_info", views=video.views_count)
    
    # Додати рейтинг
    if RATINGS_ENABLED and video.ratings_count > 0:
        msg_text += get_text(user_id, "rating_info",
            rating=round(video.avg_rating, 1),
            count=video.ratings_count
        )
    
    if video.is_premium:
        msg_text += get_text(user_id, "premium_badge")
    
    # Відправити текст
    if msg_text:
        await message.answer(msg_text)
    
    # Постер
    if video.poster_file_id:
        try:
            await bot.send_photo(message.chat.id, video.poster_file_id)
        except:
            pass
    
    # ПОКАЗАТИ РЕКЛАМУ
    await show_ad(message, user_id)
    
    # Відправити відео
    try:
        await bot.send_video(message.chat.id, video.file_id)
    except Exception as e:
        logger.error(f"Error sending video {video.code}: {e}")
        await message.answer("❌ Помилка при відправці відео. Спробуйте пізніше.")
        return
    
    # Кнопки дій (улюблене, рейтинг, підписка)
    action_kb = await get_video_action_keyboard(user_id, video.code, video.series_name)
    await message.answer("━━━━━━━━━━━━━━", reply_markup=action_kb)
    
    # Рекомендації
    if RECOMMENDATIONS_ENABLED:
        await show_recommendations(message, user_id, video.code)
    
    logger.info(f"User {user_id} viewed video: {video.code} (premium: {video.is_premium})")

# ==================== ВИДАЛЕННЯ ВІДЕО ====================

@dp.message(UserState.waiting_for_delete_code, F.text)
async def handle_delete_code(message: Message, state: FSMContext):
    """Видалення відео по коду"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    if not await is_admin(user_id):
        return
    
    code = input_validator.sanitize_html(text)
    
    async with async_session_maker() as session:
        result = await session.execute(select(Video).where(Video.code == code))
        video = result.scalar_one_or_none()
        
        if not video:
            await message.answer("❌ Відео з таким кодом не знайдено.")
            return
        
        await session.delete(video)
        await session.commit()
    
    await message.answer(f"✅ Відео <code>{code}</code> видалено!")
    log_admin_action(user_id, "VIDEO_DELETED", code)
    
    await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    await state.set_state(UserState.waiting_for_code)

# ==================== РЕДАГУВАННЯ ВІДЕО ====================

@dp.message(UserState.waiting_for_edit_code, F.text)
async def handle_edit_code(message: Message, state: FSMContext):
    """Вибір відео для редагування"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    if not await is_admin(user_id):
        return
    
    code = input_validator.sanitize_html(text)
    
    async with async_session_maker() as session:
        result = await session.execute(select(Video).where(Video.code == code))
        video = result.scalar_one_or_none()
        
        if not video:
            await message.answer("❌ Відео з таким кодом не знайдено.")
            return
    
    await state.update_data(edit_video_code=code)
    
    await message.answer(
        f"✏️ Редагування відео <code>{code}</code>\n\n"
        f"Що редагувати?\n\n"
        f"1️⃣ - Назва\n2️⃣ - Рік\n3️⃣ - Жанр\n4️⃣ - Опис\n5️⃣ - Преміум статус\n\n"
        f"Введіть номер поля:",
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_edit_field)

@dp.message(UserState.waiting_for_edit_field, F.text)
async def handle_edit_field(message: Message, state: FSMContext):
    """Вибір поля для редагування"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    field_map = {"1": "title", "2": "year", "3": "genre", "4": "description", "5": "is_premium"}
    
    if text not in field_map:
        await message.answer("❌ Невірний вибір. Введіть номер від 1 до 5.")
        return
    
    field = field_map[text]
    await state.update_data(edit_field=field)
    
    if field == "is_premium":
        await message.answer("⭐ Преміум статус:\n\n1 - Так\n0 - Ні", reply_markup=get_cancel_keyboard(user_id))
    else:
        await message.answer(f"✏️ Введіть нове значення для {field}:", reply_markup=get_cancel_keyboard(user_id))
    
    await state.set_state(UserState.waiting_for_edit_value)

@dp.message(UserState.waiting_for_edit_value, F.text)
async def handle_edit_value(message: Message, state: FSMContext):
    """Збереження нового значення"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    user_data = await state.get_data()
    code = user_data.get("edit_video_code")
    field = user_data.get("edit_field")
    
    async with async_session_maker() as session:
        result = await session.execute(select(Video).where(Video.code == code))
        video = result.scalar_one_or_none()
        
        if not video:
            await message.answer("❌ Відео не знайдено.")
            return
        
        if field == "is_premium":
            video.is_premium = (text == "1")
        else:
            setattr(video, field, input_validator.sanitize_html(text))
        
        await session.commit()
    
    await message.answer(f"✅ Поле {field} оновлено!")
    log_admin_action(user_id, "VIDEO_EDITED", f"{code} - {field}")
    
    await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    await state.set_state(UserState.waiting_for_code)

# ==================== ДОДАТИ АДМІНА ====================

@dp.message(UserState.waiting_for_admin_id, F.text)
async def handle_add_admin_id(message: Message, state: FSMContext):
    """Додавання адміна по ID"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    if not await is_admin(user_id):
        return
    
    new_admin_id = input_validator.validate_telegram_id(text)
    
    if not new_admin_id:
        await message.answer("❌ Невірний Telegram ID. Введіть число.")
        return
    
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == new_admin_id))
        user = result.scalar_one_or_none()
        
        if user:
            if user.is_admin:
                await message.answer(f"ℹ️ Користувач {new_admin_id} вже є адміністратором.")
            else:
                user.is_admin = True
                await session.commit()
                await message.answer(f"✅ Користувач {new_admin_id} тепер адміністратор!")
                log_admin_action(user_id, "ADMIN_ADDED", str(new_admin_id))
        else:
            new_user = User(telegram_id=new_admin_id, language="uk", is_admin=True)
            session.add(new_user)
            await session.commit()
            await message.answer(f"✅ Створено нового адміністратора: {new_admin_id}")
            log_admin_action(user_id, "ADMIN_CREATED", str(new_admin_id))
    
    await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    await state.set_state(UserState.waiting_for_code)

@dp.message(UserState.waiting_for_grant_premium_user, F.text)
async def handle_grant_premium_user(message: Message, state: FSMContext):
    """Вибір користувача для преміуму"""
    user_id = message.from_user.id
    
    # ✅ ПЕРЕВІРКА СКАСУВАННЯ
    if await check_cancel(message, state, user_id):
        return
    
    text = message.text.strip()
    
    if not await is_admin(user_id):
        return
    
    target_user_id = input_validator.validate_telegram_id(text)
    
    if not target_user_id:
        await message.answer("❌ Невірний Telegram ID. Введіть число.")
        return
    
    await state.update_data(grant_premium_user_id=target_user_id)
    
    await message.answer(
        f"⭐ Видати преміум користувачу {target_user_id}\n\n"
        f"Оберіть тариф:\n\n"
        f"1️⃣ - Тижневий (7 днів)\n2️⃣ - Місячний (30 днів)\n3️⃣ - Річний (365 днів)\n\n"
        f"Або введіть кількість днів вручну:",
        reply_markup=get_cancel_keyboard(user_id)
    )
    await state.set_state(UserState.waiting_for_grant_premium_plan)

@dp.message(UserState.waiting_for_grant_premium_plan, F.text)
async def handle_grant_premium_choice(message: Message, state: FSMContext):
    """Видача преміуму (створюємо користувача, якщо його немає в БД)"""
    user_id = message.from_user.id

    if await check_cancel(message, state, user_id):
        return

    if not await is_admin(user_id):
        return

    text = message.text.strip()
    user_data = await state.get_data()
    target_user_id = user_data.get("grant_premium_user_id")

    days_map = {"1": 7, "2": 30, "3": 365}

    if text in days_map:
        days = days_map[text]
    else:
        try:
            days = int(text)
            if days < 1:
                raise ValueError
        except ValueError:
            await message.answer("❌ Невірне значення. Введіть номер тарифу (1-3) або кількість днів.")
            return

    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == target_user_id))
        target_user = result.scalar_one_or_none()

        # Створюємо користувача, якщо його немає
        if not target_user:
            target_user = User(telegram_id=target_user_id, language="uk")
            session.add(target_user)
            await session.flush()

        # Продовження або новий преміум
        if target_user.is_premium and target_user.premium_expires and target_user.premium_expires > datetime.utcnow():
            target_user.premium_expires += timedelta(days=days)
        else:
            target_user.is_premium = True
            target_user.premium_expires = datetime.utcnow() + timedelta(days=days)
            target_user.premium_plan = "admin_granted"

        await session.commit()
        expires = target_user.premium_expires.strftime("%Y-%m-%d %H:%M")

    await message.answer(
        f"✅ Преміум видано!\n\n👤 Користувач: {target_user_id}\n⏰ Тривалість: {days} днів\n📅 Діє до: {expires}"
    )

    try:
        await bot.send_message(
            target_user_id,
            f"🎉 Вітаємо! Вам видано Преміум підписку!\n\n⏰ Тривалість: {days} днів\n📅 Діє до: {expires}\n\n✨ Користуйтесь без реклами!"
        )
    except:
        pass

    log_admin_action(user_id, "PREMIUM_GRANTED", f"{target_user_id} - {days} days")
    await message.answer("✅ Готово!", reply_markup=get_admin_keyboard(user_id))
    await state.set_state(UserState.waiting_for_code)

# ==================== ЕКСПОРТ БД ====================

async def handle_export_db(message: Message):
    """Експорт бази даних"""
    user_id = message.from_user.id
    
    if not await is_admin(user_id):
        return
    
    import os
    
    db_file = "bot_data.db"
    
    if not os.path.exists(db_file):
        await message.answer("❌ Файл бази даних не знайдено.")
        return
    
    try:
        document = InputFile(db_file)
        await message.answer_document(
            document=document,
            caption=f"📤 Експорт бази даних\n📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        log_admin_action(user_id, "DATABASE_EXPORTED")
    except Exception as e:
        await message.answer(f"❌ Помилка експорту: {e}")
        logger.error(f"Database export error: {e}")

# ==================== СПИСОК АДМІНІВ ====================
async def handle_list_admins(message: Message):
    """Показати список адмінів"""
    user_id = message.from_user.id
    
    if not await is_admin(user_id):
        return
    
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.is_admin == True)
        )
        admins = result.scalars().all()
    
    if not admins:
        await message.answer("❌ Адмінів не знайдено.")
        return
    
    msg = "👥 <b>Список адміністраторів</b>\n\n"
    
    for admin in admins:
        username = f"@{admin.username}" if admin.username else "—"
        name = admin.first_name or "Unknown"
        
        msg += f"• ID: <code>{admin.telegram_id}</code>\n"
        msg += f"  Ім'я: {name}\n"
        msg += f"  Username: {username}\n"
        msg += f"  Дата: {admin.created_at.strftime('%Y-%m-%d')}\n\n"
    
    await message.answer(msg)
    log_admin_action(user_id, "ADMINS_LIST_VIEWED")

async def ensure_primary_admin():
    """Гарантує, що ADMIN_ID існує в БД і має статус адміна."""
    try:
        async with async_session_maker() as session:
            result = await session.execute(select(User).where(User.telegram_id == ADMIN_ID))
            admin_user = result.scalar_one_or_none()
            if admin_user:
                if not admin_user.is_admin:
                    admin_user.is_admin = True
                    await session.commit()
                    logger.info(f"✅ User {ADMIN_ID} promoted to admin")
            else:
                new_admin = User(telegram_id=ADMIN_ID, language="en", is_admin=True)
                session.add(new_admin)
                await session.commit()
                logger.info(f"✅ Admin user {ADMIN_ID} created")
    except Exception as e:
        logger.error(f"Failed to ensure ADMIN_ID admin: {e}")
# ==================== ЗАПУСК БОТА ====================

async def main():
    """Запуск бота з усіма покращеннями"""
    logger.info("🚀 Bot starting...")
    logger.info(f"Current user: avtoZAZ")
    logger.info(f"Date: 2025-11-11 20:12:30 UTC")
    
    # Ініціалізація бази даних
    await init_db()
    await ensure_primary_admin()
    logger.info("✅ Database initialized")
    # Гарантируем, что ADMIN_ID есть в БД и отмечен как админ
try:
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == ADMIN_ID))
        admin_user = result.scalar_one_or_none()
        if admin_user:
            if not admin_user.is_admin:
                admin_user.is_admin = True
                await session.commit()
                logger.info(f"✅ User {ADMIN_ID} promoted to admin")
        else:
            new_admin = User(telegram_id=ADMIN_ID, language="en", is_admin=True)
            session.add(new_admin)
            await session.commit()
            logger.info(f"✅ Admin user {ADMIN_ID} created")
except Exception as e:
    logger.error(f"Failed to ensure ADMIN_ID admin: {e}")
    
    # Запуск фонового завдання очистки кешу
    if CACHE_ENABLED:
        asyncio.create_task(cache_cleanup_task())
        logger.info("✅ Cache cleanup task started")
    
    me = await bot.get_me()
    logger.info(f"✅ Started as @{me.username}")
    
    print(f"\n{'='*60}")
    print(f"✅ БОТ ЗАПУЩЕНО ЯК @{me.username}")
    print(f"{'='*60}")
    print(f"📊 База даних: SQLite + SQLAlchemy (асинхронна)")
    print(f"📁 Логи: {LOGS_DIR}/")
    print(f"⭐ Премиум: АКТИВОВАНО (Telegram Stars)")
    print(f"📢 Реклама: АКТИВОВАНА (по мовах користувача)")
    print(f"🔍 Пошук: АКТИВОВАНО")
    print(f"🔒 Безпека: МАКСИМАЛЬНА")
    print(f"   • Rate limiting: {MAX_REQUESTS_PER_MINUTE} req/min")
    print(f"   • Spam protection: АКТИВОВАНО")
    print(f"   • Input validation: АКТИВОВАНО")
    print(f"   • Security logging: {LOGS_DIR}/security.log")
    print(f"\n🆕 НОВІ ФУНКЦІЇ:")
    print(f"   📜 Історія переглядів: ✅")
    print(f"   ⭐ Улюблене: ✅")
    print(f"   ⭐ Рейтинги: {'✅' if RATINGS_ENABLED else '❌'}")
    print(f"   🎬 Рекомендації: {'✅' if RECOMMENDATIONS_ENABLED else '❌'}")
    print(f"   🔥 Топ тижня: {'✅' if RECOMMENDATIONS_ENABLED else '❌'}")
    print(f"   👥 Реферальна система: {'✅' if REFERRAL_ENABLED else '❌'}")
    print(f"   🔔 Підписки на серіали: {'✅' if SERIES_NOTIFICATIONS_ENABLED else '❌'}")
    print(f"   🗂️ Каталог по категоріях: ✅")
    print(f"   📢 Масова розсилка: ✅")
    print(f"   🎫 Промокоди: {'✅' if PROMO_CODES_ENABLED else '❌'}")
    print(f"   📊 Поглиблена аналітика: {'✅' if ANALYTICS_ENABLED else '❌'}")
    print(f"   ⚡ Кешування: {'✅' if CACHE_ENABLED else '❌'}")
    print(f"   ❌ Кнопка скасування: ✅")
    print(f"\n👮 Завантаження відео: ТІЛЬКИ АДМІНИ")
    print(f"📹 Макс розмір відео: 2 ГБ")
    print(f"🌍 Мови: English, Русский, Українська")
    print(f"{'='*60}\n")
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot stopped by user")
        print("\n🛑 Бот зупинено.")