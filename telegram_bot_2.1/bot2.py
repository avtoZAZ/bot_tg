# bot.py - Telegram Video Bot Premium Edition
# Автор: avtoZAZ
# Дата: 2025-11-11
# Версія: 3.2.1 (фікси: broadcast_manager, ensure_primary_admin, видача преміуму новим користувачам)

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

from sqlalchemy import select, func, and_, or_
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
from broadcast import BroadcastManager  # ІМПОРТ КЛАСУ ДО ІНІЦІАЛІЗАЦІЇ
from security import (
    rate_limiter, input_validator, spam_protection, security_logger
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

user_video_count = defaultdict(int)
MAX_VIDEO_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB
user_languages_cache: Dict[int, str] = {}

# ==================== ТЕКСТИ (скорочено не редагували) ====================
# (УВАГА: залишаємо повний словник як у вашому поточному файлі)
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
    waiting_for_grant_premium_plan = State()
    waiting_for_promo_code = State()
    waiting_for_broadcast_message = State()
    waiting_for_rating = State()

# ==================== ДОПОМІЖНІ ФУНКЦІЇ ====================

def get_text(user_id: int, key: str, **kwargs) -> str:
    lang = user_languages_cache.get(user_id, "en")
    base = TEXTS.get(lang, TEXTS["en"]).get(key, TEXTS["en"].get(key, ""))
    return base.format(**kwargs) if kwargs else base

def check_rate_limit(user_id: int) -> bool:
    return rate_limiter.check_rate_limit(user_id, MAX_REQUESTS_PER_MINUTE, 60)

async def is_premium_user(user_id: int) -> bool:
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == user_id))
        user = result.scalar_one_or_none()
        if not user or not user.is_premium:
            return False
        if user.premium_expires and user.premium_expires < datetime.utcnow():
            user.is_premium = False
            await session.commit()
            return False
        return True

async def is_admin(user_id: int) -> bool:
    async with async_session_maker() as session:
        result = await session.execute(select(User.is_admin).where(User.telegram_id == user_id))
        return bool(result.scalar_one_or_none())

def log_admin_action(user_id: int, action: str, details: str = ""):
    if LOG_SENSITIVE_DATA:
        admin_logger.info(f"Admin {user_id} - {action} - {details}")
    else:
        admin_logger.info(f"Admin *** - {action}")

async def show_ad(message: Message, user_id: int):
    if not ADS_ENABLED:
        return
    if ADS_ONLY_FREE_USERS and (await is_premium_user(user_id) or await is_admin(user_id)):
        return
    user_video_count[user_id] += 1
    if user_video_count[user_id] % AD_FREQUENCY != 0:
        return

    async with async_session_maker() as session:
        result = await session.execute(select(User.language).where(User.telegram_id == user_id))
        user_lang = result.scalar_one_or_none() or "en"

    ads_for_lang = ADS_LIST.get(user_lang, ADS_LIST.get("en", []))
    if not ads_for_lang:
        return
    ad = random.choice(ads_for_lang)

    try:
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

        def timer_line(sec_left: int) -> str:
            progress = "▓" * (5 - sec_left) + "░" * sec_left
            suffix = "сек..." if user_lang in ("uk", "ru") else "sec..."
            return f"[{progress}] {sec_left} {suffix}"

        caption = f"{ad_text}\n\n{timer_line(5)}"
        if ad.get("photo"):
            ad_msg = await message.answer_photo(ad["photo"], caption=caption, reply_markup=keyboard)
        else:
            ad_msg = await message.answer(caption, reply_markup=keyboard)

        for s in range(4, 0, -1):
            await asyncio.sleep(1)
            new_caption = f"{ad_text}\n\n{timer_line(s)}"
            try:
                if ad.get("photo"):
                    await ad_msg.edit_caption(new_caption, reply_markup=keyboard)
                else:
                    await ad_msg.edit_text(new_caption, reply_markup=keyboard)
            except:
                pass
        await asyncio.sleep(1)

        if TRACK_AD_CLICKS:
            ad_id = f"{user_lang}_{ads_for_lang.index(ad)}"
            await log_ad_click(user_id, ad_id, clicked=False)
    except Exception as e:
        logger.error(f"Ad error: {e}")

async def check_cancel(message: Message, state: FSMContext, user_id: int) -> bool:
    if message.text.strip() in {"❌ Скасувати", "❌ Отменить", "❌ Cancel", "/cancel", "/back"}:
        await state.clear()
        lang = user_languages_cache.get(user_id, "uk")
        if lang == "uk":
            txt = "✅ Дію скасовано. Повернення до головного меню..."
        elif lang == "ru":
            txt = "✅ Действие отменено. Возвращение в главное меню..."
        else:
            txt = "✅ Action cancelled. Returning to main menu..."
        kb = get_admin_keyboard(user_id) if await is_admin(user_id) else get_user_keyboard(user_id)
        await message.answer(txt, reply_markup=kb)
        await state.set_state(UserState.waiting_for_code)
        return True
    return False

# ==================== КЛАВІАТУРИ ====================

def get_language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="English 🇬🇧", callback_data="lang_en")],
        [InlineKeyboardButton(text="Русский 🇷🇺", callback_data="lang_ru")],
        [InlineKeyboardButton(text="Українська 🇺🇦", callback_data="lang_uk")]
    ])

def get_user_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(get_text(user_id, "search_button")), KeyboardButton(get_text(user_id, "browse_button"))],
        [KeyboardButton(get_text(user_id, "history_button")), KeyboardButton(get_text(user_id, "favorites_button"))],
        [KeyboardButton(get_text(user_id, "top_week_button")), KeyboardButton(get_text(user_id, "referral_button"))],
        [KeyboardButton(get_text(user_id, "premium_button")), KeyboardButton(get_text(user_id, "my_subscription_button"))],
    ])

def get_admin_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(get_text(user_id, "upload_video_button"))],
        [KeyboardButton(get_text(user_id, "stats_button")), KeyboardButton(get_text(user_id, "analytics_button"))],
        [KeyboardButton(get_text(user_id, "delete_video_button")), KeyboardButton(get_text(user_id, "edit_video_button"))],
        [KeyboardButton(get_text(user_id, "list_videos_button")), KeyboardButton(get_text(user_id, "list_series_button"))],
        [KeyboardButton(get_text(user_id, "broadcast_button")), KeyboardButton(get_text(user_id, "promo_button"))],
        [KeyboardButton(get_text(user_id, "list_admins_button")), KeyboardButton(get_text(user_id, "add_admin_button"))],
        [KeyboardButton("⭐ Видати преміум"), KeyboardButton(get_text(user_id, "export_db_button"))],
    ])

def get_premium_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for pid, info in PREMIUM_PLANS.items():
        rows.append([InlineKeyboardButton(
            text=f"{info['emoji']} {info['name_en']} - {info['price_stars']} ⭐",
            callback_data=f"buy_premium_{pid}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_browse_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(get_text(user_id, "category_movies"), callback_data="browse_movies")],
        [InlineKeyboardButton(get_text(user_id, "category_series"), callback_data="browse_series")],
        [InlineKeyboardButton(get_text(user_id, "category_anime"), callback_data="browse_anime")],
    ])

def get_rating_keyboard(video_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton("⭐", callback_data=f"rate_{video_code}_1"),
            InlineKeyboardButton("⭐⭐", callback_data=f"rate_{video_code}_2"),
            InlineKeyboardButton("⭐⭐⭐", callback_data=f"rate_{video_code}_3"),
            InlineKeyboardButton("⭐⭐⭐⭐", callback_data=f"rate_{video_code}_4"),
            InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data=f"rate_{video_code}_5"),
        ]
    ])

async def get_video_action_keyboard(user_id: int, code: str, series_name: str = None) -> InlineKeyboardMarkup:
    buttons = []
    fav = await is_favorite(user_id, code)
    buttons.append([InlineKeyboardButton(
        get_text(user_id, "remove_from_favorites") if fav else get_text(user_id, "add_to_favorites"),
        callback_data=f"fav_{code}"
    )])
    if RATINGS_ENABLED:
        buttons.append([InlineKeyboardButton(get_text(user_id, "rate_video"), callback_data=f"rate_{code}_prompt")])
    if SERIES_NOTIFICATIONS_ENABLED and series_name:
        buttons.append([InlineKeyboardButton(get_text(user_id, "subscribe_to_series"), callback_data=f"sub_{series_name}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_broadcast_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(get_text(user_id, "broadcast_all"), callback_data="broadcast_all")],
        [InlineKeyboardButton(get_text(user_id, "broadcast_free"), callback_data="broadcast_free")],
        [InlineKeyboardButton(get_text(user_id, "broadcast_premium"), callback_data="broadcast_premium")],
        [InlineKeyboardButton(get_text(user_id, "broadcast_language"), callback_data="broadcast_lang")],
    ])

def get_promo_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(get_text(user_id, "create_promo_button"), callback_data="promo_create")],
        [InlineKeyboardButton(get_text(user_id, "list_promos_button"), callback_data="promo_list")],
        [InlineKeyboardButton(get_text(user_id, "promo_activate"), callback_data="promo_activate")],
    ])

def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton("❌ Скасувати")]])

# ==================== MIDDLEWARE ====================

class BanCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
                       event: TelegramObject, data: Dict[str, Any]) -> Any:
        uid = getattr(event.from_user, "id", None) if hasattr(event, "from_user") and event.from_user else None
        if uid:
            if uid in BLOCKED_USERS or rate_limiter.is_blocked(uid):
                return
        return await handler(event, data)

# ==================== ІНІЦІАЛІЗАЦІЯ ====================

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=storage)

dp.message.middleware(BanCheckMiddleware())
dp.callback_query.middleware(BanCheckMiddleware())

broadcast_manager = BroadcastManager(bot)
import broadcast as _broadcast
_broadcast.broadcast_manager = broadcast_manager
logger.info("✅ BroadcastManager initialized")

# ==================== КОМАНДА /CANCEL ====================

@dp.message(Command("cancel"))
async def handle_cancel_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current = await state.get_state()
    if current:
        await state.clear()
        lang = user_languages_cache.get(user_id, "uk")
        msg = {
            "uk": "✅ Поточну дію скасовано.",
            "ru": "✅ Текущее действие отменено.",
        }.get(lang, "✅ Current action cancelled.")
        kb = get_admin_keyboard(user_id) if await is_admin(user_id) else get_user_keyboard(user_id)
        await message.answer(msg, reply_markup=kb)
        await state.set_state(UserState.waiting_for_code)
    else:
        lang = user_languages_cache.get(user_id, "uk")
        msg = {
            "uk": "ℹ️ Немає активних дій для скасування.",
            "ru": "ℹ️ Нет активных действий для отмены.",
        }.get(lang, "ℹ️ No active actions to cancel.")
        await message.answer(msg)

# ==================== /START ====================

@dp.message(Command("start"))
async def handle_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in BLOCKED_USERS:
        security_logger.log_suspicious_activity(user_id, "BLOCKED_USER_START_ATTEMPT")
        return
    await state.clear()
    user = await get_or_create_user(user_id, message.from_user.username, message.from_user.first_name)
    user_languages_cache[user_id] = user.language
    if REFERRAL_ENABLED and message.text and len(message.text.split()) > 1 and not user.referred_by:
        code = message.text.split()[1]
        ok, referral_msg = await referral_system.apply_referral(user_id, code)
        if ok:
            await message.answer(referral_msg)

    if not user.language or user.language == "en":
        await message.answer("Please select your language: / Выберите язык: / Оберіть мову:", reply_markup=get_language_keyboard())
    else:
        kb = get_admin_keyboard(user_id) if user.is_admin else get_user_keyboard(user_id)
        greet = get_text(user_id, "welcome_message")
        if user.is_admin:
            greet += "\n\n" + get_text(user_id, "admin_menu_info")
        await message.answer(greet, reply_markup=kb)
        await state.set_state(UserState.waiting_for_code)

@dp.callback_query(F.data.startswith("lang_"))
async def handle_language_selection(callback: CallbackQuery, state: FSMContext):
    lang_code = callback.data.split("_")[1]
    user_id = callback.from_user.id
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.language = lang_code
            await session.commit()
    user_languages_cache[user_id] = lang_code
    await callback.message.delete()
    kb = get_admin_keyboard(user_id) if await is_admin(user_id) else get_user_keyboard(user_id)
    greet = get_text(user_id, "welcome_message")
    if await is_admin(user_id):
        greet += "\n\n" + get_text(user_id, "admin_menu_info")
    await callback.message.answer(greet, reply_markup=kb)
    await state.set_state(UserState.waiting_for_code)

# ==================== ОСНОВНИЙ ОБРОБНИК ТЕКСТУ ====================

@dp.message(UserState.waiting_for_code, F.text)
async def handle_text_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_rate_limit(user_id):
        await message.answer(get_text(user_id, "rate_limit"))
        return
    if spam_protection.is_spam(user_id, message.text):
        security_logger.log_suspicious_activity(user_id, "SPAM_DETECTED", message.text[:50])
        rate_limiter.block_user(user_id, BLOCK_TIME)
        return

    text = input_validator.sanitize_html(message.text.strip())
    if text in {"◀️ Назад", "◀️ Back", "/back"}:
        await state.clear()
        kb = get_admin_keyboard(user_id) if await is_admin(user_id) else get_user_keyboard(user_id)
        await message.answer(get_text(user_id, "action_cancelled"), reply_markup=kb)
        await state.set_state(UserState.waiting_for_code)
        return

    is_admin_user = await is_admin(user_id)
    if is_admin_user:
        if text in {"📹 Завантажити відео", "📹 Загрузить видео", "📹 Upload Video"}:
            await message.answer(get_text(user_id, "send_video_prompt"), reply_markup=get_cancel_keyboard())
            await state.set_state(UserState.waiting_for_video)
            return
        if text in {"📊 Статистика", "📊 Statistics"}:
            await show_statistics(message, user_id); return
        if text in {"📊 Аналітика", "📊 Analytics"}:
            await show_analytics(message, user_id); return
        if text in {"🗑️ Видалити відео", "🗑️ Удалить видео", "🗑️ Delete Video"}:
            await message.answer("🗑️ Введіть код відео для видалення:", reply_markup=get_cancel_keyboard())
            await state.set_state(UserState.waiting_for_delete_code); return
        if text in {"✏️ Редагувати", "✏️ Редактировать", "✏️ Edit Metadata"}:
            await message.answer("✏️ Введіть код відео для редагування:", reply_markup=get_cancel_keyboard())
            await state.set_state(UserState.waiting_for_edit_code); return
        if text in {"📝 Всі відео", "📝 Все видео", "📝 All Videos"}:
            await show_all_videos(message, user_id, 1); return
        if text in {"📺 Список серіалів", "📺 Список сериалов", "📺 Series List"}:
            await show_all_series(message, user_id); return
        if text in {"📢 Розсилка", "📢 Рассылка", "📢 Broadcast"}:
            await message.answer("📢 Оберіть тип розсилки:", reply_markup=get_broadcast_keyboard(user_id)); return
        if text in {"🎫 Промокоди", "🎫 Promo Codes"}:
            await message.answer("🎫 Промокоди:", reply_markup=get_promo_keyboard(user_id)); return
        if text in {"👤 Додати адміна", "👤 Добавить админа", "👤 Add Admin"}:
            await message.answer("👤 Введіть Telegram ID користувача:", reply_markup=get_cancel_keyboard())
            await state.set_state(UserState.waiting_for_admin_id); return
        if text in {"👥 Список адмінів", "👥 Список админов", "👥 Admins List"}:
            await handle_list_admins(message); return
        if text in {"⭐ Видати преміум", "⭐ Выдать премиум", "⭐ Grant Premium"}:
            await message.answer("⭐ Введіть Telegram ID користувача:", reply_markup=get_cancel_keyboard())
            await state.set_state(UserState.waiting_for_grant_premium_user); return
        if text in {"📤 Експорт БД", "📤 Экспорт БД", "📤 Export DB"}:
            await handle_export_db(message); return

    # Користувацькі кнопки
    if text in {"🔍 Пошук", "🔍 Поиск", "🔍 Search"}:
        await message.answer(get_text(user_id, "enter_search_query"), reply_markup=get_cancel_keyboard())
        await state.set_state(UserState.waiting_for_search); return
    if text in {"📜 Історія", "📜 История", "📜 History"}:
        await show_user_history(message, user_id); return
    if text in {"⭐ Улюблене", "⭐ Избранное", "⭐ Favorites"}:
        await show_user_favorites(message, user_id); return
    if text in {"🗂️ Каталог", "🗂️ Browse"}:
        await message.answer(get_text(user_id, "browse_categories"), reply_markup=get_browse_keyboard(user_id)); return
    if text in {"🔥 Топ тижня", "🔥 Топ недели", "🔥 Top Week"}:
        await show_top_weekly(message, user_id); return
    if text in {"👥 Запросити друзів", "👥 Пригласить друзей", "👥 Invite Friends"}:
        await show_referral_info(message, user_id); return
    if text in {"⭐ Преміум", "⭐ Премиум", "⭐ Premium"}:
        await show_premium_plans(message, user_id); return
    if text in {"👤 Моя підписка", "👤 Моя подписка", "👤 My Subscription"}:
        await show_subscription_status(message, user_id); return

    # Відео код
    if not input_validator.validate_code(text, max_length=15):
        security_logger.log_suspicious_activity(user_id, "INVALID_CODE_FORMAT", text)
        await message.answer(get_text(user_id, "code_not_found"))
        return

    if CACHE_ENABLED:
        cached = await video_cache.get(f"video_{text}")
        if cached:
            await send_video_to_user(message, user_id, cached)
            return

    async with async_session_maker() as session:
        result = await session.execute(select(Video).where(Video.code == text))
        video = result.scalar_one_or_none()
        if not video:
            await message.answer(get_text(user_id, "code_not_found"))
            return
        video.views_count += 1
        await session.commit()
        if CACHE_ENABLED:
            await video_cache.set(f"video_{text}", video)
    await send_video_to_user(message, user_id, video)

# ==================== (Далі: решта ваших обробників БЕЗ змін або з мінімальними поправками) ====================
# Через обмеження обсягу тут показані лише ключові фрагменти.
# Вище ми виправили критичні місця. Інші handlers (upload, edit, delete, rating, broadcast, promo) залишаються як у вашій останній версії.

# Вставте тут весь ваш існуючий код обробників: завантаження відео, callbacks, пошук, промокоди, видача преміуму (уже оновлена), експорт БД тощо
# -------------- START OF SHORTENED SECTION --------------
# (Скопіюйте з вашого поточного робочого файлу від @dp.message(UserState.waiting_for_video, F.video) до ensure_primary_admin() без змін – ми їх не правили, окрім преміум обробника)
# -------------- END OF SHORTENED SECTION --------------
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
            reply_markup=get_cancel_keyboard()
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
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserState.waiting_for_series_name)
    else:
        await state.update_data(is_series=False)
        await callback.message.delete()
        await callback.message.answer(
            get_text(callback.from_user.id, "enter_title"),
            reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
            reply_markup=get_cancel_keyboard()
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
            reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
            reply_markup=get_cancel_keyboard()
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
        reply_markup=get_cancel_keyboard()
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
        await message.answer("⭐ Преміум статус:\n\n1 - Так\n0 - Ні", reply_markup=get_cancel_keyboard())
    else:
        await message.answer(f"✏️ Введіть нове значення для {field}:", reply_markup=get_cancel_keyboard())
    
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
        reply_markup=get_cancel_keyboard()
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


async def send_video_to_user(message: Message, user_id: int, video: Video):
    if video.is_premium and not (await is_premium_user(user_id) or await is_admin(user_id)):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(get_text(user_id, "subscribe_button"), callback_data="show_premium_plans")]
        ])
        await message.answer(get_text(user_id, "premium_required"), reply_markup=kb)
        return

    await add_to_history(user_id, video.code)

    info = ""
    if video.title:
        if video.is_series:
            info = get_text(user_id, "episode_info", title=video.title, season=video.season or "?", episode=video.episode or "?")
        else:
            info = get_text(user_id, "movie_info", title=video.title)
    if video.year:
        info += get_text(user_id, "year_info", year=video.year)
    if video.genre:
        info += get_text(user_id, "genre_info", genre=video.genre)
    if video.description:
        info += get_text(user_id, "description_info", description=video.description)
    info += get_text(user_id, "views_info", views=video.views_count)
    if RATINGS_ENABLED and video.ratings_count > 0:
        info += get_text(user_id, "rating_info", rating=round(video.avg_rating, 1), count=video.ratings_count)
    if video.is_premium:
        info += get_text(user_id, "premium_badge")

    if info:
        await message.answer(info)
    if video.poster_file_id:
        try:
            await bot.send_photo(message.chat.id, video.poster_file_id)
        except:
            pass

    await show_ad(message, user_id)

    try:
        await bot.send_video(message.chat.id, video.file_id)
    except Exception as e:
        logger.error(f"Video send error {video.code}: {e}")
        await message.answer("❌ Помилка при відправці відео.")
        return

    kb = await get_video_action_keyboard(user_id, video.code, video.series_name)
    await message.answer("━━━━━━━━━━━━━━", reply_markup=kb)

    if RECOMMENDATIONS_ENABLED:
        await show_recommendations(message, user_id, video.code)

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
    logger.info("🚀 Bot starting...")
    await init_db()
    await ensure_primary_admin()
    logger.info("✅ Database initialized")

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
    print(f"⭐ Премиум: {'АКТИВОВАНО' if PROMO_CODES_ENABLED else '—'} (Telegram Stars)")
    print(f"📢 Реклама: {'АКТИВОВАНА' if ADS_ENABLED else 'ВИМКНЕНО'}")
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