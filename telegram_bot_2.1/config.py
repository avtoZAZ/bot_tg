# config.py - Конфігурація бота
# Автор: avtoZAZ
# Дата: 2025-11-11 20:16:00 UTC

import os
from dotenv import load_dotenv

# Завантажити змінні з .env
load_dotenv()

# ==================== ОСНОВНІ НАЛАШТУВАННЯ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "1309506590"))

# ==================== БАЗА ДАНИХ ====================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///bot_data.db")

# ==================== БЕЗПЕКА ====================
MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "30"))
MAX_FAILED_ATTEMPTS = int(os.getenv("MAX_FAILED_ATTEMPTS", "5"))
BLOCK_TIME = int(os.getenv("BLOCK_TIME", "3600"))
BLOCKED_USERS = [int(x) for x in os.getenv("BLOCKED_USERS", "").split(",") if x]
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS", "t.me,telegram.me,telegra.ph").split(",")
LOG_SENSITIVE_DATA = os.getenv("LOG_SENSITIVE_DATA", "False").lower() == "true"

ADMIN_WHITELIST_ENABLED = os.getenv("ADMIN_WHITELIST_ENABLED", "False").lower() == "true"
ADMIN_WHITELIST = [int(x) for x in os.getenv("ADMIN_WHITELIST", "").split(",") if x]
VALIDATE_AD_URLS = os.getenv("VALIDATE_AD_URLS", "True").lower() == "true"

# ==================== РЕКЛАМА ====================
ADS_ENABLED = os.getenv("ADS_ENABLED", "True").lower() == "true"
AD_FREQUENCY = int(os.getenv("AD_FREQUENCY", "3"))
ADS_ONLY_FREE_USERS = os.getenv("ADS_ONLY_FREE_USERS", "True").lower() == "true"

ADS_LIST = {
    "en": [
        {
            "text": "🎬 <b>Premium Subscription</b>\n\nGet unlimited access to all premium content!",
            "button_text": "Subscribe Now ⭐",
            "button_url": None,
            "photo": None
        }
    ],
    "ru": [
        {
            "text": "🎬 <b>Премиум подписка</b>\n\nПолучите безлимитный доступ ко всему премиум контенту!",
            "button_text": "Подписаться ⭐",
            "button_url": None,
            "photo": None
        }
    ],
    "uk": [
        {
            "text": "🎬 <b>Преміум підписка</b>\n\nОтримайте безлімітний доступ до всього преміум контенту!",
            "button_text": "Підписатися ⭐",
            "button_url": None,
            "photo": None
        }
    ]
}

# ==================== КЕШУВАННЯ ====================
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "True").lower() == "true"
VIDEO_CACHE_SIZE = int(os.getenv("VIDEO_CACHE_SIZE", "200"))
VIDEO_CACHE_TTL = int(os.getenv("VIDEO_CACHE_TTL", "300"))
RECOMMENDATIONS_CACHE_SIZE = int(os.getenv("RECOMMENDATIONS_CACHE_SIZE", "100"))
RECOMMENDATIONS_CACHE_TTL = int(os.getenv("RECOMMENDATIONS_CACHE_TTL", "600"))

# ==================== РЕКОМЕНДАЦІЇ ====================
RECOMMENDATIONS_ENABLED = os.getenv("RECOMMENDATIONS_ENABLED", "True").lower() == "true"
RECOMMENDATIONS_LIMIT = int(os.getenv("RECOMMENDATIONS_LIMIT", "5"))
TOP_WEEKLY_LIMIT = int(os.getenv("TOP_WEEKLY_LIMIT", "5"))

# ==================== РЕФЕРАЛЬНА СИСТЕМА ====================
REFERRAL_ENABLED = os.getenv("REFERRAL_ENABLED", "True").lower() == "true"
REFERRAL_BONUS_DAYS = int(os.getenv("REFERRAL_BONUS_DAYS", "3"))
REFERRAL_PAYMENT_BONUS_DAYS = int(os.getenv("REFERRAL_PAYMENT_BONUS_DAYS", "7"))

# ==================== РОЗСИЛКА ====================
BROADCAST_DELAY = float(os.getenv("BROADCAST_DELAY", "0.05"))
BROADCAST_CHUNK_SIZE = int(os.getenv("BROADCAST_CHUNK_SIZE", "20"))
BROADCAST_CHUNK_DELAY = float(os.getenv("BROADCAST_CHUNK_DELAY", "1.0"))

# ==================== ПРОМОКОДИ ====================
PROMO_CODES_ENABLED = os.getenv("PROMO_CODES_ENABLED", "True").lower() == "true"

# ==================== ПІДПИСКИ НА СЕРІАЛИ ====================
SERIES_NOTIFICATIONS_ENABLED = os.getenv("SERIES_NOTIFICATIONS_ENABLED", "True").lower() == "true"

# ==================== РЕЙТИНГИ ====================
RATINGS_ENABLED = os.getenv("RATINGS_ENABLED", "True").lower() == "true"
MIN_RATINGS_FOR_DISPLAY = int(os.getenv("MIN_RATINGS_FOR_DISPLAY", "3"))

# ==================== АНАЛІТИКА ====================
ANALYTICS_ENABLED = os.getenv("ANALYTICS_ENABLED", "True").lower() == "true"
SAVE_SEARCH_QUERIES = os.getenv("SAVE_SEARCH_QUERIES", "True").lower() == "true"
TRACK_AD_CLICKS = os.getenv("TRACK_AD_CLICKS", "True").lower() == "true"

# ==================== ПРЕМІУМ ТАРИФИ ====================

PREMIUM_PLANS = {
    "weekly": {
        "name_en": "Weekly Plan",
        "name_ru": "Недельный",
        "name_uk": "Тижневий",
        "emoji": "📅",
        "duration_days": 7,
        "price_stars": 10
    },
    "monthly": {
        "name_en": "Monthly Plan",
        "name_ru": "Месячный",
        "name_uk": "Місячний",
        "emoji": "📆",
        "duration_days": 30,
        "price_stars": 30
    },
    "yearly": {
        "name_en": "Yearly Plan",
        "name_ru": "Годовой",
        "name_uk": "Річний",
        "emoji": "🎉",
        "duration_days": 365,
        "price_stars": 250
    }
}

# ==================== ЩОДЕННІ РЕКОМЕНДАЦІЇ ====================
DAILY_RECOMMENDATIONS_ENABLED = os.getenv("DAILY_RECOMMENDATIONS_ENABLED", "True").lower() == "true"
DAILY_RECOMMENDATIONS_HOUR = int(os.getenv("DAILY_RECOMMENDATIONS_HOUR", "12"))

# ==================== AI ПОШУК ====================
AI_SEARCH_ENABLED = os.getenv("AI_SEARCH_ENABLED", "True").lower() == "true"
AI_SEARCH_API_KEY = os.getenv("AI_SEARCH_API_KEY", "")  # OpenAI API key (optional)

# ==================== ЛОГИ ====================

LOGS_DIR = "logs"

# Створити папку логів якщо не існує
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)