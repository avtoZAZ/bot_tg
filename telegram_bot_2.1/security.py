# security.py - Модуль безпеки
# Автор: avtoZAZ
# Дата: 2025-11-11 19:48:58 UTC

import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Set, List
import re
import html

logger = logging.getLogger(__name__)

# ==================== RATE LIMITER ====================

class RateLimiter:
    """Rate limiting для захисту від флуду"""
    
    def __init__(self):
        self.requests: Dict[int, List[float]] = defaultdict(list)
        self.blocked_users: Dict[int, float] = {}
    
    def check_rate_limit(self, user_id: int, max_requests: int, time_window: int) -> bool:
        """
        Перевірити чи не перевищено ліміт запитів
        
        Args:
            user_id: ID користувача
            max_requests: Максимум запитів
            time_window: Вікно часу в секундах
        
        Returns:
            True якщо дозволено, False якщо заблоковано
        """
        current_time = time.time()
        
        # Очистити старі запити
        self.requests[user_id] = [
            req_time for req_time in self.requests[user_id]
            if current_time - req_time < time_window
        ]
        
        # Перевірити ліміт
        if len(self.requests[user_id]) >= max_requests:
            return False
        
        # Додати новий запит
        self.requests[user_id].append(current_time)
        return True
    
    def block_user(self, user_id: int, duration: int):
        """Заблокувати користувача на певний час"""
        self.blocked_users[user_id] = time.time() + duration
        logger.warning(f"User {user_id} blocked for {duration} seconds")
    
    def is_blocked(self, user_id: int) -> bool:
        """Перевірити чи користувач заблокований"""
        if user_id not in self.blocked_users:
            return False
        
        if time.time() > self.blocked_users[user_id]:
            # Блокування закінчилося
            del self.blocked_users[user_id]
            return False
        
        return True

# ==================== INPUT VALIDATOR ====================

class InputValidator:
    """Валідація вводу користувачів"""
    
    @staticmethod
    def sanitize_html(text: str) -> str:
        """Очистити HTML від небезпечних тегів"""
        # Екранувати HTML
        return html.escape(text)
    
    @staticmethod
    def validate_code(code: str, max_length: int = 15) -> bool:
        """Валідація коду відео"""
        if not code or len(code) > max_length:
            return False
        
        # Дозволені тільки букви, цифри, дефіси
        return bool(re.match(r'^[a-zA-Z0-9_-]+$', code))
    
    @staticmethod
    def validate_url(url: str, allowed_domains: List[str]) -> bool:
        """Валідація URL"""
        if not url:
            return False
        
        # Перевірити чи URL починається з http/https
        if not url.startswith(('http://', 'https://')):
            return False
        
        # Перевірити домен
        for domain in allowed_domains:
            if domain in url:
                return True
        
        return False
    
    @staticmethod
    def validate_telegram_id(text: str) -> int:
        """Валідація Telegram ID"""
        try:
            user_id = int(text.strip())
            if user_id > 0:
                return user_id
            return None
        except ValueError:
            return None

# ==================== SPAM PROTECTION ====================

class SpamProtection:
    """Захист від спаму"""
    
    def __init__(self):
        self.message_history: Dict[int, List[tuple]] = defaultdict(list)
        self.spam_threshold = 5  # Однакових повідомлень за хвилину
        self.time_window = 60  # секунд
    
    def is_spam(self, user_id: int, message: str = None, media_type: str = None) -> bool:
        """
        Перевірити чи повідомлення є спамом
        
        Args:
            user_id: ID користувача
            message: Текст повідомлення
            media_type: Тип медіа (sticker, gif, photo, etc)
        
        Returns:
            True якщо спам, False якщо ні
        """
        current_time = time.time()
        
        # Очистити стару історію
        self.message_history[user_id] = [
            (msg, msg_time) for msg, msg_time in self.message_history[user_id]
            if current_time - msg_time < self.time_window
        ]
        
        # Ключ для перевірки (текст або тип медіа)
        check_key = message if message else f"media_{media_type}"
        
        # Підрахувати однакові повідомлення
        same_messages = [
            msg for msg, msg_time in self.message_history[user_id]
            if msg == check_key
        ]
        
        # Додати нове повідомлення
        self.message_history[user_id].append((check_key, current_time))
        
        # Перевірити чи спам
        if len(same_messages) >= self.spam_threshold:
            logger.warning(f"Spam detected from user {user_id}: {check_key}")
            return True
        
        return False
    
    def clear_history(self, user_id: int):
        """Очистити історію користувача"""
        if user_id in self.message_history:
            del self.message_history[user_id]

# ==================== SECURITY LOGGER ====================

class SecurityLogger:
    """Логування безпеки"""
    
    def __init__(self):
        self.security_log = logging.getLogger('security')
        handler = logging.FileHandler('logs/security.log', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.security_log.addHandler(handler)
        self.security_log.setLevel(logging.WARNING)
    
    def log_suspicious_activity(self, user_id: int, event: str, details: str = ""):
        """Логувати підозрілу активність"""
        self.security_log.warning(f"User {user_id} - {event} - {details}")
    
    def log_security_event(self, event: str, details: str = ""):
        """Логувати безпекову подію"""
        self.security_log.warning(f"{event} - {details}")
    
    def log_admin_action(self, admin_id: int, action: str, details: str = ""):
        """Логувати дії адміністратора"""
        self.security_log.info(f"Admin {admin_id} - {action} - {details}")

# ==================== ГЛОБАЛЬНІ ЕКЗЕМПЛЯРИ ====================

rate_limiter = RateLimiter()
input_validator = InputValidator()
spam_protection = SpamProtection()
security_logger = SecurityLogger()