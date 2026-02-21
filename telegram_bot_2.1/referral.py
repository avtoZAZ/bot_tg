# referral.py - Реферальна система з бонусами
# Автор: avtoZAZ
# Дата: 2025-11-11 19:18:05 UTC

from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from sqlalchemy import select, func
from database import async_session_maker, User, Payment
import random
import string

class ReferralSystem:
    """Реферальна система з бонусами"""
    
    # Бонуси за реферала
    REFERRAL_BONUS_DAYS = 3  # Днів безкоштовного преміуму
    REFERRAL_PAYMENT_BONUS_DAYS = 7  # Якщо реферал купив преміум
    
    @staticmethod
    async def generate_referral_code(user_id: int) -> str:
        """Згенерувати унікальний реферальний код"""
        async with async_session_maker() as session:
            result = await session.execute(
                select(User).where(User.telegram_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if user and user.referral_code:
                return user.referral_code
            
            # Генерація нового коду
            while True:
                code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                
                # Перевірка унікальності
                result = await session.execute(
                    select(User).where(User.referral_code == code)
                )
                if not result.scalar_one_or_none():
                    if user:
                        user.referral_code = code
                        await session.commit()
                    return code
    
    @staticmethod
    async def apply_referral(new_user_id: int, referral_code: str) -> Tuple[bool, Optional[str]]:
        """
        Застосувати реферальний код для нового користувача
        Повертає (успіх, повідомлення)
        """
        async with async_session_maker() as session:
            # Знайти реферера
            result = await session.execute(
                select(User).where(User.referral_code == referral_code)
            )
            referrer = result.scalar_one_or_none()
            
            if not referrer:
                return False, "Невірний реферальний код"
            
            if referrer.telegram_id == new_user_id:
                return False, "Не можна використовувати власний код"
            
            # Знайти нового користувача
            result = await session.execute(
                select(User).where(User.telegram_id == new_user_id)
            )
            new_user = result.scalar_one_or_none()
            
            if not new_user:
                return False, "Користувача не знайдено"
            
            if new_user.referred_by:
                return False, "Ви вже використали реферальний код"
            
            # Застосувати реферальний код
            new_user.referred_by = referrer.telegram_id
            
            # Бонус новому користувачу
            bonus_expires = datetime.utcnow() + timedelta(days=ReferralSystem.REFERRAL_BONUS_DAYS)
            new_user.is_premium = True
            new_user.premium_expires = bonus_expires
            new_user.premium_plan = "referral_bonus"
            
            await session.commit()
            
            return True, f"✅ Ви отримали {ReferralSystem.REFERRAL_BONUS_DAYS} днів безкоштовного преміуму!"
    
    @staticmethod
    async def reward_referrer_on_payment(referred_user_id: int):
        """
        Нагородити реферера коли його реферал купив преміум
        """
        async with async_session_maker() as session:
            # Знайти реферала
            result = await session.execute(
                select(User).where(User.telegram_id == referred_user_id)
            )
            referred_user = result.scalar_one_or_none()
            
            if not referred_user or not referred_user.referred_by:
                return
            
            # Знайти реферера
            result = await session.execute(
                select(User).where(User.telegram_id == referred_user.referred_by)
            )
            referrer = result.scalar_one_or_none()
            
            if not referrer:
                return
            
            # Додати бонусні дні до преміуму реферера
            if referrer.is_premium and referrer.premium_expires:
                # Якщо вже є преміум - продовжити
                referrer.premium_expires += timedelta(days=ReferralSystem.REFERRAL_PAYMENT_BONUS_DAYS)
            else:
                # Якщо немає - дати новий
                referrer.is_premium = True
                referrer.premium_expires = datetime.utcnow() + timedelta(days=ReferralSystem.REFERRAL_PAYMENT_BONUS_DAYS)
                referrer.premium_plan = "referral_reward"
            
            await session.commit()
    
    @staticmethod
    async def get_referral_stats(user_id: int) -> dict:
        """Отримати статистику рефералів користувача"""
        async with async_session_maker() as session:
            # Кількість рефералів
            result = await session.execute(
                select(func.count(User.id))
                .where(User.referred_by == user_id)
            )
            total_referrals = result.scalar() or 0
            
            # Кількість рефералів з преміум
            result = await session.execute(
                select(func.count(User.id))
                .where(
                    User.referred_by == user_id,
                    User.is_premium == True
                )
            )
            premium_referrals = result.scalar() or 0
            
            # Загальні бонусні дні
            bonus_days = premium_referrals * ReferralSystem.REFERRAL_PAYMENT_BONUS_DAYS
            
            return {
                "total_referrals": total_referrals,
                "premium_referrals": premium_referrals,
                "bonus_days_earned": bonus_days
            }
    
    @staticmethod
    async def get_referral_leaderboard(limit: int = 10) -> List[Tuple[int, str, int]]:
        """Топ-10 рефереров"""
        async with async_session_maker() as session:
            result = await session.execute(
                select(
                    User.telegram_id,
                    User.first_name,
                    func.count(User.id).label('referrals')
                )
                .join(User, User.telegram_id == User.referred_by, isouter=True)
                .group_by(User.telegram_id, User.first_name)
                .order_by(func.count(User.id).desc())
                .limit(limit)
            )
            
            return [(row.telegram_id, row.first_name or "Unknown", row.referrals) 
                    for row in result.all()]

referral_system = ReferralSystem()