# promo.py - Генерація та валідація промокодів
# Автор: avtoZAZ
# Дата: 2025-11-11 19:18:05 UTC

from datetime import datetime, timedelta
from typing import Optional, Tuple
from sqlalchemy import select, and_
from database import async_session_maker, PromoCode, User
import random
import string

class PromoCodeManager:
    """Менеджер промокодів"""
    
    @staticmethod
    async def generate_promo(
        duration_days: int,
        max_uses: int = 1,
        created_by: int = 0,
        expires_in_days: Optional[int] = None,
        custom_code: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Згенерувати промокод
        
        Повертає (успіх, код або повідомлення помилки)
        """
        async with async_session_maker() as session:
            # Генерація коду
            if custom_code:
                code = custom_code.upper()
                
                # Перевірка унікальності
                result = await session.execute(
                    select(PromoCode).where(PromoCode.code == code)
                )
                if result.scalar_one_or_none():
                    return False, "Промокод вже існує"
            else:
                # Генерація випадкового коду
                while True:
                    code = 'PREMIUM' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                    
                    result = await session.execute(
                        select(PromoCode).where(PromoCode.code == code)
                    )
                    if not result.scalar_one_or_none():
                        break
            
            # Термін дії промокоду
            expires_at = None
            if expires_in_days:
                expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
            
            # Створення промокоду
            promo = PromoCode(
                code=code,
                duration_days=duration_days,
                max_uses=max_uses,
                current_uses=0,
                is_active=True,
                created_by=created_by,
                expires_at=expires_at
            )
            
            session.add(promo)
            await session.commit()
            
            return True, code
    
    @staticmethod
    async def activate_promo(user_id: int, promo_code: str) -> Tuple[bool, str]:
        """
        Активувати промокод для користувача
        
        Повертає (успіх, повідомлення)
        """
        async with async_session_maker() as session:
            # Знайти промокод
            result = await session.execute(
                select(PromoCode).where(PromoCode.code == promo_code.upper())
            )
            promo = result.scalar_one_or_none()
            
            if not promo:
                return False, "❌ Промокод не знайдено"
            
            # Перевірки
            if not promo.is_active:
                return False, "❌ Промокод деактивовано"
            
            if promo.expires_at and promo.expires_at < datetime.utcnow():
                return False, "❌ Термін дії промокоду минув"
            
            if promo.current_uses >= promo.max_uses:
                return False, "❌ Промокод вичерпано"
            
            # Знайти користувача
            result = await session.execute(
                select(User).where(User.telegram_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                return False, "❌ Користувача не знайдено"
            
            # Активувати преміум
            bonus_expires = datetime.utcnow() + timedelta(days=promo.duration_days)
            
            if user.is_premium and user.premium_expires and user.premium_expires > datetime.utcnow():
                # Якщо вже є преміум - продовжити
                user.premium_expires += timedelta(days=promo.duration_days)
                message = f"✅ Промокод активовано!\n⭐ Ваш преміум продовжено на {promo.duration_days} днів\nДіє до: {user.premium_expires.strftime('%Y-%m-%d %H:%M')}"
            else:
                # Якщо немає - дати новий
                user.is_premium = True
                user.premium_expires = bonus_expires
                user.premium_plan = "promo_code"
                message = f"✅ Промокод активовано!\n⭐ Ви отримали {promo.duration_days} днів преміуму\nДіє до: {bonus_expires.strftime('%Y-%m-%d %H:%M')}"
            
            # Оновити використання промокоду
            promo.current_uses += 1
            
            await session.commit()
            
            return True, message
    
    @staticmethod
    async def deactivate_promo(promo_code: str) -> bool:
        """Деактивувати промокод"""
        async with async_session_maker() as session:
            result = await session.execute(
                select(PromoCode).where(PromoCode.code == promo_code.upper())
            )
            promo = result.scalar_one_or_none()
            
            if not promo:
                return False
            
            promo.is_active = False
            await session.commit()
            
            return True
    
    @staticmethod
    async def get_promo_stats(promo_code: str) -> Optional[dict]:
        """Отримати статистику промокоду"""
        async with async_session_maker() as session:
            result = await session.execute(
                select(PromoCode).where(PromoCode.code == promo_code.upper())
            )
            promo = result.scalar_one_or_none()
            
            if not promo:
                return None
            
            return {
                "code": promo.code,
                "duration_days": promo.duration_days,
                "max_uses": promo.max_uses,
                "current_uses": promo.current_uses,
                "remaining_uses": promo.max_uses - promo.current_uses,
                "is_active": promo.is_active,
                "created_at": promo.created_at.strftime('%Y-%m-%d %H:%M'),
                "expires_at": promo.expires_at.strftime('%Y-%m-%d %H:%M') if promo.expires_at else "Безстроково"
            }
    
    @staticmethod
    async def list_active_promos(created_by: Optional[int] = None) -> list:
        """Список активних промокодів"""
        async with async_session_maker() as session:
            query = select(PromoCode).where(PromoCode.is_active == True)
            
            if created_by:
                query = query.where(PromoCode.created_by == created_by)
            
            query = query.order_by(PromoCode.created_at.desc())
            
            result = await session.execute(query)
            promos = result.scalars().all()
            
            return [
                {
                    "code": p.code,
                    "duration_days": p.duration_days,
                    "uses": f"{p.current_uses}/{p.max_uses}",
                    "expires": p.expires_at.strftime('%Y-%m-%d') if p.expires_at else "∞"
                }
                for p in promos
            ]

promo_manager = PromoCodeManager()