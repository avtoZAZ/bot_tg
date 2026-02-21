# broadcast.py - Система масової розсилки
# Автор: avtoZAZ
# Дата: 2025-11-11 20:17:15 UTC

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from sqlalchemy import select, and_
from database import async_session_maker, User, SeriesSubscription

logger = logging.getLogger(__name__)

class BroadcastManager:
    """Менеджер масової розсилки"""
    
    def __init__(self, bot):
        self.bot = bot
        self.delay = 0.05  # Затримка між повідомленнями (секунди)
        self.chunk_size = 20  # Розмір чанку
        self.chunk_delay = 1.0  # Затримка між чанками
    
    async def send_broadcast(
        self,
        text: str,
        filter_premium: Optional[bool] = None,
        filter_language: Optional[str] = None,
        photo: Optional[str] = None,
        reply_markup: Optional[Any] = None
    ) -> Dict[str, int]:
        """
        Відправити розсилку користувачам
        
        Args:
            text: Текст повідомлення
            filter_premium: True - тільки premium, False - тільки free, None - всім
            filter_language: Фільтр по мові (en/ru/uk)
            photo: File ID фото (опціонально)
            reply_markup: Клавіатура (опціонально)
        
        Returns:
            Статистика: {success, failed, blocked, total}
        """
        
        stats = {
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "total": 0
        }
        
        # Отримати користувачів
        async with async_session_maker() as session:
            query = select(User.telegram_id)
            
            # Фільтри
            conditions = []
            
            if filter_premium is not None:
                conditions.append(User.is_premium == filter_premium)
            
            if filter_language:
                conditions.append(User.language == filter_language)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            result = await session.execute(query)
            user_ids = [row[0] for row in result.all()]
        
        stats["total"] = len(user_ids)
        
        logger.info(f"Starting broadcast to {stats['total']} users")
        
        # Розсилка по чанках
        for i in range(0, len(user_ids), self.chunk_size):
            chunk = user_ids[i:i + self.chunk_size]
            
            for user_id in chunk:
                try:
                    if photo:
                        await self.bot.send_photo(
                            chat_id=user_id,
                            photo=photo,
                            caption=text,
                            reply_markup=reply_markup
                        )
                    else:
                        await self.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            reply_markup=reply_markup
                        )
                    
                    stats["success"] += 1
                    await asyncio.sleep(self.delay)
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    if "bot was blocked" in error_msg.lower():
                        stats["blocked"] += 1
                    else:
                        stats["failed"] += 1
                    
                    logger.warning(f"Failed to send to {user_id}: {e}")
            
            # Пауза між чанками
            if i + self.chunk_size < len(user_ids):
                await asyncio.sleep(self.chunk_delay)
        
        logger.info(f"Broadcast completed: {stats}")
        return stats
    
    async def send_to_series_subscribers(
        self,
        series_name: str,
        text: str,
        photo: Optional[str] = None,
        reply_markup: Optional[Any] = None
    ) -> Dict[str, int]:
        """
        Відправити сповіщення підписникам серіалу
        
        Args:
            series_name: Назва серіалу
            text: Текст повідомлення
            photo: File ID фото (опціонально)
            reply_markup: Клавіатура (опціонально)
        
        Returns:
            Статистика: {success, failed, blocked, total}
        """
        
        stats = {
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "total": 0
        }
        
        # Отримати підписників
        async with async_session_maker() as session:
            result = await session.execute(
                select(SeriesSubscription.user_id)
                .where(SeriesSubscription.series_name == series_name)
            )
            user_ids = [row[0] for row in result.all()]
        
        stats["total"] = len(user_ids)
        
        logger.info(f"Sending series notification to {stats['total']} subscribers of {series_name}")
        
        # Розсилка
        for user_id in user_ids:
            try:
                if photo:
                    await self.bot.send_photo(
                        chat_id=user_id,
                        photo=photo,
                        caption=text,
                        reply_markup=reply_markup
                    )
                else:
                    await self.bot.send_message(
                        chat_id=user_id,
                        text=text,
                        reply_markup=reply_markup
                    )
                
                stats["success"] += 1
                await asyncio.sleep(self.delay)
                
            except Exception as e:
                error_msg = str(e)
                
                if "bot was blocked" in error_msg.lower():
                    stats["blocked"] += 1
                else:
                    stats["failed"] += 1
                
                logger.warning(f"Failed to send series notification to {user_id}: {e}")
        
        logger.info(f"Series notification completed: {stats}")
        return stats
    
    async def send_to_user(
        self,
        user_id: int,
        text: str,
        photo: Optional[str] = None,
        reply_markup: Optional[Any] = None
    ) -> bool:
        """
        Відправити повідомлення одному користувачу
        
        Args:
            user_id: ID користувача
            text: Текст повідомлення
            photo: File ID фото (опціонально)
            reply_markup: Клавіатура (опціонально)
        
        Returns:
            True якщо успішно, False якщо помилка
        """
        
        try:
            if photo:
                await self.bot.send_photo(
                    chat_id=user_id,
                    photo=photo,
                    caption=text,
                    reply_markup=reply_markup
                )
            else:
                await self.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=reply_markup
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message to {user_id}: {e}")
            return False


# Глобальний екземпляр (буде ініціалізований в bot.py)
broadcast_manager = None