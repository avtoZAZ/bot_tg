# cache.py - LRU кеш для популярного контенту
# Автор: avtoZAZ
# Дата: 2025-11-11 19:15:42 UTC

from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Optional
import asyncio

class TTLCache:
    """Кеш з обмеженням часу життя (TTL) та розміру (LRU)"""
    
    def __init__(self, max_size: int = 100, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: dict = {}
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Отримати значення з кешу"""
        async with self._lock:
            if key not in self.cache:
                return None
            
            # Перевірити TTL
            if self._is_expired(key):
                self._delete(key)
                return None
            
            # LRU: перемістити в кінець (найбільш використовуваний)
            self.cache.move_to_end(key)
            return self.cache[key]
    
    async def set(self, key: str, value: Any):
        """Зберегти значення в кеші"""
        async with self._lock:
            # Якщо ключ вже існує, видалити старе значення
            if key in self.cache:
                self._delete(key)
            
            # Якщо кеш повний, видалити найстаріше
            if len(self.cache) >= self.max_size:
                oldest_key = next(iter(self.cache))
                self._delete(oldest_key)
            
            # Додати нове значення
            self.cache[key] = value
            self.timestamps[key] = datetime.utcnow()
    
    async def delete(self, key: str):
        """Видалити значення з кешу"""
        async with self._lock:
            self._delete(key)
    
    async def clear(self):
        """Очистити весь кеш"""
        async with self._lock:
            self.cache.clear()
            self.timestamps.clear()
    
    async def cleanup_expired(self):
        """Видалити всі прострочені записи"""
        async with self._lock:
            expired_keys = [
                key for key in self.cache.keys()
                if self._is_expired(key)
            ]
            for key in expired_keys:
                self._delete(key)
    
    def _is_expired(self, key: str) -> bool:
        """Перевірити чи запис прострочений"""
        if key not in self.timestamps:
            return True
        
        age = datetime.utcnow() - self.timestamps[key]
        return age.total_seconds() > self.ttl_seconds
    
    def _delete(self, key: str):
        """Видалити запис (без блокування)"""
        if key in self.cache:
            del self.cache[key]
        if key in self.timestamps:
            del self.timestamps[key]
    
    async def get_stats(self) -> dict:
        """Отримати статистику кешу"""
        async with self._lock:
            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "ttl_seconds": self.ttl_seconds,
                "fill_percentage": round(len(self.cache) / self.max_size * 100, 2)
            }

# Глобальні кеші
video_cache = TTLCache(max_size=200, ttl_seconds=300)  # 5 хвилин
recommendations_cache = TTLCache(max_size=100, ttl_seconds=600)  # 10 хвилин
top_videos_cache = TTLCache(max_size=50, ttl_seconds=3600)  # 1 година

async def cache_cleanup_task():
    """Фонове завдання очистки кешів"""
    while True:
        await asyncio.sleep(60)  # Кожну хвилину
        await video_cache.cleanup_expired()
        await recommendations_cache.cleanup_expired()
        await top_videos_cache.cleanup_expired()