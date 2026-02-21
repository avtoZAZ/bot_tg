# recommendations.py - Система рекомендацій
# Автор: avtoZAZ
# Дата: 2025-11-11 19:15:42 UTC

from typing import List
from sqlalchemy import select, and_, or_
from database import async_session_maker, Video, ViewHistory
from cache import recommendations_cache
import random

async def get_recommendations(video_code: str, user_id: int = None, limit: int = 5) -> List[Video]:
    """Отримати рекомендації на основі відео"""
    
    # Перевірити кеш
    cache_key = f"rec_{video_code}_{limit}"
    cached = await recommendations_cache.get(cache_key)
    if cached:
        return cached
    
    async with async_session_maker() as session:
        # Отримати поточне відео
        result = await session.execute(
            select(Video).where(Video.code == video_code)
        )
        current_video = result.scalar_one_or_none()
        
        if not current_video:
            return []
        
        recommendations = []
        
        # 1. Якщо це серіал - порекомендувати інші епізоди
        if current_video.is_series and current_video.series_name:
            result = await session.execute(
                select(Video)
                .where(
                    and_(
                        Video.series_name == current_video.series_name,
                        Video.code != video_code
                    )
                )
                .order_by(Video.season, Video.episode)
                .limit(limit)
            )
            recommendations = list(result.scalars().all())
        
        # 2. Якщо не вистачає - рекомендувати по жанру
        if len(recommendations) < limit and current_video.genre:
            result = await session.execute(
                select(Video)
                .where(
                    and_(
                        Video.genre == current_video.genre,
                        Video.code != video_code,
                        ~Video.code.in_([v.code for v in recommendations])
                    )
                )
                .order_by(Video.avg_rating.desc(), Video.views_count.desc())
                .limit(limit - len(recommendations))
            )
            recommendations.extend(result.scalars().all())
        
        # 3. Якщо все ще не вистачає - по типу та рейтингу
        if len(recommendations) < limit:
            result = await session.execute(
                select(Video)
                .where(
                    and_(
                        Video.video_type == current_video.video_type,
                        Video.code != video_code,
                        ~Video.code.in_([v.code for v in recommendations])
                    )
                )
                .order_by(Video.avg_rating.desc(), Video.views_count.desc())
                .limit(limit - len(recommendations))
            )
            recommendations.extend(result.scalars().all())
        
        # Перемішати для різноманітності
        random.shuffle(recommendations)
        recommendations = recommendations[:limit]
        
        # Зберегти в кеш
        await recommendations_cache.set(cache_key, recommendations)
        
        return recommendations

async def get_top_weekly(limit: int = 5) -> List[Video]:
    """Топ-5 за тиждень"""
    from datetime import datetime, timedelta
    
    # Перевірити кеш
    cache_key = f"top_weekly_{limit}"
    cached = await top_videos_cache.get(cache_key)
    if cached:
        return cached
    
    week_ago = datetime.utcnow() - timedelta(days=7)
    
    async with async_session_maker() as session:
        # Відео з найбільшою кількістю переглядів за тиждень
        result = await session.execute(
            select(Video.code, func.count(ViewHistory.id).label('weekly_views'))
            .join(ViewHistory)
            .where(ViewHistory.viewed_at >= week_ago)
            .group_by(Video.code)
            .order_by(func.count(ViewHistory.id).desc())
            .limit(limit)
        )
        
        top_codes = [row.code for row in result.all()]
        
        if not top_codes:
            # Якщо немає переглядів за тиждень, повернути топ по загальних переглядах
            result = await session.execute(
                select(Video)
                .order_by(Video.views_count.desc())
                .limit(limit)
            )
            top_videos = list(result.scalars().all())
        else:
            result = await session.execute(
                select(Video).where(Video.code.in_(top_codes))
            )
            top_videos = list(result.scalars().all())
            
            # Сортувати в порядку топ_кодів
            top_videos.sort(key=lambda v: top_codes.index(v.code))
        
        # Зберегти в кеш
        await top_videos_cache.set(cache_key, top_videos)
        
        return top_videos

async def get_personalized_recommendations(user_id: int, limit: int = 10) -> List[Video]:
    """Персоналізовані рекомендації на основі історії користувача"""
    
    async with async_session_maker() as session:
        # Отримати останні 5 переглянутих відео
        result = await session.execute(
            select(Video)
            .join(ViewHistory)
            .where(ViewHistory.user_id == user_id)
            .order_by(ViewHistory.viewed_at.desc())
            .limit(5)
        )
        recent_videos = result.scalars().all()
        
        if not recent_videos:
            # Якщо немає історії, повернути топ-рейтинг
            result = await session.execute(
                select(Video)
                .where(Video.ratings_count > 0)
                .order_by(Video.avg_rating.desc())
                .limit(limit)
            )
            return list(result.scalars().all())
        
        # Збір жанрів, серіалів та типів з історії
        genres = set()
        series_names = set()
        video_types = set()
        
        for video in recent_videos:
            if video.genre:
                genres.add(video.genre)
            if video.series_name:
                series_names.add(video.series_name)
            video_types.add(video.video_type)
        
        # Отримати рекомендації
        conditions = []
        
        if genres:
            conditions.append(Video.genre.in_(list(genres)))
        if series_names:
            conditions.append(Video.series_name.in_(list(series_names)))
        if video_types:
            conditions.append(Video.video_type.in_(list(video_types)))
        
        if not conditions:
            conditions.append(Video.id > 0)  # Fallback
        
        # Виключити вже переглянуті
        viewed_codes = [v.code for v in recent_videos]
        
        result = await session.execute(
            select(Video)
            .where(
                and_(
                    or_(*conditions),
                    ~Video.code.in_(viewed_codes)
                )
            )
            .order_by(Video.avg_rating.desc(), Video.views_count.desc())
            .limit(limit)
        )
        
        return list(result.scalars().all())

# Імпорт для top_weekly
from sqlalchemy import func
from cache import top_videos_cache