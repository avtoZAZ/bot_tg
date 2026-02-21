# analytics.py - Поглиблена аналітика
# Автор: avtoZAZ
# Дата: 2025-11-11 19:18:05 UTC

from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from sqlalchemy import select, func, and_, or_
from database import (
    async_session_maker, User, Video, ViewHistory,
    SearchQuery, AdClick, Payment, Rating
)

class Analytics:
    """Клас для поглибленої аналітики"""
    
    @staticmethod
    async def get_retention_report(days: int = 30) -> dict:
        """
        Детальний звіт про утримання користувачів
        
        Показує:
        - Нові користувачі за період
        - Активні з них
        - Retention rate
        - Когортний аналіз
        """
        async with async_session_maker() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Нові користувачі
            result = await session.execute(
                select(func.count(User.id))
                .where(User.created_at >= cutoff_date)
            )
            new_users = result.scalar() or 0
            
            # Активні (мали активність за останній тиждень)
            week_ago = datetime.utcnow() - timedelta(days=7)
            result = await session.execute(
                select(func.count(User.id))
                .where(
                    User.created_at >= cutoff_date,
                    User.last_active >= week_ago
                )
            )
            active_users = result.scalar() or 0
            
            # Retention rate
            retention = (active_users / new_users * 100) if new_users > 0 else 0
            
            # Когортний аналіз по тижнях
            cohorts = []
            for week in range(4):
                week_start = datetime.utcnow() - timedelta(weeks=week+1)
                week_end = datetime.utcnow() - timedelta(weeks=week)
                
                result = await session.execute(
                    select(func.count(User.id))
                    .where(
                        User.created_at >= week_start,
                        User.created_at < week_end
                    )
                )
                cohort_size = result.scalar() or 0
                
                result = await session.execute(
                    select(func.count(User.id))
                    .where(
                        User.created_at >= week_start,
                        User.created_at < week_end,
                        User.last_active >= week_ago
                    )
                )
                cohort_active = result.scalar() or 0
                
                cohort_retention = (cohort_active / cohort_size * 100) if cohort_size > 0 else 0
                
                cohorts.append({
                    "week": f"Week -{week+1}",
                    "new_users": cohort_size,
                    "active_users": cohort_active,
                    "retention": round(cohort_retention, 2)
                })
            
            return {
                "period_days": days,
                "new_users": new_users,
                "active_users": active_users,
                "retention_rate": round(retention, 2),
                "cohorts": cohorts
            }
    
    @staticmethod
    async def get_ad_analytics() -> dict:
        """
        Детальна аналітика реклами
        
        Показує:
        - Загальна кількість показів
        - Кліки
        - CTR (Click-Through Rate)
        - CTR по мовах
        """
        async with async_session_maker() as session:
            # Загальна статистика
            result = await session.execute(
                select(
                    func.count(AdClick.id).label('total_shows'),
                    func.sum(AdClick.clicked.cast(Integer)).label('total_clicks')
                )
            )
            row = result.one()
            
            total_shows = row.total_shows or 0
            total_clicks = row.total_clicks or 0
            ctr = (total_clicks / total_shows * 100) if total_shows > 0 else 0
            
            # CTR за останній тиждень
            week_ago = datetime.utcnow() - timedelta(days=7)
            result = await session.execute(
                select(
                    func.count(AdClick.id).label('weekly_shows'),
                    func.sum(AdClick.clicked.cast(Integer)).label('weekly_clicks')
                )
                .where(AdClick.created_at >= week_ago)
            )
            row = result.one()
            
            weekly_shows = row.weekly_shows or 0
            weekly_clicks = row.weekly_clicks or 0
            weekly_ctr = (weekly_clicks / weekly_shows * 100) if weekly_shows > 0 else 0
            
            # Топ рекламних блоків
            result = await session.execute(
                select(
                    AdClick.ad_id,
                    func.count(AdClick.id).label('shows'),
                    func.sum(AdClick.clicked.cast(Integer)).label('clicks')
                )
                .group_by(AdClick.ad_id)
                .order_by(func.count(AdClick.id).desc())
                .limit(5)
            )
            
            top_ads = []
            for row in result.all():
                ad_shows = row.shows or 0
                ad_clicks = row.clicks or 0
                ad_ctr = (ad_clicks / ad_shows * 100) if ad_shows > 0 else 0
                
                top_ads.append({
                    "ad_id": row.ad_id,
                    "shows": ad_shows,
                    "clicks": ad_clicks,
                    "ctr": round(ad_ctr, 2)
                })
            
            return {
                "total_shows": total_shows,
                "total_clicks": total_clicks,
                "overall_ctr": round(ctr, 2),
                "weekly_shows": weekly_shows,
                "weekly_clicks": weekly_clicks,
                "weekly_ctr": round(weekly_ctr, 2),
                "top_ads": top_ads
            }
    
    @staticmethod
    async def get_search_analytics() -> dict:
        """
        Аналітика пошукових запитів
        
        Показує:
        - Топ пошукових запитів
        - Запити без результатів
        - Тренди пошуку
        """
        async with async_session_maker() as session:
            # Загальна кількість пошуків
            result = await session.execute(
                select(func.count(SearchQuery.id))
            )
            total_searches = result.scalar() or 0
            
            # Топ-10 популярних запитів
            result = await session.execute(
                select(
                    SearchQuery.query,
                    func.count(SearchQuery.id).label('count')
                )
                .group_by(SearchQuery.query)
                .order_by(func.count(SearchQuery.id).desc())
                .limit(10)
            )
            
            popular_queries = [
                {"query": row.query, "count": row.count}
                for row in result.all()
            ]
            
            # Запити без результатів
            result = await session.execute(
                select(
                    SearchQuery.query,
                    func.count(SearchQuery.id).label('count')
                )
                .where(SearchQuery.results_count == 0)
                .group_by(SearchQuery.query)
                .order_by(func.count(SearchQuery.id).desc())
                .limit(10)
            )
            
            failed_queries = [
                {"query": row.query, "count": row.count}
                for row in result.all()
            ]
            
            # Пошуки за останній тиждень
            week_ago = datetime.utcnow() - timedelta(days=7)
            result = await session.execute(
                select(func.count(SearchQuery.id))
                .where(SearchQuery.created_at >= week_ago)
            )
            weekly_searches = result.scalar() or 0
            
            return {
                "total_searches": total_searches,
                "weekly_searches": weekly_searches,
                "popular_queries": popular_queries,
                "failed_queries": failed_queries,
                "failed_percentage": round(len(failed_queries) / total_searches * 100, 2) if total_searches > 0 else 0
            }
    
    @staticmethod
    async def get_revenue_analytics() -> dict:
        """Аналітика доходів"""
        async with async_session_maker() as session:
            # Загальний дохід
            result = await session.execute(
                select(func.sum(Payment.amount))
                .where(Payment.currency == "XTR")
            )
            total_revenue = result.scalar() or 0
            
            # Дохід за місяць
            month_ago = datetime.utcnow() - timedelta(days=30)
            result = await session.execute(
                select(func.sum(Payment.amount))
                .where(
                    Payment.currency == "XTR",
                    Payment.created_at >= month_ago
                )
            )
            monthly_revenue = result.scalar() or 0
            
            # Розподіл по планах
            result = await session.execute(
                select(
                    Payment.plan,
                    func.count(Payment.id).label('count'),
                    func.sum(Payment.amount).label('revenue')
                )
                .where(Payment.currency == "XTR")
                .group_by(Payment.plan)
                .order_by(func.sum(Payment.amount).desc())
            )
            
            plans_breakdown = [
                {
                    "plan": row.plan,
                    "purchases": row.count,
                    "revenue": row.revenue or 0
                }
                for row in result.all()
            ]
            
            # Середній чек
            result = await session.execute(
                select(func.count(Payment.id))
                .where(Payment.currency == "XTR")
            )
            total_payments = result.scalar() or 0
            
            avg_payment = (total_revenue / total_payments) if total_payments > 0 else 0
            
            return {
                "total_revenue": total_revenue,
                "monthly_revenue": monthly_revenue,
                "total_payments": total_payments,
                "avg_payment": round(avg_payment, 2),
                "plans_breakdown": plans_breakdown
            }
    
    @staticmethod
    async def get_content_analytics() -> dict:
        """Аналітика контенту"""
        async with async_session_maker() as session:
            # Загальна кількість відео
            result = await session.execute(
                select(func.count(Video.id))
            )
            total_videos = result.scalar() or 0
            
            # По типах
            result = await session.execute(
                select(
                    Video.video_type,
                    func.count(Video.id).label('count')
                )
                .group_by(Video.video_type)
            )
            
            type_map = {1: "Movies", 2: "Series", 3: "Anime"}
            by_type = [
                {"type": type_map.get(row.video_type, "Unknown"), "count": row.count}
                for row in result.all()
            ]
            
            # По мовах
            result = await session.execute(
                select(
                    Video.language,
                    func.count(Video.id).label('count')
                )
                .group_by(Video.language)
            )
            
            lang_map = {1: "English", 2: "Russian", 3: "Chinese", 4: "Spanish", 5: "Hindi"}
            by_language = [
                {"language": lang_map.get(row.language, "Unknown"), "count": row.count}
                for row in result.all()
            ]
            
            # Преміум vs Безкоштовні
            result = await session.execute(
                select(
                    Video.is_premium,
                    func.count(Video.id).label('count')
                )
                .group_by(Video.is_premium)
            )
            
            premium_breakdown = {
                "free": 0,
                "premium": 0
            }
            for row in result.all():
                if row.is_premium:
                    premium_breakdown["premium"] = row.count
                else:
                    premium_breakdown["free"] = row.count
            
            # Топ по рейтингу
            result = await session.execute(
                select(Video.title, Video.avg_rating, Video.ratings_count)
                .where(Video.ratings_count > 0)
                .order_by(Video.avg_rating.desc())
                .limit(5)
            )
            
            top_rated = [
                {
                    "title": row.title or "Untitled",
                    "rating": round(row.avg_rating, 2),
                    "votes": row.ratings_count
                }
                for row in result.all()
            ]
            
            return {
                "total_videos": total_videos,
                "by_type": by_type,
                "by_language": by_language,
                "premium_breakdown": premium_breakdown,
                "top_rated": top_rated
            }

analytics = Analytics()

# Імпорт для AdClick.clicked.cast(Integer)
from sqlalchemy import Integer