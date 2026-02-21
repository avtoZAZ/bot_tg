# database.py - SQLAlchemy асинхронна база даних
# Автор: avtoZAZ
# Дата: 2025-11-11 19:15:42 UTC

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Boolean, DateTime, Float, ForeignKey, Text, select, func
from datetime import datetime
from typing import Optional, List
import json

# База даних
DATABASE_URL = "sqlite+aiosqlite:///bot_data.db"

engine = create_async_engine(DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

# ==================== МОДЕЛІ ====================

class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    language: Mapped[str] = mapped_column(String(10), default="en")
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False)
    premium_expires: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    premium_plan: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_active: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    referral_code: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True)
    referred_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Relationships
    history: Mapped[List["ViewHistory"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    favorites: Mapped[List["Favorite"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    ratings: Mapped[List["Rating"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    subscriptions: Mapped[List["SeriesSubscription"]] = relationship(back_populates="user", cascade="all, delete-orphan")

class Video(Base):
    __tablename__ = "videos"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    file_id: Mapped[str] = mapped_column(String(255))
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    year: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    genre: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    poster_file_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_series: Mapped[bool] = mapped_column(Boolean, default=False)
    series_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    episode: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False)
    language: Mapped[int] = mapped_column(Integer, default=1)  # 1=en, 2=ru, 3=cn, 4=es, 5=hi
    video_type: Mapped[int] = mapped_column(Integer, default=1)  # 1=movie, 2=series, 3=anime
    views_count: Mapped[int] = mapped_column(Integer, default=0)
    avg_rating: Mapped[float] = mapped_column(Float, default=0.0)
    ratings_count: Mapped[int] = mapped_column(Integer, default=0)
    uploaded_by: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    history: Mapped[List["ViewHistory"]] = relationship(back_populates="video", cascade="all, delete-orphan")
    favorites: Mapped[List["Favorite"]] = relationship(back_populates="video", cascade="all, delete-orphan")
    ratings: Mapped[List["Rating"]] = relationship(back_populates="video", cascade="all, delete-orphan")

class ViewHistory(Base):
    __tablename__ = "view_history"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.telegram_id", ondelete="CASCADE"))
    video_code: Mapped[str] = mapped_column(ForeignKey("videos.code", ondelete="CASCADE"))
    viewed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="history")
    video: Mapped["Video"] = relationship(back_populates="history")

class Favorite(Base):
    __tablename__ = "favorites"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.telegram_id", ondelete="CASCADE"))
    video_code: Mapped[str] = mapped_column(ForeignKey("videos.code", ondelete="CASCADE"))
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="favorites")
    video: Mapped["Video"] = relationship(back_populates="favorites")

class Rating(Base):
    __tablename__ = "ratings"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.telegram_id", ondelete="CASCADE"))
    video_code: Mapped[str] = mapped_column(ForeignKey("videos.code", ondelete="CASCADE"))
    stars: Mapped[int] = mapped_column(Integer)  # 1-5
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="ratings")
    video: Mapped["Video"] = relationship(back_populates="ratings")

class SeriesSubscription(Base):
    __tablename__ = "series_subscriptions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.telegram_id", ondelete="CASCADE"))
    series_name: Mapped[str] = mapped_column(String(255), index=True)
    subscribed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="subscriptions")

class PromoCode(Base):
    __tablename__ = "promo_codes"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    duration_days: Mapped[int] = mapped_column(Integer)
    max_uses: Mapped[int] = mapped_column(Integer, default=1)
    current_uses: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_by: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

class SearchQuery(Base):
    __tablename__ = "search_queries"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    query: Mapped[str] = mapped_column(String(500))
    results_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

class AdClick(Base):
    __tablename__ = "ad_clicks"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    ad_id: Mapped[str] = mapped_column(String(100))
    clicked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

class Payment(Base):
    __tablename__ = "payments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    plan: Mapped[str] = mapped_column(String(50))
    amount: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(10))
    telegram_payment_charge_id: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), default="completed")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

# ==================== ФУНКЦІЇ БД ====================

async def init_db():
    """Ініціалізація бази даних"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session() -> AsyncSession:
    """Отримати сесію бази даних"""
    async with async_session_maker() as session:
        yield session

async def get_or_create_user(telegram_id: int, username: str = None, first_name: str = None, language: str = "en") -> User:
    """Отримати або створити користувача"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            # Генерація реферального коду
            import random
            import string
            referral_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            
            user = User(
                telegram_id=telegram_id,
                username=username,
                first_name=first_name,
                language=language,
                referral_code=referral_code
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
        else:
            # Оновлення останньої активності
            user.last_active = datetime.utcnow()
            if username:
                user.username = username
            if first_name:
                user.first_name = first_name
            await session.commit()
        
        return user

async def add_to_history(user_id: int, video_code: str):
    """Додати відео в історію"""
    async with async_session_maker() as session:
        # Видалити старий запис якщо є
        await session.execute(
            select(ViewHistory).where(
                ViewHistory.user_id == user_id,
                ViewHistory.video_code == video_code
            )
        )
        
        history = ViewHistory(user_id=user_id, video_code=video_code)
        session.add(history)
        
        # Збільшити лічильник переглядів
        result = await session.execute(
            select(Video).where(Video.code == video_code)
        )
        video = result.scalar_one_or_none()
        if video:
            video.views_count += 1
        
        await session.commit()

async def get_user_history(user_id: int, limit: int = 10) -> List[Video]:
    """Отримати історію перегляду користувача"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Video)
            .join(ViewHistory)
            .where(ViewHistory.user_id == user_id)
            .order_by(ViewHistory.viewed_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

async def toggle_favorite(user_id: int, video_code: str) -> bool:
    """Додати/видалити з улюблених. Повертає True якщо додано, False якщо видалено"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Favorite).where(
                Favorite.user_id == user_id,
                Favorite.video_code == video_code
            )
        )
        favorite = result.scalar_one_or_none()
        
        if favorite:
            await session.delete(favorite)
            await session.commit()
            return False
        else:
            favorite = Favorite(user_id=user_id, video_code=video_code)
            session.add(favorite)
            await session.commit()
            return True

async def get_user_favorites(user_id: int) -> List[Video]:
    """Отримати улюблені відео користувача"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Video)
            .join(Favorite)
            .where(Favorite.user_id == user_id)
            .order_by(Favorite.added_at.desc())
        )
        return result.scalars().all()

async def is_favorite(user_id: int, video_code: str) -> bool:
    """Перевірити чи відео в улюблених"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Favorite).where(
                Favorite.user_id == user_id,
                Favorite.video_code == video_code
            )
        )
        return result.scalar_one_or_none() is not None

async def rate_video(user_id: int, video_code: str, stars: int):
    """Оцінити відео (1-5 зірок)"""
    async with async_session_maker() as session:
        # Перевірити чи вже оцінював
        result = await session.execute(
            select(Rating).where(
                Rating.user_id == user_id,
                Rating.video_code == video_code
            )
        )
        rating = result.scalar_one_or_none()
        
        if rating:
            rating.stars = stars
        else:
            rating = Rating(user_id=user_id, video_code=video_code, stars=stars)
            session.add(rating)
        
        # Оновити середній рейтинг відео
        result = await session.execute(
            select(func.avg(Rating.stars), func.count(Rating.id))
            .where(Rating.video_code == video_code)
        )
        avg_rating, count = result.one()
        
        video_result = await session.execute(
            select(Video).where(Video.code == video_code)
        )
        video = video_result.scalar_one_or_none()
        if video:
            video.avg_rating = float(avg_rating or 0)
            video.ratings_count = count or 0
        
        await session.commit()

async def get_top_rated(limit: int = 10) -> List[Video]:
    """Отримати топ-рейтинг відео"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Video)
            .where(Video.ratings_count > 0)
            .order_by(Video.avg_rating.desc(), Video.ratings_count.desc())
            .limit(limit)
        )
        return result.scalars().all()

async def subscribe_to_series(user_id: int, series_name: str) -> bool:
    """Підписатися на серіал. Повертає True якщо підписано, False якщо відписано"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(SeriesSubscription).where(
                SeriesSubscription.user_id == user_id,
                SeriesSubscription.series_name == series_name
            )
        )
        subscription = result.scalar_one_or_none()
        
        if subscription:
            await session.delete(subscription)
            await session.commit()
            return False
        else:
            subscription = SeriesSubscription(user_id=user_id, series_name=series_name)
            session.add(subscription)
            await session.commit()
            return True

async def get_series_subscribers(series_name: str) -> List[int]:
    """Отримати список підписників серіалу"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(SeriesSubscription.user_id)
            .where(SeriesSubscription.series_name == series_name)
        )
        return [row[0] for row in result.all()]

async def log_search_query(user_id: int, query: str, results_count: int):
    """Зберегти пошуковий запит"""
    async with async_session_maker() as session:
        search = SearchQuery(user_id=user_id, query=query, results_count=results_count)
        session.add(search)
        await session.commit()

async def log_ad_click(user_id: int, ad_id: str, clicked: bool = False):
    """Зберегти статистику реклами"""
    async with async_session_maker() as session:
        ad_click = AdClick(user_id=user_id, ad_id=ad_id, clicked=clicked)
        session.add(ad_click)
        await session.commit()

async def get_retention_stats(days: int = 30) -> dict:
    """Статистика утримання користувачів"""
    async with async_session_maker() as session:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Користувачі зареєстровані за період
        result = await session.execute(
            select(func.count(User.id))
            .where(User.created_at >= cutoff_date)
        )
        new_users = result.scalar()
        
        # Активні з них
        result = await session.execute(
            select(func.count(User.id))
            .where(
                User.created_at >= cutoff_date,
                User.last_active >= cutoff_date
            )
        )
        active_users = result.scalar()
        
        retention = (active_users / new_users * 100) if new_users > 0 else 0
        
        return {
            "new_users": new_users,
            "active_users": active_users,
            "retention_rate": round(retention, 2)
        }

async def get_ad_ctr_stats() -> dict:
    """Статистика CTR реклами"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(
                func.count(AdClick.id).label('total_shows'),
                func.sum(AdClick.clicked).label('total_clicks')
            )
        )
        row = result.one()
        
        total_shows = row.total_shows or 0
        total_clicks = row.total_clicks or 0
        ctr = (total_clicks / total_shows * 100) if total_shows > 0 else 0
        
        return {
            "total_shows": total_shows,
            "total_clicks": total_clicks,
            "ctr": round(ctr, 2)
        }

async def get_popular_searches(limit: int = 10) -> List[tuple]:
    """Популярні пошукові запити"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(SearchQuery.query, func.count(SearchQuery.id).label('count'))
            .group_by(SearchQuery.query)
            .order_by(func.count(SearchQuery.id).desc())
            .limit(limit)
        )
        return [(row.query, row.count) for row in result.all()]

async def get_failed_searches(limit: int = 10) -> List[tuple]:
    """Запити без результатів"""
    async with async_session_maker() as session:
        result = await session.execute(
            select(SearchQuery.query, func.count(SearchQuery.id).label('count'))
            .where(SearchQuery.results_count == 0)
            .group_by(SearchQuery.query)
            .order_by(func.count(SearchQuery.id).desc())
            .limit(limit)
        )
        return [(row.query, row.count) for row in result.all()]