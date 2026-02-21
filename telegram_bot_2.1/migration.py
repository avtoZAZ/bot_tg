# migration.py - Міграція даних з JSON на SQLite
# Автор: avtoZAZ
# Дата: 2025-11-11 19:20:17 UTC

import asyncio
import json
from datetime import datetime, timedelta
from database import (
    init_db, async_session_maker,
    User, Video, ViewHistory, Favorite, Rating, Payment
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def migrate_from_json(json_file: str = "data.json"):
    """
    Міграція даних з JSON файлу в SQLite базу даних
    
    ВАЖЛИВО: Запускати ОДИН РАЗ!
    """
    
    logger.info("=" * 60)
    logger.info("🔄 Початок міграції з JSON на SQLite")
    logger.info("=" * 60)
    
    # Ініціалізація БД
    await init_db()
    logger.info("✅ База даних ініціалізована")
    
    # Завантаження JSON
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"✅ JSON файл завантажено: {json_file}")
    except FileNotFoundError:
        logger.error(f"❌ Файл {json_file} не знайдено!")
        return
    except json.JSONDecodeError as e:
        logger.error(f"❌ Помилка парсингу JSON: {e}")
        return
    
    async with async_session_maker() as session:
        
        # ==================== 1. МІГРАЦІЯ КОРИСТУВАЧІВ ====================
        logger.info("\n📤 Міграція користувачів...")
        
        user_langs = data.get("user_langs", {})
        admins = data.get("admins", [])
        premium_users = data.get("premium_users", {})
        
        # Всі унікальні користувачі
        all_user_ids = set()
        all_user_ids.update([int(uid) for uid in user_langs.keys()])
        all_user_ids.update(admins)
        all_user_ids.update([int(uid) for uid in premium_users.keys()])
        
        # Додати користувачів з історії
        for user_id_str in data.get("user_history", {}).keys():
            all_user_ids.add(int(user_id_str))
        
        migrated_users = 0
        for user_id in all_user_ids:
            user_id_str = str(user_id)
            
            # Перевірити чи вже існує
            from sqlalchemy import select
            result = await session.execute(
                select(User).where(User.telegram_id == user_id)
            )
            if result.scalar_one_or_none():
                continue
            
            # Створити користувача
            user = User(
                telegram_id=user_id,
                language=user_langs.get(user_id_str, "en"),
                is_admin=(user_id in admins)
            )
            
            # Преміум
            if user_id_str in premium_users:
                premium_info = premium_users[user_id_str]
                expires = datetime.fromisoformat(premium_info["expires"])
                
                user.is_premium = (expires > datetime.utcnow())
                user.premium_expires = expires
                user.premium_plan = premium_info.get("plan", "unknown")
            
            session.add(user)
            migrated_users += 1
        
        await session.commit()
        logger.info(f"✅ Мігровано користувачів: {migrated_users}")
        
        # ==================== 2. МІГРАЦІЯ ВІДЕО ====================
        logger.info("\n📤 Міграція відео...")
        
        videos_data = data.get("videos", {})
        migrated_videos = 0
        
        for code, video_info in videos_data.items():
            # Перевірити чи вже існує
            result = await session.execute(
                select(Video).where(Video.code == code)
            )
            if result.scalar_one_or_none():
                continue
            
            # Створити відео
            video = Video(
                code=code,
                file_id=video_info.get("file_id", ""),
                title=video_info.get("title"),
                year=video_info.get("year"),
                genre=video_info.get("genre"),
                description=video_info.get("description"),
                poster_file_id=video_info.get("poster_file_id"),
                is_series=video_info.get("is_series", False),
                series_name=video_info.get("series_name"),
                season=video_info.get("season"),
                episode=video_info.get("episode"),
                is_premium=video_info.get("is_premium", False),
                uploaded_by=video_info.get("uploaded_by", 0),
                views_count=0  # Буде оновлено з views
            )
            
            # Парсинг created_at
            if "uploaded_at" in video_info:
                try:
                    video.created_at = datetime.fromisoformat(video_info["uploaded_at"])
                except:
                    pass
            
            session.add(video)
            migrated_videos += 1
        
        await session.commit()
        logger.info(f"✅ Мігровано відео: {migrated_videos}")
        
        # ==================== 3. МІГРАЦІЯ ПЕРЕГЛЯДІВ ====================
        logger.info("\n📤 Міграція переглядів...")
        
        views_data = data.get("views", {})
        user_history = data.get("user_history", {})
        
        # Оновити лічильник переглядів
        for code, view_count in views_data.items():
            result = await session.execute(
                select(Video).where(Video.code == code)
            )
            video = result.scalar_one_or_none()
            if video:
                video.views_count = view_count
        
        await session.commit()
        
        # Створити історію переглядів
        migrated_history = 0
        for user_id_str, codes in user_history.items():
            user_id = int(user_id_str)
            
            # Перевірити чи існує користувач
            result = await session.execute(
                select(User).where(User.telegram_id == user_id)
            )
            if not result.scalar_one_or_none():
                continue
            
            # Додати записи історії (останні 10)
            for i, code in enumerate(codes[:10]):
                # Перевірити чи існує відео
                result = await session.execute(
                    select(Video).where(Video.code == code)
                )
                if not result.scalar_one_or_none():
                    continue
                
                # Створити запис історії (з припущенням часу)
                viewed_at = datetime.utcnow() - timedelta(days=i)
                
                history = ViewHistory(
                    user_id=user_id,
                    video_code=code,
                    viewed_at=viewed_at
                )
                session.add(history)
                migrated_history += 1
        
        await session.commit()
        logger.info(f"✅ Мігровано записів історії: {migrated_history}")
        
        # ==================== 4. МІГРАЦІЯ ПЛАТЕЖІВ ====================
        logger.info("\n📤 Міграція платежів...")
        
        payments_data = data.get("payments", [])
        migrated_payments = 0
        
        for payment_info in payments_data:
            payment = Payment(
                user_id=payment_info.get("user_id", 0),
                plan=payment_info.get("plan", "unknown"),
                amount=payment_info.get("amount", 0),
                currency=payment_info.get("currency", "XTR"),
                telegram_payment_charge_id=payment_info.get("telegram_payment_charge_id", ""),
                status=payment_info.get("status", "completed")
            )
            
            # Парсинг дати
            if "date" in payment_info:
                try:
                    payment.created_at = datetime.fromisoformat(payment_info["date"])
                except:
                    pass
            
            session.add(payment)
            migrated_payments += 1
        
        await session.commit()
        logger.info(f"✅ Мігровано платежів: {migrated_payments}")
        
        # ==================== ПІДСУМОК ====================
        logger.info("\n" + "=" * 60)
        logger.info("✅ МІГРАЦІЯ ЗАВЕРШЕНА УСПІШНО!")
        logger.info("=" * 60)
        logger.info(f"👥 Користувачів: {migrated_users}")
        logger.info(f"🎬 Відео: {migrated_videos}")
        logger.info(f"👁️ Записів історії: {migrated_history}")
        logger.info(f"💰 Платежів: {migrated_payments}")
        logger.info("=" * 60)
        logger.info("\n⚠️ ВАЖЛИВО:")
        logger.info("1. Створіть бекап data.json перед видаленням!")
        logger.info("2. Перевірте працездатність бота з новою БД")
        logger.info("3. Тільки після цього видаляйте data.json")
        logger.info("=" * 60)

async def verify_migration():
    """Перевірка міграції"""
    logger.info("\n🔍 Перевірка міграції...")
    
    async with async_session_maker() as session:
        from sqlalchemy import select, func
        
        # Підрахунок записів
        result = await session.execute(select(func.count(User.id)))
        users_count = result.scalar()
        
        result = await session.execute(select(func.count(Video.id)))
        videos_count = result.scalar()
        
        result = await session.execute(select(func.count(ViewHistory.id)))
        history_count = result.scalar()
        
        result = await session.execute(select(func.count(Payment.id)))
        payments_count = result.scalar()
        
        logger.info(f"✅ Користувачів в БД: {users_count}")
        logger.info(f"✅ Відео в БД: {videos_count}")
        logger.info(f"✅ Записів історії: {history_count}")
        logger.info(f"✅ Платежів: {payments_count}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🔄 СКРИПТ МІГРАЦІЇ З JSON НА SQLite")
    print("="*60)
    print("\n⚠️ ПОПЕРЕДЖЕННЯ:")
    print("• Цей скрипт мігрує дані з data.json в SQLite базу")
    print("• Запускайте тільки ОДИН РАЗ!")
    print("• Створіть бекап data.json перед запуском!")
    print("\n" + "="*60)
    
    confirm = input("\n👉 Продовжити міграцію? (yes/no): ")
    
    if confirm.lower() in ['yes', 'y']:
        print("\n🚀 Запуск міграції...\n")
        asyncio.run(migrate_from_json())
        asyncio.run(verify_migration())
        print("\n✅ Готово!")
    else:
        print("\n❌ Міграція скасована.")