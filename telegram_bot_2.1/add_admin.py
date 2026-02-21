# add_admin.py - Додати головного адміністратора
# Автор: avtoZAZ
# Дата: 2025-11-11 19:55:31 UTC

import asyncio
from database import init_db, async_session_maker, User
from sqlalchemy import select

async def add_main_admin():
    """Додати головного адміністратора"""
    
    ADMIN_ID = 1309506590  # ВАШ TELEGRAM ID
    
    print(f"\n{'='*60}")
    print(f"🔧 ДОДАВАННЯ ГОЛОВНОГО АДМІНІСТРАТОРА")
    print(f"{'='*60}")
    print(f"👤 Telegram ID: {ADMIN_ID}")
    print(f"👤 Username: avtoZAZ")
    print(f"🌍 Мова: українська (uk)")
    print(f"{'='*60}\n")
    
    print("🔄 Ініціалізація бази даних...")
    await init_db()
    print("✅ База даних ініціалізована\n")
    
    async with async_session_maker() as session:
        # Перевірити чи користувач вже існує
        result = await session.execute(
            select(User).where(User.telegram_id == ADMIN_ID)
        )
        user = result.scalar_one_or_none()
        
        if user:
            print(f"ℹ️  Користувач знайдено в базі даних")
            print(f"   Поточний статус адміна: {user.is_admin}")
            
            # Оновити статус на адміна
            user.is_admin = True
            user.language = "uk"
            await session.commit()
            
            print(f"✅ Статус оновлено на АДМІНІСТРАТОР!\n")
        else:
            print(f"ℹ️  Користувач не знайдено, створюємо нового...")
            
            # Створити нового користувача-адміна
            new_admin = User(
                telegram_id=ADMIN_ID,
                username="avtoZAZ",
                first_name="avtoZAZ",
                language="uk",
                is_admin=True
            )
            session.add(new_admin)
            await session.commit()
            
            print(f"✅ Новий адміністратор створений!\n")
        
        # Фінальна перевірка
        result = await session.execute(
            select(User).where(User.telegram_id == ADMIN_ID)
        )
        user = result.scalar_one_or_none()
        
        if user and user.is_admin:
            print(f"{'='*60}")
            print(f"✅ УСПІШНО! ВИ ТЕПЕР ГОЛОВНИЙ АДМІНІСТРАТОР!")
            print(f"{'='*60}")
            print(f"👤 ID: {user.telegram_id}")
            print(f"👤 Username: {user.username or 'N/A'}")
            print(f"🌍 Мова: {user.language}")
            print(f"👑 Адміністратор: ✅ ТАК")
            print(f"📅 Створено: {user.created_at}")
            print(f"{'='*60}\n")
            
            print(f"💡 НАСТУПНІ КРОКИ:")
            print(f"   1. Запустіть бота: py -3.11 bot.py")
            print(f"   2. Відкрийте Telegram")
            print(f"   3. Знайдіть вашого бота")
            print(f"   4. Надішліть команду /start")
            print(f"   5. Ви побачите адміністраторське меню! 🎉\n")
        else:
            print(f"❌ ПОМИЛКА! Щось пішло не так!")
            print(f"   Перевірте базу даних або зверніться до розробника.\n")

if __name__ == "__main__":
    try:
        asyncio.run(add_main_admin())
    except Exception as e:
        print(f"\n❌ ПОМИЛКА: {e}\n")
        import traceback
        traceback.print_exc()