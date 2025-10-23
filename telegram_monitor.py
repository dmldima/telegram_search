import os
import re
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
import json

# Конфигурация из переменных окружения
API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING')
BOT_TOKEN = os.getenv('BOT_TOKEN')
YOUR_USER_ID = int(os.getenv('YOUR_USER_ID'))

# Каналы для мониторинга (через запятую)
CHANNELS = os.getenv('CHANNELS', '').split(',')
CHANNELS = [ch.strip() for ch in CHANNELS if ch.strip()]

# Ключевые слова для поиска (через запятую)
# Используйте ОСНОВЫ слов: "инвест" вместо "инвестиции,инвестиция,инвестирование"
KEYWORDS = os.getenv('KEYWORDS', '').split(',')
KEYWORDS = [kw.strip().lower() for kw in KEYWORDS if kw.strip()]

# Слова-исключения (через запятую)
EXCLUDE_KEYWORDS = os.getenv('EXCLUDE_KEYWORDS', '').split(',')
EXCLUDE_KEYWORDS = [kw.strip().lower() for kw in EXCLUDE_KEYWORDS if kw.strip()]

# Регулярные выражения (через точку с запятой)
PATTERNS_STR = os.getenv('PATTERNS', '')
PATTERNS = [p.strip() for p in PATTERNS_STR.split(';') if p.strip()]

# Глубина поиска - количество сообщений для проверки (по умолчанию 100)
SEARCH_DEPTH = int(os.getenv('SEARCH_DEPTH', '100'))

# Временной диапазон в часах (по умолчанию 24 часа)
TIME_RANGE_HOURS = int(os.getenv('TIME_RANGE_HOURS', '24'))

# Файл для хранения ID обработанных сообщений
PROCESSED_FILE = 'processed_messages.json'


def load_processed_messages():
    """Загружает список уже обработанных сообщений"""
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Конвертируем список в словарь с timestamp для очистки старых записей
            if isinstance(data, list):
                # Старый формат - конвертируем
                return {msg_id: datetime.now().isoformat() for msg_id in data}
            return data
    return {}


def save_processed_messages(processed_dict):
    """Сохраняет список обработанных сообщений с очисткой старых"""
    # Удаляем записи старше 30 дней
    cutoff_date = datetime.now() - timedelta(days=30)
    cleaned = {
        msg_id: timestamp 
        for msg_id, timestamp in processed_dict.items()
        if datetime.fromisoformat(timestamp) > cutoff_date
    }
    
    with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    
    return len(processed_dict) - len(cleaned)  # Количество удаленных


def should_forward_message(message_text):
    """
    Определяет, нужно ли пересылать сообщение
    Поддерживает поиск по основам слов (окончания игнорируются)
    """
    if not message_text:
        return False
    
    text_lower = message_text.lower()
    
    # Проверка исключающих слов (с поддержкой окончаний)
    for exclude_word in EXCLUDE_KEYWORDS:
        if exclude_word:
            # Ищем слово как часть других слов (окончания)
            if exclude_word in text_lower:
                return False
    
    # Проверка ключевых слов (с поддержкой окончаний)
    for keyword in KEYWORDS:
        if keyword:
            # Ищем основу слова - она может быть частью слова с окончанием
            # Например "инвест" найдет: инвестиции, инвестиция, инвестирование, инвестор
            if keyword in text_lower:
                return True
    
    # Проверка регулярных выражений
    for pattern in PATTERNS:
        if pattern:
            try:
                if re.search(pattern, message_text, re.IGNORECASE):
                    return True
            except re.error:
                print(f"⚠️ Некорректное регулярное выражение: {pattern}")
    
    return False


async def monitor_channel(client, bot, channel_username, processed_dict):
    """
    Мониторит один канал и возвращает статистику
    """
    new_processed = 0
    forwarded = 0
    skipped_duplicates = 0
    
    try:
        # Получаем канал
        channel = await client.get_entity(channel_username)
        
        print(f"📡 Проверяю канал: {channel_username}")
        
        # Получаем сообщения за заданный период
        time_threshold = datetime.now() - timedelta(hours=TIME_RANGE_HOURS)
        
        # Получаем сообщения с учетом глубины поиска
        async for message in client.iter_messages(channel, limit=SEARCH_DEPTH):
            # Пропускаем старые сообщения
            if message.date.replace(tzinfo=None) < time_threshold:
                break
            
            # Создаем уникальный ID для сообщения (канал + ID сообщения)
            unique_id = f"{channel_username}:{message.id}"
            
            # Пропускаем уже обработанные (проверка на дубли)
            if unique_id in processed_dict:
                skipped_duplicates += 1
                continue
            
            # Проверяем текст сообщения
            message_text = message.text or ""
            
            # Добавляем в обработанные с timestamp
            processed_dict[unique_id] = datetime.now().isoformat()
            new_processed += 1
            
            # Проверяем, нужно ли пересылать
            if should_forward_message(message_text):
                success = False
                
                # Попытка 1: Пересылка оригинального сообщения через БОТА
                # (работает только для публичных каналов)
                try:
                    result = await bot.forward_messages(
                        entity=YOUR_USER_ID,
                        messages=message.id,
                        from_peer=channel
                    )
                    
                    if result:
                        print(f"✅ Переслано: {channel_username} / {message.id}")
                        forwarded += 1
                        success = True
                        await asyncio.sleep(1)
                    
                except Exception as e:
                    # Пересылка не удалась - это нормально для групп/приватных каналов
                    pass
                
                # Попытка 2: Отправка текстом (основной способ)
                if not success:
                    try:
                        channel_name = channel_username.strip('@')
                        
                        # Формируем сообщение
                        fallback_text = f"📢 **Пост из {channel_username}**\n\n"
                        
                        # Добавляем ссылку если это публичный канал
                        if not channel_username.startswith('-'):
                            fallback_text += f"🔗 https://t.me/{channel_name}/{message.id}\n\n"
                        
                        # Добавляем текст
                        if message_text:
                            fallback_text += f"{message_text[:3000]}"
                            if len(message_text) > 3000:
                                fallback_text += "\n\n... (сообщение обрезано)"
                        else:
                            fallback_text += "(Сообщение без текста - возможно только медиа)"
                        
                        await bot.send_message(YOUR_USER_ID, fallback_text)
                        print(f"✅ Отправлено: {channel_username} / {message.id}")
                        forwarded += 1
                        await asyncio.sleep(1)
                        
                    except Exception as bot_error:
                        print(f"❌ Не удалось отправить {message.id}: {bot_error}")
                        # Не увеличиваем forwarded
        
        return new_processed, forwarded, skipped_duplicates
        
    except Exception as e:
        print(f"❌ Ошибка в канале {channel_username}: {e}")
        await bot.send_message(
            YOUR_USER_ID, 
            f"⚠️ Ошибка при обработке {channel_username}:\n{e}"
        )
        return 0, 0, 0


async def main():
    """Основная функция для мониторинга каналов"""
    
    # Проверяем наличие каналов
    if not CHANNELS:
        print("❌ Не указаны каналы для мониторинга!")
        print("Добавьте переменную CHANNELS в GitHub Variables")
        return
    
    # Проверяем наличие ключевых слов или паттернов
    if not KEYWORDS and not PATTERNS:
        print("⚠️ Не указаны ключевые слова или паттерны!")
        print("Будут пересылаться ВСЕ сообщения (кроме исключений)")
    
    # Создаем клиент с session string
    client = TelegramClient(
        StringSession(SESSION_STRING), 
        API_ID, 
        API_HASH
    )
    
    # Создаем клиент бота
    bot = TelegramClient(
        StringSession(), 
        API_ID, 
        API_HASH
    )
    
    await client.connect()
    await bot.start(bot_token=BOT_TOKEN)
    
    if not await client.is_user_authorized():
        print("❌ Session string недействителен!")
        await bot.send_message(
            YOUR_USER_ID,
            "❌ Session string недействителен! Сгенерируйте новый."
        )
        return
    
    print(f"🚀 Бот запущен: {datetime.now()}")
    print(f"📺 Каналов для мониторинга: {len(CHANNELS)}")
    print(f"🔑 Ключевых слов: {len(KEYWORDS)}")
    print(f"🚫 Слов-исключений: {len(EXCLUDE_KEYWORDS)}")
    print(f"🔍 Регулярных выражений: {len(PATTERNS)}")
    print(f"📊 Глубина поиска: {SEARCH_DEPTH} сообщений")
    print(f"⏱️ Временной диапазон: {TIME_RANGE_HOURS} часов")
    
    # Загружаем уже обработанные сообщения
    processed_dict = load_processed_messages()
    initial_processed_count = len(processed_dict)
    print(f"💾 Загружено обработанных сообщений: {initial_processed_count}")
    
    total_new = 0
    total_forwarded = 0
    total_skipped = 0
    
    try:
        # Мониторим каждый канал
        for channel in CHANNELS:
            new, forwarded, skipped = await monitor_channel(
                client, 
                bot, 
                channel, 
                processed_dict
            )
            total_new += new
            total_forwarded += forwarded
            total_skipped += skipped
            
            # Задержка между каналами
            await asyncio.sleep(2)
        
        # Сохраняем обработанные ID и очищаем старые
        removed_count = save_processed_messages(processed_dict)
        
        print(f"\n✨ Всего обработано новых: {total_new}")
        print(f"📤 Всего переслано: {total_forwarded}")
        print(f"⏭️ Пропущено дублей: {total_skipped}")
        print(f"🧹 Удалено старых записей: {removed_count}")
        
        # Не отправляем никаких служебных сообщений
        # Пользователь получает только сами посты
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        await bot.send_message(YOUR_USER_ID, f"⚠️ Критическая ошибка:\n{e}")
    
    finally:
        await client.disconnect()
        await bot.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
