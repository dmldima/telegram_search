import os
import re
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
import asyncio
import json
import sys

# Конфигурация из переменных окружения.
# Числовые значения парсятся в main() ПОСЛЕ проверки наличия переменных,
# иначе отсутствие переменной даёт TypeError на импорте (int(None)) вместо
# понятного сообщения об ошибке.
API_ID = None
API_HASH = None
SESSION_STRING = None
BOT_TOKEN = None
YOUR_USER_ID = None

# НОВОЕ: Конфигурация нескольких наборов мониторинга
# Формат JSON: [{"name": "set1", "channels": ["@chan1"], "keywords": ["word1"], "exclude": ["bad1"], "patterns": ["regex1"]}, ...]
MONITOR_SETS_JSON = os.getenv('MONITOR_SETS', '')

# СТАРАЯ конфигурация (для обратной совместимости)
CHANNELS = os.getenv('CHANNELS', '').split(',')
CHANNELS = [ch.strip() for ch in CHANNELS if ch.strip()]

KEYWORDS = os.getenv('KEYWORDS', '').split(',')
KEYWORDS = [kw.strip().lower() for kw in KEYWORDS if kw.strip()]

EXCLUDE_KEYWORDS = os.getenv('EXCLUDE_KEYWORDS', '').split(',')
EXCLUDE_KEYWORDS = [kw.strip().lower() for kw in EXCLUDE_KEYWORDS if kw.strip()]

PATTERNS_STR = os.getenv('PATTERNS', '')
PATTERNS = [p.strip() for p in PATTERNS_STR.split(';') if p.strip()]

# Глубина поиска - количество сообщений для проверки (по умолчанию 100)
SEARCH_DEPTH = int(os.getenv('SEARCH_DEPTH', '100'))

# Временной диапазон в часах (по умолчанию 24 часа)
TIME_RANGE_HOURS = int(os.getenv('TIME_RANGE_HOURS', '24'))

# Файл для хранения ID обработанных сообщений
PROCESSED_FILE = 'processed_messages.json'

# Файл-блокировка для предотвращения параллельных запусков
LOCK_FILE = '/tmp/telegram_search.lock'


def parse_monitor_sets():
    """
    Парсит конфигурацию наборов мониторинга
    Возвращает список наборов или None если используется старая конфигурация
    """
    if not MONITOR_SETS_JSON:
        # Используем старую конфигурацию
        if CHANNELS or KEYWORDS or PATTERNS:
            return [{
                'name': 'default',
                'channels': CHANNELS,
                'keywords': KEYWORDS,
                'exclude': EXCLUDE_KEYWORDS,
                'patterns': PATTERNS
            }]
        return []
    
    try:
        sets = json.loads(MONITOR_SETS_JSON)
        
        # Нормализуем данные
        normalized = []
        for s in sets:
            # Валидация структуры
            if not validate_monitor_set(s):
                print(f"⚠️ Пропущен некорректный набор: {s.get('name', 'unknown')}")
                continue
                
            normalized.append({
                'name': s.get('name', f'set_{len(normalized)+1}'),
                'channels': [ch.strip() for ch in s.get('channels', []) if ch.strip()],
                'keywords': [kw.strip().lower() for kw in s.get('keywords', []) if kw.strip()],
                'exclude': [ex.strip().lower() for ex in s.get('exclude', []) if ex.strip()],
                'patterns': [p.strip() for p in s.get('patterns', []) if p.strip()]
            })
        
        return normalized
    
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга MONITOR_SETS JSON: {e}")
        print("Используется старая конфигурация")
        return [{
            'name': 'default',
            'channels': CHANNELS,
            'keywords': KEYWORDS,
            'exclude': EXCLUDE_KEYWORDS,
            'patterns': PATTERNS
        }]


def validate_monitor_set(monitor_set):
    """Валидация структуры набора мониторинга"""
    required_keys = ['name', 'channels', 'keywords', 'exclude', 'patterns']
    
    if not isinstance(monitor_set, dict):
        return False
    
    for key in required_keys:
        if key not in monitor_set:
            print(f"⚠️ Отсутствует обязательное поле: {key}")
            return False
    
    return True


def load_processed_messages():
    """Загружает список уже обработанных сообщений"""
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Конвертируем список в словарь с timestamp для очистки старых записей
            if isinstance(data, list):
                # Старый формат - конвертируем
                now_iso = datetime.now(timezone.utc).isoformat()
                return {msg_id: now_iso for msg_id in data}
            return data
    return {}


def save_processed_messages(processed_dict):
    """Сохраняет список обработанных сообщений с очисткой старых"""
    # Удаляем записи старше 30 дней
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
    cleaned = {}
    for msg_id, timestamp in processed_dict.items():
        try:
            dt = datetime.fromisoformat(timestamp)
            # Легаси-записи хранились без таймзоны — считаем их UTC,
            # иначе сравнение naive и aware дат бросит TypeError.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            # Некорректный timestamp — не теряем запись, считаем свежей
            dt = datetime.now(timezone.utc)
        if dt > cutoff_date:
            cleaned[msg_id] = timestamp

    with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    return len(processed_dict) - len(cleaned)  # Количество удаленных


def should_forward_message(message_text, keywords, exclude_keywords, patterns):
    """
    Определяет, нужно ли пересылать сообщение
    Поддерживает поиск по основам слов (окончания игнорируются)
    """
    if not message_text:
        return False
    
    text_lower = message_text.lower()
    
    # Проверка исключающих слов (с поддержкой окончаний)
    for exclude_word in exclude_keywords:
        if exclude_word:
            # Ищем слово как часть других слов (окончания)
            if exclude_word in text_lower:
                return False
    
    # Проверка ключевых слов (с поддержкой окончаний)
    for keyword in keywords:
        if keyword:
            # Ищем основу слова - она может быть частью слова с окончанием
            # Например "инвест" найдет: инвестиции, инвестиция, инвестирование, инвестор
            if keyword in text_lower:
                return True
    
    # Проверка регулярных выражений
    for pattern in patterns:
        if pattern:
            try:
                if re.search(pattern, message_text, re.IGNORECASE):
                    return True
            except re.error:
                print(f"⚠️ Некорректное регулярное выражение: {pattern}")
    
    return False


async def send_match(bot, channel_username, message, message_text, set_name):
    """
    Отправляет совпавшее сообщение пользователю текстом.
    Возвращает True при успешной отправке, иначе False.

    Пересылка через bot.forward_messages убрана намеренно: бот не имеет доступа
    к entity канала, разрешённому пользовательским клиентом, поэтому она почти
    всегда падала и приводила к лишнему API-вызову на каждое совпадение.
    parse_mode не используется — текст канала произвольный и мог бы ломать
    markdown-разметку, поэтому отправляем как обычный текст.
    """
    retry_count = 0
    max_retries = 3

    while retry_count < max_retries:
        try:
            channel_name = channel_username.strip('@')

            text = f"📢 [{set_name}] Пост из {channel_username}\n\n"

            # Ссылка только для публичных каналов (не числовой -100... id)
            if not channel_username.startswith('-'):
                text += f"🔗 https://t.me/{channel_name}/{message.id}\n\n"

            if message_text:
                text += message_text[:3000]
                if len(message_text) > 3000:
                    text += "\n\n... (сообщение обрезано)"
            else:
                text += "(Сообщение без текста — возможно, только медиа)"

            await bot.send_message(YOUR_USER_ID, text)
            print(f"✅ [{set_name}] Отправлено: {channel_username} / {message.id}")
            await asyncio.sleep(1)
            return True

        except FloodWaitError as flood_error:
            wait_time = flood_error.seconds
            print(f"⏳ [{set_name}] FloodWait: ждём {wait_time} секунд...")
            await asyncio.sleep(wait_time)
            retry_count += 1

        except Exception as bot_error:
            print(f"❌ [{set_name}] Не удалось отправить {message.id}: {bot_error}")
            return False

    print(f"❌ [{set_name}] Исчерпаны попытки отправки {message.id}")
    return False


async def monitor_channel(client, bot, channel_username, keywords, exclude_keywords, patterns, processed_dict, set_name, errors):
    """
    Мониторит один канал и возвращает статистику
    """
    new_processed = 0
    forwarded = 0
    skipped_duplicates = 0

    try:
        # Получаем канал
        channel = await client.get_entity(channel_username)

        print(f"📡 [{set_name}] Проверяю канал: {channel_username}")

        # Получаем сообщения за заданный период (message.date — aware UTC)
        time_threshold = datetime.now(timezone.utc) - timedelta(hours=TIME_RANGE_HOURS)

        # Получаем сообщения с учетом глубины поиска
        async for message in client.iter_messages(channel, limit=SEARCH_DEPTH):
            # Пропускаем старые сообщения
            if message.date < time_threshold:
                break

            # Создаем уникальный ID для сообщения (канал + ID сообщения)
            unique_id = f"{channel_username}:{message.id}"

            # Пропускаем уже обработанные (проверка на дубли)
            if unique_id in processed_dict:
                skipped_duplicates += 1
                continue

            # Проверяем текст сообщения
            message_text = message.text or ""

            # Проверяем, нужно ли пересылать
            if should_forward_message(message_text, keywords, exclude_keywords, patterns):
                # Помечаем обработанным ТОЛЬКО после успешной отправки, иначе
                # временный сбой отправки приведёт к безвозвратной потере
                # совпавшего сообщения. При неудаче — ретрай на следующем прогоне.
                if await send_match(bot, channel_username, message, message_text, set_name):
                    processed_dict[unique_id] = datetime.now(timezone.utc).isoformat()
                    new_processed += 1
                    forwarded += 1
            else:
                # Несовпавшие помечаем сразу, чтобы не проверять их повторно
                processed_dict[unique_id] = datetime.now(timezone.utc).isoformat()
                new_processed += 1

        return new_processed, forwarded, skipped_duplicates

    except Exception as e:
        # Не шлём уведомление на каждый канал (спам) — накапливаем и шлём сводку
        print(f"❌ [{set_name}] Ошибка в канале {channel_username}: {e}")
        errors.append(f"[{set_name}] {channel_username}: {e}")
        return 0, 0, 0


async def process_monitor_set(client, bot, monitor_set, processed_dict, errors):
    """
    Обрабатывает один набор мониторинга
    """
    set_name = monitor_set['name']
    channels = monitor_set['channels']
    keywords = monitor_set['keywords']
    exclude_keywords = monitor_set['exclude']
    patterns = monitor_set['patterns']
    
    if not channels:
        print(f"⚠️ [{set_name}] Нет каналов для мониторинга")
        return 0, 0, 0
    
    if not keywords and not patterns:
        print(f"⚠️ [{set_name}] Нет ключевых слов или паттернов!")
        print(f"   Будут пересылаться ВСЕ сообщения (кроме исключений)")
    
    print(f"\n{'='*60}")
    print(f"🔍 Обработка набора: {set_name}")
    print(f"📺 Каналов: {len(channels)}")
    print(f"🔑 Ключевых слов: {len(keywords)}")
    print(f"🚫 Слов-исключений: {len(exclude_keywords)}")
    print(f"🔍 Регулярных выражений: {len(patterns)}")
    print(f"{'='*60}")
    
    total_new = 0
    total_forwarded = 0
    total_skipped = 0
    
    for channel in channels:
        new, forwarded, skipped = await monitor_channel(
            client,
            bot,
            channel,
            keywords,
            exclude_keywords,
            patterns,
            processed_dict,
            set_name,
            errors
        )
        total_new += new
        total_forwarded += forwarded
        total_skipped += skipped
        
        # Задержка между каналами
        await asyncio.sleep(2)
    
    print(f"\n📊 [{set_name}] Результаты:")
    print(f"   Обработано новых: {total_new}")
    print(f"   Переслано: {total_forwarded}")
    print(f"   Пропущено дублей: {total_skipped}")
    
    return total_new, total_forwarded, total_skipped


async def main():
    """Основная функция для мониторинга каналов"""
    
    # Проверка критичных переменных окружения
    required_vars = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'SESSION_STRING', 'BOT_TOKEN', 'YOUR_USER_ID']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print(f"❌ Отсутствуют обязательные переменные: {', '.join(missing)}")
        return

    # Парсим числовые переменные здесь (а не на импорте), чтобы некорректное
    # значение давало понятное сообщение, а не TypeError/ValueError в трейсбеке.
    global API_ID, API_HASH, SESSION_STRING, BOT_TOKEN, YOUR_USER_ID
    try:
        API_ID = int(os.getenv('TELEGRAM_API_ID'))
        YOUR_USER_ID = int(os.getenv('YOUR_USER_ID'))
    except (TypeError, ValueError):
        print("❌ TELEGRAM_API_ID и YOUR_USER_ID должны быть целыми числами")
        return
    API_HASH = os.getenv('TELEGRAM_API_HASH')
    SESSION_STRING = os.getenv('SESSION_STRING')
    BOT_TOKEN = os.getenv('BOT_TOKEN')

    # Проверка на параллельный запуск (только для Unix-систем)
    lock_file = None
    if sys.platform != 'win32':
        try:
            import fcntl
            lock_file = open(LOCK_FILE, 'w')
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print("🔒 Блокировка установлена")
        except IOError:
            print("❌ Скрипт уже запущен! Завершение.")
            return
        except ImportError:
            print("⚠️ fcntl недоступен, пропускаем блокировку")
    
    try:
        # Парсим наборы мониторинга
        monitor_sets = parse_monitor_sets()
        
        if not monitor_sets:
            print("❌ Не указаны наборы для мониторинга!")
            print("Добавьте MONITOR_SETS в JSON формате или используйте старые переменные")
            return
        
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
        
        print(f"🚀 Бот запущен: {datetime.now(timezone.utc).isoformat()}")
        print(f"📦 Всего наборов мониторинга: {len(monitor_sets)}")
        print(f"📊 Глубина поиска: {SEARCH_DEPTH} сообщений")
        print(f"⏱️ Временной диапазон: {TIME_RANGE_HOURS} часов")
        
        # Загружаем уже обработанные сообщения
        processed_dict = load_processed_messages()
        initial_processed_count = len(processed_dict)
        print(f"💾 Загружено обработанных сообщений: {initial_processed_count}")
        
        total_new = 0
        total_forwarded = 0
        total_skipped = 0
        errors = []

        try:
            # Обрабатываем каждый набор последовательно
            for monitor_set in monitor_sets:
                new, forwarded, skipped = await process_monitor_set(
                    client,
                    bot,
                    monitor_set,
                    processed_dict,
                    errors
                )
                total_new += new
                total_forwarded += forwarded
                total_skipped += skipped

            print(f"\n{'='*60}")
            print(f"✨ ИТОГО по всем наборам:")
            print(f"   Обработано новых: {total_new}")
            print(f"   Переслано: {total_forwarded}")
            print(f"   Пропущено дублей: {total_skipped}")
            print(f"{'='*60}")

            # Единая сводка об ошибках каналов вместо уведомления на каждый канал
            if errors:
                summary = "⚠️ Ошибки при обработке каналов:\n\n" + "\n".join(
                    f"• {e}" for e in errors
                )
                try:
                    await bot.send_message(YOUR_USER_ID, summary[:4000])
                except Exception as notify_err:
                    print(f"❌ Не удалось отправить сводку ошибок: {notify_err}")

        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            try:
                await bot.send_message(YOUR_USER_ID, f"⚠️ Критическая ошибка:\n{e}")
            except Exception:
                pass

        finally:
            # Сохраняем состояние В ЛЮБОМ случае, иначе при ошибке в середине
            # прогона отметки «обработано» теряются и дубли шлются повторно.
            removed_count = save_processed_messages(processed_dict)
            print(f"🧹 Удалено старых записей: {removed_count}")
            await client.disconnect()
            await bot.disconnect()
    
    finally:
        # Освобождаем блокировку
        if lock_file:
            try:
                import fcntl
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
                os.remove(LOCK_FILE)
                print("🔓 Блокировка снята")
            except Exception:
                pass


if __name__ == '__main__':
    asyncio.run(main())
