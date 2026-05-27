import asyncio
import sqlite3
import logging
import aiohttp
import re
import time
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN        = "8902414270:AAHKEygVD18jir32ebWTuIJFMhAnkFAjQsU"
CRYPTO_PAY_TOKEN = "586296:AA1tejqFx3eBIFPbqugJyXZwWsTVpSmvn5D"
ADMIN_ID         = 8325037674

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

conn   = sqlite3.connect("vz_bot.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        balance    INTEGER DEFAULT 0,
        total_vz   INTEGER DEFAULT 0,
        vip_until  INTEGER DEFAULT 0,
        is_banned  INTEGER DEFAULT 0,
        warns      INTEGER DEFAULT 0
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS penalty (
        user_id    INTEGER PRIMARY KEY,
        until      INTEGER DEFAULT 0
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT DEFAULT ''
    )
""")
cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('required_channels', '')")
conn.commit()


def load_required_channels() -> list[str]:
    cursor.execute("SELECT value FROM settings WHERE key = 'required_channels'")
    row = cursor.fetchone()
    if row and row[0]:
        return [ch.strip() for ch in row[0].split(",") if ch.strip()]
    return []


def save_required_channels(channels: list[str]) -> None:
    cursor.execute("UPDATE settings SET value = ? WHERE key = 'required_channels'", (",".join(channels),))
    conn.commit()


required_channels: list[str] = load_required_channels()


def get_user(user_id: int) -> tuple[int, int, int, int, int]:
    cursor.execute("SELECT balance, total_vz, vip_until, is_banned, warns FROM users WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()
    if not res:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return (0, 0, 0, 0, 0)
    return res


def get_all_users() -> list[int]:
    cursor.execute("SELECT user_id FROM users")
    return [row[0] for row in cursor.fetchall()]


def add_vz_stat(user_id: int) -> None:
    cursor.execute("UPDATE users SET total_vz = total_vz + 1 WHERE user_id = ?", (user_id,))
    conn.commit()


def add_balance(user_id: int, amount: int) -> None:
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()


def set_user_ban(user_id: int, status: int) -> None:
    cursor.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (status, user_id))
    conn.commit()


def add_warn(user_id: int) -> int:
    cursor.execute("UPDATE users SET warns = warns + 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    cursor.execute("SELECT warns FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()[0]


def reset_warns(user_id: int) -> None:
    cursor.execute("UPDATE users SET warns = 0 WHERE user_id = ?", (user_id,))
    conn.commit()


def set_vip_24h(user_id: int) -> None:
    now = int(time.time())
    _, _, current_vip, _, _ = get_user(user_id)
    base_time = max(now, current_vip)
    new_until = base_time + 86400
    cursor.execute("UPDATE users SET vip_until = ? WHERE user_id = ?", (new_until, user_id))
    conn.commit()


def set_penalty(user_id: int, seconds: int = 600) -> None:
    until = int(time.time()) + seconds
    cursor.execute("INSERT OR REPLACE INTO penalty (user_id, until) VALUES (?, ?)", (user_id, until))
    conn.commit()


def get_penalty_left(user_id: int) -> int:
    cursor.execute("SELECT until FROM penalty WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        left = row[0] - int(time.time())
        return max(0, left)
    return 0


search_queue:  list[int]             = []
vip_queue:     list[tuple[int, str]] = []
pending_vip:   dict[int, str]        = {}
sessions:      dict[int, int]        = {}
user_links:    dict[int, str]        = {}  
display_links: dict[int, str]        = {}  
ready_status:  set[int]              = set()


main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Начать вз")],
        [KeyboardButton(text="Профиль"), KeyboardButton(text="Подписка")]
    ],
    resize_keyboard=True,
    is_persistent=True  
)

ready_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✅ Готово")],
        [KeyboardButton(text="❌ Отмена")]  
    ],
    resize_keyboard=True,
    is_persistent=True
)


def subscription_kb(channels: list[str]) -> InlineKeyboardMarkup:
    buttons = []
    for ch in channels:
        if ":" in ch and (ch.startswith("-100") or ch.split(":", 1)[0].isdigit()):
            chat_id, link = ch.split(":", 1)
            buttons.append([InlineKeyboardButton(text="📢 Подписаться", url=link.strip())])
        else:
            clean_ch = ch.replace("https://t.me/", "").replace("http://t.me/", "").lstrip("@")
            buttons.append([InlineKeyboardButton(text=f"📢 @{clean_ch}", url=f"https://t.me/{clean_ch}")])
    buttons.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


class VZState(StatesGroup):
    waiting_for_link = State()
    searching        = State()
    in_session       = State()


async def check_bot_admin_and_get_link(chat_id_raw: str) -> str | None:
    chat_id = chat_id_raw.strip()
    if chat_id.startswith("-100") or chat_id.isdigit():
        try: chat_id = int(chat_id)
        except ValueError: return None
    try:
        bot_id = (await bot.get_me()).id
        member = await bot.get_chat_member(chat_id=chat_id, user_id=bot_id)
        if member.status not in ("administrator", "creator"):
            return None
        
        if member.can_invite_users or member.status == "creator":
            try:
                invite_link_obj = await bot.create_chat_invite_link(chat_id=chat_id, name="ВЗ Бот Ссылка")
                return invite_link_obj.invite_link
            except TelegramAPIError:
                pass
                
        chat = await bot.get_chat(chat_id=chat_id)
        if chat.username:
            return f"https://t.me/{chat.username}"
            
        return "Ссылка недоступна (выдайте боту право 'Пригласительные ссылки')"
    except TelegramAPIError:
        return None


async def is_user_subscribed(user_id: int, chat_id_raw: str) -> bool:
    chat_id = chat_id_raw.strip()
    if chat_id.startswith("-100") or chat_id.isdigit():
        try: chat_id = int(chat_id)
        except ValueError: return False
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status not in ("left", "kicked", "banned")
    except TelegramAPIError as e:
        logger.error(f"Error checking channel {chat_id}: {e}")
        try:
            await bot.send_message(
                ADMIN_ID, 
                f"🚨 <b>БОТ НЕ АДМИН В КАНАЛЕ!</b>\nКанал: <code>{chat_id}</code>\n"
                f"Все проверки ОП остановлены до восстановления прав!",
                parse_mode="HTML"
            )
        except Exception: pass
        return False


async def check_subscriptions(user_id: int) -> list[str]:
    not_subscribed = []
    for channel in required_channels:
        target = channel.split(":", 1)[0].strip() if ":" in channel else channel
        if not await is_user_subscribed(user_id, target):
            not_subscribed.append(channel)
    return not_subscribed


async def require_subscription(message: Message) -> bool:
    if not required_channels: return True
    missing = await check_subscriptions(message.from_user.id)
    if missing:
        await message.answer(
            "⚠️ <b>Для использования бота необходимо подписаться на каналы!</b>\n"
            "<i>Если вы подписаны, подождите минуту (администратор обновляет права бота).</i>",
            parse_mode="HTML", reply_markup=subscription_kb(missing)
        )
        return False
    return True


CRYPTO_API = "https://pay.crypt.bot/api"

async def create_crypto_invoice(amount_usd: float) -> tuple[str | None, int | None]:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {"asset": "USDT", "amount": f"{amount_usd:.2f}", "description": "ВЗ+ Безлимит на 24 часа"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_API}/createInvoice", json=payload, headers=headers, timeout=10) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data["result"]["bot_invoice_url"], data["result"]["invoice_id"]
    except Exception as e: logger.error(f"CryptoBot error: {e}")
    return None, None


async def check_crypto_invoice(invoice_id: int) -> bool:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CRYPTO_API}/getInvoices", params={"invoice_ids": str(invoice_id)}, headers=headers, timeout=10) as resp:
                data = await resp.json()
                if data.get("ok") and data["result"].get("items"):
                    return data["result"]["items"][0]["status"] == "paid"
    except Exception as e: logger.error(f"CryptoBot check error: {e}")
    return False


async def safe_send(user_id: int, text: str, **kwargs) -> bool:
    try: await bot.send_message(user_id, text, **kwargs); return True
    except TelegramAPIError: return False


async def finish_session(user_id: int, partner_id: int) -> None:
    ready_status.discard(user_id)
    ready_status.discard(partner_id)
    sessions.pop(user_id, None)
    sessions.pop(partner_id, None)

    for uid in (user_id, partner_id):
        add_vz_stat(uid)
        add_balance(uid, 1)
        uid_ctx = dp.fsm.resolve_context(bot, chat_id=uid, user_id=uid)
        await uid_ctx.clear()

        await safe_send(uid, "🎉 Взаимная подписка успешно завершена! Вам начислен +1 балл.", reply_markup=main_kb)
        
        _, _, vip_until, _, _ = get_user(uid)
        if vip_until > int(time.time()) and uid in user_links:
            vip_queue.append((uid, f"{user_links[uid]}|{display_links.get(uid, '')}"))


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    _, _, _, is_banned, _ = get_user(user_id)
    if is_banned:
        await message.answer("❌ Вы заблокированы администратором.")
        return
        
    if await state.get_state() == VZState.in_session:
        await message.answer("⚠️ Вы находитесь в активной сессии! Завершите её или нажмите «❌ Отмена».", reply_markup=ready_kb)
        return
    await state.clear()
    if user_id in search_queue: search_queue.remove(user_id)
    if not await require_subscription(message): return
    await message.answer("👋 Добро пожаловать в систему взаимного продвижения!", reply_markup=main_kb)


@dp.message(F.text == "❌ Отмена")
async def cancel_vz_session(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    current_state = await state.get_state()

    if current_state in (VZState.searching, VZState.waiting_for_link):
        if user_id in search_queue:
            try: search_queue.remove(user_id)
            except ValueError: pass
        await state.clear()
        await message.answer("❌ Поиск партнера отменен.", reply_markup=main_kb)
        return

    if current_state == VZState.in_session:
        partner_id = sessions.get(user_id)
        
        if partner_id in ready_status and user_id not in ready_status:
            current_warns = add_warn(user_id)
            
            ready_status.discard(user_id)
            sessions.pop(user_id, None)
            await state.clear()
            
            if current_warns >= 3:
                set_penalty(user_id, 1200)
                set_user_ban(user_id, 1)
                reset_warns(user_id)
                await message.answer(
                    "🚨 <b>ВНИМАНИЕ! ВЫ ЗАБЛОКИРОВАНЫ СИСТЕМОЙ!</b>\n\n"
                    "❌ Причина: Получено 3/3 варнов за срыв сессий.\n"
                    "⏱️ Ограничение на поиск: <b>20 минут</b>.",
                    parse_mode="HTML", reply_markup=main_kb
                )
            else:
                await message.answer(
                    f"⚠️ <b>Предупреждение за срыв сессии!</b>\n"
                    f"Партнер уже выполнил подписку, а вы покинули сессию.\n"
                    f"📊 Ваши варны: <b>{current_warns}/3</b>. При достижении 3 варнов последует бан.",
                    parse_mode="HTML", reply_markup=main_kb
                )

            if partner_id:
                ready_status.discard(partner_id)
                sessions.pop(partner_id, None)
                partner_ctx = dp.fsm.resolve_context(bot, chat_id=partner_id, user_id=partner_id)
                await partner_ctx.clear()
                await safe_send(
                    partner_id, 
                    "⚠️ Партнёр досрочно разорвал сессию. Ему начислено предупреждение.\n"
                    "Вы возвращены в начало очереди!", 
                    reply_markup=main_kb
                )
                if partner_id in user_links:
                    p_state = dp.fsm.resolve_context(bot, chat_id=partner_id, user_id=partner_id)
                    await p_state.set_state(VZState.searching)
                    if partner_id not in search_queue: search_queue.append(partner_id)
                    await check_queue()
            return

        ready_status.discard(user_id)
        sessions.pop(user_id, None)
        await state.clear()
        await message.answer("❌ Сессия отменена без штрафа. Ни один из участников не подтвердил подписку.", reply_markup=main_kb)

        if partner_id:
            ready_status.discard(partner_id)
            sessions.pop(partner_id, None)
            partner_ctx = dp.fsm.resolve_context(bot, chat_id=partner_id, user_id=partner_id)
            await partner_ctx.clear()
            await safe_send(partner_id, "❌ Партнёр отменил сессию. Очередь сброшена.", reply_markup=main_kb)
        return
    await message.answer("У вас нет активных сессий.", reply_markup=main_kb)


@dp.message(F.text == "Профиль")
async def show_profile(message: Message) -> None:
    if not await require_subscription(message): return
    user_id = message.from_user.id
    balance, total_vz, vip_until, is_banned, warns = get_user(user_id)
    
    if is_banned and get_penalty_left(user_id) == 0:
        set_user_ban(user_id, 0)
        is_banned = 0

    if vip_until > int(time.time()):
        t_left = vip_until - int(time.time())
        vip_status = f"🔥 Активен (Осталось: {t_left // 3600}ч {(t_left % 3600) // 60}м)"
    else: vip_status = "❌ Не активен"

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if balance >= 5000:
        kb.inline_keyboard.append([InlineKeyboardButton(text="🔄 Обменять 5000 баллов на ВЗ+ (24ч)", callback_data="buy_vip_points")])

    await message.answer(
        f"👤 <b>Ваш личный профиль:</b>\n\n"
        f"📊 Баланс баллов: <b>{balance}</b>\n"
        f"🤝 Выполнено ВЗ: {total_vz}\n"
        f"⚠️ Предупреждения (Варны): <b>{warns}/3</b>\n"
        f"🌟 Статус ВЗ+: {vip_status}\n\n"
        f"<i>💡 Накопите 5000 баллов через честные ВЗ и обменяйте их на 24 часа безлимита ВЗ+!</i>",
        parse_mode="HTML", reply_markup=kb if balance >= 5000 else main_kb
    )


@dp.callback_query(F.data == "buy_vip_points")
async def buy_vip_points_callback(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    balance, _, _, is_banned, _ = get_user(user_id)
    if is_banned:
        await callback.answer("❌ Вы заблокированы за срыв сессий!", show_alert=True)
        return
    if balance < 5000:
        await callback.answer("❌ Недостаточно баллов для обмена!", show_alert=True)
        return
    
    add_balance(user_id, -5000)
    set_vip_24h(user_id)
    await callback.message.edit_text("🎉 Тариф ВЗ+ активирован на 24 часа! Успешного продвижения!")


@dp.message(F.text == "Подписка")
async def show_subscription(message: Message) -> None:
    if not await require_subscription(message): return
    pay_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Купить ВЗ+ на 24 часа ($1.50 USDT)", callback_data="pay_crypto")]
    ])
    await message.answer(
        "💎 <b>Тариф ВЗ+ (БЕЗЛИМИТНЫЙ ДОСТУП НА 24 ЧАСА)</b>\n\n"
        "Вы получаете полный приоритет в общей очереди на 24 часа! Система продвигает ваши каналы в первую очередь.\n\n"
        "<b>Стоимость:</b> $1.50 USDT или 5000 баллов.",
        parse_mode="HTML", reply_markup=pay_kb
    )


@dp.callback_query(F.data == "pay_crypto")
async def process_pay_crypto(callback: CallbackQuery) -> None:
    await callback.answer()
    url, invoice_id = await create_crypto_invoice(1.50)
    if not url or invoice_id is None:
        await callback.message.answer("❌ На стороне платежного сервиса произошла ошибка.")
        return
    check_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Перейти к оплате CryptoBot", url=url)],
        [InlineKeyboardButton(text="🔄 Проверить зачисление", callback_data=f"check_{invoice_id}")]
    ])
    await callback.message.answer("🏷️ Счет успешно создан. Произведите оплату и подтвердите ее ниже.", reply_markup=check_kb)


@dp.callback_query(F.data.startswith("check_") & ~F.data.startswith("check_sub"))
async def verify_crypto_payment(callback: CallbackQuery) -> None:
    parts = callback.data.split("_")
    if len(parts) != 2 or not parts[1].isdigit(): return
    invoice_id = int(parts[1])
    if await check_crypto_invoice(invoice_id):
        set_vip_24h(callback.from_user.id)
        await callback.message.edit_text("🎉 Оплата получена! Безлимитный режим ВЗ+ предоставлен на 24 часа.")
    else:
        await callback.answer("❌ Оплата не обнаружена.", show_alert=True)


@dp.message(F.text == "Начать вз")
async def start_vz(message: Message, state: FSMContext) -> None:
    if not await require_subscription(message): return
    user_id = message.from_user.id
    
    penalty_left = get_penalty_left(user_id)
    if penalty_left > 0:
        await message.answer(
            f"❌ <b>ДОСТУП ЗАБЛОКИРОВАН!</b>\nВы нарушили правила сервиса.\n"
            f"Блокировка снимется автоматически через: <b>{penalty_left // 60} мин. {penalty_left % 60} сек.</b>",
            parse_mode="HTML"
        )
        return
    else:
        _, _, _, is_banned, _ = get_user(user_id)
        if is_banned:
            set_user_ban(user_id, 0)

    if await state.get_state() in (VZState.searching, VZState.in_session):
        await message.answer("⚠️ Вы уже находитесь в режиме взаимодействия!", reply_markup=ready_kb)
        return

    if user_id in pending_vip:
        vip_data = pending_vip[user_id]
        vip_target, vip_disp = vip_data.split("|")[0], vip_data.split("|")[1] if "|" in vip_data else vip_data
        if await is_user_subscribed(user_id, vip_target): pending_vip.pop(user_id, None)
        else:
            await message.answer(f"⚠️ Для продолжения подпишитесь на премиум-канал партнера:\n{vip_disp}")
            return

    global vip_queue
    vip_queue = [(vid, vdata) for vid, vdata in vip_queue if get_user(vid)[2] > int(time.time())]

    if vip_queue:
        vip_index = next((i for i, (vid, _) in enumerate(vip_queue) if vid != user_id), None)
        if vip_index is not None:
            _, raw_vip_data = vip_queue.pop(vip_index)
            vip_target, vip_disp = raw_vip_data.split("|")[0], raw_vip_data.split("|")[1] if "|" in raw_vip_data else raw_vip_data
            if not await is_user_subscribed(user_id, vip_target):
                pending_vip[user_id] = raw_vip_data
                await message.answer(f"🌟 Обязательное задание ВЗ+: подпишитесь на канал:\n{vip_disp}\n\nПосле этого нажмите «Начать вз» повторно.")
                return

    bot_info = await bot.get_me()
    deep_link = f"https://t.me/{bot_info.username}?startchannel=true&admin=post_messages+edit_messages+delete_messages+invite_users"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить бота в канал", url=deep_link)],
        [InlineKeyboardButton(text="Проверить привязку", callback_data="verify_bot_added")]
    ])

    await message.answer(
        "📢 <b>Привязка канала без ссылок!</b>\n\n"
        "Чтобы запустить продвижение, вам больше не нужно отправлять ссылки вручную.\n"
        "Просто нажмите кнопку ниже, выберите свой канал и подтвердите его добавление в качестве администратора с базовыми правами.\n\n"
        "<i>Бот автоматически получит ID канала и сгенерирует безопасную инвайт-ссылку для вашего будущего партнера!</i>",
        parse_mode="HTML", reply_markup=kb
    )


@dp.callback_query(F.data == "verify_bot_added")
async def verify_bot_added_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    
    bot_id = (await bot.get_me()).id
    user_id = callback.from_user.id
    
    await callback.message.edit_text("🔄 Сканируем каналы, где вы являетесь владельцем/администратором и где добавлен бот...")
    
    found_chat_id = None
    generated_link = None
    
    # Так как Telegram API не предоставляет прямого метода "получить список каналов бота", 
    # мы проверяем кэш активных сессий или заставляем пользователя переслать пост, если автоматика не сработала.
    # Оптимальный UX: просим пользователя просто переслать любой пост из своего канала боту.
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )
    await callback.message.answer(
        "📝 <b>Последний шаг:</b>\n"
        "Перешлите в этот диалог <b>любой пост (сообщение)</b> из вашего канала, который вы только что привязали.\n\n"
        "<i>Это нужно, чтобы бот мгновенно узнал ID вашего канала через API.</i>",
        parse_mode="HTML", reply_markup=kb
    )
    await state.set_state(VZState.waiting_for_link)


@dp.message(VZState.waiting_for_link)
async def process_forwarded_msg(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Привязка канала отменена.", reply_markup=main_kb)
        return

    target_chat = None
    if message.forward_from_chat:
        target_chat = str(message.forward_from_chat.id)
    else:
        # Если пользователь отправил обычный текст вместо форварда
        raw_text = message.text.strip() if message.text else ""
        if (raw_text.startswith("-100") or raw_text.isdigit()):
            target_chat = raw_text
        else:
            match = re.search(r"(?:t|telegram)\.me\/([a-zA-Z0-9_]{5,})", raw_text)
            if match:
                target_chat = f"@{match.group(1)}"
            elif not raw_text.startswith("@") and "/" not in raw_text and len(raw_text) > 3:
                target_chat = f"@{raw_text}"

    if not target_chat:
        await message.answer("⚠️ Ошибка! Пожалуйста, именно <b>перешлите пост</b> из вашего канала или введите его ID/Юзернейм.")
        return

    invite_link = await check_bot_admin_and_get_link(target_chat)
    if not invite_link:
        await message.answer(
            f"❌ <b>Бот не найден в админах канала!</b>\n\n"
            f"Убедитесь, что вы добавили бота в канал <code>{target_chat}</code> "
            f"и выдали ему право на <b>'Пригласительные ссылки' (Invite users via link)</b>.\n\n"
            f"После этого перешлите пост ещё раз.",
            parse_mode="HTML"
        )
        return

    user_links[user_id] = target_chat
    display_links[user_id] = invite_link

    await message.answer("🔍 Канал успешно привязан! Подбираем для вас подходящего партнера...", reply_markup=ready_kb)
    await state.set_state(VZState.searching)
    if user_id not in search_queue: search_queue.append(user_id)
    await check_queue()


async def check_queue() -> None:
    while len(search_queue) >= 2:
        user1, user2 = search_queue.pop(0), search_queue.pop(0)
        sessions[user1], sessions[user2] = user2, user1

        for u, partner in ((user1, user2), (user2, user1)):
            u_ctx = dp.fsm.resolve_context(bot, chat_id=u, user_id=u)
            await u_ctx.set_state(VZState.in_session)
            
            p_chat = user_links.get(partner, "")
            members_text = ""
            if p_chat:
                if p_chat.startswith("-100") or p_chat.isdigit():
                    try: p_chat = int(p_chat)
                    except ValueError: pass
                try:
                    count = await bot.get_chat_member_count(chat_id=p_chat)
                    members_text = f"\n📊 Участников в канале партнера: <b>{count}</b>"
                except TelegramAPIError:
                    pass

            await safe_send(
                u, f"✅ <b>Партнер найден!</b>{members_text}\n\n👉 Ссылка для подписки: {display_links.get(partner, '—')}\n\nПосле выполнения подписки нажмите кнопку.",
                parse_mode="HTML", reply_markup=ready_kb
            )


@dp.message(VZState.in_session, F.text == "✅ Готово")
async def process_ready(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    partner_id = sessions.get(user_id)
    if not partner_id: await state.clear(); return

    if user_id in ready_status: return

    if not await is_user_subscribed(user_id, user_links.get(partner_id, "")):
        await message.answer(f"❌ Подписка не найдена! Выполните задание: {display_links.get(partner_id, '')}")
        return

    ready_status.add(user_id)
    if partner_id in ready_status: await finish_session(user_id, partner_id)
    else:
        await message.answer("⏳ Готовность подтверждена. Ожидаем ответа от партнера...", reply_markup=ready_kb)
        await safe_send(partner_id, "🔔 Партнер уже выполнил условия подписки и ожидает вас!")


@dp.message(VZState.in_session)
async def block_other_messages(message: Message) -> None:
    if message.from_user.id in ready_status: return
    await message.answer("⚠️ Вы находитесь в процессе взаимной подписки. Нажмите «✅ Готово» или «❌ Отмена».", reply_markup=ready_kb)


@dp.callback_query(F.data == "check_sub")
async def callback_check_sub(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if await check_subscriptions(callback.from_user.id):
        await callback.answer("❌ Подписка не найдена либо у бота отсутствуют права администратора в канале!", show_alert=True)
        return
    try: await callback.message.delete()
    except TelegramBadRequest: pass
    await bot.send_message(callback.from_user.id, "✅ Доступ разблокирован!", reply_markup=main_kb)


@dp.message(Command("send"))
async def admin_broadcast(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    text = message.text.removeprefix("/send").strip()
    if not text: return
    users = get_all_users()
    await message.answer(f"📢 Запуск рассылки на {len(users)} пользователей...")
    for uid in users:
        await safe_send(uid, text)
        await asyncio.sleep(0.05)
    await message.answer("✅ Рассылка завершена.")

@dp.message(Command("setchannel"))
async def admin_set_channel(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    arg = message.text.removeprefix("/setchannel").strip()
    if arg and arg not in required_channels:
        required_channels.append(arg); save_required_channels(required_channels)
        await message.answer("✅ Канал добавлен.")

@dp.message(Command("removechannel"))
async def admin_remove_channel(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    arg = message.text.removeprefix("/removechannel").strip()
    if arg in required_channels:
        required_channels.remove(arg); save_required_channels(required_channels)
        await message.answer("✅ Канал удален.")

@dp.message(Command("channels"))
async def admin_list_channels(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    await message.answer("📋 Список ОП каналов:\n" + "\n".join(f"• {ch}" for ch in required_channels))


@dp.message()
async def global_fallback_handler(message: Message, state: FSMContext) -> None:
    if await state.get_state() == VZState.in_session: return
    await state.clear()
    if not await require_subscription(message): return
    await message.answer("⏳ Пожалуйста, используйте кнопки встроенного меню.", reply_markup=main_kb)


async def main() -> None:
    logger.info("Starting bot process...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())