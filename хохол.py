import asyncio
import logging
import os
import re
import sqlite3
import time
from collections import deque

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("vz_bot.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        balance INTEGER DEFAULT 0,
        total_vz INTEGER DEFAULT 0,
        vip_until INTEGER DEFAULT 0,
        warns INTEGER DEFAULT 0,
        mute_until INTEGER DEFAULT 0
    )
"""
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT DEFAULT ''
    )
"""
)

cursor.execute(
    "INSERT OR IGNORE INTO settings (key, value) VALUES ('required_channels', '')"
)

conn.commit()

# =========================
# CONFIG
# =========================

WARN_LIMIT = 3
MUTE_TIME = 3600
REWARD = 500
VIP_COST = 500
VIP_TIME = 86400

# =========================
# MEMORY
# =========================

search_queue = deque()
sessions = {}
ready_users = set()
user_links = {}
display_links = {}

# =========================
# STATES
# =========================

class VZState(StatesGroup):
    waiting_channel = State()
    searching = State()
    in_session = State()

# =========================
# KEYBOARDS
# =========================

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Начать ВЗ")],
        [KeyboardButton(text="Профиль"), KeyboardButton(text="Подписка")],
    ],
    resize_keyboard=True,
)

session_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✅ Готово")],
        [KeyboardButton(text="❌ Отмена")],
    ],
    resize_keyboard=True,
)

# =========================
# DATABASE FUNCTIONS
# =========================


def get_user(user_id: int):
    cursor.execute(
        "SELECT balance, total_vz, vip_until, warns, mute_until FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()

    if not row:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return (0, 0, 0, 0, 0)

    return row


def add_balance(user_id: int, amount: int):
    cursor.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (amount, user_id),
    )
    conn.commit()


def add_vz(user_id: int):
    cursor.execute(
        "UPDATE users SET total_vz = total_vz + 1 WHERE user_id = ?",
        (user_id,),
    )
    conn.commit()


def add_warn(user_id: int):
    cursor.execute(
        "UPDATE users SET warns = warns + 1 WHERE user_id = ?",
        (user_id,),
    )
    conn.commit()

    cursor.execute("SELECT warns FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()[0]


def clear_warns(user_id: int):
    cursor.execute(
        "UPDATE users SET warns = 0 WHERE user_id = ?",
        (user_id,),
    )
    conn.commit()


def set_mute(user_id: int, seconds: int):
    mute_until = int(time.time()) + seconds

    cursor.execute(
        "UPDATE users SET mute_until = ? WHERE user_id = ?",
        (mute_until, user_id),
    )
    conn.commit()


def get_mute_left(user_id: int):
    cursor.execute(
        "SELECT mute_until FROM users WHERE user_id = ?",
        (user_id,),
    )

    row = cursor.fetchone()

    if not row:
        return 0

    return max(0, row[0] - int(time.time()))


def activate_vip(user_id: int):
    now = int(time.time())

    cursor.execute(
        "SELECT vip_until FROM users WHERE user_id = ?",
        (user_id,),
    )

    current = cursor.fetchone()[0]

    base = max(now, current)

    cursor.execute(
        "UPDATE users SET vip_until = ? WHERE user_id = ?",
        (base + VIP_TIME, user_id),
    )

    conn.commit()

# =========================
# CHANNELS
# =========================


def load_required_channels():
    cursor.execute(
        "SELECT value FROM settings WHERE key = 'required_channels'"
    )

    row = cursor.fetchone()

    if not row or not row[0]:
        return []

    return [x.strip() for x in row[0].split(",") if x.strip()]


required_channels = load_required_channels()

# =========================
# HELPERS
# =========================


async def safe_send(user_id: int, text: str, **kwargs):
    try:
        await bot.send_message(user_id, text, **kwargs)
        return True
    except Exception:
        return False


async def get_bot_id():
    me = await bot.get_me()
    return me.id


async def check_bot_admin(chat_id):
    try:
        bot_id = await get_bot_id()
        member = await bot.get_chat_member(chat_id, bot_id)
        return member.status in ("administrator", "creator")
    except:
        return False


def normalize_channel(text: str):
    text = text.strip()

    if text.startswith("@"):
        return text

    if "t.me/" in text:
        username = text.split("t.me/")[-1].replace("/", "")
        return f"@{username}"

    if not text.startswith("@"):
        return f"@{text}"

    return text


async def is_user_subscribed(user_id: int, channel: str):
    try:
        member = await bot.get_chat_member(channel, user_id)
        return member.status not in ("left", "kicked")
    except:
        return False

# =========================
# SUB CHECK
# =========================


async def require_subscription(message: Message):
    if not required_channels:
        return True

    missing = []

    for channel in required_channels:
        if not await is_user_subscribed(message.from_user.id, channel):
            missing.append(channel)

    if not missing:
        return True

    buttons = []

    for ch in missing:
        clean = ch.replace("@", "")

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"Подписаться @{clean}",
                    url=f"https://t.me/{clean}",
                )
            ]
        )

    buttons.append(
        [InlineKeyboardButton(text="✅ Проверить", callback_data="check_sub")]
    )

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await message.answer(
        "⚠️ Подпишитесь на каналы для использования бота.",
        reply_markup=kb,
    )

    return False

# =========================
# START
# =========================


@dp.message(Command("start"))
async def start(message: Message, state: FSMContext):
    await state.clear()

    get_user(message.from_user.id)

    if not await require_subscription(message):
        return

    await message.answer(
        "👋 Добро пожаловать в VZ систему. Используйте кнопки ниже.",
        reply_markup=main_kb,
    )

# =========================
# PROFILE
# =========================


@dp.message(F.text == "Профиль")
async def profile(message: Message):
    if not await require_subscription(message):
        return

    balance, total_vz, vip_until, warns, mute_until = get_user(
        message.from_user.id
    )

    vip = "❌ Нет"

    if vip_until > int(time.time()):
        left = vip_until - int(time.time())
        vip = f"🔥 Активен ({left // 3600}ч)"

    mute_text = "❌ Нет"

    if mute_until > int(time.time()):
        mute_text = f"⏳ {get_mute_left(message.from_user.id) // 60} мин"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Купить VIP за 500 очков",
                    callback_data="buy_vip",
                )
            ]
        ]
    )

    await message.answer(
        f"👤 Ваш профиль\n\n"
        f"💰 Очки: {balance}\n"
        f"🤝 ВЗ: {total_vz}\n"
        f"⚠️ Предупреждения: {warns}/{WARN_LIMIT}\n"
        f"🔇 Мут: {mute_text}\n"
        f"🌟 VIP: {vip}",
        reply_markup=kb,
    )

# =========================
# VIP
# =========================


@dp.callback_query(F.data == "buy_vip")
async def buy_vip(callback: CallbackQuery):
    balance, *_ = get_user(callback.from_user.id)

    if balance < VIP_COST:
        await callback.answer("Недостаточно очков", show_alert=True)
        return

    add_balance(callback.from_user.id, -VIP_COST)
    activate_vip(callback.from_user.id)

    await callback.message.edit_text(
        "🔥 VIP активирован на 24 часа"
    )

# =========================
# START VZ
# =========================


@dp.message(F.text == "Начать ВЗ")
async def start_vz(message: Message, state: FSMContext):
    if not await require_subscription(message):
        return

    mute_left = get_mute_left(message.from_user.id)

    if mute_left > 0:
        await message.answer(
            f"❌ У вас мут. Осталось {mute_left // 60} мин"
        )
        return

    await message.answer(
        "📢 Отправьте @username канала или ссылку.\n\n"
        "Также добавьте бота в администраторы и разместите рекламу нашего чата.",
        reply_markup=session_kb,
    )

    await state.set_state(VZState.waiting_channel)

# =========================
# CHANNEL INPUT
# =========================


@dp.message(VZState.waiting_channel)
async def process_channel(message: Message, state: FSMContext):
    text = message.text.strip()

    if text == "❌ Отмена":
        await state.clear()

        await message.answer(
            "❌ Отменено",
            reply_markup=main_kb,
        )
        return

    channel = normalize_channel(text)

    if not await check_bot_admin(channel):
        await message.answer(
            "❌ Бот не является администратором канала"
        )
        return

    user_links[message.from_user.id] = channel
    display_links[message.from_user.id] = channel

    if message.from_user.id not in search_queue:
        search_queue.append(message.from_user.id)

    await state.set_state(VZState.searching)

    await message.answer(
        "🔍 Поиск партнёра...",
        reply_markup=session_kb,
    )

    await check_queue()

# =========================
# QUEUE
# =========================


async def check_queue():
    while len(search_queue) >= 2:
        user1 = search_queue.popleft()
        user2 = search_queue.popleft()

        sessions[user1] = user2
        sessions[user2] = user1

        ctx1 = dp.fsm.resolve_context(bot, chat_id=user1, user_id=user1)
        ctx2 = dp.fsm.resolve_context(bot, chat_id=user2, user_id=user2)

        await ctx1.set_state(VZState.in_session)
        await ctx2.set_state(VZState.in_session)

        await safe_send(
            user1,
            f"✅ Найден партнёр\n\nПодпишитесь: {display_links[user2]}",
            reply_markup=session_kb,
        )

        await safe_send(
            user2,
            f"✅ Найден партнёр\n\nПодпишитесь: {display_links[user1]}",
            reply_markup=session_kb,
        )

# =========================
# READY
# =========================


@dp.message(VZState.in_session, F.text == "✅ Готово")
async def ready(message: Message):
    user_id = message.from_user.id
    partner = sessions.get(user_id)

    if not partner:
        return

    target_channel = user_links.get(partner)

    if not await is_user_subscribed(user_id, target_channel):
        await message.answer(
            "❌ Подписка не найдена"
        )
        return

    ready_users.add(user_id)

    if partner in ready_users:
        await finish_session(user_id, partner)
    else:
        await message.answer(
            "⏳ Ожидаем второго пользователя"
        )

# =========================
# FINISH SESSION
# =========================


async def finish_session(user1: int, user2: int):
    ready_users.discard(user1)
    ready_users.discard(user2)

    sessions.pop(user1, None)
    sessions.pop(user2, None)

    add_balance(user1, REWARD)
    add_balance(user2, REWARD)

    add_vz(user1)
    add_vz(user2)

    clear_warns(user1)
    clear_warns(user2)

    ctx1 = dp.fsm.resolve_context(bot, chat_id=user1, user_id=user1)
    ctx2 = dp.fsm.resolve_context(bot, chat_id=user2, user_id=user2)

    await ctx1.clear()
    await ctx2.clear()

    await safe_send(
        user1,
        f"🎉 ВЗ завершено. +{REWARD} очков",
        reply_markup=main_kb,
    )

    await safe_send(
        user2,
        f"🎉 ВЗ завершено. +{REWARD} очков",
        reply_markup=main_kb,
    )

# =========================
# CANCEL
# =========================


@dp.message(F.text == "❌ Отмена")
async def cancel(message: Message, state: FSMContext):
    user_id = message.from_user.id

    current = await state.get_state()

    if current == VZState.searching:
        try:
            search_queue.remove(user_id)
        except:
            pass

        await state.clear()

        await message.answer(
            "❌ Поиск отменён",
            reply_markup=main_kb,
        )

        return

    if current == VZState.in_session:
        partner = sessions.get(user_id)

        warns = add_warn(user_id)

        sessions.pop(user_id, None)
        ready_users.discard(user_id)

        await state.clear()

        if warns >= WARN_LIMIT:
            set_mute(user_id, MUTE_TIME)
            clear_warns(user_id)

            await message.answer(
                "🔇 Вы получили мут на 1 час за нарушения.",
                reply_markup=main_kb,
            )
        else:
            await message.answer(
                f"⚠️ Предупреждение {warns}/{WARN_LIMIT}",
                reply_markup=main_kb,
            )

        if partner:
            sessions.pop(partner, None)
            ready_users.discard(partner)

            ctx = dp.fsm.resolve_context(
                bot,
                chat_id=partner,
                user_id=partner,
            )

            await ctx.clear()

            await safe_send(
                partner,
                "⚠️ Партнёр покинул сессию",
                reply_markup=main_kb,
            )

# =========================
# CHECK SUB
# =========================


@dp.callback_query(F.data == "check_sub")
async def check_sub(callback: CallbackQuery):
    ok = True

    for channel in required_channels:
        if not await is_user_subscribed(callback.from_user.id, channel):
            ok = False
            break

    if not ok:
        await callback.answer(
            "❌ Подписка не найдена",
            show_alert=True,
        )
        return

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    await bot.send_message(
        callback.from_user.id,
        "✅ Доступ открыт",
        reply_markup=main_kb,
    )

# =========================
# ADMIN
# =========================


@dp.message(Command("send"))
async def broadcast(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    text = message.text.replace("/send", "").strip()

    cursor.execute("SELECT user_id FROM users")
    users = [x[0] for x in cursor.fetchall()]

    success = 0

    for uid in users:
        if await safe_send(uid, text):
            success += 1

        await asyncio.sleep(0.03)

    await message.answer(
        f"✅ Отправлено: {success}"
    )

# =========================
# FALLBACK
# =========================


@dp.message()
async def fallback(message: Message):
    await message.answer(
        "⏳ Используйте кнопки меню",
        reply_markup=main_kb,
    )

# =========================
# MAIN
# =========================


async def main():
    logger.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())