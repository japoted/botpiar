#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
██╗   ██╗███████╗    ██████╗  ██████╗ ████████╗
██║   ██║╚══███╔╝    ██╔══██╗██╔═══██╗╚══██╔══╝
██║   ██║  ███╔╝     ██████╔╝██║   ██║   ██║
╚██╗ ██╔╝ ███╔╝      ██╔══██╗██║   ██║   ██║
 ╚████╔╝ ███████╗    ██████╔╝╚██████╔╝   ██║
  ╚═══╝  ╚══════╝    ╚═════╝  ╚═════╝    ╚═╝

Telegram Bot — Система Взаимных Подписок (ВЗ)
Version: 2.0 | aiogram 3.x | Production Ready
"""

# ==============================================================
# SECTION 1: IMPORTS
# ==============================================================
import asyncio
import logging
import os
import re
import time
import json
import traceback
from collections import deque, defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Union

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
    TelegramAPIError,
    TelegramNotFound,
)
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    ChatMemberAdministrator,
    ChatMemberOwner,
    User,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from dotenv import load_dotenv

# ==============================================================
# SECTION 2: CONFIGURATION
# ==============================================================
load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
ADMIN_IDS: List[int] = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
CRYPTOBOT_TOKEN: str = os.getenv("CRYPTOBOT_TOKEN", "")
VIP_PRICE_CRYPTO: float = float(os.getenv("VIP_PRICE_CRYPTO", "1.0"))
VIP_PRICE_POINTS: int = int(os.getenv("VIP_PRICE_POINTS", "500"))
VIP_DURATION_HOURS: int = int(os.getenv("VIP_DURATION_HOURS", "24"))
POINTS_PER_VZ: int = int(os.getenv("POINTS_PER_VZ", "500"))
MAX_WARNS: int = int(os.getenv("MAX_WARNS", "3"))
MUTE_DURATION_MINUTES: int = int(os.getenv("MUTE_DURATION_MINUTES", "60"))
DB_PATH: str = os.getenv("DB_PATH", "vz_bot.db")
FLOOD_LIMIT: int = int(os.getenv("FLOOD_LIMIT", "3"))      # messages
FLOOD_WINDOW: int = int(os.getenv("FLOOD_WINDOW", "5"))    # seconds
SESSION_TIMEOUT: int = int(os.getenv("SESSION_TIMEOUT", "300"))  # seconds

CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"

REQUIRED_PERMISSIONS = [
    "can_manage_chat",
    "can_delete_messages",
    "can_invite_users",
    "can_restrict_members",
    "can_post_messages",
    "can_edit_messages",
]

PERMISSION_LABELS: Dict[str, str] = {
    "can_manage_chat":      "🔧 Управление чатом",
    "can_delete_messages":  "🗑 Удаление сообщений",
    "can_invite_users":     "📨 Приглашение участников",
    "can_restrict_members": "🚫 Ограничение участников",
    "can_post_messages":    "📢 Публикация сообщений",
    "can_edit_messages":    "✏️ Редактирование сообщений",
}

# ==============================================================
# SECTION 3: LOGGING
# ==============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("VZ_BOT")

# ==============================================================
# SECTION 4: FSM STATES
# ==============================================================
class VZStates(StatesGroup):
    # VZ registration flow
    waiting_bot_added      = State()
    waiting_channel_link   = State()
    waiting_promo_content  = State()
    # Session flow
    in_session             = State()
    # VIP flow
    vip_menu               = State()
    # Admin broadcast
    admin_broadcast        = State()
    # Admin ban
    admin_ban_id           = State()

# ==============================================================
# SECTION 5: QUEUE SYSTEM
# ==============================================================
class VZQueue:
    """Thread-safe queue for VZ sessions."""
    def __init__(self):
        self._regular: deque = deque()
        self._vip: deque = deque()
        self._in_queue: set = set()          # user_ids currently in queue
        self._active_sessions: Dict[int, int] = {}  # user_id -> partner_id

    def add(self, user_id: int, data: dict, is_vip: bool = False) -> bool:
        if user_id in self._in_queue or user_id in self._active_sessions:
            return False
        entry = {"user_id": user_id, "data": data, "joined_at": time.time()}
        if is_vip:
            self._vip.appendleft(entry)
        else:
            self._regular.append(entry)
        self._in_queue.add(user_id)
        return True

    def remove(self, user_id: int) -> bool:
        self._in_queue.discard(user_id)
        for q in (self._vip, self._regular):
            for i, entry in enumerate(list(q)):
                if entry["user_id"] == user_id:
                    # rebuild deque without this entry
                    new_q = deque(e for e in q if e["user_id"] != user_id)
                    q.clear()
                    q.extend(new_q)
                    return True
        return False

    def pop_pair(self) -> Optional[Tuple[dict, dict]]:
        # VIP vs VIP first, then VIP vs regular, then regular vs regular
        if len(self._vip) >= 2:
            return self._vip.popleft(), self._vip.popleft()
        if self._vip and self._regular:
            return self._vip.popleft(), self._regular.popleft()
        if len(self._regular) >= 2:
            return self._regular.popleft(), self._regular.popleft()
        return None

    def finalize_pop(self, uid1: int, uid2: int):
        self._in_queue.discard(uid1)
        self._in_queue.discard(uid2)
        self._active_sessions[uid1] = uid2
        self._active_sessions[uid2] = uid1

    def get_partner(self, user_id: int) -> Optional[int]:
        return self._active_sessions.get(user_id)

    def end_session(self, user_id: int):
        partner_id = self._active_sessions.pop(user_id, None)
        if partner_id:
            self._active_sessions.pop(partner_id, None)

    def is_in_queue(self, user_id: int) -> bool:
        return user_id in self._in_queue

    def is_in_session(self, user_id: int) -> bool:
        return user_id in self._active_sessions

    def queue_position(self, user_id: int, is_vip: bool = False) -> int:
        q = self._vip if is_vip else self._regular
        for i, e in enumerate(q):
            if e["user_id"] == user_id:
                return i + 1
        # check other queue too
        for i, e in enumerate(self._regular if is_vip else self._vip):
            if e["user_id"] == user_id:
                return i + 1
        return -1

    @property
    def regular_count(self) -> int:
        return len(self._regular)

    @property
    def vip_count(self) -> int:
        return len(self._vip)


vz_queue = VZQueue()

# Pending confirmations: session_id -> {uid1, uid2, confirmed, promo1, promo2}
active_sessions: Dict[str, dict] = {}

# ==============================================================
# SECTION 6: DATABASE
# ==============================================================
class Database:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    full_name   TEXT,
                    points      INTEGER DEFAULT 0,
                    vz_count    INTEGER DEFAULT 0,
                    warns       INTEGER DEFAULT 0,
                    muted_until INTEGER DEFAULT 0,
                    vip_until   INTEGER DEFAULT 0,
                    is_banned   INTEGER DEFAULT 0,
                    created_at  INTEGER DEFAULT (strftime('%s','now'))
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS required_channels (
                    channel_id   TEXT PRIMARY KEY,
                    channel_name TEXT,
                    added_by     INTEGER,
                    added_at     INTEGER DEFAULT (strftime('%s','now'))
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id  TEXT PRIMARY KEY,
                    user1_id    INTEGER,
                    user2_id    INTEGER,
                    user1_promo TEXT,
                    user2_promo TEXT,
                    status      TEXT DEFAULT 'active',
                    created_at  INTEGER DEFAULT (strftime('%s','now')),
                    finished_at INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS penalty (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER,
                    reason      TEXT,
                    created_at  INTEGER DEFAULT (strftime('%s','now'))
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS invoices (
                    invoice_id  TEXT PRIMARY KEY,
                    user_id     INTEGER,
                    amount      REAL,
                    status      TEXT DEFAULT 'pending',
                    created_at  INTEGER DEFAULT (strftime('%s','now'))
                )
            """)
            await db.commit()
        logger.info("✅ Database initialized")

    # ── User operations ──────────────────────────────────────
    async def get_user(self, user_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def ensure_user(self, user: User) -> dict:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO users (user_id, username, full_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    full_name=excluded.full_name
            """, (user.id, user.username, user.full_name))
            await db.commit()
        return await self.get_user(user.id)

    async def add_points(self, user_id: int, points: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET points = points + ? WHERE user_id=?",
                (points, user_id)
            )
            await db.commit()

    async def spend_points(self, user_id: int, points: int) -> bool:
        user = await self.get_user(user_id)
        if not user or user["points"] < points:
            return False
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET points = points - ? WHERE user_id=?",
                (points, user_id)
            )
            await db.commit()
        return True

    async def increment_vz(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET vz_count = vz_count + 1 WHERE user_id=?",
                (user_id,)
            )
            await db.commit()

    async def add_warn(self, user_id: int, reason: str = "") -> int:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET warns = warns + 1 WHERE user_id=?",
                (user_id,)
            )
            await db.execute(
                "INSERT INTO penalty (user_id, reason) VALUES (?, ?)",
                (user_id, reason)
            )
            await db.commit()
        user = await self.get_user(user_id)
        return user["warns"] if user else 0

    async def clear_warns(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET warns=0 WHERE user_id=?", (user_id,))
            await db.commit()

    async def set_mute(self, user_id: int, until_ts: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET muted_until=?, warns=0 WHERE user_id=?",
                (until_ts, user_id)
            )
            await db.commit()

    async def set_vip(self, user_id: int, until_ts: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET vip_until=? WHERE user_id=?",
                (until_ts, user_id)
            )
            await db.commit()

    async def ban_user(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET is_banned=1 WHERE user_id=?",
                (user_id,)
            )
            await db.commit()

    async def unban_user(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE users SET is_banned=0 WHERE user_id=?",
                (user_id,)
            )
            await db.commit()

    async def get_all_users(self) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE is_banned=0") as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

    async def get_stats(self) -> dict:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT COUNT(*) as total FROM users") as c:
                total = (await c.fetchone())["total"]
            async with db.execute(
                "SELECT COUNT(*) as cnt FROM users WHERE vip_until > ?",
                (int(time.time()),)
            ) as c:
                vip_count = (await c.fetchone())["cnt"]
            async with db.execute("SELECT COUNT(*) as cnt FROM sessions WHERE status='completed'") as c:
                sessions_count = (await c.fetchone())["cnt"]
            async with db.execute("SELECT SUM(points) as pts FROM users") as c:
                total_pts = (await c.fetchone())["pts"] or 0
        return {
            "total_users": total,
            "vip_users": vip_count,
            "completed_sessions": sessions_count,
            "total_points": total_pts,
        }

    # ── Required channels ─────────────────────────────────────
    async def add_required_channel(self, channel_id: str, channel_name: str, admin_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO required_channels (channel_id, channel_name, added_by)
                VALUES (?, ?, ?)
            """, (channel_id, channel_name, admin_id))
            await db.commit()

    async def remove_required_channel(self, channel_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "DELETE FROM required_channels WHERE channel_id=?",
                (channel_id,)
            )
            await db.commit()

    async def get_required_channels(self) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM required_channels") as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

    # ── Sessions ──────────────────────────────────────────────
    async def save_session(self, session_id: str, uid1: int, uid2: int, promo1: str, promo2: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO sessions
                (session_id, user1_id, user2_id, user1_promo, user2_promo, status)
                VALUES (?, ?, ?, ?, ?, 'active')
            """, (session_id, uid1, uid2, promo1, promo2))
            await db.commit()

    async def complete_session(self, session_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE sessions SET status='completed', finished_at=strftime('%s','now')
                WHERE session_id=?
            """, (session_id,))
            await db.commit()

    async def cancel_session(self, session_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE sessions SET status='cancelled' WHERE session_id=?",
                (session_id,)
            )
            await db.commit()

    # ── Invoices ──────────────────────────────────────────────
    async def save_invoice(self, invoice_id: str, user_id: int, amount: float):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO invoices (invoice_id, user_id, amount) VALUES (?, ?, ?)
            """, (invoice_id, user_id, amount))
            await db.commit()

    async def get_invoice(self, invoice_id: str) -> Optional[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM invoices WHERE invoice_id=?", (invoice_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def complete_invoice(self, invoice_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE invoices SET status='paid' WHERE invoice_id=?",
                (invoice_id,)
            )
            await db.commit()


db = Database(DB_PATH)

# ==============================================================
# SECTION 7: ANTI-FLOOD MIDDLEWARE
# ==============================================================
class AntiFloodMiddleware(BaseMiddleware):
    def __init__(self, limit: int = FLOOD_LIMIT, window: int = FLOOD_WINDOW):
        self.limit = limit
        self.window = window
        self._user_timestamps: Dict[int, List[float]] = defaultdict(list)

    async def __call__(self, handler, event, data):
        user: Optional[User] = None
        if isinstance(event, Message):
            user = event.from_user
        elif isinstance(event, CallbackQuery):
            user = event.from_user

        if not user:
            return await handler(event, data)

        uid = user.id
        now = time.time()
        # Clean old timestamps
        self._user_timestamps[uid] = [
            t for t in self._user_timestamps[uid] if now - t < self.window
        ]
        self._user_timestamps[uid].append(now)

        if len(self._user_timestamps[uid]) > self.limit:
            if isinstance(event, Message):
                await safe_send(
                    event.answer,
                    "⏳ <b>Слишком много сообщений!</b>\nПожалуйста, подождите немного.",
                )
            elif isinstance(event, CallbackQuery):
                await event.answer("⏳ Не так быстро!", show_alert=True)
            return  # block processing
        return await handler(event, data)

# ==============================================================
# SECTION 8: UTILITY FUNCTIONS
# ==============================================================
async def safe_send(func, *args, **kwargs) -> Optional[Message]:
    """Send message with retry on flood wait and error handling."""
    for attempt in range(3):
        try:
            return await func(*args, **kwargs)
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"FloodWait {wait}s, attempt {attempt+1}")
            await asyncio.sleep(wait)
        except TelegramForbiddenError:
            logger.warning(f"Bot blocked by user")
            return None
        except TelegramBadRequest as e:
            logger.warning(f"BadRequest: {e}")
            return None
        except Exception as e:
            logger.error(f"safe_send error: {e}")
            return None
    return None


def is_vip(user_data: dict) -> bool:
    return user_data.get("vip_until", 0) > int(time.time())


def is_muted(user_data: dict) -> bool:
    return user_data.get("muted_until", 0) > int(time.time())


def mute_remaining(user_data: dict) -> str:
    remaining = user_data.get("muted_until", 0) - int(time.time())
    if remaining <= 0:
        return "0 мин"
    minutes = remaining // 60
    seconds = remaining % 60
    if minutes > 0:
        return f"{minutes} мин {seconds} сек"
    return f"{seconds} сек"


def format_profile(user_data: dict) -> str:
    uid = user_data["user_id"]
    name = user_data.get("full_name") or "Пользователь"
    username = f"@{user_data['username']}" if user_data.get("username") else "—"
    points = user_data.get("points", 0)
    vz_count = user_data.get("vz_count", 0)
    warns = user_data.get("warns", 0)
    vip = "✅ VIP" if is_vip(user_data) else "❌ Нет"
    mute = f"🔇 До {datetime.fromtimestamp(user_data['muted_until']).strftime('%H:%M')}" \
        if is_muted(user_data) else "✅ Чист"

    vip_until_str = ""
    if is_vip(user_data):
        vip_until = datetime.fromtimestamp(user_data["vip_until"])
        vip_until_str = f"\n╠ <b>VIP до:</b> {vip_until.strftime('%d.%m %H:%M')}"

    return (
        f"👤 <b>Профиль</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"╠ <b>ID:</b> <code>{uid}</code>\n"
        f"╠ <b>Имя:</b> {name}\n"
        f"╠ <b>Username:</b> {username}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"╠ 💰 <b>Очки:</b> {points:,}\n"
        f"╠ 🔄 <b>Успешных ВЗ:</b> {vz_count}\n"
        f"╠ ⚠️ <b>Предупреждения:</b> {warns}/{MAX_WARNS}\n"
        f"╠ 👑 <b>VIP:</b> {vip}{vip_until_str}\n"
        f"╠ 🔇 <b>Мут:</b> {mute}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )


def generate_session_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]


async def check_user_subscription(bot: Bot, user_id: int, channel_id: str) -> bool:
    """Check if user is a member of the channel."""
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status not in (
            ChatMemberStatus.LEFT,
            ChatMemberStatus.KICKED,
            ChatMemberStatus.BANNED if hasattr(ChatMemberStatus, "BANNED") else "kicked",
        )
    except Exception:
        return False


async def check_all_subscriptions(bot: Bot, user_id: int) -> List[dict]:
    """Returns list of channels user is NOT subscribed to."""
    channels = await db.get_required_channels()
    missing = []
    for ch in channels:
        if not await check_user_subscription(bot, user_id, ch["channel_id"]):
            missing.append(ch)
    return missing


async def check_bot_admin(bot: Bot, chat_id: Union[int, str]) -> Tuple[bool, List[str]]:
    """
    Check if bot is admin with required permissions.
    Returns (is_ok, list_of_missing_permissions).
    """
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat_id, user_id=me.id)
    except (TelegramBadRequest, TelegramNotFound, TelegramForbiddenError):
        return False, REQUIRED_PERMISSIONS

    if not isinstance(member, (ChatMemberAdministrator, ChatMemberOwner)):
        return False, REQUIRED_PERMISSIONS

    if isinstance(member, ChatMemberOwner):
        return True, []

    missing = []
    for perm in REQUIRED_PERMISSIONS:
        val = getattr(member, perm, None)
        if not val:
            missing.append(perm)
    return len(missing) == 0, missing


async def resolve_channel(bot: Bot, raw: str) -> Optional[dict]:
    """
    Resolve channel/chat from link, @username, or ID.
    Returns chat info dict or None.
    """
    raw = raw.strip()
    chat_id: Union[str, int] = raw

    # Try to extract from t.me link
    if "t.me/" in raw or "telegram.me/" in raw:
        match = re.search(r"(?:t\.me|telegram\.me)/([a-zA-Z0-9_+]+)", raw)
        if match:
            slug = match.group(1)
            if slug.startswith("+"):
                return None  # private invite link, can't resolve
            chat_id = f"@{slug}"

    # Ensure @ prefix for usernames
    if re.match(r"^[a-zA-Z][a-zA-Z0-9_]{4,}$", raw):
        chat_id = f"@{raw}"

    try:
        chat_id_parsed = int(raw)
        chat_id = chat_id_parsed
    except (ValueError, TypeError):
        pass

    try:
        chat = await bot.get_chat(chat_id)
        return {
            "id": chat.id,
            "title": chat.title or chat.full_name or str(chat.id),
            "username": chat.username,
            "type": chat.type,
        }
    except Exception:
        return None

# ==============================================================
# SECTION 9: CRYPTOBOT API
# ==============================================================
class CryptoBotAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {"Crypto-Pay-API-Token": token}

    async def create_invoice(self, amount: float, currency: str = "USDT", payload: str = "") -> Optional[dict]:
        if not self.token:
            return None
        url = f"{CRYPTOBOT_API_URL}/createInvoice"
        params = {
            "asset": currency,
            "amount": str(amount),
            "payload": payload,
            "description": f"VIP активация на {VIP_DURATION_HOURS}ч — VZ Bot",
            "paid_btn_name": "callbackGame",
            "paid_btn_url": "https://t.me/",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=params, headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return data["result"]
                    logger.error(f"CryptoBot error: {data}")
                    return None
        except Exception as e:
            logger.error(f"CryptoBot request error: {e}")
            return None

    async def get_invoices(self, invoice_ids: Optional[List[str]] = None) -> List[dict]:
        if not self.token:
            return []
        url = f"{CRYPTOBOT_API_URL}/getInvoices"
        params = {}
        if invoice_ids:
            params["invoice_ids"] = ",".join(invoice_ids)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return data["result"].get("items", [])
                    return []
        except Exception:
            return []

    async def check_invoice(self, invoice_id: str) -> Optional[str]:
        """Returns 'paid', 'active', 'expired', or None on error."""
        invoices = await self.get_invoices([invoice_id])
        for inv in invoices:
            if str(inv.get("invoice_id")) == str(invoice_id):
                return inv.get("status")
        return None


cryptobot = CryptoBotAPI(CRYPTOBOT_TOKEN)

# ==============================================================
# SECTION 10: KEYBOARDS
# ==============================================================
def kb_main_menu(is_vip_user: bool = False) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔄 Начать ВЗ"))
    builder.row(
        KeyboardButton(text="👤 Профиль"),
        KeyboardButton(text="👑 VIP"),
    )
    builder.row(
        KeyboardButton(text="ℹ️ Помощь"),
        KeyboardButton(text="📊 Статистика"),
    )
    return builder.as_markup(resize_keyboard=True)


def kb_confirm_added() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Я добавил бота", callback_data="bot_added_confirm")
    builder.button(text="❌ Отмена", callback_data="vz_cancel")
    builder.adjust(1)
    return builder.as_markup()


def kb_cancel_state() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="vz_cancel")
    return builder.as_markup()


def kb_vz_session(session_id: str, role: str) -> InlineKeyboardMarkup:
    """Keyboard for active VZ session."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Готово — подписался!", callback_data=f"session_done:{session_id}:{role}")
    builder.button(text="❌ Отмена", callback_data=f"session_cancel:{session_id}:{role}")
    builder.adjust(1)
    return builder.as_markup()


def kb_vip_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=f"💰 Купить за {VIP_PRICE_POINTS} очков", callback_data="vip_buy_points")
    builder.button(text="💳 Купить за крипту (USDT)", callback_data="vip_buy_crypto")
    builder.button(text="🔙 Назад", callback_data="vip_back")
    builder.adjust(1)
    return builder.as_markup()


def kb_subscription_check(channels: List[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for ch in channels:
        name = ch.get("channel_name", "Канал")
        cid = ch["channel_id"]
        url = f"https://t.me/{cid.lstrip('@')}" if str(cid).startswith("@") else f"https://t.me/c/{str(cid).lstrip('-100')}"
        builder.button(text=f"📢 {name}", url=url)
    builder.button(text="✅ Я подписался", callback_data="check_sub_done")
    builder.adjust(1)
    return builder.as_markup()


def kb_check_payment(invoice_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_payment:{invoice_id}")
    builder.button(text="❌ Отмена", callback_data="vip_back")
    builder.adjust(1)
    return builder.as_markup()


def kb_admin_panel() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="📨 Рассылка", callback_data="admin_broadcast")
    builder.button(text="🚫 Забанить", callback_data="admin_ban")
    builder.button(text="✅ Разбанить", callback_data="admin_unban")
    builder.button(text="📋 Каналы", callback_data="admin_channels")
    builder.adjust(2)
    return builder.as_markup()

# ==============================================================
# SECTION 11: ROUTERS
# ==============================================================
router = Router()

# ==============================================================
# SECTION 12: GUARD HELPERS
# ==============================================================
async def guard_banned(message: Message, user_data: dict) -> bool:
    """Returns True if user is banned (and sends error). Should STOP execution."""
    if user_data.get("is_banned"):
        await safe_send(
            message.answer,
            "🚫 <b>Доступ заблокирован</b>\n\nВы заблокированы в данном боте.",
        )
        return True
    return False


async def guard_muted(message: Message, user_data: dict) -> bool:
    """Returns True if user is muted."""
    if is_muted(user_data):
        rem = mute_remaining(user_data)
        await safe_send(
            message.answer,
            f"🔇 <b>Вы получили мут</b>\n\n"
            f"Причина: превышение предупреждений\n"
            f"⏳ Осталось: <b>{rem}</b>",
        )
        return True
    return False


async def guard_subscriptions(message: Message, bot: Bot, user_id: int) -> bool:
    """Returns True if user is missing subscriptions."""
    missing = await check_all_subscriptions(bot, user_id)
    if missing:
        text = (
            "📢 <b>Необходима подписка</b>\n\n"
            "Для использования бота подпишитесь на каналы:\n"
        )
        for ch in missing:
            text += f"  • {ch['channel_name']}\n"
        await safe_send(
            message.answer,
            text,
            reply_markup=kb_subscription_check(missing),
        )
        return True
    return False

# ==============================================================
# SECTION 13: START & MAIN MENU
# ==============================================================
@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user_data = await db.ensure_user(message.from_user)

    if await guard_banned(message, user_data):
        return

    name = message.from_user.first_name or "Пользователь"
    vip_badge = " 👑" if is_vip(user_data) else ""

    text = (
        f"👋 Привет, <b>{name}</b>{vip_badge}!\n\n"
        f"🔄 <b>VZ Bot</b> — сервис взаимных подписок\n\n"
        f"📈 Расти вместе с другими!\n"
        f"Пиарь свой канал, бота или чат — и получай подписчиков взамен.\n\n"
        f"💰 За каждое успешное ВЗ: <b>+{POINTS_PER_VZ} очков</b>\n"
        f"👑 VIP даёт приоритет в очереди\n\n"
        f"⬇️ Выберите действие:"
    )
    await safe_send(
        message.answer,
        text,
        reply_markup=kb_main_menu(is_vip(user_data)),
    )


@router.message(F.text == "👤 Профиль")
async def menu_profile(message: Message, bot: Bot):
    user_data = await db.get_user(message.from_user.id)
    if not user_data:
        user_data = await db.ensure_user(message.from_user)
    if await guard_banned(message, user_data):
        return
    await safe_send(message.answer, format_profile(user_data))


@router.message(F.text == "ℹ️ Помощь")
async def menu_help(message: Message):
    text = (
        "📖 <b>Справка по боту</b>\n\n"
        "<b>Что такое ВЗ?</b>\n"
        "Взаимная подписка — вы подписываетесь на канал партнёра, он подписывается на ваш.\n\n"
        "<b>Как начать:</b>\n"
        "1️⃣ Нажмите «🔄 Начать ВЗ»\n"
        "2️⃣ Добавьте бота в ваш канал как администратора\n"
        "3️⃣ Укажите ссылку на ваш канал\n"
        "4️⃣ Укажите что хотите пиарить\n"
        "5️⃣ Ожидайте партнёра в очереди\n\n"
        "<b>Правила:</b>\n"
        "⚠️ Срыв сессии = предупреждение\n"
        f"🔇 После {MAX_WARNS} предупреждений — мут на {MUTE_DURATION_MINUTES} мин\n"
        f"💰 За ВЗ: +{POINTS_PER_VZ} очков\n\n"
        "<b>VIP режим:</b>\n"
        "👑 Приоритет в очереди\n"
        "⚡️ Безлимитный доступ 24 часа\n\n"
        f"<b>Стоимость VIP:</b>\n"
        f"• {VIP_PRICE_POINTS} очков\n"
        f"• {VIP_PRICE_CRYPTO} USDT"
    )
    await safe_send(message.answer, text)


@router.message(F.text == "📊 Статистика")
async def menu_stats(message: Message):
    stats = await db.get_stats()
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Пользователей: <b>{stats['total_users']:,}</b>\n"
        f"👑 VIP активных: <b>{stats['vip_users']:,}</b>\n"
        f"🔄 Завершённых ВЗ: <b>{stats['completed_sessions']:,}</b>\n"
        f"💰 Очков выдано: <b>{stats['total_points']:,}</b>\n\n"
        f"🟢 В очереди: <b>{vz_queue.regular_count + vz_queue.vip_count}</b> чел."
    )
    await safe_send(message.answer, text)

# ==============================================================
# SECTION 14: SUBSCRIPTION CHECK
# ==============================================================
@router.callback_query(F.data == "check_sub_done")
async def cb_check_sub_done(call: CallbackQuery, bot: Bot, state: FSMContext):
    await call.answer()
    user_id = call.from_user.id
    missing = await check_all_subscriptions(bot, user_id)
    if missing:
        names = ", ".join(ch["channel_name"] for ch in missing)
        await call.message.edit_text(
            f"❌ <b>Вы ещё не подписались</b>\n\n"
            f"Не подписаны на: <b>{names}</b>\n\n"
            f"Подпишитесь и нажмите кнопку снова.",
            reply_markup=kb_subscription_check(missing),
        )
    else:
        await call.message.edit_text(
            "✅ <b>Подписка подтверждена!</b>\n\nТеперь можете пользоваться ботом.",
        )

# ==============================================================
# SECTION 15: VZ FLOW — START
# ==============================================================
@router.message(F.text == "🔄 Начать ВЗ")
async def vz_start(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    user_data = await db.ensure_user(message.from_user)

    if await guard_banned(message, user_data):
        return
    if await guard_muted(message, user_data):
        return
    if await guard_subscriptions(message, bot, user_id):
        return

    # Check if already in queue or session
    if vz_queue.is_in_queue(user_id):
        await safe_send(
            message.answer,
            "⏳ <b>Вы уже в очереди!</b>\n\nОжидайте партнёра.",
        )
        return
    if vz_queue.is_in_session(user_id):
        await safe_send(
            message.answer,
            "🔄 <b>У вас уже есть активная сессия!</b>\n\nЗавершите текущую ВЗ.",
        )
        return

    text = (
        "📋 <b>Начало ВЗ — Шаг 1/3</b>\n\n"
        "Для начала вам нужно добавить <b>этого бота</b> в свой канал или чат как <b>администратора</b>.\n\n"
        "🔑 <b>Бот должен иметь следующие права:</b>\n"
        "  ✅ Управление чатом\n"
        "  ✅ Удаление сообщений\n"
        "  ✅ Приглашение участников\n"
        "  ✅ Ограничение участников\n"
        "  ✅ Публикация сообщений\n"
        "  ✅ Редактирование сообщений\n\n"
        "📌 Идеально — выдать <b>все права администратора</b>.\n\n"
        "После добавления нажмите кнопку ниже 👇"
    )
    await state.set_state(VZStates.waiting_bot_added)
    await safe_send(
        message.answer,
        text,
        reply_markup=kb_confirm_added(),
    )


@router.callback_query(F.data == "bot_added_confirm", VZStates.waiting_bot_added)
async def cb_bot_added(call: CallbackQuery, state: FSMContext):
    await call.answer()
    text = (
        "🔗 <b>Шаг 2/3 — Укажите ваш канал</b>\n\n"
        "Отправьте одно из следующего:\n"
        "  • Ссылку: <code>https://t.me/mychannel</code>\n"
        "  • Username: <code>@mychannel</code>\n"
        "  • ID канала: <code>-1001234567890</code>\n\n"
        "⚠️ Это должен быть <b>канал или чат, куда вы добавили бота</b> как администратора."
    )
    await state.set_state(VZStates.waiting_channel_link)
    await call.message.edit_text(text, reply_markup=kb_cancel_state())


@router.message(VZStates.waiting_channel_link)
async def vz_channel_link(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    raw = message.text.strip() if message.text else ""

    if not raw:
        await safe_send(
            message.answer,
            "❌ Пожалуйста, отправьте ссылку, @username или ID канала.",
            reply_markup=kb_cancel_state(),
        )
        return

    # Resolve channel
    status_msg = await safe_send(message.answer, "🔍 <b>Проверяю канал...</b>")
    chat_info = await resolve_channel(bot, raw)

    if not chat_info:
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Канал не найден</b>\n\n"
                "Убедитесь, что:\n"
                "  • Канал существует\n"
                "  • Канал публичный или бот добавлен\n"
                "  • Ссылка/username правильные\n\n"
                "Попробуйте снова:",
                reply_markup=kb_cancel_state(),
            )
        return

    # Check bot is admin
    is_ok, missing_perms = await check_bot_admin(bot, chat_info["id"])

    if not is_ok:
        if missing_perms == REQUIRED_PERMISSIONS:
            err_text = (
                f"❌ <b>Бот не является администратором</b>\n\n"
                f"Канал: <b>{chat_info['title']}</b>\n\n"
                f"Добавьте бота как администратора с полными правами и попробуйте снова."
            )
        else:
            missing_labels = "\n".join(f"  ❌ {PERMISSION_LABELS.get(p, p)}" for p in missing_perms)
            err_text = (
                f"⚠️ <b>Недостаточно прав</b>\n\n"
                f"Канал: <b>{chat_info['title']}</b>\n\n"
                f"<b>Отсутствующие права:</b>\n{missing_labels}\n\n"
                f"Выдайте боту все необходимые права и попробуйте снова."
            )
        if status_msg:
            await status_msg.edit_text(err_text, reply_markup=kb_cancel_state())
        return

    # Success! Save channel info to state
    await state.update_data(channel=chat_info)

    success_text = (
        f"✅ <b>Канал подтверждён!</b>\n\n"
        f"📢 <b>{chat_info['title']}</b>\n"
        f"🆔 ID: <code>{chat_info['id']}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📣 <b>Шаг 3/3 — Что пиарим?</b>\n\n"
        f"Отправьте что именно вы хотите рекламировать:\n"
        f"  • Ссылку на канал/чат/бота\n"
        f"  • @username\n"
        f"  • Текстовое описание\n"
        f"  • Telegram-ссылку\n\n"
        f"Это увидит ваш партнёр по ВЗ."
    )
    await state.set_state(VZStates.waiting_promo_content)
    if status_msg:
        await status_msg.edit_text(success_text, reply_markup=kb_cancel_state())


@router.message(VZStates.waiting_promo_content)
async def vz_promo_content(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    user_data = await db.get_user(user_id)

    # Accept text, forward, or link
    promo_text = ""
    if message.text:
        promo_text = message.text.strip()
    elif message.caption:
        promo_text = message.caption.strip()
    elif message.forward_from_chat:
        promo_text = f"@{message.forward_from_chat.username}" if message.forward_from_chat.username \
            else str(message.forward_from_chat.id)
    else:
        await safe_send(
            message.answer,
            "❌ Отправьте текст, ссылку или @username для пиара.",
            reply_markup=kb_cancel_state(),
        )
        return

    if len(promo_text) > 500:
        await safe_send(
            message.answer,
            "❌ Текст слишком длинный (максимум 500 символов).",
            reply_markup=kb_cancel_state(),
        )
        return

    state_data = await state.get_data()
    channel = state_data.get("channel", {})
    vip_user = is_vip(user_data) if user_data else False

    # Add to queue
    vz_data = {
        "user_id": user_id,
        "promo": promo_text,
        "channel": channel,
        "user_name": message.from_user.full_name,
        "is_vip": vip_user,
    }

    added = vz_queue.add(user_id, vz_data, is_vip=vip_user)
    if not added:
        await safe_send(
            message.answer,
            "⚠️ <b>Вы уже в очереди или сессии!</b>\n\nОжидайте своей очереди.",
        )
        await state.clear()
        return

    await state.clear()

    queue_pos = vz_queue.queue_position(user_id, is_vip=vip_user)
    vip_badge = " 👑" if vip_user else ""

    await safe_send(
        message.answer,
        f"⏳ <b>Вы добавлены в очередь{vip_badge}</b>\n\n"
        f"📢 Пиарим: <code>{promo_text}</code>\n"
        f"📋 Позиция в очереди: <b>{queue_pos}</b>\n\n"
        f"🔄 Ищем партнёра...\n"
        f"Как только найдётся пара — вы получите уведомление.\n\n"
        f"💡 <i>Не выходите из бота!</i>",
    )

    # Try to match immediately
    await try_match_queue(bot)


async def try_match_queue(bot: Bot):
    """Try to find a matching pair and start a session."""
    pair = vz_queue.pop_pair()
    if not pair:
        return

    entry1, entry2 = pair
    uid1 = entry1["user_id"]
    uid2 = entry2["user_id"]

    vz_queue.finalize_pop(uid1, uid2)
    session_id = generate_session_id()

    promo1 = entry1["data"]["promo"]
    promo2 = entry2["data"]["promo"]

    # Save session to DB
    await db.save_session(session_id, uid1, uid2, promo1, promo2)

    # Track in memory
    active_sessions[session_id] = {
        "uid1": uid1,
        "uid2": uid2,
        "promo1": promo1,
        "promo2": promo2,
        "confirmed": set(),
        "created_at": time.time(),
    }

    user1_name = entry1["data"].get("user_name", "Партнёр")
    user2_name = entry2["data"].get("user_name", "Партнёр")

    # Message to user 1: subscribe to user2's promo
    msg1 = (
        f"🎉 <b>Найден партнёр!</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Ваш партнёр:</b> {user2_name}\n\n"
        f"📢 <b>Подпишитесь на:</b>\n"
        f"<code>{promo2}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ После подписки нажмите <b>«Готово»</b>\n"
        f"⏳ Сессия истечёт через {SESSION_TIMEOUT // 60} минут"
    )

    # Message to user 2: subscribe to user1's promo
    msg2 = (
        f"🎉 <b>Найден партнёр!</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Ваш партнёр:</b> {user1_name}\n\n"
        f"📢 <b>Подпишитесь на:</b>\n"
        f"<code>{promo1}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ После подписки нажмите <b>«Готово»</b>\n"
        f"⏳ Сессия истечёт через {SESSION_TIMEOUT // 60} минут"
    )

    await safe_send(bot.send_message, uid1, msg1, reply_markup=kb_vz_session(session_id, "user1"))
    await safe_send(bot.send_message, uid2, msg2, reply_markup=kb_vz_session(session_id, "user2"))

    # Schedule session timeout
    asyncio.create_task(session_timeout_task(bot, session_id, SESSION_TIMEOUT))

# ==============================================================
# SECTION 16: SESSION HANDLERS
# ==============================================================
async def session_timeout_task(bot: Bot, session_id: str, timeout: int):
    """Cancel session after timeout if not completed."""
    await asyncio.sleep(timeout)
    session = active_sessions.get(session_id)
    if not session:
        return  # already handled

    uid1 = session["uid1"]
    uid2 = session["uid2"]
    confirmed = session["confirmed"]

    # End session
    del active_sessions[session_id]
    vz_queue.end_session(uid1)
    vz_queue.end_session(uid2)
    await db.cancel_session(session_id)

    timeout_msg = (
        "⏰ <b>Сессия истекла</b>\n\n"
        "Время ожидания вышло. Сессия ВЗ автоматически отменена.\n"
        "Вы можете начать новую ВЗ."
    )

    if uid1 not in confirmed:
        warns = await db.add_warn(uid1, reason="Session timeout")
        await handle_warn_result(bot, uid1, warns, "Сессия истекла (не подтвердили подписку)")
    else:
        await safe_send(bot.send_message, uid1, timeout_msg)

    if uid2 not in confirmed:
        warns = await db.add_warn(uid2, reason="Session timeout")
        await handle_warn_result(bot, uid2, warns, "Сессия истекла (не подтвердили подписку)")
    else:
        await safe_send(bot.send_message, uid2, timeout_msg)


async def handle_warn_result(bot: Bot, user_id: int, warns: int, reason: str = ""):
    """Send appropriate warn/mute message."""
    if warns >= MAX_WARNS:
        mute_until = int(time.time()) + MUTE_DURATION_MINUTES * 60
        await db.set_mute(user_id, mute_until)
        mute_time = datetime.fromtimestamp(mute_until).strftime("%H:%M %d.%m")
        await safe_send(
            bot.send_message,
            user_id,
            f"🔇 <b>Вы получили мут!</b>\n\n"
            f"Причина: {reason}\n"
            f"Предупреждений: {warns}/{MAX_WARNS}\n\n"
            f"⏳ Мут действует до: <b>{mute_time}</b>\n"
            f"({MUTE_DURATION_MINUTES} минут)",
        )
    else:
        await safe_send(
            bot.send_message,
            user_id,
            f"⚠️ <b>Предупреждение {warns}/{MAX_WARNS}</b>\n\n"
            f"Причина: {reason}\n\n"
            f"{'❗️ Ещё одно — и вы получите мут!' if warns == MAX_WARNS - 1 else f'После {MAX_WARNS} предупреждений — мут на {MUTE_DURATION_MINUTES} мин.'}",
        )


@router.callback_query(F.data.startswith("session_done:"))
async def cb_session_done(call: CallbackQuery, bot: Bot):
    await call.answer("🔄 Проверяю подписку...")
    parts = call.data.split(":")
    session_id = parts[1]
    role = parts[2]
    user_id = call.from_user.id

    session = active_sessions.get(session_id)
    if not session:
        await call.message.edit_text(
            "❌ <b>Сессия не найдена</b>\n\nВозможно, она уже завершена или истекла."
        )
        return

    uid1 = session["uid1"]
    uid2 = session["uid2"]

    # Determine which promo this user needs to subscribe to
    if role == "user1":
        target_promo = session["promo2"]
        partner_id = uid2
    else:
        target_promo = session["promo1"]
        partner_id = uid1

    # Try to verify subscription via Telegram API
    verified = False
    subscription_error = ""

    # Extract channel id/username from promo if possible
    promo_channel_id = None
    if target_promo.startswith("@"):
        promo_channel_id = target_promo
    elif "t.me/" in target_promo:
        match = re.search(r"t\.me/([a-zA-Z0-9_]+)", target_promo)
        if match:
            promo_channel_id = f"@{match.group(1)}"

    if promo_channel_id:
        try:
            verified = await check_user_subscription(bot, user_id, promo_channel_id)
            if not verified:
                subscription_error = f"Вы не подписаны на {promo_channel_id}"
        except Exception as e:
            # Can't verify — let it pass with manual confirmation
            verified = True
            logger.warning(f"Can't verify subscription for {user_id} to {promo_channel_id}: {e}")
    else:
        # Can't auto-verify (text promo, not a channel) — accept on trust
        verified = True

    if not verified:
        await call.answer(
            f"❌ Подписка не найдена!\n{subscription_error}\n\nПодпишитесь и повторите.",
            show_alert=True,
        )
        return

    # Mark this user as confirmed
    session["confirmed"].add(user_id)

    await call.message.edit_text(
        f"✅ <b>Подписка подтверждена!</b>\n\n"
        f"Ожидаем подтверждения от партнёра...",
    )

    # Notify partner
    await safe_send(
        bot.send_message,
        partner_id,
        "ℹ️ <b>Партнёр подтвердил подписку!</b>\n\nПодпишитесь и нажмите «Готово».",
    )

    # Check if both confirmed
    if uid1 in session["confirmed"] and uid2 in session["confirmed"]:
        await complete_vz_session(bot, session_id)


async def complete_vz_session(bot: Bot, session_id: str):
    """Complete a VZ session and reward both users."""
    session = active_sessions.pop(session_id, None)
    if not session:
        return

    uid1 = session["uid1"]
    uid2 = session["uid2"]

    vz_queue.end_session(uid1)
    vz_queue.end_session(uid2)

    await db.complete_session(session_id)
    await db.add_points(uid1, POINTS_PER_VZ)
    await db.add_points(uid2, POINTS_PER_VZ)
    await db.increment_vz(uid1)
    await db.increment_vz(uid2)

    success_msg = (
        f"🎊 <b>ВЗ успешно завершено!</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Начислено: <b>+{POINTS_PER_VZ} очков</b>\n"
        f"🏆 Отличная работа!\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"Нажмите «🔄 Начать ВЗ» для новой сессии."
    )

    await safe_send(bot.send_message, uid1, success_msg)
    await safe_send(bot.send_message, uid2, success_msg)
    logger.info(f"✅ VZ session {session_id} completed: {uid1} <-> {uid2}")


@router.callback_query(F.data.startswith("session_cancel:"))
async def cb_session_cancel(call: CallbackQuery, bot: Bot):
    await call.answer()
    parts = call.data.split(":")
    session_id = parts[1]
    role = parts[2]
    user_id = call.from_user.id

    session = active_sessions.get(session_id)
    if not session:
        await call.message.edit_text("❌ Сессия не найдена или уже завершена.")
        return

    uid1 = session["uid1"]
    uid2 = session["uid2"]
    partner_id = uid2 if role == "user1" else uid1

    # Remove session
    del active_sessions[session_id]
    vz_queue.end_session(uid1)
    vz_queue.end_session(uid2)
    await db.cancel_session(session_id)

    # Penalize canceller
    warns = await db.add_warn(user_id, reason="Отмена сессии")

    await call.message.edit_text(
        "❌ <b>Вы отменили сессию ВЗ</b>\n\n"
        f"⚠️ Начислено предупреждение ({warns}/{MAX_WARNS})"
    )

    await handle_warn_result(bot, user_id, warns, "Отмена сессии ВЗ")

    # Notify partner
    await safe_send(
        bot.send_message,
        partner_id,
        "😔 <b>Партнёр отменил ВЗ</b>\n\n"
        "Ваш партнёр отменил сессию. Вы можете начать новую ВЗ.",
    )
    logger.info(f"❌ Session {session_id} cancelled by {user_id}")

# ==============================================================
# SECTION 17: CANCEL STATE
# ==============================================================
@router.callback_query(F.data == "vz_cancel")
async def cb_vz_cancel(call: CallbackQuery, state: FSMContext):
    await call.answer()
    user_id = call.from_user.id
    vz_queue.remove(user_id)
    await state.clear()
    await call.message.edit_text(
        "❌ <b>Отменено</b>\n\nВозвращаемся в главное меню.",
    )

# ==============================================================
# SECTION 18: VIP SYSTEM
# ==============================================================
@router.message(F.text == "👑 VIP")
async def menu_vip(message: Message, bot: Bot):
    user_data = await db.get_user(message.from_user.id)
    if not user_data:
        user_data = await db.ensure_user(message.from_user)

    if await guard_banned(message, user_data):
        return

    if is_vip(user_data):
        vip_until = datetime.fromtimestamp(user_data["vip_until"])
        text = (
            f"👑 <b>Ваш VIP активен!</b>\n\n"
            f"⏳ Действует до: <b>{vip_until.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"<b>Ваши привилегии:</b>\n"
            f"  ⚡️ Приоритет в очереди\n"
            f"  🔝 Попадаете к первым в пару\n"
            f"  ♾ Без ограничений"
        )
        await safe_send(message.answer, text)
        return

    points = user_data.get("points", 0)
    text = (
        f"👑 <b>VIP режим</b>\n\n"
        f"<b>Что даёт VIP:</b>\n"
        f"  ⚡️ Приоритет в очереди ВЗ\n"
        f"  🔝 Отдельная VIP-очередь\n"
        f"  ♾ Безлимитный доступ на {VIP_DURATION_HOURS} часов\n\n"
        f"<b>Стоимость:</b>\n"
        f"  💰 {VIP_PRICE_POINTS} очков (у вас: {points:,})\n"
        f"  💳 {VIP_PRICE_CRYPTO} USDT (через CryptoBot)\n\n"
        f"Выберите способ оплаты:"
    )
    await safe_send(
        message.answer,
        text,
        reply_markup=kb_vip_menu(),
    )


@router.callback_query(F.data == "vip_buy_points")
async def cb_vip_buy_points(call: CallbackQuery, bot: Bot):
    await call.answer()
    user_id = call.from_user.id
    user_data = await db.get_user(user_id)

    if not user_data:
        return

    if is_vip(user_data):
        await call.answer("✅ У вас уже есть VIP!", show_alert=True)
        return

    points = user_data.get("points", 0)
    if points < VIP_PRICE_POINTS:
        await call.answer(
            f"❌ Недостаточно очков!\nНужно: {VIP_PRICE_POINTS}\nЕсть: {points}",
            show_alert=True,
        )
        return

    success = await db.spend_points(user_id, VIP_PRICE_POINTS)
    if not success:
        await call.answer("❌ Ошибка списания очков.", show_alert=True)
        return

    vip_until = int(time.time()) + VIP_DURATION_HOURS * 3600
    await db.set_vip(user_id, vip_until)

    vip_until_dt = datetime.fromtimestamp(vip_until)
    await call.message.edit_text(
        f"✅ <b>VIP активирован!</b>\n\n"
        f"💰 Списано: {VIP_PRICE_POINTS} очков\n"
        f"⏳ Действует до: <b>{vip_until_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"⚡️ Теперь вы в приоритетной очереди!"
    )


@router.callback_query(F.data == "vip_buy_crypto")
async def cb_vip_buy_crypto(call: CallbackQuery, bot: Bot):
    await call.answer("💳 Создаю счёт...")

    if not CRYPTOBOT_TOKEN:
        await call.answer(
            "❌ Оплата через крипту временно недоступна.",
            show_alert=True,
        )
        return

    user_id = call.from_user.id
    payload = f"vip:{user_id}:{int(time.time())}"

    invoice = await cryptobot.create_invoice(
        amount=VIP_PRICE_CRYPTO,
        currency="USDT",
        payload=payload,
    )

    if not invoice:
        await call.answer("❌ Ошибка создания счёта. Попробуйте позже.", show_alert=True)
        return

    invoice_id = str(invoice.get("invoice_id"))
    pay_url = invoice.get("bot_invoice_url") or invoice.get("mini_app_invoice_url", "")

    await db.save_invoice(invoice_id, user_id, VIP_PRICE_CRYPTO)

    builder = InlineKeyboardBuilder()
    if pay_url:
        builder.button(text="💳 Оплатить", url=pay_url)
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_payment:{invoice_id}")
    builder.button(text="❌ Отмена", callback_data="vip_back")
    builder.adjust(1)

    await call.message.edit_text(
        f"💳 <b>Счёт создан!</b>\n\n"
        f"💰 Сумма: <b>{VIP_PRICE_CRYPTO} USDT</b>\n"
        f"🆔 Счёт: <code>{invoice_id}</code>\n\n"
        f"Нажмите «Оплатить» и завершите оплату.\n"
        f"После — нажмите «Проверить оплату».",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("check_payment:"))
async def cb_check_payment(call: CallbackQuery, bot: Bot):
    await call.answer("🔄 Проверяю...")
    invoice_id = call.data.split(":")[1]
    user_id = call.from_user.id

    inv_db = await db.get_invoice(invoice_id)
    if not inv_db or inv_db["user_id"] != user_id:
        await call.answer("❌ Счёт не найден.", show_alert=True)
        return

    if inv_db["status"] == "paid":
        await call.answer("✅ Уже оплачено!", show_alert=True)
        return

    status = await cryptobot.check_invoice(invoice_id)

    if status == "paid":
        await db.complete_invoice(invoice_id)
        vip_until = int(time.time()) + VIP_DURATION_HOURS * 3600
        await db.set_vip(user_id, vip_until)
        vip_until_dt = datetime.fromtimestamp(vip_until)

        await call.message.edit_text(
            f"🎉 <b>Оплата получена! VIP активирован!</b>\n\n"
            f"💳 {VIP_PRICE_CRYPTO} USDT\n"
            f"⏳ Действует до: <b>{vip_until_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"⚡️ Приоритет в очереди активирован!"
        )
    elif status == "active":
        await call.answer("⏳ Оплата ещё не получена. Попробуйте позже.", show_alert=True)
    elif status == "expired":
        await call.answer("❌ Счёт истёк. Создайте новый.", show_alert=True)
    else:
        await call.answer("⚠️ Не удалось проверить оплату. Попробуйте позже.", show_alert=True)


@router.callback_query(F.data == "vip_back")
async def cb_vip_back(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text(
        "👑 <b>VIP меню закрыто</b>\n\nВернитесь в главное меню.",
    )

# ==============================================================
# SECTION 19: ADMIN COMMANDS
# ==============================================================
def admin_only(func):
    """Decorator to restrict handler to admins."""
    import functools
    @functools.wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id not in ADMIN_IDS:
            await safe_send(message.answer, "❌ Нет доступа.")
            return
        return await func(message, *args, **kwargs)
    return wrapper


@router.message(Command("admin"))
@admin_only
async def cmd_admin(message: Message):
    await safe_send(
        message.answer,
        "🛠 <b>Панель администратора</b>\n\n"
        "/stats — статистика\n"
        "/send — рассылка\n"
        "/ban [user_id] — заблокировать\n"
        "/unban [user_id] — разблокировать\n"
        "/warn [user_id] — предупреждение\n"
        "/clearwarn [user_id] — сбросить варны\n"
        "/setchannel — добавить обязательный канал\n"
        "/removechannel — удалить обязательный канал\n"
        "/channels — список каналов\n"
        "/queue — статус очереди",
        reply_markup=kb_admin_panel(),
    )


@router.message(Command("stats"))
@admin_only
async def cmd_stats(message: Message):
    stats = await db.get_stats()
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{stats['total_users']:,}</b>\n"
        f"👑 VIP активных: <b>{stats['vip_users']:,}</b>\n"
        f"🔄 Завершённых ВЗ: <b>{stats['completed_sessions']:,}</b>\n"
        f"💰 Очков выдано: <b>{stats['total_points']:,}</b>\n\n"
        f"🟢 В очереди сейчас: <b>{vz_queue.regular_count + vz_queue.vip_count}</b>\n"
        f"  ↳ Обычные: {vz_queue.regular_count}\n"
        f"  ↳ VIP: {vz_queue.vip_count}\n"
        f"🔄 Активных сессий: <b>{len(active_sessions)}</b>"
    )
    await safe_send(message.answer, text)


@router.message(Command("queue"))
@admin_only
async def cmd_queue(message: Message):
    text = (
        "📋 <b>Очередь ВЗ</b>\n\n"
        f"🟡 Обычная очередь: {vz_queue.regular_count} чел.\n"
        f"👑 VIP очередь: {vz_queue.vip_count} чел.\n"
        f"🔄 Активных сессий: {len(active_sessions)}\n\n"
    )
    if active_sessions:
        text += "<b>Активные сессии:</b>\n"
        for sid, s in list(active_sessions.items())[:10]:
            age = int(time.time() - s["created_at"])
            text += f"  #{sid}: {s['uid1']} ↔ {s['uid2']} ({age}с)\n"
    await safe_send(message.answer, text)


@router.message(Command("send"))
@admin_only
async def cmd_send_start(message: Message, state: FSMContext):
    await state.set_state(VZStates.admin_broadcast)
    await safe_send(
        message.answer,
        "📨 <b>Рассылка</b>\n\nОтправьте сообщение для рассылки всем пользователям.\n\n/cancel — отмена",
    )


@router.message(VZStates.admin_broadcast)
@admin_only
async def cmd_send_broadcast(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    users = await db.get_all_users()
    text = message.html_text if message.text else None

    sent = 0
    failed = 0
    for user in users:
        uid = user["user_id"]
        if uid == message.from_user.id:
            continue
        try:
            if text:
                await safe_send(bot.send_message, uid, text)
            sent += 1
            await asyncio.sleep(0.05)  # Rate limiting
        except Exception:
            failed += 1

    await safe_send(
        message.answer,
        f"📨 <b>Рассылка завершена</b>\n\n"
        f"✅ Отправлено: {sent}\n"
        f"❌ Ошибок: {failed}",
    )


@router.message(Command("ban"))
@admin_only
async def cmd_ban(message: Message, bot: Bot):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(message.answer, "❌ Использование: /ban [user_id]")
        return
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_send(message.answer, "❌ Неверный ID пользователя.")
        return

    await db.ban_user(target_id)
    await safe_send(message.answer, f"🚫 Пользователь <code>{target_id}</code> заблокирован.")
    await safe_send(
        bot.send_message,
        target_id,
        "🚫 <b>Вы заблокированы в боте.</b>",
    )


@router.message(Command("unban"))
@admin_only
async def cmd_unban(message: Message, bot: Bot):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(message.answer, "❌ Использование: /unban [user_id]")
        return
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_send(message.answer, "❌ Неверный ID пользователя.")
        return

    await db.unban_user(target_id)
    await safe_send(message.answer, f"✅ Пользователь <code>{target_id}</code> разблокирован.")
    await safe_send(
        bot.send_message,
        target_id,
        "✅ <b>Вы разблокированы в боте.</b>",
    )


@router.message(Command("warn"))
@admin_only
async def cmd_warn(message: Message, bot: Bot):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(message.answer, "❌ Использование: /warn [user_id] [причина]")
        return
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_send(message.answer, "❌ Неверный ID пользователя.")
        return

    reason = " ".join(args[2:]) if len(args) > 2 else "Нарушение правил (admin)"
    warns = await db.add_warn(target_id, reason)
    await safe_send(
        message.answer,
        f"⚠️ Предупреждение выдано пользователю <code>{target_id}</code>\n"
        f"Предупреждений: {warns}/{MAX_WARNS}",
    )
    await handle_warn_result(bot, target_id, warns, reason)


@router.message(Command("clearwarn"))
@admin_only
async def cmd_clearwarn(message: Message, bot: Bot):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(message.answer, "❌ Использование: /clearwarn [user_id]")
        return
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_send(message.answer, "❌ Неверный ID пользователя.")
        return

    await db.clear_warns(target_id)
    await safe_send(message.answer, f"✅ Предупреждения пользователя <code>{target_id}</code> сброшены.")
    await safe_send(bot.send_message, target_id, "✅ Ваши предупреждения сброшены администратором.")


# ── Channel management ────────────────────────────────────────
@router.message(Command("setchannel"))
@admin_only
async def cmd_setchannel(message: Message, bot: Bot):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(
            message.answer,
            "❌ Использование: /setchannel @username или /setchannel -100xxx\n\n"
            "Добавляет канал в список обязательных подписок.",
        )
        return

    raw = args[1]
    chat_info = await resolve_channel(bot, raw)
    if not chat_info:
        await safe_send(
            message.answer,
            "❌ Канал не найден. Убедитесь, что бот добавлен в него или канал публичный.",
        )
        return

    channel_id = f"@{chat_info['username']}" if chat_info.get("username") else str(chat_info["id"])
    await db.add_required_channel(channel_id, chat_info["title"], message.from_user.id)
    await safe_send(
        message.answer,
        f"✅ Канал добавлен:\n<b>{chat_info['title']}</b> ({channel_id})",
    )


@router.message(Command("removechannel"))
@admin_only
async def cmd_removechannel(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await safe_send(message.answer, "❌ Использование: /removechannel @username_или_id")
        return

    channel_id = args[1]
    await db.remove_required_channel(channel_id)
    await safe_send(message.answer, f"✅ Канал <code>{channel_id}</code> удалён из списка.")


@router.message(Command("channels"))
@admin_only
async def cmd_channels(message: Message):
    channels = await db.get_required_channels()
    if not channels:
        await safe_send(
            message.answer,
            "📋 <b>Обязательные каналы</b>\n\nСписок пуст.\nДобавьте командой /setchannel",
        )
        return

    text = "📋 <b>Обязательные каналы</b>\n\n"
    for i, ch in enumerate(channels, 1):
        added = datetime.fromtimestamp(ch["added_at"]).strftime("%d.%m.%Y")
        text += f"{i}. <b>{ch['channel_name']}</b>\n"
        text += f"   ID: <code>{ch['channel_id']}</code>\n"
        text += f"   Добавлен: {added}\n\n"

    await safe_send(message.answer, text)


# ── Admin callback handlers ───────────────────────────────────
@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    await call.answer()
    stats = await db.get_stats()
    text = (
        "📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: {stats['total_users']:,}\n"
        f"👑 VIP: {stats['vip_users']:,}\n"
        f"🔄 ВЗ: {stats['completed_sessions']:,}\n"
        f"💰 Очков: {stats['total_points']:,}"
    )
    await call.message.edit_text(text, reply_markup=kb_admin_panel())


@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("❌", show_alert=True)
        return
    await call.answer()
    await state.set_state(VZStates.admin_broadcast)
    await call.message.edit_text("📨 Отправьте текст для рассылки:")


@router.callback_query(F.data == "admin_channels")
async def cb_admin_channels(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("❌", show_alert=True)
        return
    await call.answer()
    channels = await db.get_required_channels()
    if not channels:
        text = "📋 Каналов нет. Используйте /setchannel"
    else:
        text = "📋 <b>Каналы:</b>\n" + "\n".join(
            f"{i}. {c['channel_name']} — {c['channel_id']}"
            for i, c in enumerate(channels, 1)
        )
    await call.message.edit_text(text, reply_markup=kb_admin_panel())


@router.callback_query(F.data == "admin_ban")
async def cb_admin_ban(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("❌", show_alert=True)
        return
    await call.answer()
    await state.set_state(VZStates.admin_ban_id)
    await call.message.edit_text("🚫 Введите user_id для бана:")


@router.message(VZStates.admin_ban_id)
async def handle_admin_ban_id(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await safe_send(message.answer, "❌ Неверный ID.")
        return
    await db.ban_user(target_id)
    await safe_send(message.answer, f"✅ Забанен: <code>{target_id}</code>")
    await safe_send(bot.send_message, target_id, "🚫 Вы заблокированы в боте.")

# ==============================================================
# SECTION 20: CANCEL COMMAND
# ==============================================================
@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    user_id = message.from_user.id
    vz_queue.remove(user_id)
    current = await state.get_state()
    await state.clear()
    if current:
        await safe_send(message.answer, "❌ <b>Действие отменено.</b>", reply_markup=ReplyKeyboardRemove())
    else:
        await safe_send(message.answer, "ℹ️ Нет активного действия для отмены.")

# ==============================================================
# SECTION 21: FALLBACK & ERROR HANDLERS
# ==============================================================
@router.message(StateFilter(None), F.text)
async def fallback_message(message: Message):
    """Handle unknown messages."""
    text = message.text or ""
    if text.startswith("/"):
        await safe_send(
            message.answer,
            "❓ Неизвестная команда. Используйте /start или кнопки меню.",
        )
    # Ignore other messages silently


@router.errors()
async def global_error_handler(event, exception: Exception):
    """Global error handler."""
    logger.error(f"Unhandled error: {type(exception).__name__}: {exception}")
    logger.error(traceback.format_exc())

    try:
        if hasattr(event, "message") and event.message:
            await safe_send(
                event.message.answer,
                "⚠️ <b>Произошла ошибка</b>\n\nПопробуйте снова или нажмите /start",
            )
        elif hasattr(event, "callback_query") and event.callback_query:
            await event.callback_query.answer("⚠️ Ошибка. Попробуйте снова.", show_alert=True)
    except Exception:
        pass

# ==============================================================
# SECTION 22: BOT SETUP & MAIN
# ==============================================================
async def on_startup(bot: Bot):
    """Bot startup actions."""
    await db.init()
    me = await bot.get_me()
    logger.info(f"🚀 Bot @{me.username} started | ID: {me.id}")
    logger.info(f"👑 Admins: {ADMIN_IDS}")
    logger.info(f"📊 DB: {DB_PATH}")

    # Notify admins
    for admin_id in ADMIN_IDS:
        await safe_send(
            bot.send_message,
            admin_id,
            f"✅ <b>Бот запущен!</b>\n\n@{me.username}\nID: <code>{me.id}</code>",
        )


async def on_shutdown(bot: Bot):
    """Bot shutdown actions."""
    logger.info("🛑 Bot shutting down...")
    for admin_id in ADMIN_IDS:
        await safe_send(bot.send_message, admin_id, "🛑 <b>Бот остановлен.</b>")


async def main():
    if not BOT_TOKEN:
        logger.critical("❌ BOT_TOKEN not set in .env file!")
        return

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # Middlewares
    dp.message.middleware(AntiFloodMiddleware())
    dp.callback_query.middleware(AntiFloodMiddleware())

    # Routers
    dp.include_router(router)

    # Lifecycle
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("⚡️ Starting polling...")
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        raise
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise
