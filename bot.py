# -*- coding: utf-8 -*- 
""" 
██╗   ██╗███████╗    ██████╗  ██████╗ ████████╗ 
██║   ██║╚══███╔╝    ██╔══██╗██╔═══██╗╚══██╔══╝ 
██║   ██║  ███╔╝     ██████╔╝██║   ██║   ██║ 
╚██╗ ██╔╝ ███╔╝      ██╔══██╗██║   ██║   ██║ 
 ╚████╔╝ ███████╗    ██████╔╝╚██████╔╝   ██║ 
  ╚═══╝  ╚══════╝    ╚═════╝  ╚═════╝    ╚═╝ 
Telegram Bot — Система Взаимных Подписок (ВЗ) 
Version: 3.0 | aiogram 3.x | Production Ready 
""" 
# ============================================================== 
# IMPORTS 
# ============================================================== 
import asyncio 
import logging 
import os 
import re 
import time 
import traceback 
import uuid 
from collections import deque, defaultdict 
from datetime import datetime 
from typing import Dict, List, Optional, Tuple, Union 
import aiohttp 
import aiosqlite 
from aiogram import Bot, Dispatcher, F, Router 
from aiogram.client.default import DefaultBotProperties 
from aiogram.dispatcher.middlewares.base import BaseMiddleware 
from aiogram.enums import ParseMode, ChatMemberStatus 
from aiogram.exceptions import ( 
    TelegramAPIError, 
    TelegramBadRequest, 
    TelegramForbiddenError, 
    TelegramNotFound, 
    TelegramRetryAfter, 
) 
from aiogram.filters import Command, CommandStart, StateFilter 
from aiogram.fsm.context import FSMContext 
from aiogram.fsm.state import State, StatesGroup 
from aiogram.fsm.storage.memory import MemoryStorage 
from aiogram.types import ( 
    CallbackQuery, 
    ChatMemberAdministrator, 
    ChatMemberOwner, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    KeyboardButton, 
    Message, 
    ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, 
    User, 
) 
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder 
from dotenv import load_dotenv 
# ============================================================== 
# CONFIGURATION 
# ============================================================== 
load_dotenv() 
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "") 
ADMIN_IDS: List[int] = [ 
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit() 
] 
CRYPTOBOT_TOKEN: str = os.getenv("CRYPTOBOT_TOKEN", "") 
VIP_PRICE_CRYPTO: float = float(os.getenv("VIP_PRICE_CRYPTO", "1.0")) 
VIP_PRICE_POINTS: int = int(os.getenv("VIP_PRICE_POINTS", "500")) 
VIP_DURATION_HOURS: int = int(os.getenv("VIP_DURATION_HOURS", "24")) 
POINTS_PER_VZ: int = int(os.getenv("POINTS_PER_VZ", "500")) 
MAX_WARNS: int = int(os.getenv("MAX_WARNS", "3")) 
MUTE_DURATION_MINUTES: int = int(os.getenv("MUTE_DURATION_MINUTES", "60")) 
DB_PATH: str = os.getenv("DB_PATH", "vz_bot.db") 
FLOOD_LIMIT: int = int(os.getenv("FLOOD_LIMIT", "3")) 
FLOOD_WINDOW: int = int(os.getenv("FLOOD_WINDOW", "5")) 
SESSION_TIMEOUT: int = int(os.getenv("SESSION_TIMEOUT", "300")) 
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api" 
# Telegram deep-link admin rights string 
_ADMIN_RIGHTS = ( 
    "change_info+post_messages+edit_messages+delete_messages" 
    "+invite_users+restrict_members+pin_messages+manage_video_chats+manage_chat" 
) 
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
# Global — set after bot.get_me() on startup 
BOT_USERNAME: str = "" 
BOT_ID: int = 0 
# ============================================================== 
# LOGGING 
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
# FSM STATES 
# ============================================================== 
class VZStates(StatesGroup): 
    waiting_bot_added    = State() 
    waiting_channel_link = State() 
    waiting_promo        = State() 
    in_session           = State() 
    admin_broadcast      = State() 
    admin_ban_input      = State() 
# ============================================================== 
# QUEUE SYSTEM 
# ============================================================== 
class VZQueue: 
    """Dual-lane priority queue: VIP and regular.""" 
    def __init__(self): 
        self._vip:     deque = deque() 
        self._regular: deque = deque() 
        self._in_queue: set  = set() 
        self._sessions: Dict[int, int] = {}  # uid -> partner_uid 
    # ── Enqueue ────────────────────────────────────────────── 
    def add(self, user_id: int, data: dict, vip: bool = False) -> bool: 
        if user_id in self._in_queue or user_id in self._sessions: 
            return False 
        entry = {"user_id": user_id, "data": data, "ts": time.time()} 
        (self._vip if vip else self._regular).append(entry) 
        self._in_queue.add(user_id) 
        return True 
    def remove(self, user_id: int) -> bool: 
        self._in_queue.discard(user_id) 
        for q in (self._vip, self._regular): 
            before = len(q) 
            new_q = deque(e for e in q if e["user_id"] != user_id) 
            if len(new_q) < before: 
                q.clear(); q.extend(new_q) 
                return True 
        return False 
    # ── Matching ───────────────────────────────────────────── 
    def pop_pair(self) -> Optional[Tuple[dict, dict]]: 
        """VIP+VIP → VIP+regular → regular+regular.""" 
        if len(self._vip) >= 2: 
            return self._vip.popleft(), self._vip.popleft() 
        if self._vip and self._regular: 
            return self._vip.popleft(), self._regular.popleft() 
        if len(self._regular) >= 2: 
            return self._regular.popleft(), self._regular.popleft() 
        return None 
    def activate_pair(self, uid1: int, uid2: int): 
        self._in_queue.discard(uid1) 
        self._in_queue.discard(uid2) 
        self._sessions[uid1] = uid2 
        self._sessions[uid2] = uid1 
    def end_session(self, uid: int): 
        partner = self._sessions.pop(uid, None) 
        if partner: 
            self._sessions.pop(partner, None) 
    # ── Queries ─────────────────────────────────────────────── 
    def partner_of(self, uid: int) -> Optional[int]: 
        return self._sessions.get(uid) 
    def in_queue(self, uid: int) -> bool: 
        return uid in self._in_queue 
    def in_session(self, uid: int) -> bool: 
        return uid in self._sessions 
    def position(self, uid: int) -> int: 
        for i, e in enumerate((*self._vip, *self._regular)): 
            if e["user_id"] == uid: 
                return i + 1 
        return -1 
    @property 
    def total(self) -> int: 
        return len(self._vip) + len(self._regular) 
    @property 
    def vip_count(self) -> int: 
        return len(self._vip) 
    @property 
    def regular_count(self) -> int: 
        return len(self._regular) 
vz_queue = VZQueue() 
# session_id → {uid1, uid2, promo1, promo2, confirmed: set, ts} 
live_sessions: Dict[str, dict] = {} 
# ============================================================== 
# DATABASE 
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
                    id         INTEGER PRIMARY KEY AUTOINCREMENT, 
                    user_id    INTEGER, 
                    reason     TEXT, 
                    created_at INTEGER DEFAULT (strftime('%s','now')) 
                ) 
            """) 
            await db.execute(""" 
                CREATE TABLE IF NOT EXISTS invoices ( 
                    invoice_id TEXT PRIMARY KEY, 
                    user_id    INTEGER, 
                    amount     REAL, 
                    currency   TEXT DEFAULT 'USDT', 
                    status     TEXT DEFAULT 'pending', 
                    created_at INTEGER DEFAULT (strftime('%s','now')) 
                ) 
            """) 
            await db.commit() 
        logger.info("✅ Database ready") 
    # ── Helpers ─────────────────────────────────────────────── 
    async def _fetchone(self, sql: str, params=()) -> Optional[dict]: 
        async with aiosqlite.connect(self.path) as db: 
            db.row_factory = aiosqlite.Row 
            async with db.execute(sql, params) as cur: 
                row = await cur.fetchone() 
                return dict(row) if row else None 
    async def _fetchall(self, sql: str, params=()) -> List[dict]: 
        async with aiosqlite.connect(self.path) as db: 
            db.row_factory = aiosqlite.Row 
            async with db.execute(sql, params) as cur: 
                rows = await cur.fetchall() 
                return [dict(r) for r in rows] 
    async def _execute(self, sql: str, params=()): 
        async with aiosqlite.connect(self.path) as db: 
            await db.execute(sql, params) 
            await db.commit() 
    # ── Users ───────────────────────────────────────────────── 
    async def get_user(self, uid: int) -> Optional[dict]: 
        return await self._fetchone("SELECT * FROM users WHERE user_id=?", (uid,)) 
    async def ensure_user(self, user: User) -> dict: 
        await self._execute(""" 
            INSERT INTO users (user_id, username, full_name) VALUES (?,?,?) 
            ON CONFLICT(user_id) DO UPDATE SET 
                username=excluded.username, 
                full_name=excluded.full_name 
        """, (user.id, user.username, user.full_name)) 
        return await self.get_user(user.id) 
    async def add_points(self, uid: int, pts: int): 
        await self._execute( 
            "UPDATE users SET points=points+? WHERE user_id=?", (pts, uid) 
        ) 
    async def spend_points(self, uid: int, pts: int) -> bool: 
        user = await self.get_user(uid) 
        if not user or user["points"] < pts: 
            return False 
        await self._execute( 
            "UPDATE users SET points=points-? WHERE user_id=?", (pts, uid) 
        ) 
        return True 
    async def increment_vz(self, uid: int): 
        await self._execute( 
            "UPDATE users SET vz_count=vz_count+1 WHERE user_id=?", (uid,) 
        ) 
    async def add_warn(self, uid: int, reason: str = "") -> int: 
        await self._execute( 
            "UPDATE users SET warns=warns+1 WHERE user_id=?", (uid,) 
        ) 
        await self._execute( 
            "INSERT INTO penalty (user_id, reason) VALUES (?,?)", (uid, reason) 
        ) 
        u = await self.get_user(uid) 
        return u["warns"] if u else 0 
    async def clear_warns(self, uid: int): 
        await self._execute("UPDATE users SET warns=0 WHERE user_id=?", (uid,)) 
    async def set_mute(self, uid: int, until_ts: int): 
        await self._execute( 
            "UPDATE users SET muted_until=?, warns=0 WHERE user_id=?", (until_ts, uid) 
        ) 
    async def set_vip(self, uid: int, until_ts: int): 
        await self._execute( 
            "UPDATE users SET vip_until=? WHERE user_id=?", (until_ts, uid) 
        ) 
    async def ban_user(self, uid: int): 
        await self._execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,)) 
    async def unban_user(self, uid: int): 
        await self._execute("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,)) 
    async def get_all_user_ids(self) -> List[int]: 
        rows = await self._fetchall("SELECT user_id FROM users WHERE is_banned=0") 
        return [r["user_id"] for r in rows] 
    async def get_stats(self) -> dict: 
        now = int(time.time()) 
        total   = (await self._fetchone("SELECT COUNT(*) c FROM users"))["c"] 
        vips    = (await self._fetchone("SELECT COUNT(*) c FROM users WHERE vip_until>?", (now,)))["c"] 
        done    = (await self._fetchone("SELECT COUNT(*) c FROM sessions WHERE status='completed'"))["c"] 
        pts_sum = (await self._fetchone("SELECT COALESCE(SUM(points),0) s FROM users"))["s"] 
        return {"total": total, "vip": vips, "sessions": done, "points": pts_sum} 
    # ── Channels ────────────────────────────────────────────── 
    async def add_channel(self, cid: str, name: str, admin: int): 
        await self._execute(""" 
            INSERT OR REPLACE INTO required_channels (channel_id, channel_name, added_by) 
            VALUES (?,?,?) 
        """, (cid, name, admin)) 
    async def remove_channel(self, cid: str): 
        await self._execute( 
            "DELETE FROM required_channels WHERE channel_id=?", (cid,) 
        ) 
    async def get_channels(self) -> List[dict]: 
        return await self._fetchall("SELECT * FROM required_channels") 
    # ── Sessions ────────────────────────────────────────────── 
    async def save_session(self, sid: str, u1: int, u2: int, p1: str, p2: str): 
        await self._execute(""" 
            INSERT OR REPLACE INTO sessions 
            (session_id, user1_id, user2_id, user1_promo, user2_promo) 
            VALUES (?,?,?,?,?) 
        """, (sid, u1, u2, p1, p2)) 
    async def close_session(self, sid: str, status: str): 
        await self._execute(""" 
            UPDATE sessions 
            SET status=?, finished_at=strftime('%s','now') 
            WHERE session_id=? 
        """, (status, sid)) 
    # ── Invoices ────────────────────────────────────────────── 
    async def save_invoice(self, iid: str, uid: int, amount: float, currency: str = "USDT"): 
        await self._execute(""" 
            INSERT INTO invoices (invoice_id, user_id, amount, currency) 
            VALUES (?,?,?,?) 
        """, (iid, uid, amount, currency)) 
    async def get_invoice(self, iid: str) -> Optional[dict]: 
        return await self._fetchone( 
            "SELECT * FROM invoices WHERE invoice_id=?", (iid,) 
        ) 
    async def mark_invoice_paid(self, iid: str): 
        await self._execute( 
            "UPDATE invoices SET status='paid' WHERE invoice_id=?", (iid,) 
        ) 
db = Database(DB_PATH) 
# ============================================================== 
# ANTI-FLOOD MIDDLEWARE 
# ============================================================== 
class AntiFloodMiddleware(BaseMiddleware): 
    def __init__(self, limit: int = FLOOD_LIMIT, window: int = FLOOD_WINDOW): 
        self.limit  = limit 
        self.window = window 
        self._log: Dict[int, List[float]] = defaultdict(list) 
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
        self._log[uid] = [t for t in self._log[uid] if now - t < self.window] 
        self._log[uid].append(now) 
        if len(self._log[uid]) > self.limit: 
            if isinstance(event, Message): 
                await safe_answer(event, "⏳ Не так быстро — подождите немного.") 
            elif isinstance(event, CallbackQuery): 
                await event.answer("⏳ Не так быстро!", show_alert=False) 
            return 
        return await handler(event, data) 
# ============================================================== 
# CRYPTOBOT API 
# ============================================================== 
class CryptoBotAPI: 
    def __init__(self, token: str): 
        self.token   = token 
        self.headers = {"Crypto-Pay-API-Token": token} 
    @property 
    def enabled(self) -> bool: 
        return bool(self.token) 
    async def create_invoice( 
        self, 
        amount: float, 
        currency: str = "USDT", 
        payload: str = "", 
        description: str = "", 
    ) -> Optional[dict]: 
        if not self.enabled: 
            return None 
        body = { 
            "asset":         currency, 
            "amount":        str(round(amount, 2)), 
            "payload":       payload, 
            "description":   description or f"VIP на {VIP_DURATION_HOURS}ч — VZ Bot", 
            "paid_btn_name": "callback", 
            "paid_btn_url":  f"https://t.me/{BOT_USERNAME}", 
        } 
        try: 
            async with aiohttp.ClientSession() as s: 
                async with s.post( 
                    f"{CRYPTOBOT_API_URL}/createInvoice", 
                    json=body, 
                    headers=self.headers, 
                    timeout=aiohttp.ClientTimeout(total=12), 
                ) as resp: 
                    data = await resp.json() 
            if data.get("ok"): 
                return data["result"] 
            logger.error(f"CryptoBot createInvoice error: {data}") 
            return None 
        except Exception as e: 
            logger.error(f"CryptoBot request failed: {e}") 
            return None 
    async def get_invoice_status(self, invoice_id: Union[str, int]) -> Optional[str]: 
        """Returns 'active' | 'paid' | 'expired' | None.""" 
        if not self.enabled: 
            return None 
        try: 
            async with aiohttp.ClientSession() as s: 
                async with s.get( 
                    f"{CRYPTOBOT_API_URL}/getInvoices", 
                    params={"invoice_ids": str(invoice_id)}, 
                    headers=self.headers, 
                    timeout=aiohttp.ClientTimeout(total=10), 
                ) as resp: 
                    data = await resp.json() 
            if data.get("ok"): 
                items = data["result"].get("items", []) 
                if items: 
                    return items[0].get("status") 
            return None 
        except Exception as e: 
            logger.error(f"CryptoBot getInvoices failed: {e}") 
            return None 
cryptobot = CryptoBotAPI(CRYPTOBOT_TOKEN) 
# ============================================================== 
# UTILITY FUNCTIONS 
# ============================================================== 
async def safe_send(func, *args, **kwargs) -> Optional[Message]: 
    """Call any send/edit coroutine with FloodWait retry + error swallow.""" 
    for attempt in range(3): 
        try: 
            return await func(*args, **kwargs) 
        except TelegramRetryAfter as e: 
            wait = e.retry_after + 1 
            logger.warning(f"FloodWait {wait}s (attempt {attempt+1})") 
            await asyncio.sleep(wait) 
        except TelegramForbiddenError: 
            return None 
        except TelegramBadRequest as e: 
            logger.debug(f"BadRequest: {e}") 
            return None 
        except TelegramAPIError as e: 
            logger.warning(f"APIError: {e}") 
            return None 
        except Exception as e: 
            logger.error(f"safe_send unexpected: {e}") 
            return None 
    return None 
async def safe_answer(message: Message, text: str, **kwargs) -> Optional[Message]: 
    return await safe_send(message.answer, text, **kwargs) 
async def safe_edit(message: Message, text: str, **kwargs) -> Optional[Message]: 
    return await safe_send(message.edit_text, text, **kwargs) 
def now_ts() -> int: 
    return int(time.time()) 
def is_vip(u: dict) -> bool: 
    return u.get("vip_until", 0) > now_ts() 
def is_muted(u: dict) -> bool: 
    return u.get("muted_until", 0) > now_ts() 
def mute_remaining_str(u: dict) -> str: 
    secs = max(0, u.get("muted_until", 0) - now_ts()) 
    if secs == 0: 
        return "0 сек" 
    m, s = divmod(secs, 60) 
    return f"{m} мин {s} сек" if m else f"{s} сек" 
def vip_link(for_channel: bool = True) -> str: 
    kind = "startchannel" if for_channel else "startgroup" 
    return f"https://t.me/{BOT_USERNAME}?{kind}=true&admin={_ADMIN_RIGHTS}" 
def short_id() -> str: 
    return uuid.uuid4().hex[:8] 
def fmt_ts(ts: int) -> str: 
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M") 
def fmt_profile(u: dict) -> str: 
    uid       = u["user_id"] 
    name      = u.get("full_name") or "—" 
    username  = f"@{u['username']}" if u.get("username") else "—" 
    pts       = u.get("points", 0) 
    vz_cnt    = u.get("vz_count", 0) 
    warns     = u.get("warns", 0) 
    vip_line  = f"✅ VIP до {fmt_ts(u['vip_until'])}" if is_vip(u) else "❌ Нет VIP" 
    mute_line = f"🔇 Мут до {fmt_ts(u['muted_until'])}" if is_muted(u) else "✅ Активен" 
    return ( 
        f"👤 <b>Профиль</b>\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"🆔 <code>{uid}</code>  │  {name}\n" 
        f"🔗 {username}\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"💰 Очков:            <b>{pts:,}</b>\n" 
        f"🔄 Успешных ВЗ:      <b>{vz_cnt}</b>\n" 
        f"⚠️ Предупреждения:   <b>{warns}/{MAX_WARNS}</b>\n" 
        f"👑 Статус:           {vip_line}\n" 
        f"🎙 Доступ:           {mute_line}\n" 
        f"━━━━━━━━━━━━━━━━━━━━" 
    ) 
async def check_subscriptions(bot: Bot, uid: int) -> List[dict]: 
    """Return list of channels the user is NOT subscribed to.""" 
    channels = await db.get_channels() 
    missing = [] 
    for ch in channels: 
        try: 
            member = await bot.get_chat_member(ch["channel_id"], uid) 
            if member.status in ( 
                ChatMemberStatus.LEFT, 
                ChatMemberStatus.KICKED, 
                ChatMemberStatus.BANNED if hasattr(ChatMemberStatus, "BANNED") else "kicked", 
            ): 
                missing.append(ch) 
        except Exception: 
            missing.append(ch) 
    return missing 
async def resolve_channel(bot: Bot, raw: str) -> Optional[dict]: 
    """Resolve link / @username / ID to a chat dict.""" 
    raw = raw.strip() 
    chat_id: Union[str, int] = raw 
    # Extract from t.me URL 
    m = re.search(r"(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,})", raw) 
    if m: 
        chat_id = f"@{m.group(1)}" 
    # Bare username without @ 
    elif re.match(r"^[a-zA-Z][a-zA-Z0-9_]{4,}$", raw): 
        chat_id = f"@{raw}" 
    # Try numeric ID 
    try: 
        chat_id = int(raw) 
    except (ValueError, TypeError): 
        pass 
    try: 
        chat = await bot.get_chat(chat_id) 
        return { 
            "id":       chat.id, 
            "title":    chat.title or chat.full_name or str(chat.id), 
            "username": chat.username, 
            "type":     chat.type, 
        } 
    except Exception: 
        return None 
async def check_bot_admin(bot: Bot, chat_id: Union[int, str]) -> Tuple[bool, List[str]]: 
    """Returns (ok, missing_perms).""" 
    try: 
        member = await bot.get_chat_member(chat_id, BOT_ID) 
    except Exception: 
        return False, list(REQUIRED_PERMISSIONS) 
    if isinstance(member, ChatMemberOwner): 
        return True, [] 
    if not isinstance(member, ChatMemberAdministrator): 
        return False, list(REQUIRED_PERMISSIONS) 
    missing = [p for p in REQUIRED_PERMISSIONS if not getattr(member, p, False)] 
    return len(missing) == 0, missing 

def make_clickable_promo(promo: str) -> str:
    """
    Return a clickable version of promo text for HTML parse mode.
    - If it's already a URL (http/https) — return as-is (Telegram auto-links it).
    - If it's a @username — wrap in <a href> so it's always clickable.
    - Otherwise — return plain text.
    """
    promo = promo.strip()
    # Already a full URL
    if promo.startswith("http://") or promo.startswith("https://"):
        return promo
    # @username
    if promo.startswith("@"):
        handle = promo.lstrip("@")
        return f'<a href="https://t.me/{handle}">{promo}</a>'
    # t.me/username without scheme
    m = re.match(r"^(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,})", promo)
    if m:
        return f'<a href="https://{promo}">{promo}</a>'
    # Plain text — return as-is
    return promo

# ============================================================== 
# KEYBOARDS 
# ============================================================== 
def kb_main(vip: bool = False) -> ReplyKeyboardMarkup: 
    b = ReplyKeyboardBuilder() 
    b.row(KeyboardButton(text="🔄 Начать ВЗ")) 
    b.row(KeyboardButton(text="👤 Профиль"), KeyboardButton(text="👑 VIP")) 
    b.row(KeyboardButton(text="📊 Статистика"), KeyboardButton(text="ℹ️ Помощь")) 
    return b.as_markup(resize_keyboard=True) 
def kb_add_bot_to_channel() -> InlineKeyboardMarkup: 
    """Step-1 keyboard: buttons to add bot with full admin rights + confirm.""" 
    b = InlineKeyboardBuilder() 
    b.button(text="📢 Добавить в канал (полные права)", url=vip_link(for_channel=True)) 
    b.button(text="👥 Добавить в группу (полные права)", url=vip_link(for_channel=False)) 
    b.button(text="✅ Бот добавлен, продолжить", callback_data="bot_added_confirm") 
    b.button(text="❌ Отмена", callback_data="vz_cancel") 
    b.adjust(1) 
    return b.as_markup() 
def kb_cancel() -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    b.button(text="❌ Отмена", callback_data="vz_cancel") 
    return b.as_markup() 
def kb_session(session_id: str, role: str) -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    b.button(text="✅ Подписался — готово!", callback_data=f"ses_done:{session_id}:{role}") 
    b.button(text="❌ Отмена",              callback_data=f"ses_cancel:{session_id}:{role}") 
    b.adjust(1) 
    return b.as_markup() 
def kb_vip_shop(has_points: bool) -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    if has_points: 
        b.button(text=f"💰 Купить за {VIP_PRICE_POINTS} очков", callback_data="vip_points") 
    b.button(text=f"💳 Купить за {VIP_PRICE_CRYPTO} USDT",     callback_data="vip_crypto") 
    b.button(text="🔙 Назад", callback_data="vip_close") 
    b.adjust(1) 
    return b.as_markup() 
def kb_sub_check(channels: List[dict]) -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    for ch in channels: 
        cid = ch["channel_id"] 
        url = ( 
            f"https://t.me/{cid.lstrip('@')}" 
            if str(cid).startswith("@") 
            else f"https://t.me/c/{str(cid).lstrip('-100')}" 
        ) 
        b.button(text=f"📢 {ch['channel_name']}", url=url) 
    b.button(text="✅ Подписался — проверить", callback_data="sub_recheck") 
    b.adjust(1) 
    return b.as_markup() 
def kb_invoice(pay_url: str, invoice_id: str) -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    b.button(text="💳 Перейти к оплате",    url=pay_url) 
    b.button(text="🔄 Проверить оплату",    callback_data=f"pay_check:{invoice_id}") 
    b.button(text="❌ Отмена",               callback_data="vip_close") 
    b.adjust(1) 
    return b.as_markup() 
def kb_admin() -> InlineKeyboardMarkup: 
    b = InlineKeyboardBuilder() 
    b.button(text="📊 Статистика",  callback_data="adm_stats") 
    b.button(text="📨 Рассылка",    callback_data="adm_broadcast") 
    b.button(text="🚫 Бан",         callback_data="adm_ban") 
    b.button(text="📋 Каналы",      callback_data="adm_channels") 
    b.button(text="📋 Очередь",     callback_data="adm_queue") 
    b.adjust(2) 
    return b.as_markup() 
# ============================================================== 
# GUARDS 
# ============================================================== 
async def guard(message: Message, bot: Bot) -> bool: 
    """ 
    Run all standard checks (ban, mute, subscriptions). 
    Returns True if execution should STOP. 
    """ 
    u = await db.ensure_user(message.from_user) 
    if u.get("is_banned"): 
        await safe_answer(message, "🚫 <b>Вы заблокированы в боте.</b>") 
        return True 
    if is_muted(u): 
        rem = mute_remaining_str(u) 
        await safe_answer( 
            message, 
            f"🔇 <b>Вы в муте</b>\n\n" 
            f"Нарушений было слишком много.\n" 
            f"⏳ Снятие через: <b>{rem}</b>", 
        ) 
        return True 
    missing = await check_subscriptions(bot, message.from_user.id) 
    if missing: 
        text = ( 
            "📢 <b>Требуется подписка</b>\n\n" 
            "Для использования бота необходимо подписаться на каналы:\n" 
        ) 
        for ch in missing: 
            text += f"  • {ch['channel_name']}\n" 
        await safe_answer(message, text, reply_markup=kb_sub_check(missing)) 
        return True 
    return False 
# ============================================================== 
# WARN / MUTE LOGIC 
# ============================================================== 
async def apply_warn(bot: Bot, uid: int, reason: str): 
    """Add warn, send message, apply mute if threshold reached.""" 
    warns = await db.add_warn(uid, reason) 
    if warns >= MAX_WARNS: 
        until = now_ts() + MUTE_DURATION_MINUTES * 60 
        await db.set_mute(uid, until) 
        await safe_send( 
            bot.send_message, uid, 
            f"🔇 <b>Вы получили мут на {MUTE_DURATION_MINUTES} мин</b>\n\n" 
            f"Причина: {reason}\n" 
            f"Предупреждений набрано: {warns}/{MAX_WARNS}\n" 
            f"⏳ Снятие: {fmt_ts(until)}", 
        ) 
    else: 
        tail = ( 
            "❗️ Следующее нарушение — мут!" 
            if warns == MAX_WARNS - 1 
            else f"После {MAX_WARNS} предупреждений — мут на {MUTE_DURATION_MINUTES} мин." 
        ) 
        await safe_send( 
            bot.send_message, uid, 
            f"⚠️ <b>Предупреждение {warns}/{MAX_WARNS}</b>\n\n" 
            f"Причина: {reason}\n" 
            f"{tail}", 
        ) 
# ============================================================== 
# ROUTER 
# ============================================================== 
router = Router() 
# ============================================================== 
# /start  &  MAIN MENU 
# ============================================================== 
@router.message(CommandStart()) 
async def cmd_start(message: Message, bot: Bot, state: FSMContext): 
    await state.clear() 
    u = await db.ensure_user(message.from_user) 
    if u.get("is_banned"): 
        await safe_answer(message, "🚫 <b>Вы заблокированы.</b>") 
        return 
    vip_badge = " 👑" if is_vip(u) else "" 
    await safe_answer( 
        message, 
        f"👋 Привет, <b>{message.from_user.first_name}{vip_badge}</b>!\n\n" 
        f"🔄 <b>VZ Bot</b> — сервис взаимных подписок\n\n" 
        f"Пиарь свой канал, бота или чат — получай живых подписчиков в обмен.\n\n" 
        f"💰 За каждое ВЗ: <b>+{POINTS_PER_VZ} очков</b>\n" 
        f"👑 VIP = приоритет в очереди\n\n" 
        f"⬇️ Выберите действие:", 
        reply_markup=kb_main(is_vip(u)), 
    ) 
@router.message(F.text == "👤 Профиль") 
async def menu_profile(message: Message, bot: Bot): 
    if await guard(message, bot): 
        return 
    u = await db.get_user(message.from_user.id) 
    await safe_answer(message, fmt_profile(u)) 
@router.message(F.text == "📊 Статистика") 
async def menu_stats(message: Message, bot: Bot): 
    if await guard(message, bot): 
        return 
    s = await db.get_stats() 
    await safe_answer( 
        message, 
        f"📊 <b>Статистика</b>\n\n" 
        f"👥 Пользователей:  <b>{s['total']:,}</b>\n" 
        f"👑 VIP активных:   <b>{s['vip']:,}</b>\n" 
        f"🔄 Завершено ВЗ:   <b>{s['sessions']:,}</b>\n" 
        f"💰 Очков выдано:   <b>{s['points']:,}</b>\n\n" 
        f"🟢 В очереди:      <b>{vz_queue.total}</b> чел.", 
    ) 
@router.message(F.text == "ℹ️ Помощь") 
async def menu_help(message: Message): 
    await safe_answer( 
        message, 
        "📖 <b>Как работает ВЗ</b>\n\n" 
        "1️⃣ Нажмите «🔄 Начать ВЗ»\n" 
        "2️⃣ Добавьте бота в канал нажатием кнопки\n" 
        "3️⃣ Укажите ссылку на ваш канал\n" 
        "4️⃣ Укажите что хотите пиарить\n" 
        "5️⃣ Ожидайте партнёра в очереди\n" 
        "6️⃣ Подпишитесь на канал партнёра\n" 
        "7️⃣ Нажмите «✅ Готово» и получите очки!\n\n" 
        "<b>Правила:</b>\n" 
        f"⚠️ Срыв / отмена сессии = предупреждение\n" 
        f"🔇 {MAX_WARNS} предупреждения → мут {MUTE_DURATION_MINUTES} мин\n" 
        f"💰 Успешное ВЗ = +{POINTS_PER_VZ} очков\n\n" 
        "<b>VIP:</b>\n" 
        f"• {VIP_PRICE_POINTS} очков или {VIP_PRICE_CRYPTO} USDT\n" 
        "• Приоритет в очереди на 24 часа", 
    ) 
# ============================================================== 
# SUBSCRIPTION RECHECK 
# ============================================================== 
@router.callback_query(F.data == "sub_recheck") 
async def cb_sub_recheck(call: CallbackQuery, bot: Bot): 
    await call.answer() 
    missing = await check_subscriptions(bot, call.from_user.id) 
    if missing: 
        names = ", ".join(ch["channel_name"] for ch in missing) 
        await call.message.edit_text( 
            f"❌ <b>Не подписаны на:</b> {names}\n\nПодпишитесь и проверьте снова.", 
            reply_markup=kb_sub_check(missing), 
        ) 
    else: 
        await call.message.edit_text("✅ <b>Подписка подтверждена!</b>\nТеперь можете пользоваться ботом.") 
# ============================================================== 
# VZ FLOW — STEP 1: ADD BOT 
# ============================================================== 
@router.message(F.text == "🔄 Начать ВЗ") 
async def vz_begin(message: Message, bot: Bot, state: FSMContext): 
    await state.clear() 
    uid = message.from_user.id 
    if await guard(message, bot): 
        return 
    if vz_queue.in_queue(uid): 
        await safe_answer(message, "⏳ <b>Вы уже в очереди.</b>\nОжидайте партнёра.") 
        return 
    if vz_queue.in_session(uid): 
        await safe_answer(message, "🔄 <b>У вас уже есть активная сессия.</b>") 
        return 
    await state.set_state(VZStates.waiting_bot_added) 
    await safe_answer( 
        message, 
        "📋 <b>Начало ВЗ — Шаг 1 из 3</b>\n\n" 
        "Добавьте бота в ваш канал или группу как <b>администратора с полными правами</b>.\n\n" 
        "Нажмите нужную кнопку ниже — откроется диалог Telegram с уже выбранными правами 👇", 
        reply_markup=kb_add_bot_to_channel(), 
    ) 
@router.callback_query(F.data == "bot_added_confirm", VZStates.waiting_bot_added) 
async def cb_bot_added(call: CallbackQuery, state: FSMContext): 
    await call.answer() 
    await state.set_state(VZStates.waiting_channel_link) 
    await call.message.edit_text( 
        "🔗 <b>Шаг 2 из 3 — Укажите ваш канал</b>\n\n" 
        "Отправьте одно из:\n" 
        "  • Ссылку: <code>https://t.me/mychannel</code>\n" 
        "  • Username: <code>@mychannel</code>\n" 
        "  • ID: <code>-1001234567890</code>\n\n" 
        "⚠️ Это должен быть канал/группа, куда вы только что добавили бота.", 
        reply_markup=kb_cancel(), 
    ) 
# ============================================================== 
# VZ FLOW — STEP 2: VERIFY CHANNEL 
# ============================================================== 
@router.message(VZStates.waiting_channel_link) 
async def vz_channel_input(message: Message, bot: Bot, state: FSMContext): 
    raw = (message.text or "").strip() 
    if not raw: 
        await safe_answer(message, "❌ Отправьте ссылку, @username или ID.", reply_markup=kb_cancel()) 
        return 
    # Show spinner 
    spinner = await safe_answer(message, "🔍 <b>Проверяю канал...</b>") 
    chat = await resolve_channel(bot, raw) 
    if not chat: 
        text = ( 
            "❌ <b>Канал не найден</b>\n\n" 
            "Убедитесь, что:\n" 
            "  • Канал существует\n" 
            "  • Вы добавили бота раньше\n" 
            "  • Ссылка/username правильные\n\n" 
            "Попробуйте ещё раз:" 
        ) 
        if spinner: 
            await safe_edit(spinner, text, reply_markup=kb_cancel()) 
        return 
    ok, missing_perms = await check_bot_admin(bot, chat["id"]) 
    if not ok: 
        if missing_perms == REQUIRED_PERMISSIONS: 
            err = ( 
                f"❌ <b>Бот не является администратором</b>\n\n" 
                f"Канал: <b>{chat['title']}</b>\n\n" 
                f"Воспользуйтесь кнопкой «Добавить в канал» выше — " 
                f"она откроет диалог с уже выбранными правами." 
            ) 
        else: 
            lines = "\n".join(f"  ❌ {PERMISSION_LABELS[p]}" for p in missing_perms) 
            err = ( 
                f"⚠️ <b>Недостаточно прав</b>\n\n" 
                f"Канал: <b>{chat['title']}</b>\n\n" 
                f"<b>Отсутствуют:</b>\n{lines}\n\n" 
                f"Выдайте все права и попробуйте снова." 
            ) 
        if spinner: 
            await safe_edit(spinner, err, reply_markup=kb_cancel()) 
        return 
    # Save and proceed 
    await state.update_data(channel=chat) 
    await state.set_state(VZStates.waiting_promo) 
    chat_link = f"@{chat['username']}" if chat.get("username") else f"ID {chat['id']}" 
    text = ( 
        f"✅ <b>Канал подтверждён!</b>\n\n" 
        f"📢 <b>{chat['title']}</b>  ({chat_link})\n" 
        f"🤖 Бот — администратор с нужными правами ✅\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"📣 <b>Шаг 3 из 3 — Что пиарим?</b>\n\n" 
        f"Отправьте ссылку, @username, текст или название того, что хотите продвигать.\n" 
        f"Это увидит ваш партнёр по ВЗ." 
    ) 
    if spinner: 
        await safe_edit(spinner, text, reply_markup=kb_cancel()) 
# ============================================================== 
# VZ FLOW — STEP 3: PROMO CONTENT → QUEUE 
# ============================================================== 
@router.message(VZStates.waiting_promo) 
async def vz_promo_input(message: Message, bot: Bot, state: FSMContext): 
    uid  = message.from_user.id 
    u    = await db.get_user(uid) 
    promo = "" 
    if message.text: 
        promo = message.text.strip() 
    elif message.caption: 
        promo = message.caption.strip() 
    elif message.forward_from_chat and message.forward_from_chat.username: 
        promo = f"@{message.forward_from_chat.username}" 
    else: 
        await safe_answer(message, "❌ Отправьте текст, ссылку или @username.", reply_markup=kb_cancel()) 
        return 
    if len(promo) > 500: 
        await safe_answer(message, "❌ Слишком длинно (макс. 500 символов).", reply_markup=kb_cancel()) 
        return 
    state_data = await state.get_data() 
    channel    = state_data.get("channel", {}) 
    vip_user   = is_vip(u) if u else False 
    added = vz_queue.add(uid, { 
        "user_id":   uid, 
        "promo":     promo, 
        "channel":   channel, 
        "user_name": message.from_user.full_name, 
        "is_vip":    vip_user, 
    }, vip=vip_user) 
    await state.clear() 
    if not added: 
        await safe_answer(message, "⚠️ Вы уже в очереди или активной сессии.") 
        return 
    pos       = vz_queue.position(uid) 
    vip_badge = " 👑" if vip_user else "" 
    await safe_answer( 
        message, 
        f"⏳ <b>Вы в очереди{vip_badge}!</b>\n\n" 
        f"📢 Пиарим: {make_clickable_promo(promo)}\n" 
        f"📋 Позиция: <b>{pos}</b>\n\n" 
        f"Как только найдётся пара — получите уведомление.\n" 
        f"💡 <i>Не выходите из бота!</i>", 
    ) 
    asyncio.create_task(try_match(bot)) 
# ============================================================== 
# QUEUE MATCHING 
# ============================================================== 
async def try_match(bot: Bot): 
    pair = vz_queue.pop_pair() 
    if not pair: 
        return 
    e1, e2   = pair 
    uid1     = e1["user_id"] 
    uid2     = e2["user_id"] 
    promo1   = e1["data"]["promo"] 
    promo2   = e2["data"]["promo"] 
    name1    = e1["data"].get("user_name", "Партнёр") 
    name2    = e2["data"].get("user_name", "Партнёр") 
    sid      = short_id() 
    vz_queue.activate_pair(uid1, uid2) 
    await db.save_session(sid, uid1, uid2, promo1, promo2) 
    live_sessions[sid] = { 
        "uid1":      uid1, 
        "uid2":      uid2, 
        "promo1":    promo1, 
        "promo2":    promo2, 
        "confirmed": set(), 
        "ts":        now_ts(), 
    } 
    timeout_mins = SESSION_TIMEOUT // 60 
    # FIX: use make_clickable_promo so links are tappable, not plain <code> text
    await safe_send( 
        bot.send_message, uid1, 
        f"🎉 <b>Найден партнёр!</b>\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"👤 Партнёр: <b>{name2}</b>\n\n" 
        f"📢 <b>Подпишитесь на:</b>\n{make_clickable_promo(promo2)}\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"После подписки нажмите ✅\n" 
        f"⏳ Сессия истекает через <b>{timeout_mins} мин</b>", 
        reply_markup=kb_session(sid, "u1"), 
    ) 
    await safe_send( 
        bot.send_message, uid2, 
        f"🎉 <b>Найден партнёр!</b>\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"👤 Партнёр: <b>{name1}</b>\n\n" 
        f"📢 <b>Подпишитесь на:</b>\n{make_clickable_promo(promo1)}\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"После подписки нажмите ✅\n" 
        f"⏳ Сессия истекает через <b>{timeout_mins} мин</b>", 
        reply_markup=kb_session(sid, "u2"), 
    ) 
    asyncio.create_task(session_expire(bot, sid, SESSION_TIMEOUT)) 
# ============================================================== 
# SESSION: CONFIRM 
# ============================================================== 
@router.callback_query(F.data.startswith("ses_done:")) 
async def cb_session_done(call: CallbackQuery, bot: Bot): 
    _, sid, role = call.data.split(":") 
    uid          = call.from_user.id 
    session      = live_sessions.get(sid) 
    if not session: 
        await call.answer("❌ Сессия не найдена или истекла.", show_alert=True) 
        return 
    uid1       = session["uid1"] 
    uid2       = session["uid2"] 
    partner_id = uid2 if role == "u1" else uid1 
    # Verify subscription if possible 
    target_promo = session["promo2"] if role == "u1" else session["promo1"] 
    verified     = True 
    check_id: Optional[str] = None 
    if target_promo.startswith("@"): 
        check_id = target_promo 
    else: 
        m = re.search(r"t\.me/([a-zA-Z0-9_]{5,})$", target_promo) 
        if m: 
            check_id = f"@{m.group(1)}" 
    if check_id: 
        try: 
            member = await bot.get_chat_member(check_id, uid) 
            verified = member.status not in ( 
                ChatMemberStatus.LEFT, 
                ChatMemberStatus.KICKED, 
            ) 
        except Exception: 
            verified = True  # can't verify → accept on trust 
    if not verified: 
        await call.answer( 
            f"❌ Вы не подписаны на {check_id}!\nПодпишитесь и нажмите снова.", 
            show_alert=True, 
        ) 
        return 
    await call.answer("✅ Подписка подтверждена!") 
    session["confirmed"].add(uid) 
    await call.message.edit_text( 
        f"✅ <b>Ваша подписка засчитана!</b>\n\nОжидаем подтверждения от партнёра...", 
    ) 
    await safe_send( 
        bot.send_message, partner_id, 
        "ℹ️ <b>Партнёр уже подписался!</b>\nОформите подписку и нажмите ✅", 
    ) 
    # Both confirmed? 
    if uid1 in session["confirmed"] and uid2 in session["confirmed"]: 
        await finalize_session(bot, sid) 
# ============================================================== 
# SESSION: CANCEL 
# ============================================================== 
@router.callback_query(F.data.startswith("ses_cancel:")) 
async def cb_session_cancel(call: CallbackQuery, bot: Bot): 
    _, sid, role = call.data.split(":") 
    uid          = call.from_user.id 
    session      = live_sessions.pop(sid, None) 
    if not session: 
        await call.answer("Сессия уже завершена.", show_alert=True) 
        return 
    uid1       = session["uid1"] 
    uid2       = session["uid2"] 
    partner_id = uid2 if role == "u1" else uid1 
    vz_queue.end_session(uid1) 
    vz_queue.end_session(uid2) 
    await db.close_session(sid, "cancelled") 
    await call.answer() 
    await call.message.edit_text( 
        "❌ <b>Сессия отменена</b>\n\n⚠️ Вам начислено предупреждение." 
    ) 
    await apply_warn(bot, uid, "Отмена сессии ВЗ") 
    await safe_send( 
        bot.send_message, partner_id, 
        "😔 <b>Партнёр отменил сессию.</b>\nМожете начать новую ВЗ.", 
    ) 
# ============================================================== 
# SESSION: FINALIZE & EXPIRE 
# ============================================================== 
async def finalize_session(bot: Bot, sid: str): 
    session = live_sessions.pop(sid, None) 
    if not session: 
        return 
    uid1 = session["uid1"] 
    uid2 = session["uid2"] 
    vz_queue.end_session(uid1) 
    vz_queue.end_session(uid2) 
    await db.close_session(sid, "completed") 
    await db.add_points(uid1, POINTS_PER_VZ) 
    await db.add_points(uid2, POINTS_PER_VZ) 
    await db.increment_vz(uid1) 
    await db.increment_vz(uid2) 
    msg = ( 
        f"🎊 <b>ВЗ успешно завершено!</b>\n\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n" 
        f"💰 Начислено: <b>+{POINTS_PER_VZ} очков</b>\n" 
        f"🏆 Отличная работа!\n" 
        f"━━━━━━━━━━━━━━━━━━━━\n\n" 
        f"Нажмите «🔄 Начать ВЗ» для новой сессии." 
    ) 
    await safe_send(bot.send_message, uid1, msg) 
    await safe_send(bot.send_message, uid2, msg) 
    logger.info(f"✅ Session {sid} completed: {uid1} ↔ {uid2}") 
async def session_expire(bot: Bot, sid: str, delay: int): 
    await asyncio.sleep(delay) 
    session = live_sessions.pop(sid, None) 
    if not session: 
        return  # already closed 
    uid1      = session["uid1"] 
    uid2      = session["uid2"] 
    confirmed = session["confirmed"] 
    vz_queue.end_session(uid1) 
    vz_queue.end_session(uid2) 
    await db.close_session(sid, "expired") 
    for uid in (uid1, uid2): 
        if uid in confirmed: 
            await safe_send(bot.send_message, uid, 
                "⏰ <b>Сессия истекла.</b>\nПартнёр не подтвердил. Можете начать новую ВЗ.") 
        else: 
            await apply_warn(bot, uid, "Сессия истекла без подтверждения") 
# ============================================================== 
# VZ CANCEL (state) 
# ============================================================== 
@router.callback_query(F.data == "vz_cancel") 
async def cb_vz_cancel(call: CallbackQuery, state: FSMContext): 
    await call.answer() 
    vz_queue.remove(call.from_user.id) 
    await state.clear() 
    await call.message.edit_text("❌ <b>Отменено.</b>\nВернитесь в главное меню.") 
# ============================================================== 
# VIP MENU 
# ============================================================== 
@router.message(F.text == "👑 VIP") 
async def menu_vip(message: Message, bot: Bot): 
    if await guard(message, bot): 
        return 
    u = await db.get_user(message.from_user.id) 
    if is_vip(u): 
        await safe_answer( 
            message, 
            f"👑 <b>Ваш VIP активен</b>\n\n" 
            f"⏳ Действует до: <b>{fmt_ts(u['vip_until'])}</b>\n\n" 
            f"<b>Привилегии:</b>\n" 
            f"  ⚡️ Приоритет в очереди\n" 
            f"  🔝 Отдельная VIP-очередь\n" 
            f"  ♾ Без ограничений", 
        ) 
        return 
    pts      = u.get("points", 0) 
    enough   = pts >= VIP_PRICE_POINTS 
    pts_line = f"💰 {VIP_PRICE_POINTS} очков  (у вас: <b>{pts:,}</b>)" 
    if not enough: 
        pts_line += f"\n  ↳ Не хватает: {VIP_PRICE_POINTS - pts} очков" 
    await safe_answer( 
        message, 
        f"👑 <b>VIP режим — {VIP_DURATION_HOURS} часов</b>\n\n" 
        f"<b>Что даёт VIP:</b>\n" 
        f"  ⚡️ Приоритет в очереди\n" 
        f"  🔝 Попадаете в пару первыми\n" 
        f"  ♾ Безлимитный доступ\n\n" 
        f"<b>Стоимость:</b>\n" 
        f"  {pts_line}\n" 
        f"  💳 {VIP_PRICE_CRYPTO} USDT через CryptoBot\n\n" 
        f"Выберите способ оплаты:", 
        reply_markup=kb_vip_shop(has_points=enough), 
    ) 
@router.callback_query(F.data == "vip_points") 
async def cb_vip_points(call: CallbackQuery): 
    await call.answer() 
    uid = call.from_user.id 
    u   = await db.get_user(uid) 
    if is_vip(u): 
        await call.answer("✅ VIP уже активен!", show_alert=True) 
        return 
    ok = await db.spend_points(uid, VIP_PRICE_POINTS) 
    if not ok: 
        await call.answer( 
            f"❌ Недостаточно очков. Нужно {VIP_PRICE_POINTS}.", 
            show_alert=True, 
        ) 
        return 
    until = now_ts() + VIP_DURATION_HOURS * 3600 
    await db.set_vip(uid, until) 
    await call.message.edit_text( 
        f"✅ <b>VIP активирован!</b>\n\n" 
        f"💰 Списано: {VIP_PRICE_POINTS} очков\n" 
        f"⏳ Действует до: <b>{fmt_ts(until)}</b>\n\n" 
        f"⚡️ Вы теперь в приоритетной очереди!", 
    ) 
@router.callback_query(F.data == "vip_crypto") 
async def cb_vip_crypto(call: CallbackQuery): 
    await call.answer() 
    if not cryptobot.enabled: 
        await call.message.edit_text( 
            "❌ <b>Крипто-оплата не настроена</b>\n\n" 
            "Администратор не указал токен CryptoBot.", 
            reply_markup=InlineKeyboardBuilder().button( 
                text="🔙 Назад", callback_data="vip_close" 
            ).as_markup(), 
        ) 
        return 
    uid = call.from_user.id 
    await call.message.edit_text("💳 Создаю счёт, подождите...") 
    payload  = f"vip:{uid}:{now_ts()}" 
    invoice  = await cryptobot.create_invoice( 
        amount=VIP_PRICE_CRYPTO, 
        currency="USDT", 
        payload=payload, 
    ) 
    if not invoice: 
        await call.message.edit_text( 
            "❌ <b>Ошибка при создании счёта</b>\n\n" 
            "Сервис CryptoBot временно недоступен.\n" 
            "Попробуйте позже или купите VIP за очки.", 
            reply_markup=InlineKeyboardBuilder().button( 
                text="🔙 Назад", callback_data="vip_close" 
            ).as_markup(), 
        ) 
        return 
    invoice_id = str(invoice["invoice_id"]) 
    pay_url    = invoice.get("bot_invoice_url") or invoice.get("mini_app_invoice_url", "") 
    await db.save_invoice(invoice_id, uid, VIP_PRICE_CRYPTO) 
    if not pay_url: 
        pay_url = f"https://t.me/CryptoBot?start=IV{invoice_id}" 
    await call.message.edit_text( 
        f"💳 <b>Счёт создан!</b>\n\n" 
        f"💰 Сумма:  <b>{VIP_PRICE_CRYPTO} USDT</b>\n" 
        f"🆔 ID:     <code>{invoice_id}</code>\n\n" 
        f"1. Нажмите «Перейти к оплате»\n" 
        f"2. Оплатите в CryptoBot\n" 
        f"3. Вернитесь и нажмите «Проверить оплату»\n\n" 
        f"⏳ Счёт действителен 1 час.", 
        reply_markup=kb_invoice(pay_url, invoice_id), 
    ) 
@router.callback_query(F.data.startswith("pay_check:")) 
async def cb_pay_check(call: CallbackQuery): 
    await call.answer() 
    invoice_id = call.data.split(":", 1)[1] 
    uid        = call.from_user.id 
    inv = await db.get_invoice(invoice_id) 
    if not inv or inv["user_id"] != uid: 
        await call.message.edit_text("❌ Счёт не найден.") 
        return 
    if inv["status"] == "paid": 
        await call.message.edit_text("✅ Уже оплачено и активировано!") 
        return 
    await call.message.edit_text("🔄 Проверяю оплату...") 
    status = await cryptobot.get_invoice_status(invoice_id) 
    if status == "paid": 
        await db.mark_invoice_paid(invoice_id) 
        until = now_ts() + VIP_DURATION_HOURS * 3600 
        await db.set_vip(uid, until) 
        await call.message.edit_text( 
            f"🎉 <b>Оплата получена! VIP активирован!</b>\n\n" 
            f"💳 {VIP_PRICE_CRYPTO} USDT\n" 
            f"⏳ Действует до: <b>{fmt_ts(until)}</b>\n\n" 
            f"⚡️ Вы теперь в приоритетной очереди!", 
        ) 
        logger.info(f"VIP activated via crypto: uid={uid}, invoice={invoice_id}") 
    elif status == "active": 
        pay_url = f"https://t.me/CryptoBot?start=IV{invoice_id}" 
        await call.message.edit_text( 
            f"⏳ <b>Оплата ещё не поступила</b>\n\n" 
            f"Счёт: <code>{invoice_id}</code>\n\n" 
            f"Завершите оплату и нажмите «Проверить» ещё раз.", 
            reply_markup=kb_invoice(pay_url, invoice_id), 
        ) 
    elif status == "expired": 
        await call.message.edit_text( 
            "❌ <b>Счёт истёк.</b>\n\nСоздайте новый.", 
            reply_markup=InlineKeyboardBuilder().button( 
                text="🔙 Меню VIP", callback_data="vip_close" 
            ).as_markup(), 
        ) 
    else: 
        await call.message.edit_text( 
            "⚠️ <b>Не удалось проверить оплату.</b>\n\nПопробуйте чуть позже.", 
            reply_markup=kb_invoice( 
                f"https://t.me/CryptoBot?start=IV{invoice_id}", invoice_id 
            ), 
        ) 
@router.callback_query(F.data == "vip_close") 
async def cb_vip_close(call: CallbackQuery): 
    await call.answer() 
    await call.message.edit_text("👑 VIP меню закрыто. Нажмите кнопку в меню снова.") 
# ============================================================== 
# ADMIN COMMANDS 
# ============================================================== 
def is_admin(uid: int) -> bool: 
    return uid in ADMIN_IDS 
@router.message(Command("admin")) 
async def cmd_admin(message: Message): 
    if not is_admin(message.from_user.id): 
        return 
    await safe_answer( 
        message, 
        "🛠 <b>Панель администратора</b>\n\n" 
        "/stats       — статистика\n" 
        "/send        — рассылка всем\n" 
        "/ban ID      — заблокировать\n" 
        "/unban ID    — разблокировать\n" 
        "/warn ID     — предупреждение\n" 
        "/clearwarn ID — сбросить варны\n" 
        "/setchannel  — добавить обяз. канал\n" 
        "/removechannel — удалить канал\n" 
        "/channels    — список каналов\n" 
        "/queue       — статус очереди", 
        reply_markup=kb_admin(), 
    ) 
@router.message(Command("stats")) 
async def cmd_stats(message: Message): 
    if not is_admin(message.from_user.id): 
        return 
    s = await db.get_stats() 
    await safe_answer( 
        message, 
        f"📊 <b>Статистика</b>\n\n" 
        f"👥 Пользователей:  {s['total']:,}\n" 
        f"👑 VIP активных:   {s['vip']:,}\n" 
        f"🔄 Завершено ВЗ:   {s['sessions']:,}\n" 
        f"💰 Очков выдано:   {s['points']:,}\n\n" 
        f"🟢 В очереди:      {vz_queue.total}\n" 
        f"  ↳ VIP: {vz_queue.vip_count}\n" 
        f"  ↳ Обычные: {vz_queue.regular_count}\n" 
        f"🔄 Активных сессий: {len(live_sessions)}", 
    ) 
@router.message(Command("queue")) 
async def cmd_queue(message: Message): 
    if not is_admin(message.from_user.id): 
        return 
    lines = [ 
        f"📋 <b>Очередь</b>", 
        f"VIP: {vz_queue.vip_count} | Обычные: {vz_queue.regular_count}", 
        f"Сессий: {len(live_sessions)}", 
    ] 
    if live_sessions: 
        lines.append("\n<b>Активные сессии:</b>") 
        for sid, s in list(live_sessions.items())[:10]: 
            age = now_ts() - s["ts"] 
            lines.append(f"  #{sid}: {s['uid1']} ↔ {s['uid2']} ({age}с)") 
    await safe_answer(message, "\n".join(lines)) 
@router.message(Command("send")) 
async def cmd_send(message: Message, state: FSMContext): 
    if not is_admin(message.from_user.id): 
        return 
    await state.set_state(VZStates.admin_broadcast) 
    await safe_answer(message, "📨 Отправьте текст рассылки (/cancel — отмена):") 
@router.message(VZStates.admin_broadcast) 
async def do_broadcast(message: Message, bot: Bot, state: FSMContext): 
    if not is_admin(message.from_user.id): 
        return 
    await state.clear() 
    uids = await db.get_all_user_ids() 
    sent = failed = 0 
    for uid in uids: 
        if uid == message.from_user.id: 
            continue 
        result = await safe_send(bot.send_message, uid, message.html_text or "") 
        if result: 
            sent += 1 
        else: 
            failed += 1 
        await asyncio.sleep(0.05) 
    await safe_answer( 
        message, 
        f"📨 <b>Рассылка завершена</b>\n\n✅ {sent} | ❌ {failed}", 
    ) 
@router.message(Command("ban")) 
async def cmd_ban(message: Message, bot: Bot): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split() 
    if len(parts) < 2 or not parts[1].isdigit(): 
        await safe_answer(message, "❌ /ban [user_id]") 
        return 
    tid = int(parts[1]) 
    await db.ban_user(tid) 
    await safe_answer(message, f"✅ Пользователь <code>{tid}</code> заблокирован.") 
    await safe_send(bot.send_message, tid, "🚫 <b>Вы заблокированы в боте.</b>") 
@router.message(Command("unban")) 
async def cmd_unban(message: Message, bot: Bot): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split() 
    if len(parts) < 2 or not parts[1].isdigit(): 
        await safe_answer(message, "❌ /unban [user_id]") 
        return 
    tid = int(parts[1]) 
    await db.unban_user(tid) 
    await safe_answer(message, f"✅ Пользователь <code>{tid}</code> разблокирован.") 
    await safe_send(bot.send_message, tid, "✅ <b>Вас разблокировали.</b>") 
@router.message(Command("warn")) 
async def cmd_warn(message: Message, bot: Bot): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split(maxsplit=2) 
    if len(parts) < 2 or not parts[1].isdigit(): 
        await safe_answer(message, "❌ /warn [user_id] [причина]") 
        return 
    tid    = int(parts[1]) 
    reason = parts[2] if len(parts) > 2 else "Нарушение (admin)" 
    await apply_warn(bot, tid, reason) 
    u = await db.get_user(tid) 
    warns = u["warns"] if u else "?" 
    await safe_answer(message, f"⚠️ Предупреждение выдано: {tid} ({warns}/{MAX_WARNS})") 
@router.message(Command("clearwarn")) 
async def cmd_clearwarn(message: Message, bot: Bot): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split() 
    if len(parts) < 2 or not parts[1].isdigit(): 
        await safe_answer(message, "❌ /clearwarn [user_id]") 
        return 
    tid = int(parts[1]) 
    await db.clear_warns(tid) 
    await safe_answer(message, f"✅ Варны сброшены: <code>{tid}</code>") 
    await safe_send(bot.send_message, tid, "✅ Ваши предупреждения сброшены.") 
@router.message(Command("setchannel")) 
async def cmd_setchannel(message: Message, bot: Bot): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split() 
    if len(parts) < 2: 
        await safe_answer(message, "❌ /setchannel @username или /setchannel -100xxx") 
        return 
    chat = await resolve_channel(bot, parts[1]) 
    if not chat: 
        await safe_answer(message, "❌ Канал не найден.") 
        return 
    cid = f"@{chat['username']}" if chat.get("username") else str(chat["id"]) 
    await db.add_channel(cid, chat["title"], message.from_user.id) 
    await safe_answer(message, f"✅ Добавлен: <b>{chat['title']}</b> ({cid})") 
@router.message(Command("removechannel")) 
async def cmd_removechannel(message: Message): 
    if not is_admin(message.from_user.id): 
        return 
    parts = message.text.split() 
    if len(parts) < 2: 
        await safe_answer(message, "❌ /removechannel @username_или_id") 
        return 
    await db.remove_channel(parts[1]) 
    await safe_answer(message, f"✅ Удалён: <code>{parts[1]}</code>") 
@router.message(Command("channels")) 
async def cmd_channels(message: Message): 
    if not is_admin(message.from_user.id): 
        return 
    chs = await db.get_channels() 
    if not chs: 
        await safe_answer(message, "📋 Обязательных каналов нет.\nДобавьте: /setchannel @channel") 
        return 
    lines = ["📋 <b>Обязательные каналы:</b>\n"] 
    for i, ch in enumerate(chs, 1): 
        lines.append(f"{i}. <b>{ch['channel_name']}</b> — <code>{ch['channel_id']}</code>") 
    await safe_answer(message, "\n".join(lines)) 
# ── Admin inline callbacks ──────────────────────────────────── 
@router.callback_query(F.data == "adm_stats") 
async def adm_cb_stats(call: CallbackQuery): 
    if not is_admin(call.from_user.id): 
        await call.answer("❌", show_alert=True); return 
    await call.answer() 
    s = await db.get_stats() 
    await call.message.edit_text( 
        f"📊 Пользователей: {s['total']:,}\n" 
        f"👑 VIP: {s['vip']:,}\n" 
        f"🔄 ВЗ: {s['sessions']:,}\n" 
        f"💰 Очков: {s['points']:,}", 
        reply_markup=kb_admin(), 
    ) 
@router.callback_query(F.data == "adm_broadcast") 
async def adm_cb_broadcast(call: CallbackQuery, state: FSMContext): 
    if not is_admin(call.from_user.id): 
        await call.answer("❌", show_alert=True); return 
    await call.answer() 
    await state.set_state(VZStates.admin_broadcast) 
    await call.message.edit_text("📨 Отправьте текст рассылки:") 
@router.callback_query(F.data == "adm_ban") 
async def adm_cb_ban(call: CallbackQuery, state: FSMContext): 
    if not is_admin(call.from_user.id): 
        await call.answer("❌", show_alert=True); return 
    await call.answer() 
    await state.set_state(VZStates.admin_ban_input) 
    await call.message.edit_text("🚫 Введите user_id для бана:") 
@router.message(VZStates.admin_ban_input) 
async def adm_ban_input(message: Message, bot: Bot, state: FSMContext): 
    if not is_admin(message.from_user.id): 
        return 
    await state.clear() 
    tid_str = (message.text or "").strip() 
    if not tid_str.isdigit(): 
        await safe_answer(message, "❌ Введите числовой ID.") 
        return 
    tid = int(tid_str) 
    await db.ban_user(tid) 
    await safe_answer(message, f"✅ Забанен: <code>{tid}</code>") 
    await safe_send(bot.send_message, tid, "🚫 Вы заблокированы в боте.") 
@router.callback_query(F.data == "adm_channels") 
async def adm_cb_channels(call: CallbackQuery): 
    if not is_admin(call.from_user.id): 
        await call.answer("❌", show_alert=True); return 
    await call.answer() 
    chs = await db.get_channels() 
    text = "📋 <b>Каналы:</b>\n" + ( 
        "\n".join(f"  {i}. {c['channel_name']} — {c['channel_id']}" for i, c in enumerate(chs, 1)) 
        if chs else "Нет" 
    ) 
    await call.message.edit_text(text, reply_markup=kb_admin()) 
@router.callback_query(F.data == "adm_queue") 
async def adm_cb_queue(call: CallbackQuery): 
    if not is_admin(call.from_user.id): 
        await call.answer("❌", show_alert=True); return 
    await call.answer() 
    await call.message.edit_text( 
        f"📋 VIP: {vz_queue.vip_count} | Обычные: {vz_queue.regular_count}\n" 
        f"Активных сессий: {len(live_sessions)}", 
        reply_markup=kb_admin(), 
    ) 
# ============================================================== 
# /cancel 
# ============================================================== 
@router.message(Command("cancel")) 
async def cmd_cancel(message: Message, state: FSMContext): 
    uid = message.from_user.id 
    vz_queue.remove(uid) 
    cur = await state.get_state() 
    await state.clear() 
    text = "❌ <b>Отменено.</b>" if cur else "ℹ️ Нет активного действия." 
    await safe_answer(message, text) 
# ============================================================== 
# FALLBACK 
# ============================================================== 
@router.message(StateFilter(None)) 
async def fallback(message: Message): 
    if message.text and message.text.startswith("/"): 
        await safe_answer(message, "❓ Неизвестная команда. Используйте /start") 
@router.errors() 
async def on_error(event, exception: Exception): 
    logger.error(f"Unhandled: {type(exception).__name__}: {exception}") 
    logger.debug(traceback.format_exc()) 
    try: 
        if hasattr(event, "message") and event.message: 
            await safe_send( 
                event.message.answer, 
                "⚠️ Произошла ошибка. Нажмите /start", 
            ) 
        elif hasattr(event, "callback_query") and event.callback_query: 
            await event.callback_query.answer("⚠️ Ошибка. Попробуйте снова.", show_alert=True) 
    except Exception: 
        pass 
# ============================================================== 
# STARTUP / MAIN 
# ============================================================== 
async def on_startup(bot: Bot): 
    global BOT_USERNAME, BOT_ID 
    await db.init() 
    me           = await bot.get_me() 
    BOT_USERNAME = me.username 
    BOT_ID       = me.id 
    logger.info(f"🚀 @{BOT_USERNAME} (id={BOT_ID}) started") 
    logger.info(f"👑 Admins: {ADMIN_IDS}") 
    for aid in ADMIN_IDS: 
        await safe_send( 
            bot.send_message, aid, 
            f"✅ <b>Бот запущен</b>\n@{BOT_USERNAME} | {me.id}", 
        ) 
async def on_shutdown(bot: Bot): 
    logger.info("🛑 Shutting down…") 
    for aid in ADMIN_IDS: 
        await safe_send(bot.send_message, aid, "🛑 <b>Бот остановлен.</b>") 
async def main(): 
    if not BOT_TOKEN: 
        logger.critical("BOT_TOKEN not set in .env!") 
        return 
    bot = Bot( 
        token=BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML), 
    ) 
    dp = Dispatcher(storage=MemoryStorage()) 
    dp.message.middleware(AntiFloodMiddleware()) 
    dp.callback_query.middleware(AntiFloodMiddleware()) 
    dp.include_router(router) 
    dp.startup.register(on_startup) 
    dp.shutdown.register(on_shutdown) 
    logger.info("⚡ Polling started") 
    try: 
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"]) 
    finally: 
        await bot.session.close() 
if __name__ == "__main__": 
    try: 
        asyncio.run(main()) 
    except KeyboardInterrupt: 
        logger.info("👋 Stopped by user")
