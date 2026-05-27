import asyncio
import sqlite3
import logging
import aiohttp
import re
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======================================================
# НАСТРОЙКИ — впиши свои значения
# ======================================================
BOT_TOKEN        = "8902414270:AAHKEygVD18jir32ebWTuIJFMhAnkFAjQsU"
CRYPTO_PAY_TOKEN = "586296:AA1tejqFx3eBIFPbqugJyXZwWsTVpSmvn5D"
ADMIN_ID         = 8325037674

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# ======================================================
# БАЗА ДАННЫХ
# ======================================================
conn   = sqlite3.connect("vz_bot.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id         INTEGER PRIMARY KEY,
        balance         INTEGER DEFAULT 0,
        total_vz        INTEGER DEFAULT 0,
        vip_subs_needed INTEGER DEFAULT 0
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT DEFAULT ''
    )
""")
cursor.execute(
    "INSERT OR IGNORE INTO settings (key, value) VALUES ('required_channels', '')"
)
conn.commit()


# ======================================================
# ОБЯЗАТЕЛЬНЫЕ КАНАЛЫ (загружаются из БД при старте)
# ======================================================
def load_required_channels() -> list[str]:
    cursor.execute("SELECT value FROM settings WHERE key = 'required_channels'")
    row = cursor.fetchone()
    if row and row[0]:
        return [ch.strip() for ch in row[0].split(",") if ch.strip()]
    return []


def save_required_channels(channels: list[str]) -> None:
    cursor.execute(
        "UPDATE settings SET value = ? WHERE key = 'required_channels'",
        (",".join(channels),)
    )
    conn.commit()


required_channels: list[str] = load_required_channels()


# ======================================================
# ПОЛЬЗОВАТЕЛИ
# ======================================================
def get_user(user_id: int) -> tuple[int, int, int]:
    cursor.execute(
        "SELECT balance, total_vz, vip_subs_needed FROM users WHERE user_id = ?",
        (user_id,)
    )
    res = cursor.fetchone()
    if not res:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return (0, 0, 0)
    return res


def get_all_users() -> list[int]:
    cursor.execute("SELECT user_id FROM users")
    return [row[0] for row in cursor.fetchall()]


def add_vz_stat(user_id: int) -> None:
    cursor.execute(
        "UPDATE users SET total_vz = total_vz + 1 WHERE user_id = ?",
        (user_id,)
    )
    conn.commit()


def add_balance(user_id: int, amount: int) -> None:
    cursor.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (amount, user_id)
    )
    conn.commit()


def change_vip_subs(user_id: int, delta: int) -> None:
    cursor.execute(
        "UPDATE users SET vip_subs_needed = MAX(0, vip_subs_needed + ?) WHERE user_id = ?",
        (delta, user_id)
    )
    conn.commit()


# ======================================================
# ВРЕМЕННАЯ ПАМЯТЬ
# ======================================================
search_queue:  list[int]             = []
vip_queue:     list[tuple[int, str]] = []
pending_vip:   dict[int, str]        = {}
sessions:      dict[int, int]        = {}
user_links:    dict[int, str]        = {}  
display_links: dict[int, str]        = {}  
ready_status:  set[int]              = set()


# ======================================================
# КЛАВИАТУРЫ (С ПОСТОЯННЫМ МЕНЮ)
# ======================================================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Начать вз")],
        [KeyboardButton(text="Профиль"), KeyboardButton(text="Подписка")]
    ],
    resize_keyboard=True,
    is_persistent=True  # Клавиатура НАВСЕГДА закрепляется в интерфейсе ТГ
)

ready_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="✅ Готово")]],
    resize_keyboard=True,
    is_persistent=True
)

start_only_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="/start")]],
    resize_keyboard=True
)


def subscription_kb(channels: list[str]) -> InlineKeyboardMarkup:
    buttons = []
    for ch in channels:
        if ":" in ch and (ch.startswith("-100") or ch.split(":", 1)[0].isdigit()):
            chat_id, link = ch.split(":", 1)
            buttons.append([InlineKeyboardButton(text="📢 Подписаться (Приватный)", url=link.strip())])
        else:
            clean_ch = ch.replace("https://t.me/", "").replace("http://t.me/", "").lstrip("@")
            buttons.append([InlineKeyboardButton(text=f"📢 @{clean_ch}", url=f"https://t.me/{clean_ch}")])
    buttons.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ======================================================
# FSM
# ======================================================
class VZState(StatesGroup):
    waiting_for_link = State()
    searching        = State()
    in_session       = State()


# ======================================================
# УМНЫЙ ЧЕКЕР ПОДПИСКИ
# ======================================================
def extract_username(channel_input: str) -> str | None:
    text = channel_input.strip()
    if text.startswith("-100") or text.isdigit():
        return text
    match = re.search(r"(?:t|telegram)\.me\/([a-zA-Z0-9_]{5,})", text)
    if match:
        return f"@{match.group(1)}"
    if not text.startswith("@") and "/" not in text:
        return f"@{text}"
    return text


async def is_user_subscribed(user_id: int, chat_id_raw: str) -> bool:
    chat_id = chat_id_raw.strip()
    if chat_id.startswith("-100") or chat_id.isdigit():
        try:
            chat_id = int(chat_id)
        except ValueError:
            return False
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status not in ("left", "kicked", "banned")
    except TelegramAPIError as e:
        logger.warning(f"Ошибка проверки подписки {user_id} в чате {chat_id}: {e}")
        return False


async def check_subscriptions(user_id: int) -> list[str]:
    not_subscribed = []
    for channel in required_channels:
        target = channel.split(":", 1)[0].strip() if ":" in channel else channel
        if not await is_user_subscribed(user_id, target):
            not_subscribed.append(channel)
    return not_subscribed


async def require_subscription(message: Message) -> bool:
    if not required_channels:
        return True
    missing = await check_subscriptions(message.from_user.id)
    if missing:
        await message.answer(
            "⚠️ <b>Для использования бота подпишитесь на наши каналы:</b>",
            parse_mode="HTML",
            reply_markup=subscription_kb(missing)
        )
        return False
    return True


# ======================================================
# CRYPTO BOT API
# ======================================================
CRYPTO_API = "https://pay.crypt.bot/api"


async def create_crypto_invoice(amount_usd: float) -> tuple[str | None, int | None]:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {
        "asset": "USDT",
        "amount": f"{amount_usd:.2f}",
        "description": "Покупка пакета ВЗ+ (5 штук)"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{CRYPTO_API}/createInvoice",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if data.get("ok"):
                    result = data["result"]
                    return result["bot_invoice_url"], result["invoice_id"]
    except Exception as e:
        logger.error(f"Ошибка создания счёта CryptoBot: {e}")
    return None, None


async def check_crypto_invoice(invoice_id: int) -> bool:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    params  = {"invoice_ids": str(invoice_id)}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CRYPTO_API}/getInvoices",
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if data.get("ok"):
                    items = data["result"].get("items", [])
                    if items:
                        return items[0]["status"] == "paid"
    except Exception as e:
        logger.error(f"Ошибка проверки счёта CryptoBot: {e}")
    return False


# ======================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ======================================================
async def safe_send(user_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(user_id, text, **kwargs)
        return True
    except TelegramAPIError as e:
        logger.warning(f"Не удалось отправить сообщение {user_id}: {e}")
        return False


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

        await safe_send(
            uid,
            "🎉 Взаимная подписка успешно завершена! +1 монета.",
            reply_markup=main_kb
        )
        _, _, vip_subs = get_user(uid)
        if vip_subs > 0 and uid in user_links:
            vip_queue.append((uid, f"{user_links[uid]}|{display_links.get(uid, '')}"))
            change_vip_subs(uid, -1)


# ======================================================
# КОМАНДА /start
# ======================================================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    current_state = await state.get_state()
    get_user(user_id)

    if current_state == VZState.in_session:
        await message.answer(
            "⚠️ Вы находитесь в активной сессии взаимной подписки!\n"
            "Сначала завершите её, нажав кнопку «✅ Готово».",
            reply_markup=ready_kb
        )
        return

    await state.clear()
    if user_id in search_queue:
        try: search_queue.remove(user_id)
        except ValueError: pass

    if not await require_subscription(message):
        return

    await message.answer(
        "👋 Добро пожаловать в бот взаимных подписок!",
        reply_markup=main_kb
    )


# ======================================================
# CALLBACK
# ======================================================
@dp.callback_query(F.data == "check_sub")
async def callback_check_sub(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    missing = await check_subscriptions(user_id)

    if missing:
        try: await callback.message.edit_reply_markup(reply_markup=subscription_kb(missing))
        except TelegramBadRequest: pass
        await callback.answer("❌ Вы ещё не подписались на все каналы!", show_alert=True)
        return

    try: await callback.message.delete()
    except TelegramBadRequest: pass

    current_state = await state.get_state()
    if current_state is None:
        await bot.send_message(user_id, "✅ Подписка подтверждена!", reply_markup=main_kb)
    else:
        await callback.answer("✅ Подписка подтверждена! Продолжайте действие.", show_alert=True)


# ======================================================
# КНОПКИ МЕНЮ
# ======================================================
@dp.message(F.text == "Профиль")
async def show_profile(message: Message) -> None:
    if not await require_subscription(message): return
    balance, total_vz, vip = get_user(message.from_user.id)
    await message.answer(
        f"👤 <b>Ваш профиль:</b>\n\n💰 Баланс: {balance} монет\n🤝 Успешных ВЗ: {total_vz}\n🌟 Доступно ВЗ+ подписок: {vip}",
        parse_mode="HTML",
        reply_markup=main_kb
    )


@dp.message(F.text == "Подписка")
async def show_subscription(message: Message) -> None:
    if not await require_subscription(message): return
    pay_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Оплата криптой ($1.50 USDT)", callback_data="pay_crypto")]
    ])
    await message.answer(
        "💎 <b>Тариф ВЗ+ (Пакет на 5 подписок)</b>\n\nВы делаете всего 1 подписку, а бот взамен гарантированно приводит к вам <b>2 подписчиков</b>!\n\nВыберите удобный способ оплаты:",
        parse_mode="HTML",
        reply_markup=pay_kb
    )


@dp.callback_query(F.data == "pay_crypto")
async def process_pay_crypto(callback: CallbackQuery) -> None:
    await callback.answer()
    url, invoice_id = await create_crypto_invoice(1.50)
    if not url or invoice_id is None:
        await callback.message.answer("❌ Ошибка генерации счёта.")
        return
    check_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить через CryptoBot", url=url)],
        [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_{invoice_id}")]
    ])
    await callback.message.answer("🏷️ Счёт сформирован. Оплатите его и нажмите кнопку ниже.", reply_markup=check_kb)


@dp.callback_query(F.data.startswith("check_") & ~F.data.startswith("check_sub"))
async def verify_crypto_payment(callback: CallbackQuery) -> None:
    parts = callback.data.split("_")
    if len(parts) != 2 or not parts[1].isdigit(): return
    invoice_id = int(parts[1])
    is_paid = await check_crypto_invoice(invoice_id)
    if is_paid:
        change_vip_subs(callback.from_user.id, 5)
        await callback.message.edit_text("🎉 Оплата подтверждена! Вам начислено 5 подписок ВЗ+.")
    else:
        await callback.answer("❌ Счёт пока не оплачен. Попробуйте снова.", show_alert=True)


# ======================================================
# НАЧАТЬ ВЗ
# ======================================================
@dp.message(F.text == "Начать вз")
async def start_vz(message: Message, state: FSMContext) -> None:
    if not await require_subscription(message): return
    user_id = message.from_user.id
    current_state = await state.get_state()

    if current_state in (VZState.searching, VZState.in_session):
        await message.answer("⚠️ Вы уже ищете партнера или находитесь в сессии ВЗ!")
        return

    if user_id in pending_vip:
        vip_data = pending_vip[user_id]
        vip_target, vip_disp = vip_data.split("|")[0], vip_data.split("|")[1] if "|" in vip_data else vip_data
        if await is_user_subscribed(user_id, vip_target): pending_vip.pop(user_id, None)
        else:
            await message.answer(f"⚠️ Вы ещё не выполнили VIP-задание!\nСначала подпишитесь: {vip_disp}\n\nНажмите «Начать вз» ещё раз.")
            return

    if vip_queue:
        vip_index = next((i for i, (vid, _) in enumerate(vip_queue) if vid != user_id), None)
        if vip_index is not None:
            _, raw_vip_data = vip_queue.pop(vip_index)
            vip_target, vip_disp = raw_vip_data.split("|")[0], raw_vip_data.split("|")[1] if "|" in raw_vip_data else raw_vip_data
            if await is_user_subscribed(user_id, vip_target): pass
            else:
                pending_vip[user_id] = raw_vip_data
                await message.answer(f"🌟 Перед поиском подпишитесь на VIP-пользователя:\n{vip_disp}\n\nНажмите «Начать вз» ещё раз.")
                return

    await message.answer(
        "🔗 <b>Отправьте ваш канал для взаимной подписки.</b>\n\n"
        "📢 <b>Если открытый:</b> Ссылку (например, <code>t.me/my_channel</code>)\n"
        "🔒 <b>Если ПРИВАТНЫЙ:</b> Строго в спец-формате: <code>ID::ССЫЛКА</code>\n"
        "<i>Пример:</i> <code>-1003496063105::https://t.me/+fVyyt6...</code>",
        parse_mode="HTML"
    )
    await state.set_state(VZState.waiting_for_link)


@dp.message(VZState.waiting_for_link)
async def process_link(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    raw_text = message.text.strip() if message.text else ""

    if not raw_text or raw_text.startswith("/"):
        await message.answer("⚠️ Пожалуйста, отправьте корректную ссылку или спец-формат.")
        return

    if "::" in raw_text:
        parts = raw_text.split("::", 1)
        chat_id, invite_link = parts[0].strip(), parts[1].strip()
        if not (chat_id.startswith("-100") or chat_id.isdigit()) or not invite_link.startswith("http"):
            await message.answer("⚠️ Неверный спец-формат! Шаблон: <code>ID::ССЫЛКА</code>", parse_mode="HTML")
            return
        user_links[user_id] = chat_id
        display_links[user_id] = invite_link
    else:
        extracted = extract_username(raw_text)
        user_links[user_id] = extracted
        display_links[user_id] = f"https://t.me/{extracted.lstrip('@')}"

    await message.answer("🔍 Ищем вам пару для взаимной подписки...", reply_markup=ReplyKeyboardRemove())
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
            partner_disp = display_links.get(partner, '—')
            
            sent = await safe_send(
                u,
                f"✅ <b>Партнер найден!</b>\n\n👉 Ссылка на канал партнера: {partner_disp}\n\nПерейдите, подпишитесь и нажмите кнопку ниже.",
                parse_mode="HTML", reply_markup=ready_kb
            )
            if not sent:
                sessions.pop(user1, None); sessions.pop(user2, None)
                if partner not in search_queue: search_queue.insert(0, partner)
                other_ctx = dp.fsm.resolve_context(bot, chat_id=partner, user_id=partner)
                await other_ctx.set_state(VZState.searching)
                await safe_send(partner, "⚠️ Партнёр оказался недоступен. Ищем нового...")
                break


# ======================================================
# ПОДТВЕРЖДЕНИЕ В СЕССИИ
# ======================================================
@dp.message(VZState.in_session, F.text == "✅ Готово")
async def process_ready(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    partner_id = sessions.get(user_id)

    if not partner_id:
        await state.clear()
        await message.answer("❌ Ошибка сессии. Попробуйте заново.", reply_markup=main_kb)
        return

    if user_id in ready_status: return

    partner_channel = user_links.get(partner_id, "")
    if not await is_user_subscribed(user_id, partner_channel):
        await message.answer(
            f"❌ Робот проверил канал и <b>не нашёл</b> там вашей подписки!\n🔗 Канал: {display_links.get(partner_id, '')}\nПодпишитесь и нажмите «✅ Готово» снова.",
            parse_mode="HTML"
        )
        return

    ready_status.add(user_id)
    if partner_id in ready_status: await finish_session(user_id, partner_id)
    else:
        await message.answer("⏳ Вы успешно подтвердили подписку. Ждём партнёра...", reply_markup=ReplyKeyboardRemove())
        await safe_send(partner_id, "🔔 Ваш партнер уже подписался и ждёт вашего подтверждения!")


@dp.message(VZState.in_session)
async def block_other_messages(message: Message) -> None:
    if message.from_user.id in ready_status: return
    await message.answer("⚠️ Вы в сессии ВЗ. Нажмите «✅ Готово», чтобы завершить.", reply_markup=ready_kb)


# ======================================================
# АДМИНКА
# ======================================================
@dp.message(Command("send"))
async def admin_broadcast(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    broadcast_text = message.text.removeprefix("/send").strip()
    if not broadcast_text: return
    users = get_all_users()
    await message.answer(f"📢 Рассылка для {len(users)} пользователей...")
    s, f = 0, 0
    for user_id in users:
        if await safe_send(user_id, broadcast_text): s += 1
        else: f += 1
        await asyncio.sleep(0.05)
    await message.answer(f"📊 Доставлено: {s}\n❌ Не удалось: {f}")


@dp.message(Command("setchannel"))
async def admin_set_channel(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    arg = message.text.removeprefix("/setchannel").strip()
    if not arg: return
    if arg in required_channels: return
    required_channels.append(arg)
    save_required_channels(required_channels)
    await message.answer("✅ Канал добавлен.")


@dp.message(Command("removechannel"))
async def admin_remove_channel(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    arg = message.text.removeprefix("/removechannel").strip()
    if not arg: return
    if arg not in required_channels:
        found = next((ch for ch in required_channels if ch.startswith(arg)), None)
        if found: arg = found
        else: return
    required_channels.remove(arg)
    save_required_channels(required_channels)
    await message.answer("✅ Канал удалён.")


@dp.message(Command("channels"))
async def admin_list_channels(message: Message) -> None:
    if message.from_user.id != ADMIN_ID: return
    await message.answer(f"📋 Обязательные:\n" + "\n".join(f"• {ch}" for ch in required_channels))


# ======================================================
# 🔥 ГЛОБАЛЬНЫЙ ОБРАБОТЧИК-ЗАГЛУШКА (ВСЕГДА ВОЗВРАЩАЕТ МЕНЮ)
# ======================================================
@dp.message()
async def global_fallback_handler(message: Message, state: FSMContext) -> None:
    """
    Этот хендлер срабатывает, если ТГ получил сообщение, которое не подошло
    ни под одну команду или состояние выше. Он страхует от любых зависаний.
    """
    user_id = message.from_user.id
    current_state = await state.get_state()
    get_user(user_id)

    # Если пользователь прямо сейчас в сессии взаимного пиара, не трогаем его!
    if current_state == VZState.in_session:
        return

    # Во всех остальных случаях — жестко сбрасываем любые зависшие статусы
    await state.clear()
    
    if user_id in search_queue:
        try: search_queue.remove(user_id)
        except ValueError: pass

    # Проверяем обязательные каналы
    if not await require_subscription(message):
        return

    # Показываем главное меню с кнопкой старта
    await message.answer(
        "⏳ Вы вернулись в главное меню бота.\n"
        "Воспользуйтесь кнопками ниже или нажмите /start для обновления.",
        reply_markup=main_kb
    )


# ======================================================
# ЗАПУСК
# ======================================================
async def main() -> None:
    logger.info("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())