"""
Telegram bot: streamlined taker workflow for internal P2P.

Flow for Quick take:
1) Preview (not taken): masked card + rate + amounts, buttons: [Оплатить] [Выбрать другой]
2) On 'Оплатить': call take, fetch detail, show FULL card + amounts + rate, button: [Оплачено]

Kept features:
- login (/start)
- free orders list with pagination
- quick take flow
- export CSV
- request withdrawal (pick maker -> TRC20)
- notifications every 5s

Removed from UI and code:
- 'Мои активные', 'Детали ордера', 'Сообщение в ордер',
- 'Отменить оплату', 'Отменить ордер' (и связанные состояния/функции)

Config (env):
  - BOT_TOKEN : Telegram token
  - BASE_URL  : Django backend root (default http://localhost:8000)
"""

import logging
import os
import re
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    ConversationHandler,
    CallbackContext,
    CallbackQueryHandler,
)

# ────────────────────────── Logging ──────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("taker-bot")

# ──────────────────────── Environment ────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

# ─────────────────────── Conversation states ─────────────────
(
    STATE_USERNAME,
    STATE_PASSWORD,
    STATE_MENU,
    STATE_QUICK_PREVIEW,        # превью кандидатного ордера (ещё не взяли)
    STATE_AFTER_TAKE_ACTION,    # после take: только 'Оплачено'
    STATE_WITHDRAW_PICK_MAKER,  # выбор мейкера для вывода
    STATE_WITHDRAW_ADDRESS,     # ввод адреса TRC20
) = range(7)

# ───────────────────── In-memory session data ────────────────
user_sessions: Dict[int, requests.Session] = {}
user_roles: Dict[int, Optional[str]] = {}
last_notif_id: Dict[int, int] = {}

# quick take candidates per chat
candidates_by_chat: Dict[int, List[dict]] = {}

# pagination caches
PAGE_SIZE = 10
cache_free_orders: Dict[int, List[dict]] = {}

# current taken order id (для 'Оплачено')
current_order_id: Dict[int, int] = {}

# withdraw selection
user_selected_maker: Dict[int, Tuple[int, str]] = {}  # chat_id -> (maker_id, maker_username)

# ───────────────────────── Helpers ───────────────────────────
def _norm_status(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "_")

FREE_STATUSES = {"awaiting_payment", "pending", "new"}

def _order_sort_key(o: dict):
    ts = o.get("created_at") or ""
    try:
        if isinstance(ts, str) and ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return o.get("id") or 0

def _rate(o: dict) -> float:
    try:
        usdt = float(o.get("amount_usdt") or 0)
        uah  = float(o.get("amount_uah") or 0)
        return (uah / usdt) if usdt else 0.0
    except Exception:
        return 0.0

def _extract_masked_card(o: dict) -> str:
    # маска для превью
    for key in ("card_mask", "card", "bank", "iban"):
        v = o.get(key)
        if v:
            return str(v)
    return "—"

def _extract_full_card(o: dict) -> str:
    # после take — стараемся получить полный номер
    for key in ("card_full", "card_number", "card", "iban"):
        v = o.get(key)
        if v:
            return str(v)
    return _extract_masked_card(o)

def _fmt_order_line(o: dict) -> str:
    eid = o.get("external_order_id") or o.get("id")
    return f"{eid}: @{o.get('maker')} | USDT {o.get('amount_usdt')}"

def _page_kb(prefix: str, page: int, total: int) -> InlineKeyboardMarkup:
    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"{prefix}:page:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"{prefix}:page:{page+1}"))
    if nav:
        buttons.append(nav)
    return InlineKeyboardMarkup(buttons) if buttons else InlineKeyboardMarkup([])

# ───────────────────────── CSRF helpers ──────────────────────
def _get_csrf_token(session: requests.Session) -> Optional[str]:
    token = session.cookies.get("csrftoken") or session.cookies.get("csrf")
    if token:
        return token
    try:
        session.get(f"{BASE_URL}/api/me/", timeout=10)
        token = session.cookies.get("csrftoken") or session.cookies.get("csrf")
        if token:
            return token
    except Exception:
        pass
    try:
        session.get(f"{BASE_URL}/", timeout=10)
        token = session.cookies.get("csrftoken") or session.cookies.get("csrf")
        if token:
            return token
    except Exception:
        pass
    return None

def _csrf_headers(session: requests.Session) -> dict:
    token = _get_csrf_token(session)
    headers = {}
    if token:
        headers["X-CSRFToken"] = token
        headers["Referer"] = BASE_URL + "/"
    return headers

# ───────────────────────── API helpers ───────────────────────
def _fetch_paginated(session: requests.Session, url: str) -> List[dict]:
    items: List[dict] = []
    try:
        while url:
            r = session.get(url, timeout=20)
            if not r.ok:
                logger.error("GET %s failed: %s %s", url, r.status_code, r.text)
                break
            data = r.json()
            if isinstance(data, dict) and "results" in data:
                items.extend(data.get("results", []))
                url = data.get("next")
            elif isinstance(data, list):
                items = data
                url = None
            else:
                maybe = data.get("results") if isinstance(data, dict) else None
                if isinstance(maybe, list):
                    items = maybe
                url = None
    except Exception as e:
        logger.error("Pagination fetch error: %s", e)
    return items

def api_login(username: str, password: str) -> Optional[requests.Session]:
    s = requests.Session()
    try:
        r = s.post(f"{BASE_URL}/api/login/", data={"username": username, "password": password}, timeout=15)
        if r.ok:
            _get_csrf_token(s)
            return s
        logger.error("Login failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.error("Login error: %s", e)
    return None

def api_me(session: requests.Session) -> Optional[dict]:
    try:
        r = session.get(f"{BASE_URL}/api/me/", timeout=15)
        if r.ok:
            return r.json()
        logger.error("GET /api/me failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.error("api_me error: %s", e)
    return None

def api_get_free_orders(session: requests.Session) -> List[dict]:
    # при необходимости поменяй параметр на awaiting_payment
    url = f"{BASE_URL}/api/orders/?status=pending"
    items = _fetch_paginated(session, url)
    return [o for o in items if not o.get("taken_by") and _norm_status(o.get("status")) in FREE_STATUSES]

def api_get_order(session: requests.Session, order_id: int) -> Optional[dict]:
    try:
        r = session.get(f"{BASE_URL}/api/orders/{order_id}/", timeout=15)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None

def api_take_order(session: requests.Session, order_id: int) -> Tuple[bool, str, Optional[dict]]:
    try:
        headers = _csrf_headers(session)
        r = session.post(f"{BASE_URL}/api/orders/{order_id}/take/", headers=headers, timeout=15)
        if r.ok:
            order = api_get_order(session, order_id)
            return True, "OK", order
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        return False, f"{r.status_code}: {detail}", None
    except Exception as e:
        logger.error("take error: %s", e)
        return False, "connection_error", None

def api_mark_paid(session: requests.Session, order_id: int) -> Tuple[bool, str]:
    headers = _csrf_headers(session)
    try:
        r = session.post(f"{BASE_URL}/api/orders/{order_id}/mark-paid/", headers=headers, timeout=15)
        if r.ok:
            return True, "Оплата отмечена."
    except Exception as e:
        logger.error("mark_paid error: %s", e)
    # Фолбэк (если нет экшена) — коммент в карточке
    try:
        rr = session.post(
            f"{BASE_URL}/api/messages/",
            json={"order": order_id, "text": "Оплачено тейкером"},
            headers=headers,
            timeout=15,
        )
        if rr.ok:
            return True, "Оплата отмечена (коммент добавлен)."
    except Exception:
        pass
    return False, "Не удалось отметить оплату."

def api_export_orders_csv(session: requests.Session) -> Optional[bytes]:
    try:
        r = session.get(f"{BASE_URL}/orders/export.csv", timeout=30)
        if r.ok:
            return r.content
        logger.error("CSV export failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.error("csv error: %s", e)
    return None

def api_get_maker_balances(session: requests.Session) -> List[dict]:
    try:
        r = session.get(f"{BASE_URL}/api/maker-balances/", timeout=15)
        if r.ok:
            return r.json() or []
        logger.error("maker-balances failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.error("maker-balances error: %s", e)
    return []

def api_request_withdrawal(session: requests.Session, maker_id: int, address: str) -> Tuple[bool, str]:
    headers = _csrf_headers(session)
    try:
        r = session.post(
            f"{BASE_URL}/api/request-withdrawal/",
            data={"maker_id": maker_id, "address": address},
            headers=headers,
            timeout=20,
        )
        if r.ok:
            return True, "Запрос на вывод отправлен."
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        return False, f"{r.status_code}: {detail}"
    except Exception as e:
        logger.error("withdraw error: %s", e)
    return False, "Ошибка при запросе вывода."

def api_notifications_since(session: requests.Session, since_id: int) -> List[dict]:
    try:
        r = session.get(f"{BASE_URL}/api/notifications/", params={"since": since_id}, timeout=15)
        if r.ok:
            body = r.json() or {}
            return body.get("notifications", [])
        logger.error("notifications failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.error("notif error: %s", e)
    return []

# ───────────────────────── Handlers ──────────────────────────
def start(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    if chat_id in user_sessions:
        update.message.reply_text("Вы уже вошли. /menu для команд.", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU
    update.message.reply_text("Добро пожаловать! Введите имя пользователя:", reply_markup=ReplyKeyboardRemove())
    return STATE_USERNAME

def handle_username(update: Update, context: CallbackContext) -> int:
    context.user_data["username"] = update.message.text.strip()
    update.message.reply_text("Введите пароль:")
    return STATE_PASSWORD

def handle_password(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    username = context.user_data.get("username")
    password = update.message.text

    session = api_login(username, password)
    if not session:
        update.message.reply_text("Ошибка входа. Попробуйте ещё раз. Введите имя пользователя:")
        return STATE_USERNAME

    profile = api_me(session)
    if not profile or profile.get("role") != "taker":
        update.message.reply_text("Доступен кабинет тейкера. Попросите доступ либо войдите как тейкер.",
                                  reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    user_sessions[chat_id] = session
    user_roles[chat_id] = "taker"
    last_notif_id[chat_id] = 0

    # notifications
    job_name = f"notif_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    context.job_queue.run_repeating(poll_notifications, interval=5, first=5,
                                    context={"chat_id": chat_id}, name=job_name)

    update.message.reply_text(
        f"Успешный вход как тейкер @{username}. Используйте /menu для команд.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_MENU

def show_menu(update: Update, context: CallbackContext) -> int:
    keyboard = [
        ["Свободные ордера", "Quick take"],
        ["Скачать CSV", "Запросить вывод"],
        ["Logout"],
    ]
    update.message.reply_text(
        "Выберите действие:",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
    return STATE_MENU

def logout(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    job_name = f"notif_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    for d in (user_sessions, user_roles, last_notif_id, candidates_by_chat,
              cache_free_orders, current_order_id, user_selected_maker):
        d.pop(chat_id, None)
    update.message.reply_text("Вы вышли из системы.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# ────────────── Свободные ордера (список + пагинация) ────────
def cmd_free_orders(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU
    items = api_get_free_orders(session)
    items.sort(key=_order_sort_key)
    cache_free_orders[chat_id] = items
    if not items:
        update.message.reply_text("Свободных ордеров нет.")
        return STATE_MENU
    _send_orders_page(update, chat_id, items, page=0, prefix="free")
    return STATE_MENU

def _send_orders_page(update_or_query, chat_id: int, items: List[dict], page: int, prefix: str):
    start = page * PAGE_SIZE
    end = min(len(items), start + PAGE_SIZE)
    lines = [ _fmt_order_line(o) for o in items[start:end] ]
    kb = _page_kb(prefix, page, total=len(items))
    text = "Свободные ордера:\n" + ("\n".join(lines) if lines else "—")
    if isinstance(update_or_query, Update) and update_or_query.message:
        update_or_query.message.reply_text(text, reply_markup=kb)
    else:
        update_or_query.edit_message_text(text, reply_markup=kb)

def cb_page(update: Update, context: CallbackContext):
    query = update.callback_query
    chat_id = update.effective_chat.id
    query.answer()
    data = query.data  # e.g., "free:page:1"
    try:
        prefix, _, spage = data.split(":")
        page = int(spage)
    except Exception:
        return
    items = cache_free_orders.get(chat_id, [])
    _send_orders_page(query, chat_id, items, page, prefix)

# ───────────────────────── Quick take ────────────────────────
def quick_take(update: Update, context: CallbackContext) -> int:
    """Stage 1: preview candidate (masked card + rate) with choice."""
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    items = api_get_free_orders(session)
    if not items:
        update.message.reply_text("Нет доступных ордеров для взятия.")
        return STATE_MENU

    items.sort(key=_order_sort_key)
    candidates_by_chat[chat_id] = items
    return present_next_candidate(update, context)

def present_next_candidate(update: Update, context: CallbackContext) -> int:
    """Show next free order preview: masked card + amounts + rate, ask to Pay or Next."""
    chat_id = update.effective_chat.id
    cands = candidates_by_chat.get(chat_id, [])
    if not cands:
        update.message.reply_text("Больше доступных ордеров нет.")
        return STATE_MENU

    o = cands.pop(0)
    candidates_by_chat[chat_id] = cands  # save back
    amt_uah = o.get("amount_uah") or 0
    amt_usdt = o.get("amount_usdt") or 0
    rate = _rate(o)
    masked = _extract_masked_card(o)

    text = (
        f"Ордер {o.get('external_order_id') or o.get('id')}\n"
        f"Мейкер: @{o.get('maker')}\n"
        f"USDT: {amt_usdt} | UAH: {amt_uah} | Курс: {rate:.2f}\n"
        f"Карта (маска): {masked}\n\n"
        f"Выберите действие:"
    )
    keyboard = [["Оплатить", "Выбрать другой"]]
    update.message.reply_text(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
    # сохраним текущего кандидата (ещё не взяли)
    context.user_data["preview_candidate"] = o
    return STATE_QUICK_PREVIEW

def handle_quick_preview_choice(update: Update, context: CallbackContext) -> int:
    """Handle 'Оплатить' (do take) or 'Выбрать другой' (next candidate)."""
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    choice = update.message.text
    cand = context.user_data.get("preview_candidate")

    if choice == "Выбрать другой":
        return present_next_candidate(update, context)

    if choice == "Оплатить":
        if not cand:
            update.message.reply_text("Кандидат не найден. Запустите Quick take заново.")
            return STATE_MENU
        ok, detail, order = api_take_order(session, cand.get("id"))
        if not ok:
            update.message.reply_text(f"Не удалось взять ордер: {detail}",
                                      reply_markup=ReplyKeyboardRemove())
            return STATE_MENU

        order = order or api_get_order(session, cand.get("id")) or cand
        amt_uah = order.get("amount_uah") or 0
        amt_usdt = order.get("amount_usdt") or 0
        rate = _rate(order)
        full_card = _extract_full_card(order)

        notice = ""
        if full_card == _extract_masked_card(order):
            notice = "\n\n⚠️ Бэкенд не вернул полный номер карты. Показана маска."

        msg = (
            f"Вы взяли ордер {order.get('external_order_id') or order.get('id')}.\n"
            f"Карта для оплаты: {full_card}\n"
            f"Сумма к оплате: {amt_uah} UAH | Курс: {rate:.2f}{notice}"
        )
        keyboard = [["Оплачено", "Отменить"]]
        update.message.reply_text(
            msg,
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
        )
        current_order_id[chat_id] = order.get("id")
        context.user_data.pop("preview_candidate", None)
        return STATE_AFTER_TAKE_ACTION

    update.message.reply_text("Неизвестный выбор. Используйте Quick take снова.")
    return STATE_MENU

def handle_after_take_action(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    oid = current_order_id.get(chat_id)
    action = update.message.text

    if not oid:
        update.message.reply_text("Не найден активный ордер.", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU

    if action == "Оплачено":
        ok, msg = api_mark_paid(session, oid)
        update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
        current_order_id.pop(chat_id, None)
        return STATE_MENU

    if action == "Отменить":
        ok, msg = api_cancel_order(session, oid)
        update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
        current_order_id.pop(chat_id, None)
        return STATE_MENU

    update.message.reply_text("Неверный выбор.", reply_markup=ReplyKeyboardRemove())
    return STATE_MENU

# ───────────────────────── CSV export ────────────────────────
def cmd_csv(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU
    data = api_export_orders_csv(session)
    if not data:
        update.message.reply_text("Не удалось скачать CSV.")
        return STATE_MENU
    bio = BytesIO(data); bio.name = "orders.csv"
    update.message.reply_document(InputFile(bio))
    return STATE_MENU

# ───────────────────────── Withdraw ──────────────────────────
def cmd_withdraw(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU
    makers = api_get_maker_balances(session)
    if not makers:
        update.message.reply_text("У вас нет доступного баланса у мейкеров.")
        return STATE_MENU
    buttons = []
    for m in makers:
        text = f"@{m['username']} — {m['available']} USDT"
        cb = f"wd_maker:{m['id']}:{m['username']}"
        buttons.append([InlineKeyboardButton(text, callback_data=cb)])
    update.message.reply_text("Выберите мейкера для вывода:", reply_markup=InlineKeyboardMarkup(buttons))
    return STATE_WITHDRAW_PICK_MAKER

def cb_withdraw_pick_maker(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    chat_id = update.effective_chat.id
    query.answer()
    data = query.data  # wd_maker:<id>:<username>
    try:
        logger.info(data)
        _, maker_id, maker_username = data.split(":", 2)
        maker_id = int(maker_id)
    except Exception:
        query.edit_message_text("Не удалось распознать выбор. Попробуйте ещё раз.")
        return STATE_WITHDRAW_PICK_MAKER
    user_selected_maker[chat_id] = (maker_id, maker_username)
    query.edit_message_text(f"Введите TRC20 адрес для вывода у @{maker_username}:")
    return STATE_WITHDRAW_ADDRESS

def handle_withdraw_address(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    addr = update.message.text.strip()
    if len(addr) < 10:
        update.message.reply_text("Похоже на некорректный TRC20 адрес. Введите снова:")
        return STATE_WITHDRAW_ADDRESS
    maker_tuple = user_selected_maker.get(chat_id)
    if not maker_tuple:
        update.message.reply_text("Мейкер не выбран. Начните заново: «Запросить вывод».")
        return STATE_MENU
    maker_id, _ = maker_tuple
    ok, msg = api_request_withdrawal(session, maker_id, addr)
    update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
    return STATE_MENU

# ─────────────────────── Notifications poller ───────────────────────────
def poll_notifications(context: CallbackContext) -> None:
    job_context = context.job.context
    chat_id = job_context.get("chat_id")
    session = user_sessions.get(chat_id)
    if not session:
        return
    last_id = last_notif_id.get(chat_id, 0)
    try:
        notifications = api_notifications_since(session, last_id)
        notifications.reverse()  # oldest first
        for n in notifications:
            nid = n.get("id")
            if nid and nid > last_id:
                last_notif_id[chat_id] = nid
            message = n.get("message", "")
            context.bot.send_message(chat_id=chat_id, text=message)

            # подсказка: связаться с мейкером по ордеру
            maker_match = re.search(r"@([A-Za-z0-9_]+)", message)
            order_match = re.search(r"order\\s+([A-Za-zA-Z0-9\\-]+)", message, re.IGNORECASE)
            maker_user = maker_match.group(1) if maker_match else None
            order_id = order_match.group(1) if order_match else None
            if maker_user and order_id:
                contact = f"Свяжитесь с @{maker_user} по поводу данного ордера ({order_id})"
                context.bot.send_message(chat_id=chat_id, text=contact)
    except Exception as e:
        logger.error("Notification polling error: %s", e)

# ─────────────────────────── Router ──────────────────────────
def handle_menu_choice(update: Update, context: CallbackContext) -> int:
    text = update.message.text
    if text == "Свободные ордера":
        return cmd_free_orders(update, context)
    if text == "Quick take":
        return quick_take(update, context)
    if text == "Скачать CSV":
        return cmd_csv(update, context)
    if text == "Запросить вывод":
        return cmd_withdraw(update, context)
    if text == "Logout":
        return logout(update, context)
    return STATE_MENU

# ──────────────────────────── main ───────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в окружении")

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("menu", show_menu)],
        states={
            STATE_USERNAME: [MessageHandler(Filters.text & ~Filters.command, handle_username)],
            STATE_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, handle_password)],
            STATE_MENU: [
                MessageHandler(Filters.text & ~Filters.command, handle_menu_choice),
                CallbackQueryHandler(cb_page, pattern=r"^(free):page:\d+$"),
                CallbackQueryHandler(cb_withdraw_pick_maker, pattern=r"^wd_maker:"),
            ],

            # Quick take preview → Pay/Next
            STATE_QUICK_PREVIEW: [MessageHandler(Filters.text & ~Filters.command, handle_quick_preview_choice)],

            # After take → only Paid
            STATE_AFTER_TAKE_ACTION: [MessageHandler(Filters.text & ~Filters.command, handle_after_take_action)],

            # Withdraw
            STATE_WITHDRAW_PICK_MAKER: [CallbackQueryHandler(cb_withdraw_pick_maker, pattern=r"^wd_maker:")],
            STATE_WITHDRAW_ADDRESS: [MessageHandler(Filters.text & ~Filters.command, handle_withdraw_address)],
        },
        fallbacks=[CommandHandler("logout", logout)],
        allow_reentry=True,
    )

    dp.add_handler(conv)
    dp.add_handler(CommandHandler("logout", logout))
    dp.add_handler(CommandHandler("menu", show_menu))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
