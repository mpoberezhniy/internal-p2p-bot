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
    STATE_QUICK_PREVIEW,        # preview candidate (not yet taken)
    STATE_AFTER_TAKE_ACTION,    # after take: "Оплачено" only (legacy quick-take flow)
    STATE_WITHDRAW_PICK_MAKER,  # choose maker for withdrawal
    STATE_WITHDRAW_ADDRESS,     # input TRC20 address
    STATE_VIEW_ORDER,           # view opened order (from available/my orders)
    STATE_PARTIAL_AMOUNT,       # input partial pay amount
    STATE_COMMENT_MENU,         # after pay / partial-pay: menu to add comment or go to menu
    STATE_COMMENT_TEXT,         # input comment text
) = range(11)

# ───────────────────── In-memory session data ────────────────
user_sessions: Dict[int, requests.Session] = {}
user_roles: Dict[int, Optional[str]] = {}
last_notif_id: Dict[int, int] = {}

# quick take candidates per chat
candidates_by_chat: Dict[int, List[dict]] = {}

# pagination caches
PAGE_SIZE = 10
cache_available_orders: Dict[int, List[dict]] = {}

# current opened order id (for view-order actions)
current_order_id: Dict[int, int] = {}

# withdraw selection
user_selected_maker: Dict[int, Tuple[int, str]] = {}  # chat_id -> (maker_id, maker_username)

# ───────────────────────── Helpers ───────────────────────────
def fallback_show_menu(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Используйте /menu для навигации.",
        reply_markup=ReplyKeyboardMarkup([["/menu"]], resize_keyboard=True)
    )
    return STATE_MENU

def _norm_status(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "_")

FREE_STATUSES = {"awaiting_payment", "pending", "new"}
MY_ACTIVE_STATUSES = {"taken", "partially_paid"}

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
    for key in ("card_mask", "card", "bank", "iban"):
        v = o.get(key)
        if v:
            return str(v)
    return "—"

def _extract_full_card(o: dict) -> str:
    for key in ("card_full", "card_number", "card", "iban"):
        v = o.get(key)
        if v:
            return str(v)
    return _extract_masked_card(o)

def _fmt_order_line(o: dict, me_username: Optional[str]) -> str:
    eid = o.get("external_order_id") or o.get("id")
    maker = f"@{o.get('maker')}" if o.get("maker") else "@maker"
    usdt = o.get("amount_usdt")
    uah  = o.get("amount_uah")
    rate = _rate(o)
    masked = _extract_masked_card(o)
    tb = str(o.get("taken_by") or "")
    mine = bool(me_username and (tb == me_username))
    rem = o.get("remaining_uah")
    rem_part = f" | Ост: {rem}" if rem else ""
    base = f"{eid}: {maker} | USDT {usdt} | UAH {uah} | Курс {rate:.2f} | Карта {masked}{rem_part}"
    if mine:
        base = "⭐ " + base
    return base

def _page_kb(prefix: str, page: int, total: int) -> List[InlineKeyboardButton]:
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"{prefix}:page:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"{prefix}:page:{page+1}"))
    return nav

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

def api_get_orders_by_status(session: requests.Session, status: str) -> List[dict]:
    url = f"{BASE_URL}/api/orders/?status={status}"
    return _fetch_paginated(session, url)

def api_get_available_orders(session: requests.Session) -> List[dict]:
    # только pending и не взятые
    items = api_get_orders_by_status(session, "pending")
    out = [o for o in items if not o.get("taken_by")]
    return out

def api_get_my_active_orders(session: requests.Session, me_username: str) -> List[dict]:
    taken = api_get_orders_by_status(session, "taken")
    pp    = api_get_orders_by_status(session, "partially_paid")
    mine = [o for o in (taken + pp) if str(o.get("taken_by") or "") == me_username]
    return mine

def api_get_order(session: requests.Session, order_id: int) -> Optional[dict]:
    try:
        r = session.get(f"{BASE_URL}/api/orders/{order_id}/", timeout=15)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None



def api_get_order_comments(session: requests.Session, order_id: int) -> dict:
    """Получить комментарии и вложения ордера через API."""
    try:
        r = session.get(f"{BASE_URL}/api/order/{order_id}/comments/", timeout=15)
        if r.ok:
            return r.json()
    except Exception as e:
        logger.error("order_comments error: %s", e)
    # единый формат ответа
    return {"results": [], "order_attachments": []}


def api_add_order_comment(session: requests.Session, order_id: int, text: str, files: Optional[List[Tuple[str, Tuple[str, BytesIO, str]]]] = None) -> Tuple[bool, str]:
    """Добавить комментарий к ордеру через API (с текстом и/или файлами)."""
    headers = _csrf_headers(session)
    try:
        kwargs = {
            "headers": headers,
            "timeout": 15,
        }
        # Всегда передаём текст (может быть пустой строкой)
        data = {"text": text or "Файл добавлен."}
        if files:
            kwargs["data"] = data
            kwargs["files"] = files
        else:
            kwargs["data"] = data
        r = session.post(
            f"{BASE_URL}/api/order/{order_id}/comments/",
            **kwargs,
        )
        if r.ok:
            return True, "Комментарий добавлен."
        try:
            body = r.json()
        except Exception:
            body = r.text
        return False, f"{r.status_code}: {body}"
    except Exception as e:
        logger.error("add_comment error: %s", e)
        return False, "Ошибка при добавлении комментария."

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
    # fallback: comment
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

def api_partial_pay(session: requests.Session, order_id: int, amount: str) -> Tuple[bool, str, Optional[dict]]:
    headers = _csrf_headers(session)
    try:
        r = session.post(f"{BASE_URL}/api/orders/{order_id}/partial-pay/",
                         json={"amount": amount}, headers=headers, timeout=15)
        if r.ok:
            return True, "Частичная оплата учтена.", r.json()
        try:
            body = r.json()
        except Exception:
            body = r.text
        return False, f"{r.status_code}: {body}", None
    except Exception as e:
        return False, str(e), None

def api_cancel_order(session: requests.Session, order_id: int) -> Tuple[bool, str]:
    """Safe fallback for legacy 'Отменить' button in quick-take flow (if exposed)."""
    headers = _csrf_headers(session)
    try:
        r = session.post(f"{BASE_URL}/api/orders/{order_id}/cancel/", headers=headers, timeout=15)
        if r.ok:
            return True, "Ордер отменён."
        return False, f"{r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)

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

    update.message.reply_text(
        f"Успешный вход как тейкер @{username}. Используйте /menu для команд.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_MENU

def show_menu(update: Update, context: CallbackContext) -> int:
    keyboard = [
        ["Доступные ордера", "Мои активные ордера"],
        ["Quick take", "Скачать CSV"],
        ["Запросить вывод", "Logout"],
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
              cache_available_orders, current_order_id, user_selected_maker):
        d.pop(chat_id, None)
    update.message.reply_text("Вы вышли из системы.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# ────────────── Доступные ордера (только pending & not taken) ────────
def cmd_available_orders(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    me = api_me(session) or {}
    me_username = me.get("username")

    free = api_get_available_orders(session)  # только pending & not taken
    items = sorted(free, key=_order_sort_key, reverse=True)

    cache_available_orders[chat_id] = items

    if not items:
        update.message.reply_text("Доступных (pending) ордеров нет.")
        return STATE_MENU

    _send_available_page(update, chat_id, items, page=0, me_username=me_username, prefix="avail")
    return STATE_MENU

# ────────────── Мои активные ордера (taken/partially_paid) ───────────
def cmd_my_active_orders(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    me = api_me(session) or {}
    me_username = me.get("username") or ""
    items = api_get_my_active_orders(session, me_username)
    items = sorted(items, key=_order_sort_key, reverse=True)

    cache_available_orders[chat_id] = items  # переиспользуем кэш/пагинацию
    if not items:
        update.message.reply_text("У вас нет активных ордеров.")
        return STATE_MENU

    _send_available_page(update, chat_id, items, page=0, me_username=me_username, prefix="myact")
    return STATE_MENU

# ────────────── Общая страница листинга (с пагинацией) ───────────────
def _send_available_page(update_or_query, chat_id: int, items: List[dict], page: int,
                         me_username: Optional[str], prefix: str):
    start = page * PAGE_SIZE
    end = min(len(items), start + PAGE_SIZE)

    kb_rows = []
    # Per-order buttons
    for o in items[start:end]:
        label = _fmt_order_line(o, me_username)
        kb_rows.append([InlineKeyboardButton(label, callback_data=f"ord:{o.get('id')}")])

    # Navigation
    nav = _page_kb(prefix, page, total=len(items))
    if nav:
        kb_rows.append(nav)

    kb = InlineKeyboardMarkup(kb_rows)

    text = "Доступные ордера:\n" if prefix == "avail" else "Мои активные ордера:\n"
    if isinstance(update_or_query, Update) and update_or_query.message:
        update_or_query.message.reply_text(text, reply_markup=kb)
    else:
        update_or_query.edit_message_text(text, reply_markup=kb)

def cb_page(update: Update, context: CallbackContext):
    query = update.callback_query
    chat_id = update.effective_chat.id
    query.answer()
    data = query.data  # e.g., "avail:page:1" / "myact:page:2"
    try:
        prefix, _, spage = data.split(":")
        page = int(spage)
    except Exception:
        return
    items = cache_available_orders.get(chat_id, [])
    # need me_username for labeling
    session = user_sessions.get(chat_id)
    me = api_me(session) or {}
    me_username = me.get("username")
    _send_available_page(query, chat_id, items, page, me_username, prefix)

def cb_open_order(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    chat_id = update.effective_chat.id
    s = user_sessions.get(chat_id)
    if not s:
        q.edit_message_text("Сначала войдите: /start")
        return STATE_MENU
    try:
        _, spk = q.data.split(":")
        oid = int(spk)
    except Exception:
        q.edit_message_text("Неверный формат выбора ордера.")
        return STATE_MENU
    o = api_get_order(s, oid)
    if not o:
        q.edit_message_text("Ордер не найден или недоступен.")
        return STATE_MENU
    _bot_show_order_detail(update, context, o)
    return STATE_VIEW_ORDER

# ───────────────────────── Order view ────────────────────────
def _bot_show_order_detail(update: Update, context: CallbackContext, order_obj: dict):
    chat_id = update.effective_chat.id
    current_order_id[chat_id] = order_obj.get("id")

    me = api_me(user_sessions.get(chat_id) or requests.Session()) or {}
    me_username = me.get("username") or ""

    status    = _norm_status(order_obj.get("status"))
    eid       = order_obj.get("external_order_id") or order_obj.get("id")
    maker     = f"@{order_obj.get('maker')}" if order_obj.get("maker") else "@maker"
    amt_usdt  = order_obj.get("amount_usdt")
    amt_uah   = order_obj.get("amount_uah")
    rate      = _rate(order_obj)
    masked    = _extract_masked_card(order_obj)
    full_card = _extract_full_card(order_obj)

    remaining = float(amt_uah) - float(order_obj.get("amount_paid_uah", 0))

    mine = str(order_obj.get("taken_by") or "") == me_username
    is_free = (status in FREE_STATUSES) and not order_obj.get("taken_by")

    lines = [
        f"Ордер {eid}",
        f"Мейкер: {maker}",
        f"USDT: {amt_usdt} | UAH: {amt_uah} | Курс: {rate:.2f}",
        f"Карта: {full_card if mine else masked}",
        f"Статус: {status}",
        f"К оплате сейчас: {remaining}",
    ]

    # добавить пару пустых строк и комментарии по ордеру (если есть)
    session = user_sessions.get(chat_id)
    if session:
        comments_data = api_get_order_comments(session, order_obj.get("id"))
        comments = (comments_data or {}).get("results") or []
        if comments:
            lines.append("")
            lines.append("")
            lines.append("Комментарии (5 последних):")
            # показываем последние несколько комментариев
            for c in comments[-5:]:
                uname = c.get("username") or f"id {c.get('user_id')}"
                text_c = c.get("text") or ""
                lines.append(f"- @{uname}: {text_c}")

    text = "\n".join(lines)

    if mine:
        kb = ReplyKeyboardMarkup([["Оплачено", "Частично оплачено", "Оплатить позже"], ["Назад"]],
                                 one_time_keyboard=True, resize_keyboard=True)
    elif is_free:
        kb = ReplyKeyboardMarkup([["Взять"], ["Назад"]],
                                 one_time_keyboard=True, resize_keyboard=True)
    else:
        kb = ReplyKeyboardMarkup([["Назад"]], one_time_keyboard=True, resize_keyboard=True)

    try:
        msg = update.message
    except Exception:
        msg = None
    if msg is not None:
        msg.reply_text(text, reply_markup=kb)
    else:
        context.bot.send_message(chat_id, text, reply_markup=kb)
    return STATE_VIEW_ORDER

def _bot_handle_view_order_action(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    s = user_sessions.get(chat_id)
    if not s:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    oid = current_order_id.get(chat_id)
    if not oid:
        update.message.reply_text("Ордер не выбран.")
        return STATE_MENU

    choice = (update.message.text or "").strip()
    if choice == "Назад":
        update.message.reply_text("← В меню. /menu", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU

    if choice == "Оплатить позже":
        update.message.reply_text("Ок, вернись когда будешь готов.", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU

    if choice == "Взять":
        ok, detail, order = api_take_order(s, oid)
        if not ok:
            update.message.reply_text(f"Не удалось взять ордер: {detail}", reply_markup=ReplyKeyboardRemove())
            return STATE_MENU
        _bot_show_order_detail(update, context, order or api_get_order(s, oid) or {"id": oid})
        return STATE_VIEW_ORDER

    if choice == "Оплачено":
        ok, msg = api_mark_paid(s, oid)
        if not ok:
            update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
            return STATE_MENU
        # после отметки оплаты предлагаем оставить комментарий
        update.message.reply_text(
            msg + "\n\nМожешь оставить комментарий к ордеру.",
            reply_markup=ReplyKeyboardMarkup(
                [["Оставить комментарий", "В меню"]],
                one_time_keyboard=True,
                resize_keyboard=True,
            ),
        )
        return STATE_COMMENT_MENU

    if choice == "Частично оплачено":
        update.message.reply_text("Введи сумму частичной оплаты (UAH):")
        context.user_data["await_partial_for"] = oid
        return STATE_PARTIAL_AMOUNT

    update.message.reply_text("Не понял выбор.")
    return STATE_MENU

def _bot_handle_partial_amount(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    s = user_sessions.get(chat_id)
    oid = context.user_data.get("await_partial_for")
    amount = (update.message.text or "").strip().replace(",", ".")
    if not oid:
        update.message.reply_text("Ордер не выбран.")
        return STATE_MENU
    if not re.match(r"^\d+(\.\d+)?$", amount):
        update.message.reply_text("Неверный формат суммы. Пример: 123.45")
        return STATE_PARTIAL_AMOUNT
    ok, msg, body = api_partial_pay(s, oid, amount)
    if not ok:
        update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
        return STATE_MENU
    # reread order to show updated remaining
    o = api_get_order(s, oid) or {}
    remaining = float(o.get("amount_uah", 0)) - float(o.get("amount_paid_uah", 0))
    update.message.reply_text(
        f"Частичная оплата учтена. Статус: {o.get('status')}. Остаток: {remaining}",
    )
    update.message.reply_text(
        "Можешь оставить комментарий к ордеру.",
        reply_markup=ReplyKeyboardMarkup(
            [["Оставить комментарий", "В меню"]],
            one_time_keyboard=True,
            resize_keyboard=True,
        ),
    )
    return STATE_COMMENT_MENU

# ───────────────────────── Quick take ────────────────────────


def _bot_handle_comment_menu(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    choice = (update.message.text or "").strip()
    if choice == "В меню":
        update.message.reply_text("Возвращаемся в меню. /menu", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU
    if choice == "Оставить комментарий":
        oid = current_order_id.get(chat_id)
        if not oid:
            update.message.reply_text("Ордер не выбран.", reply_markup=ReplyKeyboardRemove())
            return STATE_MENU
        update.message.reply_text("Отправь текст комментария:", reply_markup=ReplyKeyboardRemove())
        return STATE_COMMENT_TEXT
    update.message.reply_text("Пожалуйста, выбери 'Оставить комментарий' или 'В меню'.")
    return STATE_COMMENT_MENU


def _bot_handle_comment_text(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU
    oid = current_order_id.get(chat_id)
    if not oid:
        update.message.reply_text("Ордер не выбран.", reply_markup=ReplyKeyboardRemove())
        return STATE_MENU

    msg = update.message

    # текст берём из text или из caption (для фото/документа)
    text = (msg.caption or msg.text or "").strip()

    files = []

    # фото
    if msg.photo:
        # берём самое крупное фото
        photo = msg.photo[-1]
        try:
            tg_file = photo.get_file()
            bio = BytesIO()
            tg_file.download(out=bio)
            bio.seek(0)
            filename = f"photo_{tg_file.file_unique_id}.jpg"
            files.append(("file", (filename, bio, "image/jpeg")))
        except Exception as e:
            logger.error("download photo for comment failed: %s", e)

    # документ
    if msg.document:
        try:
            tg_file = msg.document.get_file()
            bio = BytesIO()
            tg_file.download(out=bio)
            bio.seek(0)
            filename = msg.document.file_name or f"file_{tg_file.file_unique_id}"
            mime = msg.document.mime_type or "application/octet-stream"
            files.append(("file", (filename, bio, mime)))
        except Exception as e:
            logger.error("download document for comment failed: %s", e)

    if not text and not files:
        update.message.reply_text("Комментарий не может быть пустым, отправь текст или файл/фото.")
        return STATE_COMMENT_TEXT

    ok, msg_resp = api_add_order_comment(session, oid, text, files=files or None)
    if not ok:
        # msg_resp может быть очень длинным (HTML-страница ошибки и т.п.), режем до безопасной длины
        safe_msg = (msg_resp or "")
        if len(safe_msg) > 1000:
            safe_msg = safe_msg[:1000] + "…"
        update.message.reply_text(safe_msg, reply_markup=ReplyKeyboardRemove())
        return STATE_MENU
    update.message.reply_text(
        "Комментарий добавлен.\n\nМожешь добавить ещё или вернуться в меню.",
        reply_markup=ReplyKeyboardMarkup(
            [["Оставить комментарий", "В меню"]],
            one_time_keyboard=True,
            resize_keyboard=True,
        ),
    )
    return STATE_COMMENT_MENU

def quick_take(update: Update, context: CallbackContext) -> int:
    """Stage 1: preview candidate (masked card + rate) with choice."""
    chat_id = update.effective_chat.id
    session = user_sessions.get(chat_id)
    if not session:
        update.message.reply_text("Сначала войдите: /start")
        return STATE_MENU

    items = api_get_available_orders(session)
    if not items:
        update.message.reply_text("Нет доступных ордеров для взятия.")
        return STATE_MENU

    items.sort(key=_order_sort_key, reverse=True)
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
        keyboard = [["Оплачено"]]
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

# ─────────────────────────── Router ──────────────────────────
def handle_menu_choice(update: Update, context: CallbackContext) -> int:
    text = update.message.text
    if text == "Доступные ордера":
        return cmd_available_orders(update, context)
    if text == "Мои активные ордера":
        return cmd_my_active_orders(update, context)
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
                # Inline callbacks while in menu
                CallbackQueryHandler(cb_open_order, pattern=r"^ord:\d+$"),
                CallbackQueryHandler(cb_page, pattern=r"^(avail|myact):page:\d+$"),
                CallbackQueryHandler(cb_withdraw_pick_maker, pattern=r"^wd_maker:"),
                # Text menu
                MessageHandler(Filters.text & ~Filters.command, handle_menu_choice),
            ],

            # Available/my order view
            STATE_VIEW_ORDER: [MessageHandler(Filters.text & ~Filters.command, _bot_handle_view_order_action)],
            STATE_PARTIAL_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, _bot_handle_partial_amount)],
            STATE_COMMENT_MENU: [MessageHandler(Filters.text & ~Filters.command, _bot_handle_comment_menu)],
            STATE_COMMENT_MENU: [MessageHandler(Filters.text & ~Filters.command, _bot_handle_comment_menu)],
            STATE_COMMENT_TEXT: [MessageHandler((Filters.text | Filters.photo | Filters.document) & ~Filters.command, _bot_handle_comment_text)],

            # Quick take flow
            STATE_QUICK_PREVIEW: [MessageHandler(Filters.text & ~Filters.command, handle_quick_preview_choice)],

            # Withdraw
            STATE_WITHDRAW_PICK_MAKER: [CallbackQueryHandler(cb_withdraw_pick_maker, pattern=r"^wd_maker:")],
            STATE_WITHDRAW_ADDRESS: [MessageHandler(Filters.text & ~Filters.command, handle_withdraw_address)],
        },
        fallbacks=[
            CommandHandler("logout", logout),
            CommandHandler("menu", show_menu),
            MessageHandler(Filters.text & ~Filters.command, fallback_show_menu),
        ],
        allow_reentry=True,
    )

    dp.add_handler(conv)
    dp.add_handler(CommandHandler("logout", logout))
    dp.add_handler(CommandHandler("menu", show_menu))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
