import logging
import os
from io import BytesIO
from typing import Dict, Optional, List, Tuple
from collections import deque

import requests
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("remainders-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

# Conversation states
(
    STATE_USERNAME,
    STATE_PASSWORD,
    STATE_MENU,
    STATE_WAIT_RECEIPT_FILE,
    STATE_WAIT_CARD,
) = range(5)

# In-memory per-chat session state
user_sessions: Dict[int, requests.Session] = {}
pending_paid_orders = deque(maxlen=500)  # items: (chat_id:int, order_id:int)
pending_receipt_blob = deque(maxlen=500)  # items: (chat_id:int, token:str, filename:str, data:bytes, mime:str)

def _pp_remove_paid(chat_id: int, order_id: int) -> None:
    try:
        while True:
            pending_paid_orders.remove((chat_id, order_id))
    except ValueError:
        return

def _pp_add_paid(chat_id: int, order_id: int) -> None:
    _pp_remove_paid(chat_id, order_id)
    pending_paid_orders.append((chat_id, order_id))

def _pp_paid_set(chat_id: int) -> set:
    return {oid for (cid, oid) in pending_paid_orders if cid == chat_id}

def _rb_push(chat_id: int, filename: str, bio: BytesIO, mime: str) -> str:
    token = f"{chat_id}:{int(__import__('time').time()*1000)}:{__import__('uuid').uuid4().hex[:8]}"
    pending_receipt_blob.append((chat_id, token, filename, bio.getvalue(), mime))
    return token

def _rb_get(chat_id: int, token: Optional[str]) -> Optional[tuple]:
    if token:
        for item in reversed(pending_receipt_blob):
            if item[0] == chat_id and item[1] == token:
                return item
    # fallback: latest receipt for this chat
    for item in reversed(pending_receipt_blob):
        if item[0] == chat_id:
            return item
    return None

def _rb_pop(chat_id: int, token: Optional[str]) -> None:
    item = _rb_get(chat_id, token)
    if not item:
        return
    try:
        pending_receipt_blob.remove(item)
    except ValueError:
        pass


def _menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Доступные остатки", "Прикрепить квитанцию"], ["\/logout"]],
        resize_keyboard=True,
    )


# ───────────────────────── CSRF helpers ─────────────────────────

def _get_csrf_token(session: requests.Session) -> Optional[str]:
    token = session.cookies.get("csrftoken") or session.cookies.get("csrf")
    if token:
        return token
    # poke API / root to get CSRF cookie if server issues it
    for url in (f"{BASE_URL}/api/me/", f"{BASE_URL}/"):
        try:
            session.get(url, timeout=10)
            token = session.cookies.get("csrftoken") or session.cookies.get("csrf")
            if token:
                return token
        except Exception:
            continue
    return None


def _csrf_headers(session: requests.Session) -> dict:
    token = _get_csrf_token(session)
    if not token:
        return {}
    return {
        "X-CSRFToken": token,
        "Referer": BASE_URL + "/",
    }


# ───────────────────────── API helpers ──────────────────────────

def api_login(username: str, password: str) -> Optional[requests.Session]:
    s = requests.Session()
    try:
        r = s.post(
            f"{BASE_URL}/api/login/",
            data={"username": username, "password": password},
            timeout=15,
        )
        if r.ok:
            _get_csrf_token(s)
            return s
        logger.warning("Login failed: %s %s", r.status_code, r.text)
        return None
    except Exception as e:
        logger.exception("Login error: %s", e)
        return None


def _ensure_session(update: Update) -> Optional[requests.Session]:
    chat_id = update.effective_chat.id
    s = user_sessions.get(chat_id)
    if not s:
        if update.message:
            update.message.reply_text("Нужно залогиниться. Нажми /start")
        return None
    return s


def _fetch_remainders(session: requests.Session) -> List[dict]:
    r = session.get(f"{BASE_URL}/api/taker-remainders/", timeout=20)
    if not r.ok:
        raise RuntimeError(
            f"GET /api/taker-remainders/ failed: {r.status_code} {r.text}"
        )
    data = r.json() or {}
    rems = data.get("remainders") or []
    if not isinstance(rems, list):
        return []
    return rems


def _reserve_remainder(session: requests.Session, order_id: int) -> Tuple[str, str]:
    # Returns (reserved_uah, detail)
    r = session.post(
        f"{BASE_URL}/api/taker-remainders/reserve/",
        json={"order_id": order_id},
        headers=_csrf_headers(session),
        timeout=20,
    )
    if r.status_code in (401, 403):
        raise PermissionError(r.text)
    if not r.ok:
        raise RuntimeError(f"POST reserve failed: {r.status_code} {r.text}")
    data = r.json() or {}
    return str(data.get("reserved_uah") or ""), str(data.get("detail") or "ok")


def _fetch_order_detail(session: requests.Session, order_id: int) -> dict:
    r = session.get(f"{BASE_URL}/api/orders/{order_id}/", timeout=20)
    if not r.ok:
        raise RuntimeError(
            f"GET /api/orders/{order_id}/ failed: {r.status_code} {r.text}"
        )
    return r.json() or {}


def api_add_order_comment(session: requests.Session, order_id: int, text: str, files=None) -> Tuple[bool, str]:
    """Как в bot.py: добавить комментарий к ордеру с текстом и/или файлами."""
    headers = _csrf_headers(session)
    try:
        kwargs = {"headers": headers, "timeout": 40}
        data = {"text": text or "Файл добавлен."}
        kwargs["data"] = data
        if files:
            kwargs["files"] = files
        r = session.post(f"{BASE_URL}/api/order/{order_id}/comments/", **kwargs)
        if r.ok:
            return True, "OK"
        try:
            body = r.json()
        except Exception:
            body = r.text
        return False, f"{r.status_code}: {body}"
    except Exception as e:
        logger.error("add_comment error: %s", e)
        return False, str(e)


def _upload_receipt_to_order(session: requests.Session, order_id: int, filename: str, bio: BytesIO, mime: str) -> None:
    bio.seek(0)
    files = [("file", (filename, bio, mime or "application/octet-stream"))]
    ok, msg = api_add_order_comment(session, order_id, "Квитанция по оплате остатка", files=files)
    if not ok:
        raise RuntimeError(f"Upload receipt failed: {msg}")



# ───────────────────────── Handlers ────────────────────────────

def start(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Бот для оплаты остатков.\n\nВведи username:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_USERNAME


def on_username(update: Update, context: CallbackContext):
    context.user_data["username"] = (update.message.text or "").strip()
    update.message.reply_text("Теперь пароль:")
    return STATE_PASSWORD


def on_password(update: Update, context: CallbackContext):
    username = (context.user_data.get("username") or "").strip()
    password = (update.message.text or "").strip()
    if not username or not password:
        update.message.reply_text("Нужны username и пароль. /start")
        return ConversationHandler.END

    s = api_login(username, password)
    if not s:
        update.message.reply_text(
            "Не удалось залогиниться. Проверь логин/пароль. /start"
        )
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    user_sessions[chat_id] = s
    update.message.reply_text("Ок. Выбери действие:", reply_markup=_menu_kb())
    return STATE_MENU


def logout(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    user_sessions.pop(chat_id, None)
        # drop any pending in-memory state for this chat (bounded deques)
    try:
        for item in list(pending_paid_orders):
            if item[0] == chat_id:
                pending_paid_orders.remove(item)
    except Exception:
        pass
    try:
        for item in list(pending_receipt_blob):
            if item[0] == chat_id:
                pending_receipt_blob.remove(item)
    except Exception:
        pass
    update.message.reply_text(
        "Вышел. /start чтобы войти снова.", reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


def show_remainders(update: Update, context: CallbackContext):
    s = _ensure_session(update)
    if not s:
        return ConversationHandler.END

    try:
        rems = _fetch_remainders(s)
    except Exception as e:
        update.message.reply_text(f"Ошибка получения остатков: {e}", reply_markup=_menu_kb())
        return STATE_MENU

    if not rems:
        update.message.reply_text("Доступных остатков нет.", reply_markup=_menu_kb())
        return STATE_MENU

    buttons: List[List[InlineKeyboardButton]] = []
    for r in rems[:50]:  # safety cap
        oid = r.get("order_id")
        ext = r.get("external_order_id") or oid
        maker = r.get("maker_username") or ""
        amount = r.get("remaining_uah") or ""
        mask = r.get("card_mask") or ""
        if oid is None:
            continue
        title = f"{ext} | {maker} | {amount} грн | {mask}"
        buttons.append([InlineKeyboardButton(title, callback_data=f"rem:pick:{oid}")])

    buttons.append([InlineKeyboardButton("Обновить", callback_data="rem:refresh")])
    update.message.reply_text(
        "Доступные остатки (выбери ордер):",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return STATE_MENU


def remainders_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()

    chat_id = update.effective_chat.id
    s = user_sessions.get(chat_id)
    if not s:
        query.edit_message_text("Сессия потеряна. /start")
        return ConversationHandler.END

    data = query.data or ""

    if data == "rem:refresh":
        try:
            rems = _fetch_remainders(s)
        except Exception as e:
            query.edit_message_text(f"Ошибка получения остатков: {e}")
            return STATE_MENU

        if not rems:
            query.edit_message_text("Доступных остатков нет.")
            return STATE_MENU

        buttons: List[List[InlineKeyboardButton]] = []
        for r in rems[:50]:
            oid = r.get("order_id")
            ext = r.get("external_order_id") or oid
            maker = r.get("maker_username") or ""
            amount = r.get("remaining_uah") or ""
            mask = r.get("card_mask") or ""
            if oid is None:
                continue
            title = f"{ext} | {maker} | {amount} грн | {mask}"
            buttons.append([InlineKeyboardButton(title, callback_data=f"rem:pick:{oid}")])
        buttons.append([InlineKeyboardButton("Обновить", callback_data="rem:refresh")])
        query.edit_message_text(
            "Доступные остатки (выбери ордер):",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return STATE_MENU

    if data.startswith("rem:pick:"):
        try:
            order_id = int(data.split(":")[-1])
        except Exception:
            query.edit_message_text("Некорректный order_id")
            return STATE_MENU

        # 1) Reserve remainder
        try:
            reserved_uah, _detail = _reserve_remainder(s, order_id)
        except Exception as e:
            query.edit_message_text(f"Не удалось зарезервировать остаток: {e}")
            return STATE_MENU

        # 2) Fetch full card from order detail
        try:
            od = _fetch_order_detail(s, order_id)
        except Exception as e:
            query.edit_message_text(
                f"Резерв создан, но не удалось получить детали ордера: {e}"
            )
            return STATE_MENU

        card_full = str(
            od.get("card_full")
            or od.get("card_number")
            or od.get("card")
            or od.get("iban")
            or od.get("card_mask")
            or "—"
        )
        bank = str(od.get("bank") or "")
        ext = str(od.get("external_order_id") or order_id)
        amount_uah = reserved_uah or str(od.get("remaining_uah") or "")

        # Считаем, что если реквизиты показаны — оплата уже начата и мы ждём квитанцию.
        _pp_add_paid(chat_id, order_id)


        text = (
            f"Ордер: {ext}\n"
            f"Сумма остатка: {amount_uah} грн\n"
            f"Банк: {bank}\n"
            f"Карта: {card_full}\n\n"
            f"Квитанцию прикрепи через меню: «Прикрепить квитанцию»."
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("В меню", callback_data="rem:menu")]]
        )
        query.edit_message_text(text, reply_markup=kb)
        return STATE_MENU

    if data.startswith("rem:paid:"):
        try:
            order_id = int(data.split(":")[-1])
        except Exception:
            query.edit_message_text("Некорректный order_id")
            return STATE_MENU

        _pp_add_paid(chat_id, order_id)
        query.edit_message_text(
            "Отметил как оплачено. Можешь оплатить другие остатки параллельно.\n\n"
            "Когда будет квитанция — открой меню и нажми «Прикрепить квитанцию».",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("В меню", callback_data="rem:menu")]]
            ),
        )
        return STATE_MENU

    if data == "rem:menu":
        query.edit_message_text("Меню")
        context.bot.send_message(chat_id=chat_id, text="Выбери действие:", reply_markup=_menu_kb())
        return STATE_MENU

    if data.startswith("rem:attach:"):
        try:
            parts = data.split(":")
            oid = int(parts[2])
            token = parts[3] if len(parts) > 3 and parts[3] else None
            context.user_data['receipt_token'] = token or context.user_data.get('receipt_token')
            
        except Exception:
            query.edit_message_text("Некорректный order_id")
            return STATE_MENU

        blob = _rb_get(chat_id, context.user_data.get('receipt_token'))
        if not blob:
            query.edit_message_text("Нет квитанции. Нажми «Прикрепить квитанцию» и пришли файл.")
            return STATE_MENU

        filename, bio, mime = blob
        try:
            _upload_receipt_to_order(s, oid, filename, bio, mime)
        except Exception as e:
            query.edit_message_text(f"Не удалось прикрепить квитанцию: {e}")
            return STATE_MENU

        _rb_pop(chat_id, context.user_data.get('receipt_token'))
        context.user_data.pop('receipt_token', None)
        _pp_remove_paid(chat_id, oid)
        query.edit_message_text(f"Квитанция прикреплена к ордеру #{oid}.")
        context.bot.send_message(chat_id=chat_id, text="Выбери действие:", reply_markup=_menu_kb())
        return STATE_MENU

    query.edit_message_text("Неизвестная команда.")
    return STATE_MENU


def _digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def show_menu(update: Update, context: CallbackContext):
    # Just re-show menu keyboard
    if update.message:
        update.message.reply_text("Выбери действие:", reply_markup=_menu_kb())
    return STATE_MENU


def start_attach_receipt(update: Update, context: CallbackContext):
    s = _ensure_session(update)
    if not s:
        return ConversationHandler.END
    update.message.reply_text(
        "Пришли квитанцию (фото или файл).",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_WAIT_RECEIPT_FILE


def on_receipt_file(update: Update, context: CallbackContext):
    s = _ensure_session(update)
    if not s:
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    msg = update.message

    files = []
    filename = "receipt.bin"
    mime = "application/octet-stream"
    bio = BytesIO()

    try:
        if msg.photo:
            photo = msg.photo[-1]
            tg_file = photo.get_file()
            tg_file.download(out=bio)
            bio.seek(0)
            filename = f"receipt_{tg_file.file_unique_id}.jpg"
            mime = "image/jpeg"
        elif msg.document:
            doc = msg.document
            tg_file = doc.get_file()
            tg_file.download(out=bio)
            bio.seek(0)
            filename = doc.file_name or f"receipt_{tg_file.file_unique_id}"
            mime = doc.mime_type or "application/octet-stream"
        else:
            msg.reply_text("Пришли фото или файл (document).", reply_markup=_menu_kb())
            return STATE_MENU
    except Exception as e:
        msg.reply_text(f"Не смог скачать файл из Telegram: {e}", reply_markup=_menu_kb())
        return STATE_MENU

    token = _rb_push(chat_id, filename, bio, mime)
    context.user_data['receipt_token'] = token
    msg.reply_text("Теперь укажи номер карты получателя (можно последние 4 цифры).")
    return STATE_WAIT_CARD


def _find_order_candidates_by_card(session: requests.Session, card_digits: str, allowed_order_ids: Optional[set]) -> List[int]:
    card_digits = _digits(card_digits)
    if not card_digits:
        return []
    last4 = card_digits[-4:] if len(card_digits) >= 4 else card_digits

    try:
        rems = _fetch_remainders(session)
    except Exception:
        rems = []

    candidates = []
    for r in rems:
        oid = r.get("order_id")
        if oid is None:
            continue
        try:
            oid = int(oid)
        except Exception:
            continue
        if allowed_order_ids and oid not in allowed_order_ids:
            continue
        mask = _digits(str(r.get("card_mask") or ""))
        # Quick match by last4
        if last4 and (last4 in mask):
            candidates.append(oid)

    # If nothing matched by mask, try fetching full card for allowed orders (slower but accurate)
    if not candidates and allowed_order_ids:
        for oid in list(allowed_order_ids)[:30]:
            try:
                od = _fetch_order_detail(session, oid)
            except Exception:
                continue
            full = _digits(str(
                od.get("card_full")
                or od.get("card_number")
                or od.get("card")
                or od.get("iban")
                or od.get("card_mask")
                or ""
            ))
            if not full:
                continue
            if last4 and (full.endswith(last4) or last4 in full):
                candidates.append(oid)

    # uniq preserve order
    out = []
    for oid in candidates:
        if oid not in out:
            out.append(oid)
    return out


def on_card_number(update: Update, context: CallbackContext):
    s = _ensure_session(update)
    if not s:
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    blob = _rb_get(chat_id, context.user_data.get('receipt_token'))
    if not blob:
        update.message.reply_text("Сначала нажми «Прикрепить квитанцию» и пришли файл.", reply_markup=_menu_kb())
        return STATE_MENU

    card_in = (update.message.text or "").strip()
    card_digits = _digits(card_in)
    if len(card_digits) < 4:
        update.message.reply_text("Нужно хотя бы 4 цифры (последние 4). Попробуй ещё раз.")
        return STATE_WAIT_CARD

    allowed = _pp_paid_set(chat_id)
    allowed = set(allowed) if allowed else None

    matches = _find_order_candidates_by_card(s, card_digits, allowed)

    if not matches:
        update.message.reply_text(
            "Не нашёл ордер по этой карте среди отмеченных оплат. "
            "Проверь последние 4 цифры или отметь оплату кнопкой «Оплачено» у остатка.",
            reply_markup=_menu_kb(),
        )
        return STATE_MENU

    filename, bio, mime = blob

    if len(matches) == 1:
        oid = matches[0]
        try:
            _upload_receipt_to_order(s, oid, filename, bio, mime)
        except Exception as e:
            update.message.reply_text(f"Не удалось прикрепить квитанцию: {e}", reply_markup=_menu_kb())
            return STATE_MENU

        _rb_pop(chat_id, context.user_data.get('receipt_token'))
        context.user_data.pop('receipt_token', None)
        _pp_remove_paid(chat_id, oid)
        update.message.reply_text(f"Квитанция прикреплена к ордеру #{oid}.", reply_markup=_menu_kb())
        return STATE_MENU

    # multiple matches → ask to choose
    rows = []
    for oid in matches[:20]:
        rows.append([InlineKeyboardButton(f"Ордер #{oid}", callback_data=f"rem:attach:{oid}:{context.user_data.get('receipt_token', '')}")])
    update.message.reply_text(
        "Нашёл несколько ордеров по этой карте. Выбери, куда прикрепить квитанцию:",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return STATE_MENU




def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env var is required")

    updater = Updater(token=BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            STATE_USERNAME: [MessageHandler(Filters.text & ~Filters.command, on_username)],
            STATE_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, on_password)],
            STATE_MENU: [
                MessageHandler(Filters.regex(r"^Доступные остатки$"), show_remainders),
                MessageHandler(Filters.regex(r"^Прикрепить квитанцию$"), start_attach_receipt),
                MessageHandler(Filters.regex(r"^В меню$"), show_menu),
                CommandHandler("logout", logout),
            ],
            STATE_WAIT_RECEIPT_FILE: [
                MessageHandler(Filters.photo | Filters.document, on_receipt_file),
                CommandHandler("logout", logout),
            ],
            STATE_WAIT_CARD: [
                MessageHandler(Filters.text & ~Filters.command, on_card_number),
                CommandHandler("logout", logout),
            ],
        },
        fallbacks=[CommandHandler("logout", logout)],
        allow_reentry=True,
    )

    dp.add_handler(conv)
    dp.add_handler(CallbackQueryHandler(remainders_callback, pattern=r"^rem:"))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()

