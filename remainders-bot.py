import logging
import os
from io import BytesIO
from typing import Dict, Optional, List, Tuple

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
    STATE_WAIT_RECEIPT,
) = range(4)

# In-memory per-chat session state
user_sessions: Dict[int, requests.Session] = {}
pending_receipt_order: Dict[int, int] = {}  # chat_id -> order_id


def _menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Доступные остатки"], ["/logout"]],
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


def _upload_receipt_to_order(
    session: requests.Session, order_id: int, filename: str, file_bytes: bytes
) -> None:
    files = {"file": (filename, file_bytes)}
    data = {"text": "Квитанция по оплате остатка"}
    r = session.post(
        f"{BASE_URL}/api/order/{order_id}/comments/",
        data=data,
        files=files,
        headers=_csrf_headers(session),
        timeout=40,
    )
    if not r.ok:
        raise RuntimeError(f"Upload receipt failed: {r.status_code} {r.text}")


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
    pending_receipt_order.pop(chat_id, None)
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

        text = (
            f"Ордер: {ext}\n"
            f"Сумма остатка: {amount_uah} грн\n"
            f"Банк: {bank}\n"
            f"Карта: {card_full}\n\n"
            f"После оплаты нажми «Оплачено» и прикрепи квитанцию."
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Оплачено", callback_data=f"rem:paid:{order_id}")]]
        )
        query.edit_message_text(text, reply_markup=kb)
        return STATE_MENU

    if data.startswith("rem:paid:"):
        try:
            order_id = int(data.split(":")[-1])
        except Exception:
            query.edit_message_text("Некорректный order_id")
            return STATE_MENU

        pending_receipt_order[chat_id] = order_id
        query.edit_message_text("Отправь квитанцию (фото или файл).")
        return STATE_WAIT_RECEIPT

    query.edit_message_text("Неизвестная команда.")
    return STATE_MENU


def on_receipt(update: Update, context: CallbackContext):
    s = _ensure_session(update)
    if not s:
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    order_id = pending_receipt_order.get(chat_id)
    if not order_id:
        update.message.reply_text(
            "Нет выбранного остатка. Нажми «Доступные остатки».",
            reply_markup=_menu_kb(),
        )
        return STATE_MENU

    file_bytes: Optional[bytes] = None
    filename: str = "receipt"

    try:
        if update.message.photo:
            photo = update.message.photo[-1]
            tg_file = photo.get_file()
            bio = BytesIO()
            tg_file.download(out=bio)
            file_bytes = bio.getvalue()
            filename = "receipt.jpg"
        elif update.message.document:
            doc = update.message.document
            tg_file = doc.get_file()
            bio = BytesIO()
            tg_file.download(out=bio)
            file_bytes = bio.getvalue()
            filename = doc.file_name or "receipt.bin"
        else:
            update.message.reply_text("Пришли фото или файл (document).", reply_markup=_menu_kb())
            return STATE_WAIT_RECEIPT
    except Exception as e:
        update.message.reply_text(
            f"Не смог скачать файл из Telegram: {e}", reply_markup=_menu_kb()
        )
        return STATE_MENU

    try:
        _upload_receipt_to_order(s, order_id, filename, file_bytes or b"")
    except Exception as e:
        update.message.reply_text(f"Не удалось загрузить квитанцию: {e}", reply_markup=_menu_kb())
        return STATE_MENU

    pending_receipt_order.pop(chat_id, None)
    update.message.reply_text("Квитанция прикреплена ✅", reply_markup=_menu_kb())
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
                MessageHandler(Filters.regex(r"^Доступные остатки снова$"), show_remainders),
                CommandHandler("logout", logout),
            ],
            STATE_WAIT_RECEIPT: [
                MessageHandler(Filters.photo | Filters.document, on_receipt),
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
