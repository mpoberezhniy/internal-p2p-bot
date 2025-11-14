import os, re, logging, requests
from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, ConversationHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("p2p-notif-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

STATE_USERNAME, STATE_PASSWORD, STATE_RUNNING = range(3)

sessions = {}       # chat_id -> requests.Session
last_notif_id = {}  # chat_id -> int

def _csrf(s: requests.Session):
    t = s.cookies.get("csrftoken") or s.cookies.get("csrf")
    if not t:
        try:
            s.get(f"{BASE_URL}/", timeout=10)
        except Exception:
            pass
        t = s.cookies.get("csrftoken") or s.cookies.get("csrf")
    return {"X-CSRFToken": t, "Referer": BASE_URL + "/"} if t else {}

def api_login(username: str, password: str):
    s = requests.Session()
    r = s.post(f"{BASE_URL}/api/login/", data={"username": username, "password": password}, timeout=15)
    if r.ok:
        _csrf(s)
        return s
    log.error("login failed %s %s", r.status_code, r.text)
    return None

def api_me(s: requests.Session):
    try:
        r = s.get(f"{BASE_URL}/api/me/", timeout=10)
        return r.json() if r.ok else None
    except Exception:
        return None

def api_notifications_since(s: requests.Session, since: int):
    try:
        r = s.get(f"{BASE_URL}/api/notifications/", params={"since": since}, timeout=15)
        if r.ok:
            body = r.json() or {}
            return body.get("notifications", [])
    except Exception:
        pass
    return []



def api_order_comments(s: requests.Session, order_id: int):
    """Получить комментарии и вложения ордера для вывода в уведомлениях."""
    try:
        r = s.get(f"{BASE_URL}/api/order/{order_id}/comments/", timeout=15)
        if r.ok:
            return r.json() or {}
    except Exception as e:
        log.error("order_comments error: %s", e)
    return {"results": [], "order_attachments": []}


def _extract_order_id_from_url(url: str):
    """Вытащить pk ордера из Notification.url вида /order/<pk>/"""
    if not url:
        return None
    m = re.search(r"/order/(\d+)/", url)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _format_comments_block(data):
    """Сформировать текстовый блок с последними комментариями к ордеру."""
    comments = (data or {}).get("results") or []
    if not comments:
        return None
    # берём последние несколько, чтобы не заспамить
    lines = ["", "", "Комментарии по ордеру:"]
    for c in comments:
        uname = c.get("username") or f"id {c.get('user_id')}"
        text = (c.get("text") or "").strip()
        if len(text) > 300:
            text = text[:300] + "…"
        lines.append(f"- {uname}: {text}")
    block = "\n".join(lines)
    # подстрахуемся от переполнения лимита Telegram
    if len(block) > 1500:
        block = block[:1500] + "…"
    return block

def start(update: Update, _):
    if update.effective_chat.id in sessions:
        update.message.reply_text("Вы уже подписаны на уведомления. /stop для выхода.")
        return STATE_RUNNING
    update.message.reply_text("Уведомления: введите имя пользователя:")
    return STATE_USERNAME

def handle_username(update: Update, context: CallbackContext):
    context.user_data["username"] = update.message.text.strip()
    update.message.reply_text("Введите пароль:")
    return STATE_PASSWORD

def handle_password(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    u, p = context.user_data.get("username"), update.message.text
    s = api_login(u, p)
    if not s:
        update.message.reply_text("Ошибка входа. Попробуйте снова: /start")
        return ConversationHandler.END
    me = api_me(s)
    if not me:
        update.message.reply_text("Профиль не получен. /start")
        return ConversationHandler.END
    sessions[chat_id] = s
    last_notif_id[chat_id] = 0
    update.message.reply_text("Подписка на уведомления включена. Используйте /stop для отключения.")
    # запустим поллинг через job_queue
    jobname = f"notif_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(jobname):
        job.schedule_removal()
    context.job_queue.run_repeating(poll_notifications, interval=5, first=2,
                                    context={"chat_id": chat_id}, name=jobname)
    return STATE_RUNNING

def poll_notifications(context: CallbackContext):
    chat_id = context.job.context["chat_id"]
    s = sessions.get(chat_id)
    if not s:
        return
    last_id = last_notif_id.get(chat_id, 0)
    items = api_notifications_since(s, last_id)
    items.reverse()
    for n in items:
        nid = n.get("id") or 0
        if nid > last_id:
            last_notif_id[chat_id] = nid
        msg = n.get("message", "")
        # основное уведомление
        final_msg = msg

        # если это уведомление о завершённом ордере для тейкера – подтягиваем комментарии
        msg_low = msg.lower()
        logging.info(msg_low)
        if "confirmed payment for order" in msg_low:
            logging.info("confirmed payment for order" in msg_low)
            url = n.get("url", "")
            order_id = _extract_order_id_from_url(url)
            logging.info(order_id)
            if order_id:
                data = api_order_comments(s, order_id)
                logging.info(str(data))
                block = _format_comments_block(data)
                if block:
                    final_msg += block
        try:
            context.bot.send_message(chat_id, final_msg)
        except Exception as e:
            log.error("send notification failed: %s", e)

def stop(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    for d in (sessions, last_notif_id):
        d.pop(chat_id, None)
    for job in context.job_queue.get_jobs_by_name(f"notif_{chat_id}"):
        job.schedule_removal()
    update.message.reply_text("Уведомления отключены.")
    return ConversationHandler.END

def main():
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("BOT_TOKEN отсутствует")
    up = Updater(token, use_context=True)
    dp = up.dispatcher
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            STATE_USERNAME: [MessageHandler(Filters.text & ~Filters.command, handle_username)],
            STATE_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, handle_password)],
            STATE_RUNNING:  [MessageHandler(Filters.text & ~Filters.command, lambda u,c: STATE_RUNNING)],
        },
        fallbacks=[CommandHandler("stop", stop)],
        allow_reentry=True,
    )
    dp.add_handler(conv)
    dp.add_handler(CommandHandler("stop", stop))
    up.start_polling()
    up.idle()

if __name__ == "__main__":
    main()
