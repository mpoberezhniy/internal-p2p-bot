import logging
import os
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal, getcontext
from typing import Dict, Tuple, List, Optional

import csv
import io
import requests
from binance.client import Client
from telegram import Update
from telegram.ext import (
    Updater,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)

# --- decimal precision ---
getcontext().prec = 16

# --- logging ---
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ============ CONFIG VIA ENV ============

TELEGRAM_BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# Binance SELL api key/secret
BINANCE_API_KEY_SELL = os.environ.get("BINANCE_API_KEY_SELL", "MXzox85hJVqFFn3ww4Xg8afkNLHsG32rZFVwhgsbvwX5GykiGTk0UtyEN9fModxX")
BINANCE_API_SECRET_SELL = os.environ.get("BINANCE_API_SECRET_SELL", "XjeA8XC3XVN0Tgu88eqcxCJ42cdtogNKmk4aJEldv5V3BmUIQOp0HugACIso3KJx")

# Internal-p2p BASE / LOGIN
INTERNAL_P2P_BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
INTERNAL_P2P_LOGIN_URL = os.environ.get("INTERNAL_P2P_LOGIN_URL", "").strip()

FIAT = os.environ.get("FIAT", "UAH")
ASSET = os.environ.get("ASSET", "USDT")

# ============ STATES FOR /profitw WIZARD ============
STATE_PERIOD = 1
STATE_FILTERS = 2

# ============ HELP TEXTS ============

HELP_TEXT = (
    "Я считаю профит между BUY (internal-p2p CSV) и SELL (Binance P2P).\n\n"
    "Сначала авторизуйся:\n"
    "  /login <username> <password>\n\n"
    "Быстрый режим:\n"
    "  /profit <from_date> <to_date> [фильтры]\n"
    "Примеры:\n"
    "  /profit 2025-12-01 2025-12-07\n"
    "  /profit 2025-12-01 2025-12-07 bank=MONO status=COMPLETED\n"
    "  /profit 2025-12-01 2025-12-07 maker=1 taker=2\n\n"
    "Диалоговый режим с удобным вводом фильтров:\n"
    "  /profitw\n\n"
    "Даты: YYYY-MM-DD (включительно). Фильтры прокидываются в CSV-эндпоинт "
    "как GET-параметры (bank, status, maker, taker, q, min_uah, max_uah, min_usdt, max_usdt и т.д.)."
)


# ============ HELPERS ============

def parse_date_str(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def aggregate_trades_binance(trades: List[dict]) -> Tuple[Decimal, Decimal]:
    total_crypto = Decimal("0")
    total_fiat = Decimal("0")
    for t in trades:
        if t.get("asset") != ASSET:
            continue
        if t.get("fiat") != FIAT:
            continue
        if t.get("orderStatus") != "COMPLETED":
            continue
        amount = Decimal(str(t["amount"]))
        total_price = Decimal(str(t["totalPrice"]))
        total_crypto += amount
        total_fiat += total_price
    return total_fiat, total_crypto

def aggregate_cancelled_trades_binance(trades: List[dict]) -> Tuple[int, Decimal]:
    """Считает количество и объем (в USDT) отмененных SELL-ордеров в Binance P2P.

    Binance C2C trade history возвращает orderStatus, где для отмен:
      - CANCELLED
      - CANCELLED_BY_SYSTEM
    (держим также CANCELED/CANCELED_BY_SYSTEM на всякий случай).
    """
    cancelled_statuses = {
        "CANCELLED",
        "CANCELLED_BY_SYSTEM",
    }
    cnt = 0
    total_crypto = Decimal("0")
    for t in trades:
        if t.get("asset") != ASSET:
            continue
        if t.get("fiat") != FIAT:
            continue
        if t.get("orderStatus") not in cancelled_statuses:
            continue
        try:
            amount = Decimal(str(t.get("amount", "0")))
        except Exception:
            amount = Decimal("0")
        cnt += 1
        total_crypto += amount
    return cnt, total_crypto



def get_binance_sell_trades(
    client: Client,
    start_dt: datetime,
    end_dt: datetime,
) -> List[dict]:
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    page = 1
    rows = 50
    trades: List[dict] = []

    while True:
        resp = client.get_c2c_trade_history(
            tradeType="SELL",
            startTimestamp=start_ts,
            endTimestamp=end_ts,
            page=page,
            rows=rows,
        )
        if not resp.get("success", False):
            raise RuntimeError(f"Binance returned error: {resp}")
        data = resp.get("data", [])
        if not data:
            break
        trades.extend(data)
        if len(data) < rows:
            break
        page += 1

    return trades


def get_internal_p2p_csv_url() -> str:
    if not INTERNAL_P2P_BASE_URL:
        raise RuntimeError("BASE_URL is not set")
    return f"{INTERNAL_P2P_BASE_URL.rstrip('/')}/orders/export.csv"


def get_internal_p2p_login_url() -> str:
    if INTERNAL_P2P_LOGIN_URL:
        return INTERNAL_P2P_LOGIN_URL
    if not INTERNAL_P2P_BASE_URL:
        raise RuntimeError("Neither INTERNAL_P2P_LOGIN_URL nor INTERNAL_P2P_BASE_URL is set")
    return f"{INTERNAL_P2P_BASE_URL.rstrip('/')}/api/login/"


def build_internal_p2p_params(
    from_date: datetime,
    to_date: datetime,
    extra_filters: Dict[str, str],
) -> Dict[str, str]:
    """
    Формируем query-параметры для CSV.
    Никакого дефолтного status=COMPLETED — только если пользователь явно указал.
    """
    params: Dict[str, str] = {
        "created_from": from_date.date().isoformat(),
        "created_to": to_date.date().isoformat(),
    }
    for key, value in extra_filters.items():
        params[key] = value
    return params


def fetch_internal_p2p_csv(
    from_date: datetime,
    to_date: datetime,
    extra_filters: Dict[str, str],
    cookies: Optional[Dict[str, str]],
) -> str:
    url = get_internal_p2p_csv_url()
    params = build_internal_p2p_params(from_date, to_date, extra_filters)
    logger.info(f'getting csv from {url = }, {params = }')
    resp = requests.get(
        url,
        params=params,
        cookies=cookies,
        timeout=60,
    )
    logger.info("CSV response: status=%s len=%s first_300=%r",
                resp.status_code, len(resp.text), resp.text[:300])
    if resp.status_code != 200:
        raise RuntimeError(f"internal-p2p CSV error: {resp.status_code} {resp.text[:500]}")
    return resp.text


def aggregate_internal_p2p_from_csv(csv_text: str) -> Tuple[Decimal, Decimal, int]:
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    total_uah = Decimal("0")
    total_usdt = Decimal("0")
    rows = 0
    for row in reader:
        rows += 1
        try:
            uah = Decimal(row.get("uah") or "0")
            usdt = Decimal(row.get("usdt") or "0")
        except Exception as e:
            logger.warning("Failed to parse row %s: %r", rows, e)
            continue
        total_uah += uah
        total_usdt += usdt
    logger.info(
        "aggregate_internal_p2p_from_csv: rows=%s total_uah=%s total_usdt=%s",
        rows, total_uah, total_usdt,
    )
    return total_uah, total_usdt, rows


def parse_extra_filters(tokens: List[str]) -> Dict[str, str]:
    res: Dict[str, str] = {}
    for t in tokens:
        if "=" not in t:
            continue
        k, v = t.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        if k == "status":
            v = v.lower()
        res[k] = v
    return res


def format_profit_message(
    from_date: datetime,
    to_date: datetime,
    bought_fiat: Decimal,
    bought_crypto: Decimal,
    rows_count: int,
    sold_fiat: Decimal,
    sold_crypto: Decimal,
    cancelled_orders: int,
    cancelled_usdt: Decimal,
) -> str:
    if bought_crypto == 0 or sold_crypto == 0:
        return (
            "Нет достаточных данных для расчёта.\n"
            f"BUY (internal CSV): строк={rows_count}, USDT={bought_crypto}, UAH={bought_fiat}\n"
            f"SELL (Binance): USDT={sold_crypto}, UAH={sold_fiat}\n"
            f"CANCELLED SELL orders: count={cancelled_orders}, USDT={cancelled_usdt}"
        )

    try:
        avg_buy_rate = bought_fiat / bought_crypto
    except Exception:
        avg_buy_rate = None

    try:
        avg_sell_rate = sold_fiat / sold_crypto
    except Exception:
        avg_sell_rate = None

    if not avg_buy_rate or not avg_sell_rate:
        return (
            "Не удалось корректно посчитать средние цены.\n"
            f"BUY: USDT={bought_crypto}, UAH={bought_fiat}\n"
            f"SELL: USDT={sold_crypto}, UAH={sold_fiat}\n"
            f"CANCELLED SELL orders: count={cancelled_orders}, USDT={cancelled_usdt}"
        )

    profit_rate = avg_sell_rate / avg_buy_rate
    # оставляю твою поправку -sold_crypto/1000, если она тебе нужна
    profit_amount = sold_crypto * avg_sell_rate / avg_buy_rate - sold_crypto - sold_crypto / 1000

    lines = [
        f"Период: {from_date.date()} – {to_date.date()}",
        "",
        f"BUY (internal-p2p CSV):",
        f"  строк: {rows_count}",
        f"  USDT:  {bought_crypto}",
        f"  UAH:   {bought_fiat}",
        f"  avg BUY rate:  {avg_buy_rate} UAH/USDT",
        "",
        f"SELL (Binance P2P):",
        f"  USDT:  {sold_crypto}",
        f"  UAH:   {sold_fiat}",
        f"  avg SELL rate: {avg_sell_rate} UAH/USDT",
        "",
        f"CANCELLED SELL orders (Binance P2P):",
        f"  count: {cancelled_orders}",
        f"  USDT:  {cancelled_usdt}",
        "",
        f"Profit rate:   {profit_rate}",
        f"Profit amount: {profit_amount} USDT",
    ]
    return "\n".join(lines)


def ensure_binance_config():
    if not BINANCE_API_KEY_SELL or not BINANCE_API_SECRET_SELL:
        raise RuntimeError(
            "Binance API ключи не настроены. "
            "Проверь BINANCE_API_KEY_SELL / BINANCE_API_SECRET_SELL."
        )


# ============ TELEGRAM HANDLERS ============

def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(
        "Привет! 👋\n"
        "Я бот для расчёта профита между BUY (internal-p2p CSV) и SELL (Binance P2P).\n\n"
        + HELP_TEXT
    )


def help_cmd(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(HELP_TEXT)


def login(update: Update, context: CallbackContext) -> None:
    """
    /login username password
    Авторизация в internal-p2p, сохранение session cookies в user_data.
    """
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Использование:\n"
            "  /login <username> <password>\n\n"
            "Пароль отправляется этому боту, так что запускать его стоит на своём сервере."
        )
        return

    username, password = args[0], args[1]
    login_url = get_internal_p2p_login_url()

    try:
        s = requests.Session()
        resp = s.post(
            login_url,
            data={"username": username, "password": password},
            timeout=30,
        )
        logger.info("Login response: status=%s text=%r", resp.status_code, resp.text[:200])
        if resp.status_code != 200:
            update.message.reply_text(
                f"Логин не удался: HTTP {resp.status_code}\n{resp.text[:300]}"
            )
            return

        context.user_data["p2p_cookies"] = s.cookies.get_dict()
        context.user_data["p2p_username"] = username
        update.message.reply_text(
            f"Успешный логин в internal-p2p как {username} ✅"
        )
    except Exception as e:
        logger.exception("Login error")
        update.message.reply_text(f"Ошибка при логине: {e}")


def profit(update: Update, context: CallbackContext) -> None:
    """
    Быстрый режим:
    /profit 2025-12-01 2025-12-07 bank=MONO status=completed
    """
    try:
        ensure_binance_config()
    except Exception as e:
        update.message.reply_text(str(e))
        return

    cookies = context.user_data.get("p2p_cookies")
    if not cookies:
        update.message.reply_text(
            "Нет авторизации для internal-p2p.\n"
            "Сначала сделай /login <username> <password>."
        )
        return

    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Нужно минимум два аргумента: from_date и to_date.\n\n" + HELP_TEXT
        )
        return

    from_str, to_str, *filter_tokens = args
    try:
        from_date = parse_date_str(from_str)
        to_date = parse_date_str(to_str)
    except Exception:
        update.message.reply_text(
            "Не удалось распарсить даты. Используй формат YYYY-MM-DD, например:\n"
            "/profit 2025-12-01 2025-12-07"
        )
        return

    if to_date < from_date:
        update.message.reply_text("to_date не может быть меньше from_date.")
        return

    extra_filters = parse_extra_filters(filter_tokens)
    start_dt, end_dt = from_date, to_date

    update.message.reply_text(
        f"Считаю профит за период {from_date.date()} – {to_date.date()}...\n"
        f"Фильтры для internal-p2p CSV: {extra_filters or 'нет'}"
    )

    try:
        csv_text = fetch_internal_p2p_csv(from_date, to_date, extra_filters, cookies)
        bought_fiat, bought_crypto, rows_count = aggregate_internal_p2p_from_csv(csv_text)

        if rows_count == 0:
            debug_params = build_internal_p2p_params(from_date, to_date, extra_filters)
            update.message.reply_text(f"DEBUG: CSV пустой. Параметры запроса: {debug_params}")

        sell_client = Client(BINANCE_API_KEY_SELL, BINANCE_API_SECRET_SELL)
        sell_trades = get_binance_sell_trades(sell_client, start_dt, end_dt)
        sold_fiat, sold_crypto = aggregate_trades_binance(sell_trades)
        cancelled_orders, cancelled_usdt = aggregate_cancelled_trades_binance(sell_trades)

    except Exception as e:
        logger.exception("Error during profit calculation")
        update.message.reply_text(f"Ошибка при расчёте: {e}")
        return

    msg = format_profit_message(
        from_date,
        to_date,
        bought_fiat,
        bought_crypto,
        rows_count,
        sold_fiat,
        sold_crypto,
        cancelled_orders,
        cancelled_usdt,
    )
    update.message.reply_text(msg)


# ---------- WIZARD /profitw ----------

def profitw_start(update: Update, context: CallbackContext) -> int:
    """
    /profitw — диалоговый режим.
    Шаг 1: спросить период.
    """
    try:
        ensure_binance_config()
    except Exception as e:
        update.message.reply_text(str(e))
        return ConversationHandler.END

    cookies = context.user_data.get("p2p_cookies")
    if not cookies:
        update.message.reply_text(
            "Нет авторизации для internal-p2p.\n"
            "Сначала сделай /login <username> <password>."
        )
        return ConversationHandler.END

    update.message.reply_text(
        "Введи период в формате:\n"
        "YYYY-MM-DD YYYY-MM-DD\n\n"
        "Например:\n"
        "2025-12-01 2025-12-07"
    )
    return STATE_PERIOD


def profitw_period(update: Update, context: CallbackContext) -> int:
    text = (update.message.text or "").strip()
    parts = text.split()
    if len(parts) != 2:
        update.message.reply_text(
            "Ожидалось две даты через пробел, например:\n"
            "2025-12-01 2025-12-07"
        )
        return STATE_PERIOD

    from_str, to_str = parts
    try:
        from_date = parse_date_str(from_str)
        to_date = parse_date_str(to_str)
    except Exception:
        update.message.reply_text(
            "Не удалось распарсить даты. Используй формат YYYY-MM-DD."
        )
        return STATE_PERIOD

    if to_date < from_date:
        update.message.reply_text("to_date не может быть меньше from_date.")
        return STATE_PERIOD

    context.user_data["profit_from_date"] = from_date
    context.user_data["profit_to_date"] = to_date
    context.user_data["profit_filters"] = {}

    update.message.reply_text(
        "Теперь укажи фильтры для internal-p2p CSV.\n\n"
        "Формат: key=value\n"
        "Можно по одному фильтру в сообщение, например:\n"
        "bank=MONO\n"
        "status=completed\n"
        "maker=1\n\n"
        "Когда закончишь — напиши /done"
    )
    return STATE_FILTERS


def profitw_add_filter(update: Update, context: CallbackContext) -> int:
    text = (update.message.text or "").strip()
    if not text:
        return STATE_FILTERS

    tokens = text.split()
    new_filters = parse_extra_filters(tokens)
    pf: Dict[str, str] = context.user_data.get("profit_filters", {})
    pf.update(new_filters)
    context.user_data["profit_filters"] = pf

    update.message.reply_text(
        f"Текущие фильтры: {pf}\n"
        "Добавь ещё фильтров или напиши /done для расчёта."
    )
    return STATE_FILTERS


def profitw_done(update: Update, context: CallbackContext) -> int:
    cookies = context.user_data.get("p2p_cookies")
    if not cookies:
        update.message.reply_text(
            "Нет авторизации для internal-p2p. Сначала /login."
        )
        return ConversationHandler.END

    from_date: datetime = context.user_data.get("profit_from_date")
    to_date: datetime = context.user_data.get("profit_to_date")
    extra_filters: Dict[str, str] = context.user_data.get("profit_filters", {})

    if not from_date or not to_date:
        update.message.reply_text("Период не задан. Начни заново: /profitw")
        return ConversationHandler.END

    start_dt, end_dt = from_date, to_date
    update.message.reply_text(
        f"Считаю профит за период {from_date.date()} – {to_date.date()}...\n"
        f"Фильтры: {extra_filters or 'нет'}"
    )

    try:
        csv_text = fetch_internal_p2p_csv(from_date, to_date, extra_filters, cookies)
        bought_fiat, bought_crypto, rows_count = aggregate_internal_p2p_from_csv(csv_text)

        if rows_count == 0:
            debug_params = build_internal_p2p_params(from_date, to_date, extra_filters)
            update.message.reply_text(f"DEBUG: CSV пустой. Параметры запроса: {debug_params}")

        sell_client = Client(BINANCE_API_KEY_SELL, BINANCE_API_SECRET_SELL)
        sell_trades = get_binance_sell_trades(sell_client, start_dt, end_dt)
        sold_fiat, sold_crypto = aggregate_trades_binance(sell_trades)
        cancelled_orders, cancelled_usdt = aggregate_cancelled_trades_binance(sell_trades)

    except Exception as e:
        logger.exception("Error during profitw calculation")
        update.message.reply_text(f"Ошибка при расчёте: {e}")
        return ConversationHandler.END

    msg = format_profit_message(
        from_date,
        to_date,
        bought_fiat,
        bought_crypto,
        rows_count,
        sold_fiat,
        sold_crypto,
        cancelled_orders,
        cancelled_usdt,
    )
    update.message.reply_text(msg)
    return ConversationHandler.END


def profitw_cancel(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Ок, отменено.")
    return ConversationHandler.END


# ============ MAIN ============

def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env var is not set")

    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CommandHandler("login", login))
    dp.add_handler(CommandHandler("profit", profit))

    conv = ConversationHandler(
        entry_points=[CommandHandler("profitw", profitw_start)],
        states={
            STATE_PERIOD: [MessageHandler(Filters.text & ~Filters.command, profitw_period)],
            STATE_FILTERS: [
                CommandHandler("done", profitw_done),
                MessageHandler(Filters.text & ~Filters.command, profitw_add_filter),
            ],
        },
        fallbacks=[CommandHandler("cancel", profitw_cancel)],
    )
    dp.add_handler(conv)

    logger.info("Bot starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
