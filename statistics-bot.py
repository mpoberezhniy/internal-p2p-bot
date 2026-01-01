"""
Telegram bot for generating detailed statistics about the internal P2P
exchange.

This bot supersedes the original ``profits-bot`` by collecting a
broader set of metrics from both the internal exchange and Binance
P2P.  In addition to computing the overall profit between BUY and
SELL trades, it aggregates statistics by arbitrary time intervals
(hourly, daily, weekly or monthly) and renders the results as
graphs in a PDF report.  The report includes:

* Number of orders created on the site (per interval).
* Number of Binance trades (per interval).
* Profit and profit rate (per interval).
* Number and volume of cancelled Binance trades (per interval).
* Net transaction flow on the site (credits minus debits, per interval).
* Summary of current user balances.

Usage:

1. Authenticate against the internal site via ``/login <username>
   <password>``.  This stores the session cookies in the user
   context.
2. Request statistics with ``/stats <from_date> <to_date> [interval]
   [pdf]``.  The optional ``interval`` argument can be one of
   ``hour``, ``day``, ``week`` or ``month`` (default: ``day``).  If
   ``pdf`` is supplied as the final argument the bot will return a
   PDF document containing the charts and tables; otherwise the
   computed statistics will be rendered as plain text messages.

Note that this bot relies on the same environment variables as the
original profits bot for configuration: ``BOT_TOKEN`` for the
Telegram API token, ``BINANCE_API_KEY_SELL``/``BINANCE_API_SECRET_SELL``
for accessing Binance P2P trade history, and ``BASE_URL`` and
``INTERNAL_P2P_LOGIN_URL`` for pointing at the internal P2P site.

The report generation uses ``matplotlib`` for charting and
``reportlab`` for PDF creation.  If these libraries are missing an
informative error message is sent to the user.
"""

import logging
import os
import io
import csv
from collections import defaultdict, OrderedDict, Counter
from datetime import datetime, date, timedelta
from decimal import Decimal, getcontext
from typing import Dict, Tuple, List, Optional, Callable

import requests

try:
    # matplotlib is used for chart creation.  We set the backend to
    # ``Agg`` so that charts can be generated in headless environments.
    import matplotlib
    matplotlib.use("Agg")  # type: ignore
    import matplotlib.pyplot as plt  # type: ignore
    # Additional import for date formatting; available only if matplotlib is present
    import matplotlib.dates as mdates  # type: ignore
except Exception:
    # In environments without matplotlib the plotting functions will not work
    plt = None  # type: ignore
    mdates = None  # type: ignore

try:
    # reportlab is used for PDF creation.  It may not be available
    # in all environments.  We detect its presence at runtime.
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas as pdf_canvas  # type: ignore
    from reportlab.lib.utils import ImageReader  # type: ignore
except Exception:
    pdf_canvas = None  # type: ignore
    ImageReader = None  # type: ignore

from binance.client import Client  # type: ignore
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

# Internal-p2p BASE / LOGIN
INTERNAL_P2P_BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
INTERNAL_P2P_LOGIN_URL = os.environ.get("INTERNAL_P2P_LOGIN_URL", "").strip()

FIAT = os.environ.get("FIAT", "UAH")
ASSET = os.environ.get("ASSET", "USDT")

# ============ TIME INTERVAL HELPERS ============

def floor_datetime(dt: datetime, interval: str) -> datetime:
    """Round a datetime down to the start of the given interval.

    Supported intervals: 'hour', 'day', 'week', 'month'.  The
    returned datetime is naive (no timezone) and aligned to the
    beginning of the interval.
    """
    if interval == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif interval == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == "week":
        # ISO week starts on Monday; subtract weekday() days
        start = dt - timedelta(days=dt.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unsupported interval: {interval}")


def parse_date_str(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


# ============ FETCH FUNCTIONS ============

def get_internal_p2p_login_url() -> str:
    if INTERNAL_P2P_LOGIN_URL:
        return INTERNAL_P2P_LOGIN_URL
    if not INTERNAL_P2P_BASE_URL:
        raise RuntimeError("Neither INTERNAL_P2P_LOGIN_URL nor BASE_URL is set")
    return f"{INTERNAL_P2P_BASE_URL.rstrip('/')}/api/login/"


def get_internal_p2p_csv_url() -> str:
    if not INTERNAL_P2P_BASE_URL:
        raise RuntimeError("BASE_URL is not set")
    return f"{INTERNAL_P2P_BASE_URL.rstrip('/')}/orders/export.csv"


def get_internal_api_url(path: str) -> str:
    if not INTERNAL_P2P_BASE_URL:
        raise RuntimeError("BASE_URL is not set")
    return f"{INTERNAL_P2P_BASE_URL.rstrip('/')}/api/{path.lstrip('/')}"


def fetch_internal_p2p_csv(
    from_date: datetime,
    to_date: datetime,
    extra_filters: Dict[str, str],
    cookies: Optional[Dict[str, str]],
) -> str:
    """
    Download the orders CSV from the internal P2P site within the
    specified date range.  Extra filters are passed through as
    query‑parameters.  An active session cookie is required.

    This function remains for backwards compatibility with the
    original profits bot, but the statistics bot no longer relies on
    CSV exports.  Instead, aggregated statistics are fetched via the
    ``/api/statistics/`` endpoint (see ``fetch_statistics_report``).
    """
    url = get_internal_p2p_csv_url()
    params = {
        "created_from": from_date.date().isoformat(),
        "created_to": to_date.date().isoformat(),
    }
    for k, v in extra_filters.items():
        params[k] = v
    logger.info(f"Fetching CSV from {url=} {params=}")
    resp = requests.get(url, params=params, cookies=cookies, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(
            f"internal-p2p CSV error: {resp.status_code} {resp.text[:300]}"
        )
    return resp.text


def fetch_internal_transactions(
    from_date: datetime,
    to_date: datetime,
    cookies: Optional[Dict[str, str]],
) -> List[dict]:
    """
    Fetch transactions from the internal API within the specified date
    range.  Returns a list of transaction dictionaries.  If the
    endpoint is not available or returns an error, an empty list is
    returned.  Transactions include ``created_at``, ``amount``,
    ``kind`` and ``currency``.
    """
    url = get_internal_api_url("transactions/")
    params = {
        "created_from": from_date.isoformat(),
        "created_to": to_date.isoformat(),
    }
    try:
        resp = requests.get(url, params=params, cookies=cookies, timeout=60)
        if resp.status_code != 200:
            logger.warning(
                "internal transactions API returned %s: %s",
                resp.status_code,
                resp.text[:300],
            )
            return []
        data = resp.json()
        # API returns list of transaction objects; we trust its structure
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Error fetching transactions: %s", e)
        return []


def fetch_internal_balances(cookies: Optional[Dict[str, str]]) -> List[dict]:
    """
    Fetch all user balances via the internal API.  Returns a list
    containing dictionaries with ``user``, ``currency`` and ``amount``.
    If the API is unavailable an empty list is returned.
    """
    url = get_internal_api_url("balances/")
    try:
        resp = requests.get(url, cookies=cookies, timeout=60)
        if resp.status_code != 200:
            logger.warning(
                "internal balances API returned %s: %s",
                resp.status_code,
                resp.text[:200],
            )
            return []
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Error fetching balances: %s", e)
        return []


# ============ NEW AGGREGATED STATISTICS ============

def fetch_statistics_report(
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    cookies: Optional[Dict[str, str]],
) -> List[dict]:
    """
    Fetch aggregated statistics from the internal P2P API.

    The internal API provides a ``/api/statistics/`` endpoint which
    aggregates minute‑level statistics (``StatisticsMinute`` rows)
    across arbitrary intervals.  This function calls that endpoint
    with the given start/end datetimes and interval and returns the
    list of period buckets.  Each element in the returned list is a
    dictionary with a ``period_start`` ISO timestamp and aggregated
    metrics such as ``maker_updates``, ``taker_updates``, ``orders_count``,
    ``maker_ads_count``, ``taker_ads_count``, ``withdraw_onchain_count``
    and so on.  See ``exchange/api_views.statistics_report`` for the
    exact fields returned.

    If the request fails or returns a non‑200 status code a
    ``RuntimeError`` is raised.
    """
    url = get_internal_api_url("statistics/")
    params = {
        "start": from_dt.isoformat(),
        "end": to_dt.isoformat(),
        "interval": interval,
    }
    logger.info(f"Fetching statistics report from {url=} {params=}")
    try:
        resp = requests.get(url, params=params, cookies=cookies, timeout=60)
        if resp.status_code != 200:
            logger.warning(
                "internal statistics API returned %s: %s",
                resp.status_code,
                resp.text[:300],
            )
            raise RuntimeError(
                f"statistics API error: {resp.status_code} {resp.text[:200]}"
            )
        data = resp.json().get("data", [])
        # ensure we always return a list
        if not isinstance(data, list):
            return []
        return data
    except Exception as e:
        logger.exception("Error fetching statistics report")
        raise RuntimeError(f"Error fetching statistics report: {e}")


# === Binance withdrawals history ===
def fetch_binance_withdrawals_history(
    from_date: datetime,
    to_date: datetime,
    cookies: Optional[Dict[str, str]],
    offset: int = 0,
    limit: int = 100000,
) -> List[dict]:
    """
    Fetch detailed Binance withdrawals history for the authenticated user via the
    internal API endpoint ``binance_withdrawals_history``.  The date range
    specified by ``from_date`` and ``to_date`` is used to filter results on
    the server side.  Results are paginated using ``offset`` and ``limit``.

    The returned list contains withdrawal dictionaries with at least the
    fields ``timestamp`` (ISO datetime string), ``amount`` (numeric) and a
    recipient/address identifier (recipient, address, toAddress or similar).

    If the endpoint returns an unexpected response or an error occurs a
    ``RuntimeError`` is raised.
    """
    url = get_internal_api_url("binance_withdrawals_history")
    params = {
        "start": from_date.isoformat(),
        "end": to_date.isoformat(),
        "offset": offset,
        "limit": limit,
    }
    try:
        resp = requests.get(url, params=params, cookies=cookies, timeout=60)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch withdrawals history: {e}")
    if resp.status_code != 200:
        raise RuntimeError(
            f"binance_withdrawals_history error: HTTP {resp.status_code} {resp.text[:200]}"
        )
    try:
        json_data = resp.json()
    except Exception:
        json_data = None
    if not json_data:
        return []
    if isinstance(json_data, list):
        records = json_data
    elif isinstance(json_data, dict):
        records = (
            json_data.get("data")
            or json_data.get("withdrawals")
            or json_data.get("result")
            or []
        )
        if not isinstance(records, list):
            records = []
    else:
        records = []
    return records


def create_statistics_report_pdf(
    data: List[dict],
    balances: List[dict],
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    withdrawals: Optional[List[dict]] = None,
) -> bytes:
    """
    Generate a PDF report containing charts and tables based on
    aggregated statistics returned by ``fetch_statistics_report``.

    The report includes the following visualisations:
      * Line chart of orders count per interval.
      * Line chart of volume bought and sold (USDT) per interval.
      * Line chart of average profit in USDT per interval.
      * Line chart of average profit rate per interval.
      * Line chart of cancelled trade counts per interval.
      * Line chart of net transaction flow per interval.
      * Line chart of total balances (internal vs Binance) per interval.
      * Multi‑series line chart of maker vs taker ads count per interval.
      * Multi‑series line chart of maker vs taker updates per interval.
      * Multi‑series line chart of on‑chain vs off‑chain withdrawal counts per interval.
      * Multi‑series line chart of on‑chain vs off‑chain withdrawal amounts per interval.
      * Pie chart of total ads count by role (maker vs taker).
      * Pie chart of total withdrawal volume by type (on‑chain vs off‑chain).

    A summary section on the first page lists aggregated totals for
    each metric across the entire period and the top 10 user
    balances in the specified asset.

    Returns a byte string containing the generated PDF.  Requires
    ``matplotlib`` and ``reportlab`` to be installed.
    """
    # Ensure dependencies are available
    if plt is None or pdf_canvas is None or ImageReader is None:
        raise RuntimeError(
            "Missing dependencies: matplotlib and reportlab are required to generate PDF reports."
        )
    # Parse data into time‑series
    x_vals: List[datetime] = []
    orders: List[int] = []
    bought_usdt: List[float] = []
    sold_usdt: List[float] = []
    profit_usdt: List[float] = []
    profit_rate: List[float] = []
    cancelled: List[int] = []
    net_tx_flow: List[float] = []
    maker_ads: List[int] = []
    taker_ads: List[int] = []
    maker_updates: List[int] = []
    taker_updates: List[int] = []
    withdraw_on_count: List[int] = []
    withdraw_off_count: List[int] = []
    withdraw_on_usdt: List[float] = []
    withdraw_off_usdt: List[float] = []
    binance_total_balance: List[float] = []
    internal_total_balance: List[float] = []
    # Aggregated sums for pie charts and summary
    total_orders = 0
    total_maker_ads = 0
    total_taker_ads = 0
    total_maker_updates = 0
    total_taker_updates = 0
    total_withdraw_on_count = 0
    total_withdraw_off_count = 0
    total_withdraw_on_usdt = 0.0
    total_withdraw_off_usdt = 0.0
    total_cancelled = 0
    total_profit_usdt = 0.0
    total_profit_amount = 0.0
    profit_amounts: List[float] = []
    total_profit_rate_sum = 0.0
    total_profit_rate_count = 0
    total_bought_usdt = 0.0
    total_sold_usdt = 0.0
    total_net_tx_flow = 0.0
    # Convert records to lists
    for rec in sorted(data, key=lambda r: r.get("period_start", "")):
        ps = rec.get("period_start")
        try:
            dt = datetime.fromisoformat(ps)
        except Exception:
            continue
        x_vals.append(dt)
        oc = int(rec.get("orders_count", 0) or 0)
        orders.append(oc)
        total_orders += oc
        # bought/sold volumes
        bu = float(rec.get("bought_usdt", 0) or 0)
        su = float(rec.get("sold_usdt", 0) or 0)
        bought_usdt.append(bu)
        sold_usdt.append(su)
        total_bought_usdt += bu
        total_sold_usdt += su
        # profit averages (may be None)
        p_usdt = rec.get("profit_usdt_avg")
        p_rate = rec.get("profit_rate_avg")
        if p_usdt is None:
            profit_usdt.append(0.0)
        else:
            profit_usdt.append(float(p_usdt))
            total_profit_usdt += float(p_usdt)
        if p_rate is None:
            profit_rate.append(0.0)
        else:
            profit_rate.append(float(p_rate))
            total_profit_rate_sum += float(p_rate)
            total_profit_rate_count += 1
        # cancelled
        cc = int(rec.get("cancelled_count", 0) or 0)
        cancelled.append(cc)
        total_cancelled += cc
        # net transaction flow
        ntf = float(rec.get("net_tx_flow", 0) or 0)
        net_tx_flow.append(ntf)
        total_net_tx_flow += ntf
        # ads counts
        ma = int(rec.get("maker_ads_count", 0) or 0)
        ta = int(rec.get("taker_ads_count", 0) or 0)
        maker_ads.append(ma)
        taker_ads.append(ta)
        total_maker_ads += ma
        total_taker_ads += ta
        # updates
        mu = int(rec.get("maker_updates", 0) or 0)
        tu = int(rec.get("taker_updates", 0) or 0)
        maker_updates.append(mu)
        taker_updates.append(tu)
        total_maker_updates += mu
        total_taker_updates += tu
        # withdraw counts and amounts
        woc = int(rec.get("withdraw_onchain_count", 0) or 0)
        wfc = int(rec.get("withdraw_offchain_count", 0) or 0)
        withdraw_on_count.append(woc)
        withdraw_off_count.append(wfc)
        total_withdraw_on_count += woc
        total_withdraw_off_count += wfc
        wou = float(rec.get("withdraw_onchain_usdt", 0) or 0)
        wfu = float(rec.get("withdraw_offchain_usdt", 0) or 0)
        withdraw_on_usdt.append(wou)
        withdraw_off_usdt.append(wfu)
        total_withdraw_on_usdt += wou
        total_withdraw_off_usdt += wfu
        # balances
        btb = float(rec.get("binance_total_balance_usdt", 0) or 0)
        itb = float(rec.get("total_balance_usdt", 0) or 0)
        binance_total_balance.append(btb)
        internal_total_balance.append(itb)
        # Compute the profit amount for this interval using aggregated
        # buy/sell volumes.  We use Decimals to avoid precision loss.
        # The formula mirrors the profit calculation from the original
        # profits bot: profit = sold_crypto * (avg_sell_rate / avg_buy_rate)
        #           - sold_crypto - sold_crypto * 0.1% (fee).  If there
        # were no buys or sells in the bucket, profit is treated as zero.
        try:
            from decimal import Decimal
            bc_val = rec.get("bought_usdt", 0) or 0
            sc_val = rec.get("sold_usdt", 0) or 0
            bf_val = rec.get("bought_uah", 0) or 0
            sf_val = rec.get("sold_uah", 0) or 0
            bc = Decimal(str(bc_val))
            sc = Decimal(str(sc_val))
            bf = Decimal(str(bf_val))
            sf = Decimal(str(sf_val))
            profit_amount_this = 0.0
            if bc != 0 and sc != 0:
                avg_buy_rate = bf / bc
                avg_sell_rate = sf / sc
                pa = sc * avg_sell_rate / avg_buy_rate - sc - sc / Decimal("1000")
                profit_amount_this = float(pa)
                total_profit_amount += profit_amount_this
            profit_amounts.append(profit_amount_this)
        except Exception:
            # On error, assume zero profit for this interval
            profit_amounts.append(0.0)
    # Summary profit rate average
    avg_profit_rate = (total_profit_rate_sum / total_profit_rate_count) if total_profit_rate_count else None
    # Build images
    images: List[Tuple[str, bytes]] = []
    # Helper functions
    def _format_axis(ax):
        """
        Format the x‑axis based on the selected interval.  This helper sets
        an appropriate locator and formatter so that hours, days, weeks or
        months are displayed on the axis.  It is used by all line and
        multi‑line plots.
        """
        # Choose locator/formatter based on interval
        if interval == "hour":
            locator = mdates.HourLocator()
            formatter = mdates.DateFormatter("%H:%M")
        elif interval == "day":
            locator = mdates.DayLocator()
            formatter = mdates.DateFormatter("%d %b")
        elif interval == "week":
            locator = mdates.WeekdayLocator()
            formatter = mdates.DateFormatter("%Y-W%U")
        elif interval == "month":
            locator = mdates.MonthLocator()
            formatter = mdates.DateFormatter("%Y-%m")
        else:
            # Fallback to automatic locator/formatter
            locator = mdates.AutoDateLocator()
            formatter = mdates.AutoDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

    def plot_line(title: str, y: List[float], ylabel: str, clip_negative: bool = True) -> bytes:
        """
        Plot a single series as a line chart.  Optionally clip negative
        values to zero and fix the lower y‑axis bound at 0 to prevent
        negative tick labels.  The x‑axis is formatted based on the
        reporting interval.
        """
        # Convert to floats and clip negative values if requested
        if clip_negative:
            y_data = [max(0.0, float(v)) for v in y]
        else:
            y_data = [float(v) for v in y]
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(x_vals, y_data, marker="o")
        ax.set_title(title)
        ax.set_xlabel(f"Time ({interval})")
        ax.set_ylabel(ylabel)
        _format_axis(ax)
        if clip_negative:
            ax.set_ylim(bottom=0)
        fig.tight_layout()
        fig.autofmt_xdate()
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()

    def plot_multi_line(title: str, series: List[Tuple[str, List[float]]], ylabel: str, clip_negative: bool = True) -> bytes:
        """
        Plot multiple series on a single set of axes.  Negative values can
        be clipped to zero across all series.  The x‑axis is formatted
        based on the reporting interval.
        """
        # Prepare data, clipping negatives if requested
        series_data = []
        for label, values in series:
            if clip_negative:
                cleaned = [max(0.0, float(v)) for v in values]
            else:
                cleaned = [float(v) for v in values]
            series_data.append((label, cleaned))
        fig, ax = plt.subplots(figsize=(8, 4))
        for label, values in series_data:
            ax.plot(x_vals, values, marker="o", label=label)
        ax.set_title(title)
        ax.set_xlabel(f"Time ({interval})")
        ax.set_ylabel(ylabel)
        ax.legend()
        _format_axis(ax)
        if clip_negative:
            ax.set_ylim(bottom=0)
        fig.tight_layout()
        fig.autofmt_xdate()
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()

    def plot_pie(title: str, labels: List[str], sizes: List[float]) -> bytes:
        """
        Plot a pie chart without specifying explicit colours.  The chart
        automatically handles percentage labelling and tight layout.
        """
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.pie(sizes, labels=labels, autopct='%1.1f%%')
        ax.set_title(title)
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()
    # Charts
    images.append((f"Orders per {interval}", plot_line(
        f"Orders per {interval}", [float(v) for v in orders], "Count"
    )))
    # Volume charts
    images.append((f"USDT volumes per {interval}", plot_multi_line(
        f"USDT volumes per {interval}",
        [("Bought USDT", [float(v) for v in bought_usdt]), ("Sold USDT", [float(v) for v in sold_usdt])],
        f"Volume ({ASSET})",
    )))
    # Profit per interval (actual profit amounts)
    images.append((f"Profit per {interval}", plot_line(
        f"Profit per {interval}", [float(v) for v in profit_amounts], f"Profit ({ASSET})"
    )))
    # Profit rate average
    images.append((f"Average profit rate per {interval}", plot_line(
        f"Average profit rate per {interval}", [float(v) for v in profit_rate], "Rate"
    )))
    # Cancelled trades
    images.append((f"Cancelled trades per {interval}", plot_line(
        f"Cancelled trades per {interval}", [float(v) for v in cancelled], "Count"
    )))
    # Net transaction flow
    images.append((f"Net transaction flow per {interval}", plot_line(
        f"Net transaction flow per {interval}", net_tx_flow, f"Net flow ({ASSET})"
    )))
    # Balances comparison
    # Show only the Binance balance on the balances chart (internal balance is omitted)
    images.append((f"Binance balances per {interval}", plot_line(
        f"Binance balances per {interval}", [float(v) for v in binance_total_balance], f"Balance ({ASSET})"
    )))
    # Ads counts comparison
    images.append((f"Ads count per {interval}", plot_multi_line(
        f"Ads count per {interval}",
        [("Maker ads", [float(v) for v in maker_ads]), ("Taker ads", [float(v) for v in taker_ads])],
        "Count",
    )))
    # Updates comparison
    images.append((f"Updates per {interval}", plot_multi_line(
        f"Updates per {interval}",
        [("Maker updates", [float(v) for v in maker_updates]), ("Taker updates", [float(v) for v in taker_updates])],
        "Count",
    )))
    # Withdraw counts
    images.append((f"Withdraw counts per {interval}", plot_multi_line(
        f"Withdraw counts per {interval}",
        [("On‑chain", [float(v) for v in withdraw_on_count]), ("Off‑chain", [float(v) for v in withdraw_off_count])],
        "Count",
    )))
    # Withdraw amounts
    images.append((f"Withdraw amounts per {interval}", plot_multi_line(
        f"Withdraw amounts per {interval}",
        [("On‑chain USDT", withdraw_on_usdt), ("Off‑chain USDT", withdraw_off_usdt)],
        f"Amount ({ASSET})",
    )))
    # Pie charts: ads distribution
    if total_maker_ads + total_taker_ads > 0:
        images.append((
            "Ads distribution (maker vs taker)",
            plot_pie(
                "Ads distribution (maker vs taker)",
                ["Maker", "Taker"],
                [total_maker_ads, total_taker_ads],
            ),
        ))
    if total_withdraw_on_usdt + total_withdraw_off_usdt > 0:
        images.append((
            "Withdrawal volume distribution",
            plot_pie(
                "Withdrawal volume distribution",
                ["On‑chain", "Off‑chain"],
                [total_withdraw_on_usdt, total_withdraw_off_usdt],
            ),
        ))

    # If we have detailed withdrawal events, add pie charts by recipient
    if withdrawals:
        try:
            # Aggregate counts and sums by recipient/address/identifier
            counts: Counter = Counter()
            sums: Counter = Counter()
            for w in withdrawals:
                # Determine a recipient identifier; fall back to address or transaction id
                recipient = (
                    w.get("recipient")
                    or w.get("address")
                    or w.get("to")
                    or w.get("toAddress")
                    or w.get("uid")
                    or w.get("user")
                    or w.get("txId")
                    or w.get("withdrawOrderId")
                    or "Unknown"
                )
                recipient = str(recipient)
                counts[recipient] += 1
                try:
                    amt = float(w.get("amount") or 0)
                except Exception:
                    amt = 0.0
                sums[recipient] += amt
            # Helper to get top values and group others
            def _top(counter: Counter) -> Tuple[List[str], List[float]]:
                items = list(counter.items())
                items.sort(key=lambda x: x[1], reverse=True)
                top_n = items[:10]
                other_sum = sum(v for _, v in items[10:])
                lbls = [r for r, _ in top_n]
                vals = [v for _, v in top_n]
                if other_sum > 0:
                    lbls.append("Other")
                    vals.append(other_sum)
                return lbls, vals
            count_labels, count_vals = _top(counts)
            sum_labels, sum_vals = _top(sums)
            if count_vals:
                images.append((
                    "Withdrawal count by recipient",
                    plot_pie("Withdrawal count by recipient", count_labels, count_vals),
                ))
            if sum_vals:
                images.append((
                    "Withdrawal sum by recipient",
                    plot_pie("Withdrawal sum by recipient", sum_labels, sum_vals),
                ))
        except Exception:
            # In case of any errors computing charts, silently skip
            pass
    # Create PDF
    buf = io.BytesIO()
    c = pdf_canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, height - 40, "Statistics Report")
    c.setFont("Helvetica", 10)
    c.drawString(40, height - 60, f"Period: {from_dt.date()} – {to_dt.date()} (interval: {interval})")
    y_pos = height - 85
    # Summary section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_pos, "Summary Totals")
    y_pos -= 18
    c.setFont("Helvetica", 9)
    # Prepare summary lines.  ``total_profit_usdt`` is the sum of
    # average profits across intervals (legacy behaviour).  ``total_profit_amount``
    # is the actual total profit computed from aggregated buy/sell volumes.
    summary_lines = [
        f"Total orders: {total_orders}",
        f"Total maker ads: {total_maker_ads}",
        f"Total taker ads: {total_taker_ads}",
        f"Total maker updates: {total_maker_updates}",
        f"Total taker updates: {total_taker_updates}",
        f"Total bought USDT: {total_bought_usdt:.4f}",
        f"Total sold USDT: {total_sold_usdt:.4f}",
        f"Total cancelled trades: {total_cancelled}",
        f"Total net transaction flow: {total_net_tx_flow:.4f}",
        f"Total withdraw on‑chain count: {total_withdraw_on_count}",
        f"Total withdraw off‑chain count: {total_withdraw_off_count}",
        f"Total withdraw on‑chain USDT: {total_withdraw_on_usdt:.4f}",
        f"Total withdraw off‑chain USDT: {total_withdraw_off_usdt:.4f}",
        f"Total profit: {total_profit_amount:.4f}",
        f"Average profit rate: {avg_profit_rate:.4f}" if avg_profit_rate is not None else "Average profit rate: N/A",
    ]
    for line in summary_lines:
        c.drawString(50, y_pos, line)
        y_pos -= 12
        if y_pos < 150:
            c.showPage()
            y_pos = height - 40
            c.setFont("Helvetica", 9)
    # The list of current balances has been removed from the report.  To keep
    # the first page focused on aggregated statistics, the balances summary
    # is omitted entirely.  If a balances overview is needed, it can be
    # generated separately by the bot.
    # Now draw charts
    for title, img_data in images:
        c.showPage()
        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, height - 40, title)
        image = ImageReader(io.BytesIO(img_data))
        iw, ih = image.getSize()
        max_width = width - 80
        max_height = height - 100
        scale = min(max_width / iw, max_height / ih)
        draw_w, draw_h = iw * scale, ih * scale
        c.drawImage(
            image,
            40,
            height - 60 - draw_h,
            width=draw_w,
            height=draw_h,
        )

    # If withdrawal events exist, append a table of up to 60 entries
    if withdrawals:
        # Sort by timestamp ascending
        try:
            entries = sorted(withdrawals, key=lambda w: w.get("timestamp") or "")
        except Exception:
            entries = withdrawals[:]
        # Limit to first 60 entries
        entries = entries[:60]
        # Iterate pages and draw table
        idx = 0
        while idx < len(entries):
            c.showPage()
            c.setFont("Helvetica-Bold", 12)
            c.drawString(40, height - 40, "Withdrawal events")
            c.setFont("Helvetica", 9)
            y = height - 60
            # Table header
            c.drawString(40, y, "Timestamp")
            c.drawString(180, y, "Amount")
            c.drawString(260, y, "Recipient")
            y -= 14
            for _ in range(40):  # max rows per page
                if idx >= len(entries) or y < 50:
                    break
                row = entries[idx]
                ts = row.get("timestamp") or ""
                amt = row.get("amount") or ""
                recp = row.get("recipient") or row.get("address") or row.get("to") or row.get("toAddress") or row.get("uid") or row.get("user") or row.get("txId") or row.get("withdrawOrderId") or ""
                c.drawString(40, y, str(ts)[:19])
                c.drawString(180, y, str(amt))
                # Trim recipient if too long
                recp_str = str(recp)
                if len(recp_str) > 32:
                    recp_str = recp_str[:29] + "..."
                c.drawString(260, y, recp_str)
                y -= 12
                idx += 1
        # End of table
    c.save()
    return buf.getvalue()


def get_binance_sell_trades(
    client: Client, start_dt: datetime, end_dt: datetime
) -> List[dict]:
    """Return a list of Binance P2P SELL trades within the given time range."""
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


def aggregate_trades_binance(trades: List[dict], interval: str) -> Tuple[Dict[datetime, Decimal], Dict[datetime, Decimal], Dict[datetime, int]]:
    """
    Aggregate Binance trades into per‑interval totals.

    Returns three dictionaries keyed by the floored datetime:
      - sold_fiat_totals: Sum of fiat (UAH) per interval for completed orders.
      - sold_crypto_totals: Sum of crypto (USDT) per interval for completed orders.
      - cancelled_counts: Count of cancelled orders per interval.

    Only trades matching the global ``ASSET`` and ``FIAT`` and with
    ``orderStatus`` == COMPLETED are included in the sold totals.  Trades
    with orderStatus in the cancellation set increment the cancelled
    counter instead.
    """
    sold_fiat_totals: Dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
    sold_crypto_totals: Dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
    cancelled_counts: Dict[datetime, int] = defaultdict(int)
    cancelled_statuses = {
        "CANCELLED",
        "CANCELED",
        "CANCEL",
        "CANCELLED_BY_SYSTEM",
        "AUTO_CANCELLED",
    }
    for t in trades:
        if t.get("asset") != ASSET:
            continue
        if t.get("fiat") != FIAT:
            continue
        # Use createTime or createTimestamp for grouping
        ts = t.get("createTime") or t.get("createTimestamp") or t.get("orderTime")
        try:
            dt = datetime.fromtimestamp(int(ts) / 1000.0)
        except Exception:
            continue
        bucket = floor_datetime(dt, interval)
        status = (t.get("orderStatus") or "").upper()
        if status in cancelled_statuses:
            cancelled_counts[bucket] += 1
            continue
        if status != "COMPLETED" and status != "FINISHED" and status != "SUCCESS":
            continue
        try:
            amount = Decimal(str(t.get("amount", "0")))
            total_price = Decimal(str(t.get("totalPrice", "0")))
        except Exception:
            continue
        sold_fiat_totals[bucket] += total_price
        sold_crypto_totals[bucket] += amount
    return sold_fiat_totals, sold_crypto_totals, cancelled_counts


def aggregate_internal_orders_csv(
    csv_text: str, interval: str
) -> Tuple[Dict[datetime, Decimal], Dict[datetime, Decimal], Dict[datetime, int]]:
    """
    Aggregate internal orders from CSV into per‑interval totals.

    Returns three dictionaries keyed by the floored datetime:
      - bought_fiat_totals: Sum of UAH per interval.
      - bought_crypto_totals: Sum of USDT per interval.
      - order_counts: Number of orders per interval.

    The CSV is expected to have columns: id, maker, taker, bank, uah, usdt,
    status, created.  The created column is parsed to a datetime and
    bucketed according to ``interval``.  Orders with invalid dates
    or amounts are skipped.
    """
    bought_fiat_totals: Dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
    bought_crypto_totals: Dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
    order_counts: Dict[datetime, int] = defaultdict(int)
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    for row in reader:
        try:
            created_str = row.get("created") or ""
            created_dt = datetime.fromisoformat(created_str)
            bucket = floor_datetime(created_dt, interval)
        except Exception:
            continue
        try:
            uah = Decimal(row.get("uah") or "0")
            usdt = Decimal(row.get("usdt") or "0")
        except Exception:
            continue
        bought_fiat_totals[bucket] += uah
        bought_crypto_totals[bucket] += usdt
        order_counts[bucket] += 1
    return bought_fiat_totals, bought_crypto_totals, order_counts


def aggregate_transactions(
    transactions: List[dict], interval: str
) -> Dict[datetime, Decimal]:
    """
    Aggregate transaction flows into per‑interval net amounts.

    Each transaction dictionary is expected to have ``created_at`` (an
    ISO8601 timestamp), ``amount`` (string or number), ``kind`` (credit
    or debit) and ``currency``.  Only transactions matching the global
    ``ASSET`` currency are considered.  Credits add to the total and
    debits subtract from it.  Returns a mapping from the floored
    datetime to the net amount for that interval.
    """
    flows: Dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
    for tr in transactions:
        if tr.get("currency") != ASSET:
            continue
        ts = tr.get("created_at") or tr.get("created") or ""
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            continue
        bucket = floor_datetime(dt, interval)
        try:
            amount = Decimal(str(tr.get("amount", "0")))
        except Exception:
            continue
        kind = (tr.get("kind") or "").lower()
        if kind == "debit":
            amount = -amount
        flows[bucket] += amount
    return flows


# ============ STATISTICS COMPUTATION ============

class IntervalStats:
    """
    Container for all statistics aggregated by interval.  It holds
    ordered dictionaries keyed by a datetime representing the start of
    each bucket.  Missing intervals are filled with zeros to allow
    consistent plotting.
    """

    def __init__(self, buckets: List[datetime]):
        self.buckets = buckets
        # Bought totals (internal) per interval
        self.bought_fiat = OrderedDict((b, Decimal("0")) for b in buckets)
        self.bought_crypto = OrderedDict((b, Decimal("0")) for b in buckets)
        self.order_counts = OrderedDict((b, 0) for b in buckets)
        # Sold totals (Binance) per interval
        self.sold_fiat = OrderedDict((b, Decimal("0")) for b in buckets)
        self.sold_crypto = OrderedDict((b, Decimal("0")) for b in buckets)
        self.cancelled_counts = OrderedDict((b, 0) for b in buckets)
        # Transaction flows per interval
        self.tx_flows = OrderedDict((b, Decimal("0")) for b in buckets)

    def populate(
        self,
        bought_fiat: Dict[datetime, Decimal],
        bought_crypto: Dict[datetime, Decimal],
        order_counts: Dict[datetime, int],
        sold_fiat: Dict[datetime, Decimal],
        sold_crypto: Dict[datetime, Decimal],
        cancelled_counts: Dict[datetime, int],
        tx_flows: Dict[datetime, Decimal],
    ) -> None:
        """Populate the internal dictionaries from supplied aggregates."""
        for b in self.buckets:
            if b in bought_fiat:
                self.bought_fiat[b] = bought_fiat[b]
            if b in bought_crypto:
                self.bought_crypto[b] = bought_crypto[b]
            if b in order_counts:
                self.order_counts[b] = order_counts[b]
            if b in sold_fiat:
                self.sold_fiat[b] = sold_fiat[b]
            if b in sold_crypto:
                self.sold_crypto[b] = sold_crypto[b]
            if b in cancelled_counts:
                self.cancelled_counts[b] = cancelled_counts[b]
            if b in tx_flows:
                self.tx_flows[b] = tx_flows[b]

    def profit_rate(self, b: datetime) -> Optional[Decimal]:
        """Compute the profit rate for a bucket if possible."""
        bc = self.bought_crypto[b]
        sf = self.sold_fiat[b]
        sc = self.sold_crypto[b]
        bf = self.bought_fiat[b]
        if bc == 0 or sc == 0:
            return None
        try:
            avg_buy_rate = bf / bc
            avg_sell_rate = sf / sc
            return avg_sell_rate / avg_buy_rate
        except Exception:
            return None

    def profit_amount(self, b: datetime) -> Optional[Decimal]:
        """Compute the profit amount (in USDT) for a bucket."""
        bc = self.bought_crypto[b]
        sc = self.sold_crypto[b]
        bf = self.bought_fiat[b]
        sf = self.sold_fiat[b]
        if bc == 0 or sc == 0:
            return None
        try:
            avg_buy_rate = bf / bc
            avg_sell_rate = sf / sc
            # subtract sold_crypto and the small 0.1% fee (sc/1000) as in profit bot
            return sc * avg_sell_rate / avg_buy_rate - sc - sc / Decimal("1000")
        except Exception:
            return None


def generate_buckets(from_dt: datetime, to_dt: datetime, interval: str) -> List[datetime]:
    """Generate a list of bucket start datetimes from from_dt to to_dt inclusive."""
    buckets = []
    current = floor_datetime(from_dt, interval)
    end = floor_datetime(to_dt, interval)
    delta: Callable[[datetime], datetime]
    if interval == "hour":
        step = timedelta(hours=1)
    elif interval == "day":
        step = timedelta(days=1)
    elif interval == "week":
        step = timedelta(weeks=1)
    elif interval == "month":
        # step: add one month by moving to the first of next month
        def add_month(d: datetime) -> datetime:
            year, month = d.year, d.month
            if month == 12:
                return d.replace(year=year + 1, month=1, day=1)
            else:
                return d.replace(month=month + 1, day=1)
        delta = add_month  # type: ignore
        step = None  # type: ignore
    else:
        raise ValueError(f"Unsupported interval: {interval}")
    while current <= end:
        buckets.append(current)
        if interval == "month":
            current = delta(current)  # type: ignore
        else:
            current = current + step  # type: ignore
    return buckets


# ============ PDF GENERATION ============

def create_report_pdf(
    stats: IntervalStats,
    balances: List[dict],
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
) -> bytes:
    """
    Generate a PDF report with charts and tables for the aggregated
    statistics.  Returns the PDF as a byte array.
    """
    if plt is None or pdf_canvas is None or ImageReader is None:
        raise RuntimeError(
            "Missing dependencies: matplotlib and reportlab are required to generate PDF reports."
        )
    # Create charts and save them into memory buffers
    images = []  # List of (title, image_data)
    # Helper to plot a single metric
    def plot_series(title: str, data_dict: Dict[datetime, Decimal], ylabel: str) -> bytes:
        x = [b for b in stats.buckets]
        y = [float(data_dict.get(b, Decimal("0"))) for b in x]
        plt.figure(figsize=(8, 4))
        plt.plot(x, y, marker="o")
        plt.title(title)
        plt.xlabel(f"Time ({interval})")
        plt.ylabel(ylabel)
        plt.tight_layout()
        buf = io.BytesIO()
        plt.gcf().autofmt_xdate()
        plt.savefig(buf, format="png")
        plt.close()
        return buf.getvalue()

    # Orders count per interval
    images.append((
        f"Number of orders per {interval}",
        plot_series(
            f"Orders per {interval}",
            stats.order_counts,
            "Count",
        ),
    ))
    # Binance trades count per interval (sold trades)
    trades_count_dict = {b: int(stats.sold_crypto[b] and stats.sold_crypto[b] != 0) for b in stats.buckets}
    images.append((
        f"Binance SELL trades per {interval}",
        plot_series(
            f"Binance SELL trades per {interval}",
            trades_count_dict,
            "Count",
        ),
    ))
    # Profit amount per interval
    profit_amount_dict = {}
    for b in stats.buckets:
        pa = stats.profit_amount(b)
        profit_amount_dict[b] = pa if pa is not None else Decimal("0")
    images.append((
        f"Profit amount per {interval}",
        plot_series(
            f"Profit amount per {interval}",
            profit_amount_dict,
            f"Profit ({ASSET})",
        ),
    ))
    # Cancelled trades per interval
    images.append((
        f"Cancelled trades per {interval}",
        plot_series(
            f"Cancelled trades per {interval}",
            stats.cancelled_counts,
            "Count",
        ),
    ))
    # Transaction flows per interval
    images.append((
        f"Net transaction flow per {interval}",
        plot_series(
            f"Net transaction flow per {interval}",
            stats.tx_flows,
            f"Net amount ({ASSET})",
        ),
    ))

    # Create PDF
    buf = io.BytesIO()
    c = pdf_canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    # Title
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, height - 40, "Statistics Report")
    c.setFont("Helvetica", 10)
    c.drawString(40, height - 60, f"Period: {from_dt.date()} – {to_dt.date()} (interval: {interval})")
    y_pos = height - 90
    # (Current balances section removed to keep the first page concise.)
    # Previously, this code displayed a table of user balances.  It has been removed.
    # Draw charts
    for title, img_data in images:
        # Start a new page if necessary
        c.showPage()
        # Insert chart title
        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, height - 40, title)
        # Insert image; keep aspect ratio
        image = ImageReader(io.BytesIO(img_data))
        iw, ih = image.getSize()
        max_width = width - 80
        max_height = height - 100
        scale = min(max_width / iw, max_height / ih)
        draw_w, draw_h = iw * scale, ih * scale
        c.drawImage(
            image,
            40,
            height - 60 - draw_h,
            width=draw_w,
            height=draw_h,
        )
    c.save()
    return buf.getvalue()


# ============ TELEGRAM HANDLERS ============

STATE_PERIOD = 1
STATE_INTERVAL = 2


HELP_TEXT = (
    "Этот бот собирает и визуализирует подробную статистику по работе P2P обменника.\n\n"
    "Команды:\n"
    "/start — приветствие и справка\n"
    "/help — эта справка\n"
    "/login <username> <password> — авторизация в internal‑p2p\n"
    "/stats <from_date> <to_date> [interval] [pdf] — собирать статистику за указанный период.\n"
    "  interval может быть hour/day/week/month (по умолчанию day).\n"
    "  При добавлении pdf бот отправит PDF‑отчёт с графиками, таблицами и диаграммами; без pdf выводятся краткие итоги текстом.\n"
    "В отчёт входят: количество ордеров, рекламных обновлений мейкеров/тейкеров, число объявлений, отменённые сделки, прибыль, "
    "объёмы купли/продажи, выводы средств, баланс на бирже и внутри сайта, а также доли on/off‑chain выводов и рекламы."
)


def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(
        "Привет! 👋\n" + HELP_TEXT
    )


def help_cmd(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(HELP_TEXT)


def login(update: Update, context: CallbackContext) -> None:
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


def stats(update: Update, context: CallbackContext) -> None:
    """Quick command to compute statistics and optionally return a PDF."""
    # Validate session and input arguments
    cookies = context.user_data.get("p2p_cookies")
    if not cookies:
        update.message.reply_text(
            "Нет авторизации для internal-p2p. Сначала сделай /login <username> <password>."
        )
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Нужно минимум два аргумента: from_date и to_date.\n\n" + HELP_TEXT
        )
        return
    from_str, to_str, *rest = args
    try:
        from_dt = parse_date_str(from_str)
        to_dt = parse_date_str(to_str)
    except Exception:
        update.message.reply_text(
            "Не удалось распарсить даты. Используй формат YYYY-MM-DD."
        )
        return
    if to_dt < from_dt:
        update.message.reply_text("to_date не может быть меньше from_date.")
        return
    # Determine interval and pdf flag
    interval = "day"
    generate_pdf = False
    for token in rest:
        token_lower = token.lower()
        if token_lower in {"hour", "day", "week", "month"}:
            interval = token_lower
        elif token_lower in {"pdf", "report"}:
            generate_pdf = True
    update.message.reply_text(
        f"Собираю статистику за период {from_dt.date()} – {to_dt.date()} (интервал: {interval})..."
    )
    try:
        # Fetch aggregated stats from API
        stats_data = fetch_statistics_report(from_dt, to_dt, interval, cookies)
        # Fetch balances for summary
        balances = fetch_internal_balances(cookies)
    except Exception as e:
        logger.exception("Error during statistics calculation")
        update.message.reply_text(f"Ошибка при расчёте статистики: {e}")
        return
    if generate_pdf:
        try:
            # Attempt to fetch detailed withdrawals for recipient pie charts
            withdrawals: List[dict] = []
            try:
                withdrawals = fetch_binance_withdrawals_history(from_dt, to_dt, cookies)
            except Exception as w_err:
                logger.warning("Error fetching withdrawals history: %s", w_err)
                withdrawals = []
            pdf_bytes = create_statistics_report_pdf(
                stats_data,
                balances,
                from_dt,
                to_dt,
                interval,
                withdrawals=withdrawals,
            )
            update.message.reply_document(
                document=io.BytesIO(pdf_bytes),
                filename=f"statistics_{from_dt.date()}_{to_dt.date()}.pdf",
                caption="Статистический отчёт",
            )
        except Exception as e:
            logger.exception("Error generating PDF report")
            update.message.reply_text(f"Ошибка при генерации PDF: {e}")
        return
    # Otherwise prepare textual summary
    lines: List[str] = []
    total_orders = 0
    total_maker_ads = 0
    total_taker_ads = 0
    total_maker_updates = 0
    total_taker_updates = 0
    total_cancelled = 0
    total_withdraw_on = 0
    total_withdraw_off = 0
    total_withdraw_on_usdt = 0.0
    total_withdraw_off_usdt = 0.0
    total_profit_usdt_sum = 0.0
    total_profit_rate_sum = 0.0
    profit_rate_count = 0
    total_net_flow = 0.0
    # New: track total profit across all intervals by computing profit
    # amounts using aggregated buy/sell volumes.  This mirrors the
    # original profits bot calculation.  It will be displayed alongside
    # the legacy sum of average profits.
    total_profit_amount_sum = 0.0
    for rec in stats_data:
        ps = rec.get("period_start")
        try:
            dt = datetime.fromisoformat(ps)
        except Exception:
            continue
        oc = int(rec.get("orders_count", 0) or 0)
        ma = int(rec.get("maker_ads_count", 0) or 0)
        ta = int(rec.get("taker_ads_count", 0) or 0)
        mu = int(rec.get("maker_updates", 0) or 0)
        tu = int(rec.get("taker_updates", 0) or 0)
        woc = int(rec.get("withdraw_onchain_count", 0) or 0)
        wfc = int(rec.get("withdraw_offchain_count", 0) or 0)
        wousdt = float(rec.get("withdraw_onchain_usdt", 0) or 0)
        wfusdt = float(rec.get("withdraw_offchain_usdt", 0) or 0)
        canc = int(rec.get("cancelled_count", 0) or 0)
        pr_usdt = rec.get("profit_usdt_avg")
        pr_rate = rec.get("profit_rate_avg")
        net_flow = float(rec.get("net_tx_flow", 0) or 0)
        # accumulate totals
        total_orders += oc
        total_maker_ads += ma
        total_taker_ads += ta
        total_maker_updates += mu
        total_taker_updates += tu
        total_withdraw_on += woc
        total_withdraw_off += wfc
        total_withdraw_on_usdt += wousdt
        total_withdraw_off_usdt += wfusdt
        total_cancelled += canc
        total_net_flow += net_flow
        if pr_usdt is not None:
            total_profit_usdt_sum += float(pr_usdt)
        if pr_rate is not None:
            total_profit_rate_sum += float(pr_rate)
            profit_rate_count += 1
        # Build per bucket line
        # Compute actual profit amount for this interval using aggregated buy/sell volumes
        try:
            from decimal import Decimal
            bc_val = rec.get("bought_usdt", 0) or 0
            sc_val = rec.get("sold_usdt", 0) or 0
            bf_val = rec.get("bought_uah", 0) or 0
            sf_val = rec.get("sold_uah", 0) or 0
            bc = Decimal(str(bc_val))
            sc = Decimal(str(sc_val))
            bf = Decimal(str(bf_val))
            sf = Decimal(str(sf_val))
            profit_amount_this = None
            if bc != 0 and sc != 0:
                avg_buy_rate = bf / bc
                avg_sell_rate = sf / sc
                pa = sc * avg_sell_rate / avg_buy_rate - sc - sc / Decimal("1000")
                profit_amount_this = float(pa)
        except Exception:
            profit_amount_this = None
        # Prepare profit display strings
        profit_usdt_str = f"{profit_amount_this:.4f} {ASSET}" if profit_amount_this is not None else "N/A"
        profit_rate_str = f"{float(pr_rate):.4f}" if pr_rate is not None else "N/A"
        lines.append(
            f"{dt} — orders: {oc}, maker ads: {ma}, taker ads: {ta}, maker updates: {mu}, taker updates: {tu}, "
            f"withdraw on/off: {woc}/{wfc}, profit: {profit_usdt_str}, rate: {profit_rate_str}, cancelled: {canc}, net flow: {net_flow:.4f}"
        )

        # Compute actual profit amount for this interval using aggregated
        # buy/sell volumes (UAH and USDT).  This replicates the formula
        # used by the profits bot: profit = sold_crypto * (avg_sell_rate / avg_buy_rate)
        #       - sold_crypto - sold_crypto * 0.1% fee.  We use Decimals
        # for intermediate calculations to reduce rounding errors.
        try:
            from decimal import Decimal
            bc_val = rec.get("bought_usdt", 0) or 0
            sc_val = rec.get("sold_usdt", 0) or 0
            bf_val = rec.get("bought_uah", 0) or 0
            sf_val = rec.get("sold_uah", 0) or 0
            bc = Decimal(str(bc_val))
            sc = Decimal(str(sc_val))
            bf = Decimal(str(bf_val))
            sf = Decimal(str(sf_val))
            if bc != 0 and sc != 0:
                avg_buy_rate = bf / bc
                avg_sell_rate = sf / sc
                pa = sc * avg_sell_rate / avg_buy_rate - sc - sc / Decimal("1000")
                total_profit_amount_sum += float(pa)
        except Exception:
            # If any error occurs, skip this record for total profit
            pass
    # Totals summary
    lines.append("")
    lines.append(f"Всего ордеров: {total_orders}")
    lines.append(f"Всего maker ads: {total_maker_ads}")
    lines.append(f"Всего taker ads: {total_taker_ads}")
    lines.append(f"Всего maker updates: {total_maker_updates}")
    lines.append(f"Всего taker updates: {total_taker_updates}")
    lines.append(f"Всего отменённых: {total_cancelled}")
    lines.append(f"Всего withdraw on/off: {total_withdraw_on}/{total_withdraw_off}")
    lines.append(f"Всего withdraw on‑chain USDT: {total_withdraw_on_usdt:.4f}")
    lines.append(f"Всего withdraw off‑chain USDT: {total_withdraw_off_usdt:.4f}")
    lines.append(f"Суммарный поток транзакций: {total_net_flow:.4f} {ASSET}")
    # Display actual total profit and legacy sum of average profits.  The
    # actual profit is computed using aggregated buy/sell volumes.
    lines.append(f"Итоговая прибыль: {total_profit_amount_sum:.4f} {ASSET}")
    lines.append(f"Сумма средней прибыли по интервалам: {total_profit_usdt_sum:.4f} {ASSET}")
    avg_pr = (total_profit_rate_sum / profit_rate_count) if profit_rate_count else None
    lines.append(f"Средняя прибыльная ставка: {avg_pr:.4f}" if avg_pr is not None else "Средняя прибыльная ставка: N/A")
    # (Removed Top balances section from the textual summary.)
    update.message.reply_text("\n".join(lines))


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env var is not set")
    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CommandHandler("login", login))
    dp.add_handler(CommandHandler("stats", stats))
    logger.info("Statistics bot starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
