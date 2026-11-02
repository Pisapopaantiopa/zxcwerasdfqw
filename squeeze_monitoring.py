#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Монитор сквизов Binance USDT-M Futures -> Telegram  (v2: realtime + журнал коллов)

Как это устроено
  1. СКАНЕР (REST, раз в SCAN_INTERVAL_SEC): по ликвидным перпетуалам считает 'сетап' —
     перекос позиционирования, funding, open interest, близость к зоне стопов, тонкий стакан.
     Монеты с баллом >= SEND_SCORE уходят в Telegram как колл типа 📋 сетап.
     Монеты с баллом >= WATCH_SCORE попадают в WATCHLIST.
  2. REALTIME-ДВИЖОК (WebSocket, каждую секунду): держит живые markPrice / funding / ликвидации
     по ВСЕМ монетам и на каждом кадре перепроверяет watchlist. Если у монеты из watchlist
     начался сквиз (импульс против толпы, пробой 48ч-уровня, всплеск ликвидаций толпы,
     сброс OI) — немедленно уходит колл типа ⚡ триггер, не дожидаясь следующего скана.
     Монеты вне watchlist, на которых внезапно пошёл импульс или ликвидации, автоматически
     дополучают метрики по REST и добавляются в watchlist ('промоут').
  3. ЖУРНАЛ КОЛЛОВ (SQLite): каждый отправленный сигнал пишется в БД; через свечи 1м бот сам
     проверяет исход: hit (цена прошла +HIT_PCT% в сторону сквиза раньше, чем -STOP_PCT% против),
     miss или flat (за OUTCOME_HORIZON_SEC не случилось ни того, ни другого).
  4. БОТ С КНОПКАМИ: постоянная клавиатура «Журнал / Статистика / Статус / Помощь»,
     журнал с пагинацией, фильтрами и карточкой колла, статистика hit-rate по признакам.
     Бот отвечает ТОЛЬКО чату из TELEGRAM_CHAT_ID.

Запуск:
  pip install aiohttp
  .env рядом (запускайте из той же папки):  TELEGRAM_BOT_TOKEN=...  TELEGRAM_CHAT_ID=...
  python squeeze_monitor.py              # постоянная работа
  python squeeze_monitor.py --once       # один скан + одна проверка исходов
  python squeeze_monitor.py --dry-run    # сигналы в консоль, без Telegram

Binance с 06.03.2026 переехал на новые WebSocket-адреса (/market, /public); старые отключены
23.04.2026. Здесь используется wss://fstream.binance.com/market (BINANCE_WS в .env).

Не является финансовой рекомендацией.
"""
from __future__ import annotations

import argparse
import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

import aiohttp
from yarl import URL

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# ----------------------------------------------------------------------------
# Конфиг (всё переопределяется через переменные окружения / .env)
# ----------------------------------------------------------------------------
def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_dotenv()


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _i(name: str, default: int) -> int:
    return int(os.getenv(name, default))


TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
TG_API = os.getenv("TELEGRAM_API", "https://api.telegram.org")

BASE = os.getenv("BINANCE_REST", "https://fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS", "wss://fstream.binance.com/market")
MARK_SPEED = os.getenv("RT_MARK_SPEED", "1s")  # '1s' или '' (кадр раз в 3 с)

DB_PATH = os.getenv("DB_PATH", "squeeze_journal.sqlite3")
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Moscow")

# --- сканер ---
SCAN_INTERVAL_SEC = _i("SCAN_INTERVAL_SEC", 600)
SCAN_START_DELAY = _i("SCAN_START_DELAY", 20)         # пауза перед 1-м сканом: дать WS накопить данные
MIN_QUOTE_VOLUME = _f("MIN_QUOTE_VOLUME", 3_000_000)
TOP_ALWAYS = _i("TOP_ALWAYS", 40)
MAX_DETAIL = _i("MAX_DETAIL", 120)
CONCURRENCY = _i("CONCURRENCY", 10)
DATA_RPS = _f("DATA_RPS", 2.5)                        # /futures/data/*: лимит 1000 запросов / 5 мин

# --- пороги признаков сетапа ---
PCT_EXTREME = _f("PCT_EXTREME", 90)
FUND_LONG = _f("FUND_LONG", 0.0003)
FUND_SHORT = _f("FUND_SHORT", -0.0002)
OI_24H_PCT = _f("OI_24H_PCT", 10)
OI_4H_PCT = _f("OI_4H_PCT", 2)
OI_Z = _f("OI_Z", 1.5)
NEAR_LEVEL_PCT = _f("NEAR_LEVEL_PCT", 1.5)
DEPTH_BAND_PCT = _f("DEPTH_BAND_PCT", 0.5)
DEPTH_OI_RATIO = _f("DEPTH_OI_RATIO", 0.006)
LIQ_WINDOW_SEC = _i("LIQ_WINDOW_SEC", 900)
LIQ_ACTIVE_USD = _f("LIQ_ACTIVE_USD", 200_000)

WEIGHTS = {
    "skew_g": 1.0, "skew_t": 1.0, "funding": 1.0, "oi_build": 1.0, "oi_trap": 1.0,
    "level": 1.0, "depth": 1.0, "session": 0.5, "liq": 1.0,
}
MAX_SCORE = sum(WEIGHTS.values())
SEND_SCORE = _f("SEND_SCORE", 5.0)
MAX_ALERTS_PER_CYCLE = _i("MAX_ALERTS_PER_CYCLE", 8)
COOLDOWN_SEC = _i("COOLDOWN_SEC", 4 * 3600)
HIST_LIMIT = 168

# --- realtime ---
RT_ENABLED = os.getenv("RT_ENABLED", "1") != "0"
WATCH_SCORE = _f("WATCH_SCORE", 3.5)               # мин. балл сетапа для watchlist и для триггера
WATCH_MAX = _i("WATCH_MAX", 25)
RT_WINDOW_SEC = _i("RT_WINDOW_SEC", 300)           # окно для импульса и всплеска ликвидаций
RT_MOVE_PCT = _f("RT_MOVE_PCT", 1.5)               # импульс против толпы за окно, %
RT_LIQ_USD = _f("RT_LIQ_USD", 150_000)             # ликвидации толпы за окно, USD
RT_OI_DROP_PCT = _f("RT_OI_DROP_PCT", 1.0)         # сброс OI от максимума окна, %
RT_OI_WINDOW_SEC = _i("RT_OI_WINDOW_SEC", 900)
RT_OI_POLL_SEC = _i("RT_OI_POLL_SEC", 30)          # опрос OI для watchlist
RT_MIN_TRIGGERS = _i("RT_MIN_TRIGGERS", 2)         # сколько триггер-признаков нужно одновременно
RT_COOLDOWN_SEC = _i("RT_COOLDOWN_SEC", 1800)      # пауза между триггерами по одной монете
RT_PROMOTE_MOVE_PCT = _f("RT_PROMOTE_MOVE_PCT", 3.0)
RT_PROMOTE_LIQ_USD = _f("RT_PROMOTE_LIQ_USD", 300_000)
RT_PROMOTE_COOLDOWN_SEC = _i("RT_PROMOTE_COOLDOWN_SEC", 900)
RT_PROMOTE_TTL_SEC = _i("RT_PROMOTE_TTL_SEC", 7200)
RT_PROMO_SCAN_SEC = _f("RT_PROMO_SCAN_SEC", 5.0)

# --- журнал и оценка исходов ---
HIT_PCT = _f("HIT_PCT", 3.0)
STOP_PCT = _f("STOP_PCT", 3.0)
OUTCOME_HORIZON_SEC = _i("OUTCOME_HORIZON_SEC", 24 * 3600)
OUTCOME_CHECK_SEC = _i("OUTCOME_CHECK_SEC", 300)
PAGE_SIZE = _i("PAGE_SIZE", 5)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("squeeze")

try:
    LOCAL = ZoneInfo(LOCAL_TZ) if ZoneInfo else timezone(timedelta(hours=3))
except Exception:  # noqa: BLE001  (нет tzdata, например на Windows)
    LOCAL = timezone(timedelta(hours=3))

FLAG_ORDER = ["skew_g", "skew_t", "funding", "oi_build", "oi_trap", "level", "depth", "liq", "session"]
TRIG_ORDER = ["impulse", "break", "liq_burst", "flush"]
FLAG_NAMES = {
    "skew_g": "перекос (аккаунты)", "skew_t": "перекос (топ-трейдеры)", "funding": "funding",
    "oi_build": "рост OI", "oi_trap": "OI-ловушка", "level": "у зоны стопов", "depth": "тонкий стакан",
    "session": "тонкая сессия", "liq": "ликвидации",
    "impulse": "импульс", "break": "пробой уровня", "liq_burst": "всплеск ликвидаций", "flush": "сброс OI",
}
OUT_ICON = {"hit": "✅", "miss": "❌", "flat": "➖", None: "⏳"}
KIND_LABEL = {"setup": "📋 сетап", "trigger": "⚡ триггер"}
CROWD_LABEL = {"short": "🚀 шорт-сквиз", "long": "🩸 лонг-сквиз"}


# ----------------------------------------------------------------------------
# Утилиты
# ----------------------------------------------------------------------------
def esc(x) -> str:
    return html.escape(str(x), quote=False)


def pctl(series: list[float], v: float) -> float:
    return 100.0 * sum(1 for x in series if x <= v) / len(series)


def mean_std(xs: list[float]) -> tuple[float, float]:
    n = len(xs)
    mu = sum(xs) / n
    return mu, (sum((x - mu) ** 2 for x in xs) / n) ** 0.5


def usd(x: float) -> str:
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:.2f}{suf}"
    return f"${x:.0f}"


def is_thin_session() -> bool:
    now = datetime.now(timezone.utc)
    return now.weekday() >= 5 or now.hour < 6


def fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, LOCAL).strftime("%d.%m %H:%M")


def fmt_dur(sec: float) -> str:
    sec = int(max(0, sec))
    d, r = divmod(sec, 86400)
    h, r = divmod(r, 3600)
    m = r // 60
    if d:
        return f"{d}д {h}ч"
    if h:
        return f"{h}ч {m}м"
    return f"{m}м"


def short(sym: str) -> str:
    return sym[:-4] if sym.endswith("USDT") else sym


def clip(text: str, limit: int = 3900) -> str:
    if len(text) <= limit:
        return text
    cut = text.rfind("\n", 0, limit)
    return text[: cut if cut > 0 else limit]


# ----------------------------------------------------------------------------
# Клиент Binance REST с учётом лимитов
# ----------------------------------------------------------------------------
class ApiError(Exception):
    pass


class Pacer:
    """Равномерно распределяет запросы во времени."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.lock = asyncio.Lock()
        self.next = 0.0

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            delay = self.next - now
            if delay > 0:
                await asyncio.sleep(delay)
                now = time.monotonic()
            self.next = now + self.interval


class Binance:
    def __init__(self, session: aiohttp.ClientSession):
        self.s = session
        self.used_weight = 0
        self.data_pacer = Pacer(DATA_RPS)
        self.gen_pacer = Pacer(12)

    async def get(self, path: str, params: Optional[dict] = None, data: bool = False, retries: int = 4):
        for attempt in range(retries):
            if self.used_weight > 1800:  # лимит 2400/мин, оставляем запас
                wait = 61 - (time.time() % 60)
                log.warning("Вес запросов %s/мин — пауза %.0f с", self.used_weight, wait)
                await asyncio.sleep(wait)
                self.used_weight = 0
            await (self.data_pacer if data else self.gen_pacer).wait()
            try:
                async with self.s.get(
                    BASE + path, params=params, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    w = r.headers.get("x-mbx-used-weight-1m")
                    if w:
                        self.used_weight = int(w)
                    if r.status in (429, 418):
                        ra = int(r.headers.get("Retry-After", "30"))
                        log.warning("HTTP %s, жду %s с", r.status, ra)
                        await asyncio.sleep(ra + 1)
                        continue
                    if r.status == 451:
                        raise ApiError("HTTP 451: доступ к Binance Futures заблокирован для вашего IP/региона")
                    if 400 <= r.status < 500:
                        raise ApiError(f"HTTP {r.status} {path} {params}")
                    r.raise_for_status()
                    return await r.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == retries - 1:
                    raise ApiError(f"{type(e).__name__}: {e}")
                await asyncio.sleep(2 ** attempt)
        raise ApiError(f"Не удалось получить {path}")


# ----------------------------------------------------------------------------
# Живой поток Binance: markPrice@arr + forceOrder@arr (WebSocket)
# ----------------------------------------------------------------------------
def ws_url() -> str:
    mark = "!markPrice@arr@1s" if MARK_SPEED == "1s" else "!markPrice@arr"
    return f"{WS_BASE}/stream?streams={mark}/!forceOrder@arr"


class LiveFeed:
    """Хранит в памяти живое состояние рынка: цена, индекс, funding, история цены и ликвидации.
    На каждом кадре markPrice вызывает синхронный колбэк on_frame (реакция на события)."""

    HIST_STEP = max(1.0, min(5.0, RT_WINDOW_SEC / 4))
    KEEP_SEC = max(RT_WINDOW_SEC, LIQ_WINDOW_SEC) + 120

    def __init__(self):
        self.mark: dict[str, float] = {}
        self.index: dict[str, float] = {}
        self.funding: dict[str, float] = {}
        self.hist: dict[str, deque] = defaultdict(deque)
        self.events: dict[str, deque] = defaultdict(deque)
        self.frames = 0
        self.last_frame = 0.0
        self.n_symbols = 0
        self.connected = False
        self.on_frame: Optional[Callable[[], None]] = None

    # --- чтение ---
    def funding_8h(self, sym: str, f_int: dict[str, float]) -> Optional[float]:
        r = self.funding.get(sym)
        return None if r is None else r * 8 / f_int.get(sym, 8)

    def move_pct(self, sym: str, window: float) -> Optional[float]:
        """Изменение цены за последние window секунд (None — если истории пока мало)."""
        dq, p = self.hist.get(sym), self.mark.get(sym)
        if not dq or not p:
            return None
        target = time.time() - window
        if dq[0][0] > target + window * 0.4:
            return None
        base = dq[-1][1]
        for t, q in dq:
            if t >= target:
                base = q
                break
        return (p / base - 1) * 100 if base else None

    def recent(self, sym: str, window: Optional[float] = None) -> tuple[float, float]:
        """(ликвидации лонгов, ликвидации шортов) в USD за window секунд."""
        dq = self.events.get(sym)
        if not dq:
            return 0.0, 0.0
        cut = time.time() - (LIQ_WINDOW_SEC if window is None else window)
        ll = sum(u for t, s, u in dq if t >= cut and s == "SELL")
        ls = sum(u for t, s, u in dq if t >= cut and s == "BUY")
        return ll, ls

    # --- разбор сообщений ---
    def _on_mark(self, arr: list) -> None:
        now = time.time()
        n = 0
        for x in arr:
            try:
                s = x["s"]
                if not s.endswith("USDT"):
                    continue
                p = float(x["p"])
                self.mark[s] = p
                i = x.get("i")
                if i:
                    self.index[s] = float(i)
                r = x.get("r")
                if r not in (None, ""):
                    self.funding[s] = float(r)
            except (KeyError, ValueError, TypeError, AttributeError):
                continue
            dq = self.hist[s]
            if not dq or now - dq[-1][0] >= self.HIST_STEP:
                dq.append((now, p))
                cut = now - self.KEEP_SEC
                while dq and dq[0][0] < cut:
                    dq.popleft()
            n += 1
        self.n_symbols = n
        self.frames += 1
        self.last_frame = now
        if self.on_frame:
            try:
                self.on_frame()
            except Exception:  # noqa: BLE001
                log.exception("Ошибка в on_frame")

    def _on_force(self, ev: dict) -> None:
        o = ev.get("o")
        if not o:
            return
        try:
            price = float(o.get("ap") or 0) or float(o["p"])
            qty = float(o.get("z") or 0) or float(o["q"])
            dq = self.events[o["s"]]
            now = time.time()
            dq.append((now, o["S"], price * qty))
            cut = now - self.KEEP_SEC
            while dq and dq[0][0] < cut:
                dq.popleft()
        except (KeyError, ValueError, TypeError):
            return

    def handle_text(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if isinstance(data, list):
            if data and isinstance(data[0], dict) and data[0].get("e") == "forceOrder":
                for ev in data:
                    self._on_force(ev)
            else:
                self._on_mark(data)
        elif isinstance(data, dict) and data.get("e") == "forceOrder":
            self._on_force(data)

    async def run(self, session: aiohttp.ClientSession) -> None:
        backoff = 1.0
        while True:
            try:
                async with session.ws_connect(URL(ws_url(), encoded=True), heartbeat=20, max_msg_size=0) as ws:
                    log.info("WebSocket подключён: %s", ws_url().split("?")[0])
                    self.connected = True
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.handle_text(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                          aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("WebSocket: %s: %s", type(e).__name__, e)
            self.connected = False
            await asyncio.sleep(min(backoff, 30))
            backoff *= 2


# ----------------------------------------------------------------------------
# Модели данных
# ----------------------------------------------------------------------------
@dataclass
class Candidate:
    sym: str
    mark: float
    vol: float
    f8: float
    prem_pct: float


@dataclass
class Metrics:
    sym: str
    price: float
    funding_8h: float
    premium_pct: float
    oi_usd: float
    oi_4h: float
    oi_24h: float
    oi_z: float
    glob_ratio: float
    glob_pct: float
    top_ratio: float
    top_pct: float
    price_4h: float
    price_24h: float
    dist_high: float  # % от цены до 48ч-хая (отрицательное — хай пробит)
    dist_low: float   # % от цены до 48ч-лоя
    liq_long: float
    liq_short: float
    depth_usd: Optional[float] = None
    depth_ratio: Optional[float] = None
    hi48: float = 0.0
    lo48: float = 0.0
    ts: float = 0.0


@dataclass
class Result:
    sym: str
    crowd: str  # 'long' — под угрозой лонги (лонг-сквиз), 'short' — шорты (шорт-сквиз)
    score: float
    flags: dict = field(default_factory=dict)
    m: Optional[Metrics] = None


@dataclass
class Watch:
    crowd: str
    score: float
    m: Metrics
    src: str   # 'scan' | 'promo'
    ts: float


# ----------------------------------------------------------------------------
# Сбор метрик по одной монете (REST)
# ----------------------------------------------------------------------------
async def analyze(api: Binance, c: Candidate, feed: LiveFeed) -> Optional[Metrics]:
    p = {"symbol": c.sym, "period": "1h", "limit": HIST_LIMIT}
    try:
        oi_h, gl_h, top_h, kl = await asyncio.gather(
            api.get("/futures/data/openInterestHist", p, data=True),
            api.get("/futures/data/globalLongShortAccountRatio", p, data=True),
            api.get("/futures/data/topLongShortPositionRatio", p, data=True),
            api.get("/fapi/v1/klines", {"symbol": c.sym, "interval": "1h", "limit": 48}),
        )
        oi = [float(x["sumOpenInterestValue"]) for x in oi_h]
        gl = [float(x["longShortRatio"]) for x in gl_h]
        tp = [float(x["longShortRatio"]) for x in top_h]
        closes = [float(k[4]) for k in kl]
        highs = [float(k[2]) for k in kl]
        lows = [float(k[3]) for k in kl]
        if len(oi) < 26 or len(gl) < 24 or len(tp) < 24 or len(closes) < 25:
            return None

        price = c.mark
        mu, sd = mean_std(oi)
        oi_z = (oi[-1] - mu) / sd if sd > 0 else 0.0
        ll, ls = feed.recent(c.sym, LIQ_WINDOW_SEC)
        return Metrics(
            sym=c.sym, price=price, funding_8h=c.f8, premium_pct=c.prem_pct,
            oi_usd=oi[-1],
            oi_4h=(oi[-1] / oi[-5] - 1) * 100 if oi[-5] else 0.0,
            oi_24h=(oi[-1] / oi[-25] - 1) * 100 if oi[-25] else 0.0,
            oi_z=oi_z,
            glob_ratio=gl[-1], glob_pct=pctl(gl, gl[-1]),
            top_ratio=tp[-1], top_pct=pctl(tp, tp[-1]),
            price_4h=(closes[-1] / closes[-5] - 1) * 100,
            price_24h=(closes[-1] / closes[-25] - 1) * 100,
            dist_high=(max(highs) / price - 1) * 100,
            dist_low=(1 - min(lows) / price) * 100,
            liq_long=ll, liq_short=ls,
            hi48=max(highs), lo48=min(lows), ts=time.time(),
        )
    except (ApiError, KeyError, ValueError, ZeroDivisionError, TypeError) as e:
        log.debug("%s: пропуск (%s)", c.sym, e)
        return None


async def fetch_depth_usd(api: Binance, sym: str) -> Optional[float]:
    """Суммарная глубина (bid+ask) в полосе ±DEPTH_BAND_PCT% от mid в USDT."""
    band = DEPTH_BAND_PCT / 100
    try:
        d = await api.get("/fapi/v1/depth", {"symbol": sym, "limit": 1000})
        bids = [(float(p), float(q)) for p, q in d["bids"]]
        asks = [(float(p), float(q)) for p, q in d["asks"]]
    except (ApiError, KeyError, ValueError):
        return None
    if not bids or not asks:
        return None
    mid = (bids[0][0] + asks[0][0]) / 2

    def side(levels: list[tuple[float, float]]) -> float:
        total = 0.0
        for p, q in levels:
            if abs(p - mid) / mid > band:
                break
            total += p * q
        last = abs(levels[-1][0] - mid) / mid
        if 0 < last < band:  # стакан закончился раньше границы полосы
            total *= band / last
        return total

    return side(bids) + side(asks)


# ----------------------------------------------------------------------------
# Скоринг сетапа
# ----------------------------------------------------------------------------
def evaluate(m: Metrics, crowd: str, thin_session: bool) -> Result:
    d = 1 if crowd == "long" else -1  # +1: толпа в лонгах, -1: толпа в шортах
    fl: dict[str, str] = {}

    def extreme(p: float) -> bool:
        return p >= PCT_EXTREME if d == 1 else p <= 100 - PCT_EXTREME

    if extreme(m.glob_pct) and (m.glob_ratio > 1) == (d == 1):
        fl["skew_g"] = f"Перекос аккаунтов: L/S {m.glob_ratio:.2f} (перцентиль 7д: {m.glob_pct:.0f})"
    if extreme(m.top_pct) and (m.top_ratio > 1) == (d == 1):
        fl["skew_t"] = f"Топ-трейдеры: L/S позиций {m.top_ratio:.2f} (перцентиль 7д: {m.top_pct:.0f})"

    if (d == 1 and m.funding_8h >= FUND_LONG) or (d == -1 and m.funding_8h <= FUND_SHORT):
        fl["funding"] = f"Funding {m.funding_8h * 100:+.3f}%/8ч, premium {m.premium_pct:+.2f}%"

    if m.oi_24h >= OI_24H_PCT or m.oi_z >= OI_Z:
        fl["oi_build"] = f"OI {usd(m.oi_usd)}: {m.oi_24h:+.1f}% за 24ч, z-score {m.oi_z:.1f}"
    if m.oi_4h >= OI_4H_PCT and (-d * m.price_4h) >= -0.3:
        fl["oi_trap"] = f"OI {m.oi_4h:+.1f}% за 4ч, цена {m.price_4h:+.1f}% (против толпы или стоит)"

    if d == 1 and m.dist_low <= NEAR_LEVEL_PCT:
        fl["level"] = f"Цена в {m.dist_low:.2f}% от 48ч-лоя (под ним стопы лонгов)"
    if d == -1 and m.dist_high <= NEAR_LEVEL_PCT:
        fl["level"] = f"Цена в {m.dist_high:.2f}% от 48ч-хая (над ним стопы шортов)"

    if m.depth_ratio is not None and m.depth_ratio <= DEPTH_OI_RATIO:
        fl["depth"] = f"Глубина ±{DEPTH_BAND_PCT}%: {usd(m.depth_usd or 0)} ({m.depth_ratio * 100:.2f}% от OI)"
    if thin_session:
        fl["session"] = "Тонкая сессия (выходные / ночь UTC)"

    liq = m.liq_long if d == 1 else m.liq_short
    if liq >= LIQ_ACTIVE_USD:
        side = "лонгов" if d == 1 else "шортов"
        fl["liq"] = f"Ликвидации {side} за {LIQ_WINDOW_SEC // 60} мин: {usd(liq)} — сквиз уже идёт"

    return Result(m.sym, crowd, sum(WEIGHTS[k] for k in fl), fl, m)


def best_result(m: Metrics, thin: bool) -> Result:
    a, b = evaluate(m, "long", thin), evaluate(m, "short", thin)
    return a if a.score >= b.score else b


def has_skew(flags: dict) -> bool:
    return "skew_g" in flags or "skew_t" in flags


# ----------------------------------------------------------------------------
# Realtime: живые метрики и триггер-признаки
# ----------------------------------------------------------------------------
def live_metrics(m: Metrics, feed: LiveFeed, f_int: dict[str, float]) -> Metrics:
    """Снимок сканера, обновлённый живыми данными из WebSocket."""
    s = m.sym
    price = feed.mark.get(s) or m.price
    idx = feed.index.get(s)
    f8 = feed.funding_8h(s, f_int)
    ll, ls = feed.recent(s, LIQ_WINDOW_SEC)
    ratio = price / m.price if m.price else 1.0
    return replace(
        m,
        price=price,
        funding_8h=m.funding_8h if f8 is None else f8,
        premium_pct=(price / idx - 1) * 100 if idx else m.premium_pct,
        price_4h=((1 + m.price_4h / 100) * ratio - 1) * 100,
        price_24h=((1 + m.price_24h / 100) * ratio - 1) * 100,
        dist_high=(m.hi48 / price - 1) * 100 if m.hi48 else m.dist_high,
        dist_low=(1 - m.lo48 / price) * 100 if m.lo48 else m.dist_low,
        liq_long=ll,
        liq_short=ls,
    )


def trigger_flags(crowd: str, m: Metrics, feed: LiveFeed, oi_flush: Optional[float]) -> dict[str, str]:
    """Признаки того, что сквиз против данной толпы НАЧИНАЕТСЯ прямо сейчас."""
    d = 1 if crowd == "long" else -1
    fl: dict[str, str] = {}
    win = f"{max(1, RT_WINDOW_SEC // 60)} мин" if RT_WINDOW_SEC >= 60 else f"{RT_WINDOW_SEC} с"

    mv = feed.move_pct(m.sym, RT_WINDOW_SEC)
    if mv is not None and -d * mv >= RT_MOVE_PCT:
        fl["impulse"] = f"Импульс против толпы: {mv:+.2f}% за {win}"

    if d == -1 and m.hi48 and m.price >= m.hi48:
        fl["break"] = f"Пробит 48ч-хай {m.hi48:g} (сняты стопы шортов)"
    if d == 1 and m.lo48 and m.price <= m.lo48:
        fl["break"] = f"Пробит 48ч-лой {m.lo48:g} (сняты стопы лонгов)"

    ll, ls = feed.recent(m.sym, RT_WINDOW_SEC)
    liq = ll if d == 1 else ls
    if liq >= RT_LIQ_USD:
        fl["liq_burst"] = f"Ликвидации {'лонгов' if d == 1 else 'шортов'} за {win}: {usd(liq)}"

    if oi_flush is not None and oi_flush <= -RT_OI_DROP_PCT:
        fl["flush"] = f"OI упал на {abs(oi_flush):.1f}% от максимума за {RT_OI_WINDOW_SEC // 60} мин (принудительное закрытие)"
    return fl


# ----------------------------------------------------------------------------
# Оценка исхода колла по свечам 1м (тройной барьер)
# ----------------------------------------------------------------------------
def evaluate_outcome(crowd: str, entry: float, candles: list, call_ts: float, now: float,
                     horizon: Optional[float] = None, hit_pct: Optional[float] = None,
                     stop_pct: Optional[float] = None) -> dict:
    """candles — свечи Binance (open_time_ms, o, h, l, c, ...) в хронологии, начиная после колла.
    hit: цена прошла hit_pct% в сторону сквиза раньше, чем stop_pct% против.
    miss: наоборот (если оба барьера в одной свече — считаем miss, консервативно).
    flat: за horizon не случилось ни того, ни другого."""
    horizon = OUTCOME_HORIZON_SEC if horizon is None else horizon
    hit_pct = HIT_PCT if hit_pct is None else hit_pct
    stop_pct = STOP_PCT if stop_pct is None else stop_pct
    sign = 1 if crowd == "short" else -1   # шорт-сквиз = цена вверх
    end_ts = call_ts + horizon
    mfe = mae = 0.0
    last_close: Optional[float] = None
    for k in candles:
        ot = k[0] / 1000.0
        if ot >= end_ts:
            break
        hi, lo, cl = float(k[2]), float(k[3]), float(k[4])
        up, down = (hi / entry - 1) * 100, (1 - lo / entry) * 100
        fav, adv = (up, down) if sign > 0 else (down, up)
        mfe, mae = max(mfe, fav), max(mae, adv)
        last_close = cl
        t_end = max(0.0, (ot + 60 - call_ts) / 60)
        if adv >= stop_pct:
            return dict(outcome="miss", mfe=mfe, mae=mae, ret_end=-stop_pct, t_end=t_end, ret_now=-stop_pct)
        if fav >= hit_pct:
            return dict(outcome="hit", mfe=mfe, mae=mae, ret_end=hit_pct, t_end=t_end, ret_now=hit_pct)
    ret_now = sign * (last_close / entry - 1) * 100 if last_close else None
    if now >= end_ts:
        return dict(outcome="flat", mfe=mfe, mae=mae, ret_end=ret_now if ret_now is not None else 0.0,
                    t_end=horizon / 60, ret_now=ret_now)
    return dict(outcome=None, mfe=mfe, mae=mae, ret_end=None, t_end=None, ret_now=ret_now)


# ----------------------------------------------------------------------------
# Журнал коллов (SQLite)
# ----------------------------------------------------------------------------
class Journal:
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS calls(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL, sym TEXT NOT NULL, crowd TEXT NOT NULL, kind TEXT NOT NULL,
        score REAL NOT NULL, price REAL NOT NULL,
        flags TEXT NOT NULL, trig TEXT NOT NULL, metrics TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open', outcome TEXT,
        mfe REAL, mae REAL, ret_now REAL, ret_end REAL, t_end REAL, checked_ts REAL
    );
    CREATE INDEX IF NOT EXISTS ix_calls_key ON calls(sym, crowd, kind, ts);
    CREATE INDEX IF NOT EXISTS ix_calls_status ON calls(status, ts);
    """
    FILTERS = {
        "a": "1=1", "o": "status='open'", "c": "status='closed'",
        "s": "kind='setup'", "t": "kind='trigger'",
    }

    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        self.db.executescript(self.SCHEMA)
        self.db.commit()

    def add(self, *, sym: str, crowd: str, kind: str, score: float, price: float,
            flags: dict, trig: dict, metrics: dict) -> int:
        cur = self.db.execute(
            "INSERT INTO calls(ts,sym,crowd,kind,score,price,flags,trig,metrics) VALUES(?,?,?,?,?,?,?,?,?)",
            (time.time(), sym, crowd, kind, score, price,
             json.dumps(flags, ensure_ascii=False), json.dumps(trig, ensure_ascii=False),
             json.dumps(metrics, ensure_ascii=False)),
        )
        self.db.commit()
        return int(cur.lastrowid or 0)

    def get(self, cid: int):
        return self.db.execute("SELECT * FROM calls WHERE id=?", (cid,)).fetchone()

    def open_calls(self):
        return self.db.execute("SELECT * FROM calls WHERE status='open' ORDER BY ts").fetchall()

    def all_rows(self):
        return self.db.execute("SELECT * FROM calls ORDER BY ts").fetchall()

    def page(self, filt: str, page: int, size: int):
        w = self.FILTERS.get(filt, "1=1")
        total = self.db.execute(f"SELECT COUNT(*) FROM calls WHERE {w}").fetchone()[0]
        pages = max(1, -(-total // size))
        page = min(max(page, 0), pages - 1)
        rows = self.db.execute(
            f"SELECT * FROM calls WHERE {w} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            (size, page * size),
        ).fetchall()
        return rows, total, page, pages

    def counts(self) -> dict:
        c = {"hit": 0, "miss": 0, "flat": 0, "open": 0}
        for status, outcome, n in self.db.execute(
            "SELECT status, outcome, COUNT(*) FROM calls GROUP BY status, outcome"
        ):
            if status == "open":
                c["open"] += n
            elif outcome in c:
                c[outcome] += n
        return c

    def count_since(self, ts: float, kind: str) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM calls WHERE ts>=? AND kind=?", (ts, kind)
        ).fetchone()[0]

    def latest_by_key(self) -> dict:
        """(sym, crowd, kind) -> (ts, score, были_ли_ликвидации): чтобы cooldown переживал перезапуск."""
        out = {}
        for r in self.db.execute("SELECT sym,crowd,kind,ts,score,flags FROM calls ORDER BY id"):
            try:
                liq = "liq" in json.loads(r["flags"])
            except ValueError:
                liq = False
            out[(r["sym"], r["crowd"], r["kind"])] = (r["ts"], r["score"], liq)
        return out

    def update_progress(self, cid: int, mfe: float, mae: float, ret_now: Optional[float]) -> None:
        self.db.execute("UPDATE calls SET mfe=?, mae=?, ret_now=?, checked_ts=? WHERE id=?",
                        (mfe, mae, ret_now, time.time(), cid))
        self.db.commit()

    def close(self, cid: int, outcome: str, ret_end: Optional[float], t_end: Optional[float],
              mfe: float, mae: float) -> None:
        self.db.execute(
            "UPDATE calls SET status='closed', outcome=?, ret_end=?, ret_now=?, t_end=?, mfe=?, mae=?, "
            "checked_ts=? WHERE id=?",
            (outcome, ret_end, ret_end, t_end, mfe, mae, time.time(), cid),
        )
        self.db.commit()


# ----------------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------------
MENU_KB = {
    "keyboard": [[{"text": "📒 Журнал"}, {"text": "📊 Статистика"}],
                 [{"text": "📡 Статус"}, {"text": "❓ Помощь"}]],
    "resize_keyboard": True,
    "is_persistent": True,
}


class Telegram:
    def __init__(self, session: aiohttp.ClientSession, dry: bool):
        self.s = session
        self.dry = dry

    async def call(self, method: str, payload: Optional[dict] = None, timeout: float = 20):
        if self.dry:
            return None
        url = f"{TG_API}/bot{TG_TOKEN}/{method}"
        for _ in range(2):
            try:
                async with self.s.post(url, json=payload or {},
                                       timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    data = await r.json(content_type=None)
                    if r.status == 429:
                        await asyncio.sleep(data.get("parameters", {}).get("retry_after", 5) + 1)
                        continue
                    if not data.get("ok"):
                        desc = str(data.get("description", ""))
                        if "not modified" not in desc:
                            log.error("Telegram %s: %s", method, desc)
                        return None
                    return data.get("result")
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                # URL с токеном в лог не выводим
                log.error("Telegram %s недоступен: %s", method, type(e).__name__)
                return None
        return None

    async def send(self, text: str, markup: Optional[dict] = None, chat=None):
        if self.dry:
            print(html.unescape(re.sub(r"<[^>]+>", "", text)))
            print("-" * 60)
            return None
        payload = {"chat_id": chat or TG_CHAT, "text": clip(text), "parse_mode": "HTML",
                   "disable_web_page_preview": True}
        if markup:
            payload["reply_markup"] = markup
        return await self.call("sendMessage", payload)

    async def edit(self, chat, message_id: int, text: str, markup: Optional[dict] = None):
        payload = {"chat_id": chat, "message_id": message_id, "text": clip(text),
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        if markup:
            payload["reply_markup"] = markup
        return await self.call("editMessageText", payload)

    async def answer_cb(self, cb_id: str, text: str = ""):
        return await self.call("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})


def alert_markup(cid: int) -> dict:
    return {"inline_keyboard": [[
        {"text": f"🔎 Колл #{cid}", "callback_data": f"d:{cid}:a:0"},
        {"text": "📒 Журнал", "callback_data": "j:a:0"},
    ]]}


def format_alert(r: Result, kind: str, trig: Optional[dict], cid: int) -> str:
    m = r.m
    assert m is not None
    if kind == "trigger":
        head = ("⚡🚀 СКВИЗ НАЧИНАЕТСЯ — выносит шорты (цена растёт)" if r.crowd == "short"
                else "⚡🩸 СКВИЗ НАЧИНАЕТСЯ — выносит лонги (цена падает)")
    else:
        head = ("🚀 Риск ШОРТ-сквиза (шорты перегружены → возможен рост)" if r.crowd == "short"
                else "🩸 Риск ЛОНГ-сквиза (лонги перегружены → возможен слив)")
    lines = [
        head,
        f"<b>{esc(m.sym)}</b> · score <b>{r.score:.1f}/{MAX_SCORE:.1f}</b> · колл #{cid}",
        f"Цена {m.price:g} (4ч {m.price_4h:+.1f}%, 24ч {m.price_24h:+.1f}%)",
        "",
    ]
    if trig:
        lines += ["⚡ " + esc(trig[k]) for k in TRIG_ORDER if k in trig]
        lines.append("")
    lines += ["✅ " + esc(r.flags[k]) for k in FLAG_ORDER if k in r.flags]
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Приложение: сканер + realtime-движок + оценка исходов
# ----------------------------------------------------------------------------
class App:
    def __init__(self, api: Binance, feed: LiveFeed, journal: Journal, tg: Telegram):
        self.api, self.feed, self.journal, self.tg = api, feed, journal, tg
        self.watch: dict[str, Watch] = {}
        self.cands: dict[str, Candidate] = {}
        self.f_int: dict[str, float] = {}
        self.universe: set[str] = set()
        self.universe_ts = 0.0
        self.last_call: dict[tuple, tuple] = journal.latest_by_key()
        self.oi_hist: dict[str, deque] = defaultdict(deque)
        self.promo_cd: dict[str, float] = {}
        self.promote_q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self.last_scan_ts = 0.0
        self.last_scan_info = "ещё не было"
        self.started = time.time()
        self._last_promo_scan = 0.0
        self._bg: set = set()

    # ---------------- отправка коллов ----------------
    async def emit(self, r: Result, kind: str, trig: Optional[dict]) -> int:
        m = r.m
        assert m is not None
        cid = self.journal.add(sym=r.sym, crowd=r.crowd, kind=kind, score=r.score, price=m.price,
                               flags=r.flags, trig=trig or {}, metrics=asdict(m))
        self.last_call[(r.sym, r.crowd, kind)] = (time.time(), r.score, "liq" in r.flags)
        await self.tg.send(format_alert(r, kind, trig, cid), None if self.tg.dry else alert_markup(cid))
        return cid

    # ---------------- сканер (REST) ----------------
    async def scan(self) -> None:
        t0 = time.time()
        api = self.api

        if not self.universe or time.time() - self.universe_ts > 6 * 3600:
            info = await api.get("/fapi/v1/exchangeInfo")
            self.universe = {
                s["symbol"] for s in info["symbols"]
                if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            }
            self.universe_ts = time.time()
            log.info("USDT-перпетуалов на Binance: %d", len(self.universe))

        tick_l, prem_l = await asyncio.gather(
            api.get("/fapi/v1/ticker/24hr"), api.get("/fapi/v1/premiumIndex"))
        try:
            fi_l = await api.get("/fapi/v1/fundingInfo")
        except ApiError:
            fi_l = []
        tick = {x["symbol"]: x for x in tick_l}
        prem = {x["symbol"]: x for x in prem_l}
        self.f_int = {x["symbol"]: float(x.get("fundingIntervalHours") or 8) for x in fi_l}

        rows: list[Candidate] = []
        for sym in self.universe:
            t, p = tick.get(sym), prem.get(sym)
            if not t or not p:
                continue
            try:
                vol = float(t["quoteVolume"])
                if vol < MIN_QUOTE_VOLUME:
                    continue
                mark, idx = float(p["markPrice"]), float(p["indexPrice"])
                f8 = float(p["lastFundingRate"]) * 8 / self.f_int.get(sym, 8)
                rows.append(Candidate(sym, mark, vol, f8, (mark / idx - 1) * 100 if idx else 0.0))
            except (KeyError, ValueError):
                continue

        rows.sort(key=lambda r: r.vol, reverse=True)
        self.cands = {r.sym: r for r in rows}
        top = rows[:TOP_ALWAYS]
        rest = [r for r in rows[TOP_ALWAYS:] if r.f8 >= FUND_LONG or r.f8 <= FUND_SHORT]
        rest.sort(key=lambda r: abs(r.f8 - 0.0001), reverse=True)
        cands = (top + rest)[:MAX_DETAIL]
        log.info("Скан: %d ликвидных монет, на детальный анализ: %d", len(rows), len(cands))

        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(c: Candidate) -> Optional[Metrics]:
            async with sem:
                return await analyze(api, c, self.feed)

        metrics = [m for m in await asyncio.gather(*(worker(c) for c in cands)) if m]
        thin = is_thin_session()

        prelim_min = SEND_SCORE - WEIGHTS["depth"]
        need_depth = [m for m in metrics if best_result(m, thin).score >= prelim_min]

        async def depth_worker(m: Metrics) -> None:
            async with sem:
                d = await fetch_depth_usd(api, m.sym)
                if d is not None and m.oi_usd > 0:
                    m.depth_usd, m.depth_ratio = d, d / m.oi_usd

        await asyncio.gather(*(depth_worker(m) for m in need_depth))

        results = [best_result(m, thin) for m in metrics]
        self.update_watch(results)

        signals = [r for r in results if r.score >= SEND_SCORE and has_skew(r.flags)]
        signals.sort(key=lambda r: r.score, reverse=True)

        now = time.time()
        to_send: list[Result] = []
        for r in signals:
            prev = self.last_call.get((r.sym, r.crowd, "setup"))
            active = "liq" in r.flags
            if (prev is None or now - prev[0] > COOLDOWN_SEC or r.score >= prev[1] + 1.5
                    or (active and not prev[2])):
                to_send.append(r)
        to_send = to_send[:MAX_ALERTS_PER_CYCLE]

        for r in to_send:
            await self.emit(r, "setup", None)
            await asyncio.sleep(1.1)  # лимит Telegram ~1 сообщение/сек в один чат

        self.last_scan_ts = time.time()
        self.last_scan_info = (f"проанализировано {len(metrics)}, сетапов {len(signals)}, "
                               f"отправлено {len(to_send)}, watchlist {len(self.watch)}")
        log.info("Скан за %.0f с: %s", time.time() - t0, self.last_scan_info)

    def update_watch(self, results: list[Result]) -> None:
        now = time.time()
        ranked = [r for r in results if r.score >= WATCH_SCORE and has_skew(r.flags) and r.m]
        ranked.sort(key=lambda r: r.score, reverse=True)
        new = {r.sym: Watch(r.crowd, r.score, r.m, "scan", now) for r in ranked[:WATCH_MAX]}  # type: ignore[arg-type]
        for sym, w in self.watch.items():  # промоут-монеты живут RT_PROMOTE_TTL_SEC
            if w.src == "promo" and now - w.ts < RT_PROMOTE_TTL_SEC and sym not in new:
                new[sym] = w
        self.watch = new
        for sym in [s for s in self.oi_hist if s not in new]:
            del self.oi_hist[sym]

    async def scan_loop(self, once: bool = False) -> None:
        if not once:
            await asyncio.sleep(SCAN_START_DELAY)
        while True:
            t0 = time.time()
            try:
                await self.scan()
            except ApiError as e:
                log.error("Ошибка API: %s", e)
            except Exception:  # noqa: BLE001
                log.exception("Сбой скана")
            if once:
                return
            await asyncio.sleep(max(5, SCAN_INTERVAL_SEC - (time.time() - t0)))

    # ---------------- realtime-движок ----------------
    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    def oi_flush(self, sym: str) -> Optional[float]:
        """Изменение OI (в контрактах, без влияния цены) от максимума окна, %. Только падение: <= 0."""
        dq = self.oi_hist.get(sym)
        if not dq:
            return None
        cut = time.time() - RT_OI_WINDOW_SEC
        vals = [q for t, q in dq if t >= cut]
        if len(vals) < 2:
            return None
        mx = max(vals)
        return (vals[-1] / mx - 1) * 100 if mx > 0 else None

    async def poll_oi(self, sym: str) -> Optional[float]:
        try:
            d = await self.api.get("/fapi/v1/openInterest", {"symbol": sym})
            qty = float(d["openInterest"])
        except (ApiError, KeyError, ValueError, TypeError):
            return None
        dq = self.oi_hist[sym]
        now = time.time()
        dq.append((now, qty))
        while dq and dq[0][0] < now - RT_OI_WINDOW_SEC * 2:
            dq.popleft()
        return qty

    async def oi_loop(self) -> None:
        while True:
            for sym in list(self.watch):
                await self.poll_oi(sym)
            await asyncio.sleep(RT_OI_POLL_SEC)

    def on_frame(self) -> None:
        """Синхронный колбэк: вызывается на КАЖДОМ кадре markPrice (раз в секунду). Без await."""
        now = time.time()
        thin = is_thin_session()

        # 1) триггеры по watchlist
        for sym, w in list(self.watch.items()):
            if now - self.last_call.get((sym, w.crowd, "trigger"), (0.0,))[0] < RT_COOLDOWN_SEC:
                continue
            live = live_metrics(w.m, self.feed, self.f_int)
            tf = trigger_flags(w.crowd, live, self.feed, self.oi_flush(sym))
            if len(tf) < RT_MIN_TRIGGERS:
                continue
            res = evaluate(live, w.crowd, thin)
            if res.score < WATCH_SCORE or not has_skew(res.flags):
                continue
            # cooldown ставим сразу (до await), чтобы следующий кадр не продублировал колл
            self.last_call[(sym, w.crowd, "trigger")] = (now, res.score, "liq" in res.flags)
            log.info("ТРИГГЕР %s (%s): %s", sym, w.crowd, ", ".join(tf))
            self._spawn(self._fire(res, tf))

        # 2) промоут: монеты вне watchlist, где внезапно пошёл импульс или ликвидации
        if now - self._last_promo_scan >= RT_PROMO_SCAN_SEC:
            self._last_promo_scan = now
            for sym in self.cands:
                if sym in self.watch or self.promo_cd.get(sym, 0) > now:
                    continue
                mv = self.feed.move_pct(sym, RT_WINDOW_SEC)
                ll, ls = self.feed.recent(sym, RT_WINDOW_SEC)
                if (mv is not None and abs(mv) >= RT_PROMOTE_MOVE_PCT) or max(ll, ls) >= RT_PROMOTE_LIQ_USD:
                    self.promo_cd[sym] = now + RT_PROMOTE_COOLDOWN_SEC
                    try:
                        self.promote_q.put_nowait(sym)
                    except asyncio.QueueFull:
                        pass

    async def _fire(self, res: Result, tf: dict) -> None:
        try:
            await self.emit(res, "trigger", tf)
        except Exception:  # noqa: BLE001
            log.exception("Не удалось отправить триггер")

    async def promote_worker(self) -> None:
        """Дополучает метрики по REST для монеты, на которой началось движение, и добавляет в watchlist."""
        while True:
            sym = await self.promote_q.get()
            c = self.cands.get(sym)
            if not c or sym in self.watch:
                continue
            try:
                f8 = self.feed.funding_8h(sym, self.f_int)
                c = replace(c, mark=self.feed.mark.get(sym, c.mark), f8=c.f8 if f8 is None else f8)
                m = await analyze(self.api, c, self.feed)
                if not m:
                    continue
                r = best_result(m, is_thin_session())
                if r.score >= WATCH_SCORE and has_skew(r.flags):
                    self.watch[sym] = Watch(r.crowd, r.score, m, "promo", time.time())
                    await self.poll_oi(sym)
                    log.info("Промоут %s → watchlist (%s, score %.1f)", sym, r.crowd, r.score)
            except Exception:  # noqa: BLE001
                log.exception("Промоут %s", sym)

    # ---------------- оценка исходов ----------------
    async def check_outcomes(self) -> None:
        now = time.time()
        for row in self.journal.open_calls():
            age = now - row["ts"]
            if age < 60:
                continue
            try:
                kl = await self.api.get("/fapi/v1/klines", {
                    "symbol": row["sym"], "interval": "1m",
                    "startTime": (int(row["ts"] // 60) + 1) * 60 * 1000,
                    "limit": min(1500, int(age / 60) + 3),
                })
            except ApiError as e:
                log.warning("Исход #%s %s: %s", row["id"], row["sym"], e)
                if age > OUTCOME_HORIZON_SEC + 6 * 3600:  # монета делистнута/недоступна — не держим вечно
                    self.journal.close(row["id"], "flat", 0.0, OUTCOME_HORIZON_SEC / 60, row["mfe"] or 0.0,
                                       row["mae"] or 0.0)
                continue
            res = evaluate_outcome(row["crowd"], row["price"], kl, row["ts"], now)
            if res["outcome"]:
                self.journal.close(row["id"], res["outcome"], res["ret_end"], res["t_end"], res["mfe"], res["mae"])
                log.info("Колл #%s %s → %s (%+.1f%%)", row["id"], row["sym"], res["outcome"], res["ret_end"] or 0)
            else:
                self.journal.update_progress(row["id"], res["mfe"], res["mae"], res["ret_now"])

    async def outcome_loop(self) -> None:
        while True:
            try:
                await self.check_outcomes()
            except Exception:  # noqa: BLE001
                log.exception("Сбой проверки исходов")
            await asyncio.sleep(OUTCOME_CHECK_SEC)


# ----------------------------------------------------------------------------
# Бот: кнопки, журнал, статистика
# ----------------------------------------------------------------------------
FILTER_NAMES = {"a": "все", "o": "открытые", "c": "закрытые", "s": "сетапы", "t": "триггеры"}


class Bot:
    def __init__(self, app: App):
        self.app = app
        self.tg = app.tg
        self.journal = app.journal
        self._warned: set = set()

    def allowed(self, chat_id) -> bool:
        return str(chat_id) == str(TG_CHAT)

    # ---------- представления ----------
    def cur_ret(self, r) -> Optional[float]:
        p = self.app.feed.mark.get(r["sym"])
        if p:
            return (1 if r["crowd"] == "short" else -1) * (p / r["price"] - 1) * 100
        return r["ret_now"]

    def result_line(self, r) -> str:
        mfe, mae = r["mfe"] or 0.0, r["mae"] or 0.0
        ex = f"MFE +{mfe:.1f}% / MAE −{mae:.1f}%"
        if r["status"] == "closed":
            ret = r["ret_end"] if r["ret_end"] is not None else 0.0
            dur = fmt_dur((r["t_end"] or 0) * 60)
            if r["outcome"] == "flat":
                return f"закрыт по времени ({dur}): {ret:+.1f}% · {ex}"
            return f"итог {ret:+.1f}% за {dur} · {ex}"
        cur = self.cur_ret(r)
        cur_s = f"сейчас {cur:+.1f}%" if cur is not None else "ждём данных"
        return f"{cur_s} · идёт {fmt_dur(time.time() - r['ts'])} · {ex}"

    def entry(self, r) -> str:
        return "\n".join([
            f"{OUT_ICON.get(r['outcome'], '⏳')} <b>#{r['id']} {esc(r['sym'])}</b> · "
            f"{CROWD_LABEL.get(r['crowd'], r['crowd'])} · {KIND_LABEL.get(r['kind'], r['kind'])}",
            f"{fmt_ts(r['ts'])} · score {r['score']:.1f} · вход {r['price']:g}",
            self.result_line(r),
        ])

    def journal_view(self, filt: str, page: int):
        filt = filt if filt in Journal.FILTERS else "a"
        rows, total, page, pages = self.journal.page(filt, page, PAGE_SIZE)
        c = self.journal.counts()
        head = (f"📒 <b>Журнал коллов</b> · {FILTER_NAMES[filt]} · стр. {page + 1}/{pages} · найдено {total}\n"
                f"✅ {c['hit']} · ❌ {c['miss']} · ➖ {c['flat']} · ⏳ {c['open']}")
        if rows:
            text = head + "\n\n" + "\n\n".join(self.entry(r) for r in rows)
        else:
            text = head + "\n\nПока пусто — колы появятся, когда монитор найдёт сигнал."

        kb: list = []
        btns = [{"text": f"{OUT_ICON.get(r['outcome'], '⏳')} #{r['id']} {short(r['sym'])}",
                 "callback_data": f"d:{r['id']}:{filt}:{page}"} for r in rows]
        for i in range(0, len(btns), 3):
            kb.append(btns[i:i + 3])
        nav = []
        if page > 0:
            nav.append({"text": "◀️", "callback_data": f"j:{filt}:{page - 1}"})
        nav.append({"text": f"{page + 1}/{pages}", "callback_data": "x"})
        if page < pages - 1:
            nav.append({"text": "▶️", "callback_data": f"j:{filt}:{page + 1}"})
        kb.append(nav)

        def fb(f: str) -> dict:
            return {"text": ("• " if f == filt else "") + FILTER_NAMES[f].capitalize(),
                    "callback_data": f"j:{f}:0"}

        kb.append([fb("a"), fb("o"), fb("c")])
        kb.append([fb("s"), fb("t"), {"text": "🔄", "callback_data": f"j:{filt}:{page}"}])
        return text, {"inline_keyboard": kb}

    def detail_view(self, cid: int, filt: str, page: int):
        back = {"inline_keyboard": [[{"text": "◀️ К журналу", "callback_data": f"j:{filt}:{page}"}]]}
        r = self.journal.get(cid)
        if not r:
            return "Колл не найден.", back
        try:
            flags, trig, m = json.loads(r["flags"]), json.loads(r["trig"]), json.loads(r["metrics"])
        except ValueError:
            flags, trig, m = {}, {}, {}
        sign = 1 if r["crowd"] == "short" else -1
        lines = [
            f"{OUT_ICON.get(r['outcome'], '⏳')} <b>Колл #{r['id']} · {esc(r['sym'])}</b>",
            f"{CROWD_LABEL.get(r['crowd'], r['crowd'])} · {KIND_LABEL.get(r['kind'], r['kind'])} · "
            f"score {r['score']:.1f}/{MAX_SCORE:.1f}",
            f"Время: {fmt_ts(r['ts'])} · вход {r['price']:g}",
        ]
        cur = self.app.feed.mark.get(r["sym"])
        if r["status"] == "open" and cur:
            lines.append(f"Сейчас: {cur:g} ({sign * (cur / r['price'] - 1) * 100:+.2f}% в сторону сквиза)")
        verdict = {"hit": f"✅ HIT — цена прошла +{HIT_PCT:g}% в сторону сквиза раньше, чем −{STOP_PCT:g}% против",
                   "miss": f"❌ MISS — сначала −{STOP_PCT:g}% против сквиза",
                   "flat": f"➖ FLAT — за {OUTCOME_HORIZON_SEC // 3600} ч ни {HIT_PCT:g}%, ни −{STOP_PCT:g}%",
                   None: f"⏳ Открыт: ждём +{HIT_PCT:g}% или −{STOP_PCT:g}% (макс. {OUTCOME_HORIZON_SEC // 3600} ч)"}
        lines.append(verdict.get(r["outcome"], ""))
        lines.append(self.result_line(r))
        if trig:
            lines += ["", "<b>Триггеры (realtime):</b>"] + ["⚡ " + esc(trig[k]) for k in TRIG_ORDER if k in trig]
        lines += ["", "<b>Условия сетапа:</b>"] + ["✅ " + esc(flags[k]) for k in FLAG_ORDER if k in flags]
        if m:
            lines += ["", "<b>Метрики на момент колла:</b>",
                      f"funding {m.get('funding_8h', 0) * 100:+.3f}%/8ч · premium {m.get('premium_pct', 0):+.2f}%",
                      f"OI {usd(m.get('oi_usd', 0))} ({m.get('oi_24h', 0):+.1f}% за 24ч)",
                      f"L/S аккаунты {m.get('glob_ratio', 0):.2f} · топ-трейдеры {m.get('top_ratio', 0):.2f}"]
        return "\n".join(lines), back

    def stats_view(self):
        rows = self.journal.all_rows()
        closed = [r for r in rows if r["status"] == "closed"]
        hit = sum(1 for r in closed if r["outcome"] == "hit")
        miss = sum(1 for r in closed if r["outcome"] == "miss")
        flat = sum(1 for r in closed if r["outcome"] == "flat")

        def rate(sub) -> str:
            n = len(sub)
            h = sum(1 for r in sub if r["outcome"] == "hit")
            return f"{100 * h / n:.0f}% ({h}/{n})" if n else "—"

        lines = [
            "📊 <b>Статистика коллов</b>",
            f"Всего {len(rows)} · закрыто {len(closed)} · открыто {len(rows) - len(closed)}",
            f"✅ {hit} · ❌ {miss} · ➖ {flat}",
            f"Hit-rate: <b>{rate(closed)}</b>" + (f" · среди решённых (без ➖): {100 * hit / (hit + miss):.0f}%"
                                                  if hit + miss else ""),
            f"<i>hit = +{HIT_PCT:g}% в сторону сквиза раньше, чем −{STOP_PCT:g}%, за {OUTCOME_HORIZON_SEC // 3600} ч</i>",
        ]
        if closed:
            lines.append(f"Средний MFE +{sum(r['mfe'] or 0 for r in closed) / len(closed):.1f}% / "
                         f"MAE −{sum(r['mae'] or 0 for r in closed) / len(closed):.1f}%")
            lines += ["", "<b>По типу:</b>"]
            for k in ("setup", "trigger"):
                sub = [r for r in closed if r["kind"] == k]
                if sub:
                    lines.append(f"{KIND_LABEL[k]}: {rate(sub)}")
            lines += ["", "<b>По стороне:</b>"]
            for k in ("short", "long"):
                sub = [r for r in closed if r["crowd"] == k]
                if sub:
                    lines.append(f"{CROWD_LABEL[k]}: {rate(sub)}")
            lines += ["", "<b>По баллу:</b>"]
            for name, lo, hi in (("< 5", 0, 5), ("5–6", 5, 6), ("≥ 6", 6, 99)):
                sub = [r for r in closed if lo <= r["score"] < hi]
                if sub:
                    lines.append(f"{name}: {rate(sub)}")
            agg: dict[str, list] = defaultdict(lambda: [0, 0])
            for r in closed:
                try:
                    keys = list(json.loads(r["flags"])) + list(json.loads(r["trig"]))
                except ValueError:
                    continue
                for k in set(keys):
                    agg[k][0] += 1
                    agg[k][1] += r["outcome"] == "hit"
            shown = [(k, n, h) for k, (n, h) in agg.items() if n >= 3]
            if shown:
                lines += ["", "<b>По признакам</b> (hit-rate, когда признак был):"]
                for k, n, h in sorted(shown, key=lambda x: -x[1]):
                    lines.append(f"{FLAG_NAMES.get(k, k)}: {100 * h / n:.0f}% (n={n})")
        if len(closed) < 30:
            lines += ["", "⚠️ Закрытых коллов меньше 30 — выводы статистически слабые, пороги пока не трогайте."]
        return "\n".join(lines), {"inline_keyboard": [[{"text": "🔄 Обновить", "callback_data": "st"}]]}

    def status_view(self):
        a, f = self.app, self.app.feed
        now = time.time()
        live = f.connected and now - f.last_frame < 15
        ws = (f"🟢 подключён · монет {f.n_symbols} · кадр {fmt_dur(now - f.last_frame)} назад"
              if live else "🔴 нет данных (переподключение)")
        watch = sorted(a.watch.items(), key=lambda kv: kv[1].score, reverse=True)
        wl = ", ".join(f"{short(s)}{'↑' if w.crowd == 'short' else '↓'}{w.score:.1f}" for s, w in watch[:10])
        lines = [
            "📡 <b>Статус</b>",
            f"Поток Binance: {ws}",
            f"Realtime-режим: {'включён' if RT_ENABLED else 'выключен'}",
            f"Watchlist ({len(a.watch)}): {wl or 'пусто'}",
            "<i>↑ шорты под угрозой, ↓ лонги под угрозой</i>",
            f"Последний скан: {fmt_dur(now - a.last_scan_ts) + ' назад' if a.last_scan_ts else 'ещё не было'}"
            f" — {esc(a.last_scan_info)}",
            f"Коллов за 24 ч: сетапов {self.journal.count_since(now - 86400, 'setup')}, "
            f"триггеров {self.journal.count_since(now - 86400, 'trigger')} · открытых {self.journal.counts()['open']}",
            f"Пороги: сетап ≥ {SEND_SCORE:g}, watchlist ≥ {WATCH_SCORE:g}, триггер: ≥ {RT_MIN_TRIGGERS} признаков "
            f"(импульс ≥ {RT_MOVE_PCT:g}% за {RT_WINDOW_SEC // 60 or 1} мин, ликвидации ≥ {usd(RT_LIQ_USD)})",
            f"Аптайм: {fmt_dur(now - a.started)}",
        ]
        return "\n".join(lines), {"inline_keyboard": [[{"text": "🔄 Обновить", "callback_data": "ss"}]]}

    def help_text(self) -> str:
        return (
            "❓ <b>Справка</b>\n"
            "📒 <b>Журнал</b> — все сигналы с исходами, фильтры, карточка каждого колла\n"
            "📊 <b>Статистика</b> — hit-rate по типам, баллам и признакам\n"
            "📡 <b>Статус</b> — поток Binance, watchlist, последний скан\n\n"
            "<b>Колл</b> — каждый отправленный сигнал.\n"
            "📋 <b>сетап</b> — условия для сквиза созрели (скан раз в несколько минут).\n"
            "⚡ <b>триггер</b> — сквиз начинается прямо сейчас (realtime, по секундному потоку).\n\n"
            f"Исход: ✅ hit — цена прошла +{HIT_PCT:g}% в сторону сквиза раньше, чем −{STOP_PCT:g}% против; "
            f"❌ miss — наоборот; ➖ flat — за {OUTCOME_HORIZON_SEC // 3600} ч ни то ни другое; ⏳ — ещё идёт.\n"
            "MFE / MAE — максимальное движение в пользу / против колла."
        )

    # ---------- обработка апдейтов ----------
    async def on_msg(self, m: dict) -> None:
        chat = m["chat"]["id"]
        if not self.allowed(chat):
            if chat not in self._warned:
                self._warned.add(chat)
                log.warning("Сообщение из неразрешённого чата %s — игнорирую", chat)
            return
        text = (m.get("text") or "").strip().lower()
        cmd = text.split()[0].split("@")[0] if text.startswith("/") and text.split() else text
        if cmd in ("/start", "/menu"):
            await self.tg.send("👋 Монитор сквизов. Кнопки внизу: журнал коллов, статистика, статус.", MENU_KB, chat)
        elif cmd in ("/journal", "📒 журнал", "журнал"):
            t, kb = self.journal_view("a", 0)
            await self.tg.send(t, kb, chat)
        elif cmd in ("/stats", "📊 статистика", "статистика"):
            t, kb = self.stats_view()
            await self.tg.send(t, kb, chat)
        elif cmd in ("/status", "📡 статус", "статус"):
            t, kb = self.status_view()
            await self.tg.send(t, kb, chat)
        elif cmd in ("/help", "❓ помощь", "помощь"):
            await self.tg.send(self.help_text(), MENU_KB, chat)

    async def on_cb(self, cb: dict) -> None:
        msg = cb.get("message") or {}
        chat = (msg.get("chat") or {}).get("id")
        if chat is None or not self.allowed(chat):
            return
        data = cb.get("data") or ""
        p = data.split(":")
        try:
            if p[0] == "j":
                view = self.journal_view(p[1], int(p[2]))
            elif p[0] == "d":
                view = self.detail_view(int(p[1]), p[2] if p[2] in Journal.FILTERS else "a", int(p[3]))
            elif p[0] == "st":
                view = self.stats_view()
            elif p[0] == "ss":
                view = self.status_view()
            else:
                await self.tg.answer_cb(cb["id"])
                return
        except (IndexError, ValueError):
            await self.tg.answer_cb(cb["id"])
            return
        await self.tg.answer_cb(cb["id"], "Обновлено" if p[0] in ("st", "ss") else "")
        if msg.get("message_id") is not None:
            await self.tg.edit(chat, msg["message_id"], view[0], view[1])

    async def run(self) -> None:
        await self.tg.call("deleteWebhook", {"drop_pending_updates": False})
        await self.tg.call("setMyCommands", {"commands": [
            {"command": "journal", "description": "Журнал коллов"},
            {"command": "stats", "description": "Статистика"},
            {"command": "status", "description": "Статус монитора"},
            {"command": "help", "description": "Справка"},
        ]})
        offset = 0
        while True:
            ups = await self.tg.call("getUpdates", {
                "offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}, timeout=40)
            if ups is None:
                await asyncio.sleep(3)
                continue
            for u in ups:
                offset = max(offset, u["update_id"] + 1)
                try:
                    if "callback_query" in u:
                        await self.on_cb(u["callback_query"])
                    elif "message" in u:
                        await self.on_msg(u["message"])
                except Exception:  # noqa: BLE001
                    log.exception("Ошибка обработки апдейта")


# ----------------------------------------------------------------------------
# Запуск
# ----------------------------------------------------------------------------
async def supervise(name: str, factory: Callable):
    """Перезапускает фоновую задачу, если она упала с ошибкой."""
    while True:
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("Задача '%s' упала, перезапуск через 5 с", name)
            await asyncio.sleep(5)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="один скан + проверка исходов и выход")
    ap.add_argument("--dry-run", action="store_true", help="сигналы в консоль вместо Telegram (без бота)")
    args = ap.parse_args()

    if not args.dry_run and (not TG_TOKEN or not TG_CHAT):
        sys.exit("Задайте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID (или используйте --dry-run)")

    journal = Journal(DB_PATH)
    async with aiohttp.ClientSession() as session:
        api, feed, tg = Binance(session), LiveFeed(), Telegram(session, args.dry_run)
        app = App(api, feed, journal, tg)

        if args.once:
            await app.scan_loop(once=True)
            await app.check_outcomes()
            return

        if RT_ENABLED:
            feed.on_frame = app.on_frame
        await tg.send(
            f"✅ Монитор сквизов запущен (realtime {'вкл' if RT_ENABLED else 'выкл'}). "
            f"Сетап ≥ {SEND_SCORE:.1f}/{MAX_SCORE:.1f}, скан раз в {SCAN_INTERVAL_SEC // 60} мин. "
            "Журнал коллов — кнопка «📒 Журнал».", MENU_KB)

        tasks = [
            supervise("feed", lambda: feed.run(session)),
            supervise("scan", app.scan_loop),
            supervise("outcomes", app.outcome_loop),
        ]
        if RT_ENABLED:
            tasks += [supervise("oi", app.oi_loop), supervise("promote", app.promote_worker)]
        if not args.dry_run:
            tasks.append(supervise("bot", Bot(app).run))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
