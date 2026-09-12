
import os
import json
import time
import uuid
import threading
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import upstox_client

load_dotenv()

IST = ZoneInfo("Asia/Kolkata")

TOKEN = os.getenv("UPSTOX_TOKEN", "").strip()
DATABASE_URL = os.getenv("NEON_DATABASE_URL", "").strip()

POLL_FLUSH_SECONDS = int(os.getenv("AGGRESSION_FLUSH_SECONDS", "180"))
UNIVERSE_WAIT_SECONDS = int(os.getenv("UNIVERSE_WAIT_SECONDS", "15"))
MARKET_START = dtime(
    int(os.getenv("MONEY_FLOW_FREEZE_HOUR", "9")),
    int(os.getenv("MONEY_FLOW_FREEZE_MINUTE", "20")),
)
MARKET_END = dtime(15, 20)
EXPECTED_UNIVERSE_SIZE = int(os.getenv("MONEY_FLOW_TOP_N", "50"))

if not TOKEN:
    raise RuntimeError("UPSTOX_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL is missing")


def log(msg):
    print(f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST | {msg}", flush=True)


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS public.futures_aggression_snapshots (
        id BIGSERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        money_flow_rank INTEGER,
        symbol TEXT NOT NULL,
        future_instrument_key TEXT NOT NULL,

        ltp NUMERIC,
        last_trade_time TIMESTAMPTZ,
        last_trade_qty BIGINT,
        volume_traded BIGINT,
        open_interest BIGINT,

        best_bid_price NUMERIC,
        best_bid_qty BIGINT,
        best_ask_price NUMERIC,
        best_ask_qty BIGINT,

        depth_bid_qty BIGINT,
        depth_ask_qty BIGINT,
        book_imbalance NUMERIC,
        total_buy_qty BIGINT,
        total_sell_qty BIGINT,
        total_qty_imbalance NUMERIC,

        aggressive_buy_qty BIGINT NOT NULL DEFAULT 0,
        aggressive_sell_qty BIGINT NOT NULL DEFAULT 0,
        unclassified_trade_qty BIGINT NOT NULL DEFAULT 0,
        trade_delta BIGINT NOT NULL DEFAULT 0,
        delta_pct NUMERIC,

        price_change_3m_pct NUMERIC,
        oi_change_3m BIGINT,
        oi_change_3m_pct NUMERIC,
        price_change_t0_pct NUMERIC,
        oi_change_t0_pct NUMERIC,

        aggression_state TEXT,
        tick_count INTEGER NOT NULL DEFAULT 0,
        classified_trade_count INTEGER NOT NULL DEFAULT 0,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        UNIQUE (trading_date, ts, symbol)
    );

    CREATE INDEX IF NOT EXISTS idx_futures_aggression_date_symbol_ts
        ON public.futures_aggression_snapshots(trading_date, symbol, ts);

    -- Backward-compatible migration for existing Neon tables.
    ALTER TABLE public.futures_aggression_snapshots
        ADD COLUMN IF NOT EXISTS price_change_t0_pct NUMERIC;
    ALTER TABLE public.futures_aggression_snapshots
        ADD COLUMN IF NOT EXISTS oi_change_t0_pct NUMERIC;
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def load_today_universe():
    sql = """
    SELECT trading_date, rank, symbol, future_instrument_key,
           future_price, future_oi
    FROM public.money_flow_universe
    WHERE trading_date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
      AND future_instrument_key IS NOT NULL
    ORDER BY rank;
    """
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def to_num(v, default=0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def to_int(v, default=0):
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def ms_to_dt(v):
    try:
        if not v:
            return None
        return datetime.fromtimestamp(int(v) / 1000.0, tz=ZoneInfo("UTC"))
    except Exception:
        return None


def obj_to_dict(obj):
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # protobuf/upstox SDK objects generally expose attributes
    out = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            val = getattr(obj, name)
        except Exception:
            continue
        if callable(val):
            continue
        out[name] = val
    return out


def pick(d, *names, default=None):
    if not isinstance(d, dict):
        d = obj_to_dict(d)
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return default


def extract_market_ff(feed):
    d = obj_to_dict(feed)

    # SDK decoded structures may use either fullFeed/ff and marketFF.
    ff = pick(d, "fullFeed", "ff", "full_feed")
    ff = obj_to_dict(ff)
    market = pick(ff, "marketFF", "market_ff", "marketFf")
    if market is None:
        # Sometimes marketFF is directly available.
        market = pick(d, "marketFF", "market_ff", "marketFf")
    return obj_to_dict(market)


def extract_tick(feed):
    market = extract_market_ff(feed)
    if not market:
        return None

    ltpc = obj_to_dict(pick(market, "ltpc", default={}))
    ltp = to_num(pick(ltpc, "ltp"))
    ltt = pick(ltpc, "ltt")
    ltq = to_int(pick(ltpc, "ltq"))

    level = obj_to_dict(pick(market, "marketLevel", "market_level", default={}))
    quotes = pick(level, "bidAskQuote", "bid_ask_quote", default=[]) or []
    quotes = list(quotes)

    depth_bid = 0
    depth_ask = 0
    best_bid_p = best_ask_p = None
    best_bid_q = best_ask_q = 0

    for idx, q in enumerate(quotes[:5]):
        qd = obj_to_dict(q)
        bp = to_num(pick(qd, "bidP", "bp"), None)
        ap = to_num(pick(qd, "askP", "ap"), None)
        bq = to_int(pick(qd, "bidQ", "bq"))
        aq = to_int(pick(qd, "askQ", "aq"))
        depth_bid += bq
        depth_ask += aq
        if idx == 0:
            best_bid_p, best_ask_p = bp, ap
            best_bid_q, best_ask_q = bq, aq

    vtt = to_int(pick(market, "vtt"))
    oi = to_int(pick(market, "oi"))
    tbq = to_int(pick(market, "tbq"))
    tsq = to_int(pick(market, "tsq"))

    return {
        "ltp": ltp,
        "ltt": ms_to_dt(ltt),
        "ltt_raw": to_int(ltt),
        "ltq": ltq,
        "vtt": vtt,
        "oi": oi,
        "best_bid_price": best_bid_p,
        "best_bid_qty": best_bid_q,
        "best_ask_price": best_ask_p,
        "best_ask_qty": best_ask_q,
        "depth_bid_qty": depth_bid,
        "depth_ask_qty": depth_ask,
        "tbq": tbq,
        "tsq": tsq,
    }


class AggressionCollector:
    def __init__(self, universe):
        self.universe = universe
        self.by_key = {r["future_instrument_key"]: r for r in universe}
        self.lock = threading.Lock()
        self.last_tick = {}
        self.bucket = {}
        self.previous_flush = {}

    def reset_bucket(self, key):
        self.bucket[key] = {
            "aggressive_buy_qty": 0,
            "aggressive_sell_qty": 0,
            "unclassified_trade_qty": 0,
            "tick_count": 0,
            "classified_trade_count": 0,
            "latest": None,
        }

    def on_message(self, message):
        # SDK callback may deliver dict-like decoded payload.
        msg = obj_to_dict(message)
        feeds = pick(msg, "feeds", default={}) or {}
        feeds = obj_to_dict(feeds)

        with self.lock:
            for key, feed in feeds.items():
                if key not in self.by_key:
                    continue

                tick = extract_tick(feed)
                if not tick:
                    continue

                if key not in self.bucket:
                    self.reset_bucket(key)

                b = self.bucket[key]
                prev = self.last_tick.get(key)
                b["tick_count"] += 1

                # Only classify a genuinely new trade (new LTT).
                is_new_trade = (
                    tick["ltq"] > 0 and
                    tick["ltt_raw"] > 0 and
                    (prev is None or tick["ltt_raw"] != prev.get("ltt_raw"))
                )

                if is_new_trade:
                    # Use the prior visible best bid/ask where available to avoid
                    # classifying against a book already changed by the trade.
                    ref = prev or tick
                    bid = ref.get("best_bid_price")
                    ask = ref.get("best_ask_price")
                    px = tick["ltp"]
                    qty = tick["ltq"]

                    side = None
                    if ask is not None and px >= ask:
                        side = "BUY"
                    elif bid is not None and px <= bid:
                        side = "SELL"
                    elif bid is not None and ask is not None:
                        mid = (bid + ask) / 2.0
                        if px > mid:
                            side = "BUY"
                        elif px < mid:
                            side = "SELL"

                    if side == "BUY":
                        b["aggressive_buy_qty"] += qty
                        b["classified_trade_count"] += 1
                    elif side == "SELL":
                        b["aggressive_sell_qty"] += qty
                        b["classified_trade_count"] += 1
                    else:
                        b["unclassified_trade_qty"] += qty

                b["latest"] = tick
                self.last_tick[key] = tick

    def flush(self):
        now = datetime.now(IST).replace(second=0, microsecond=0)
        rows = []

        with self.lock:
            for key, meta in self.by_key.items():
                b = self.bucket.get(key)
                if not b or not b["latest"]:
                    continue

                t = b["latest"]
                prev = self.previous_flush.get(key)

                buy = b["aggressive_buy_qty"]
                sell = b["aggressive_sell_qty"]
                delta = buy - sell
                classified_qty = buy + sell
                delta_pct = (delta / classified_qty * 100.0) if classified_qty else None

                depth_total = t["depth_bid_qty"] + t["depth_ask_qty"]
                book_imb = (
                    (t["depth_bid_qty"] - t["depth_ask_qty"]) / depth_total * 100.0
                    if depth_total else None
                )

                total_total = t["tbq"] + t["tsq"]
                total_imb = (
                    (t["tbq"] - t["tsq"]) / total_total * 100.0
                    if total_total else None
                )

                price_change = None
                oi_change = None
                oi_change_pct = None
                if prev:
                    if prev["ltp"]:
                        price_change = (t["ltp"] / prev["ltp"] - 1.0) * 100.0
                    oi_change = t["oi"] - prev["oi"]
                    if prev["oi"]:
                        oi_change_pct = oi_change / prev["oi"] * 100.0

                # Cumulative movement from the stock's frozen daily universe
                # baseline. These are the stock equivalents of the index T0
                # fields and remain comparable across differently priced names.
                baseline_price = to_num(meta.get("future_price"), None)
                baseline_oi = to_int(meta.get("future_oi"), 0)
                price_change_t0_pct = (
                    (t["ltp"] / baseline_price - 1.0) * 100.0
                    if baseline_price else None
                )
                oi_change_t0_pct = (
                    (t["oi"] / baseline_oi - 1.0) * 100.0
                    if baseline_oi else None
                )

                state = "NEUTRAL"
                if price_change is not None and oi_change_pct is not None:
                    if price_change < 0 and oi_change_pct > 0:
                        state = "NEW SHORT BUILD"
                    elif price_change > 0 and oi_change_pct > 0:
                        state = "NEW LONG BUILD"
                    elif price_change > 0 and oi_change_pct < 0:
                        state = "SHORT COVERING"
                    elif price_change < 0 and oi_change_pct < 0:
                        state = "LONG UNWINDING"

                rows.append((
                    now.date(), now, meta["rank"], meta["symbol"], key,
                    t["ltp"], t["ltt"], t["ltq"], t["vtt"], t["oi"],
                    t["best_bid_price"], t["best_bid_qty"],
                    t["best_ask_price"], t["best_ask_qty"],
                    t["depth_bid_qty"], t["depth_ask_qty"], book_imb,
                    t["tbq"], t["tsq"], total_imb,
                    buy, sell, b["unclassified_trade_qty"], delta, delta_pct,
                    price_change, oi_change, oi_change_pct,
                    price_change_t0_pct, oi_change_t0_pct,
                    state, b["tick_count"], b["classified_trade_count"]
                ))

                self.previous_flush[key] = dict(t)
                self.reset_bucket(key)

        if not rows:
            return

        sql = """
        INSERT INTO public.futures_aggression_snapshots (
            trading_date, ts, money_flow_rank, symbol, future_instrument_key,
            ltp, last_trade_time, last_trade_qty, volume_traded, open_interest,
            best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
            depth_bid_qty, depth_ask_qty, book_imbalance,
            total_buy_qty, total_sell_qty, total_qty_imbalance,
            aggressive_buy_qty, aggressive_sell_qty, unclassified_trade_qty,
            trade_delta, delta_pct,
            price_change_3m_pct, oi_change_3m, oi_change_3m_pct,
            price_change_t0_pct, oi_change_t0_pct,
            aggression_state, tick_count, classified_trade_count
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s
        )
        ON CONFLICT (trading_date, ts, symbol) DO UPDATE SET
            ltp = EXCLUDED.ltp,
            last_trade_time = EXCLUDED.last_trade_time,
            last_trade_qty = EXCLUDED.last_trade_qty,
            volume_traded = EXCLUDED.volume_traded,
            open_interest = EXCLUDED.open_interest,
            best_bid_price = EXCLUDED.best_bid_price,
            best_bid_qty = EXCLUDED.best_bid_qty,
            best_ask_price = EXCLUDED.best_ask_price,
            best_ask_qty = EXCLUDED.best_ask_qty,
            depth_bid_qty = EXCLUDED.depth_bid_qty,
            depth_ask_qty = EXCLUDED.depth_ask_qty,
            book_imbalance = EXCLUDED.book_imbalance,
            total_buy_qty = EXCLUDED.total_buy_qty,
            total_sell_qty = EXCLUDED.total_sell_qty,
            total_qty_imbalance = EXCLUDED.total_qty_imbalance,
            aggressive_buy_qty = EXCLUDED.aggressive_buy_qty,
            aggressive_sell_qty = EXCLUDED.aggressive_sell_qty,
            unclassified_trade_qty = EXCLUDED.unclassified_trade_qty,
            trade_delta = EXCLUDED.trade_delta,
            delta_pct = EXCLUDED.delta_pct,
            price_change_3m_pct = EXCLUDED.price_change_3m_pct,
            oi_change_3m = EXCLUDED.oi_change_3m,
            oi_change_3m_pct = EXCLUDED.oi_change_3m_pct,
            price_change_t0_pct = EXCLUDED.price_change_t0_pct,
            oi_change_t0_pct = EXCLUDED.oi_change_t0_pct,
            aggression_state = EXCLUDED.aggression_state,
            tick_count = EXCLUDED.tick_count,
            classified_trade_count = EXCLUDED.classified_trade_count;
        """
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        log(f"Wrote {len(rows)} futures aggression rows.")

def wait_for_universe():
    while True:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            log("Weekend; sleeping.")
            time.sleep(300)
            continue
        if now.time() < MARKET_START:
            log(f"Waiting for {MARKET_START.strftime('%H:%M')} Money Flow freeze...")
            time.sleep(30)
            continue
        rows = load_today_universe()
        if len(rows) == EXPECTED_UNIVERSE_SIZE:
            ranks = [int(r["rank"]) for r in rows]
            symbols = {r["symbol"] for r in rows}
            if ranks == list(range(1, EXPECTED_UNIVERSE_SIZE + 1)) and len(symbols) == EXPECTED_UNIVERSE_SIZE:
                log(f"Loaded verified Top {EXPECTED_UNIVERSE_SIZE} Money Flow universe.")
                return rows
            log("Universe rows exist but rank/symbol verification failed; retrying...")
            time.sleep(UNIVERSE_WAIT_SECONDS)
            continue
        if rows:
            log(
                f"Universe is incomplete ({len(rows)}/{EXPECTED_UNIVERSE_SIZE}); "
                "waiting for the atomic freeze to complete..."
            )
        else:
            log("Today's Money Flow universe not frozen yet; retrying...")
        time.sleep(UNIVERSE_WAIT_SECONDS)

def main():
    ensure_table()
    universe = wait_for_universe()
    collector = AggressionCollector(universe)

    config = upstox_client.Configuration()
    config.access_token = TOKEN
    api_client = upstox_client.ApiClient(config)

    keys = [r["future_instrument_key"] for r in universe]
    streamer = upstox_client.MarketDataStreamerV3(api_client, keys, "full")
    streamer.on("message", collector.on_message)
    # Upstox SDK versions can pass zero, one, or two callback arguments.
    # Accept all variants so normal disconnects do not raise callback errors.
    streamer.on(
        "open",
        lambda *args: log(
            f"Upstox V3 stream connected; subscribed to {len(keys)} futures in FULL mode."
        ),
    )
    streamer.on(
        "error",
        lambda *args: log(
            f"STREAM ERROR: {args[-1] if args else 'Unknown error'}"
        ),
    )
    streamer.on(
        "close",
        lambda *args: log("Upstox V3 stream closed."),
    )

    streamer.connect()

    last_flush = time.monotonic()
    try:
        while True:
            now = datetime.now(IST)
            if now.time() > MARKET_END:
                collector.flush()
                log("Market collection window complete.")
                break
            if time.monotonic() - last_flush >= POLL_FLUSH_SECONDS:
                collector.flush()
                last_flush = time.monotonic()
            time.sleep(1)
    finally:
        try:
            streamer.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    main()
