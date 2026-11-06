"""
MojidSignalBot v2 — SIGNALS ONLY. Never places an order.

Drives PairEngine exactly the way runner.py does:
  strict_rules=True, practice_zone=True, rsi_enabled=False,
  reversal_enabled=True, momentum_enabled=True
  closed candle -> step(); live quote -> confirm_fcb()/confirm_rsi();
  take_entries() -> same dispatch checks as runner.dispatch_entries();
  one open signal at a time across all pairs (runner: ONE_TRADE_AT_A_TIME).

There is no broker trade, so each signal is settled from its execution
candle's close vs the price when the signal was sent, then fed back through
broker_settled() so pole continuation and loss reversals work as in the bot.

Expiry test (logging only): every signal is also checked at 1, 2, 3 and 5
minutes from the same 1m candles. Results are kept per strategy in
expiry_stats.json and shown in /status. Signals and timers are unchanged.
"""

import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

import iq_feed
from strategy import PairEngine
from pair_selection import DEFAULT_COUNT, select_pairs
from entry_confirmation import ENTRY_WINDOW_SECONDS, number
from telegram_alerts import TelegramAlerts, pair_flags, format_signal_time  # your Replit format

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = str(os.environ["TELEGRAM_CHAT_ID"])
CHANNEL_ID = str(os.environ.get("TELEGRAM_CHANNEL_ID", "")).strip()
TIMEZONE = os.environ.get("TELEGRAM_TIMEZONE", "Africa/Lagos")
MAX_RUNTIME_MIN = float(os.environ.get("MAX_RUNTIME_MIN", "345"))
POLL_SECONDS = 0.2           # per-pair worker poll (each pair has its own thread)
PAIR_CHECK_SECONDS = 30      # main.PAIR_CHECK_SECONDS
FEED_STALL_SECONDS = 180
PREALERT_SECOND = 50         # "get ready" ~10s before the next candle opens
ACTIVE_COUNT = DEFAULT_COUNT
WAT = timezone(timedelta(hours=1))
EXPIRIES = (1, 2, 3, 5)      # minutes checked in the background expiry test
STATS_FILE = os.environ.get("EXPIRY_STATS_FILE", "expiry_stats.json")

TG = f"https://api.telegram.org/bot{TOKEN}"
KEYBOARD = {"keyboard": [[{"text": "/status"}]], "resize_keyboard": True}
KIND_LABEL = {
    "FCB": "FCB breakout", "pole": "Pole continuation",
    "RSI reversal": "RSI reversal", "LOSS reversal": "Loss reversal",
    "RSI momentum": "RSI momentum",
}

stop = threading.Event()
lock = threading.RLock()
active_pairs, engines, last_seen = [], {}, {}
open_trade = None
workers = {}                 # pair -> stop Event for that pair's thread
prealerts = {}               # pair -> {"side", "a_time", "signaled"}
current_api = None
stats = {"signals": 0, "win": 0, "loss": 0, "equal": 0}
last_new_candle = time.time()
started_at = time.time()
shadows = []                 # signals still waiting for their 2/3/5 minute results


# ---------------- Expiry test (logging only) ----------------

def load_expiry_stats():
    try:
        with open(STATS_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


expiry_stats = load_expiry_stats()   # {kind: {"1": {"win","loss","equal"}, ...}}


def save_expiry_stats():
    try:
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(expiry_stats, f)
        os.replace(tmp, STATS_FILE)
    except Exception as e:
        print(f"[expiry stats save error] {e}")


def track_expiries(pair, entry, price):
    if not number(price) or not number(entry.get("execution_candle")):
        return
    shadows.append({"pair": pair, "kind": entry.get("kind") or "other", "side": entry["side"],
                    "price": price, "slot": int(entry["execution_candle"]), "res": {}})


def update_expiries(pair, candle):
    """An N-minute expiry is decided by the close of the candle opening at slot + (N-1) minutes."""
    if not number(candle.get("time")):
        return
    done = []
    for s in shadows:
        if s["pair"] != pair:
            continue
        for n in EXPIRIES:
            key = str(n)
            if key in s["res"]:
                continue
            target = s["slot"] + (n - 1) * 60
            if candle["time"] == target and number(candle.get("close")):
                close = candle["close"]
                if close == s["price"]:
                    s["res"][key] = "equal"
                elif (close > s["price"]) == (s["side"] == "CALL"):
                    s["res"][key] = "win"
                else:
                    s["res"][key] = "loss"
            elif candle["time"] > target:
                s["res"][key] = "missed"   # candle gap: never guess
        if len(s["res"]) == len(EXPIRIES):
            done.append(s)
    for s in done:
        shadows.remove(s)
        per_kind = expiry_stats.setdefault(s["kind"], {})
        for key, result in s["res"].items():
            if result == "missed":
                continue
            bucket = per_kind.setdefault(key, {"win": 0, "loss": 0, "equal": 0})
            bucket[result] += 1
    if done:
        save_expiry_stats()


def expiry_text():
    if not expiry_stats:
        return "Expiry test: no completed signals yet"
    lines = ["Expiry test (win rate, decided signals):"]
    totals = {}
    for kind in sorted(expiry_stats):
        parts = []
        for n in EXPIRIES:
            b = expiry_stats[kind].get(str(n), {})
            w, l = b.get("win", 0), b.get("loss", 0)
            t = totals.setdefault(str(n), [0, 0])
            t[0] += w
            t[1] += l
            parts.append(f"{n}m {w / (w + l) * 100:.0f}% ({w + l})" if w + l else f"{n}m –")
        lines.append(f"• {KIND_LABEL.get(kind, kind)}: " + " | ".join(parts))
    parts = []
    for n in EXPIRIES:
        w, l = totals.get(str(n), [0, 0])
        parts.append(f"{n}m {w / (w + l) * 100:.0f}% ({w + l})" if w + l else f"{n}m –")
    lines.append("• All: " + " | ".join(parts))
    return "\n".join(lines)


# ---------------- Telegram ----------------

def send_to(chat_id, text, keyboard=False):
    body = {"chat_id": chat_id, "text": text}
    if keyboard:
        body["reply_markup"] = KEYBOARD
    try:
        r = requests.post(f"{TG}/sendMessage", json=body, timeout=10)
        if not r.json().get("ok"):
            print(f"[telegram] rejected for {'channel' if chat_id == CHANNEL_ID else 'chat'}: "
                  f"{r.json().get('description')}")
    except Exception as e:
        print(f"[telegram send error] {e}")


def send(text):
    """Private control chat: status, results, alerts."""
    send_to(CHAT_ID, text, keyboard=True)


def broadcast_plain(text):
    def run():
        if CHANNEL_ID:
            send_to(CHANNEL_ID, text)
        if CHAT_ID != CHANNEL_ID:
            send_to(CHAT_ID, text, keyboard=True)
    threading.Thread(target=run, daemon=True).start()


def broadcast_signal(text, slot):
    """Channel first, then private chat. Never send after the entry window
    (same rule as telegram_alerts.signal_window_open)."""
    def run():
        late = time.time() - slot
        if late > ENTRY_WINDOW_SECONDS:
            send(f"⌛ Signal dropped — would arrive {late:.0f}s after candle open "
                 f"(window {ENTRY_WINDOW_SECONDS:g}s)")
            print(f"[signal] dropped, {late:.1f}s late")
            return
        if CHANNEL_ID:
            send_to(CHANNEL_ID, text)
        if CHAT_ID != CHANNEL_ID:
            send_to(CHAT_ID, f"{text}\n⏱ Sent {time.time() - slot:.1f}s after candle open",
                    keyboard=True)
        print(f"[signal] delivered {time.time() - slot:.1f}s after candle open")
    threading.Thread(target=run, daemon=True).start()


def wat(ts):
    return datetime.fromtimestamp(ts, tz=WAT).strftime("%H:%M")


def status_text():
    with lock:
        mins_up = int((time.time() - started_at) / 60)
        lines = [
            "📊 Signal bot (signals only, no trades placed)",
            f"Up {mins_up} min | run ends in {int(MAX_RUNTIME_MIN - mins_up)} min",
            f"Last candle: {int(time.time() - last_new_candle)}s ago",
            f"Signals: {stats['signals']} | W {stats['win']} / L {stats['loss']} / ="
            f" {stats['equal']} (from candle closes)",
            f"Open signal: {open_trade['pair'] + ' ' + open_trade['entry']['side'] if open_trade else 'none'}",
            expiry_text(),
            "Pairs:",
        ]
        for p in active_pairs:
            eng = engines.get(p)
            if eng:
                lines.append(f"• {p} | {eng.trend or '-'} | {eng.reason}"
                             f" | REV: {eng.reversal_status} | MOM: {eng.momentum_status}")
    return "\n".join(lines)


def command_loop():
    offset = None
    try:
        r = requests.get(f"{TG}/getUpdates", params={"offset": -1, "timeout": 0}, timeout=15).json()
        if r.get("result"):
            offset = r["result"][-1]["update_id"] + 1
    except Exception as e:
        print(f"[telegram init error] {e}")
    while not stop.is_set():
        try:
            resp = requests.get(f"{TG}/getUpdates",
                                params={"offset": offset, "timeout": 25}, timeout=35)
            if resp.status_code == 409:
                print("[telegram] 409 Conflict: another instance is polling. Exiting.")
                stop.set()
                return
            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                if str(msg.get("chat", {}).get("id")) != CHAT_ID:
                    continue
                text = (msg.get("text") or "").strip().lower()
                if text.startswith("/status") or text.startswith("/start"):
                    send(status_text())
        except Exception as e:
            print(f"[telegram poll error] {e}")
            time.sleep(5)


# ---------------- Engines (same flags as runner.sync_active_pairs) ----------------

def configure(eng):
    eng.strict_rules = True
    eng.practice_zone = True
    eng.rsi_enabled = False
    eng.reversal_enabled = True
    eng.momentum_enabled = True


def release_open_trade(pair):
    """A removed pair's open signal is released without a result (no follow-ups)."""
    global open_trade
    if open_trade and open_trade["pair"] == pair:
        eng = engines.get(pair)
        if eng:
            with eng.lock:
                eng.broker_settled(open_trade["token"], "equal", time.time(),
                                   allow_entries=False, settled_entry=open_trade["entry"])
        open_trade = None


def sync_pairs(api):
    available = iq_feed.get_open_pairs(api, iq_feed.PAIR_POOL)
    previous = list(active_pairs)
    desired = select_pairs(previous, available, iq_feed.PAIR_POOL, ACTIVE_COUNT)
    removed = [p for p in previous if p not in desired]
    added = [p for p in desired if p not in previous]
    for p in removed:
        if p in workers:
            workers.pop(p).set()
        with lock:
            active_pairs.remove(p)
            release_open_trade(p)
            shadows[:] = [s for s in shadows if s["pair"] != p]
        if p in engines:
            with engines[p].lock:
                engines[p].discard_candidates()
        iq_feed.stop_stream(api, p)
    for p in added:
        iq_feed.start_stream(api, p)
    for p in added:
        history = iq_feed.load_history(api, p)
        eng = engines.get(p) or PairEngine(
            strict_rules=True, reversal_enabled=True, practice_zone=True)
        with eng.lock:
            configure(eng)
            eng.seed(history)
        with lock:
            engines[p] = eng
            last_seen[p] = eng.last_time
            active_pairs.append(p)
        ev = threading.Event()
        workers[p] = ev
        threading.Thread(target=pair_worker, args=(p, ev), daemon=True,
                         name=f"pair-{p}").start()
    if previous:
        for i, p in enumerate(removed):
            rep = added[i] if i < len(added) else None
            send(f"🔁 {p} suspended → {'now watching ' + rep if rep else 'no open replacement'}")


# ---------------- Virtual settlement ----------------

def settle_if_due(pair, eng, candle):
    """Settle BEFORE step(), as a broker settlement normally arrives first."""
    global open_trade
    t = open_trade
    if not t or t["pair"] != pair:
        return
    entry = t["entry"]
    slot = entry["execution_candle"]
    if candle["time"] < slot:
        return
    price = t["entry_price"]
    if candle["time"] == slot and number(price) and number(candle.get("close")):
        close = candle["close"]
        if close == price:
            result = "equal"
        elif (close > price) == (entry["side"] == "CALL"):
            result = "win"
        else:
            result = "loss"
    else:
        result = "equal"  # missed the expiry candle: release, never guess
    kind, engine_name = entry.get("kind"), entry.get("engine")
    loss_candle = slot if result == "loss" and (
        engine_name == "FCB" and kind in ("FCB", "pole")
        or engine_name == "RSI" and kind == "RSI reversal") else None
    win_expiry = slot + 60 if result == "win" and engine_name == "FCB" else None
    eng.broker_settled(t["token"], result, time.time(), allow_entries=True,
                       loss_candle=loss_candle, win_expiry=win_expiry, settled_entry=entry)
    open_trade = None
    stats[result] += 1
    icon = {"win": "✅ WIN", "loss": "❌ LOSS", "equal": "➖ DRAW"}[result]
    send(f"{icon}  {pair} {entry['side']} ({KIND_LABEL.get(kind, kind)})\n"
         f"Entry {price} → close {candle.get('close')}")


# ---------------- Dispatch (mirrors runner.dispatch_entries, minus the buy) ----------------

def skip(eng, entry):
    kind = entry.get("kind")
    if entry.get("engine") != "RSI":
        return
    if kind == "RSI reversal":
        eng.reversal_skipped(entry)
    elif kind == "LOSS reversal":
        eng.loss_reversal_skipped(entry)
    elif kind == "RSI momentum":
        eng.momentum_skipped(entry)
    else:
        eng.recovery_skipped(entry)


def telegram_eligible(entry):
    """Exact filter from your telegram_alerts.enqueue: FCB breakout, pole
    continuation, and first-attempt RSI reversal only. RSI momentum and
    loss reversals are never broadcast."""
    slot, confirmed = entry.get("execution_candle"), entry.get("confirmed_at")
    engine, kind = entry.get("engine"), entry.get("kind")
    fcb_signal = engine == "FCB" and kind in ("FCB", "pole")
    reversal_signal = (
        engine == "RSI" and kind == "RSI reversal"
        and entry.get("trigger") == "rsi_reversal"
        and type(entry.get("attempt", 1)) is int and entry.get("attempt", 1) == 1
        and number(entry.get("signal_time"))
        and number(slot) and slot == entry["signal_time"] + 60
    )
    now = time.time()
    return ((fcb_signal or reversal_signal) and entry.get("side") in ("CALL", "PUT")
            and number(slot) and slot % 60 == 0
            and number(confirmed) and slot <= confirmed <= now <= slot + ENTRY_WINDOW_SECONDS)


def dispatch(pair, eng, entry, quote):
    global open_trade
    if not telegram_eligible(entry):
        return skip(eng, entry)
    entry = eng.annotate_validation(entry)
    if entry.get("kind") in ("RSI reversal", "LOSS reversal"):
        blocked = not eng.validation_allowed(entry)
    else:
        blocked = eng.zone_data["zone"] != "CLEAR"
    if blocked:
        return skip(eng, entry)
    slot = entry.get("execution_candle")
    if number(slot) and time.time() > slot + ENTRY_WINDOW_SECONDS:
        return skip(eng, entry)
    if entry["engine"] == "FCB" and not eng.fcb_entry_allowed(entry):
        return
    if entry["engine"] == "RSI" and not eng.rsi_entry_allowed(entry):
        return skip(eng, entry)
    if open_trade is not None or eng.pending[entry["engine"]] is not None:
        return skip(eng, entry)  # runner: ONE_TRADE_AT_A_TIME / PAIR_ENGINE_OCCUPIED
    token = uuid.uuid4().hex
    price = quote.get("close") if quote else None
    with lock:
        open_trade = {"pair": pair, "token": token, "entry": entry, "entry_price": price}
        stats["signals"] += 1
        track_expiries(pair, entry, price)
    eng.submitted(token, entry)
    quote_obs = entry.get("confirmation_quote")
    event = {
        "pair": str(pair), "engine": entry["engine"], "kind": entry.get("kind"),
        "side": entry["side"], "mode": "PRACTICE", "slot": int(slot),
        "timezone": TIMEZONE, "extreme_rsi": entry.get("extreme_rsi"),
        "quote": quote_obs.get("close") if isinstance(quote_obs, dict) else price,
    }
    broadcast_signal(TelegramAlerts.text(event), slot)
    pa = prealerts.get(pair)
    if pa and pa["a_time"] + 60 == slot:
        pa["signaled"] = True
    print(f"[signal] {pair} {entry['side']} {entry.get('kind')} @ {price} | "
          f"sent {time.time() - slot:.1f}s after candle open")


# ---------------- Main loop ----------------

def pair_worker(pair, ev):
    """Each pair is watched in its own thread, like runner's PairMonitoring."""
    while not stop.is_set() and not ev.is_set():
        try:
            if current_api is not None and pair in engines:
                poll_pair(current_api, pair)
        except Exception as e:
            print(f"[pair error] {pair}: {e}")
            time.sleep(1)
        time.sleep(POLL_SECONDS)


def outside_side(candle, upper, lower):
    """Same breakout rule as the strategy: close outside the band, body aligned."""
    if not candle or not number(candle.get("close")) or not number(candle.get("open")):
        return None
    c, o = candle["close"], candle["open"]
    if number(upper) and c > upper and c > o:
        return "CALL"
    if number(lower) and c < lower and c < o:
        return "PUT"
    return None


def cancel_prealert(pair, why):
    pa = prealerts.pop(pair, None)
    if pa and not pa["signaled"]:
        broadcast_plain(f"❌ CANCELLED — {pair} {'BUY' if pa['side'] == 'CALL' else 'SELL'} "
                        f"setup did not confirm ({why}). Do not enter.")


def maybe_prealert(pair, eng, quote, now):
    """Heads-up from the forming candle A, ~10s before candle B opens.
    Fresh FCB breakouts only; the real NEW SIGNAL still decides."""
    a_time = quote.get("time") if number(quote.get("time")) else int(now // 60) * 60
    second = now - a_time
    if not PREALERT_SECOND <= second < 58 or pair in prealerts:
        return
    if open_trade is not None or eng.fcb.active or eng.zone_data.get("zone") != "CLEAR":
        return
    upper, lower = eng.bands.preview(quote)
    side = outside_side(quote, upper, lower)
    if not side:
        return
    prealerts[pair] = {"side": side, "a_time": a_time, "signaled": False}
    pair_type = " [OTC]" if pair.endswith("-OTC") else " [REGULAR]"
    direction = "BUY 🟢" if side == "CALL" else "SELL 🔴"
    broadcast_plain(
        "⏰ GET READY!\n"
        f"{pair_flags(pair)}  {pair}{pair_type}\n"
        f"📉 Possible direction: {direction}\n"
        f"➡️ Entry: {format_signal_time(a_time + 60, TIMEZONE)} (in ~{60 - second:.0f}s)\n"
        "⚠️ Wait for NEW SIGNAL to confirm before entering"
    )
    print(f"[prealert] {pair} {side} at :{second:.0f}")


def poll_pair(api, pair):
    global last_new_candle
    eng = engines[pair]
    candle = iq_feed.latest_candle(pair, api=api)
    if candle and candle["time"] != last_seen.get(pair):
        last_seen[pair] = candle["time"]
        last_new_candle = time.time()
        with lock, eng.lock:
            pa = prealerts.get(pair)
            if pa and pa["a_time"] == candle["time"]:
                upper, lower = eng.bands.preview(candle)
                if outside_side(candle, upper, lower) != pa["side"]:
                    cancel_prealert(pair, "candle closed back inside the band")
            settle_if_due(pair, eng, candle)
            update_expiries(pair, candle)
            eng.step(candle)
    now = time.time()
    quote = iq_feed.forming_candle(api, pair, now)
    if not quote:
        return
    broker_now = api.get_server_timestamp()
    with lock, eng.lock:
        eng.confirm_fcb(quote, now, broker_now)
        eng.confirm_rsi(quote, now, broker_now)
        for entry in eng.take_entries(now):
            dispatch(pair, eng, entry, quote)
        pa = prealerts.get(pair)
        if pa and now > pa["a_time"] + 60 + ENTRY_WINDOW_SECONDS:
            if pa["signaled"]:
                prealerts.pop(pair, None)
            else:
                cancel_prealert(pair, "next candle did not confirm")


def main():
    global current_api
    current_api = iq_feed.get_session()
    sync_pairs(current_api)          # also starts one watcher thread per pair
    threading.Thread(target=command_loop, daemon=True).start()
    send("✅ Signal bot v4 started — FCB, pole & RSI reversal only\n"
         "Signals only, no trades placed.\n"
         f"Watching {len(active_pairs)} pairs")
    last_check, stall_alerted = time.time(), False
    while not stop.is_set():
        over = time.time() - started_at >= MAX_RUNTIME_MIN * 60
        if over and (open_trade is None or time.time() - started_at >= MAX_RUNTIME_MIN * 60 + 150):
            print("[runner] max runtime reached, exiting for handoff")
            break
        try:
            current_api = iq_feed.get_session()
            if time.time() - last_check >= PAIR_CHECK_SECONDS:
                sync_pairs(current_api)
                last_check = time.time()
            if not stall_alerted and time.time() - last_new_candle > FEED_STALL_SECONDS:
                send(f"⚠️ No new candles for {FEED_STALL_SECONDS // 60}+ min — feed may be stalled")
                stall_alerted = True
            elif time.time() - last_new_candle <= FEED_STALL_SECONDS:
                stall_alerted = False
        except Exception as e:
            print(f"[loop error] {e}")
        time.sleep(1)
    stop.set()
    for ev in list(workers.values()):
        ev.set()
    with lock:
        save_expiry_stats()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        send(f"❌ Signal bot crashed: {e}")
        raise
    sys.exit(0)
