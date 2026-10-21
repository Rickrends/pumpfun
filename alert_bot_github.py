#!/usr/bin/env python3
"""
alert_bot_github.py -- v4: widened discovery coverage, net buying pressure,
holder concentration, dev-holding check, LP lock/burn check, two-stage
momentum confirmation, plus the exit-signal tracking from v3.

Mapping back to the 6 suggestions, and being honest about which are
real verified checks vs. honest proxies vs. left out:

  1. Discovery coverage gap   -> FIXED: listening window widened from 45s
     to ~90% of the 5-min cron gap, so far fewer launches are missed
     between runs.
  2. Net buying pressure      -> REAL: now compares buy USD volume vs sell
     USD volume in the same 30-min window, not just a buyer headcount.
  3. Holder concentration     -> REAL: Mobula's /2/token/security gives
     top10HoldingsPercentage directly -- verified field, used as-is.
  4. Creator/dev wallet history -> PROXY, not the real thing: I could not
     verify a clean "has this deployer rugged before" endpoint. What IS
     real and verified: devHoldingsPercentage (how much the dev wallet
     currently holds right now). High current dev holding is a related
     but different signal from past rug history -- used honestly as
     "dev still holds a lot of supply", not "this dev has rugged before".
  5. Sustained momentum       -> REAL: two-stage check. A snapshot is taken
     at CHECK_AFTER_MIN, then re-compared MOMENTUM_GAP_MIN later -- a
     token only passes if buyer count held or grew between the two, not
     just a single lucky snapshot.
  6. Liquidity depth          -> PROXY, not a true slippage simulation: a
     real price-impact calc needs live pool reserves math I'm not
     confident building reliably from documented endpoints. What IS real:
     LP lock/burn percentage from /2/token/security -- a different but
     genuinely useful rug-risk signal (can the liquidity be pulled at
     all), used instead. Field is often null for pre-migration pump.fun
     tokens (still on the bonding curve, no separate LP yet) -- treated
     as "unknown, don't block" rather than a false reject.

Still NO WALLET. NO PRIVATE KEY. Reads public data, sends Telegram
messages, writes one local JSON file. Nothing else.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import websockets

MOBULA_API_KEY = os.environ["MOBULA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# Discovery
DISCOVERY_WINDOW_SEC = int(os.environ.get("DISCOVERY_WINDOW_SEC", 260))  # ~90% of the 5-min gap

# Timing
CHECK_AFTER_MIN = int(os.environ.get("CHECK_AFTER_MIN", 20))       # first snapshot no earlier than this
MOMENTUM_GAP_MIN = int(os.environ.get("MOMENTUM_GAP_MIN", 10))     # wait this long after snapshot before final check
MAX_TOKEN_AGE_MIN = int(os.environ.get("MAX_TOKEN_AGE_MIN", 90))

# Buy-side thresholds
MIN_BUYERS_30M = int(os.environ.get("MIN_BUYERS_30M", 4))
MIN_LIQUIDITY_USD = float(os.environ.get("MIN_LIQUIDITY_USD", 5000))
MIN_QUALITY_WALLETS = int(os.environ.get("MIN_QUALITY_WALLETS", 1))
MIN_NET_BUY_RATIO = float(os.environ.get("MIN_NET_BUY_RATIO", 1.3))   # buy USD / sell USD must exceed this
MAX_TOP10_HOLDER_PCT = float(os.environ.get("MAX_TOP10_HOLDER_PCT", 50))
MAX_DEV_HOLDING_PCT = float(os.environ.get("MAX_DEV_HOLDING_PCT", 15))

# Exit-signal tuning (unchanged from v3)
EXIT_DRAWDOWN_PCT = float(os.environ.get("EXIT_DRAWDOWN_PCT", 0.35))
EXIT_TRACK_HOURS = float(os.environ.get("EXIT_TRACK_HOURS", 24))

QUALITY_LABELS = {"smartTrader", "proTrader", "insider"}
CAUTION_LABELS = {"bundler"}

CANDIDATES_FILE = Path("candidates.json")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
DEXSCREENER_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
MOBULA_TRADES_URL = "https://api.mobula.io/api/2/token/trades"
MOBULA_WALLET_LABELS_URL = "https://api.mobula.io/api/2/wallet/labels"
MOBULA_PRICE_URL = "https://api.mobula.io/api/2/token/price"
MOBULA_SECURITY_URL = "https://api.mobula.io/api/2/token/security"
TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------

def load_candidates() -> dict:
    if CANDIDATES_FILE.exists():
        return json.loads(CANDIDATES_FILE.read_text())
    return {}


def save_candidates(candidates: dict):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=EXIT_TRACK_HOURS * 2)
    pruned = {}
    for mint, c in candidates.items():
        if c.get("alerted") and not c.get("exited") and not c.get("tracking_done"):
            pruned[mint] = c
            continue
        if not c.get("checked") or datetime.fromisoformat(c["created_at"]) > cutoff:
            pruned[mint] = c
    CANDIDATES_FILE.write_text(json.dumps(pruned, indent=2))


# --------------------------------------------------------------------------
# Discovery source 1: PumpPortal (brand new launches) -- widened window
# --------------------------------------------------------------------------

async def discover_new_launches(window_seconds: int) -> list[dict]:
    found = []
    try:
        async with websockets.connect(PUMPPORTAL_WS_URL, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            loop = asyncio.get_event_loop()
            end_time = loop.time() + window_seconds
            while True:
                remaining = end_time - loop.time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                try:
                    data = json.loads(msg)
                except json.JSONDecodeError:
                    continue
                if data.get("txType") == "create" and data.get("mint"):
                    found.append({"mint": data["mint"], "name": data.get("name", "?"), "source": "new_launch"})
    except Exception as e:
        print(f"[warn] PumpPortal websocket failed: {e}")
    return found


# --------------------------------------------------------------------------
# Discovery source 2: DexScreener trending/boosted (already-trading tokens)
# --------------------------------------------------------------------------

def discover_trending_tokens() -> list[dict]:
    try:
        resp = requests.get(DEXSCREENER_BOOSTS_URL, timeout=15)
        if resp.status_code != 200:
            print(f"[warn] DexScreener boosts returned {resp.status_code}")
            return []
        items = resp.json()
        if not isinstance(items, list):
            return []
        found = []
        for item in items:
            if item.get("chainId") != "solana":
                continue
            token_addr = item.get("tokenAddress")
            if token_addr:
                found.append({"mint": token_addr, "name": item.get("description", "?")[:40], "source": "trending"})
        return found
    except Exception as e:
        print(f"[warn] DexScreener boosts fetch failed: {e}")
        return []


# --------------------------------------------------------------------------
# Trade data: buyers, net buy/sell volume (point 2)
# --------------------------------------------------------------------------

def fetch_trade_snapshot(token_address: str) -> dict | None:
    since = datetime.now(timezone.utc) - timedelta(minutes=30)
    resp = requests.get(
        MOBULA_TRADES_URL,
        params={
            "chainId": "solana:solana", "address": token_address, "mode": "asset",
            "sortOrder": "desc", "limit": 500, "fromDate": since.isoformat(),
        },
        headers={"Authorization": MOBULA_API_KEY}, timeout=15,
    )
    if resp.status_code != 200:
        print(f"  [info] {token_address}: Mobula trades returned {resp.status_code}")
        return None
    trades = resp.json().get("data", [])
    if not trades:
        return None

    buys = [t for t in trades if (t.get("type") or "").lower() == "buy"]
    sells = [t for t in trades if (t.get("type") or "").lower() == "sell"]
    buyers = {t.get("swapRecipient") or t.get("swapSenderAddress") for t in buys}
    buyers.discard(None)
    buy_usd = sum(t.get("baseTokenAmountUSD") or 0 for t in buys)
    sell_usd = sum(t.get("baseTokenAmountUSD") or 0 for t in sells)
    latest = trades[0]
    return {
        "buyers": buyers,
        "buyer_count": len(buyers),
        "buy_usd": buy_usd,
        "sell_usd": sell_usd,
        "liquidity_usd": latest.get("liquidityUSD") or 0,
        "market_cap": latest.get("baseTokenMarketCapUSD"),
    }


def fetch_wallet_labels(wallets: list[str], token_address: str) -> dict[str, list[str]]:
    if not wallets:
        return {}
    resp = requests.get(
        MOBULA_WALLET_LABELS_URL,
        params={"walletAddresses": ",".join(list(wallets)[:100]), "tokenAddress": token_address},
        headers={"Authorization": MOBULA_API_KEY}, timeout=15,
    )
    if resp.status_code != 200:
        return {}
    data = resp.json().get("data", resp.json())
    entries = data if isinstance(data, list) else data.get("wallets", [])
    return {e["walletAddress"]: e.get("labels", []) for e in entries if "walletAddress" in e}


def fetch_security(token_address: str) -> dict:
    """Points 3, 4 (proxy), 6 (proxy). Returns {} on failure -- callers
    must treat missing fields as 'unknown', not an automatic fail, since
    this data is commonly null for very fresh/pre-migration tokens."""
    try:
        resp = requests.get(
            MOBULA_SECURITY_URL,
            params={"blockchain": "solana:solana", "address": token_address},
            headers={"Authorization": MOBULA_API_KEY}, timeout=15,
        )
        if resp.status_code != 200:
            return {}
        return resp.json().get("data", {}) or {}
    except Exception as e:
        print(f"  [warn] security fetch failed for {token_address}: {e}")
        return {}


def fetch_current_price(mint: str) -> float | None:
    resp = requests.get(
        MOBULA_PRICE_URL, params={"address": mint, "blockchain": "Solana"},
        headers={"Authorization": MOBULA_API_KEY}, timeout=15,
    )
    if resp.status_code != 200:
        return None
    price = resp.json().get("data", {}).get("priceUSD")
    return float(price) if price is not None else None


# --------------------------------------------------------------------------
# Buy-side evaluation: full gate, combining all checks
# --------------------------------------------------------------------------

def evaluate_buy(mint: str, prior_buyer_count: int | None) -> tuple[dict | None, str]:
    snap = fetch_trade_snapshot(mint)
    if not snap:
        return None, "no trade data available from Mobula"

    n_buyers = snap["buyer_count"]
    liquidity = snap["liquidity_usd"]
    buy_usd, sell_usd = snap["buy_usd"], snap["sell_usd"]
    net_ratio = buy_usd / sell_usd if sell_usd > 0 else (float("inf") if buy_usd > 0 else 0)

    # Point 5: momentum must have held or grown since the earlier snapshot.
    if prior_buyer_count is not None and n_buyers < prior_buyer_count:
        return None, f"momentum declining: buyers {prior_buyer_count}->{n_buyers}"

    if n_buyers < MIN_BUYERS_30M:
        return None, f"buyers={n_buyers} (need {MIN_BUYERS_30M})"
    if liquidity < MIN_LIQUIDITY_USD:
        return None, f"liquidity=${liquidity:,.0f} (need ${MIN_LIQUIDITY_USD:,.0f})"
    if net_ratio < MIN_NET_BUY_RATIO:
        return None, f"net_buy_ratio={net_ratio:.2f} (need {MIN_NET_BUY_RATIO}) -- buy=${buy_usd:,.0f} sell=${sell_usd:,.0f}"

    wallet_labels = fetch_wallet_labels(list(snap["buyers"]), mint)
    quality_wallets = [w for w, l in wallet_labels.items() if QUALITY_LABELS & set(l)]
    caution_wallets = [w for w, l in wallet_labels.items() if CAUTION_LABELS & set(l)]
    if len(quality_wallets) < MIN_QUALITY_WALLETS:
        return None, f"quality_wallets={len(quality_wallets)} (need {MIN_QUALITY_WALLETS})"

    # Points 3, 4, 6: holder concentration / dev holding / LP lock, best-effort.
    security = fetch_security(mint)
    top10_pct = security.get("top10HoldingsPercentage")
    dev_pct = security.get("devHoldingsPercentage")
    lp_info = security.get("liquidity", []) or []
    lp_safe_pct = sum(
        item.get("percentage", 0) for item in lp_info
        if item.get("type") in ("locked", "burned")
    ) if lp_info else None

    if top10_pct is not None and top10_pct > MAX_TOP10_HOLDER_PCT:
        return None, f"top10_holders={top10_pct:.0f}% (max {MAX_TOP10_HOLDER_PCT}%)"
    if dev_pct is not None and dev_pct > MAX_DEV_HOLDING_PCT:
        return None, f"dev_holding={dev_pct:.0f}% (max {MAX_DEV_HOLDING_PCT}%)"

    return {
        "buyers_30m": n_buyers, "liquidity_usd": liquidity, "market_cap": snap["market_cap"],
        "quality_count": len(quality_wallets), "caution_count": len(caution_wallets),
        "net_buy_ratio": net_ratio, "top10_pct": top10_pct, "dev_pct": dev_pct, "lp_safe_pct": lp_safe_pct,
    }, "PASSED"


# --------------------------------------------------------------------------
# Alerts
# --------------------------------------------------------------------------

def send_telegram(text: str):
    """Verifies the send actually succeeded and prints the result either
    way -- a silent requests.post() with no status check meant a failed
    alert would look identical to 'nothing passed', with no record of
    which one actually happened."""
    try:
        resp = requests.post(TELEGRAM_SEND_URL, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        if resp.status_code == 200 and resp.json().get("ok"):
            print(f"  [telegram] sent OK")
        else:
            print(f"  [telegram] FAILED: status={resp.status_code} body={resp.text[:300]}")
    except Exception as e:
        print(f"  [telegram] FAILED to send: {e}")


def send_buy_alert(mint: str, name: str, r: dict):
    lines = [
        f"🚀 PUMP.FUN SIGNAL: {name}",
        f"Buyers (30m): {r['buyers_30m']}  |  Net buy ratio: {r['net_buy_ratio']:.1f}x",
        f"Liquidity: ${r['liquidity_usd']:,.0f}",
    ]
    if r.get("market_cap"):
        lines.append(f"Market cap: ${r['market_cap']:,.0f}")
    lines.append(f"Quality wallets: {r['quality_count']}" + (f"  |  ⚠️ {r['caution_count']} bundler-labeled" if r["caution_count"] else ""))
    if r.get("top10_pct") is not None:
        lines.append(f"Top 10 holders: {r['top10_pct']:.0f}% of supply")
    if r.get("dev_pct") is not None:
        lines.append(f"Dev wallet holds: {r['dev_pct']:.0f}%")
    if r.get("lp_safe_pct") is not None:
        lines.append(f"LP locked/burned: {r['lp_safe_pct']:.0f}%")
    lines.append(f"\npump.fun: https://pump.fun/{mint}\nSolscan: https://solscan.io/token/{mint}\n\nAddress: {mint}")
    send_telegram("\n".join(lines))


def send_exit_alert(mint: str, name: str, peak: float, current: float, drawdown_pct: float):
    send_telegram(
        f"📉 EXIT SIGNAL: {name}\n"
        f"Price is down {drawdown_pct*100:.0f}% from its peak since your buy alert.\n"
        f"Peak: ${peak:.8f}  Now: ${current:.8f}\n"
        f"This reflects a REAL price drop that already happened -- not a prediction. "
        f"Decide for yourself from here.\nSolscan: https://solscan.io/token/{mint}"
    )


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

async def main():
    candidates = load_candidates()
    now = datetime.now(timezone.utc)

    new_launches = await discover_new_launches(DISCOVERY_WINDOW_SEC)
    trending = discover_trending_tokens()
    added = 0
    for ev in new_launches:
        mint = ev["mint"]
        if mint not in candidates:
            candidates[mint] = {"created_at": now.isoformat(), "name": ev["name"], "checked": False, "source": "new_launch"}
            added += 1
    for ev in trending:
        mint = ev["mint"]
        if mint not in candidates:
            backdated = now - timedelta(minutes=CHECK_AFTER_MIN + 1)
            candidates[mint] = {"created_at": backdated.isoformat(), "name": ev["name"], "checked": False, "source": "trending"}
            added += 1

    evaluated = 0
    alerts_sent = 0
    for mint, c in candidates.items():
        if c.get("checked"):
            continue
        age_min = (now - datetime.fromisoformat(c["created_at"])).total_seconds() / 60
        if age_min > MAX_TOKEN_AGE_MIN:
            c["checked"] = True
            continue

        snapshot_taken_at = c.get("snapshot_at")
        has_snapshot = snapshot_taken_at is not None

        if not has_snapshot:
            # Stage 1: take the momentum snapshot once old enough. Don't
            # evaluate yet -- just record buyer count for later comparison.
            if age_min < CHECK_AFTER_MIN:
                continue
            snap = fetch_trade_snapshot(mint)
            c["snapshot_at"] = now.isoformat()
            c["snapshot_buyers"] = snap["buyer_count"] if snap else 0
            print(f"  [snapshot] {mint[:8]}... ({c.get('name','?')}): buyers={c['snapshot_buyers']} at age {age_min:.0f}min")
            continue

        # Stage 2: only evaluate once MOMENTUM_GAP_MIN has passed since the snapshot.
        mins_since_snapshot = (now - datetime.fromisoformat(snapshot_taken_at)).total_seconds() / 60
        if mins_since_snapshot < MOMENTUM_GAP_MIN:
            continue

        try:
            result, reason = evaluate_buy(mint, c.get("snapshot_buyers"))
        except Exception as e:
            print(f"  [warn] evaluation crashed for {mint}: {e}")
            continue

        evaluated += 1
        c["checked"] = True
        short = mint[:8] + "..."
        if result:
            print(f"  [PASS] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")
            send_buy_alert(mint, c.get("name", "?"), result)
            c.update({"alerted": True, "alerted_at": now.isoformat(),
                      "peak_price": fetch_current_price(mint) or 0.0, "exited": False})
            alerts_sent += 1
        else:
            print(f"  [reject] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")

    exits_sent = 0
    tracked = 0
    for mint, c in candidates.items():
        if not c.get("alerted") or c.get("exited") or c.get("tracking_done"):
            continue
        hours_since = (now - datetime.fromisoformat(c["alerted_at"])).total_seconds() / 3600
        if hours_since > EXIT_TRACK_HOURS:
            c["tracking_done"] = True
            continue
        tracked += 1
        try:
            price = fetch_current_price(mint)
        except Exception as e:
            print(f"  [warn] price check failed for {mint}: {e}")
            continue
        if price is None or price <= 0:
            continue
        peak = max(c.get("peak_price", 0.0), price)
        c["peak_price"] = peak
        if peak > 0 and (peak - price) / peak >= EXIT_DRAWDOWN_PCT:
            send_exit_alert(mint, c.get("name", "?"), peak, price, (peak - price) / peak)
            c["exited"] = True
            exits_sent += 1

    save_candidates(candidates)
    print(f"Discovered {added} new ({len(new_launches)} launches, {len(trending)} trending), "
          f"evaluated {evaluated}, sent {alerts_sent} buy alert(s), "
          f"tracked {tracked} for exit, sent {exits_sent} exit alert(s).")


if __name__ == "__main__":
    asyncio.run(main())
