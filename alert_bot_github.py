#!/usr/bin/env python3
"""
alert_bot_github.py -- v6: real unique-buyer tracking (PumpPortal trade
stream), real holder-concentration and mint/freeze-authority checks
(Solana's own free RPC), and explicit UNKNOWN vs REJECT reporting.

What changed from v5, and why
-------------------------------
1. UNIQUE BUYERS, FOR REAL (not a transaction-count proxy anymore):
   PumpPortal documents a second subscription, confirmed from their own
   docs and examples: {"method": "subscribeTokenTrade", "keys": [mints]}.
   Events carry a real wallet address (traderPublicKey), buy/sell side,
   and SOL amount -- on the SAME free, keyless connection already used
   for discovery. This run's listening window now also watches every
   still-active candidate's trades, and the set of distinct wallets seen
   buying is accumulated across runs in candidates.json until the token
   is evaluated. This only covers pump.fun bonding-curve trades without
   a funded PumpPortal API key -- tokens discovered via the "trending"
   source (already-migrated, DexScreener-only) likely won't have this
   data, and fall back to DexScreener's transaction-count proxy, with
   the alert explicitly saying which method was used so the two are
   never confused as equivalent.

2. HOLDER CONCENTRATION + MINT/FREEZE AUTHORITY, FOR REAL: both are
   retrieved directly from Solana's own public JSON-RPC
   (getTokenLargestAccounts, getTokenSupply, getAccountInfo) -- not from
   any third-party provider, so nothing here can be paywalled out from
   under this bot the way Mobula was. Honest limitation: the top
   holder accounts returned are TOKEN ACCOUNTS, not necessarily distinct
   WALLETS (one wallet can hold tokens in more than one account) --
   same simplification most public scanners make, stated plainly rather
   than silently assumed.

3. UNKNOWN vs REJECT, explicitly separated in the logs: a token with no
   pair data, a failed RPC call, or missing trade data is logged as
   [unknown] -- data genuinely unavailable -- never folded into [reject],
   which is reserved for cases where real data was available and
   actually failed a threshold. A 403 or a missing field is never
   silently treated as "this token failed the filters."

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

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# Discovery / listening
DISCOVERY_WINDOW_SEC = int(os.environ.get("DISCOVERY_WINDOW_SEC", 260))

# Timing
CHECK_AFTER_MIN = int(os.environ.get("CHECK_AFTER_MIN", 20))
MOMENTUM_GAP_MIN = int(os.environ.get("MOMENTUM_GAP_MIN", 10))
MAX_TOKEN_AGE_MIN = int(os.environ.get("MAX_TOKEN_AGE_MIN", 90))

# Buy-side thresholds
MIN_UNIQUE_BUYERS = int(os.environ.get("MIN_UNIQUE_BUYERS", 4))    # real distinct wallets, when PumpPortal trade data exists
MIN_BUYS_H1 = int(os.environ.get("MIN_BUYS_H1", 8))                # fallback: DexScreener tx count, when it doesn't
MIN_LIQUIDITY_USD = float(os.environ.get("MIN_LIQUIDITY_USD", 5000))
MIN_NET_BUY_RATIO = float(os.environ.get("MIN_NET_BUY_RATIO", 1.3))

# Security thresholds (Solana RPC, free, no key)
MAX_TOP10_HOLDER_PCT = float(os.environ.get("MAX_TOP10_HOLDER_PCT", 50))
REQUIRE_MINT_AUTHORITY_REVOKED = os.environ.get("REQUIRE_MINT_AUTHORITY_REVOKED", "true").lower() == "true"

# Exit-signal tuning (unchanged)
EXIT_DRAWDOWN_PCT = float(os.environ.get("EXIT_DRAWDOWN_PCT", 0.35))
EXIT_TRACK_HOURS = float(os.environ.get("EXIT_TRACK_HOURS", 24))

CANDIDATES_FILE = Path("candidates.json")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
DEXSCREENER_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
DEXSCREENER_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/solana"
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
# PumpPortal: new-launch discovery AND real trade data, one connection
# --------------------------------------------------------------------------

async def listen_pumpportal(window_seconds: int, trade_mints: list[str]) -> tuple[list[dict], dict]:
    """
    Single websocket session, two subscriptions at once:
      - subscribeNewToken: brand-new launches (unchanged from before)
      - subscribeTokenTrade: real per-trade events for every mint passed
        in trade_mints (every still-active candidate), confirmed format
        from PumpPortal's own docs/examples: {"method":"subscribeTokenTrade",
        "keys":[...]}, events carry txType ("buy"/"sell"), traderPublicKey,
        solAmount, mint.

    Returns (new_launch_events, trade_activity) where trade_activity is
    {mint: {"buyers": set(), "buy_sol": float, "sell_sol": float}}.
    """
    new_launches = []
    trade_activity: dict[str, dict] = {}

    try:
        async with websockets.connect(PUMPPORTAL_WS_URL, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            if trade_mints:
                # PumpPortal doesn't document a hard cap, but keep this
                # bounded to something reasonable per run regardless.
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": trade_mints[:500]}))

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

                tx_type = data.get("txType")
                if tx_type == "create" and data.get("mint"):
                    new_launches.append({"mint": data["mint"], "name": data.get("name", "?"), "source": "new_launch"})
                elif tx_type in ("buy", "sell") and data.get("mint") and data.get("traderPublicKey"):
                    mint = data["mint"]
                    entry = trade_activity.setdefault(mint, {"buyers": set(), "buy_sol": 0.0, "sell_sol": 0.0})
                    sol_amount = float(data.get("solAmount", 0) or 0)
                    if tx_type == "buy":
                        entry["buyers"].add(data["traderPublicKey"])
                        entry["buy_sol"] += sol_amount
                    else:
                        entry["sell_sol"] += sol_amount
    except Exception as e:
        print(f"[warn] PumpPortal websocket failed: {e}")

    return new_launches, trade_activity


# --------------------------------------------------------------------------
# DexScreener: trending discovery + fallback market data
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


def fetch_dexscreener_pair(mint: str) -> dict | None:
    try:
        resp = requests.get(f"{DEXSCREENER_PAIRS_URL}/{mint}", timeout=15)
    except Exception as e:
        print(f"  [unknown] {mint}: DexScreener pairs fetch failed: {e}")
        return None
    if resp.status_code != 200:
        print(f"  [unknown] {mint}: DexScreener pairs returned {resp.status_code}")
        return None
    data = resp.json()
    pairs = data if isinstance(data, list) else data.get("pairs", [])
    pairs = [p for p in pairs if p.get("liquidity", {}).get("usd") is not None]
    if not pairs:
        return None
    best = max(pairs, key=lambda p: p["liquidity"]["usd"])
    txns_h1 = best.get("txns", {}).get("h1", {}) or {}
    return {
        "price_usd": float(best["priceUsd"]) if best.get("priceUsd") else None,
        "liquidity_usd": best["liquidity"]["usd"],
        "market_cap": best.get("marketCap"),
        "buys_h1": txns_h1.get("buys", 0),
        "sells_h1": txns_h1.get("sells", 0),
    }


# --------------------------------------------------------------------------
# Solana's own free RPC: holder concentration + mint/freeze authority
# --------------------------------------------------------------------------

def solana_rpc(method: str, params: list) -> dict | None:
    try:
        resp = requests.post(
            SOLANA_RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  [unknown] Solana RPC {method} returned {resp.status_code}")
            return None
        body = resp.json()
        if "error" in body:
            print(f"  [unknown] Solana RPC {method} error: {body['error']}")
            return None
        return body.get("result")
    except Exception as e:
        print(f"  [unknown] Solana RPC {method} failed: {e}")
        return None


def fetch_holder_concentration(mint: str) -> tuple[float | None, bool]:
    """Returns (top10_pct_of_supply, data_available). Top accounts are
    TOKEN ACCOUNTS, not guaranteed-distinct wallets -- a known
    simplification, stated here rather than silently assumed."""
    largest = solana_rpc("getTokenLargestAccounts", [mint])
    supply = solana_rpc("getTokenSupply", [mint])
    if not largest or not supply:
        return None, False
    accounts = largest.get("value", [])
    total = float(supply.get("value", {}).get("uiAmount") or 0)
    if not accounts or total <= 0:
        return None, False
    top10_sum = sum(float(a.get("uiAmount") or 0) for a in accounts[:10])
    return (top10_sum / total) * 100, True


def fetch_mint_authority_status(mint: str) -> tuple[bool | None, bool]:
    """Returns (mint_authority_revoked, data_available). True = safe
    (authority renounced, supply can't be inflated). Most pump.fun
    bonding-curve tokens revoke this automatically on creation -- this
    check matters most for "trending"-source tokens from elsewhere."""
    info = solana_rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    if not info or not info.get("value"):
        return None, False
    try:
        parsed = info["value"]["data"]["parsed"]["info"]
        return parsed.get("mintAuthority") is None, True
    except (KeyError, TypeError):
        return None, False


# --------------------------------------------------------------------------
# Buy-side evaluation
# --------------------------------------------------------------------------

def evaluate_buy(mint: str, accumulated: dict, prior_buy_count: int | None) -> tuple[dict | None, str]:
    real_buyers = len(accumulated.get("buyer_wallets", []))
    buy_sol = accumulated.get("buy_sol", 0.0)
    sell_sol = accumulated.get("sell_sol", 0.0)
    has_real_trade_data = real_buyers > 0 or buy_sol > 0 or sell_sol > 0

    pair = fetch_dexscreener_pair(mint)
    if not pair and not has_real_trade_data:
        return None, "UNKNOWN: no data from either PumpPortal trades or DexScreener"

    liquidity = pair["liquidity_usd"] if pair else None
    market_cap = pair["market_cap"] if pair else None

    if has_real_trade_data:
        buy_count = real_buyers
        net_ratio = buy_sol / sell_sol if sell_sol > 0 else (float("inf") if buy_sol > 0 else 0)
        method = "real_wallets"
        min_buy_count = MIN_UNIQUE_BUYERS
    else:
        buy_count = pair["buys_h1"]
        sells = pair["sells_h1"]
        net_ratio = buy_count / sells if sells > 0 else (float("inf") if buy_count > 0 else 0)
        method = "tx_count_proxy"
        min_buy_count = MIN_BUYS_H1

    if prior_buy_count is not None and buy_count < prior_buy_count:
        return None, f"momentum declining ({method}): {prior_buy_count}->{buy_count}"
    if buy_count < min_buy_count:
        return None, f"buy_count={buy_count} (need {min_buy_count}, method={method})"
    if liquidity is not None and liquidity < MIN_LIQUIDITY_USD:
        return None, f"liquidity=${liquidity:,.0f} (need ${MIN_LIQUIDITY_USD:,.0f})"
    if liquidity is None:
        return None, "UNKNOWN: liquidity data unavailable"
    if net_ratio < MIN_NET_BUY_RATIO:
        return None, f"net_buy_ratio={net_ratio:.2f} (need {MIN_NET_BUY_RATIO}, method={method})"

    # Security gate -- only spent on candidates that already passed the above,
    # to keep RPC call volume proportional to near-qualifying tokens.
    top10_pct, holder_data_ok = fetch_holder_concentration(mint)
    mint_revoked, authority_data_ok = fetch_mint_authority_status(mint)

    if holder_data_ok and top10_pct is not None and top10_pct > MAX_TOP10_HOLDER_PCT:
        return None, f"top10_holders={top10_pct:.0f}% (max {MAX_TOP10_HOLDER_PCT}%)"
    if authority_data_ok and REQUIRE_MINT_AUTHORITY_REVOKED and mint_revoked is False:
        return None, "mint authority NOT revoked -- supply can still be inflated"

    return {
        "buy_count": buy_count, "method": method, "net_buy_ratio": net_ratio,
        "liquidity_usd": liquidity, "market_cap": market_cap,
        "top10_pct": top10_pct if holder_data_ok else None,
        "mint_revoked": mint_revoked if authority_data_ok else None,
    }, "PASSED"


# --------------------------------------------------------------------------
# Alerts
# --------------------------------------------------------------------------

def send_telegram(text: str):
    try:
        resp = requests.post(TELEGRAM_SEND_URL, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        if resp.status_code == 200 and resp.json().get("ok"):
            print(f"  [telegram] sent OK")
        else:
            print(f"  [telegram] FAILED: status={resp.status_code} body={resp.text[:300]}")
    except Exception as e:
        print(f"  [telegram] FAILED to send: {e}")


def send_buy_alert(mint: str, name: str, r: dict):
    method_label = "real unique wallets" if r["method"] == "real_wallets" else "tx-count proxy (no PumpPortal trade data)"
    lines = [
        f"🚀 PUMP.FUN SIGNAL: {name}",
        f"Buyers: {r['buy_count']} ({method_label})",
        f"Net buy ratio: {r['net_buy_ratio']:.1f}x",
        f"Liquidity: ${r['liquidity_usd']:,.0f}",
    ]
    if r.get("market_cap"):
        lines.append(f"Market cap: ${r['market_cap']:,.0f}")
    if r.get("top10_pct") is not None:
        lines.append(f"Top 10 holders: {r['top10_pct']:.0f}% of supply")
    else:
        lines.append("Top 10 holders: ⚠️ unknown (RPC data unavailable)")
    if r.get("mint_revoked") is not None:
        lines.append(f"Mint authority: {'revoked (safe)' if r['mint_revoked'] else '⚠️ STILL ACTIVE'}")
    else:
        lines.append("Mint authority: ⚠️ unknown (RPC data unavailable)")
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

    active_mints = [m for m, c in candidates.items() if not c.get("checked")]
    new_launches, trade_activity = await listen_pumpportal(DISCOVERY_WINDOW_SEC, active_mints)
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

    # Merge this run's observed trade activity into each candidate's
    # running accumulated totals (persists across runs until checked).
    for mint, activity in trade_activity.items():
        if mint not in candidates:
            continue
        c = candidates[mint]
        existing_buyers = set(c.get("buyer_wallets", []))
        c["buyer_wallets"] = list(existing_buyers | activity["buyers"])
        c["buy_sol"] = c.get("buy_sol", 0.0) + activity["buy_sol"]
        c["sell_sol"] = c.get("sell_sol", 0.0) + activity["sell_sol"]

    evaluated = 0
    unknown_count = 0
    alerts_sent = 0
    for mint, c in candidates.items():
        if c.get("checked"):
            continue
        age_min = (now - datetime.fromisoformat(c["created_at"])).total_seconds() / 60
        if age_min > MAX_TOKEN_AGE_MIN:
            c["checked"] = True
            continue

        snapshot_taken_at = c.get("snapshot_at")
        if not snapshot_taken_at:
            if age_min < CHECK_AFTER_MIN:
                continue
            c["snapshot_at"] = now.isoformat()
            c["snapshot_buy_count"] = len(c.get("buyer_wallets", []))
            print(f"  [snapshot] {mint[:8]}... ({c.get('name','?')}): buy_count={c['snapshot_buy_count']} at age {age_min:.0f}min")
            continue

        mins_since_snapshot = (now - datetime.fromisoformat(snapshot_taken_at)).total_seconds() / 60
        if mins_since_snapshot < MOMENTUM_GAP_MIN:
            continue

        accumulated = {"buyer_wallets": c.get("buyer_wallets", []), "buy_sol": c.get("buy_sol", 0.0), "sell_sol": c.get("sell_sol", 0.0)}
        try:
            result, reason = evaluate_buy(mint, accumulated, c.get("snapshot_buy_count"))
        except Exception as e:
            print(f"  [warn] evaluation crashed for {mint}: {e}")
            continue

        short = mint[:8] + "..."
        if reason.startswith("UNKNOWN"):
            unknown_count += 1
            print(f"  [unknown] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")
            continue  # don't mark checked -- try again once data might exist

        evaluated += 1
        c["checked"] = True
        if result:
            print(f"  [PASS] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")
            send_buy_alert(mint, c.get("name", "?"), result)
            pair_now = fetch_dexscreener_pair(mint)
            c.update({"alerted": True, "alerted_at": now.isoformat(),
                      "peak_price": (pair_now["price_usd"] if pair_now else 0.0) or 0.0, "exited": False})
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
        pair = fetch_dexscreener_pair(mint)
        price = pair["price_usd"] if pair else None
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
          f"trade activity for {len(trade_activity)} mint(s), "
          f"evaluated {evaluated}, {unknown_count} unknown, sent {alerts_sent} buy alert(s), "
          f"tracked {tracked} for exit, sent {exits_sent} exit alert(s).")


if __name__ == "__main__":
    asyncio.run(main())
