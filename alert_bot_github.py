#!/usr/bin/env python3
"""
alert_bot_github.py -- v7: adds two artificial-pump detectors on top of
v6's real buyer-wallet tracking and Solana RPC security checks.

What's new in this version, and why
--------------------------------------
1. MARKET-CAP GROWTH-RATE CHECK: a token hitting an extreme market cap
   within minutes of launch (the exact case that prompted this -- $12M
   at 42 minutes old) is a classic signature of a sniper/bundle pump,
   not organic demand. Computed as market_cap / age_in_minutes, rejected
   if it exceeds MAX_MCAP_PER_MIN_USD. This is a heuristic cutoff, not a
   scientifically derived line -- tune it if it's too strict or too loose
   for what you're seeing.

2. SINGLE-BUYER DOMINANCE CHECK: rather than just averaging SOL per
   buyer (which blurs out a single whale), this now tracks EXACTLY how
   much SOL each individual wallet contributed (buyer_sol_map, not just
   a set of addresses), and rejects if any one wallet accounts for more
   than MAX_SINGLE_BUYER_SHARE_PCT of all buy volume -- a direct
   statistical fingerprint of one actor controlling the "demand" rather
   than many independent buyers. Only applies when real wallet-level
   data exists (method == real_wallets); the DexScreener tx-count
   fallback has no per-wallet breakdown to check this against.

Honest limitation carried over from v6, still true here: neither check
proves coordination -- a wallet could legitimately buy a large position
once. These are statistical red flags worth surfacing, not certainties.
A real funding-source-clustering check (do the buyer wallets share a
common funding wallet) would be more conclusive but requires extra
Solana RPC calls per buyer and hasn't been built yet.

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

DISCOVERY_WINDOW_SEC = int(os.environ.get("DISCOVERY_WINDOW_SEC", 260))

CHECK_AFTER_MIN = int(os.environ.get("CHECK_AFTER_MIN", 20))
MOMENTUM_GAP_MIN = int(os.environ.get("MOMENTUM_GAP_MIN", 10))
MAX_TOKEN_AGE_MIN = int(os.environ.get("MAX_TOKEN_AGE_MIN", 90))

MIN_UNIQUE_BUYERS = int(os.environ.get("MIN_UNIQUE_BUYERS", 4))
MIN_BUYS_H1 = int(os.environ.get("MIN_BUYS_H1", 8))
MIN_LIQUIDITY_USD = float(os.environ.get("MIN_LIQUIDITY_USD", 5000))
MIN_NET_BUY_RATIO = float(os.environ.get("MIN_NET_BUY_RATIO", 1.3))

MAX_TOP10_HOLDER_PCT = float(os.environ.get("MAX_TOP10_HOLDER_PCT", 50))
REQUIRE_MINT_AUTHORITY_REVOKED = os.environ.get("REQUIRE_MINT_AUTHORITY_REVOKED", "true").lower() == "true"

# New: artificial-pump detectors
MAX_MCAP_PER_MIN_USD = float(os.environ.get("MAX_MCAP_PER_MIN_USD", 50000))
MAX_SINGLE_BUYER_SHARE_PCT = float(os.environ.get("MAX_SINGLE_BUYER_SHARE_PCT", 40))
MIN_PRICE_GROWTH_PCT = float(os.environ.get("MIN_PRICE_GROWTH_PCT", 5))  # price must have risen at least this much since the snapshot, not just buyer count

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
# PumpPortal: new-launch discovery AND real per-wallet trade data
# --------------------------------------------------------------------------

async def listen_pumpportal(window_seconds: int, trade_mints: list[str]) -> tuple[list[dict], dict]:
    """
    Returns (new_launch_events, trade_activity) where trade_activity is
    {mint: {"buyer_sol": {wallet: cumulative_sol}, "sell_sol": float}} --
    now tracking PER-WALLET SOL amounts, not just a set of addresses,
    so single-buyer dominance can be computed later.
    """
    new_launches = []
    trade_activity: dict[str, dict] = {}

    try:
        async with websockets.connect(PUMPPORTAL_WS_URL, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            if trade_mints:
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
                    entry = trade_activity.setdefault(mint, {"buyer_sol": {}, "sell_sol": 0.0})
                    sol_amount = float(data.get("solAmount", 0) or 0)
                    if tx_type == "buy":
                        wallet = data["traderPublicKey"]
                        entry["buyer_sol"][wallet] = entry["buyer_sol"].get(wallet, 0.0) + sol_amount
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

def evaluate_buy(mint: str, age_min: float, accumulated: dict, prior_buy_count: int | None,
                  snapshot_price: float | None) -> tuple[dict | None, str]:
    buyer_sol = accumulated.get("buyer_sol", {})
    sell_sol = accumulated.get("sell_sol", 0.0)
    real_buyers = len(buyer_sol)
    buy_sol = sum(buyer_sol.values())
    has_real_trade_data = real_buyers > 0 or buy_sol > 0 or sell_sol > 0

    pair = fetch_dexscreener_pair(mint)
    if not pair and not has_real_trade_data:
        return None, "UNKNOWN: no data from either PumpPortal trades or DexScreener"

    liquidity = pair["liquidity_usd"] if pair else None
    market_cap = pair["market_cap"] if pair else None
    price_now = pair["price_usd"] if pair else None

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

    # Price-growth check: buyer COUNT not declining isn't the same thing as
    # PRICE not declining -- stragglers can keep trickling in even after a
    # token has already peaked and started rolling over. This is the direct
    # fix for "buy alert fires, then an immediate exit signal with no real
    # price gain in between."
    if snapshot_price is not None and price_now is not None and snapshot_price > 0:
        price_growth_pct = ((price_now - snapshot_price) / snapshot_price) * 100
        if price_growth_pct < MIN_PRICE_GROWTH_PCT:
            return None, (f"price not rising: {price_growth_pct:+.1f}% since snapshot "
                          f"(need +{MIN_PRICE_GROWTH_PCT}%) -- snapshot=${snapshot_price:.8f} now=${price_now:.8f}")

    # Artificial-pump check 1: market cap growth rate vs token age.
    if market_cap is not None and age_min > 0:
        mcap_per_min = market_cap / age_min
        if mcap_per_min > MAX_MCAP_PER_MIN_USD:
            return None, (f"mcap_growth=${mcap_per_min:,.0f}/min (max ${MAX_MCAP_PER_MIN_USD:,.0f}/min) "
                          f"-- ${market_cap:,.0f} at {age_min:.0f}min old, looks like a sniper/bundle pump")

    # Artificial-pump check 2: single-wallet dominance of buy volume.
    if method == "real_wallets" and buy_sol > 0:
        max_single = max(buyer_sol.values())
        share_pct = (max_single / buy_sol) * 100
        if share_pct > MAX_SINGLE_BUYER_SHARE_PCT:
            return None, (f"single_buyer_share={share_pct:.0f}% (max {MAX_SINGLE_BUYER_SHARE_PCT}%) "
                          f"-- one wallet contributed {max_single:.2f} of {buy_sol:.2f} total SOL bought")

    # Security gate -- only spent on candidates already past the above checks.
    top10_pct, holder_data_ok = fetch_holder_concentration(mint)
    mint_revoked, authority_data_ok = fetch_mint_authority_status(mint)

    if holder_data_ok and top10_pct is not None and top10_pct > MAX_TOP10_HOLDER_PCT:
        return None, f"top10_holders={top10_pct:.0f}% (max {MAX_TOP10_HOLDER_PCT}%)"
    if authority_data_ok and REQUIRE_MINT_AUTHORITY_REVOKED and mint_revoked is False:
        return None, "mint authority NOT revoked -- supply can still be inflated"

    # Honest peak baseline for exit tracking: use whichever of the two
    # observed prices (snapshot-time or now) was actually higher, so a
    # token that already peaked slightly before this alert doesn't start
    # exit-tracking from an artificially low point.
    observed_peak = max(p for p in (snapshot_price, price_now) if p is not None) if (snapshot_price or price_now) else 0.0

    return {
        "buy_count": buy_count, "method": method, "net_buy_ratio": net_ratio,
        "liquidity_usd": liquidity, "market_cap": market_cap,
        "price_now": price_now, "observed_peak": observed_peak,
        "mcap_per_min": (market_cap / age_min) if (market_cap and age_min > 0) else None,
        "max_buyer_share_pct": (max(buyer_sol.values()) / buy_sol * 100) if (method == "real_wallets" and buy_sol > 0) else None,
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
    if r.get("price_now"):
        lines.append(f"Current price: ${r['price_now']:.8f}")
    if r.get("market_cap"):
        lines.append(f"Market cap: ${r['market_cap']:,.0f}" + (f" (${r['mcap_per_min']:,.0f}/min growth)" if r.get("mcap_per_min") else ""))
    if r.get("max_buyer_share_pct") is not None:
        lines.append(f"Largest single buyer: {r['max_buyer_share_pct']:.0f}% of total buy volume")
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

    for mint, activity in trade_activity.items():
        if mint not in candidates:
            continue
        c = candidates[mint]
        existing_buyer_sol = c.get("buyer_sol", {})
        for wallet, sol in activity["buyer_sol"].items():
            existing_buyer_sol[wallet] = existing_buyer_sol.get(wallet, 0.0) + sol
        c["buyer_sol"] = existing_buyer_sol
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
            snap_pair = fetch_dexscreener_pair(mint)
            c["snapshot_at"] = now.isoformat()
            c["snapshot_buy_count"] = len(c.get("buyer_sol", {}))
            c["snapshot_price"] = snap_pair["price_usd"] if snap_pair else None
            price_str = f"${c['snapshot_price']}" if c["snapshot_price"] else "unknown"
            print(f"  [snapshot] {mint[:8]}... ({c.get('name','?')}): buy_count={c['snapshot_buy_count']} "
                  f"price={price_str} at age {age_min:.0f}min")
            continue

        mins_since_snapshot = (now - datetime.fromisoformat(snapshot_taken_at)).total_seconds() / 60
        if mins_since_snapshot < MOMENTUM_GAP_MIN:
            continue

        accumulated = {"buyer_sol": c.get("buyer_sol", {}), "sell_sol": c.get("sell_sol", 0.0)}
        try:
            result, reason = evaluate_buy(mint, age_min, accumulated, c.get("snapshot_buy_count"), c.get("snapshot_price"))
        except Exception as e:
            print(f"  [warn] evaluation crashed for {mint}: {e}")
            continue

        short = mint[:8] + "..."
        if reason.startswith("UNKNOWN"):
            unknown_count += 1
            print(f"  [unknown] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")
            continue

        evaluated += 1
        c["checked"] = True
        if result:
            print(f"  [PASS] {short} ({c.get('name','?')}) [{c.get('source')}]: {reason}")
            send_buy_alert(mint, c.get("name", "?"), result)
            c.update({"alerted": True, "alerted_at": now.isoformat(),
                      "peak_price": result["observed_peak"], "exited": False})
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
