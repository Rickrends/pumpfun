#!/usr/bin/env python3
"""
alert_bot_github.py -- single-shot version for GitHub Actions.

Discovery source: PumpPortal's free WebSocket (wss://pumpportal.fun/api/data,
method "subscribeNewToken") -- verified from their own docs and real
example output as a genuinely free, keyless, purpose-built pump.fun
creation-event feed. (Moralis was removed: its pump.fun endpoints sit
behind a paid plan, not the free tier -- confirmed after the user hit
that wall directly.)

Two-phase design, which matters for correctness, not just style:
  Phase 1 (discover) -- listen to the PumpPortal feed for a short window
  each run, record any brand-new mint into candidates.json with its
  creation time. A token with 0 buyers yet (which is every token at the
  instant of creation) can never pass a buyer-count check -- so nothing
  gets evaluated against your criteria the moment it's discovered.

  Phase 2 (evaluate) -- on each run, look at candidates old enough to
  have real trading activity (CHECK_AFTER_MIN to MAX_TOKEN_AGE_MIN
  minutes old) and run the same buyer-count + liquidity + wallet-quality
  check as before (via Mobula) exactly once per candidate. Qualifying
  ones get the Telegram alert; all get marked "checked" either way, so
  the same token isn't re-evaluated (and re-billed in API calls) forever.

State (both the discovery log and the evaluation results) lives in
candidates.json, committed back to the repo by the workflow after each
run -- GitHub Actions runners are thrown away after every job, so
without this the bot would forget every token between runs.

NO WALLET. NO PRIVATE KEY. Reads public data, sends Telegram messages,
writes one local JSON file. Nothing else.
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

DISCOVERY_WINDOW_SEC = int(os.environ.get("DISCOVERY_WINDOW_SEC", 45))
CHECK_AFTER_MIN = int(os.environ.get("CHECK_AFTER_MIN", 20))    # don't evaluate before a token is at least this old
MAX_TOKEN_AGE_MIN = int(os.environ.get("MAX_TOKEN_AGE_MIN", 90))  # give up evaluating past this age
MIN_BUYERS_30M = int(os.environ.get("MIN_BUYERS_30M", 4))
MIN_LIQUIDITY_USD = float(os.environ.get("MIN_LIQUIDITY_USD", 5000))
MIN_QUALITY_WALLETS = int(os.environ.get("MIN_QUALITY_WALLETS", 1))

QUALITY_LABELS = {"smartTrader", "proTrader", "insider"}
CAUTION_LABELS = {"bundler"}

CANDIDATES_FILE = Path("candidates.json")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
MOBULA_TRADES_URL = "https://api.mobula.io/api/2/token/trades"
MOBULA_WALLET_LABELS_URL = "https://api.mobula.io/api/2/wallet/labels"
TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------

def load_candidates() -> dict:
    if CANDIDATES_FILE.exists():
        return json.loads(CANDIDATES_FILE.read_text())
    return {}


def save_candidates(candidates: dict):
    # Light housekeeping: drop anything already resolved and past max age,
    # so the file doesn't grow forever.
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=MAX_TOKEN_AGE_MIN * 2)
    pruned = {
        mint: c for mint, c in candidates.items()
        if not c.get("checked") or datetime.fromisoformat(c["created_at"]) > cutoff
    }
    CANDIDATES_FILE.write_text(json.dumps(pruned, indent=2))


# --------------------------------------------------------------------------
# Phase 1: discover new launches via PumpPortal (free, no key)
# --------------------------------------------------------------------------

async def discover_new_tokens(window_seconds: int) -> list[dict]:
    """
    Listens to PumpPortal's live creation-event feed for `window_seconds`
    and returns whatever "create" events arrived. Verified real event
    shape (from PumpPortal's own example output):
        {"txType": "create", "mint": "...", "marketCapSol": ...,
         "vSolInBondingCurve": ..., "vTokensInBondingCurve": ...,
         "traderPublicKey": "...", "signature": "..."}
    name/symbol are not guaranteed present on every event -- handled
    defensively below.
    """
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
                    found.append(data)
    except Exception as e:
        print(f"[warn] PumpPortal websocket failed: {e}")
    return found


# --------------------------------------------------------------------------
# Phase 2: evaluate a candidate once it's old enough (same logic as before)
# --------------------------------------------------------------------------

def fetch_recent_buyers(token_address: str) -> dict | None:
    since = datetime.now(timezone.utc) - timedelta(minutes=30)
    resp = requests.get(
        MOBULA_TRADES_URL,
        params={
            "chainId": "solana:solana",
            "address": token_address,
            "mode": "asset",
            "sortOrder": "desc",
            "limit": 500,
            "fromDate": since.isoformat(),
        },
        headers={"Authorization": MOBULA_API_KEY},
        timeout=15,
    )
    if resp.status_code != 200:
        return None
    trades = resp.json().get("data", [])
    if not trades:
        return None
    buys = [t for t in trades if (t.get("type") or "").lower() == "buy"]
    buyers = {t.get("swapRecipient") or t.get("swapSenderAddress") for t in buys}
    buyers.discard(None)
    latest = trades[0]
    return {
        "buyers": buyers,
        "liquidity_usd": latest.get("liquidityUSD") or 0,  # field name unverified -- confirm against a live response
        "market_cap": latest.get("baseTokenMarketCapUSD"),
    }


def fetch_wallet_labels(wallets: list[str], token_address: str) -> dict[str, list[str]]:
    if not wallets:
        return {}
    resp = requests.get(
        MOBULA_WALLET_LABELS_URL,
        params={"walletAddresses": ",".join(list(wallets)[:100]), "tokenAddress": token_address},
        headers={"Authorization": MOBULA_API_KEY},
        timeout=15,
    )
    if resp.status_code != 200:
        return {}
    data = resp.json().get("data", resp.json())
    entries = data if isinstance(data, list) else data.get("wallets", [])
    return {e["walletAddress"]: e.get("labels", []) for e in entries if "walletAddress" in e}


def evaluate_candidate(mint: str) -> dict | None:
    buyer_data = fetch_recent_buyers(mint)
    if not buyer_data:
        return None
    if len(buyer_data["buyers"]) < MIN_BUYERS_30M or buyer_data["liquidity_usd"] < MIN_LIQUIDITY_USD:
        return None

    wallet_labels = fetch_wallet_labels(list(buyer_data["buyers"]), mint)
    quality_wallets = [w for w, labels in wallet_labels.items() if QUALITY_LABELS & set(labels)]
    caution_wallets = [w for w, labels in wallet_labels.items() if CAUTION_LABELS & set(labels)]

    if len(quality_wallets) < MIN_QUALITY_WALLETS:
        return None

    return {
        "buyers_30m": len(buyer_data["buyers"]),
        "liquidity_usd": buyer_data["liquidity_usd"],
        "market_cap": buyer_data["market_cap"],
        "quality_count": len(quality_wallets),
        "caution_count": len(caution_wallets),
    }


def send_telegram_alert(mint: str, name: str, result: dict):
    lines = [
        f"🚀 PUMP.FUN SIGNAL: {name}",
        f"Buyers (30m): {result['buyers_30m']}",
        f"Liquidity: ${result['liquidity_usd']:,.0f}",
    ]
    if result.get("market_cap"):
        lines.append(f"Market cap: ${result['market_cap']:,.0f}")
    lines.append(f"Quality wallets (smart/pro/insider): {result['quality_count']}")
    if result["caution_count"] > 0:
        lines.append(f"⚠️ {result['caution_count']} bundler-labeled wallet(s) in buyers -- check for coordinated activity")
    lines.append(f"\npump.fun: https://pump.fun/{mint}")
    lines.append(f"Solscan: https://solscan.io/token/{mint}")
    lines.append(f"\nAddress: {mint}")

    requests.post(TELEGRAM_SEND_URL, json={"chat_id": TELEGRAM_CHAT_ID, "text": "\n".join(lines)}, timeout=15)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

async def main():
    candidates = load_candidates()
    now = datetime.now(timezone.utc)

    # Phase 1: discover
    new_events = await discover_new_tokens(DISCOVERY_WINDOW_SEC)
    added = 0
    for ev in new_events:
        mint = ev["mint"]
        if mint not in candidates:
            candidates[mint] = {
                "created_at": now.isoformat(),
                "name": ev.get("name", "?"),
                "checked": False,
            }
            added += 1

    # Phase 2: evaluate anything old enough and not yet checked
    alerts_sent = 0
    evaluated = 0
    for mint, c in candidates.items():
        if c.get("checked"):
            continue
        age_min = (now - datetime.fromisoformat(c["created_at"])).total_seconds() / 60
        if age_min < CHECK_AFTER_MIN:
            continue  # too young still, leave for a later run
        if age_min > MAX_TOKEN_AGE_MIN:
            c["checked"] = True  # give up on stale candidates without spending an API call
            continue

        try:
            result = evaluate_candidate(mint)
        except Exception as e:
            print(f"[warn] evaluation failed for {mint}: {e}")
            continue

        evaluated += 1
        c["checked"] = True
        if result:
            send_telegram_alert(mint, c.get("name", "?"), result)
            alerts_sent += 1

    save_candidates(candidates)
    print(f"Discovered {added} new token(s), evaluated {evaluated}, sent {alerts_sent} alert(s).")


if __name__ == "__main__":
    asyncio.run(main())
