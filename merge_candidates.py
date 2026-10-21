#!/usr/bin/env python3
"""
merge_candidates.py -- application-level merge for candidates.json,
used by scan.yml's "Save candidate state" step when a git push is
rejected because another run wrote first.

Why this exists instead of letting git merge the JSON textually: git's
line-based merge doesn't understand JSON structure, so two
independently-rewritten versions of the same file reliably produce a
content conflict even when nothing meaningfully clashed. Worse, a naive
"just take whichever version" resolution risks silently erasing an
"alerted" or "exited" flag set by the run that lost the race -- which
would cause a LATER run to re-evaluate and send a duplicate Telegram
alert for a token already alerted on.

This merges per-token, explicitly preferring whichever side has
progressed further (alerted > checked, etc.) so that can't happen.
"""

import json

with open("candidates.json") as f:
    local = json.load(f)
with open("/tmp/remote_candidates.json") as f:
    remote = json.load(f)


def richer(a: dict, b: dict) -> dict:
    for flag in ("alerted", "exited", "checked"):
        if a.get(flag) and not b.get(flag):
            return a
        if b.get(flag) and not a.get(flag):
            return b
    return a if len(a) >= len(b) else b


merged = dict(remote)
for mint, entry in local.items():
    merged[mint] = richer(entry, merged[mint]) if mint in merged else entry

with open("candidates.json", "w") as f:
    json.dump(merged, f, indent=2)

print(f"Merged: {len(local)} local + {len(remote)} remote -> {len(merged)} total entries")
