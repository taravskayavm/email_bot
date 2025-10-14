from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone


def main() -> None:
    path = Path("var/send_stats.jsonl")
    if not path.exists():
        print("no send_stats.jsonl yet")
        return
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=1)
    seen = set()
    total = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            raw_time = rec.get("time")
            if not isinstance(raw_time, str):
                continue
            try:
                ts = datetime.fromisoformat(raw_time)
            except Exception:
                continue
            if ts >= cutoff:
                email = rec.get("email")
                if email and email not in seen:
                    seen.add(email)
                    total += 1
    print(f"sent in last 24h: {total}")
    for x in list(seen)[:20]:
        print(f"  - {x}")


if __name__ == "__main__":
    main()
