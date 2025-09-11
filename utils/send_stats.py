import json, os
from pathlib import Path
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover - fallback for older Python
    ZoneInfo = None

_PATH = Path(os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl"))
_PATH.parent.mkdir(parents=True, exist_ok=True)
_TZ_NAME = os.getenv("REPORT_TZ", "Europe/Moscow")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_local(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    if ZoneInfo:
        return dt_utc.astimezone(ZoneInfo(_TZ_NAME))
    # fallback: системная локаль (на крайний случай)
    return dt_utc.astimezone()


def log_success(email: str, group: str) -> None:
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": (email or "").strip(),
        "group": (group or "").strip().lower(),
        "status": "sent",
    }
    with _PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log_error(email: str, group: str, reason: str) -> None:
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": (email or "").strip(),
        "group": (group or "").strip().lower(),
        "status": "error",
        "reason": reason[:300],
    }
    with _PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _iter_today_week(scope: str):
    """Предикат для отбора записей по ЛОКАЛЬНОЙ (REPORT_TZ) дате/неделе."""

    now_local = _to_local(_now_utc())
    if scope == "day":
        start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif scope == "week":
        # неделя с понедельника в локальной TZ
        start = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=now_local.weekday()
        )
        end = start + timedelta(days=7)
    else:  # pragma: no cover - defensive
        raise ValueError("scope must be 'day' or 'week'")

    def pred(ts_iso: str) -> bool:
        try:
            dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        except Exception:
            return False
        loc = _to_local(dt)
        return start <= loc < end

    return pred


def summarize(scope: str) -> dict:
    ok = 0
    err = 0
    if not _PATH.exists():
        return {"ok": 0, "err": 0}
    pred = _iter_today_week(scope)
    with _PATH.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            ts = rec.get("ts")
            if not ts or not pred(ts):
                continue
            if rec.get("status") == "sent":
                ok += 1
            elif rec.get("status") == "error":
                err += 1
    return {"ok": ok, "err": err}


def summarize_today() -> dict:
    return summarize("day")


def summarize_week() -> dict:
    return summarize("week")


def current_tz_label() -> str:
    """Возвращает краткую метку TZ для подписи отчётов, напр. 'MSK'."""

    if _TZ_NAME.lower() in ("europe/moscow", "moscow", "msk"):
        return "MSK"
    return _TZ_NAME

