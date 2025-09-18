import json, os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from emailbot.services.cooldown import normalize_email_for_key

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover - fallback for older Python
    ZoneInfo = None


def _resolve_path(p: str) -> Path:
    """Return absolute path resolving ``~`` and relative segments."""

    path = Path(p).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


_TZ_NAME = (os.getenv("REPORT_TZ", "Europe/Moscow") or "Europe/Moscow").strip()


def _stats_path() -> Path:
    """Resolve stats file path from ``SEND_STATS_PATH`` each call.

    This makes the module respect runtime overrides of the environment
    variable, which is important for tests monkeypatching it.
    """

    path = _resolve_path(os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl"))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _tzinfo():
    """Resolve timezone from ``REPORT_TZ`` with MSK fallback."""

    name = _TZ_NAME.lower()
    if name in ("msk", "moscow", "europe/moscow", "russia/moscow"):
        try:
            return ZoneInfo("Europe/Moscow")
        except Exception:
            return timezone(timedelta(hours=3))
    try:
        return ZoneInfo(_TZ_NAME)
    except Exception:
        return timezone.utc


def _to_local(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(_tzinfo())


def log_success(email: str, group: str, extra: dict | None = None) -> None:
    email_value = normalize_email_for_key(email) or (email or "").strip()
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": email_value,
        "group": (group or "").strip().lower(),
        "status": "success",
    }
    if extra:
        rec.update(extra)
    path = _stats_path()
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log_error(email: str, group: str, reason: str, extra: dict | None = None) -> None:
    email_value = normalize_email_for_key(email) or (email or "").strip()
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": email_value,
        "group": (group or "").strip().lower(),
        "status": "error",
        "reason": reason[:300],
    }
    if extra:
        rec.update(extra)
    path = _stats_path()
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log_bounce(email: str, reason: str, uuid: str = "", message_id: str = "") -> None:
    email_value = normalize_email_for_key(email) or (email or "").strip()
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": email_value,
        "status": "bounce",
        "reason": str(reason)[:300],
    }
    if uuid:
        rec["uuid"] = uuid
    if message_id:
        rec["message_id"] = message_id
    path = _stats_path()
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _iter_today_week(scope: str):
    """Предикат для отбора записей по ЛОКАЛЬНОЙ (REPORT_TZ/фоллбэк MSK) дате/неделе."""

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
    path = _stats_path()
    if not path.exists():
        return {"ok": 0, "err": 0}
    pred = _iter_today_week(scope)
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            ts = rec.get("ts")
            if not ts or not pred(ts):
                continue
            status = rec.get("status")
            if status == "success":
                ok += 1
            elif status in ("error", "bounce"):
                err += 1
    return {"ok": ok, "err": err, "success": ok, "error": err}


def summarize_today() -> dict:
    return summarize("day")


def summarize_week() -> dict:
    return summarize("week")


def current_tz_label() -> str:
    """Возвращает краткую метку TZ для подписи отчётов, напр. 'MSK'."""

    if _TZ_NAME.lower() in ("europe/moscow", "moscow", "msk", "russia/moscow"):
        return "MSK"
    return _TZ_NAME

