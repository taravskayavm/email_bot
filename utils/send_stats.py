import json, os
from collections import Counter
from pathlib import Path
from datetime import datetime, timedelta, timezone

from utils.paths import ensure_parent
from emailbot.utils.paths import resolve_project_path
from emailbot.utils.fs import append_jsonl_atomic

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover - fallback for older Python
    ZoneInfo = None

_TZ_NAME = (os.getenv("REPORT_TZ", "Europe/Moscow") or "Europe/Moscow").strip()


def _stats_path() -> Path:
    """Resolve stats file path from ``SEND_STATS_PATH`` each call.

    This makes the module respect runtime overrides of the environment
    variable, which is important for tests monkeypatching it.
    """

    raw = os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
    expanded = os.path.expandvars(os.path.expanduser(str(raw)))
    path = resolve_project_path(expanded)
    ensure_parent(path)
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


def _normalize_for_stats(email: str) -> str:
    try:
        from emailbot.services.cooldown import normalize_email_for_key

        return normalize_email_for_key(email)
    except Exception:
        try:
            from emailbot.history_key import normalize_history_key

            return normalize_history_key(email)
        except Exception:
            return (email or "").strip().lower()


def log_success(email: str, group: str, extra: dict | None = None) -> None:
    email_value = _normalize_for_stats(email) or (email or "").strip()
    rec = {
        "ts": _now_utc().isoformat().replace("+00:00", "Z"),
        "email": email_value,
        "group": (group or "").strip().lower(),
        "status": "success",
    }
    if extra:
        rec.update(extra)
    path = _stats_path()
    append_jsonl_atomic(path, rec)


def log_error(email: str, group: str, reason: str, extra: dict | None = None) -> None:
    email_value = _normalize_for_stats(email) or (email or "").strip()
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
    append_jsonl_atomic(path, rec)


def log_bounce(email: str, reason: str, uuid: str = "", message_id: str = "") -> None:
    email_value = _normalize_for_stats(email) or (email or "").strip()
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
    append_jsonl_atomic(path, rec)


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


def _ts_to_epoch(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except Exception:
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
            return dt.timestamp()
    return None


def print_summary_report(days: int = 180) -> None:
    """Aggregate ``var/send_stats.jsonl`` into a concise console report."""

    path = _stats_path()
    if not path.exists():
        print("No send stats found.")
        return

    cutoff = _now_utc() - timedelta(days=max(days, 0))
    cutoff_ts = cutoff.timestamp()
    by_domain: Counter[str] = Counter()
    by_status: Counter[str] = Counter()
    by_group: Counter[str] = Counter()
    total = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue

            ts = _ts_to_epoch(record.get("ts"))
            if ts is None or ts < cutoff_ts:
                continue

            email = (record.get("email") or "").strip().lower()
            status = (record.get("status") or "unknown").strip().lower()
            group = (record.get("group") or "n/a").strip().lower()
            if "@" in email:
                domain = email.rsplit("@", 1)[-1] or "n/a"
            else:
                domain = "n/a"

            by_domain[domain] += 1
            by_status[status] += 1
            by_group[group] += 1
            total += 1

    print(f"Report for last {days} days: {total} record(s)\n")

    print("=== Summary by status ===")
    for key, value in by_status.most_common():
        print(f"{key:12s} {value}")

    print("\n=== Top domains ===")
    for key, value in by_domain.most_common(20):
        print(f"{key:30s} {value}")

    print("\n=== Top groups ===")
    for key, value in by_group.most_common(20):
        print(f"{key:20s} {value}")

