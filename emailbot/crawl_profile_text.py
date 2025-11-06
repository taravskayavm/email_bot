from __future__ import annotations


def describe_crawl_profile(
    depth: int,
    max_pages: int,
    time_budget_seconds: int,
    same_domain: bool | int | str,
) -> str:
    """Return a human-readable description of crawl limits."""

    try:
        depth_i = int(depth)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        depth_i = 0

    try:
        pages_i = int(max_pages)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        pages_i = 0

    try:
        budget_i = int(time_budget_seconds)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        budget_i = 0

    scope_flag = str(same_domain).strip().lower()
    same_domain_only = scope_flag in {"1", "true", "yes", "on"}
    scope_text = "только внутри домена" if same_domain_only else "с внешними переходами"

    return (
        "🌐 Профиль краулинга: "
        f"глубина {depth_i}, лимит {pages_i} стр., "
        f"тайм-бюджет {budget_i} сек., {scope_text}."
    )

