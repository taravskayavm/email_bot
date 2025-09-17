"""Data models shared across the e-mail bot codebase."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Iterable


@dataclass(slots=True)
class EmailEntry:
    """A lightweight record describing a single e-mail address.

    The model intentionally captures only the minimum amount of metadata
    required across the extraction → deduplication → delivery pipeline.

    Parameters
    ----------
    email:
        Normalised e-mail address.
    source:
        A short label describing where the address came from
        (``pdf``, ``url``, ``zip``, ``manual`` …).
    status:
        Processing status for the address, defaults to ``"new"``.
    last_sent:
        Timestamp of the last successful delivery, if known.
    meta:
        Arbitrary payload with auxiliary information (file path, author name,
        scrape context, etc.).
    """

    email: str
    source: str
    status: str = "new"
    last_sent: datetime | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the entry into a dictionary safe for JSON output."""

        data = asdict(self)
        if self.last_sent is not None:
            data["last_sent"] = self.last_sent.isoformat()
        return data

    @staticmethod
    def from_email(
        email: str,
        *,
        source: str,
        status: str = "new",
        last_sent: datetime | None = None,
        meta: dict[str, Any] | None = None,
    ) -> "EmailEntry":
        """Construct an :class:`EmailEntry` from raw pieces of data."""

        return EmailEntry(
            email=email,
            source=source,
            status=status,
            last_sent=last_sent,
            meta=dict(meta) if meta else {},
        )

    @staticmethod
    def wrap_list(
        emails: Iterable[str],
        *,
        source: str,
        status: str = "new",
        last_sent: datetime | None = None,
        meta: dict[str, Any] | None = None,
    ) -> list["EmailEntry"]:
        """Wrap a sequence of addresses in :class:`EmailEntry` objects."""

        base_meta = dict(meta) if meta else {}
        return [
            EmailEntry(
                email=e,
                source=source,
                status=status,
                last_sent=last_sent,
                meta=dict(base_meta),
            )
            for e in emails
        ]
