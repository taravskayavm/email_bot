from __future__ import annotations

from pathlib import Path


def main() -> None:
    p = Path("var/last_batch_digest.json")
    if not p.exists():
        print("digest not found")
        return
    print(p.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
