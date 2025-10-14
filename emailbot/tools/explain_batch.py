from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    d_path = Path("var/last_batch_digest.json")
    e_path = Path("var/last_batch_examples.json")
    if not d_path.exists():
        print("no last batch digest: var/last_batch_digest.json")
        return
    digest = json.loads(d_path.read_text(encoding="utf-8"))
    examples = {}
    if e_path.exists():
        examples = json.loads(e_path.read_text(encoding="utf-8"))
    print("=== LAST BATCH DIGEST ===")
    for k, v in digest.items():
        print(f"{k}: {v}")
    if examples:
        print("\n=== EXAMPLES ===")
        for k, lst in examples.items():
            if lst:
                print(f"{k}:")
                for x in lst[:10]:
                    print(f"  - {x}")


if __name__ == "__main__":
    main()
