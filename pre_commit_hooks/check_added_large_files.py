#!/usr/bin/env python3
import os
import sys

MAX_KB = int(os.environ.get("MAXKB", "500"))

def main(argv: list[str]) -> int:
    ok = True
    for path in argv:
        try:
            if os.path.getsize(path) > MAX_KB * 1024:
                size_kb = os.path.getsize(path) / 1024
                print(f"{path}: {size_kb:.1f}KB exceeds {MAX_KB}KB", file=sys.stderr)
                ok = False
        except OSError:
            pass
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
