#!/usr/bin/env python3
import re
import sys

PATTERN = re.compile(r'^(<<<<<<<|=======|>>>>>>>)( .*)?$')

def main(argv: list[str]) -> int:
    ok = True
    for path in argv:
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
                for lineno, line in enumerate(fh, 1):
                    if PATTERN.match(line):
                        print(f"{path}:{lineno}: merge conflict marker", file=sys.stderr)
                        ok = False
                        break
        except OSError:
            pass
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
