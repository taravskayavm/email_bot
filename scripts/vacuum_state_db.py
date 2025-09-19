#!/usr/bin/env python
"""Run VACUUM/optimize on the state database."""
# [EBOT-LOG-ROTATE-006]
import os
import sqlite3

from utils.paths import expand_path, ensure_parent

DB = expand_path(os.getenv("STATE_DB_PATH", "var/state.db"))


def main() -> None:
    """Ensure the database path exists and vacuum the SQLite file."""
    ensure_parent(DB)

    with sqlite3.connect(DB) as connection:
        connection.execute("PRAGMA optimize;")
        connection.execute("VACUUM;")

    print("vacuum done")


if __name__ == "__main__":
    main()
