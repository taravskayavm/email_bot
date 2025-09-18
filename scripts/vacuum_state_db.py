#!/usr/bin/env python
"""Run VACUUM/optimize on the state database."""
# [EBOT-LOG-ROTATE-006]
import os
import sqlite3
from pathlib import Path

DB = Path(os.getenv("STATE_DB_PATH", "var/state.db"))


def main() -> None:
    """Ensure the database path exists and vacuum the SQLite file."""
    DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB) as connection:
        connection.execute("PRAGMA optimize;")
        connection.execute("VACUUM;")

    print("vacuum done")


if __name__ == "__main__":
    main()
