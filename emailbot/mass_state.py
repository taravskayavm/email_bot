import json
import os
from typing import Any, Dict, Optional

STATE_PATH = "/mnt/data/mass_state.json"


def load_state() -> Optional[Dict[str, Any]]:
    """Load saved mass mailing state if present."""
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def save_state(state: Dict[str, Any]) -> None:
    """Persist mass mailing state to disk."""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def clear_state() -> None:
    """Remove saved state."""
    try:
        os.remove(STATE_PATH)
    except FileNotFoundError:
        pass
