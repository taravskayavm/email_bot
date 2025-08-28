import logging
from pathlib import Path
from dotenv import load_dotenv


def load_env(script_dir: Path) -> None:
    """Load environment variables from .env files."""
    try:
        load_dotenv(dotenv_path=script_dir / ".env")
        load_dotenv()
    except Exception:
        pass


def setup_logging(log_file: Path) -> None:
    """Configure basic logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
