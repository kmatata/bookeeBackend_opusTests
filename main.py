from pathlib import Path

import uvicorn

from app.config import get_settings
from app.log import setup_logging


def main() -> None:
    s = get_settings()
    setup_logging(state_dir=Path(s.state_dir), level=s.log_level)
    uvicorn.run(
        "app:app",
        host=s.host,
        port=s.port,
        log_level=s.log_level.lower(),
        log_config=None,   # let our JSON root logger handle all uvicorn output
        loop="uvloop",
        http="httptools",
        access_log=True,
    )


if __name__ == "__main__":
    main()
