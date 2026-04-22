import uvicorn

from app.config import get_settings


def main() -> None:
    s = get_settings()
    uvicorn.run(
        "app:app",
        host=s.host,
        port=s.port,
        log_level=s.log_level.lower(),
        loop="uvloop",
        http="httptools",
        access_log=True,
    )


if __name__ == "__main__":
    main()
