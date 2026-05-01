from __future__ import annotations

import logging
import os
import time

from oziebot_common.health import start_health_server
from oziebot_strategy_engine.runner import build_runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("strategy-engine")


def main() -> None:
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://oziebot:oziebot@localhost:5432/oziebot",
    )
    poll_interval = float(os.environ.get("OZIEBOT_STRATEGY_RUNNER_POLL_SEC", "1.0"))

    runner = build_runner(database_url=database_url)
    health = start_health_server("strategy-engine")

    log.info("strategy signal runner started")
    health.mark_ready()
    while True:
        try:
            health.touch()
            processed = runner.run_once()
            if processed:
                log.info("runner processed_signals=%s", processed)
        except Exception:
            log.exception("runner iteration failed")
        finally:
            health.touch()
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
