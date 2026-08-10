#!/usr/bin/env python3
"""Wait until the standalone master reports exactly two live workers."""

from __future__ import annotations

import json
import time
import urllib.request


MASTER_STATUS_URL = "http://spark-master:8080/json/"
EXPECTED_WORKERS = 2
TIMEOUT_SECONDS = 180


def main() -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_error = "master status not queried"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(MASTER_STATUS_URL, timeout=5) as response:
                status = json.load(response)
            workers = status.get("workers", [])
            live = [worker for worker in workers if worker.get("state") == "ALIVE"]
            if len(live) == EXPECTED_WORKERS:
                profiles = sorted(
                    (worker.get("cores"), worker.get("memory")) for worker in live
                )
                expected_profiles = [(2, 2048), (2, 2048)]
                if profiles != expected_profiles:
                    raise RuntimeError(
                        f"Unexpected worker profiles {profiles}; expected {expected_profiles}"
                    )
                print("Spark master reports exactly two live 2-core/2-GB workers")
                return
            last_error = f"master reports {len(live)} live workers"
        except Exception as exc:  # The master may still be starting.
            last_error = str(exc)
        time.sleep(2)
    raise TimeoutError(f"Spark cluster did not become ready: {last_error}")


if __name__ == "__main__":
    main()
