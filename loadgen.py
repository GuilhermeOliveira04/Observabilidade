import os
import random
import time

import httpx

CHECKOUT_URL = os.getenv("CHECKOUT_URL", "http://checkout:8000/checkout")
SCENARIO = os.getenv("LOADGEN_SCENARIO", "fast")
TIMEOUT = httpx.Timeout(15.0, connect=2.0)


def main() -> None:
    with httpx.Client(timeout=TIMEOUT) as client:
        while True:
            started = time.perf_counter()
            try:
                response = client.post(CHECKOUT_URL, params={"scenario": SCENARIO})
                print(f"{response.status_code} {response.text[:300]}", flush=True)
            except httpx.HTTPError as exc:
                print(f"request failed: {exc}", flush=True)
            pause = random.uniform(0.5, 1.0)
            time.sleep(max(0.0, pause - (time.perf_counter() - started)))


if __name__ == "__main__":
    main()
