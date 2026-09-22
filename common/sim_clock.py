"""
Compressed simulated clock.

1 simulated day = SIM_DAY_SECONDS real seconds (default 300 = 5 minutes, per the brief).

Every container reads the SAME real Unix clock (time.time()), so
``time.time() % SIM_DAY_SECONDS`` repeats in lock-step across every service without
any explicit coordination, shared state file, or startup handshake -- as long as
SIM_DAY_SECONDS is set to the same value everywhere (docker-compose.yml does this
via a single shared env var). This is what lets the meter simulator's day/night solar
cycle and the API's "low solar share during daytime" alert agree on what "daytime" means.
"""
import os
import time

SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", "300"))


def simulated_hour_of_day(sim_day_seconds: int = None) -> float:
    """Returns a float in [0, 24) representing the current point in the compressed day."""
    seconds = sim_day_seconds or SIM_DAY_SECONDS
    elapsed = time.time() % seconds
    return (elapsed / seconds) * 24
