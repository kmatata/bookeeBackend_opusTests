from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class TimingCollector:
    """Collect named perf_counter measurements scoped to one request."""

    metrics: dict[str, float] = field(default_factory=dict)

    @contextmanager
    def measure(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.metrics[name] = (time.perf_counter() - t0) * 1000.0

    def set(self, name: str, ms: float) -> None:
        self.metrics[name] = ms

    def get(self, name: str) -> float | None:
        return self.metrics.get(name)


def compute_ema(
    prev: float | None,
    x: float,
    *,
    alpha: float,
    samples: int,
    warmup: int,
) -> float:
    """Update an EMA. For the first `warmup` samples use a cumulative mean
    so the signal stabilises; after that, switch to the exponential form
    `alpha*x + (1-alpha)*prev`.

    `samples` is the count AFTER including x (so 1 on first observation).
    """
    if samples <= 0:
        raise ValueError("samples must be >= 1")
    if samples == 1 or prev is None:
        return float(x)
    if samples <= warmup:
        # running cumulative mean: prev == mean of (samples-1) prior values
        return prev + (x - prev) / samples
    return alpha * x + (1.0 - alpha) * prev
