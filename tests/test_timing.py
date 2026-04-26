from __future__ import annotations

import time

import pytest

from app.timing import TimingCollector, compute_ema


def test_collector_measures_block():
    tc = TimingCollector()
    with tc.measure("work"):
        time.sleep(0.01)
    assert tc.get("work") is not None
    assert 5.0 < tc.get("work") < 200.0  # generous bounds for CI jitter


def test_compute_ema_first_sample_returns_x():
    assert compute_ema(None, 100.0, alpha=0.2, samples=1, warmup=5) == 100.0


def test_compute_ema_warmup_is_cumulative_mean():
    # feed 1,2,3,4,5 — mean is 3.0 after the 5th sample inside warmup
    ema: float | None = None
    for i, x in enumerate((1.0, 2.0, 3.0, 4.0, 5.0), start=1):
        ema = compute_ema(ema, x, alpha=0.2, samples=i, warmup=5)
    assert ema == pytest.approx(3.0)


def test_compute_ema_post_warmup_exponential():
    # after warmup the formula is alpha*x + (1-alpha)*prev
    out = compute_ema(10.0, 20.0, alpha=0.2, samples=6, warmup=5)
    assert out == pytest.approx(0.2 * 20.0 + 0.8 * 10.0)


def test_compute_ema_converges_on_steady_state():
    ema: float | None = None
    for i in range(1, 201):
        ema = compute_ema(ema, 100.0, alpha=0.2, samples=i, warmup=5)
    assert ema == pytest.approx(100.0, abs=1e-6)


def test_compute_ema_rejects_zero_samples():
    with pytest.raises(ValueError):
        compute_ema(None, 1.0, alpha=0.2, samples=0, warmup=5)
