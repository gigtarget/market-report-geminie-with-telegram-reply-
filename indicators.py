from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class RsiSnapshot:
    value: float
    label: str


@dataclass
class MacdSnapshot:
    macd: float
    signal: float
    histogram: float
    label: str


@dataclass
class SupertrendSnapshot:
    value: float
    direction: int
    label: str


def compute_rsi(close_series: pd.Series, period: int = 14) -> RsiSnapshot:
    if close_series.empty:
        raise ValueError("Close series is empty")

    delta = close_series.diff()
    gains = delta.clip(lower=0)
    losses = (-delta).clip(lower=0)

    avg_gain = gains.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    value = float(rsi.iloc[-1])
    if value >= 60:
        label = "strong"
    elif value <= 40:
        label = "weak"
    else:
        label = "neutral"

    return RsiSnapshot(value=value, label=label)


def compute_macd(
    close_series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> MacdSnapshot:
    if close_series.empty:
        raise ValueError("Close series is empty")

    ema_fast = close_series.ewm(span=fast, adjust=False).mean()
    ema_slow = close_series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line

    macd_value = float(macd_line.iloc[-1])
    signal_value = float(signal_line.iloc[-1])
    hist_value = float(histogram.iloc[-1])
    label = "bullish momentum" if hist_value > 0 else "bearish momentum"

    return MacdSnapshot(
        macd=macd_value,
        signal=signal_value,
        histogram=hist_value,
        label=label,
    )


def compute_supertrend(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 10,
    multiplier: float = 3.0,
) -> SupertrendSnapshot:
    if close.empty:
        raise ValueError("Close series is empty")

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()

    hl2 = (high + low) / 2
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    direction = pd.Series(index=close.index, dtype=int)

    direction.iloc[0] = 1

    for i in range(1, len(close)):
        prev = i - 1
        if basic_upper.iloc[i] < final_upper.iloc[prev] or close.iloc[prev] > final_upper.iloc[prev]:
            final_upper.iloc[i] = basic_upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[prev]

        if basic_lower.iloc[i] > final_lower.iloc[prev] or close.iloc[prev] < final_lower.iloc[prev]:
            final_lower.iloc[i] = basic_lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[prev]

        if close.iloc[i] > final_upper.iloc[i]:
            direction.iloc[i] = 1
        elif close.iloc[i] < final_lower.iloc[i]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[prev]

    supertrend = pd.Series(index=close.index, dtype=float)
    supertrend[direction == 1] = final_lower[direction == 1]
    supertrend[direction == -1] = final_upper[direction == -1]

    last_value = float(supertrend.iloc[-1])
    last_direction = int(direction.iloc[-1])
    label = "Bullish" if last_direction == 1 else "Bearish"

    return SupertrendSnapshot(value=last_value, direction=last_direction, label=label)
