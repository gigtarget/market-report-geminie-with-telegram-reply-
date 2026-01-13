from __future__ import annotations

import logging
from typing import List, Optional

from report_builder import BreadthSnapshot, KeyLevels, MarketReport, SectorMove
from templates import classify_market, get_opening_line


def _format_number(value: float) -> str:
    return f"{value:,.0f}"


def _format_change(value: float) -> str:
    return f"{value:+,.0f}"


def _format_index_move(name: str, snapshot) -> str:
    direction = "up" if snapshot.percent_change > 0 else "down" if snapshot.percent_change < 0 else "flat"
    return (
        f"{name} {direction} {abs(snapshot.percent_change):.0f}% to {_format_number(snapshot.close)}"
    )


def _breadth_read(breadth: BreadthSnapshot) -> str:
    adv = breadth.advances
    dec = breadth.declines
    if dec == 0:
        ratio_display = "∞"
        ratio = float("inf")
    else:
        ratio = adv / dec
        ratio_display = f"{ratio:.0f}"

    if ratio >= 1.25:
        label = "positive"
    elif ratio >= 0.90:
        label = "neutral"
    else:
        label = "weak"

    return f"A/D {adv}/{dec} ({ratio_display}) → {label}"


def _executive_takeaway(report: MarketReport) -> List[str]:
    bullets: List[str] = []
    indices = {idx.name: idx for idx in report.indices}
    nifty = indices.get("Nifty 50")

    if nifty:
        bullets.append(_format_index_move("Nifty 50", nifty))

    if report.breadth:
        breadth_note = _breadth_read(report.breadth)
        bullets.append(f"Breadth: {breadth_note}.")

    if report.fii_dii and report.fii_dii.fii:
        fii_net = float(report.fii_dii.fii.net)
        flow_state = "buying" if fii_net > 0 else "selling" if fii_net < 0 else "flat"
        bullets.append(f"FII net {flow_state} ({_format_number(fii_net)}).")

    if report.vix:
        arrow = "↑" if report.vix.percent_change > 0 else "↓" if report.vix.percent_change < 0 else "→"
        bullets.append(
            f"VIX {arrow} {report.vix.percent_change:+.0f}% to {report.vix.value:.0f}."
        )

    if len(bullets) < 2:
        bullets.append("Risk dashboard below summarizes breadth, flows, and volatility.")

    return bullets[:3]


def _risk_dashboard(report: MarketReport) -> List[str]:
    lines: List[str] = ["Risk Dashboard:"]

    if report.breadth:
        breadth = report.breadth
        coverage = breadth.coverage_note or "full"
        lines.append(
            f"Breadth: Adv {breadth.advances} | Dec {breadth.declines} | Unch {breadth.unchanged} "
            f"| Coverage {coverage} | {_breadth_read(breadth)}"
        )
    else:
        lines.append("Breadth: unavailable.")

    if report.fii_dii:
        fii_net = report.fii_dii.fii.net if report.fii_dii.fii else 0.0
        dii_net = report.fii_dii.dii.net if report.fii_dii.dii else 0.0
        lines.append(
            f"Flows: FII net {_format_number(float(fii_net))} | DII net {_format_number(float(dii_net))}"
        )
    elif report.fii_dii_warning:
        lines.append(f"Flows: {report.fii_dii_warning}")
    else:
        lines.append("Flows: unavailable.")

    if report.vix:
        arrow = "↑" if report.vix.percent_change > 0 else "↓" if report.vix.percent_change < 0 else "→"
        lines.append(
            f"VIX: {report.vix.value:.0f} ({arrow} {report.vix.percent_change:+.0f}%)"
        )
    else:
        lines.append("VIX: unavailable.")

    return lines


def _key_levels_block(report: MarketReport) -> List[str]:
    lines: List[str] = ["Key Levels (next session | prev-day pivots):"]

    if report.key_levels:
        printed_any = False
        for key in ["Nifty 50", "Nifty Bank", "Sensex"]:
            levels = report.key_levels.get(key)
            if levels:
                lines.append(
                    f"{levels.name}: S1 {levels.s1:,.0f} | Pivot {levels.pivot:,.0f} | "
                    f"R1 {levels.r1:,.0f} | S2 {levels.s2:,.0f} | R2 {levels.r2:,.0f}"
                )
                printed_any = True

        for name, levels in report.key_levels.items():
            if name in {"Nifty 50", "Nifty Bank", "Sensex"}:
                continue
            lines.append(
                f"{levels.name}: S1 {levels.s1:,.0f} | Pivot {levels.pivot:,.0f} | "
                f"R1 {levels.r1:,.0f} | S2 {levels.s2:,.0f} | R2 {levels.r2:,.0f}"
            )
            printed_any = True

        if not printed_any:
            lines.append("Key levels unavailable.")
    else:
        lines.append("Key levels unavailable.")

    nifty_levels = report.key_levels.get("Nifty 50") if report.key_levels else None
    nifty_close = next((idx.close for idx in report.indices if idx.name == "Nifty 50"), None)

    if nifty_levels and nifty_close is not None:
        if nifty_close < nifty_levels.s2:
            pivot_line = (
                f"Close vs Pivot: Nifty 50 closed below S2 (~{nifty_levels.s2:,.0f}) and "
                f"Pivot (~{nifty_levels.pivot:,.0f}) → downside bias; reclaim S1/Pivot to neutralize."
            )
        elif nifty_close < nifty_levels.s1:
            pivot_line = (
                f"Close vs Pivot: Nifty 50 closed below S1 (~{nifty_levels.s1:,.0f}) and "
                f"Pivot (~{nifty_levels.pivot:,.0f}) → cautious bias until reclaimed."
            )
        else:
            position = "above" if nifty_close >= nifty_levels.pivot else "below"
            bias = "constructive bias" if position == "above" else "cautious bias until reclaimed"
            pivot_line = (
                f"Close vs Pivot: Nifty 50 closed {position} Pivot (~{nifty_levels.pivot:,.0f})"
                f" → {bias}."
            )
    else:
        pivot_line = "Close vs Pivot: Nifty 50 pivot unavailable."

    lines.append(pivot_line)
    return lines


def _indicator_block(report: MarketReport) -> List[str]:
    lines = ["Indicators (Daily | TradingView-aligned):"]
    if not report.indicators:
        lines.append("Indicators unavailable.")
        return lines

    for name, indicator in report.indicators.items():
        lines.append(
            f"{name}: RSI(14) {indicator.rsi:.0f} ({indicator.rsi_label}); "
            f"MACD(12,26,9) {indicator.macd:.0f}/{indicator.macd_signal:.0f}/{indicator.macd_hist:.0f} "
            f"({indicator.macd_label}); Supertrend(10,3) {indicator.supertrend_direction} "
            f"@ {indicator.supertrend:,.0f}"
        )

    return lines


def _indices_snapshot(report: MarketReport) -> List[str]:
    lines = ["Market Indices Snapshot:"]
    for idx in report.indices:
        lines.append(
            f"{idx.name}: {_format_number(idx.close)} "
            f"({_format_change(idx.change)} | {_format_change(idx.percent_change)}%)"
        )
    return lines


def _strongest_sector(moves: Optional[List[SectorMove]]) -> Optional[SectorMove]:
    if not moves:
        return None
    return sorted(moves, key=lambda item: item.percent_change, reverse=True)[0]


def _weakest_sectors(moves: Optional[List[SectorMove]], count: int = 3) -> List[SectorMove]:
    if not moves:
        return []
    return sorted(moves, key=lambda item: item.percent_change)[:count]


def _sector_block(moves: Optional[List[SectorMove]], coverage_line: Optional[str]) -> List[str]:
    lines: List[str] = ["Sectors:"]

    if coverage_line:
        lines.append(coverage_line)

    if not moves:
        lines.append("Sector data unavailable.")
        return lines

    strongest = _strongest_sector(moves)
    weakest = _weakest_sectors(moves)

    if strongest:
        lines.append(f"Top strong: {strongest.sector} ({strongest.percent_change:+.0f}%)")
    if weakest:
        weak_line = ", ".join(
            f"{item.sector} ({item.percent_change:+.0f}%)" for item in weakest
        )
        lines.append(f"Top weak: {weak_line}")

    if len(moves) <= 10:
        lines.append("Sector Moves (%):")
        for move in moves:
            lines.append(f"• {move.sector}: {move.percent_change:+.0f}%")

    return lines


def _drivers_block(report: MarketReport) -> List[str]:
    lines = ["Why market moved today (Top 3 drivers):"]
    if report.drivers:
        lines.extend([f"• {driver}" for driver in report.drivers])
    else:
        lines.append("Drivers unavailable.")
    return lines


def _movers_block(report: MarketReport) -> List[str]:
    lines = ["Top movers (NIFTY 100 | 1D %):"]

    if report.movers_warning:
        lines.append(report.movers_warning)

    if report.top_gainers and report.bottom_performers:
        lines.append("Top 5 Gainers:")
        for mover in report.top_gainers:
            lines.append(
                f"• {mover.symbol}: {_format_number(mover.close)} "
                f"({_format_change(mover.percent_change)}%)"
            )

        lines.append("Bottom 5 Performers:")
        for mover in report.bottom_performers:
            lines.append(
                f"• {mover.symbol}: {_format_number(mover.close)} "
                f"({_format_change(mover.percent_change)}%)"
            )
    else:
        lines.append("Movers data unavailable.")

    return lines


def _news_block(report: MarketReport) -> List[str]:
    lines = ["News (Top 5):"]

    if report.news_warning:
        lines.append(report.news_warning)

    if report.news_lines:
        lines.extend([f"• {line}" for line in report.news_lines])
    elif not report.news_warning:
        lines.append("No news highlights available.")

    return lines


def _liveblog_block(report: MarketReport) -> List[str]:
    if report.liveblog_highlights is None and not report.liveblog_warning:
        return []

    lines = ["Market Highlights (Moneycontrol live):"]
    if report.liveblog_highlights:
        lines.extend([f"• {highlight}" for highlight in report.liveblog_highlights])
    elif report.liveblog_warning:
        lines.append(report.liveblog_warning)
    else:
        lines.append("Highlights unavailable today.")

    return lines


def _tomorrows_focus(report: MarketReport) -> List[str]:
    bullets: List[str] = []
    indices = {idx.name: idx for idx in report.indices}

    def _levels_rule(name: str, levels: KeyLevels, close_value: Optional[float]) -> str:
        if close_value is None:
            return (
                f"{name} rule: Hold Pivot (~{levels.pivot:,.0f}) to open R1 (~{levels.r1:,.0f}); "
                f"below Pivot → watch S1 (~{levels.s1:,.0f})."
            )
        if close_value < levels.s2:
            return (
                f"{name} rule: Below S2 (~{levels.s2:,.0f}) → downside risk active; reclaim "
                f"S1 (~{levels.s1:,.0f}) + Pivot (~{levels.pivot:,.0f}) for relief; above Pivot → "
                f"R1 (~{levels.r1:,.0f})."
            )
        if close_value < levels.s1:
            return (
                f"{name} rule: Below S1 (~{levels.s1:,.0f}) → watch S2 (~{levels.s2:,.0f}); "
                f"reclaim Pivot (~{levels.pivot:,.0f}) → R1 (~{levels.r1:,.0f})."
            )
        return (
            f"{name} rule: Hold Pivot (~{levels.pivot:,.0f}) → aim R1 (~{levels.r1:,.0f}); "
            f"below Pivot → watch S1 (~{levels.s1:,.0f})."
        )

    if report.key_levels:
        nifty_levels = report.key_levels.get("Nifty 50")
        bank_levels = report.key_levels.get("Nifty Bank")

        if nifty_levels:
            bullets.append(
                _levels_rule("Nifty", nifty_levels, indices.get("Nifty 50").close if indices.get("Nifty 50") else None)
            )
        if bank_levels:
            bullets.append(
                _levels_rule(
                    "BankNifty",
                    bank_levels,
                    indices.get("Nifty Bank").close if indices.get("Nifty Bank") else None,
                )
            )

    breadth_threshold = 60
    if report.breadth and report.breadth.total:
        breadth_threshold = max(1, int(round(max(60, report.breadth.total * 0.6))))

    weakest = _weakest_sectors(report.sector_moves, count=1)
    sector_focus = weakest[0].sector if weakest else "lagging sectors"
    bullets.append(
        f"Confirmation: Breadth (Adv ≥ {breadth_threshold}) and {sector_focus} turning green → "
        "follow-through; else caution stays."
    )

    bullets = bullets[:5]
    if not bullets:
        return ["What to Watch Next Session:", "• Key sectors and heavyweight stocks for follow-through."]

    return ["What to Watch Next Session:"] + [f"• {b}" for b in bullets]


def format_report(report: MarketReport) -> str:
    opening_line: Optional[str]
    try:
        indices_pct = {idx.name: idx.percent_change for idx in report.indices}
        direction, strength, leader = classify_market(indices_pct, report.market_closed)
        nifty_pct = indices_pct.get("Nifty 50", 0.0)
        sensex_pct = indices_pct.get("Sensex", 0.0)
        banknifty_pct = indices_pct.get("Nifty Bank", 0.0)
        opening_line = get_opening_line(
            report.session_date,
            report.market_closed,
            nifty_pct,
            sensex_pct,
            banknifty_pct,
            leader,
            strength,
            direction,
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("Falling back to summary line: %s", exc)
        opening_line = "Market recap below."

    lines = [f"Post Market Report: {report.session_date.strftime('%Y-%m-%d')}"]

    if report.warning:
        lines.append(report.warning)

    lines.extend(["", opening_line, "", "Executive Takeaway:"])
    lines.extend([f"• {item}" for item in _executive_takeaway(report)])

    lines.extend(["", *(_risk_dashboard(report))])
    lines.extend(["", *(_key_levels_block(report))])
    lines.extend(["", *(_indicator_block(report))])
    lines.extend(["", *(_indices_snapshot(report))])
    lines.extend(["", *(_sector_block(report.sector_moves, report.sector_warning))])
    lines.extend(["", *(_drivers_block(report))])
    lines.extend(["", *(_movers_block(report))])

    if report.fii_dii or report.fii_dii_warning:
        lines.extend(["", "FII/DII (NSE):"])

        if report.fii_dii_warning:
            lines.append(report.fii_dii_warning)

        if report.fii_dii:
            as_on_text = f"As on: {report.fii_dii.as_on}"
            if (
                report.market_closed
                and report.fii_dii.as_on_date
                and report.fii_dii.as_on_date < report.session_date
            ):
                lines.append(
                    f"Market closed — showing last reported FII/DII data (As on: {report.fii_dii.as_on})"
                )
            else:
                lines.append(as_on_text)

            if report.fii_dii.fii:
                lines.append(
                    "FII "
                    f"Buy: {_format_number(report.fii_dii.fii.buy)} | "
                    f"Sell: {_format_number(report.fii_dii.fii.sell)} | "
                    f"Net: {_format_number(report.fii_dii.fii.net)}"
                )
            else:
                lines.append("FII data unavailable")

            if report.fii_dii.dii:
                lines.append(
                    "DII "
                    f"Buy: {_format_number(report.fii_dii.dii.buy)} | "
                    f"Sell: {_format_number(report.fii_dii.dii.sell)} | "
                    f"Net: {_format_number(report.fii_dii.dii.net)}"
                )
            else:
                lines.append("DII data unavailable")

    liveblog_block = _liveblog_block(report)
    if liveblog_block:
        lines.extend(["", *liveblog_block])

    lines.extend(["", *(_news_block(report))])
    lines.extend(["", *(_tomorrows_focus(report))])

    return "\n".join(lines)
