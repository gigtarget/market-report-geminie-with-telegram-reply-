import { GeminiOutput } from './schemas.js';

export function formatMarkdown(output: GeminiOutput): string {
  const drivers = output.drivers
    .map((driver) => `- **${driver.headline}** – ${driver.note}`)
    .join('\n');

  const indices = [
    `- Nifty 50: ${output.domestic_indices.nifty.close.toLocaleString('en-IN')} (${output.domestic_indices.nifty.change_pct.toFixed(
      2,
    )}%) — ${output.domestic_indices.nifty.takeaway}`,
    `- Bank Nifty: ${output.domestic_indices.banknifty.close.toLocaleString('en-IN')} (${output.domestic_indices.banknifty.change_pct.toFixed(
      2,
    )}%) — ${output.domestic_indices.banknifty.takeaway}`,
    `- Sensex: ${output.domestic_indices.sensex.close.toLocaleString('en-IN')} (${output.domestic_indices.sensex.change_pct.toFixed(2)}%) — ${
      output.domestic_indices.sensex.takeaway
    }`,
  ].join('\n');

  const headwinds =
    output.headwinds.length > 0
      ? output.headwinds
          .map((item) => `- ${item.title}: ${item.data_point} → ${item.implication}`)
          .join('\n')
      : '- None flagged.';

  const calendarRows =
    output.calendar.length > 0
      ? output.calendar
          .map((item) => `- **${item.event}** @ ${item.time_ist} — ${item.impact}`)
          .join('\n')
      : '- None scheduled.';

  const rules = output.playbook.execution_rules.map((rule, idx) => `${idx + 1}. ${rule}`).join('\n');

  return [
    `1️⃣ Cover Summary`,
    `\nTitle: India Pre-Market Briefing`,
    `Subtitle: Your Tactical Edge for ${output.report_date}`,
    output.cover_summary.bias_line,
    `\n2️⃣ Today’s Bottom Line (Bias Gauge)`,
    `Bias: ${output.bottom_line.bias}`,
    `${output.bottom_line.rationale}`,
    `Trend: ${output.bottom_line.trend_condition}`,
    `Bounces: ${output.bottom_line.bounce_behavior}`,
    `Conviction: ${output.bottom_line.conviction}`,
    `\n3️⃣ Nifty 50 – Critical Levels`,
    `Current/close: ${output.nifty_levels.current}`,
    `Resistance: ${output.nifty_levels.resistance}`,
    `Support: ${output.nifty_levels.support}`,
    `"${output.nifty_levels.takeaway}"`,
    `\n4️⃣ Top 3 Market Drivers Today`,
    drivers,
    `\n5️⃣ Domestic Indices Snapshot`,
    indices,
    `\n6️⃣ Institutional Flow Analysis`,
    `FII: ₹${output.flows.fii_cr.toLocaleString('en-IN')} Cr`,
    `DII: ₹${output.flows.dii_cr.toLocaleString('en-IN')} Cr`,
    `${output.flows.dominance}`,
    `${output.flows.absorption}`,
    `${output.flows.sustainability}`,
    `\n7️⃣ Domestic Headwinds (If Any)`,
    headwinds,
    `\n8️⃣ Global Market Cues`,
    `US Futures: ${output.global_cues.us_futures}`,
    `US 10Y: ${output.global_cues.us10y}`,
    `India 10Y: ${output.global_cues.india10y}`,
    `Asia: ${output.global_cues.asia_markets}`,
    `Bottom line for India’s open: ${output.global_cues.bottom_line}`,
    `\n9️⃣ Commodities & Currency – Sector Impact`,
    `Crude oil: ${output.commodities_currency.crude.move}. Reason: ${output.commodities_currency.crude.reason}. Sector impact: ${output.commodities_currency.crude.sector_impact}.`,
    `Gold & Silver: ${output.commodities_currency.gold_silver.move}. Reason: ${output.commodities_currency.gold_silver.reason}. Sector impact: ${output.commodities_currency.gold_silver.sector_impact}.`,
    `USD/INR: ${output.commodities_currency.usd_inr.move}. Reason: ${output.commodities_currency.usd_inr.reason}. Sector impact: ${output.commodities_currency.usd_inr.sector_impact}.`,
    `\n🔟 Relative Strength / Weakness Pocket`,
    `${output.pocket_of_strength.sector}: ${output.pocket_of_strength.reason}. ${output.pocket_of_strength.tradeable}. Action: ${output.pocket_of_strength.actionable}.`,
    `\n1️⃣1️⃣ Today’s Key Events Calendar`,
    calendarRows,
    `\n1️⃣2️⃣ Retail Trader Playbook (3 Parts)`,
    `(1/3) Nifty Strategy: ${output.playbook.nifty.bias}. Resistance: ${output.playbook.nifty.resistance}. Support: ${output.playbook.nifty.support}. Trigger: ${output.playbook.nifty.trigger}.`,
    `(2/3) Bank Nifty Strategy: ${output.playbook.banknifty.bias}. Invalidation: ${output.playbook.banknifty.invalidation}. Bias changes if ${output.playbook.banknifty.change_condition}.`,
    `(3/3) Execution Rules:\n${rules}`,
  ].join('\n');
}
