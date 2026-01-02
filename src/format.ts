import { GeminiOutput } from './schemas.js';

export function formatMarkdown(output: GeminiOutput): string {
  const calendarRows = output.calendar
    .map((item) => `- **${item.event}** — ${item.impact}`)
    .join('\n');

  return [
    `*India Pre-Market Briefing*`,
    `\n*Bottom line:* ${output.bottom_line.bias}\nKey levels: ${output.bottom_line.key_levels}`,
    `\n*Indices*\n- Nifty 50: ${output.indices.nifty.summary}${output.indices.nifty.key_level ? ` (levels: ${output.indices.nifty.key_level})` : ''}\n- Bank Nifty: ${output.indices.banknifty.summary}${output.indices.banknifty.key_level ? ` (levels: ${output.indices.banknifty.key_level})` : ''}\n- Sensex: ${output.indices.sensex.summary}${output.indices.sensex.key_level ? ` (levels: ${output.indices.sensex.key_level})` : ''}`,
    `\n*Flows*\nFII: ${output.flows.fii}\nDII: ${output.flows.dii}\nTake: ${output.flows.interpretation}`,
    `\n*Headwinds*\n${output.headwinds.summary}${output.headwinds.vix ? `\nVIX: ${output.headwinds.vix}` : ''}${output.headwinds.fx ? `\nFX: ${output.headwinds.fx}` : ''}${output.headwinds.holiday ? `\nHoliday: ${output.headwinds.holiday}` : ''}`,
    `\n*Global cues*\nUS futures: ${output.global_cues.us_futures}\nAsia: ${output.global_cues.asia_markets}\nYields: ${output.global_cues.yields}\nImplication: ${output.global_cues.implication}`,
    `\n*Commodities & FX*\n${output.commodities_currency.commodities}\nCurrency: ${output.commodities_currency.currency}\nSectors to watch: ${output.commodities_currency.sectors_to_watch}`,
    `\n*Pocket of strength*\n${output.pockets_of_strength.sectors}\nInsight: ${output.pockets_of_strength.insight}`,
    `\n*Calendar*\n${calendarRows}`,
    `\n*Retail trader playbook*\nNifty: ${output.playbook.nifty_strategy}\nBankNifty: ${output.playbook.banknifty_strategy}\nExecution: ${output.playbook.execution_rules}`,
  ].join('\n');
}
