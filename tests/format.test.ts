import { describe, expect, it } from 'vitest';
import { formatMarkdown } from '../src/format.js';
import { GeminiOutput } from '../src/schemas.js';

const mockOutput: GeminiOutput = {
  bottom_line: { bias: 'Buy-on-dips while above 23250', key_levels: 'Nifty 23250/23600; BankNifty 51250/51800' },
  indices: {
    nifty: { summary: 'Holding 0.5% gains pre-open', key_level: '23250 support' },
    banknifty: { summary: 'Banks outperform with PSU strength', key_level: '51800 cap' },
    sensex: { summary: 'Mildly green with defensives in charge', key_level: 'Support near 77500' },
  },
  flows: {
    fii: 'FII +₹1,250cr signals return of foreign bids',
    dii: 'DII -₹350cr booking profits',
    interpretation: 'Bias tilts positive while FII buying persists',
  },
  headwinds: {
    vix: 'VIX -3.4% keeps optionality cheap',
    fx: 'USDINR steady near 83.1',
    holiday: 'No holiday disruptions',
    summary: 'Light headwinds, more support for carry trades',
  },
  global_cues: {
    us_futures: 'S&P futures +0.2% supports risk',
    asia_markets: 'Nikkei +1.1%, Hang Seng +0.6%, Shanghai flat',
    yields: 'US10Y 4.05%, India10Y 7.14% calm',
    implication: 'Neutral-to-positive open for India',
  },
  commodities_currency: {
    commodities: 'Brent $78.3 (-0.4%), Gold +0.3%, Silver -0.2%',
    currency: 'DXY 102.3 keeps INR stable',
    sectors_to_watch: 'O&G, metals, rate sensitives',
  },
  pockets_of_strength: {
    sectors: 'PSU banks and autos showing leadership',
    insight: 'Momentum skew favors financials-led breakout',
  },
  calendar: [
    { event: 'India PMI 09:30', impact: 'Services strength supportive' },
    { event: 'US NFP 19:00', impact: 'Could add late volatility' },
  ],
  playbook: {
    nifty_strategy: 'Buy dips above 23250 for 23600; invalidate below 23180',
    banknifty_strategy: 'Longs above 51400, trims near 51800; avoids shorts until break of 51100',
    execution_rules: 'Staggered entries, respect gap rules, keep position sizing moderate',
  },
  markdown_briefing: '',
};

describe('formatMarkdown', () => {
  it('includes all core sections for Telegram', () => {
    const md = formatMarkdown(mockOutput);
    const sections = [
      'Bottom line',
      'Indices',
      'Flows',
      'Headwinds',
      'Global cues',
      'Commodities & FX',
      'Pocket of strength',
      'Calendar',
      'Retail trader playbook',
    ];

    sections.forEach((section) => {
      expect(md).toContain(section);
    });
  });
});
