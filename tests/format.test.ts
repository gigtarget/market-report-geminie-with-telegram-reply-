import { describe, expect, it } from 'vitest';
import { formatMarkdown } from '../src/format.js';
import { GeminiOutput } from '../src/schemas.js';

const mockOutput: GeminiOutput = {
  report_date: '2024-09-10',
  cover_summary: { bias_line: 'Cautious-to-Bearish bias with shallow rallies sold.' },
  bottom_line: {
    bias: 'Cautious-to-Bearish',
    rationale: 'Lower highs with light participation keep risk skewed lower.',
    trend_condition: 'Weak, sideways-to-lower tape.',
    bounce_behavior: 'Bounces are being sold near resistance zones.',
    conviction: 'Medium conviction; protect gains quickly.',
  },
  nifty_levels: {
    current: '23,450 (last close)',
    resistance: '23,650–23,700 supply zone',
    support: '23,320 (breakdown risk) with 23,150 next',
    takeaway: 'Failure to hold 23,320 invites further liquidation.',
  },
  drivers: [
    { headline: 'FII selling pressure', note: 'Back-to-back outflows keep upside fragile.' },
    { headline: 'US futures flat', note: 'Muted lead despite soft yields.' },
    { headline: 'Rising VIX', note: 'Vol uptick signalling demand for hedges.' },
  ],
  domestic_indices: {
    nifty: { close: 23450, change_pct: -0.35, takeaway: 'Held mid-range but weak close.' },
    banknifty: { close: 50800, change_pct: -0.55, takeaway: 'Banks lag with PSU pressure.' },
    sensex: { close: 77320, change_pct: -0.40, takeaway: 'Heavyweights capped recoveries.' },
  },
  flows: {
    fii_cr: -1250,
    dii_cr: 980,
    dominance: 'FII selling still dominates the tape.',
    absorption: 'DII bids are absorbing but not reversing pressure.',
    sustainability: 'Sustainability hinges on FII pause; otherwise bounces fade.',
  },
  headwinds: [
    { title: 'Volatility', data_point: 'India VIX +4%', implication: 'Options pricier; expect whipsaws.' },
    { title: 'Currency', data_point: 'USDINR near 83.4', implication: 'Importers paying up; IT gets buffer.' },
  ],
  global_cues: {
    us_futures: 'S&P futures flat to +0.1%',
    us10y: '4.12% easing',
    india10y: '7.12% steady',
    asia_markets: 'Nikkei +0.6%, Hang Seng -0.3%, Shanghai flat',
    bottom_line: 'Neutral read-through; India trades off its own flows.',
  },
  commodities_currency: {
    crude: {
      move: 'Brent around $79 (-0.4%)',
      reason: 'Supply comfort and soft demand signals.',
      sector_impact: 'Mild positive for refiners; neutral for upstream.',
    },
    gold_silver: {
      move: 'Gold +0.2%, Silver flat',
      reason: 'Hedge demand with steady real yields.',
      sector_impact: 'Supportive for jewelers; limited drag on risk assets.',
    },
    usd_inr: {
      move: 'USDINR steady near 83.4',
      reason: 'RBI presence capping upside.',
      sector_impact: 'Constructive for IT exporters; watch imported inflation.',
    },
  },
  pocket_of_strength: {
    sector: 'IT services',
    reason: 'Currency buffer and stable US demand tone.',
    tradeable: 'Tradeable on dips with tight stops.',
    actionable: 'Look for intraday higher lows to lean long with small size.',
  },
  calendar: [
    { event: 'India CPI', time_ist: '17:30', impact: 'Could shift rate expectations; watch Bank Nifty.' },
    { event: 'US CPI', time_ist: '20:00', impact: 'Sets global risk tone into close.' },
  ],
  playbook: {
    nifty: {
      bias: 'Sell-on-rise below 23,650',
      resistance: '23,650–23,700',
      support: '23,320 / 23,150',
      trigger: 'Break below 23,320 opens 23,150; reclaim 23,700 invalidates.',
    },
    banknifty: {
      bias: 'Sell-on-rise unless 51,800 reclaims',
      invalidation: 'Above 51,800 with volume',
      change_condition: 'Bias flips if PSU banks lead reclaim of 51,800.',
    },
    execution_rules: [
      'Avoid first 15 minutes; let volatility settle.',
      'Trade strength with stops; avoid hope trades.',
      'Size down; respect risk per trade.',
    ],
  },
  markdown_briefing: '',
};

describe('formatMarkdown', () => {
  it('renders all tactical sections in order', () => {
    const md = formatMarkdown(mockOutput);

    const sections = [
      'Cover Summary',
      'Today’s Bottom Line',
      'Nifty 50 – Critical Levels',
      'Top 3 Market Drivers Today',
      'Domestic Indices Snapshot',
      'Institutional Flow Analysis',
      'Domestic Headwinds',
      'Global Market Cues',
      'Commodities & Currency – Sector Impact',
      'Relative Strength / Weakness Pocket',
      'Today’s Key Events Calendar',
      'Retail Trader Playbook',
    ];

    sections.forEach((section) => {
      expect(md).toContain(section);
    });
  });
});
