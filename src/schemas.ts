import { z } from 'zod';

const indexSnapshot = z.object({
  current: z.number(),
  change_pct: z.number(),
  close: z.number().optional(),
});

const keyLevels = z.object({
  support: z.number().optional(),
  resistance: z.number(),
  zone: z.string().optional(),
  pivot: z.number().optional(),
});

const globalAsia = z.object({
  nikkei: z.number().optional(),
  shanghai: z.string().optional(),
  hongkong: z.string().optional(),
});

const globalYields = z.object({
  us10y: z.number().optional(),
  india10y: z.number().optional(),
});

const commodityMove = z.object({
  price: z.number().optional(),
  change_pct: z.number().optional(),
  note: z.string().optional(),
});

const marketDriver = z.object({
  headline: z.string(),
  note: z.string(),
});

const indexBrief = z.object({
  close: z.number(),
  change_pct: z.number(),
  takeaway: z.string(),
});

const headwind = z.object({
  title: z.string(),
  data_point: z.string(),
  implication: z.string(),
});

const commodityBlock = z.object({
  move: z.string(),
  reason: z.string(),
  sector_impact: z.string(),
});

const calendarEvent = z.object({
  event: z.string(),
  time_ist: z.string(),
  impact: z.string(),
});

export const InputSchema = z.object({
  date_ist: z.string(),
  indices: z.object({
    nifty: indexSnapshot,
    banknifty: indexSnapshot,
    sensex: indexSnapshot,
  }),
  key_levels: z.object({
    nifty: keyLevels.extend({ support: z.number() }),
    banknifty: z.object({
      resistance: z.number(),
      pivot: z.number().optional(),
    }),
  }),
  flows: z.object({
    fii_cr: z.number(),
    dii_cr: z.number(),
    asof_date_ist: z.string(),
  }),
  volatility: z.object({
    india_vix_change_pct: z.number().optional(),
    note: z.string().optional(),
  }),
  fx: z.object({
    usdinr: z.number().optional(),
    dxy: z.number().optional(),
    note: z.string().optional(),
  }),
  global: z.object({
    sp500_futures_change_pct: z.number().optional(),
    asia: globalAsia,
    yields: globalYields,
  }),
  commodities: z.object({
    brent: commodityMove.optional(),
    gold: commodityMove.partial().optional(),
    silver: commodityMove.partial().optional(),
  }),
  events: z.array(
    z.object({
      title: z.string(),
      when_ist: z.string(),
      impact: z.string(),
    })
  ),
  narrative_overrides: z
    .object({
      bias: z.string().optional(),
      key_takeaway: z.string().optional(),
    })
    .optional(),
});

export type InputData = z.infer<typeof InputSchema>;

export const GeminiOutputSchema = z.object({
  report_date: z.string(),
  cover_summary: z.object({
    bias_line: z.string(),
  }),
  bottom_line: z.object({
    bias: z.string(),
    rationale: z.string(),
    trend_condition: z.string(),
    bounce_behavior: z.string(),
    conviction: z.string(),
  }),
  nifty_levels: z.object({
    current: z.string(),
    resistance: z.string(),
    support: z.string(),
    takeaway: z.string(),
  }),
  drivers: z.array(marketDriver).length(3),
  domestic_indices: z.object({
    nifty: indexBrief,
    banknifty: indexBrief,
    sensex: indexBrief,
  }),
  flows: z.object({
    fii_cr: z.number(),
    dii_cr: z.number(),
    dominance: z.string(),
    absorption: z.string(),
    sustainability: z.string(),
  }),
  headwinds: z.array(headwind).max(3),
  global_cues: z.object({
    us_futures: z.string(),
    us10y: z.string(),
    india10y: z.string(),
    asia_markets: z.string(),
    bottom_line: z.string(),
  }),
  commodities_currency: z.object({
    crude: commodityBlock,
    gold_silver: commodityBlock,
    usd_inr: commodityBlock,
  }),
  pocket_of_strength: z.object({
    sector: z.string(),
    reason: z.string(),
    tradeable: z.string(),
    actionable: z.string(),
  }),
  calendar: z.array(calendarEvent),
  playbook: z.object({
    nifty: z.object({
      bias: z.string(),
      resistance: z.string(),
      support: z.string(),
      trigger: z.string(),
    }),
    banknifty: z.object({
      bias: z.string(),
      invalidation: z.string(),
      change_condition: z.string(),
    }),
    execution_rules: z.array(z.string()).length(3),
  }),
  markdown_briefing: z.string(),
});

export type GeminiOutput = z.infer<typeof GeminiOutputSchema>;
