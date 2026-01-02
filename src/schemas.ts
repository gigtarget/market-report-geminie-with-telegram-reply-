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
  bottom_line: z.object({
    bias: z.string(),
    key_levels: z.string(),
  }),
  indices: z.object({
    nifty: z.object({ summary: z.string(), key_level: z.string().optional() }),
    banknifty: z.object({ summary: z.string(), key_level: z.string().optional() }),
    sensex: z.object({ summary: z.string(), key_level: z.string().optional() }),
  }),
  flows: z.object({
    fii: z.string(),
    dii: z.string(),
    interpretation: z.string(),
  }),
  headwinds: z.object({
    vix: z.string().optional(),
    fx: z.string().optional(),
    holiday: z.string().optional(),
    summary: z.string(),
  }),
  global_cues: z.object({
    us_futures: z.string(),
    asia_markets: z.string(),
    yields: z.string(),
    implication: z.string(),
  }),
  commodities_currency: z.object({
    commodities: z.string(),
    currency: z.string(),
    sectors_to_watch: z.string(),
  }),
  pockets_of_strength: z.object({
    sectors: z.string(),
    insight: z.string(),
  }),
  calendar: z.array(
    z.object({
      event: z.string(),
      impact: z.string(),
    })
  ),
  playbook: z.object({
    nifty_strategy: z.string(),
    banknifty_strategy: z.string(),
    execution_rules: z.string(),
  }),
  markdown_briefing: z.string(),
});

export type GeminiOutput = z.infer<typeof GeminiOutputSchema>;
