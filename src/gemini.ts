import { z } from 'zod';
import { config, logger } from './config.js';
import { formatMarkdown } from './format.js';
import { GeminiOutput, GeminiOutputSchema, InputData } from './schemas.js';

const GEMINI_URL = (model: string) =>
  `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;

type SectionPrompt<T> = {
  name: string;
  schema: z.ZodSchema<T>;
  description: string;
  pickInput: (input: InputData) => unknown;
};

const DomesticSectionSchema = GeminiOutputSchema.pick({
  report_date: true,
  cover_summary: true,
  bottom_line: true,
  nifty_levels: true,
  drivers: true,
  domestic_indices: true,
  flows: true,
  headwinds: true,
});

const GlobalSectionSchema = GeminiOutputSchema.pick({
  global_cues: true,
  commodities_currency: true,
  pocket_of_strength: true,
});

const PlaybookSectionSchema = GeminiOutputSchema.pick({
  calendar: true,
  playbook: true,
});

function buildSectionPrompt<T>({ description, pickInput }: SectionPrompt<T>, input: InputData): string {
  const jsonInput = JSON.stringify(pickInput(input), null, 2);

  return [
    'You are a professional institutional market strategist preparing an India pre-market tactical briefing.',
    'Tone: calm, analytical, cautious, institutional; no fluff, no motivational language, no emojis.',
    'Return ONLY valid JSON for the requested section with no markdown and no extra keys.',
    description,
    'Source data (JSON):',
    jsonInput,
  ].join('\n');
}

async function callGeminiSection<T>(spec: SectionPrompt<T>, input: InputData): Promise<T> {
  if (!config.geminiApiKey) {
    throw new Error('GEMINI_API_KEY is required');
  }

  const prompt = buildSectionPrompt(spec, input);

  const response = await fetch(GEMINI_URL(config.geminiModel), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-goog-api-key': config.geminiApiKey,
    },
    body: JSON.stringify({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: {
        responseMimeType: 'application/json',
      },
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    logger.error({ status: response.status, body: text, section: spec.name }, 'Gemini request failed');
    throw new Error(`Gemini request failed for section ${spec.name}`);
  }

  const payload = await response.json();
  const candidateText: unknown = payload?.candidates?.[0]?.content?.parts?.[0]?.text;

  if (typeof candidateText !== 'string') {
    logger.error({ payload, section: spec.name }, 'Gemini response missing text');
    throw new Error(`Gemini response missing text for section ${spec.name}`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(candidateText);
  } catch (err) {
    logger.error({ err, section: spec.name }, 'Failed to parse Gemini JSON');
    throw new Error(`Failed to parse Gemini JSON for section ${spec.name}`);
  }

  const validated = spec.schema.safeParse(parsed);
  if (!validated.success) {
    logger.error({ issues: validated.error.format(), section: spec.name }, 'Gemini validation failed');
    throw new Error(`Gemini output validation failed for section ${spec.name}`);
  }

  return validated.data;
}

export async function callGemini(input: InputData): Promise<GeminiOutput> {
  const indiaSection: SectionPrompt<z.infer<typeof DomesticSectionSchema>> = {
    name: 'Domestic structure',
    schema: DomesticSectionSchema,
    description:
      'Return JSON with keys {"report_date": string, "cover_summary": {"bias_line": string}, "bottom_line": {"bias": string, "rationale": string, "trend_condition": string, "bounce_behavior": string, "conviction": string}, "nifty_levels": {"current": string, "resistance": string, "support": string, "takeaway": string}, "drivers": [{"headline": string, "note": string}] (exactly 3), "domestic_indices": {"nifty": {"close": number, "change_pct": number, "takeaway": string}, "banknifty": {...}, "sensex": {...}}, "flows": {"fii_cr": number, "dii_cr": number, "dominance": string, "absorption": string, "sustainability": string}, "headwinds": [{"title": string, "data_point": string, "implication": string}] with up to 3 items. Keep numbers and levels grounded in provided data and avoid fabricating new figures.',
    pickInput: (data) => ({
      date_ist: data.date_ist,
      indices: data.indices,
      key_levels: data.key_levels,
      flows: data.flows,
      volatility: data.volatility,
      fx: data.fx,
      narrative_overrides: data.narrative_overrides,
    }),
  };

  const globalSection: SectionPrompt<z.infer<typeof GlobalSectionSchema>> = {
    name: 'Global cues & commodities',
    schema: GlobalSectionSchema,
    description:
      'Return JSON with {"global_cues": {"us_futures": string, "us10y": string, "india10y": string, "asia_markets": string, "bottom_line": string}, "commodities_currency": {"crude": {"move": string, "reason": string, "sector_impact": string}, "gold_silver": {...}, "usd_inr": {...}}, "pocket_of_strength": {"sector": string, "reason": string, "tradeable": string, "actionable": string}}. Commentary should mirror the supplied percentage moves and not invent new prices.',
    pickInput: (data) => ({
      date_ist: data.date_ist,
      global: data.global,
      commodities: data.commodities,
      fx: data.fx,
    }),
  };

  const playbookSection: SectionPrompt<z.infer<typeof PlaybookSectionSchema>> = {
    name: 'Calendar & playbook',
    schema: PlaybookSectionSchema,
    description:
      'Return JSON with {"calendar": [{"event": string, "time_ist": string, "impact": string}], "playbook": {"nifty": {"bias": string, "resistance": string, "support": string, "trigger": string}, "banknifty": {"bias": string, "invalidation": string, "change_condition": string}, "execution_rules": [string, string, string]}}. Tie triggers and levels back to supplied support/resistance data.',
    pickInput: (data) => ({
      date_ist: data.date_ist,
      events: data.events,
      key_levels: data.key_levels,
      indices: data.indices,
    }),
  };

  const [domestic, global, playbook] = await Promise.all([
    callGeminiSection(indiaSection, input),
    callGeminiSection(globalSection, input),
    callGeminiSection(playbookSection, input),
  ]);

  const combined = { ...domestic, ...global, ...playbook } as GeminiOutput;
  const markdown_briefing = formatMarkdown({ ...combined, markdown_briefing: '' } as GeminiOutput);

  return GeminiOutputSchema.parse({ ...combined, markdown_briefing });
}
