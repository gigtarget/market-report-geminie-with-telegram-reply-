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

const IndiaSectionSchema = GeminiOutputSchema.pick({
  bottom_line: true,
  indices: true,
  flows: true,
  headwinds: true,
});

const GlobalSectionSchema = GeminiOutputSchema.pick({
  global_cues: true,
  commodities_currency: true,
  pockets_of_strength: true,
});

const PlaybookSectionSchema = GeminiOutputSchema.pick({
  calendar: true,
  playbook: true,
});

function buildSectionPrompt<T>({ description, pickInput }: SectionPrompt<T>, input: InputData): string {
  const jsonInput = JSON.stringify(pickInput(input), null, 2);

  return [
    'You are a sell-side strategist preparing the India pre-market briefing.',
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
  const indiaSection: SectionPrompt<z.infer<typeof IndiaSectionSchema>> = {
    name: 'India indices & flows',
    schema: IndiaSectionSchema,
    description:
      'Return JSON with {"bottom_line": {"bias": string, "key_levels": string}, "indices": {"nifty": {"summary": string, "key_level"?: string}, "banknifty": {"summary": string, "key_level"?: string}, "sensex": {"summary": string, "key_level"?: string}}, "flows": {"fii": string, "dii": string, "interpretation": string}, "headwinds": {"vix"?: string, "fx"?: string, "holiday"?: string, "summary": string}}. Use the supplied levels and percentages directly; avoid guessing numbers.',
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
      'Return JSON with {"global_cues": {"us_futures": string, "asia_markets": string, "yields": string, "implication": string}, "commodities_currency": {"commodities": string, "currency": string, "sectors_to_watch": string}, "pockets_of_strength": {"sectors": string, "insight": string}}. Keep the commentary tied to the provided moves and avoid fabricating prices.',
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
      'Return JSON with {"calendar": [{"event": string, "impact": string}], "playbook": {"nifty_strategy": string, "banknifty_strategy": string, "execution_rules": string}}. Strategies should reference the supplied key levels and tone from the data.',
    pickInput: (data) => ({
      date_ist: data.date_ist,
      events: data.events,
      key_levels: data.key_levels,
      indices: data.indices,
    }),
  };

  const [india, global, playbook] = await Promise.all([
    callGeminiSection(indiaSection, input),
    callGeminiSection(globalSection, input),
    callGeminiSection(playbookSection, input),
  ]);

  const combined = { ...india, ...global, ...playbook } as GeminiOutput;
  const markdown_briefing = formatMarkdown({ ...combined, markdown_briefing: '' } as GeminiOutput);

  return GeminiOutputSchema.parse({ ...combined, markdown_briefing });
}
