import { config, logger } from './config.js';
import { InputData, GeminiOutput, GeminiOutputSchema } from './schemas.js';

const GEMINI_URL = (model: string) =>
  `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;

function buildPrompt(input: InputData): string {
  const jsonInput = JSON.stringify(input, null, 2);
  return `You are a sell-side strategist preparing the India pre-market briefing. Use the supplied market data to craft a concise, actionable outlook.\n\nRules:\n- Return ONLY valid JSON. No markdown. No extra keys.\n- Output must follow this structure:\n{\n  "bottom_line": {"bias": string, "key_levels": string},\n  "indices": {"nifty": {"summary": string, "key_level"?: string}, "banknifty": {"summary": string, "key_level"?: string}, "sensex": {"summary": string, "key_level"?: string}},\n  "flows": {"fii": string, "dii": string, "interpretation": string},\n  "headwinds": {"vix"?: string, "fx"?: string, "holiday"?: string, "summary": string},\n  "global_cues": {"us_futures": string, "asia_markets": string, "yields": string, "implication": string},\n  "commodities_currency": {"commodities": string, "currency": string, "sectors_to_watch": string},\n  "pockets_of_strength": {"sectors": string, "insight": string},\n  "calendar": [{"event": string, "impact": string}],\n  "playbook": {"nifty_strategy": string, "banknifty_strategy": string, "execution_rules": string},\n  "markdown_briefing": string\n}\n- The markdown_briefing must be Telegram-ready Markdown with all sections present.\n- Keep language crisp and bias-driven.\n\nSource market data (JSON):\n${jsonInput}`;
}

export async function callGemini(input: InputData): Promise<GeminiOutput> {
  if (!config.geminiApiKey) {
    throw new Error('GEMINI_API_KEY is required');
  }

  const prompt = buildPrompt(input);

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
    logger.error({ status: response.status, body: text }, 'Gemini request failed');
    throw new Error('Gemini request failed');
  }

  const payload = await response.json();
  const candidateText: unknown = payload?.candidates?.[0]?.content?.parts?.[0]?.text;

  if (typeof candidateText !== 'string') {
    logger.error({ payload }, 'Gemini response missing text');
    throw new Error('Gemini response missing text');
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(candidateText);
  } catch (err) {
    logger.error({ err }, 'Failed to parse Gemini JSON');
    throw new Error('Failed to parse Gemini JSON');
  }

  const validated = GeminiOutputSchema.safeParse(parsed);
  if (!validated.success) {
    logger.error({ issues: validated.error.format() }, 'Gemini validation failed');
    throw new Error('Gemini output validation failed');
  }

  return validated.data;
}
