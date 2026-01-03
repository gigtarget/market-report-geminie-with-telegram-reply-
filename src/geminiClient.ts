import { z } from "zod";

const geminiResponseSchema = z.object({
  date: z.string(),
  headline: z.string(),
  indices: z.object({
    nifty50: z.object({
      close: z.number(),
      pointChange: z.number(),
      percentChange: z.number(),
    }),
    sensex: z.object({
      close: z.number(),
      pointChange: z.number(),
      percentChange: z.number(),
    }),
    niftyBank: z.object({
      close: z.number(),
      pointChange: z.number(),
      percentChange: z.number(),
    }),
  }),
});

export type MarketIndexKey = "nifty50" | "sensex" | "niftyBank";

export type GeminiMarketResponse = z.infer<typeof geminiResponseSchema>;

const PROMPT = `You are a financial data assistant specializing in the Indian equity market.
Return the latest available market close data for Nifty 50, BSE Sensex, and Nifty Bank.
Use the most recent trading day if markets are currently closed.

Respond strictly as minified JSON that matches this shape:
{
  "date": "Friday, January 2, 2026", // readable calendar date for the close used
  "headline": "Short, clear headline summarizing overall sentiment",
  "indices": {
    "nifty50": {"close": 26328.55, "pointChange": 182.0, "percentChange": 0.70},
    "sensex": {"close": 85762.01, "pointChange": 573.41, "percentChange": 0.67},
    "niftyBank": {"close": 60150.95, "pointChange": 439.55, "percentChange": 0.74}
  }
}

Do NOT add any commentary or Markdown. Return only JSON so it can be parsed directly.
If data is unavailable, estimate conservatively based on the latest reliable closing data.`;

export async function fetchGeminiMarketSnapshot({
  apiKey,
  model,
}: {
  apiKey: string;
  model: string;
}): Promise<GeminiMarketResponse> {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model,
  )}:generateContent`;

  const response = await fetch(`${endpoint}?key=${apiKey}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: [{
        role: "user",
        parts: [{ text: PROMPT }],
      }],
      generationConfig: {
        responseMimeType: "application/json",
      },
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Gemini API error (${response.status}): ${errorText}`);
  }

  const payload = (await response.json()) as {
    candidates?: Array<{
      content?: { parts?: Array<{ text?: string }> };
    }>;
  };

  const rawText = payload.candidates?.[0]?.content?.parts?.[0]?.text;

  if (!rawText) {
    throw new Error("Gemini API returned an empty response");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(rawText);
  } catch (error) {
    throw new Error(`Failed to parse Gemini response as JSON: ${rawText}`);
  }

  const validation = geminiResponseSchema.safeParse(parsed);
  if (!validation.success) {
    throw new Error(
      `Gemini response failed validation: ${validation.error.toString()}`,
    );
  }

  return validation.data;
}
