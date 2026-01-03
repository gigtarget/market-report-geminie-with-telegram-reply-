import type { GeminiMarketResponse, MarketIndexKey } from "./geminiClient.js";

const INDEX_LABELS: Record<MarketIndexKey, string> = {
  nifty50: "Nifty 50",
  sensex: "BSE Sensex",
  niftyBank: "Nifty Bank",
};

function formatNumber(value: number): string {
  return value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatReport(data: GeminiMarketResponse): string {
  const { date, headline, indices } = data;

  const lines: string[] = [];
  lines.push(`As of the market close on ${date}:`);
  lines.push("");
  lines.push(headline.trim());
  lines.push("");
  lines.push("Market Indices Snapshot:");

  (Object.keys(indices) as MarketIndexKey[]).forEach((key) => {
    const entry = indices[key];
    const label = INDEX_LABELS[key];
    const direction = entry.pointChange >= 0 ? "+" : "";
    lines.push(
      `${label}: ${formatNumber(entry.close)} (${direction}${formatNumber(
        entry.pointChange,
      )} | ${direction}${entry.percentChange.toFixed(2)}%)`,
    );
  });

  return lines.join("\n");
}
