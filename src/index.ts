import { Bot } from "grammy";
import dotenv from "dotenv";
import { fetchGeminiMarketSnapshot } from "./geminiClient.js";
import { formatReport } from "./reportFormatter.js";

dotenv.config();

const telegramToken = process.env.TELEGRAM_BOT_TOKEN;
const geminiApiKey = process.env.GEMINI_API_KEY;
const geminiModel = process.env.GEMINI_MODEL || "gemini-2.5-flash";

if (!telegramToken) {
  throw new Error("TELEGRAM_BOT_TOKEN is not set");
}

if (!geminiApiKey) {
  throw new Error("GEMINI_API_KEY is not set");
}

const bot = new Bot(telegramToken);

bot.api.setMyCommands([{ command: "report", description: "Get the latest Indian market snapshot" }]);

bot.catch((err) => {
  console.error("Bot error", err);
});

bot.command("report", async (ctx) => {
  const loadingMessage = await ctx.reply("Generating your market report...");

  try {
    const snapshot = await fetchGeminiMarketSnapshot({
      apiKey: geminiApiKey,
      model: geminiModel,
    });

    const report = formatReport(snapshot);
    await ctx.api.editMessageText(ctx.chat.id, loadingMessage.message_id, report);
  } catch (error) {
    console.error("Failed to generate report", error);
    await ctx.api.editMessageText(
      ctx.chat.id,
      loadingMessage.message_id,
      "Sorry, I couldn't build the market report right now. Please try again shortly.",
    );
  }
});

bot.on("message", (ctx) => {
  if (ctx.message.text?.startsWith("/")) {
    return; // ignore other commands
  }
});

bot.start();
console.log("Telegram market report bot is running...");
