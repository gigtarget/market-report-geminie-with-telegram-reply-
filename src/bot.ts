import { Bot, Context } from 'grammy';
import { run, RunnerHandle } from '@grammyjs/runner';
import { config, logger } from './config.js';
import { InputData, InputSchema } from './schemas.js';
import { callGemini } from './gemini.js';

let cachedInput: InputData | null = null;
let botInstance: Bot<Context> | null = null;
let runner: RunnerHandle | null = null;

export function getCachedInput(): InputData | null {
  return cachedInput;
}

export function setCachedInput(data: InputData): void {
  cachedInput = data;
}

async function handleSetInput(ctx: Context) {
  const text = ctx.message?.text ?? '';
  const payloadText = text.replace(/^\/setinput\s*/, '').trim();

  if (!payloadText) {
    await ctx.reply('Send JSON payload after /setinput');
    return;
  }

  try {
    const parsed = JSON.parse(payloadText);
    const validated = InputSchema.parse(parsed);
    setCachedInput(validated);
    await ctx.reply('Cached input updated. Use /premarket to generate.');
  } catch (err) {
    logger.warn({ err }, 'Invalid /setinput payload');
    await ctx.reply('Invalid payload. Ensure valid JSON matching the input schema.');
  }
}

async function handlePremarket(ctx: Context) {
  if (!cachedInput) {
    await ctx.reply('No cached input. Use /setinput <json> first.');
    return;
  }

  try {
    const briefing = await callGemini(cachedInput);
    await ctx.reply(briefing.markdown_briefing, { parse_mode: 'Markdown' });
  } catch (err) {
    logger.error({ err }, 'Failed to generate briefing via bot');
    await ctx.reply('Failed to generate briefing. Please try again later.');
  }
}

export async function sendTelegramMessage(markdown: string): Promise<void> {
  if (!config.telegramChatId || !botInstance) return;

  try {
    await botInstance.api.sendMessage(Number(config.telegramChatId), markdown, {
      parse_mode: 'Markdown',
      link_preview_options: { is_disabled: true },
    });
  } catch (err) {
    logger.error({ err }, 'Failed to send Telegram message');
  }
}

export function initBot(): void {
  if (!config.telegramToken) {
    logger.info('Telegram bot token not set; bot disabled');
    return;
  }

  botInstance = new Bot(config.telegramToken);

  botInstance.command('setinput', handleSetInput);
  botInstance.command('premarket', handlePremarket);
  botInstance.command('start', async (ctx: Context) => {
    await ctx.reply('Send /setinput <json> to cache data, then /premarket to generate the briefing.');
  });

  botInstance.catch((err: unknown) => logger.error({ err }, 'Bot handler threw'));

  runner = run(botInstance);
  logger.info('Telegram bot initialized');
}

export function stopBot(): void {
  runner?.stop();
}

export function getBotInstance(): Bot<Context> | null {
  return botInstance;
}
