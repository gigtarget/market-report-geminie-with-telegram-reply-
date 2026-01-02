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
  if (!ctx.chat) {
    logger.warn('Received /premarket command without chat context');
    return;
  }

  if (!cachedInput) {
    await ctx.reply('No cached input. Use /setinput <json> first.');
    return;
  }

  try {
    const progress = await ctx.reply('Using cached input. Generating report...');
    const briefing = await callGemini(cachedInput);

    await ctx.api.editMessageText(ctx.chat.id, progress.message_id, 'Report generated. Sending to chat...');
    await ctx.reply(briefing.markdown_briefing, { parse_mode: 'Markdown' });
    await ctx.api.editMessageText(
      ctx.chat.id,
      progress.message_id,
      'Report generated and sent. You can rerun /premarket or send new data.',
    );
  } catch (err) {
    logger.error({ err }, 'Failed to generate briefing via bot');
    await ctx.reply('Failed to generate briefing. Please try again later.');
  }
}

async function handleReportPayload(ctx: Context, payloadText: string) {
  if (!ctx.chat) {
    logger.warn('Received report payload without chat context');
    return;
  }

  const trimmed = payloadText.trim();

  if (!trimmed) {
    await ctx.reply('Send the market JSON as plain text or with /report <json>.');
    return;
  }

  let progressMessageId: number | null = null;

  try {
    const parsed = JSON.parse(trimmed);
    const validated = InputSchema.parse(parsed);
    setCachedInput(validated);

    const progress = await ctx.reply('Payload received. Calling Gemini to build your report...');
    progressMessageId = progress.message_id;

    const briefing = await callGemini(validated);

    if (progressMessageId) {
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, 'Report ready. Sending to chat...');
    }

    await ctx.reply(briefing.markdown_briefing, { parse_mode: 'Markdown' });

    if (progressMessageId) {
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, '✅ Report generated and delivered.');
    }
  } catch (err) {
    logger.error({ err }, 'Failed to generate report from direct payload');

    if (progressMessageId) {
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, '❌ Failed to generate report. Check your JSON and try again.');
    } else {
      await ctx.reply('Invalid payload or generation failure. Ensure valid JSON matching the input schema.');
    }
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
  botInstance.command('report', async (ctx: Context) => {
    const text = ctx.message?.text ?? '';
    const payloadText = text.replace(/^\/report\s*/, '');
    await handleReportPayload(ctx, payloadText);
  });
  botInstance.command('start', async (ctx: Context) => {
    await ctx.reply(
      'Send /setinput <json> to cache data, /premarket to use cached data, or /report <json> to send and generate immediately.',
    );
  });

  botInstance.on('message:text', async (ctx) => {
    const text = ctx.message?.text ?? '';

    if (text.startsWith('/')) {
      return;
    }

    await handleReportPayload(ctx, text);
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
