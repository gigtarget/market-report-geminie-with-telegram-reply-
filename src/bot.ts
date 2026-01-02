import { Bot, Context } from 'grammy';
import { run, RunnerHandle } from '@grammyjs/runner';
import { readFile } from 'fs/promises';
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

async function loadDefaultInput(): Promise<InputData> {
  const fileUrl = new URL('../sample-input.json', import.meta.url);
  const raw = await readFile(fileUrl, 'utf-8');
  const parsed = JSON.parse(raw);
  return InputSchema.parse(parsed);
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

  if (!config.geminiApiKey) {
    await ctx.reply('GEMINI_API_KEY is not configured. Please set it and try again.');
    return;
  }

  try {
    const progress = await ctx.reply('Using cached input. Contacting Gemini...');
    const briefing = await callGemini(cachedInput);

    await ctx.api.editMessageText(ctx.chat.id, progress.message_id, 'Report ready. Formatting for Telegram...');
    await ctx.reply(briefing.markdown_briefing, { parse_mode: 'Markdown' });
    await ctx.api.editMessageText(
      ctx.chat.id,
      progress.message_id,
      '✅ Report generated and sent. You can rerun /premarket or send new data.',
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
  let progressMessageId: number | null = null;
  let sourceDescription = '';

  try {
    if (!config.geminiApiKey) {
      await ctx.reply('GEMINI_API_KEY is not configured. Please set it and try again.');
      return;
    }

    if (trimmed) {
      const progress = await ctx.reply('Validating payload...');
      progressMessageId = progress.message_id;

      const parsed = JSON.parse(trimmed);
      const validated = InputSchema.parse(parsed);
      setCachedInput(validated);
      sourceDescription = 'provided payload';
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, 'Payload validated. Contacting Gemini...');
    } else if (cachedInput) {
      const progress = await ctx.reply('Using cached input. Contacting Gemini...');
      progressMessageId = progress.message_id;
      sourceDescription = 'cached input';
    } else {
      const progress = await ctx.reply('No payload provided. Loading sample input...');
      progressMessageId = progress.message_id;

      const defaultInput = await loadDefaultInput();
      setCachedInput(defaultInput);
      sourceDescription = 'sample input';
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, 'Sample input loaded. Contacting Gemini...');
    }

    const briefing = await callGemini(getCachedInput()!);

    if (progressMessageId) {
      await ctx.api.editMessageText(
        ctx.chat.id,
        progressMessageId,
        `Report ready using ${sourceDescription || 'cached input'}. Formatting for Telegram...`,
      );
    }

    await ctx.reply(briefing.markdown_briefing, { parse_mode: 'Markdown' });

    if (progressMessageId) {
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, '✅ Report generated and delivered.');
    }
  } catch (err) {
    logger.error({ err }, 'Failed to generate report from direct payload');

    const message =
      err instanceof Error && err.message.includes('GEMINI_API_KEY')
        ? '❌ Failed to generate report because GEMINI_API_KEY is missing.'
        : '❌ Failed to generate report. Check your JSON and try again.';

    if (progressMessageId) {
      await ctx.api.editMessageText(ctx.chat.id, progressMessageId, message);
    } else {
      await ctx.reply(message);
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

export async function initBot(): Promise<void> {
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
      'Send /setinput <json> to cache data, /premarket to use cached data, /report <json> to send and generate immediately, or /report alone to reuse the cached input.',
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

  try {
    await botInstance.api.getMe();
  } catch (err) {
    logger.error({ err }, 'Telegram bot token is invalid or revoked; bot disabled');
    botInstance = null;
    return;
  }

  runner = run(botInstance);
  logger.info('Telegram bot initialized');
}

export function stopBot(): void {
  runner?.stop();
}

export function getBotInstance(): Bot<Context> | null {
  return botInstance;
}
