import dotenv from 'dotenv';
import pino from 'pino';

if (!process.env.RAILWAY_ENVIRONMENT && process.env.NODE_ENV !== 'production') {
  dotenv.config();
}

export const config = {
  port: parseInt(process.env.PORT ?? '3000', 10),
  geminiApiKey: process.env.GEMINI_API_KEY,
  geminiModel: process.env.GEMINI_MODEL ?? 'gemini-2.5-flash',
  telegramToken: process.env.TELEGRAM_BOT_TOKEN,
  telegramChatId: process.env.TELEGRAM_CHAT_ID,
};

export const loggerOptions = {
  level: process.env.LOG_LEVEL ?? 'info',
  redact: ['geminiApiKey', 'telegramToken', 'telegramChatId'],
} as const satisfies pino.LoggerOptions;

export const logger = pino(loggerOptions);
