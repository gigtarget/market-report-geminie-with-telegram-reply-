# Gemini-powered Indian Market Report Telegram Bot

A small Telegram bot that fetches the latest Indian equity market close data and news tone via the Gemini API, formats it as a concise report, and replies when users send `/report`. Designed for easy deployment to Railway.

## Features
- `/report` command triggers on-demand report generation only.
- Pulls structured close data for Nifty 50, BSE Sensex, and Nifty Bank through Gemini.
- Validates Gemini responses to guard against malformed payloads.
- Cleanly formatted, Telegram-friendly message with headline and snapshot.

## Prerequisites
- Node.js 18+ (for native `fetch`).
- Gemini API access and a Telegram bot token.
- Railway account (or any Node-compatible host).

## Setup
1. Install dependencies:
   ```bash
   npm install
   ```
2. Create a `.env` file:
   ```bash
   GEMINI_API_KEY=your_gemini_api_key
   GEMINI_MODEL=gemini-2.5-flash
   TELEGRAM_BOT_TOKEN=your_telegram_bot_token
   ```
3. Run locally:
   ```bash
   npm run dev
   ```
   The bot will start polling Telegram and log when ready.

## Deployment to Railway
1. Push this repository to GitHub.
2. Create a new Railway project from the repo and set the following environment variables in Railway:
   - `GEMINI_API_KEY`
   - `GEMINI_MODEL` (optional, defaults to `gemini-2.5-flash`)
   - `TELEGRAM_BOT_TOKEN`
3. Set the Railway start command to:
   ```bash
   npm run start
   ```
4. Deploy. The bot runs continuously as a Telegram service and responds to `/report`.

## How it works
- `src/geminiClient.ts` prompts Gemini to return a strict JSON payload with the latest close data and headline.
- `src/reportFormatter.ts` turns that structured response into the Telegram-friendly report text.
- `src/index.ts` wires the Telegram command handler, fetches data from Gemini on demand, and sends the formatted message.

## Environment variables
- `GEMINI_API_KEY` *(required)*: Gemini API key.
- `GEMINI_MODEL` *(optional)*: Gemini model name; defaults to `gemini-2.5-flash`.
- `TELEGRAM_BOT_TOKEN` *(required)*: Token from BotFather.

## Notes
- The bot only reacts to `/report`; other messages are ignored.
- Errors are logged and a friendly fallback message is sent if Gemini is unreachable or returns invalid data.
