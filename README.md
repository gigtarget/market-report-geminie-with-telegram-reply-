# India Pre-Market Briefing

Production-ready Fastify + TypeScript service that calls Gemini to generate an India pre-market briefing, returns validated JSON, and pushes Markdown updates to Telegram.

## Features
- POST `/generate` validates market data with Zod, calls Gemini with a strict JSON prompt, returns structured output plus Markdown, and optionally sends the Markdown to a Telegram chat.
- GET `/health` returns `{ ok: true }`.
- Telegram bot commands:
  - `/setinput <json>` caches the payload in memory (validated).
  - `/premarket` generates a briefing using the cached payload.
  - `/report <json>` (or sending the JSON as a plain text message) validates the payload, triggers generation immediately, and streams status updates back in Telegram.
- Basic rate limiting on `/generate`.
- Deploy-ready for Railway (listens on `PORT`, includes Procfile/start script). Railway Cron can call `/generate`.

## Tech stack
- Node.js 20, TypeScript
- Fastify HTTP server with `@fastify/rate-limit`
- Telegram via `grammy`
- Gemini API over REST
- Zod validation
- Pino logging (with secret redaction)

## Getting started (local)
1. Install dependencies:
   ```bash
   npm install
   ```
2. Copy env template and set keys:
   ```bash
   cp .env.example .env  # create file and populate values
   ```
   Required variables:
   - `GEMINI_API_KEY`
   - `GEMINI_MODEL` (default `gemini-2.5-flash` if omitted)
  - `TELEGRAM_BOT_TOKEN` (optional for bot)
  - `TELEGRAM_CHAT_ID` (optional for push from /generate)
  - `TELEGRAM_WEBHOOK_URL` (optional, set to enable webhook delivery instead of long polling)
   - `PORT` (defaults to 3000)
3. Run in dev mode:
   ```bash
   npm run dev
   ```
4. Build & run production bundle:
   ```bash
   npm run build
   npm start
   ```

## API usage
- Generate briefing (uses `sample-input.json` as example):
  ```bash
  curl -X POST http://localhost:3000/generate \
    -H "Content-Type: application/json" \
    -d @sample-input.json
  ```
- Health check:
  ```bash
  curl http://localhost:3000/health
  ```

## Direct Gemini call example
```bash
curl -X POST "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent" \
  -H "Content-Type: application/json" \
  -H "x-goog-api-key: $GEMINI_API_KEY" \
  -d '{"contents":[{"role":"user","parts":[{"text":"Return ONLY valid JSON."}]}],"generationConfig":{"responseMimeType":"application/json"}}'
```

## Telegram bot commands
- `/setinput {json}` – caches the payload used for generation.
- `/premarket` – generates and replies with the Markdown briefing using the cached payload.
- If no cached payload exists, the bot replies with instructions.

## Railway deployment
- The app listens on `process.env.PORT`.
- Start command (already in `package.json`):
  ```bash
  npm start
  ```
- Procfile is provided for Railway detection (`web: node dist/server.js`).
- Deploy steps:
  1. Create a new Railway project and link this repository.
  2. Set environment variables in Railway (`GEMINI_API_KEY`, optional `GEMINI_MODEL`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `PORT`).
  3. Build command: `npm run build` (Railway auto-runs when using Node projects).
  4. Start command: `npm start`.

### Railway Cron
Configure a Railway Cron job to POST to your deployed `/generate` endpoint. Example cron payload (replace URL and attach the JSON body you want Gemini to use):
```bash
curl -X POST "https://<your-railway-app>.railway.app/generate" \
  -H "Content-Type: application/json" \
  -d @sample-input.json
```
Ensure the cron job includes the market JSON body; the service caches that payload for Telegram `/premarket` requests.

## Project structure
```
/src
  bot.ts        # Telegram bot setup and cached input handling
  config.ts     # environment + logging
  format.ts     # Markdown formatter for Telegram
  gemini.ts     # Gemini REST client with Zod validation
  routes/generate.ts # Fastify route for /generate
  schemas.ts    # Zod schemas for input and Gemini output
  server.ts     # Fastify bootstrap and health route
sample-input.json
```

## Testing
Run unit tests (Vitest):
```bash
npm test
```
