# Indian Market Report Telegram Bot (Python)

A production-ready Telegram bot that delivers the latest Indian market close data for Nifty 50, Sensex, and Nifty Bank. Data is pulled directly from Yahoo Finance via `yfinance`, and the bot replies with a formatted report when users send `/report`.


## Features
- `/start` for quick usage help.
- `/report` fetches the most recent trading session close from Yahoo Finance (no hardcoded dates).
- Handles weekends/holidays automatically by using the latest available close.
- Neutral, code-generated summary plus per-index snapshot with absolute and percentage moves.
- Graceful fallback messaging and structured logging for errors.
- Ready for long-running deployment on Railway.

## Requirements
- Python 3.11+
- `TELEGRAM_BOT_TOKEN` environment variable set to your BotFather token.
- `OPENAI_API_KEY` configured for news generation (Responses API with web search).
- `TELEGRAM_REPORT_CHAT_ID` set to the numeric Telegram chat_id for the scheduled report (use `/chatid`).
- Scheduler timezone is `Asia/Kolkata`; daily report runs at **09:05 IST**.

## Setup
1. Install dependencies:
 ```bash
  pip install -r requirements.txt
  ```
2. Export your Telegram token:
  ```bash
  export TELEGRAM_BOT_TOKEN=your_telegram_token
  ```
3. Export your OpenAI API key:
   ```bash
   export OPENAI_API_KEY=your_openai_key
   ```
4. Run the bot locally:
  ```bash
  python main.py
   ```
   The bot will start polling and respond to `/start` and `/report`.

## Deployment to Railway
1. Push this repository to GitHub.
2. Create a new Railway project from the repo.
3. Set the environment variable in Railway:
   - `TELEGRAM_BOT_TOKEN`
   - `OPENAI_API_KEY`
4. Configure the start command:
   ```bash
   python main.py
   ```
5. Deploy. Railway will keep the bot process running for long-lived Telegram polling.

## How it works
- `main.py` houses all logic:
  - Fetches daily data for the three indices using `yfinance` and determines the latest trading session from returned data.
  - Builds a clean text report with close, point change, and percentage change for each index, and a neutral summary line derived from market direction.
  - Handles `/start` and `/report` via `python-telegram-bot` v20+ and logs errors without exposing secrets.

## Notes
- No AI is used for numbers; all values come directly from Yahoo Finance responses.
- Timezone conversion uses Asia/Kolkata to display the correct session date.
- If any index fetch fails, the bot replies with an apologetic error message instead of crashing.
- If you ever suspect the Telegram token is leaked, revoke it in BotFather and update `TELEGRAM_BOT_TOKEN` with the new value immediately.
- Do not run multiple instances with polling; use 1 replica or webhook.
