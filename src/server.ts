import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { config, logger } from './config.js';
import { generateRoute } from './routes/generate.js';
import { initBot } from './bot.js';

const app = Fastify({ logger: { level: logger.level, redact: logger.redact } });

app.register(rateLimit, {
  global: false,
});

app.get('/health', async () => ({ ok: true }));

app.register(generateRoute);

const start = async () => {
  try {
    initBot();
    await app.listen({ port: config.port, host: '0.0.0.0' });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

start();
