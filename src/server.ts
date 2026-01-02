import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { config, loggerOptions } from './config.js';
import { generateRoute } from './routes/generate.js';
import { initBot } from './bot.js';

const app = Fastify({ logger: loggerOptions });

// @fastify/rate-limit v10 requires Fastify v5+. On environments still pinned to
// Fastify v4 (like the current Railway deploy), registering the plugin throws a
// version mismatch error and crashes the process after a few seconds. Detect
// the Fastify major version and skip the plugin when incompatible so the
// service keeps running instead of exiting.
const fastifyMajor = Number.parseInt(app.version.split('.')[0] ?? '0', 10);

if (fastifyMajor >= 5) {
  app.register(rateLimit, { global: false });
} else {
  app.log.warn(
    { fastifyVersion: app.version },
    'Skipping @fastify/rate-limit: requires Fastify v5+',
  );
}

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
