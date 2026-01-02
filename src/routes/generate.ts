import { FastifyInstance } from 'fastify';
import { callGemini } from '../gemini.js';
import { formatMarkdown } from '../format.js';
import { InputSchema } from '../schemas.js';
import { sendTelegramMessage, setCachedInput } from '../bot.js';

export async function generateRoute(app: FastifyInstance): Promise<void> {
  app.post('/generate', {
    config: {
      rateLimit: {
        max: 5,
        timeWindow: '1 minute',
      },
    },
    schema: {
      body: { type: 'object' },
    },
    handler: async (request, reply) => {
      const parsed = InputSchema.safeParse(request.body);
      if (!parsed.success) {
        return reply.status(400).send({ error: 'Invalid input', details: parsed.error.format() });
      }

      const input = parsed.data;
      setCachedInput(input);

      try {
        const geminiOutput = await callGemini(input);
        const markdown = formatMarkdown(geminiOutput);

        await sendTelegramMessage(markdown);

        return { data: geminiOutput, markdown };
      } catch (err) {
        request.log.error({ err }, 'Failed to generate briefing');
        return reply.status(500).send({ error: 'Failed to generate briefing' });
      }
    },
  });
}
