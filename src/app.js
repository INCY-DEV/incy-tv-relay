import Fastify from 'fastify';
import { isValidCode, decodeB64url, checkCiphertext, SIZES, MAX_CT_BYTES } from './validate.js';
import { TTL_SECONDS } from './store.js';

// Лимиты из спецификации. Лимит на код (в store.js) важнее лимита на IP:
// именно он делает перебор бесполезным, потому что переход на другой IP
// от него не спасает.
export const LIMITS = {
  perIpPerMinute: 30,
  initPerIpPerHour: 10,
  bodyBytes: 64 * 1024,
};

// Long-poll: TV висит до 30 секунд, опрашивая Redis. Интервал 500 мс —
// компромисс между задержкой доставки и нагрузкой на Redis.
const POLL_TIMEOUT_MS = 30_000;
const POLL_INTERVAL_MS = 500;

export function buildApp(store, opts = {}) {
  const app = Fastify({
    // Тела не логируем: там шифротекст, но логи имеют свойство утекать.
    logger: opts.logger ?? { level: process.env.LOG_LEVEL || 'info' },
    bodyLimit: LIMITS.bodyBytes,
    trustProxy: true, // за ingress-nginx нужен реальный IP клиента
    // Автологирование запросов выключено: тела содержат шифротекст,
    // а логи имеют свойство утекать. В Fastify 5 опция помечена
    // deprecated, но замена (logController) требует экземпляр класса —
    // ради одной строки её тянуть незачем.
    disableRequestLogging: true,
  });

  const pollTimeout = opts.pollTimeoutMs ?? POLL_TIMEOUT_MS;
  const pollInterval = opts.pollIntervalMs ?? POLL_INTERVAL_MS;

  // Тело больше лимита — 413, как в спецификации (Fastify по умолчанию 400).
  app.setErrorHandler((err, req, reply) => {
    if (err.statusCode === 413 || err.code === 'FST_ERR_CTP_BODY_TOO_LARGE') {
      return reply.code(413).send({ error: 'payload_too_large' });
    }
    if (err.statusCode === 400 || err.code === 'FST_ERR_CTP_INVALID_MEDIA_TYPE') {
      return reply.code(400).send({ error: 'bad_request' });
    }
    // Внутреннюю ошибку наружу не раскрываем: пишем в лог, отдаём код.
    req.log.error({ err: err.message }, 'request failed');
    return reply.code(500).send({ error: 'internal' });
  });

  const clientIp = (req) => req.ip || 'unknown';

  // Общий лимит на IP — грубая защита от флуда.
  app.addHook('onRequest', async (req, reply) => {
    if (req.url === '/healthz' || req.url === '/readyz') return;
    const ok = await store
      .hitRateLimit(`ip:${clientIp(req)}`, LIMITS.perIpPerMinute, 60)
      .catch(() => true); // Redis недоступен — не блокируем на этом слое
    if (!ok) {
      return reply.code(429).send({ error: 'rate_limited' });
    }
  });

  // ── TV занимает код ────────────────────────────────────────────────────
  app.post('/pair/init', async (req, reply) => {
    const okInit = await store
      .hitRateLimit(`init:${clientIp(req)}`, LIMITS.initPerIpPerHour, 3600)
      .catch(() => true);
    if (!okInit) return reply.code(429).send({ error: 'rate_limited' });

    const body = req.body ?? {};
    if (!isValidCode(body.code)) {
      return reply.code(400).send({ error: 'bad_code' });
    }
    if (!decodeB64url(body.sid, SIZES.sid)) {
      return reply.code(400).send({ error: 'bad_sid' });
    }
    if (!decodeB64url(body.ya, SIZES.point)) {
      return reply.code(400).send({ error: 'bad_ya' });
    }

    const claimed = await store.claim(body.code, body.sid, body.ya);
    if (!claimed) return reply.code(409).send({ error: 'code_taken' });
    return reply.code(204).send();
  });

  // ── Sender читает {sid, ya} ────────────────────────────────────────────
  // Этот эндпоинт защищает от перебора: каждое обращение — попытка.
  app.get('/pair/:code', async (req, reply) => {
    const { code } = req.params;
    if (!isValidCode(code)) return reply.code(404).send({ error: 'not_found' });

    const res = await store.fetchForSender(code);
    if (res.status === 'locked') {
      return reply.code(429).send({ error: 'too_many_attempts' });
    }
    if (res.status !== 'ok') {
      return reply.code(404).send({ error: 'not_found' });
    }
    return reply.code(200).send({ sid: res.sid, ya: res.ya });
  });

  // ── Sender отправляет зашифрованный payload ────────────────────────────
  app.post('/pair/:code/send', async (req, reply) => {
    const { code } = req.params;
    if (!isValidCode(code)) return reply.code(404).send({ error: 'not_found' });

    const body = req.body ?? {};
    if (!decodeB64url(body.yb, SIZES.point)) {
      return reply.code(400).send({ error: 'bad_yb' });
    }
    if (!decodeB64url(body.nonce, SIZES.nonce)) {
      return reply.code(400).send({ error: 'bad_nonce' });
    }
    // Содержимое ct релей не проверяет и проверить не может —
    // только границы размера.
    if (!checkCiphertext(body.ct)) {
      const tooBig = typeof body.ct === 'string'
        && body.ct.length > Math.ceil((MAX_CT_BYTES * 4) / 3) + 4;
      return reply.code(tooBig ? 413 : 400).send({
        error: tooBig ? 'payload_too_large' : 'bad_ct',
      });
    }

    const res = await store.putPayload(code, {
      yb: body.yb, nonce: body.nonce, ct: body.ct,
    });
    if (res === 'notfound') return reply.code(404).send({ error: 'not_found' });
    if (res === 'exists') return reply.code(409).send({ error: 'already_sent' });
    return reply.code(204).send();
  });

  // ── TV забирает результат (long-poll) ──────────────────────────────────
  app.get('/pair/:code/result', async (req, reply) => {
    const { code } = req.params;
    if (!isValidCode(code)) return reply.code(404).send({ error: 'not_found' });

    const deadline = Date.now() + pollTimeout;
    // Клиент мог отвалиться — прекращаем опрос, чтобы не жечь Redis впустую.
    let aborted = false;
    req.raw.on('close', () => { aborted = true; });

    for (;;) {
      const res = await store.takePayload(code);
      if (res.status === 'ok') {
        return reply.code(200).send(res.payload);
      }
      if (res.status === 'notfound') {
        return reply.code(404).send({ error: 'not_found' });
      }
      if (aborted || Date.now() + pollInterval >= deadline) {
        // Ничего не пришло за отведённое время — TV повторит запрос.
        return reply.code(204).send();
      }
      await new Promise((r) => setTimeout(r, pollInterval));
    }
  });

  // ── Служебные ──────────────────────────────────────────────────────────
  app.get('/healthz', async () => ({ ok: true }));

  app.get('/readyz', async (req, reply) => {
    try {
      await store.ping();
      return { ok: true, ttl: TTL_SECONDS };
    } catch {
      return reply.code(503).send({ ok: false, error: 'redis_unavailable' });
    }
  });

  return app;
}
