import Fastify from 'fastify';
import { isValidCode, decodeB64url, checkCiphertext, SIZES, MAX_CT_BYTES } from './validate.js';
import { TTL_SECONDS } from './store.js';

// Лимиты из спецификации. Лимит на код (в store.js) важнее лимита на IP:
// именно он делает перебор бесполезным, потому что переход на другой IP
// от него не спасает.
export const LIMITS = {
  // 300/мин: за одним адресом CGNAT могут быть сотни устройств, а телевизор
  // на long-poll делает ~2 запроса в минуту. Защиту от перебора это не
  // ослабляет — она держится на лимите попыток по коду, а не по IP.
  perIpPerMinute: 300,
  bodyBytes: 64 * 1024,
};

// Long-poll: TV висит до 30 секунд. Доставка идёт через pub/sub, а этот
// интервал — только страховка от потерянного уведомления (publish в зазоре
// между чтением и подпиской). Поэтому он большой: холостых обращений почти нет.
const POLL_TIMEOUT_MS = 30_000;
const FALLBACK_CHECK_MS = 5_000;

// Признаки того, что упал не запрос, а хранилище. ioredis не даёт одного
// кода на все случаи, поэтому смотрим и на код, и на текст.
function isUnavailable(err) {
  const code = err?.code ?? '';
  if (['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND'].includes(code)) {
    return true;
  }
  const msg = String(err?.message ?? '');
  return /connection is closed|stream isn't writeable|max retries|connect/i.test(msg);
}

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
  const fallbackCheckMs = opts.pollIntervalMs ?? FALLBACK_CHECK_MS;

  // Тело больше лимита — 413, как в спецификации (Fastify по умолчанию 400).
  app.setErrorHandler((err, req, reply) => {
    if (err.statusCode === 413 || err.code === 'FST_ERR_CTP_BODY_TOO_LARGE') {
      return reply.code(413).send({ error: 'payload_too_large' });
    }
    if (err.statusCode === 400 || err.code === 'FST_ERR_CTP_INVALID_MEDIA_TYPE') {
      return reply.code(400).send({ error: 'bad_request' });
    }
    // Redis недоступен — это временно, и клиенту стоит повторить.
    // 500 он бы показал пользователю как «ошибка», 503 с Retry-After
    // читается однозначно.
    if (isUnavailable(err)) {
      req.log.error({ err: err.message }, 'storage unavailable');
      return reply.code(503).header('Retry-After', '2').send({ error: 'unavailable' });
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
  // Отдельного лимита на init нет намеренно. Он защищал бы только от
  // заполнения Redis, а для этого нужны тысячи запросов в секунду — их
  // и так режет общий лимит на IP, плюс volatile-ttl вытесняет старые
  // записи. Зато почасовой лимит ломал реальные сценарии: за CGNAT
  // оператора или в общежитии за одним адресом сотни телевизоров, и
  // одиннадцатый пользователь не смог бы добавить подписку.
  app.post('/pair/init', async (req, reply) => {
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
    // Отдельного поля nonce нет: первые 12 байт ct — это он. Клиенты шлют
    // `combined` из CryptoKit / libsodium одним куском, и релею незачем его
    // разбирать — содержимое он всё равно не проверяет и проверить не может.
    // Границы размера в checkCiphertext уже учитывают nonce.
    if (!checkCiphertext(body.ct)) {
      const tooBig = typeof body.ct === 'string'
        && body.ct.length > Math.ceil((MAX_CT_BYTES * 4) / 3) + 4;
      return reply.code(tooBig ? 413 : 400).send({
        error: tooBig ? 'payload_too_large' : 'bad_ct',
      });
    }

    const res = await store.putPayload(code, {
      yb: body.yb, ct: body.ct,
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
    // Клиент мог отвалиться — прекращаем ожидание, чтобы не держать
    // подписку и соединение к Redis впустую.
    const ac = new AbortController();
    req.raw.on('close', () => ac.abort());

    // Подписываемся ДО первой проверки: иначе publish, случившийся между
    // чтением записи и подпиской, потерялся бы — pub/sub не хранит
    // пропущенные сообщения. Одна подписка на весь запрос, а не на итерацию.
    const watcher = await store.watch(code);

    try {
      for (;;) {
        const res = await store.takePayload(code);
        if (res.status === 'ok') {
          return reply.code(200).send(res.payload);
        }
        if (res.status === 'notfound') {
          return reply.code(404).send({ error: 'not_found' });
        }

        const left = deadline - Date.now();
        if (ac.signal.aborted || left <= 0) {
          // Ничего не пришло за отведённое время — TV повторит запрос.
          return reply.code(204).send();
        }

        // Ждём уведомления вместо опроса раз в 500 мс: при тысячах висящих
        // клиентов опрос давал бы тысячи холостых обращений к Redis в секунду.
        // Потолок — страховка на случай, если событие всё же не дойдёт.
        await watcher.wait(Math.min(left, fallbackCheckMs), ac.signal);
      }
    } finally {
      watcher.close();
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
