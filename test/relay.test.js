import { test, describe, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { buildApp } from '../src/app.js';
import { Store, MAX_CODE_ATTEMPTS } from '../src/store.js';
import { CODE_ALPHABET } from '../src/validate.js';

// Тесты гоняются на реальном Redis: Lua-скрипты и атомарность — как раз
// то, что мок не проверит, а именно там прячутся гонки.
const REDIS_URL = process.env.TEST_REDIS_URL || 'redis://127.0.0.1:6379/9';

const b64 = (n) => randomBytes(n).toString('base64url');
const makeCode = () => Array.from({ length: 8 },
  () => CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)]).join('');

let store; let app;

before(async () => {
  process.env.REDIS_URL = REDIS_URL;
  store = Store.fromEnv();
  // Быстрый long-poll, чтобы тесты не ждали 30 секунд.
  app = buildApp(store, { logger: false, pollTimeoutMs: 1500, pollIntervalMs: 100 });
  await app.ready();
});

after(async () => {
  await app.close();
  await store.redis.flushdb();
  await store.close();
});

beforeEach(async () => {
  await store.redis.flushdb();
});

const init = (code, sid = b64(16), ya = b64(32)) =>
  app.inject({ method: 'POST', url: '/pair/init', payload: { code, sid, ya } });

describe('полный сценарий пейринга', () => {
  test('TV → Sender → TV: подписка доходит', async () => {
    const code = makeCode();
    const sid = b64(16); const ya = b64(32);

    const r1 = await init(code, sid, ya);
    assert.equal(r1.statusCode, 204, 'init принят');

    // Sender читает параметры TV.
    const r2 = await app.inject({ method: 'GET', url: `/pair/${code}` });
    assert.equal(r2.statusCode, 200);
    assert.deepEqual(r2.json(), { sid, ya }, 'Sender получил ровно то, что положил TV');

    // Sender шлёт зашифрованный payload.
    const yb = b64(32); const nonce = b64(12); const ct = b64(200);
    const r3 = await app.inject({
      method: 'POST', url: `/pair/${code}/send`, payload: { yb, nonce, ct },
    });
    assert.equal(r3.statusCode, 204);

    // TV забирает.
    const r4 = await app.inject({ method: 'GET', url: `/pair/${code}/result` });
    assert.equal(r4.statusCode, 200);
    assert.deepEqual(r4.json(), { yb, nonce, ct }, 'блоб дошёл байт в байт');

    // Повторно забрать нельзя — запись удалена.
    const r5 = await app.inject({ method: 'GET', url: `/pair/${code}/result` });
    assert.equal(r5.statusCode, 404, 'payload одноразовый');
  });

  test('long-poll дожидается payload, отправленного позже', async () => {
    const code = makeCode();
    await init(code);

    // Отправляем через 300 мс — TV в этот момент уже висит на long-poll.
    const sending = new Promise((resolve) => {
      setTimeout(async () => {
        resolve(await app.inject({
          method: 'POST', url: `/pair/${code}/send`,
          payload: { yb: b64(32), nonce: b64(12), ct: b64(120) },
        }));
      }, 300);
    });

    const [result, sent] = await Promise.all([
      app.inject({ method: 'GET', url: `/pair/${code}/result` }),
      sending,
    ]);
    assert.equal(sent.statusCode, 204);
    assert.equal(result.statusCode, 200, 'long-poll дождался');
  });

  test('long-poll возвращает 204, если никто не ответил', async () => {
    const code = makeCode();
    await init(code);
    const r = await app.inject({ method: 'GET', url: `/pair/${code}/result` });
    assert.equal(r.statusCode, 204);
  });
});

describe('защита от перебора', () => {
  test(`код сгорает после ${MAX_CODE_ATTEMPTS} попыток`, async () => {
    const code = makeCode();
    await init(code);

    // Первые MAX попыток проходят (код-то верный).
    for (let i = 0; i < MAX_CODE_ATTEMPTS; i++) {
      const r = await app.inject({ method: 'GET', url: `/pair/${code}` });
      assert.equal(r.statusCode, 200, `попытка ${i + 1} должна пройти`);
    }
    // Следующая — код уничтожен.
    const over = await app.inject({ method: 'GET', url: `/pair/${code}` });
    assert.equal(over.statusCode, 429, 'лимит сработал');

    // И код больше не существует — даже для TV.
    const after = await app.inject({ method: 'GET', url: `/pair/${code}/result` });
    assert.equal(after.statusCode, 404, 'код сгорел полностью');
  });

  test('несуществующий код → 404', async () => {
    const r = await app.inject({ method: 'GET', url: `/pair/${makeCode()}` });
    assert.equal(r.statusCode, 404);
  });
});

describe('одноразовость и занятость', () => {
  test('второй TV не займёт занятый код', async () => {
    const code = makeCode();
    assert.equal((await init(code)).statusCode, 204);
    assert.equal((await init(code)).statusCode, 409, 'код занят');
  });

  test('повторная отправка payload → 409', async () => {
    const code = makeCode();
    await init(code);
    const p = { yb: b64(32), nonce: b64(12), ct: b64(100) };
    assert.equal((await app.inject({
      method: 'POST', url: `/pair/${code}/send`, payload: p,
    })).statusCode, 204);
    assert.equal((await app.inject({
      method: 'POST', url: `/pair/${code}/send`, payload: p,
    })).statusCode, 409, 'payload уже отправлен');
  });

  test('send на несуществующий код → 404', async () => {
    const r = await app.inject({
      method: 'POST', url: `/pair/${makeCode()}/send`,
      payload: { yb: b64(32), nonce: b64(12), ct: b64(50) },
    });
    assert.equal(r.statusCode, 404);
  });
});

describe('валидация входных данных', () => {
  test('плохой код отвергается', async () => {
    for (const bad of ['SHORT', 'TOOLONGCODE', 'ABCDEFGI', 'abcdefgh', '', 'ABCD1234']) {
      const r = await init(bad);
      assert.equal(r.statusCode, 400, `код «${bad}» должен быть отвергнут`);
    }
  });

  test('точка неверной длины отвергается', async () => {
    const code = makeCode();
    assert.equal((await init(code, b64(16), b64(31))).statusCode, 400, 'ya 31 байт');
    assert.equal((await init(code, b64(15), b64(32))).statusCode, 400, 'sid 15 байт');
    assert.equal((await init(code, b64(16), 'не-base64!!')).statusCode, 400, 'не base64url');
  });

  test('nonce и yb неверной длины отвергаются', async () => {
    const code = makeCode();
    await init(code);
    const bad = [
      { yb: b64(31), nonce: b64(12), ct: b64(50) },
      { yb: b64(32), nonce: b64(11), ct: b64(50) },
      { yb: b64(32), nonce: b64(12), ct: 'x' }, // ct короче тега
    ];
    for (const p of bad) {
      const r = await app.inject({ method: 'POST', url: `/pair/${code}/send`, payload: p });
      assert.equal(r.statusCode, 400, `должно быть отвергнуто: ${JSON.stringify(p).slice(0, 60)}`);
    }
  });

  test('слишком большой ct → 413', async () => {
    const code = makeCode();
    await init(code);
    const r = await app.inject({
      method: 'POST', url: `/pair/${code}/send`,
      payload: { yb: b64(32), nonce: b64(12), ct: b64(70 * 1024) },
    });
    assert.equal(r.statusCode, 413);
  });
});

describe('служебные эндпоинты', () => {
  test('healthz и readyz отвечают', async () => {
    assert.equal((await app.inject({ url: '/healthz' })).statusCode, 200);
    assert.equal((await app.inject({ url: '/readyz' })).statusCode, 200);
  });
});

describe('TTL', () => {
  test('запись живёт с TTL и исчезает после истечения', async () => {
    const code = makeCode();
    await init(code);
    const ttl = await store.redis.ttl(`pair:${code}`);
    assert.ok(ttl > 0 && ttl <= 300, `TTL выставлен: ${ttl}`);

    // Эмулируем истечение, не дожидаясь 5 минут.
    await store.redis.del(`pair:${code}`);
    assert.equal((await app.inject({ method: 'GET', url: `/pair/${code}` })).statusCode, 404);
  });

  test('TTL не продлевается при чтении Sender-ом', async () => {
    const code = makeCode();
    await init(code);
    await store.redis.expire(`pair:${code}`, 100);
    await app.inject({ method: 'GET', url: `/pair/${code}` });
    const ttl = await store.redis.ttl(`pair:${code}`);
    assert.ok(ttl <= 100, `TTL сохранён, а не сброшен на 300: ${ttl}`);
  });
});
