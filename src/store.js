import Redis from 'ioredis';

// Хранилище пейринг-сессий.
//
// Всё живёт в Redis с TTL — на диск ничего не пишется, историю не храним.
// Операции, где важна атомарность (занять код, забрать результат, счётчик
// попыток), выполняются серверными Lua-скриптами: между GET и SET успевает
// вклиниться параллельный запрос, а тут это означало бы либо потерю payload,
// либо обход лимита попыток.
//
// Тела скриптов — константы этого файла; всё, что приходит от клиента,
// передаётся через KEYS/ARGV и для Lua остаётся данными, а не кодом.

export const TTL_SECONDS = 300; // 5 минут, как в спецификации
export const MAX_CODE_ATTEMPTS = 5; // после 5 неудач код сгорает

const key = (code) => `pair:${code}`;
// Канал уведомлений: TV ждёт здесь, отправитель публикует при записи.
const channel = (code) => `ch:${code}`;

// Sender читает {sid, ya}. Это же считается попыткой подбора кода,
// поэтому инкремент счётчика и проверка лимита — одна операция.
const LUA_FETCH = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then return cjson.encode({status='notfound'}) end
  local rec = cjson.decode(raw)
  rec.attempts = (rec.attempts or 0) + 1
  if rec.attempts > tonumber(ARGV[1]) then
    -- Превышен лимит: код сгорает целиком, чтобы перебор
    -- не продолжился с другого IP.
    redis.call('DEL', KEYS[1])
    return cjson.encode({status='locked'})
  end
  local ttl = redis.call('TTL', KEYS[1])
  if ttl < 0 then ttl = tonumber(ARGV[2]) end
  redis.call('SET', KEYS[1], cjson.encode(rec), 'EX', ttl)
  return cjson.encode({status='ok', sid=rec.sid, ya=rec.ya})
`;

// Одноразовая запись payload: проверка «уже есть» и запись должны быть
// атомарны, иначе два параллельных отправителя перезапишут друг друга.
const LUA_PUT = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then return 'notfound' end
  local rec = cjson.decode(raw)
  if rec.payload then return 'exists' end
  rec.payload = cjson.decode(ARGV[1])
  local ttl = redis.call('TTL', KEYS[1])
  if ttl < 0 then ttl = tonumber(ARGV[2]) end
  redis.call('SET', KEYS[1], cjson.encode(rec), 'EX', ttl)
  -- Будим TV, который висит на long-poll. Он может ждать на другом поде,
  -- поэтому именно pub/sub, а не событие в памяти процесса.
  redis.call('PUBLISH', KEYS[2], '1')
  return 'ok'
`;

// TV забирает результат: читаем и удаляем одной операцией, payload
// одноразовый и второй раз выдаваться не должен.
const LUA_TAKE = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then return 'notfound' end
  local rec = cjson.decode(raw)
  if not rec.payload then return 'empty' end
  redis.call('DEL', KEYS[1])
  return cjson.encode(rec.payload)
`;

// Счётчик с окном: INCR + EXPIRE на первом попадании.
const LUA_RATELIMIT = `
  local n = redis.call('INCR', KEYS[1])
  if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
  if n > tonumber(ARGV[2]) then return 0 end
  return 1
`;

export class Store {
  constructor(redis) {
    this.redis = redis;
  }

  static fromEnv(env = process.env) {
    const redis = new Redis(env.REDIS_URL || 'redis://127.0.0.1:6379', {
      // Пейринг живёт 5 минут — долго копить запросы смысла нет,
      // но очередь нужна: без неё команды, посланные до установки
      // соединения (старт пода, рестарт Redis), падают с ошибкой
      // вместо того, чтобы дождаться подключения.
      maxRetriesPerRequest: 2,
      connectTimeout: 3000,
      enableOfflineQueue: true,
    });
    return new Store(redis);
  }

  // TV занимает код. SET NX атомарен, поэтому двум TV один код не достанется.
  async claim(code, sid, ya) {
    const ok = await this.redis.set(
      key(code),
      JSON.stringify({ sid, ya, attempts: 0 }),
      'EX', TTL_SECONDS,
      'NX',
    );
    return ok === 'OK';
  }

  // Возвращает {status: 'ok'|'notfound'|'locked', sid?, ya?}
  async fetchForSender(code) {
    const out = await this.redis.eval(
      LUA_FETCH, 1, key(code), MAX_CODE_ATTEMPTS, TTL_SECONDS,
    );
    return JSON.parse(out);
  }

  // 'ok' | 'exists' | 'notfound'
  async putPayload(code, payload) {
    return this.redis.eval(
      LUA_PUT, 2, key(code), channel(code), JSON.stringify(payload), TTL_SECONDS,
    );
  }

  // Подписка на уведомление о записи. Возвращает объект с методом wait():
  // подписаться нужно ДО первой проверки записи, иначе publish, случившийся
  // в зазоре между чтением и подпиской, будет потерян — pub/sub не хранит
  // пропущенные сообщения.
  //
  // Отдельное соединение обязательно: в режиме subscribe Redis не принимает
  // обычные команды, а основной клиент нужен для чтения записи.
  async watch(code) {
    const sub = this.redis.duplicate();
    let fired = false;
    let notify = null;

    sub.on('message', () => {
      fired = true;
      if (notify) { notify(); notify = null; }
    });

    try {
      await sub.subscribe(channel(code));
    } catch {
      // Подписка не удалась — вызывающий код переживёт: он всё равно
      // перепроверяет запись по таймауту.
      sub.disconnect();
      return { wait: async (ms) => { await new Promise((r) => setTimeout(r, ms)); },
               close: () => {} };
    }

    return {
      // Ждём событие не дольше ms. Если оно уже приходило — возвращаемся сразу.
      async wait(ms, signal) {
        if (fired) { fired = false; return true; }
        return new Promise((resolve) => {
          const done = (v) => {
            clearTimeout(timer);
            notify = null;
            resolve(v);
          };
          const timer = setTimeout(() => done(false), ms);
          notify = () => done(true);
          signal?.addEventListener?.('abort', () => done(false), { once: true });
        });
      },
      close() { sub.disconnect(); },
    };
  }

  // {status: 'ok'|'empty'|'notfound', payload?}
  async takePayload(code) {
    const out = await this.redis.eval(LUA_TAKE, 1, key(code));
    if (out === 'notfound' || out === 'empty') return { status: out };
    return { status: 'ok', payload: JSON.parse(out) };
  }

  // Лимит запросов. Фиксированное окно: скользящее здесь не окупается,
  // а это дёшево и без дополнительных структур.
  async hitRateLimit(bucket, limit, windowSec) {
    const ok = await this.redis.eval(
      LUA_RATELIMIT, 1, `rl:${bucket}`, windowSec, limit,
    );
    return ok === 1;
  }

  async ping() {
    return this.redis.ping();
  }

  async close() {
    await this.redis.quit().catch(() => {});
  }
}
