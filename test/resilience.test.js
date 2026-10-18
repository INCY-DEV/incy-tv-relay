import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { buildApp } from '../src/app.js';
import { Store } from '../src/store.js';

test('readiness has a deadline even when Redis never answers; liveness remains independent', async () => {
  const app = buildApp({ ping: () => new Promise(() => {}) }, { logger: false, readinessTimeoutMs: 40 });
  try {
    const started = Date.now();
    const response = await app.inject({ url: '/readyz' });
    assert.equal(response.statusCode, 503);
    assert.equal(response.headers['retry-after'], '2');
    assert.ok(Date.now() - started < 1000);
    assert.equal((await app.inject({ url: '/healthz' })).statusCode, 200);
  } finally { await app.close(); }
});

test('readiness recovers after a Redis failure', async () => {
  let healthy = false;
  const app = buildApp({ ping: async () => {
    if (!healthy) throw new Error('connection is closed');
    return 'PONG';
  } }, { logger: false });
  try {
    assert.equal((await app.inject({ url: '/readyz' })).statusCode, 503);
    healthy = true;
    assert.equal((await app.inject({ url: '/readyz' })).statusCode, 200);
  } finally { await app.close(); }
});

for (const message of ['Command timed out', 'READONLY You cannot write against a read only replica.']) {
  test(`storage failure returns retryable 503: ${message}`, async () => {
    const app = buildApp({ hitRateLimit: async () => true, claim: async () => { throw new Error(message); } }, { logger: false });
    try {
      const response = await app.inject({ method: 'POST', url: '/pair/init', payload: {
        code: 'ABCDEFGH', sid: Buffer.alloc(16).toString('base64url'), ya: Buffer.alloc(32).toString('base64url'),
      } });
      assert.equal(response.statusCode, 503);
      assert.equal(response.headers['retry-after'], '2');
    } finally { await app.close(); }
  });
}

test('disconnected commands fail without entering an offline queue', async () => {
  const store = Store.fromEnv({ REDIS_URL: process.env.TEST_REDIS_URL || 'redis://127.0.0.1:6379/9' });
  try {
    if (store.redis.status !== 'ready') await once(store.redis, 'ready');
    store.redis.disconnect();
    await assert.rejects(store.claim('ABCDEFGH', 'test', 'test'));
    assert.equal(store.redis.offlineQueue.length, 0);
  } finally { await store.close(); }
});

test('invalid Sentinel configuration is rejected before connecting', () => {
  assert.throws(() => Store.fromEnv({ REDIS_SENTINELS: '[]' }), /REDIS_SENTINELS/);
  assert.throws(() => Store.fromEnv({ REDIS_SENTINELS: '[{"host":"redis","port":0}]' }), /REDIS_SENTINELS/);
});
