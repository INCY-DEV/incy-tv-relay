// Optional integration check: starts a disposable local Redis/Sentinel cluster.
// Requires redis-server on PATH; never connects to production.
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import Redis from 'ioredis';
import net from 'node:net';
import { Store } from '../src/store.js';
import { buildApp } from '../src/app.js';

const dir = await mkdtemp(join(tmpdir(), 'relay-sentinel-'));
const processes = [], clients = [];
let store, app;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, timeout = 20000) {
  const end = Date.now() + timeout;
  let last;
  while (Date.now() < end) {
    try { if (await check()) return; } catch (e) { last = e; }
    await sleep(100);
  }
  throw new Error(`deadline waiting for integration cluster: ${last?.message || 'condition false'}`);
}
try {
  const reservations = await Promise.all(Array.from({length:6}, () => new Promise((resolve,reject) => {
    const server = net.createServer();
    server.once('error',reject);
    server.listen(0,'127.0.0.1',()=>resolve(server));
  })));
  const ports = reservations.map(server=>server.address().port);
  await Promise.all(reservations.map(server=>new Promise(resolve=>server.close(resolve))));
  for (let i = 0; i < 3; i++) {
    const redis = spawn('redis-server', ['--port', String(ports[i]), '--bind', '127.0.0.1',
      '--save', '', '--appendonly', 'no', '--dir', dir, '--dbfilename', `redis-${i}.rdb`,
      ...(i ? ['--replicaof', '127.0.0.1', String(ports[0])] : [])], { stdio: 'ignore' });
    processes.push(redis);
    const cfg = join(dir, `sentinel-${i}.conf`);
    await writeFile(cfg, `port ${ports[3+i]}\nbind 127.0.0.1\ndir ${dir}\nsentinel monitor tv-relay 127.0.0.1 ${ports[0]} 2\nsentinel down-after-milliseconds tv-relay 3000\nsentinel failover-timeout tv-relay 15000\n`);
    processes.push(spawn('redis-server', [cfg, '--sentinel'], { stdio: 'ignore' }));
  }
  const sentinel = new Redis({ host:'127.0.0.1',port:ports[3],commandTimeout:5000 });
  sentinel.on('error',()=>{}); clients.push(sentinel);
  const master = new Redis({ host:'127.0.0.1',port:ports[0],commandTimeout:5000 });
  master.on('error',()=>{}); clients.push(master);
  await until(async () => (await sentinel.sentinel('CKQUORUM','tv-relay')).startsWith('OK'));
  await until(async () => ((await master.info('replication')).match(/state=online/g)||[]).length===2);
  store = Store.fromEnv({ REDIS_SENTINELS: JSON.stringify([0,1,2].map(i=>({host:'127.0.0.1',port:ports[3+i]}))) });
  await until(async () => await store.ping() === 'PONG');
  app = buildApp(store, { logger: false });
  const code='ABCDEFGH', sid=Buffer.alloc(16,1).toString('base64url'), ya=Buffer.alloc(32,2).toString('base64url');
  assert.equal((await app.inject({method:'POST',url:'/pair/init',payload:{code,sid,ya}})).statusCode,204);
  assert.equal(await store.redis.wait(2,1000),2);
  const oldPort = store.redis.connector.stream.remotePort;
  await sentinel.sentinel('FAILOVER','tv-relay');
  await until(async () => {
    const address = await sentinel.sentinel('get-master-addr-by-name','tv-relay');
    return Number(address[1]) !== ports[0] && store.redis.status === 'ready' && store.redis.connector.stream.remotePort !== oldPort;
  });
  const read=await app.inject({url:`/pair/${code}`});
  assert.equal(read.statusCode,200); assert.deepEqual(read.json(),{sid,ya});
  const payload={yb:Buffer.alloc(32,3).toString('base64url'),ct:Buffer.alloc(64,4).toString('base64url')};
  assert.equal((await app.inject({method:'POST',url:`/pair/${code}/send`,payload})).statusCode,204);
  const result=await app.inject({url:`/pair/${code}/result`});
  assert.equal(result.statusCode,200); assert.deepEqual(result.json(),payload);
  console.log('PASS: Sentinel promotion, automatic reconnect, existing session and post-failover writes');
} finally {
  if(app) await app.close();
  if(store) await store.close();
  for(const client of clients) client.disconnect();
  for(const process of processes) process.kill('SIGTERM');
  await Promise.all(processes.map(p => p.exitCode !== null ? Promise.resolve() : new Promise(r=>p.once('exit',r))));
  await rm(dir,{recursive:true,force:true});
}
