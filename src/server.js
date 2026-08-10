import { buildApp } from './app.js';
import { Store } from './store.js';

const store = Store.fromEnv();
const app = buildApp(store);

const port = Number(process.env.PORT || 8080);
const host = process.env.HOST || '0.0.0.0';

// Корректное завершение: k8s шлёт SIGTERM и ждёт. app.close() дожидается
// активных запросов, включая long-poll — поэтому terminationGracePeriod
// в манифесте больше 30 секунд, которые тот может висеть.
//
// Повторный сигнал не игнорируем молча: если выключение зависло, второй
// SIGTERM должен убить процесс, а не копить обработчики.
let shuttingDown = false;
const shutdown = async (signal) => {
  if (shuttingDown) {
    app.log.warn({ signal }, 'forced exit');
    process.exit(1);
  }
  shuttingDown = true;
  app.log.info({ signal }, 'shutting down');
  try {
    await app.close();
    await store.close();
  } finally {
    process.exit(0);
  }
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

try {
  await app.listen({ port, host });
  app.log.info({ port }, 'incy-tv-relay started');
} catch (err) {
  app.log.error({ err: err.message }, 'failed to start');
  process.exit(1);
}
