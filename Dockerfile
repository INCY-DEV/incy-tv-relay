# Многоступенчатая сборка: в финальный образ не попадают ни npm-кеш,
# ни dev-зависимости, ни тесты.
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

# Работаем не от root: у релея нет причин иметь лишние права.
RUN addgroup -S relay && adduser -S relay -G relay

COPY --from=deps --chown=relay:relay /app/node_modules ./node_modules
COPY --chown=relay:relay package.json ./
COPY --chown=relay:relay src ./src

USER relay
EXPOSE 8080

# Проверка живости внутри контейнера — на случай, если под завис,
# но процесс формально жив.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD node -e "fetch('http://127.0.0.1:8080/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "src/server.js"]
