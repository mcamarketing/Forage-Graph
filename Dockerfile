ARG CACHE_DATE=2026-04-06
FROM node:20-alpine AS builder
WORKDIR /app
ARG CACHE_DATE
COPY package*.json ./
RUN npm ci
COPY . .
RUN [ -d dist ] && echo "Using pre-compiled dist" || npm run build

FROM node:20-alpine
WORKDIR /app
ARG CACHE_DATE
COPY --from=builder /app/dist ./dist
COPY package*.json ./
RUN npm ci --omit=dev
EXPOSE 3000
# Rebuild trigger Wed Apr 08 01:45:00 UTC 2026 - healthcheck fix
CMD ["node", "dist/server.js"]