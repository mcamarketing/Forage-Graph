ARG CACHE_DATE=2026-04-06
FROM node:20-alpine AS builder
WORKDIR /app
ARG CACHE_DATE
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ARG CACHE_DATE
COPY --from=builder /app/dist ./dist
COPY package*.json ./
RUN npm ci --omit=dev
EXPOSE 3000
# Rebuild trigger Mon Apr 06 18:30:00 UTC 2026 - cache bust
CMD ["node", "dist/server.js"]