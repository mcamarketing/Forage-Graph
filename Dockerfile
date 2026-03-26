FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20-alpine
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY package*.json ./
RUN npm ci --omit=dev
EXPOSE 3000
# Rebuild trigger Thu Mar 26 15:30:00 GMTST 2026 - regime endpoints
CMD ["node", "dist/server.js"]