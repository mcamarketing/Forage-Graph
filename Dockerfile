FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --production=false

COPY . .
RUN npm run build

# Prune dev deps
RUN npm ci --production

EXPOSE 3000

CMD ["node", "dist/server.js"]
# Rebuild trigger Thu Mar 26 01:36:43 GMTST 2026
