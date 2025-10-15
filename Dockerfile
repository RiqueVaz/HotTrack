FROM node:18-alpine

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++ curl

WORKDIR /app

# Copiar e instalar backend
COPY backend/package*.json ./
RUN npm install --production

# Copiar backend + frontend + admin
COPY backend/ ./
COPY frontend/ ./frontend/
COPY admin-frontend/ ./admin-frontend/

# Criar usuário não-root
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

USER nodejs

EXPOSE 3000

CMD ["node", "server.js"]