FROM node:18-alpine

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++ curl

WORKDIR /app

# Copiar APENAS os arquivos de dependência primeiro (para melhor cache)
COPY package*.json ./

# Instalar dependências (incluindo dependências de produção)
RUN npm install --production

# Copiar o resto do código
COPY . .

# Verificar se as dependências foram instaladas
RUN ls -la node_modules/ | head -10

# Criar usuário não-root
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

USER nodejs

EXPOSE 3000

# Health check simplificado
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:3000/api/health || exit 1

CMD ["node", "server.js"]