# Backend Dockerfile para Render
FROM node:18-alpine

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++

WORKDIR /app

# Copiar package.json e package-lock.json
COPY backend/package*.json ./

# Instalar dependências
RUN npm ci --only=production

# Copiar código fonte do backend
COPY backend/ ./

# Criar usuário não-root para segurança
RUN addgroup -g 1001 -S nodejs
RUN adduser -S nodejs -u 1001

# Alterar propriedade dos arquivos
RUN chown -R nodejs:nodejs /app
USER nodejs

# Expor porta
EXPOSE 3000

# Comando de inicialização
CMD ["node", "server.js"]
