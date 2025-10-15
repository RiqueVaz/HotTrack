# Backend Dockerfile para Render
FROM node:18-alpine

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++ curl

WORKDIR /app

# Agora o Dockerfile está DENTRO da pasta backend, então:
# Copiar package.json e package-lock.json (que estão na mesma pasta do Dockerfile)
COPY package*.json ./

# Instalar dependências
RUN npm install --omit=dev

# Copiar código fonte (tudo da pasta atual - backend - para /app)
COPY . .

# Criar usuário não-root para segurança
RUN addgroup -g 1001 -S nodejs
RUN adduser -S nodejs -u 1001

# Alterar propriedade dos arquivos
RUN chown -R nodejs:nodejs /app
USER nodejs

# Expor porta
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:3000/api/health || exit 1

# Comando de inicialização
CMD ["node", "server.js"]