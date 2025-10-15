# Multi-stage build para todos os serviços
FROM node:18-alpine AS backend

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++ curl

WORKDIR /app

# Copiar e instalar backend
COPY backend/package*.json ./
RUN npm install --production
COPY backend/ ./

# Imagem final
FROM node:18-alpine
RUN apk add --no-cache nginx

# Copiar backend
WORKDIR /app
COPY --from=backend /app ./

# Copiar frontend (ARQUIVOS ESTÁTICOS - SEM BUILD)
COPY frontend/ /usr/share/nginx/html/frontend

# Copiar admin (ARQUIVOS ESTÁTICOS - SEM BUILD)  
COPY admin-frontend/ /usr/share/nginx/html/admin

# Configurar nginx para servir frontend e admin
RUN echo 'server { \
    listen 80; \
    server_name _; \
    root /usr/share/nginx/html/frontend; \
    index index.html; \
    location / { \
        try_files \$uri \$uri/ /index.html; \
    } \
}' > /etc/nginx/conf.d/frontend.conf

RUN echo 'server { \
    listen 81; \
    server_name _; \
    root /usr/share/nginx/html/admin; \
    index index.html; \
    location / { \
        try_files \$uri \$uri/ /index.html; \
    } \
}' > /etc/nginx/conf.d/admin.conf

# Script para iniciar todos os serviços
RUN echo '#!/bin/sh\n\
nginx &\n\
node server.js\n\
wait' > /start.sh && chmod +x /start.sh

EXPOSE 3000 80 81

CMD ["/start.sh"]