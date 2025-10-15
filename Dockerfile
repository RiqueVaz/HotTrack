# Multi-stage build para todos os serviços
FROM node:18-alpine AS backend

# Instalar dependências do sistema
RUN apk add --no-cache python3 make g++ curl

WORKDIR /app

# Copiar e instalar backend
COPY backend/package*.json ./
RUN npm install --production
COPY backend/ ./

# Build do frontend (se necessário)
FROM node:18-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Build do admin (se necessário)  
FROM node:18-alpine AS admin-builder
WORKDIR /app/admin-frontend
COPY admin-frontend/package*.json ./
RUN npm install
COPY admin-frontend/ ./
RUN npm run build

# Imagem final
FROM node:18-alpine
RUN apk add --no-cache nginx

# Copiar backend
WORKDIR /app
COPY --from=backend /app ./

# Copiar frontend buildado
COPY --from=frontend-builder /app/frontend/dist /usr/share/nginx/html/frontend

# Copiar admin buildado  
COPY --from=admin-builder /app/admin-frontend/dist /usr/share/nginx/html/admin

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