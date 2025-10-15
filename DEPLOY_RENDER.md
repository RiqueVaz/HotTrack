# 🐳 Deploy do HotTrack no Render (Docker)

Este guia te ajudará a fazer o deploy completo do HotTrack SaaS na plataforma Render usando Docker.

## 📋 Pré-requisitos

- Conta no [Render](https://render.com)
- Banco de dados Neon configurado
- Todas as chaves de API necessárias

## 🗄️ Configuração do Banco de Dados

O HotTrack usa **Neon Database** como banco principal. Certifique-se de ter:

1. ✅ Conta no [Neon](https://neon.tech)
2. ✅ Banco de dados criado
3. ✅ String de conexão PostgreSQL

## 🔧 Configuração das Variáveis de Ambiente

### Variáveis Obrigatórias

```bash
# Banco de Dados
DATABASE_URL=postgresql://username:password@hostname:5432/database

# Admin
ADMIN_API_KEY=sua_chave_admin_super_secreta

# VAPID (Notificações Push)
VAPID_PUBLIC_KEY=sua_chave_publica_vapid
VAPID_PRIVATE_KEY=sua_chave_privada_vapid
VAPID_SUBJECT=mailto:seu@email.com

# QStash (Filas)
QSTASH_TOKEN=seu_token_qstash

# Provedores PIX (opcionais)
PUSHINPAY_SPLIT_ACCOUNT_ID=seu_account_id_pushinpay
CNPAY_SPLIT_PRODUCER_ID=seu_producer_id_cnpay
OASYFY_SPLIT_PRODUCER_ID=seu_producer_id_oasyfy
BRPIX_SPLIT_RECIPIENT_ID=seu_recipient_id_brpix
```

## 🚀 Deploy no Render (Docker)

### Deploy Automático com render.yaml

1. **Faça push do código para o GitHub**
2. **No Render Dashboard:**
   - Clique em "New +"
   - Selecione "Blueprint"
   - Conecte seu repositório GitHub
   - Render detectará automaticamente os Dockerfiles

### Estrutura dos Serviços

#### 1. Backend API (Docker)
- **Tipo:** Web Service
- **Environment:** Docker
- **Dockerfile:** `./Dockerfile`
- **Health Check:** `/api/health`

#### 2. Frontend Principal (Docker)
- **Tipo:** Web Service  
- **Environment:** Docker
- **Dockerfile:** `./frontend/Dockerfile`

#### 3. Admin Frontend (Docker)
- **Tipo:** Web Service
- **Environment:** Docker  
- **Dockerfile:** `./admin-frontend/Dockerfile`

## ⚙️ Configuração das Variáveis no Render

Para cada serviço, configure as variáveis de ambiente:

1. **Backend:** Todas as variáveis obrigatórias
2. **Frontends:** Apenas URLs (já configuradas no código)

## 🔄 URLs de Produção

Após o deploy, suas URLs serão:

- **Backend API:** `https://hottrack-backend.onrender.com`
- **Frontend:** `https://hottrack-frontend.onrender.com`
- **Admin:** `https://hottrack-admin.onrender.com`

## 🐳 Deploy com Docker

### Preparação Local

#### Linux/macOS
```bash
# Executar script de preparação
./docker-deploy.sh

# Teste local (opcional)
./docker-deploy.sh --test
```

#### Windows
```cmd
REM Usando arquivo .bat
docker-deploy.bat
docker-deploy.bat --test

REM Usando PowerShell
powershell -ExecutionPolicy Bypass -File docker-deploy.ps1
powershell -ExecutionPolicy Bypass -File docker-deploy.ps1 -Test
```

#### Docker Compose Direto
```bash
# Funciona em qualquer sistema
docker-compose up --build
```

### Comandos Docker Individuais

```bash
# Build das imagens
docker build -t hottrack-backend .
docker build -t hottrack-frontend ./frontend
docker build -t hottrack-admin ./admin-frontend

# Teste local
docker-compose up --build
```

## 📊 Monitoramento

O Render fornece:

- ✅ Logs em tempo real
- ✅ Métricas de performance
- ✅ Health checks automáticos
- ✅ Deploy automático via Git

## 🔧 Troubleshooting

### Problemas Comuns

1. **Build falha:**
   - Verifique se todas as dependências estão no `package.json`
   - Confirme se o Node.js versão 18+ está sendo usado

2. **Banco não conecta:**
   - Verifique a `DATABASE_URL`
   - Confirme se o Neon está acessível

3. **CORS errors:**
   - URLs estão configuradas corretamente
   - Frontends apontam para o backend correto

### Logs

Acesse os logs em:
- Render Dashboard → Seu Serviço → Logs

## 🔐 Segurança

- ✅ Todas as chaves estão em variáveis de ambiente
- ✅ CORS configurado corretamente
- ✅ Health checks implementados
- ✅ Usuário não-root no Docker

## 📈 Próximos Passos

1. Configure domínio customizado (opcional)
2. Configure SSL (automático no Render)
3. Configure backups do banco (Neon)
4. Configure monitoramento avançado

## 🆘 Suporte

Se encontrar problemas:

1. Verifique os logs no Render Dashboard
2. Confirme todas as variáveis de ambiente
3. Teste localmente primeiro
4. Verifique a conectividade com o Neon

---

**🎉 Parabéns! Seu HotTrack está rodando no Render!**
