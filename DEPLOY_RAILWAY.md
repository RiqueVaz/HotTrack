# 🚂 Deploy do HotTrack no Railway

Este guia te ajudará a fazer o deploy completo do HotTrack SaaS na plataforma Railway.

## 📋 Pré-requisitos

- ✅ Conta no [Railway](https://railway.app)
- ✅ Banco de dados Neon configurado
- ✅ GitHub conectado ao Railway

## 🚂 Deploy no Railway

### 1. Teste Local (Opcional)

```cmd
# Teste local com Docker Compose
docker-compose up --build
```

### 2. Conectar ao Railway

1. **Acesse [Railway](https://railway.app)**
2. **Clique em "Login" e faça login com GitHub**
3. **Clique em "New Project"**
4. **Selecione "Deploy from GitHub repo"**
5. **Escolha seu repositório HotTrack**

### 3. Deploy Automático

O Railway detectará automaticamente o Dockerfile e criará um serviço único:

- **HotTrack:** `Dockerfile` → `hottrack-production-xxxx.up.railway.app`

#### Serviço Único
- **Backend API:** `http://hottrack-production-xxxx.up.railway.app/api`
- **Frontend:** `http://hottrack-production-xxxx.up.railway.app/`
- **Admin:** `http://hottrack-production-xxxx.up.railway.app/admin`

### 4. Configurar Variáveis de Ambiente

```bash
# Obrigatórias
NODE_ENV=production
DATABASE_URL=postgresql://username:password@hostname:5432/database
ADMIN_API_KEY=sua_chave_admin_super_secreta

# VAPID (Notificações Push)
VAPID_PUBLIC_KEY=sua_chave_publica_vapid
VAPID_PRIVATE_KEY=sua_chave_privada_vapid
VAPID_SUBJECT=mailto:seu@email.com

# QStash (Filas)
QSTASH_TOKEN=seu_token_qstash

# URLs (Railway gera automaticamente)
FRONTEND_URL=https://frontend-production-xxxx.up.railway.app
HOTTRACK_API_URL=https://backend-production-xxxx.up.railway.app
DOCUMENTATION_URL=https://admin-production-xxxx.up.railway.app

# Provedores PIX (opcionais)
PUSHINPAY_SPLIT_ACCOUNT_ID=seu_account_id_pushinpay
CNPAY_SPLIT_PRODUCER_ID=seu_producer_id_cnpay
OASYFY_SPLIT_PRODUCER_ID=seu_producer_id_oasyfy
BRPIX_SPLIT_RECIPIENT_ID=seu_recipient_id_brpix
```

### 5. Deploy

1. **Railway fará deploy automático** após conectar o repositório
2. **Aguarde o build** (pode levar alguns minutos)
3. **URLs serão geradas automaticamente**

## 🔄 URLs de Produção

Após o deploy, suas URLs serão:

- **HotTrack:** `https://hottrack-production-xxxx.up.railway.app`
  - **Frontend:** `https://hottrack-production-xxxx.up.railway.app/`
  - **Admin:** `https://hottrack-production-xxxx.up.railway.app/admin`
  - **API:** `https://hottrack-production-xxxx.up.railway.app/api`

## 🧪 Testando

1. **Health Check:** `https://hottrack-production-xxxx.up.railway.app/api/health`
2. **Frontend:** Acesse `https://hottrack-production-xxxx.up.railway.app/`
3. **Admin:** Acesse `https://hottrack-production-xxxx.up.railway.app/admin` com sua `ADMIN_API_KEY`

## 🔧 Troubleshooting

### Problemas Comuns

1. **Build falha:**
   - Verifique os logs no Railway Dashboard
   - Confirme se todas as dependências estão no `package.json`

2. **Banco não conecta:**
   - Verifique a `DATABASE_URL`
   - Confirme se o Neon está acessível

3. **CORS errors:**
   - URLs estão configuradas corretamente
   - Frontends apontam para o backend correto

### Logs

Acesse os logs em:
- Railway Dashboard → Seu Projeto → Logs

## 💰 Planos Railway

### Gratuito
- ✅ 500 horas/mês de execução
- ✅ Deploy automático via Git
- ✅ URLs customizadas
- ✅ Logs em tempo real

### Pro ($5/mês)
- ✅ Deploy ilimitado
- ✅ Domínio customizado
- ✅ SSL automático
- ✅ Monitoramento avançado

## 🚀 Vantagens do Railway

- ✅ **Deploy automático** via GitHub
- ✅ **Detecção automática** de Dockerfiles
- ✅ **URLs automáticas** para cada serviço
- ✅ **SSL automático** em todos os serviços
- ✅ **Logs em tempo real**
- ✅ **Deploy em segundos**
- ✅ **Escalabilidade automática**

## 📈 Próximos Passos

1. **Configure domínio customizado** (opcional)
2. **Configure monitoramento** avançado
3. **Configure backups** do banco (Neon)
4. **Configure CI/CD** avançado

## 💡 Dicas

- ✅ **Teste localmente** primeiro com `docker-compose up --build`
- ✅ **Configure todas as variáveis** antes do deploy
- ✅ **Monitore os logs** durante o build
- ✅ **Use nomes descritivos** para os serviços
- ✅ **Teste todas as funcionalidades** após o deploy

## 🆘 Suporte

Se encontrar problemas:

1. Verifique os logs no Railway Dashboard
2. Confirme todas as variáveis de ambiente
3. Teste localmente primeiro
4. Verifique a conectividade com o Neon

---

**🎉 Parabéns! Seu HotTrack está rodando no Railway!**
