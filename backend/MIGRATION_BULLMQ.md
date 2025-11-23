# Migração QStash para BullMQ + Redis

## Resumo

Este projeto foi migrado de QStash (Upstash) para BullMQ + Redis Railway para ter controle total sobre o agendamento de tarefas e filas de processamento.

## O que mudou?

### Antes (QStash)
- Dependência externa (Upstash QStash)
- Custo por requisição
- Rate limiting via headers HTTP
- Endpoints webhook para receber callbacks
- Deduplicação baseada em conteúdo

### Depois (BullMQ + Redis)
- Solução nativa auto-hospedada
- Custo fixo (apenas Redis)
- Rate limiting nativo do BullMQ
- Workers processam jobs diretamente
- Controle total sobre retries e delays

## Configuração necessária

### 1. Adicionar Redis no Railway

1. Acesse seu projeto no Railway
2. Clique em "+ New" → "Database" → "Add Redis"
3. O Railway criará automaticamente a variável `REDIS_URL`

### 2. Variáveis de ambiente

#### Adicionar (obrigatório):
```bash
REDIS_URL=redis://default:password@host:port  # Criada automaticamente pelo Railway
ENABLE_WORKERS=true  # Habilitar workers BullMQ em produção
```

#### Remover (não mais necessárias):
```bash
QSTASH_TOKEN
QSTASH_CURRENT_SIGNING_KEY
QSTASH_NEXT_SIGNING_KEY
```

### 3. Instalar dependências

```bash
npm install
```

Isso instalará:
- `bullmq@^5.13.2` - Sistema de filas
- `ioredis@^5.4.1` - Cliente Redis

### 4. Executar migration SQL

Execute a migration para adicionar o campo `scheduled_message_id` em `disparo_history`:

```sql
-- Arquivo: backend/migrations/add_scheduled_message_id_to_disparo_history.sql
ALTER TABLE disparo_history 
ADD COLUMN IF NOT EXISTS scheduled_message_id TEXT;

CREATE INDEX IF NOT EXISTS idx_disparo_history_scheduled_message_id 
ON disparo_history(scheduled_message_id) 
WHERE scheduled_message_id IS NOT NULL;
```

## Arquitetura

### Módulos criados

1. **backend/shared/redis.js**
   - Gerencia conexão com Redis Railway
   - Reconexão automática
   - Logs de status

2. **backend/shared/queue-manager.js**
   - Gerenciador central de filas
   - Criação de jobs
   - Cancelamento de jobs
   - Conversão de delays (ex: "5m" → 300000ms)

3. **backend/workers/index.js**
   - Worker para timeouts de fluxo (concurrency: 20)
   - Worker para delays de ações (concurrency: 20)
   - Worker para disparos individuais (concurrency: 10, rate: 50/s)
   - Worker para disparos agendados (concurrency: 5)

### Filas criadas

| Fila | Função | Concurrency | Rate Limit |
|------|--------|-------------|------------|
| `timeouts` | Processar timeouts de espera por resposta | 20 | 100/s |
| `delays` | Processar delays entre ações | 20 | 100/s |
| `disparos` | Processar disparos individuais | 10 | 50/s |
| `disparos-agendados` | Processar disparos agendados | 5 | 10/s |

### Substituições realizadas

#### Timeouts de fluxo
- **Antes**: `qstashClient.publishJSON()` com delay `${timeoutMinutes}m`
- **Depois**: `queueManager.addJob('timeouts', ...)` com delay em milissegundos

#### Delays de ações
- **Antes**: `qstashClient.publishJSON()` com delay `${delaySeconds}s`
- **Depois**: `queueManager.addJob('delays', ...)` com delay em milissegundos

#### Disparos em massa
- **Antes**: Acumular promises e processar em batches
- **Depois**: `queueManager.addJob('disparos', ...)` direto (BullMQ gerencia)

#### Disparos agendados
- **Antes**: `qstashClient.publishJSON()` com delay calculado
- **Depois**: `queueManager.addJob('disparos-agendados', ...)` com delay calculado

#### Cancelamentos
- **Antes**: `qstashClient.messages.delete(messageId)`
- **Depois**: `cancelBullMQJob(jobId)` (infere fila automaticamente)

## Benefícios

### 1. Custo
- **Antes**: ~$0.50 por 100k requisições no QStash
- **Depois**: Custo fixo do Redis (~$5-10/mês no Railway)
- **Economia**: Significativa em volumes altos

### 2. Performance
- Workers processam jobs localmente (sem latência de rede)
- Rate limiting mais preciso
- Retries automáticos com backoff exponencial

### 3. Controle
- Visualização de filas em tempo real (via BullMQ Board)
- Métricas detalhadas
- Logs centralizados
- Priorização de jobs

### 4. Escalabilidade
- Adicionar mais workers facilmente
- Ajustar concurrency por fila
- Distribuir carga entre múltiplos servidores

## Monitoramento

### Logs importantes

```bash
# Inicialização
[SERVER] Inicializando workers BullMQ...
[QUEUE] Fila timeouts criada
[WORKER] Worker criado para fila timeouts (concurrency: 20)

# Processamento
[QUEUE] Job 123 adicionado à fila timeouts (delay: 300000ms)
[WORKER] Processando job 123 da fila timeouts
[WORKER] ✅ Job 123 completado

# Erros
[WORKER] ❌ Job 456 falhou: Error message
[QUEUE] Erro ao cancelar job 789: Job not found
```

### Graceful shutdown

O servidor agora fecha graciosamente:
- Aguarda jobs em processamento terminarem
- Fecha workers
- Fecha filas
- Desconecta do Redis

## Troubleshooting

### Problema: Workers não inicializam
**Solução**: Verificar se `ENABLE_WORKERS=true` está definido

### Problema: Erro "REDIS_URL não configurado"
**Solução**: Adicionar Redis no Railway e reiniciar aplicação

### Problema: Jobs não são processados
**Solução**: 
1. Verificar logs do Redis
2. Verificar se workers estão rodando
3. Checar concurrency e rate limits

### Problema: Jobs ficam presos
**Solução**:
1. Aumentar timeout de jobs
2. Verificar se há deadlocks no código
3. Reiniciar workers

## Próximos passos

1. ✅ Migração completa do QStash
2. ⏳ Monitorar performance em produção
3. ⏳ Ajustar concurrency se necessário
4. ⏳ Adicionar dashboard BullMQ Board (opcional)
5. ⏳ Implementar métricas Prometheus (opcional)

## Suporte

Em caso de problemas:
1. Verificar logs do servidor
2. Verificar logs do Redis no Railway
3. Consultar documentação do BullMQ: https://docs.bullmq.io

