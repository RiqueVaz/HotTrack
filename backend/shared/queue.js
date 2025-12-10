// Configuração BullMQ e Redis para substituir QStash
const { Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

// Circuit Breaker para Redis
class RedisCircuitBreaker {
    constructor(options = {}) {
        this.failureThreshold = options.failureThreshold || 5;
        this.resetTimeout = options.resetTimeout || 30000; // 30 segundos
        this.successThreshold = options.successThreshold || 2;
        
        this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
        this.failureCount = 0;
        this.successCount = 0;
        this.nextAttempt = null;
    }
    
    async execute(operation) {
        // Se circuit está aberto, verificar se já passou o timeout
        if (this.state === 'OPEN') {
            if (Date.now() < this.nextAttempt) {
                throw new Error('Redis circuit breaker is OPEN. Redis appears to be unavailable.');
            }
            // Tentar reconectar (mover para HALF_OPEN)
            this.state = 'HALF_OPEN';
            this.successCount = 0;
        }
        
        try {
            const result = await operation();
            
            // Sucesso: resetar contadores
            if (this.state === 'HALF_OPEN') {
                this.successCount++;
                if (this.successCount >= this.successThreshold) {
                    this.state = 'CLOSED';
                    this.failureCount = 0;
                    this.successCount = 0;
                }
            } else {
                // Em CLOSED, resetar failure count em caso de sucesso
                this.failureCount = 0;
            }
            
            return result;
        } catch (error) {
            // Falha: incrementar contador
            this.failureCount++;
            
            if (this.state === 'HALF_OPEN') {
                // Se falhou em HALF_OPEN, voltar para OPEN
                this.state = 'OPEN';
                this.nextAttempt = Date.now() + this.resetTimeout;
                this.successCount = 0;
            } else if (this.failureCount >= this.failureThreshold) {
                // Se atingiu threshold, abrir circuit
                this.state = 'OPEN';
                this.nextAttempt = Date.now() + this.resetTimeout;
            }
            
            throw error;
        }
    }
    
    getState() {
        return {
            state: this.state,
            failureCount: this.failureCount,
            successCount: this.successCount,
            nextAttempt: this.nextAttempt ? new Date(this.nextAttempt).toISOString() : null
        };
    }
}

// Instância global do circuit breaker
const redisCircuitBreaker = new RedisCircuitBreaker();

// Configuração Redis otimizada
const redisConnection = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null, // BullMQ requer null para funcionar corretamente
    enableReadyCheck: false,
    enableOfflineQueue: true, // Permitir enfileirar comandos quando offline
    connectTimeout: 10000,
    lazyConnect: false,
    retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        return delay;
    },
    // Reconectar automaticamente quando conexão cair
    reconnectOnError: (err) => {
        const targetError = 'READONLY';
        if (err.message.includes(targetError)) {
            return true; // Reconectar quando Redis está em modo readonly
        }
        // Reconectar em outros erros de conexão também
        if (err.message.includes('ECONNREFUSED') || err.message.includes('ETIMEDOUT')) {
            return true;
        }
        return false;
    },
});

// Monitorar erros de conexão Redis para circuit breaker
redisConnection.on('error', (err) => {
    // Erros de conexão são tratados pelo circuit breaker
    if (err.message.includes('ECONNREFUSED') || 
        err.message.includes('ETIMEDOUT') || 
        err.message.includes('ENOTFOUND') ||
        err.message.includes('ECONNRESET')) {
        // Circuit breaker vai detectar a falha na próxima operação
    }
});

// Configurações equivalentes ao QStash
const BULLMQ_CONCURRENCY = parseInt(process.env.BULLMQ_CONCURRENCY || process.env.QSTASH_CONCURRENCY || '100', 10);
const BULLMQ_RATE_LIMIT_MAX = parseInt(process.env.BULLMQ_RATE_LIMIT_MAX || process.env.QSTASH_RATE_LIMIT_MAX || '100', 10);
const BULLMQ_RATE_LIMIT_WINDOW = 1000; // 1 segundo (equivalente ao QStash)

// Nomes das filas
const QUEUE_NAMES = {
    TIMEOUT: 'timeout-queue',
    DISPARO: 'disparo-queue',
    DISPARO_BATCH: 'disparo-batch-queue',
    DISPARO_DELAY: 'disparo-delay-queue',
    VALIDATION_DISPARO: 'validation-disparo-queue',
    SCHEDULED_DISPARO: 'scheduled-disparo-queue',
    CLEANUP_QRCODES: 'cleanup-qrcodes-queue',
};

// Configurações específicas por fila para otimização
const QUEUE_CONFIGS = {
    [QUEUE_NAMES.DISPARO_BATCH]: {
        concurrency: 200, // Reduzido de 400 para evitar contenção no Redis/DB
        limiter: undefined, // Remover limiter - rate limiting manual faz o trabalho
        // stalledInterval, maxStalledCount e lockDuration são configurações do Worker (não podem ser por job)
        // Valores conservadores que funcionam para qualquer tamanho de batch
        stalledInterval: 3600000, // 60 minutos (aumentado de 30min para evitar falsos positivos de stalled)
        maxStalledCount: 10, // Aumentado de 5 para 10 - dar mais chances antes de falhar
        lockDuration: 7200000, // 2 horas (aumentado de 1h para batches muito grandes)
        // attempts padrão (será sobrescrito por job via calculateScalableLimits quando possível)
        attempts: 5, // Padrão (fallback para jobs sem cálculo dinâmico)
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
        removeOnComplete: {
            age: 24 * 3600,
            count: 1000,
        },
        removeOnFail: {
            age: 7 * 24 * 3600,
        },
    },
    [QUEUE_NAMES.DISPARO]: {
        concurrency: 50,
        limiter: undefined,
        stalledInterval: 120000, // 2 minutos (aumentado de 1min - pode processar vários contatos)
        lockDuration: 300000, // 5 minutos - jobs individuais podem demorar alguns minutos
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.DISPARO_DELAY]: {
        concurrency: 30,
        limiter: { max: 30, duration: 1000 },
        stalledInterval: 600000, // 10 minutos (aumentado de 2min para dar mais margem para delays longos)
        lockDuration: 7200000, // 2 horas - jobs delayed podem ter delays muito longos (horas)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.VALIDATION_DISPARO]: {
        concurrency: 10,
        limiter: { max: 10, duration: 1000 },
        stalledInterval: 600000, // 10 minutos (aumentado de 5min - validação pode processar muitos contatos)
        lockDuration: 1800000, // 30 minutos - validação pode demorar muito com muitos contatos
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.SCHEDULED_DISPARO]: {
        concurrency: 5,
        limiter: { max: 5, duration: 1000 },
        stalledInterval: 600000, // 10 minutos (aumentado de 1min - processa muitos contatos e cria batches)
        lockDuration: 1800000, // 30 minutos - pode processar muitos contatos e criar muitos batches
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.TIMEOUT]: {
        concurrency: 100,
        limiter: { max: 100, duration: 1000 },
        stalledInterval: 120000, // 2 minutos - jobs podem levar até 60s para processar
        lockDuration: 3600000, // 1 hora - jobs delayed podem ter delays longos (horas) - já estava correto
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.CLEANUP_QRCODES]: {
        concurrency: 1, // Baixa concorrência - tarefa de manutenção
        limiter: { max: 1, duration: 1000 },
        stalledInterval: 600000, // 10 minutos
        lockDuration: 1800000, // 30 minutos - cleanup pode demorar se houver muitos QR codes
        attempts: 2,
        backoff: {
            type: 'fixed',
            delay: 5000,
        },
    },
};

// Cache de queues (singleton)
const queues = new Map();
const queueEvents = new Map();

/**
 * Obtém ou cria uma queue BullMQ com configurações otimizadas por fila
 */
function getQueue(queueName) {
    if (!queues.has(queueName)) {
        const config = QUEUE_CONFIGS[queueName] || {
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 2000,
            },
            removeOnComplete: {
                age: 24 * 3600,
                count: 1000,
            },
            removeOnFail: {
                age: 7 * 24 * 3600,
            },
        };
        
        const queue = new Queue(queueName, {
            connection: redisConnection,
            defaultJobOptions: {
                attempts: config.attempts || 3,
                backoff: config.backoff || {
                    type: 'exponential',
                    delay: 2000,
                },
                removeOnComplete: config.removeOnComplete || {
                    age: 24 * 3600,
                    count: 1000,
                },
                removeOnFail: config.removeOnFail || {
                    age: 7 * 24 * 3600,
                },
            },
        });
        
        queues.set(queueName, queue);
        
        // Criar QueueEvents para monitoramento
        const events = new QueueEvents(queueName, { connection: redisConnection });
        queueEvents.set(queueName, events);
    }
    
    return queues.get(queueName);
}

/**
 * Verifica rate limit SEM adicionar requisição (apenas leitura)
 * Usado para waitForRateLimit verificar se pode adicionar sem poluir o contador
 * @param {string} botToken - Token do bot Telegram
 * @returns {Promise<boolean>} - true se dentro do limite, false se excedido
 */
async function checkRateLimitWithoutAdding(botToken) {
    if (!botToken) return true;
    
    const key = `rate-limit:${botToken}`;
    const now = Date.now();
    const windowStart = now - BULLMQ_RATE_LIMIT_WINDOW;
    
    try {
        return await redisCircuitBreaker.execute(async () => {
            const pipeline = redisConnection.pipeline();
            
            // Remover entradas antigas (fora da janela)
            pipeline.zremrangebyscore(key, 0, windowStart);
            
            // Contar requisições na janela atual (SEM adicionar)
            pipeline.zcard(key);
            
            // Definir expiração da chave
            pipeline.expire(key, Math.ceil(BULLMQ_RATE_LIMIT_WINDOW / 1000));
            
            const results = await pipeline.exec();
            
            // results[1][1] é o resultado do zcard (contagem atual)
            const currentCount = results[1][1];
            
            // Verificar se está dentro do limite com margem de segurança maior (10% do limite)
            // Isso evita race conditions quando múltiplos jobs são adicionados simultaneamente
            const safetyMargin = Math.max(1, Math.floor(BULLMQ_RATE_LIMIT_MAX * 0.1));
            return currentCount < (BULLMQ_RATE_LIMIT_MAX - safetyMargin);
        });
    } catch (error) {
        const logger = require('../logger');
        logger.warn(`[RateLimit] Erro ao verificar rate limit (fail open):`, error.message);
        return true;
    }
}

/**
 * Verifica rate limit para bot token e RESERVA uma vaga (adiciona ao contador)
 * Usa lock distribuído para evitar race conditions
 * @param {string} botToken - Token do bot Telegram
 * @returns {Promise<boolean>} - true se dentro do limite e vaga reservada, false se excedido
 */
async function checkRateLimit(botToken) {
    if (!botToken) return true; // Sem bot token, permite processamento
    
    const key = `rate-limit:${botToken}`;
    const lockKey = `rate-limit-lock:${botToken}`;
    const now = Date.now();
    const windowStart = now - BULLMQ_RATE_LIMIT_WINDOW;
    const requestId = `${now}-${Math.random()}`;
    const lockId = `${now}-${Math.random()}`;
    const lockTTL = 1000; // Lock expira em 1 segundo
    
    try {
        // Usar circuit breaker para operações Redis
        return await redisCircuitBreaker.execute(async () => {
            // Tentar adquirir lock distribuído (evita race conditions)
            const lockAcquired = await redisConnection.set(lockKey, lockId, 'PX', lockTTL, 'NX');
            
            if (!lockAcquired) {
                // Se não conseguiu lock, aguardar um pouco e tentar novamente (máximo 3 tentativas)
                for (let i = 0; i < 3; i++) {
                    await new Promise(resolve => setTimeout(resolve, 10)); // 10ms
                    const retryLock = await redisConnection.set(lockKey, lockId, 'PX', lockTTL, 'NX');
                    if (retryLock) break;
                }
            }
            
            try {
                // Usar pipeline do Redis para operações atômicas
                const pipeline = redisConnection.pipeline();
                
                // Remover entradas antigas (fora da janela)
                pipeline.zremrangebyscore(key, 0, windowStart);
                
                // Contar requisições ANTES de adicionar (para verificar com margem de segurança)
                pipeline.zcard(key);
                
                // Adicionar timestamp atual
                pipeline.zadd(key, now, requestId);
                
                // Contar requisições DEPOIS de adicionar
                pipeline.zcard(key);
                
                // Definir expiração da chave
                pipeline.expire(key, Math.ceil(BULLMQ_RATE_LIMIT_WINDOW / 1000));
                
                const results = await pipeline.exec();
                
                // results[0][1] é o resultado do zcard ANTES de adicionar
                const countBefore = results[0][1];
                // results[2][1] é o resultado do zcard DEPOIS de adicionar
                const countAfter = results[2][1];
                
                // Verificar com margem de segurança (10% do limite)
                const safetyMargin = Math.max(1, Math.floor(BULLMQ_RATE_LIMIT_MAX * 0.1));
                
                // Se já estava próximo do limite ANTES de adicionar, remover e retornar false
                if (countBefore >= (BULLMQ_RATE_LIMIT_MAX - safetyMargin)) {
                    await redisConnection.zrem(key, requestId);
                    return false; // Rate limit excedido
                }
                
                // Se depois de adicionar excedeu o limite, remover e retornar false
                if (countAfter > BULLMQ_RATE_LIMIT_MAX) {
                    await redisConnection.zrem(key, requestId);
                    return false; // Rate limit excedido
                }
                
                return true; // Dentro do limite e vaga reservada
            } finally {
                // Liberar lock (apenas se ainda for nosso lock)
                const currentLock = await redisConnection.get(lockKey);
                if (currentLock === lockId) {
                    await redisConnection.del(lockKey);
                }
            }
        });
    } catch (error) {
        // Em caso de erro (incluindo circuit breaker aberto), permitir processamento (fail open)
        const logger = require('../logger');
        logger.warn(`[RateLimit] Erro ao verificar rate limit (fail open):`, error.message);
        return true;
    }
}

/**
 * Aguarda até o rate limit permitir processamento (com timeout)
 * @param {string} botToken - Token do bot Telegram
 * @param {number} maxWaitTime - Tempo máximo de espera em ms (padrão: 5s)
 * @returns {Promise<{allowed: boolean, waitTime: number}>} - Se permitido e tempo de espera necessário
 */
async function waitForRateLimit(botToken, maxWaitTime = 5000) {
    if (!botToken) return { allowed: true, waitTime: 0 };
    
    const startTime = Date.now();
    const checkInterval = 50; // Verificar a cada 50ms (mais frequente para melhor responsividade)
    let consecutiveFailures = 0;
    const maxConsecutiveFailures = 10; // Após 10 falhas consecutivas, aumentar intervalo
    
    while (Date.now() - startTime < maxWaitTime) {
        // Verificar SEM adicionar ao contador (evita poluir o contador durante a espera)
        const allowed = await checkRateLimitWithoutAdding(botToken);
        
        if (allowed) {
            // Se permitido, AGORA SIM reservar a vaga chamando checkRateLimit (que adiciona)
            const reserved = await checkRateLimit(botToken);
            if (reserved) {
                return { allowed: true, waitTime: Date.now() - startTime };
            }
            // Se não conseguiu reservar (race condition), incrementar contador de falhas
            consecutiveFailures++;
        } else {
            consecutiveFailures++;
        }
        
        // Se muitas falhas consecutivas, aumentar intervalo de verificação (backoff adaptativo)
        const currentInterval = consecutiveFailures > maxConsecutiveFailures 
            ? checkInterval * 2 
            : checkInterval;
        
        // Aguardar antes de verificar novamente
        await new Promise(resolve => setTimeout(resolve, currentInterval));
    }
    
    // Timeout: calcular tempo de espera necessário baseado no rate limit
    // Estimativa: se rate limit é 100/segundo, cada requisição precisa de ~10ms de espaçamento
    const estimatedWaitTime = Math.ceil(BULLMQ_RATE_LIMIT_WINDOW / BULLMQ_RATE_LIMIT_MAX);
    
    return { allowed: false, waitTime: estimatedWaitTime };
}

/**
 * Calcula limites escaláveis para jobs de disparo baseado no tamanho do batch
 * @param {number} contactsCount - Número de contatos no batch
 * @returns {object} - Objeto com attempts calculado (lockDuration é configuração do Worker, não pode ser por job)
 */
function calculateScalableLimits(contactsCount) {
    // Aumentar attempts baseado no tamanho do batch (batches maiores = mais tentativas)
    // Fórmula: 3 tentativas base + 1 tentativa a cada 50 contatos (máximo 10)
    const attempts = Math.min(
        Math.max(3, 3 + Math.floor(contactsCount / 50)),
        10 // Máximo 10 tentativas
    );
    
    return { attempts };
}

/**
 * Adiciona um job com delay (equivalente ao publishJSON do QStash)
 * @param {string} queueName - Nome da fila
 * @param {string} jobName - Nome do job
 * @param {object} data - Dados do job
 * @param {object} options - Opções (delay, botToken para rate limiting, etc.)
 */
async function addJobWithDelay(queueName, jobName, data, options = {}) {
    const logger = require('../logger');
    
    // Verificar circuit breaker antes de operações Redis
    const circuitState = redisCircuitBreaker.getState();
    if (circuitState.state === 'OPEN') {
        const nextAttemptTime = circuitState.nextAttempt ? new Date(circuitState.nextAttempt).getTime() : 0;
        if (Date.now() < nextAttemptTime) {
            logger.error(`[Queue] Redis circuit breaker is OPEN. Cannot add job to queue ${queueName}.`);
            throw new Error('Redis is currently unavailable. Please try again later.');
        }
    }
    
    // getQueue não faz operações Redis diretamente, apenas cria/cacheia objetos Queue
    const queue = getQueue(queueName);
    
    const { delay, botToken, jobId } = options;
    
    // Converter delay de string (ex: "30s", "5m") para milissegundos
    let delayMs = 0;
    if (delay) {
        if (typeof delay === 'string') {
            // Parse strings como "30s", "5m", "1h"
            const match = delay.match(/^(\d+)([smhd])$/);
            if (match) {
                const value = parseInt(match[1], 10);
                const unit = match[2];
                const multipliers = { s: 1000, m: 60000, h: 3600000, d: 86400000 };
                delayMs = value * multipliers[unit];
            } else {
                // Tentar parsear como número (segundos)
                delayMs = parseFloat(delay) * 1000;
            }
        } else if (typeof delay === 'number') {
            delayMs = delay;
        }
    }
    
    // Rate limiting pré-emptive para DISPARO_BATCH
    let rateLimitDelay = 0;
    if (queueName === QUEUE_NAMES.DISPARO_BATCH && botToken) {
        const logger = require('../logger');
        try {
            const rateLimitResult = await waitForRateLimit(botToken, 5000); // Max 5s de espera
            
            if (!rateLimitResult.allowed) {
                // Se rate limit excedido após timeout, calcular delay adicional
                rateLimitDelay = rateLimitResult.waitTime;
                logger.warn(`[Queue] Rate limit excedido para bot token. Adicionando delay de ${rateLimitDelay}ms ao job.`, {
                    queueName,
                    waitTime: rateLimitDelay
                });
            } else if (rateLimitResult.waitTime > 0) {
                // Se teve que esperar, logar (mas não adicionar delay extra, já esperou)
                logger.debug(`[Queue] Rate limit: aguardou ${rateLimitResult.waitTime}ms antes de adicionar job.`);
            }
        } catch (error) {
            // Em caso de erro no rate limiting, continuar (fail open)
            logger.warn(`[Queue] Erro ao verificar rate limit pré-emptive (continuando):`, error.message);
        }
    }
    
    // Adicionar delay do rate limiting ao delay original
    delayMs += rateLimitDelay;
    
    // Calcular limites escaláveis para jobs de DISPARO_BATCH
    let scalableLimits = {};
    if (queueName === QUEUE_NAMES.DISPARO_BATCH && data.contacts && Array.isArray(data.contacts)) {
        scalableLimits = calculateScalableLimits(data.contacts.length);
    }
    
    // Configurar rate limiting por bot token se fornecido
    const jobOptions = {
        delay: delayMs,
        jobId: jobId || undefined, // Para deduplicação baseada em conteúdo
        priority: queueName === QUEUE_NAMES.DISPARO_BATCH ? 1 : 0, // Prioridade para disparos (maior número = maior prioridade)
        // Aplicar attempts escalável se calculado (lockDuration é configuração do Worker, não pode ser por job)
        ...(scalableLimits.attempts && { attempts: scalableLimits.attempts }),
    };
    
    // Se botToken fornecido, adicionar como parte do jobId para rate limiting
    if (botToken) {
        // Usar botToken no jobId para garantir rate limiting por token
        const tokenHash = require('crypto').createHash('md5').update(botToken).digest('hex').substring(0, 8);
        if (!jobOptions.jobId) {
            // Criar jobId baseado em hash do conteúdo + token para deduplicação
            const contentHash = require('crypto').createHash('md5').update(JSON.stringify(data)).digest('hex').substring(0, 12);
            jobOptions.jobId = `${queueName}-${tokenHash}-${contentHash}-${Date.now()}`;
        } else {
            jobOptions.jobId = `${jobOptions.jobId}-${tokenHash}`;
        }
        
        // Adicionar botToken aos dados do job para uso no worker
        data._botToken = botToken;
    }
    
    const job = await redisCircuitBreaker.execute(async () => {
        return await queue.add(jobName, data, jobOptions);
    });
    
    return {
        jobId: job.id,
        messageId: job.id, // Compatibilidade com código antigo que usa messageId
    };
}

/**
 * Remove um job (equivalente ao messages.delete do QStash)
 */
async function removeJob(queueName, jobId) {
    const queue = getQueue(queueName);
    const job = await queue.getJob(jobId);
    
    if (job) {
        await job.remove();
        return true;
    }
    
    return false;
}

/**
 * Remove todos os jobs de um disparo específico (por history_id)
 * Busca em waiting, delayed e active
 */
async function removeJobsByHistoryId(queueName, historyId) {
    const queue = getQueue(queueName);
    let totalRemoved = 0;
    
    try {
        // Buscar jobs waiting
        const waitingJobs = await queue.getJobs(['waiting'], 0, -1);
        const waitingToRemove = waitingJobs.filter(job => job.data?.history_id === historyId);
        
        // Buscar jobs delayed
        const delayedJobs = await queue.getJobs(['delayed'], 0, -1);
        const delayedToRemove = delayedJobs.filter(job => job.data?.history_id === historyId);
        
        // Buscar jobs active
        const activeJobs = await queue.getJobs(['active'], 0, -1);
        const activeToRemove = activeJobs.filter(job => job.data?.history_id === historyId);
        
        // Remover todos os jobs encontrados
        const allJobsToRemove = [...waitingToRemove, ...delayedToRemove, ...activeToRemove];
        
        for (const job of allJobsToRemove) {
            try {
                await job.remove();
                totalRemoved++;
            } catch (error) {
                console.error(`[QUEUE] Erro ao remover job ${job.id}:`, error.message);
            }
        }
        
        return {
            removed: totalRemoved,
            waiting: waitingToRemove.length,
            delayed: delayedToRemove.length,
            active: activeToRemove.length
        };
    } catch (error) {
        console.error(`[QUEUE] Erro ao remover jobs do disparo ${historyId}:`, error);
        throw error;
    }
}

/**
 * Fecha todas as conexões
 */
async function closeAll() {
    for (const queue of queues.values()) {
        await queue.close();
    }
    for (const events of queueEvents.values()) {
        await events.close();
    }
    await redisConnection.quit();
}

/**
 * Agenda limpeza de QR codes diariamente às 6h SP (9h UTC)
 */
async function scheduleRecurringCleanupQRCodes() {
    const queue = getQueue(QUEUE_NAMES.CLEANUP_QRCODES);
    
    try {
        // Verificar se já existe job recorrente
        const repeatableJobs = await queue.getRepeatableJobs();
        const existingJob = repeatableJobs.find(job => job.name === 'cleanup-qrcodes-daily');
        
        if (!existingJob) {
            // Criar job recorrente: diariamente às 9h UTC (6h SP)
            await queue.add(
                'cleanup-qrcodes-daily',
                {},
                {
                    repeat: {
                        pattern: '0 9 * * *', // Cron: 9h UTC diariamente
                    },
                    jobId: 'cleanup-qrcodes-recurring', // ID único para evitar duplicatas
                }
            );
            console.log('[BullMQ] Job recorrente de limpeza de QR codes agendado (diário às 6h SP)');
        } else {
            console.log('[BullMQ] Job recorrente de limpeza de QR codes já existe');
        }
    } catch (error) {
        console.error('[BullMQ] Erro ao agendar limpeza de QR codes:', error);
        throw error;
    }
}

module.exports = {
    getQueue,
    addJobWithDelay,
    removeJob,
    removeJobsByHistoryId,
    closeAll,
    scheduleRecurringCleanupQRCodes,
    QUEUE_NAMES,
    QUEUE_CONFIGS,
    BULLMQ_CONCURRENCY,
    BULLMQ_RATE_LIMIT_MAX,
    BULLMQ_RATE_LIMIT_WINDOW,
    redisConnection,
    redisCircuitBreaker,
    checkRateLimit, // Exportar para uso no worker como fallback
};
