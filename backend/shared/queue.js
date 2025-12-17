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

// Limite para cálculo linear de nós (acima disso usa cálculo logarítmico)
const MAX_NODES_FOR_LINEAR_CALCULATION = 100000; // Limite para cálculo linear

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
        // stalledInterval será calculado dinamicamente baseado em métricas históricas
        // Valor padrão usado apenas como fallback se não houver métricas
        stalledInterval: 7200000, // 2 horas - fallback padrão (será sobrescrito por cálculo adaptativo)
        maxStalledCount: 5, // Reduzido de 10 para evitar reprocessamento excessivo
        lockDuration: 10800000, // 3 horas - batches muito grandes podem demorar até 1.5h (fallback padrão)
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
        stalledInterval: 1200000, // 20 minutos - fallback padrão (será sobrescrito por cálculo adaptativo)
        lockDuration: 900000, // 15 minutos - fluxos complexos podem demorar até 7-8 minutos (fallback padrão)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.DISPARO_DELAY]: {
        concurrency: 30,
        limiter: { max: 30, duration: 1000 },
        stalledInterval: 7200000, // 2 horas - fallback padrão (será sobrescrito por cálculo adaptativo)
        lockDuration: 14400000, // 4 horas - delays podem ser muito longos (até 2h de processamento) (fallback padrão)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.VALIDATION_DISPARO]: {
        concurrency: 10,
        limiter: { max: 10, duration: 1000 },
        stalledInterval: 2700000, // 45 minutos - fallback padrão (será sobrescrito por cálculo adaptativo)
        maxStalledCount: 5, // Reduzido para evitar reprocessamento excessivo
        lockDuration: 5400000, // 1.5 horas - validação pode demorar muito com muitos contatos (até 45min) (fallback padrão)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.SCHEDULED_DISPARO]: {
        concurrency: 5,
        limiter: { max: 5, duration: 1000 },
        stalledInterval: 1800000, // 30 minutos - fallback padrão (será sobrescrito por cálculo adaptativo)
        maxStalledCount: 3, // Reduzido para evitar reprocessamento excessivo
        lockDuration: 3600000, // 1 hora - pode processar muitos contatos e criar muitos batches (até 30min) (fallback padrão)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.TIMEOUT]: {
        concurrency: 20, // Reduzido de 100 para 20 para evitar contenção e race conditions sob alta carga
        limiter: { max: 20, duration: 1000 }, // Ajustado para corresponder à concorrência
        stalledInterval: 14400000, // 4 horas - AUMENTADO de 50min para 4h para acomodar jobs com delays de até 1h sem marcar como stalled prematuramente
        lockDuration: 86400000, // 24 horas - AUMENTADO de 1h para 24h para acomodar fluxos completos que podem durar até 24h (delays máx 1h + processamento)
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.CLEANUP_QRCODES]: {
        concurrency: 1, // Baixa concorrência - tarefa de manutenção
        limiter: { max: 1, duration: 1000 },
        stalledInterval: 1800000, // 30 minutos - fallback padrão (será sobrescrito por cálculo adaptativo)
        lockDuration: 3600000, // 1 hora - cleanup pode demorar se houver muitos QR codes (até 30min) (fallback padrão)
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
        
        // QueueScheduler foi deprecado no BullMQ 5.0 - jobs delayed são processados automaticamente pelos workers
        // Não é mais necessário criar QueueScheduler
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
 * Calcula lockDuration dinâmico baseado no tamanho do job
 * @param {number} contactsCount - Número de contatos no batch
 * @param {string} queueName - Nome da fila
 * @returns {number} - LockDuration em milissegundos
 */
function calculateDynamicLockDuration(contactsCount, queueName) {
    // Validar entrada
    if (!Number.isFinite(contactsCount) || isNaN(contactsCount) || contactsCount < 0) {
        contactsCount = 0;
    }
    
    // Tempo base por job (5 minutos)
    const BASE_TIME_MS = 5 * 60 * 1000;
    
    // Tempo médio por contato baseado em métricas reais
    // 100 contatos em 3-4h = ~2-2.4min por contato
    // Usar valor conservador: 2min por contato
    const TIME_PER_CONTACT_MS = 2 * 60 * 1000;
    
    // Margem de segurança: 2x o tempo estimado
    const SAFETY_MARGIN = 2;
    
    // Para valores muito grandes (> 1.000.000), usar cálculo logarítmico
    const MAX_CONTACTS_FOR_LINEAR_CALCULATION = 1000000;
    let estimatedTime;
    if (contactsCount > MAX_CONTACTS_FOR_LINEAR_CALCULATION) {
        // Cálculo logarítmico para valores muito grandes
        estimatedTime = BASE_TIME_MS + (Math.log10(contactsCount) * 60 * 60 * 1000); // log10 * 1 hora
    } else {
        // Cálculo linear normal
        estimatedTime = BASE_TIME_MS + (contactsCount * TIME_PER_CONTACT_MS);
    }
    
    let lockDuration = estimatedTime * SAFETY_MARGIN;
    
    // Validar resultado e aplicar limites técnicos
    const limits = getQueueMetricsConstants();
    
    if (!Number.isFinite(lockDuration) || isNaN(lockDuration) || lockDuration <= 0) {
        const logger = require('../logger');
        logger.warn(`[Queue] LockDuration inválido calculado (${lockDuration}), usando mínimo: ${Math.round(limits.MIN_LOCK_DURATION_MS / 60000)}min`);
        lockDuration = limits.MIN_LOCK_DURATION_MS;
    } else if (lockDuration > limits.MAX_LOCK_DURATION_MS) {
        const logger = require('../logger');
        logger.warn(`[Queue] LockDuration muito grande (${Math.round(lockDuration / 60000)}min para ${contactsCount} contatos), limitando ao máximo técnico: ${Math.round(limits.MAX_LOCK_DURATION_MS / 60000)}min`);
        lockDuration = limits.MAX_LOCK_DURATION_MS;
    } else if (lockDuration < limits.MIN_LOCK_DURATION_MS) {
        lockDuration = limits.MIN_LOCK_DURATION_MS;
    }
    
    return Math.ceil(lockDuration);
}

/**
 * Calcula stalledInterval dinâmico baseado no lockDuration
 * Deve ser ~1/3 do lockDuration para detectar stalled jobs sem ser muito agressivo
 * @param {number} lockDuration - LockDuration em milissegundos
 * @returns {number} - StalledInterval em milissegundos
 */
function calculateDynamicStalledInterval(lockDuration) {
    // Validar lockDuration antes de calcular
    const limits = getQueueMetricsConstants();
    if (!Number.isFinite(lockDuration) || isNaN(lockDuration) || lockDuration <= 0) {
        const logger = require('../logger');
        logger.warn(`[Queue] LockDuration inválido (${lockDuration}) em calculateDynamicStalledInterval, usando mínimo: ${Math.round(limits.MIN_LOCK_DURATION_MS / 60000)}min`);
        lockDuration = limits.MIN_LOCK_DURATION_MS;
    } else if (lockDuration > limits.MAX_LOCK_DURATION_MS) {
        lockDuration = limits.MAX_LOCK_DURATION_MS;
    }
    
    const calculated = Math.floor(lockDuration / 3);
    // Sem limites fixos - usar cálculo adaptativo quando possível
    // Fallback: mínimo 5 minutos ou 15% do lockDuration, máximo 60% do lockDuration ou limite técnico
    const minStalled = Math.max(5 * 60 * 1000, lockDuration * 0.15);
    const maxStalled = Math.min(lockDuration * 0.6, limits.MAX_STALLED_INTERVAL_MS);
    
    let stalledInterval = Math.max(minStalled, Math.min(calculated, maxStalled));
    
    // Validar resultado final
    if (!Number.isFinite(stalledInterval) || isNaN(stalledInterval) || stalledInterval <= 0) {
        const logger = require('../logger');
        logger.warn(`[Queue] StalledInterval inválido calculado, usando mínimo: ${Math.round((5 * 60 * 1000) / 60000)}min`);
        stalledInterval = 5 * 60 * 1000;
    }
    
    // Garantir que stalledInterval sempre seja menor que lockDuration
    if (stalledInterval >= lockDuration) {
        stalledInterval = Math.max(minStalled, lockDuration * 0.5);
    }
    
    return Math.ceil(stalledInterval);
}

// Importar módulo de métricas (carregamento lazy para evitar dependência circular)
let queueMetrics = null;
function getQueueMetrics() {
    if (!queueMetrics) {
        queueMetrics = require('./queue-metrics');
    }
    return queueMetrics;
}

// Importar constantes de limites técnicos
let queueMetricsConstants = null;
function getQueueMetricsConstants() {
    if (!queueMetricsConstants) {
        const metrics = require('./queue-metrics');
        queueMetricsConstants = {
            MIN_LOCK_DURATION_MS: metrics.MIN_LOCK_DURATION_MS,
            MAX_LOCK_DURATION_MS: metrics.MAX_LOCK_DURATION_MS,
            MAX_STALLED_INTERVAL_MS: metrics.MAX_STALLED_INTERVAL_MS
        };
    }
    return queueMetricsConstants;
}

/**
 * Obtém lockDuration otimizado para um job específico usando métricas históricas quando disponível
 * @param {string} queueName - Nome da fila
 * @param {object} jobData - Dados do job
 * @returns {Promise<object|null>} - Objeto com lockDuration e stalledInterval calculados ou null
 */
async function getOptimalLockDuration(queueName, jobData) {
    // Se já foi calculado, usar o valor existente
    if (jobData._calculatedLockDuration) {
        const metrics = getQueueMetrics();
        const stalledInterval = jobData._calculatedStalledInterval || 
            await metrics.getAdaptiveStalledInterval(jobData._calculatedLockDuration, queueName, jobData) ||
            calculateDynamicStalledInterval(jobData._calculatedLockDuration);
        return {
            lockDuration: jobData._calculatedLockDuration,
            stalledInterval
        };
    }
    
    // Tentar usar métricas históricas primeiro (sistema adaptativo)
    let hasHistoricalMetrics = false;
    try {
        const metrics = getQueueMetrics();
        const adaptiveLockDuration = await metrics.getAdaptiveLockDuration(queueName, jobData);
        
        if (adaptiveLockDuration) {
            hasHistoricalMetrics = true;
            const adaptiveStalledInterval = await metrics.getAdaptiveStalledInterval(adaptiveLockDuration, queueName, jobData);
            return {
                lockDuration: adaptiveLockDuration,
                stalledInterval: adaptiveStalledInterval
            };
        }
    } catch (error) {
        // Se erro ao consultar métricas, continuar com cálculo estimativo
        const logger = require('../logger');
        logger.debug(`[Queue] Erro ao consultar métricas históricas, usando estimativa:`, error.message);
    }
    
    // Fallback: Calcular baseado no tipo de fila e dados do job (sem limites fixos)
    // Usar margem de segurança maior quando não há métricas históricas
    let lockDuration = null;
    
    if (queueName === QUEUE_NAMES.DISPARO_BATCH && jobData.contacts && Array.isArray(jobData.contacts)) {
        lockDuration = calculateDynamicLockDuration(jobData.contacts.length, queueName);
    } else if (queueName === QUEUE_NAMES.TIMEOUT) {
        // Para timeout, estimar baseado em complexidade do flow
        // Se tem flow_nodes, estimar baseado no número de nós
        if (jobData.flow_nodes) {
            try {
                const flowNodes = typeof jobData.flow_nodes === 'string' 
                    ? JSON.parse(jobData.flow_nodes) 
                    : jobData.flow_nodes;
                const nodes = Array.isArray(flowNodes) ? flowNodes : (flowNodes.nodes || []);
                // Estimativa: usar cálculo logarítmico para valores muito grandes
                const estimatedMinutes = nodes.length > MAX_NODES_FOR_LINEAR_CALCULATION
                    ? 5 + Math.log10(nodes.length) * 60 // Cálculo logarítmico para valores muito grandes
                    : 5 + (nodes.length * 1); // Cálculo linear normal
                
                // Usar margem maior quando não há métricas históricas (4x) vs quando há métricas (2x)
                const safetyMargin = hasHistoricalMetrics ? 2 : 4;
                lockDuration = estimatedMinutes * 60 * 1000 * safetyMargin;
                
                // Log informativo quando margem aumentada é aplicada
                if (!hasHistoricalMetrics) {
                    const logger = require('../logger');
                    logger.info(`[Queue] LockDuration calculado para TIMEOUT com margem aumentada (4x) - sem métricas históricas: ${Math.round(lockDuration / 60000)}min para ${nodes.length} nós`);
                }
                
                // Limitar ao máximo técnico se exceder
                const limits = getQueueMetricsConstants();
                if (lockDuration > limits.MAX_LOCK_DURATION_MS) {
                    const logger = require('../logger');
                    logger.warn(`[Queue] LockDuration calculado muito grande (${Math.round(lockDuration / 60000)}min para ${nodes.length} nós), limitando ao máximo técnico: ${Math.round(limits.MAX_LOCK_DURATION_MS / 60000)}min`);
                    lockDuration = limits.MAX_LOCK_DURATION_MS;
                }
            } catch (e) {
                // Se erro ao parsear, usar valor padrão de 24h para fluxos completos
                lockDuration = 24 * 60 * 60 * 1000; // 24 horas padrão para fluxos que podem durar até 24h
            }
        } else {
            // Sem informações de flow, considerar delay se presente
            if (jobData._delayMs && jobData._delayMs > 0) {
                // LockDuration = delay + margem para processamento, mas mínimo 24h para fluxos completos
                const minLockDurationForDelay = jobData._delayMs + (60 * 60 * 1000);
                lockDuration = Math.max(minLockDurationForDelay, 24 * 60 * 60 * 1000); // Mínimo 24h
            } else {
                // Sem delay e sem flow_nodes, usar 24 horas padrão para fluxos completos
                lockDuration = 24 * 60 * 60 * 1000; // 24 horas padrão
            }
        }
        
        // Garantir que lockDuration seja válido e pelo menos 24h para fluxos completos
        if (!lockDuration || !Number.isFinite(lockDuration) || lockDuration <= 0) {
            // Se cálculo inválido, usar padrão de 24h para fluxos completos
            lockDuration = jobData._delayMs && jobData._delayMs > 0 
                ? Math.max(jobData._delayMs + (60 * 60 * 1000), 24 * 60 * 60 * 1000)
                : 24 * 60 * 60 * 1000; // 24 horas padrão
        }
        
        // Garantir que lockDuration seja pelo menos 24h para fluxos que podem durar até 24h
        const MIN_LOCK_DURATION_FOR_TIMEOUT = 24 * 60 * 60 * 1000; // 24 horas
        lockDuration = Math.max(lockDuration, MIN_LOCK_DURATION_FOR_TIMEOUT);
        
        // Se tem delay, garantir que lockDuration seja pelo menos delay + margem (mas já garantimos 24h mínimo)
        if (jobData._delayMs && jobData._delayMs > 0) {
            const minLockDurationForDelay = jobData._delayMs + (60 * 60 * 1000);
            if (lockDuration < minLockDurationForDelay) {
                lockDuration = minLockDurationForDelay;
            }
        }
    } else if (queueName === QUEUE_NAMES.DISPARO_DELAY) {
        // Calcular baseado no delay do job + margem para processamento
        // O delay está em jobData.delay_seconds (convertido em addJobWithDelay)
        const delayMs = jobData.delay_seconds ? jobData.delay_seconds * 1000 : null;
        if (delayMs) {
            // Lock duration = delay + 2 horas de margem (para processamento após delay), sem limite máximo fixo
            lockDuration = delayMs + (2 * 60 * 60 * 1000);
        } else {
            // Se não tem delay, usar valor conservador alto (6 horas)
            lockDuration = 6 * 60 * 60 * 1000;
        }
    } else if (queueName === QUEUE_NAMES.DISPARO) {
        // Estimar baseado em complexidade do flow (similar ao TIMEOUT)
        if (jobData.flow_nodes) {
            try {
                const flowNodes = typeof jobData.flow_nodes === 'string' 
                    ? JSON.parse(jobData.flow_nodes) 
                    : jobData.flow_nodes;
                const nodes = Array.isArray(flowNodes) ? flowNodes : (flowNodes.nodes || []);
                // Estimativa: usar cálculo logarítmico para valores muito grandes
                const estimatedMinutes = nodes.length > MAX_NODES_FOR_LINEAR_CALCULATION
                    ? 5 + Math.log10(nodes.length) * 60 // Cálculo logarítmico para valores muito grandes
                    : 5 + (nodes.length * 1); // Cálculo linear normal
                
                // Usar margem maior quando não há métricas históricas (4x) vs quando há métricas (2x)
                const safetyMargin = hasHistoricalMetrics ? 2 : 4;
                lockDuration = estimatedMinutes * 60 * 1000 * safetyMargin;
                
                // Log informativo quando margem aumentada é aplicada
                if (!hasHistoricalMetrics) {
                    const logger = require('../logger');
                    logger.info(`[Queue] LockDuration calculado para DISPARO com margem aumentada (4x) - sem métricas históricas: ${Math.round(lockDuration / 60000)}min para ${nodes.length} nós`);
                }
            } catch (e) {
                lockDuration = 30 * 60 * 1000; // 30 minutos padrão
            }
        } else {
            lockDuration = 30 * 60 * 1000; // 30 minutos padrão
        }
    } else if (queueName === QUEUE_NAMES.VALIDATION_DISPARO) {
        // Estimar baseado em número de contatos a validar
        if (jobData.total_contacts || jobData.contacts) {
            const contactsCount = jobData.total_contacts || (Array.isArray(jobData.contacts) ? jobData.contacts.length : 0);
            if (contactsCount > 0) {
                // Estimativa: 0.1 segundo por contato + base, sem limite máximo fixo
                const estimatedSeconds = 60 + (contactsCount * 0.1);
                
                // Usar margem maior quando não há métricas históricas (4x) vs quando há métricas (2x)
                const safetyMargin = hasHistoricalMetrics ? 2 : 4;
                lockDuration = estimatedSeconds * 1000 * safetyMargin;
                
                // Log informativo quando margem aumentada é aplicada
                if (!hasHistoricalMetrics) {
                    const logger = require('../logger');
                    logger.info(`[Queue] LockDuration calculado para VALIDATION_DISPARO com margem aumentada (4x) - sem métricas históricas: ${Math.round(lockDuration / 60000)}min para ${contactsCount} contatos`);
                }
            } else {
                lockDuration = 30 * 60 * 1000; // 30 minutos padrão
            }
        } else {
            lockDuration = 30 * 60 * 1000; // 30 minutos padrão
        }
    } else if (queueName === QUEUE_NAMES.SCHEDULED_DISPARO) {
        // Estimar baseado em número de contatos (similar ao VALIDATION_DISPARO)
        if (jobData.total_contacts || jobData.contacts) {
            const contactsCount = jobData.total_contacts || (Array.isArray(jobData.contacts) ? jobData.contacts.length : 0);
            if (contactsCount > 0) {
                // Estimativa: 0.05 segundo por contato + base, sem limite máximo fixo
                const estimatedSeconds = 30 + (contactsCount * 0.05);
                
                // Usar margem maior quando não há métricas históricas (4x) vs quando há métricas (2x)
                const safetyMargin = hasHistoricalMetrics ? 2 : 4;
                lockDuration = estimatedSeconds * 1000 * safetyMargin;
                
                // Log informativo quando margem aumentada é aplicada
                if (!hasHistoricalMetrics) {
                    const logger = require('../logger');
                    logger.info(`[Queue] LockDuration calculado para SCHEDULED_DISPARO com margem aumentada (4x) - sem métricas históricas: ${Math.round(lockDuration / 60000)}min para ${contactsCount} contatos`);
                }
            } else {
                lockDuration = 20 * 60 * 1000; // 20 minutos padrão
            }
        } else {
            lockDuration = 20 * 60 * 1000; // 20 minutos padrão
        }
    }
    
    // Se não calculou, retornar null para usar valores padrão da fila
    if (!lockDuration || !Number.isFinite(lockDuration) || lockDuration <= 0) {
        return null;
    }
    
    // Validação adicional para jobs TIMEOUT com delay
    if (queueName === QUEUE_NAMES.TIMEOUT && jobData._delayMs && jobData._delayMs > 0) {
        // Garantir que lockDuration seja pelo menos delay + margem
        const minLockDuration = jobData._delayMs + (60 * 60 * 1000);
        if (lockDuration < minLockDuration) {
            const logger = require('../logger');
            logger.warn(`[Queue] Ajustando lockDuration para TIMEOUT: ${Math.round(lockDuration/60000)}min -> ${Math.round(minLockDuration/60000)}min (delay: ${Math.round(jobData._delayMs/60000)}min)`);
            lockDuration = minLockDuration;
        }
    }
    
    // Limitar ao máximo técnico se exceder
    const limits = getQueueMetricsConstants();
    if (lockDuration > limits.MAX_LOCK_DURATION_MS) {
        const logger = require('../logger');
        logger.warn(`[Queue] LockDuration muito grande (${Math.round(lockDuration / 60000)}min), limitando ao máximo técnico: ${Math.round(limits.MAX_LOCK_DURATION_MS / 60000)}min`);
        lockDuration = limits.MAX_LOCK_DURATION_MS;
    }
    
    // Usar cálculo adaptativo de stalledInterval quando possível
    const metrics = getQueueMetrics();
    let stalledInterval = await metrics.getAdaptiveStalledInterval(lockDuration, queueName, jobData) ||
        calculateDynamicStalledInterval(lockDuration);
    
    // Ajustar stalledInterval para jobs TIMEOUT com delay
    if (queueName === QUEUE_NAMES.TIMEOUT && jobData._delayMs && jobData._delayMs > 0) {
        const minStalledInterval = jobData._delayMs * 2;
        if (stalledInterval < minStalledInterval) {
            stalledInterval = minStalledInterval;
        }
    }
    
    return { lockDuration, stalledInterval };
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
    
    // Para DISPARO_DELAY, converter delay para segundos e armazenar em jobData
    if (queueName === QUEUE_NAMES.DISPARO_DELAY && delayMs > 0) {
        data.delay_seconds = Math.floor(delayMs / 1000);
    }
    
    // Para TIMEOUT, adicionar delay ao jobData temporariamente para cálculo
    if (queueName === QUEUE_NAMES.TIMEOUT && delayMs > 0) {
        data._delayMs = delayMs;
    }
    
    // Calcular lockDuration dinâmico para jobs que precisam (agora async)
    let optimalLock;
    try {
        optimalLock = await getOptimalLockDuration(queueName, data);
    } catch (error) {
        const logger = require('../logger');
        logger.error(`[Queue] Erro ao calcular lockDuration para ${queueName}:`, error);
        optimalLock = null;
    }
    
    // Remover campo temporário
    if (data._delayMs !== undefined) {
        delete data._delayMs;
    }
    
    if (optimalLock) {
        // Para jobs TIMEOUT, garantir que lockDuration seja adequado para fluxos de até 24h
        // Fluxos podem durar até 24h no total e delays podem ser até 1h
        if (queueName === QUEUE_NAMES.TIMEOUT) {
            // LockDuration mínimo: 24 horas para acomodar fluxos completos
            const MIN_LOCK_DURATION_FOR_TIMEOUT = 24 * 60 * 60 * 1000; // 24 horas
            optimalLock.lockDuration = Math.max(optimalLock.lockDuration || 0, MIN_LOCK_DURATION_FOR_TIMEOUT);
            
            // Se tem delay, garantir que lockDuration seja pelo menos delay + margem (mas mínimo já é 24h)
            if (delayMs > 0) {
                // LockDuration deve ser pelo menos: delay + margem para processamento (mas já garantimos 24h mínimo)
                const minLockDurationForDelay = delayMs + (60 * 60 * 1000);
                optimalLock.lockDuration = Math.max(optimalLock.lockDuration, minLockDurationForDelay);
                
                // stalledInterval deve ser pelo menos 1.5x o delay para evitar falsos positivos
                if (optimalLock.stalledInterval) {
                    optimalLock.stalledInterval = Math.max(
                        optimalLock.stalledInterval,
                        delayMs * 1.5 // Pelo menos 1.5x o delay (delays máx 1h)
                    );
                }
            }
        }
        
        // Armazenar valores calculados no job data para o worker usar
        data._calculatedLockDuration = optimalLock.lockDuration;
        data._calculatedStalledInterval = optimalLock.stalledInterval;
        
        // Registrar métrica Prometheus do lockDuration calculado
        try {
            const metrics = require('../metrics');
            if (metrics && metrics.metrics && metrics.metrics.queueAdaptiveLockDuration) {
                const source = optimalLock.source || 'estimated';
                metrics.metrics.queueAdaptiveLockDuration.observe(
                    { queue_name: queueName, source },
                    optimalLock.lockDuration / 1000
                );
            }
        } catch (e) {
            // Ignorar erro se métricas não disponíveis
        }
    } else {
        // Fallback: garantir que jobs TIMEOUT sempre tenham lockDuration calculado
        if (queueName === QUEUE_NAMES.TIMEOUT) {
            const logger = require('../logger');
            logger.warn(`[Queue] getOptimalLockDuration retornou null para TIMEOUT, usando fallback baseado em delay: ${delayMs}ms`);
            
            // Calcular lockDuration baseado no delay (se houver) + margem, ou usar padrão de 24h
            // Fluxos podem durar até 24h no total
            let fallbackLockDuration;
            if (delayMs > 0) {
                // LockDuration = delay + margem para processamento, mas mínimo 24h para fluxos completos
                const minLockDurationForDelay = delayMs + (60 * 60 * 1000);
                fallbackLockDuration = Math.max(minLockDurationForDelay, 24 * 60 * 60 * 1000); // Mínimo 24h
            } else {
                // Sem delay, usar 24 horas padrão para acomodar fluxos completos
                fallbackLockDuration = 24 * 60 * 60 * 1000; // 24 horas
            }
            
            const fallbackStalledInterval = Math.max(
                delayMs > 0 ? delayMs * 1.5 : 4 * 60 * 60 * 1000, // 1.5x delay ou 4h (padrão para fluxos longos)
                4 * 60 * 60 * 1000 // Mínimo 4h para fluxos que podem durar até 24h
            );
            
            data._calculatedLockDuration = fallbackLockDuration;
            data._calculatedStalledInterval = fallbackStalledInterval;
        }
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
    
    // #region agent log
    if (queueName === QUEUE_NAMES.TIMEOUT && delayMs > 0) {
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'queue.js:905',message:'Job TIMEOUT criado com delay',data:{queueName,jobId:jobOptions.jobId,delayMs,delayMinutes:Math.round(delayMs/60000),calculatedLockDuration:data._calculatedLockDuration,calculatedStalledInterval:data._calculatedStalledInterval,lockDurationMinutes:data._calculatedLockDuration?Math.round(data._calculatedLockDuration/60000):null,stalledIntervalMinutes:data._calculatedStalledInterval?Math.round(data._calculatedStalledInterval/60000):null},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
    }
    // #endregion
    
    const job = await redisCircuitBreaker.execute(async () => {
        return await queue.add(jobName, data, jobOptions);
    });
    
    // #region agent log
    if (queueName === QUEUE_NAMES.TIMEOUT && delayMs > 0) {
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'queue.js:912',message:'Job TIMEOUT adicionado à fila',data:{queueName,jobId:job.id,actualDelayMs:delayMs,createdAt:Date.now()},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
    }
    // #endregion
    
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
