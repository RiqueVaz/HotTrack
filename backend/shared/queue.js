// Configuração BullMQ e Redis para substituir QStash
const { Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

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
        concurrency: 400, // Alta concorrência para disparos (aumentado de 200 para acelerar)
        limiter: undefined, // Remover limiter - rate limiting manual faz o trabalho
        stalledInterval: 600000, // 10 minutos (aumentado de 5min para dar mais margem)
        maxStalledCount: 2,
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
    },
    [QUEUE_NAMES.DISPARO]: {
        concurrency: 50,
        limiter: undefined,
        stalledInterval: 60000, // 1 minuto
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
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.VALIDATION_DISPARO]: {
        concurrency: 10,
        limiter: { max: 10, duration: 1000 },
        stalledInterval: 300000, // 5 minutos
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000,
        },
    },
    [QUEUE_NAMES.SCHEDULED_DISPARO]: {
        concurrency: 5,
        limiter: { max: 5, duration: 1000 },
        stalledInterval: 60000,
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
 * Adiciona um job com delay (equivalente ao publishJSON do QStash)
 * @param {string} queueName - Nome da fila
 * @param {string} jobName - Nome do job
 * @param {object} data - Dados do job
 * @param {object} options - Opções (delay, botToken para rate limiting, etc.)
 */
async function addJobWithDelay(queueName, jobName, data, options = {}) {
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
    
    // Configurar rate limiting por bot token se fornecido
    const jobOptions = {
        delay: delayMs,
        jobId: jobId || undefined, // Para deduplicação baseada em conteúdo
        priority: queueName === QUEUE_NAMES.DISPARO_BATCH ? 1 : 0, // Prioridade para disparos (maior número = maior prioridade)
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
    
    const job = await queue.add(jobName, data, jobOptions);
    
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
};
