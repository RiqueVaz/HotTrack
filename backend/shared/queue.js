// Configuração BullMQ e Redis para substituir QStash
const { Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

// Configuração Redis
const redisConnection = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
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

// Cache de queues (singleton)
const queues = new Map();
const queueEvents = new Map();

/**
 * Obtém ou cria uma queue BullMQ
 */
function getQueue(queueName) {
    if (!queues.has(queueName)) {
        const queue = new Queue(queueName, {
            connection: redisConnection,
            defaultJobOptions: {
                attempts: 3, // 2 retries + 1 tentativa inicial = 3 total (equivalente ao QStash)
                backoff: {
                    type: 'exponential',
                    delay: 2000,
                },
                removeOnComplete: {
                    age: 24 * 3600, // Manter jobs completos por 24 horas
                    count: 1000,
                },
                removeOnFail: {
                    age: 7 * 24 * 3600, // Manter jobs falhados por 7 dias
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
    closeAll,
    scheduleRecurringCleanupQRCodes,
    QUEUE_NAMES,
    BULLMQ_CONCURRENCY,
    BULLMQ_RATE_LIMIT_MAX,
    BULLMQ_RATE_LIMIT_WINDOW,
    redisConnection,
};
