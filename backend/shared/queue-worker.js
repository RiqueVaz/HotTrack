// Workers BullMQ para processar jobs das filas
const { Worker } = require('bullmq');
const { RateLimiter } = require('bullmq');
const { QUEUE_NAMES, BULLMQ_CONCURRENCY, BULLMQ_RATE_LIMIT_MAX, BULLMQ_RATE_LIMIT_WINDOW, redisConnection } = require('./queue');
const { processTimeoutData } = require('../worker/process-timeout');
const { processDisparoData, processDisparoBatchData } = require('../worker/process-disparo');
const logger = require('../logger');

// Cache de rate limiters por bot token
const rateLimiters = new Map();

/**
 * Obtém ou cria um rate limiter para um bot token
 */
function getRateLimiter(botToken) {
    if (!botToken) return null;
    
    if (!rateLimiters.has(botToken)) {
        const limiter = new RateLimiter({
            max: BULLMQ_RATE_LIMIT_MAX,
            duration: BULLMQ_RATE_LIMIT_WINDOW,
            keyPrefix: `rate-limit:${botToken}`,
        });
        rateLimiters.set(botToken, limiter);
    }
    
    return rateLimiters.get(botToken);
}

/**
 * Cria um worker para uma fila
 */
function createWorker(queueName, processor, options = {}) {
    const { concurrency = BULLMQ_CONCURRENCY } = options;
    
    const worker = new Worker(
        queueName,
        async (job) => {
            const { data } = job;
            const botToken = data._botToken;
            
            // Aplicar rate limiting se botToken fornecido
            if (botToken) {
                const limiter = getRateLimiter(botToken);
                if (limiter) {
                    try {
                        await limiter.consume(botToken);
                    } catch (error) {
                        // Rate limit excedido, rejeitar job para retry
                        throw new Error(`Rate limit exceeded for bot token. Retrying...`);
                    }
                }
            }
            
            // Processar job
            return await processor(data, job);
        },
        {
            connection: redisConnection,
            concurrency,
            limiter: {
                max: concurrency,
                duration: 1000,
            },
        }
    );
    
    // Event handlers
    worker.on('completed', (job) => {
        logger.debug(`[BullMQ] Job ${job.id} completed in queue ${queueName}`);
    });
    
    worker.on('failed', (job, err) => {
        logger.error(`[BullMQ] Job ${job?.id} failed in queue ${queueName}:`, err);
    });
    
    worker.on('error', (err) => {
        logger.error(`[BullMQ] Worker error in queue ${queueName}:`, err);
    });
    
    return worker;
}

// Processadores para cada tipo de job
const processors = {
    [QUEUE_NAMES.TIMEOUT]: async (data) => {
        await processTimeoutData(data);
    },
    
    [QUEUE_NAMES.DISPARO]: async (data) => {
        await processDisparoData(data);
    },
    
    [QUEUE_NAMES.DISPARO_BATCH]: async (data) => {
        await processDisparoBatchData(data);
    },
    
    [QUEUE_NAMES.DISPARO_DELAY]: async (data) => {
        // Processar disparo após delay (chama processDisparoData)
        await processDisparoData(data);
    },
    
    [QUEUE_NAMES.VALIDATION_DISPARO]: async (data) => {
        // Processar validação e disparo (chama processDisparoBatchData)
        await processDisparoBatchData(data);
    },
    
    [QUEUE_NAMES.SCHEDULED_DISPARO]: async (data) => {
        // Processar disparo agendado (chama processDisparoBatchData)
        await processDisparoBatchData(data);
    },
};

// Cache de workers (singleton)
const workers = new Map();

/**
 * Inicializa todos os workers
 */
function initializeWorkers() {
    for (const [queueName, processor] of Object.entries(processors)) {
        if (!workers.has(queueName)) {
            const worker = createWorker(queueName, processor);
            workers.set(queueName, worker);
            logger.info(`[BullMQ] Worker initialized for queue: ${queueName}`);
        }
    }
}

/**
 * Fecha todos os workers
 */
async function closeAllWorkers() {
    for (const worker of workers.values()) {
        await worker.close();
    }
    workers.clear();
    rateLimiters.clear();
}

module.exports = {
    initializeWorkers,
    closeAllWorkers,
    createWorker,
    processors,
};
