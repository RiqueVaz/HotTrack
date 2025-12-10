/**
 * Worker para processar requisições HTTP de APIs externas
 * Processa jobs das filas api-{provider}-{sellerId}
 */

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const { Worker } = require('bullmq');
const axios = require('axios');
const { redisConnection } = require('../shared/queue');
const logger = require('../logger');

// Configurações por provedor (mesmas do api-rate-limiter-bullmq.js)
const PROVIDER_CONFIGS = {
    'pushinpay': { limiter: { max: 2, duration: 1000 }, concurrency: 1, timeout: 20000 },
    'syncpay': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 10000 },
    'brpix': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 20000 },
    'cnpay': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 20000 },
    'oasyfy': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 20000 },
    'pixup': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 20000 },
    'wiinpay': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 10000 },
    'paradise': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 10000 },
    'ip-api': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 5000 },
    'utmify': { limiter: { max: 1, duration: 2000 }, concurrency: 1, timeout: 20000 },
    'default': { limiter: { max: 1, duration: 1000 }, concurrency: 1, timeout: 10000 }
};

// Workers ativos por fila
const workers = new Map();

/**
 * Cria worker para uma fila específica
 */
function createWorkerForQueue(queueName) {
    // Extrair provider do nome da fila: api-{provider}-{sellerId}
    const match = queueName.match(/^api-(.+?)-(\d+)$/);
    if (!match) {
        logger.warn(`[API Rate Limiter Worker] Nome de fila inválido: ${queueName}`);
        return null;
    }
    
    const provider = match[1];
    const config = PROVIDER_CONFIGS[provider] || PROVIDER_CONFIGS['default'];
    
    const worker = new Worker(
        queueName,
        async (job) => {
            const { method, url, headers, data, params, timeout } = job.data;
            
            try {
                const response = await axios({
                    method,
                    url,
                    headers,
                    data,
                    params,
                    timeout: timeout || config.timeout
                });
                
                return response.data;
            } catch (error) {
                // Tratamento especial para 405 (método não permitido)
                if (error.response?.status === 405) {
                    const methodError = new Error(`Método HTTP não permitido (405) para ${url}. Método usado: ${method || 'não especificado'}. Verifique se o método está correto (POST para PIX).`);
                    methodError.status = 405;
                    methodError.method = method;
                    methodError.url = url;
                    logger.error(`[API Rate Limiter Worker] Erro 405:`, methodError.message);
                    throw methodError;
                }
                
                // Tratamento especial para 429
                if (error.response?.status === 429) {
                    const retryAfter = parseInt(
                        error.response.headers['retry-after'] || 
                        error.response.headers['Retry-After'] || 
                        '5'
                    );
                    
                    // Para alguns provedores, não fazer retry
                    if (provider === 'ip-api' || provider === 'utmify') {
                        throw error;
                    }
                    
                    // Para outros, lançar erro que será tratado pelo BullMQ retry
                    const rateLimitError = new Error(`Rate limit 429: ${error.message}`);
                    rateLimitError.status = 429;
                    rateLimitError.retryAfter = retryAfter;
                    throw rateLimitError;
                }
                
                throw error;
            }
        },
        {
            connection: redisConnection,
            concurrency: config.concurrency,
            limiter: config.limiter,
            removeOnComplete: { age: 3600 },
            removeOnFail: { age: 86400 }
        }
    );
    
    worker.on('completed', (job) => {
        logger.debug(`[API Rate Limiter Worker] Job ${job.id} completado na fila ${queueName}`);
    });
    
    worker.on('failed', (job, err) => {
        logger.warn(`[API Rate Limiter Worker] Job ${job.id} falhou na fila ${queueName}:`, err.message);
    });
    
    worker.on('error', (err) => {
        logger.error(`[API Rate Limiter Worker] Erro no worker da fila ${queueName}:`, err);
    });
    
    return worker;
}

/**
 * Inicializa workers para todas as filas existentes
 */
async function initializeWorkers() {
    // Buscar todas as filas api-* do Redis
    const keys = await redisConnection.keys('bull:api-*:meta');
    const queueNames = new Set();
    
    for (const key of keys) {
        // Extrair nome da fila: bull:{queueName}:meta
        const match = key.match(/^bull:(.+?):meta$/);
        if (match) {
            queueNames.add(match[1]);
        }
    }
    
    // Criar workers para cada fila
    for (const queueName of queueNames) {
        if (!workers.has(queueName)) {
            const worker = createWorkerForQueue(queueName);
            if (worker) {
                workers.set(queueName, worker);
                logger.info(`[API Rate Limiter Worker] Worker criado para fila ${queueName}`);
            }
        }
    }
}

/**
 * Cria worker dinamicamente quando nova fila é criada
 */
function getOrCreateWorker(queueName) {
    if (!workers.has(queueName)) {
        const worker = createWorkerForQueue(queueName);
        if (worker) {
            workers.set(queueName, worker);
            logger.info(`[API Rate Limiter Worker] Worker criado dinamicamente para fila ${queueName}`);
        }
        return worker;
    }
    return workers.get(queueName);
}

/**
 * Fecha todos os workers
 */
async function closeAll() {
    for (const worker of workers.values()) {
        await worker.close();
    }
    workers.clear();
}

// Inicializar workers ao iniciar
if (require.main === module) {
    initializeWorkers().catch(err => {
        logger.error('[API Rate Limiter Worker] Erro ao inicializar workers:', err);
        process.exit(1);
    });
    
    // Reinicializar a cada 30 segundos para pegar novas filas
    setInterval(() => {
        initializeWorkers().catch(err => {
            logger.warn('[API Rate Limiter Worker] Erro ao reinicializar workers:', err.message);
        });
    }, 30000);
}

module.exports = {
    getOrCreateWorker,
    initializeWorkers,
    closeAll
};

