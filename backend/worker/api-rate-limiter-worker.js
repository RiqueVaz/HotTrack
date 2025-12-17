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
    'pushinpay': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 15000 }, // Reduzido para evitar 429
    'syncpay': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 10000 },
    'brpix': { limiter: { max: 1, duration: 2000 }, concurrency: 3, timeout: 15000 }, // Reduzido de 20s para 15s
    'cnpay': { limiter: { max: 1, duration: 2000 }, concurrency: 3, timeout: 15000 }, // Reduzido de 20s para 15s
    'oasyfy': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 15000 }, // Reduzido para evitar timeouts
    'pixup': { limiter: { max: 1, duration: 2000 }, concurrency: 3, timeout: 15000 }, // Reduzido de 20s para 15s
    'wiinpay': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 10000 },
    'paradise': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 10000 },
    'ip-api': { limiter: { max: 1, duration: 1500 }, concurrency: 1, timeout: 10000 }, // 1 req/1.5s, concorrência 1, timeout 10s
    'ipwhois-app': { limiter: { max: 1, duration: 1000 }, concurrency: 1, timeout: 10000 }, // 1 req/1s - API ilimitada, mas manter conservador
    'apip-cc': { limiter: { max: 1, duration: 100 }, concurrency: 1, timeout: 10000 }, // 1 req/100ms (10 req/s) - API permite 50 req/s, mas manter conservador
    'utmify': { limiter: { max: 1, duration: 2000 }, concurrency: 2, timeout: 20000 },
    'default': { limiter: { max: 1, duration: 1000 }, concurrency: 1, timeout: 10000 }
};

// Workers ativos por fila
const workers = new Map();

/**
 * Cria worker para uma fila específica
 */
function createWorkerForQueue(queueName) {
    try {
        // Extrair provider do nome da fila: api-{provider}-{sellerId}
        const match = queueName.match(/^api-(.+?)-(\d+)$/);
        if (!match) {
            logger.error(`[API Rate Limiter Worker] Nome de fila inválido: ${queueName}`);
            return null;
        }
        
        const provider = match[1];
        const sellerId = match[2];
        const config = PROVIDER_CONFIGS[provider] || PROVIDER_CONFIGS['default'];
        
        if (!config) {
            logger.error(`[API Rate Limiter Worker] Configuração não encontrada para provider ${provider} na fila ${queueName}`);
            return null;
        }
        
        // Verificar se já existe worker para esta fila (evitar duplicação)
        if (workers.has(queueName)) {
            logger.debug(`[API Rate Limiter Worker] Worker já existe para ${queueName}`);
            return workers.get(queueName);
        }
        
        logger.info(`[API Rate Limiter Worker] Criando worker para ${queueName} (provider: ${provider}, sellerId: ${sellerId})`);
        
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
                    
                    // Para utmify, aguardar retry-after antes de falhar
                    if (provider === 'utmify') {
                        logger.warn(`[API Rate Limiter Worker] Rate limit 429 para ${provider}. Aguardando ${retryAfter}s antes de falhar.`);
                        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                        // Após aguardar, lançar erro para que BullMQ faça retry
                        const rateLimitError = new Error(`Rate limit 429 após aguardar ${retryAfter}s: ${error.message}`);
                        rateLimitError.status = 429;
                        rateLimitError.retryAfter = retryAfter;
                        throw rateLimitError;
                    }
                    
                    // Para alguns provedores, não fazer retry (fallback será usado)
                    if (provider === 'ip-api' || provider === 'ipwhois-app' || provider === 'apip-cc') {
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
    
    worker.on('ready', () => {
        logger.info(`[API Rate Limiter Worker] Worker pronto para processar jobs na fila ${queueName}`);
    });
    
        // Verificar se worker foi criado corretamente
        if (!worker) {
            logger.error(`[API Rate Limiter Worker] Worker é null para ${queueName}`);
            return null;
        }
        
        // Verificar se worker é uma instância válida (Worker do BullMQ)
        // Não verificar propriedades específicas pois podem não estar disponíveis imediatamente
        if (typeof worker !== 'object') {
            logger.error(`[API Rate Limiter Worker] Worker inválido criado para ${queueName} - não é um objeto`);
            return null;
        }
        
        logger.info(`[API Rate Limiter Worker] Worker criado com sucesso para ${queueName}`);
        return worker;
    } catch (error) {
        logger.error(`[API Rate Limiter Worker] Erro ao criar worker para ${queueName}:`, error);
        logger.error(`[API Rate Limiter Worker] Stack trace:`, error.stack);
        return null;
    }
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
 * Retorna o worker existente ou cria um novo
 */
function getOrCreateWorker(queueName) {
    // Se já existe, retornar
    if (workers.has(queueName)) {
        return workers.get(queueName);
    }
    
    // Criar novo worker
    try {
        const worker = createWorkerForQueue(queueName);
        if (worker) {
            workers.set(queueName, worker);
            logger.info(`[API Rate Limiter Worker] Worker criado dinamicamente para fila ${queueName}`);
            return worker;
        } else {
            logger.warn(`[API Rate Limiter Worker] createWorkerForQueue retornou null para ${queueName}`);
            return null;
        }
    } catch (error) {
        logger.error(`[API Rate Limiter Worker] Erro ao criar worker para ${queueName}:`, error);
        return null;
    }
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

