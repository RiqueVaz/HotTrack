// Workers BullMQ para processar jobs das filas
const { Worker, RateLimitError } = require('bullmq');
const { QUEUE_NAMES, BULLMQ_CONCURRENCY, BULLMQ_RATE_LIMIT_MAX, BULLMQ_RATE_LIMIT_WINDOW, redisConnection } = require('./queue');
const { processTimeoutData } = require('../worker/process-timeout');
const { processDisparoData, processDisparoBatchData } = require('../worker/process-disparo');
const logger = require('../logger');
const { sqlTx, sqlWithRetry } = require('../db');

/**
 * Implementa rate limiting por bot token usando Redis
 * Usa sliding window log algorithm para rate limiting
 */
async function checkRateLimit(botToken) {
    if (!botToken) return true; // Sem bot token, permite processamento
    
    const key = `rate-limit:${botToken}`;
    const now = Date.now();
    const windowStart = now - BULLMQ_RATE_LIMIT_WINDOW;
    const requestId = `${now}-${Math.random()}`;
    
    try {
        // Usar pipeline do Redis para operações atômicas
        const pipeline = redisConnection.pipeline();
        
        // Remover entradas antigas (fora da janela)
        pipeline.zremrangebyscore(key, 0, windowStart);
        
        // Adicionar timestamp atual
        pipeline.zadd(key, now, requestId);
        
        // Contar requisições na janela atual (depois de adicionar)
        pipeline.zcard(key);
        
        // Definir expiração da chave
        pipeline.expire(key, Math.ceil(BULLMQ_RATE_LIMIT_WINDOW / 1000));
        
        const results = await pipeline.exec();
        
        // results[2][1] é o resultado do zcard (contagem depois de adicionar)
        const currentCount = results[2][1];
        
        // Se já atingiu o limite, remover a entrada que acabamos de adicionar
        if (currentCount > BULLMQ_RATE_LIMIT_MAX) {
            await redisConnection.zrem(key, requestId);
            return false; // Rate limit excedido
        }
        
        return true; // Dentro do limite
    } catch (error) {
        logger.error(`[RateLimit] Erro ao verificar rate limit para bot token:`, error);
        // Em caso de erro, permitir processamento (fail open)
        return true;
    }
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
                const allowed = await checkRateLimit(botToken);
                if (!allowed) {
                    // Rate limit excedido, rejeitar job para retry
                    // Usar RateLimitError para que o BullMQ trate como rate limit, não como falha
                    throw new RateLimitError(`Rate limit exceeded for bot token. Retrying...`);
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
        // Processar disparo após delay
        // Buscar dados do fluxo do banco, pois o payload não inclui flow_nodes, flow_edges e start_node_id
        const { history_id, chat_id, bot_id, current_node_id, variables, remaining_actions } = data;
        
        if (!history_id) {
            throw new Error('history_id é obrigatório para processar delay de disparo');
        }
        
        try {
            // Buscar disparo_flow_id do histórico
            const [history] = await sqlWithRetry(sqlTx`
                SELECT disparo_flow_id
                FROM disparo_history
                WHERE id = ${history_id}
            `);
            
            if (!history || !history.disparo_flow_id) {
                throw new Error(`Histórico de disparo ${history_id} não encontrado ou sem disparo_flow_id`);
            }
            
            // Buscar o fluxo de disparo
            const [disparoFlow] = await sqlWithRetry(sqlTx`
                SELECT nodes
                FROM disparo_flows
                WHERE id = ${history.disparo_flow_id}
            `);
            
            if (!disparoFlow || !disparoFlow.nodes) {
                throw new Error(`Fluxo de disparo ${history.disparo_flow_id} não encontrado ou sem nodes`);
            }
            
            // Parsear nodes do fluxo
            const flowData = typeof disparoFlow.nodes === 'string' ? JSON.parse(disparoFlow.nodes) : disparoFlow.nodes;
            const flowNodes = flowData.nodes || [];
            const flowEdges = flowData.edges || [];
            
            // Encontrar o start_node_id (trigger node ou primeiro action node)
            let startNodeId = null;
            const triggerNode = flowNodes.find(node => node.type === 'trigger');
            
            if (triggerNode) {
                startNodeId = triggerNode.id;
            } else {
                const actionNode = flowNodes.find(node => node.type === 'action');
                if (actionNode) {
                    startNodeId = actionNode.id;
                }
            }
            
            if (!startNodeId) {
                throw new Error(`Nenhum nó inicial (trigger ou action) encontrado no fluxo ${history.disparo_flow_id}`);
            }
            
            // Se current_node_id foi fornecido, usar ele como start_node_id para continuar de onde parou
            // Caso contrário, usar o trigger/action node encontrado
            const finalStartNodeId = current_node_id || startNodeId;
            
            // Parsear variables se for objeto, senão usar como está
            const variablesJson = typeof variables === 'string' ? variables : JSON.stringify(variables || {});
            
            // Construir payload completo para processDisparoData
            const completeData = {
                history_id,
                chat_id,
                bot_id,
                flow_nodes: JSON.stringify(flowNodes),
                flow_edges: JSON.stringify(flowEdges),
                start_node_id: finalStartNodeId,
                variables_json: variablesJson
            };
            
            // Processar disparo com dados completos
            await processDisparoData(completeData);
        } catch (error) {
            logger.error(`[BullMQ] Erro ao processar delay de disparo para history_id ${history_id}:`, error);
            throw error; // Re-throw para que o BullMQ trate como falha e faça retry
        }
    },
    
    [QUEUE_NAMES.VALIDATION_DISPARO]: async (data) => {
        // Processar validação e disparo (chama processDisparoBatchData)
        await processDisparoBatchData(data);
    },
    
    [QUEUE_NAMES.SCHEDULED_DISPARO]: async (data) => {
        // Processar disparo agendado (chama processDisparoBatchData)
        await processDisparoBatchData(data);
    },
    
    [QUEUE_NAMES.CLEANUP_QRCODES]: async (data) => {
        // Processar limpeza de QR codes
        const { cleanupQRCodesInBatches } = require('../scripts/cleanup-qrcodes-batch');
        return await cleanupQRCodesInBatches();
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
}

module.exports = {
    initializeWorkers,
    closeAllWorkers,
    createWorker,
    processors,
};
