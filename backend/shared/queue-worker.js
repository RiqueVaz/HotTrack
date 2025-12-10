// Workers BullMQ para processar jobs das filas
const { Worker, RateLimitError } = require('bullmq');
const { QUEUE_NAMES, QUEUE_CONFIGS, BULLMQ_CONCURRENCY, redisConnection, checkRateLimit } = require('./queue');
const { processTimeoutData } = require('../worker/process-timeout');
const { processDisparoData, processDisparoBatchData } = require('../worker/process-disparo');
const logger = require('../logger');
const { sqlTx, sqlWithRetry } = require('../db');

/**
 * Cria um worker para uma fila com configurações otimizadas
 */
function createWorker(queueName, processor, options = {}) {
    // Obter configurações específicas da fila ou usar padrões
    const config = QUEUE_CONFIGS[queueName] || {
        concurrency: BULLMQ_CONCURRENCY,
        limiter: { max: BULLMQ_CONCURRENCY, duration: 1000 },
        stalledInterval: 30000,
        maxStalledCount: 2,
    };
    
    const concurrency = options.concurrency || config.concurrency;
    const limiter = config.limiter;
    const stalledInterval = config.stalledInterval || 30000;
    const maxStalledCount = config.maxStalledCount || 2;
    const lockDuration = config.lockDuration; // lockDuration específico da fila (opcional)
    
    const worker = new Worker(
        queueName,
        async (job) => {
            const { data } = job;
            const botToken = data._botToken;
            
            // Log detalhado do início do processamento
            if (queueName === QUEUE_NAMES.DISPARO_BATCH) {
                logger.info(`[BullMQ-Worker] Processando job ${job.id} da fila ${queueName}`, {
                    jobId: job.id,
                    historyId: data.history_id,
                    batchIndex: data.batch_index,
                    totalBatches: data.total_batches,
                    contactsCount: data.contacts?.length || 0,
                    hasBotToken: !!botToken,
                    botTokenLength: botToken?.length || 0
                });
            }
            
            // Rate limiting pré-emptive já foi feito antes de adicionar o job
            // Manter apenas verificação básica como fallback de segurança (opcional)
            // Se rate limit foi excedido mesmo assim, usar RateLimitError para retry com backoff
            if (botToken && queueName === QUEUE_NAMES.DISPARO_BATCH) {
                // Verificação rápida como fallback (não deve ser necessário na maioria dos casos)
                const allowed = await checkRateLimit(botToken);
                if (!allowed) {
                    // Se rate limit ainda excedido (raro, mas possível), usar RateLimitError para retry
                    // Mudar para debug para reduzir spam de logs
                    logger.debug(`[BullMQ-Worker] Rate limit ainda excedido no worker para job ${job.id} (fallback)`, {
                        jobId: job.id,
                        queueName,
                        historyId: data.history_id,
                        attemptsMade: job.attemptsMade || 0
                    });
                    throw new RateLimitError(`Rate limit exceeded for bot token. Retrying with backoff...`);
                }
            } else if (queueName === QUEUE_NAMES.DISPARO_BATCH && !botToken) {
                // Log warning se não houver botToken para disparo batch
                logger.warn(`[BullMQ-Worker] Job ${job.id} sem botToken (pode causar problemas)`, {
                    jobId: job.id,
                    historyId: data.history_id,
                    batchIndex: data.batch_index
                });
            }
            
            // Processar job com tratamento de erro robusto
            try {
                return await processor(data, job);
            } catch (error) {
                logger.error(`[BullMQ-Worker] Erro ao processar job ${job.id} na fila ${queueName}:`, {
                    jobId: job.id,
                    queueName,
                    historyId: data.history_id,
                    batchIndex: data.batch_index,
                    error: error.message,
                    stack: error.stack
                });
                throw error; // Re-throw para que BullMQ faça retry
            }
        },
        {
            connection: redisConnection,
            concurrency,
            limiter: limiter, // Usar limiter específico da fila (pode ser undefined)
            // Configurações para evitar jobs stalled prematuramente
            stalledInterval: stalledInterval,
            maxStalledCount: maxStalledCount,
            // lockDuration para jobs delayed com delays longos (evita erros de lock renewal)
            ...(lockDuration && { lockDuration }),
            removeOnComplete: {
                age: 24 * 3600, // Manter jobs completos por 24 horas
                count: 1000
            },
            removeOnFail: {
                age: 7 * 24 * 3600 // Manter jobs failed por 7 dias
            }
        }
    );
    
    // Event handlers com logs detalhados
    worker.on('active', (job) => {
        logger.info(`[BullMQ] Job ${job.id} started processing in queue ${queueName}`, {
            jobId: job.id,
            queueName,
            dataKeys: Object.keys(job.data || {}),
            historyId: job.data?.history_id,
            contactsCount: job.data?.contacts?.length || 0,
            batchIndex: job.data?.batch_index,
            totalBatches: job.data?.total_batches
        });
    });
    
    worker.on('completed', (job, result) => {
        const processingTime = job.processedOn && job.finishedOn ? job.finishedOn - job.processedOn : undefined;
        logger.info(`[BullMQ] Job ${job.id} completed in queue ${queueName}`, {
            jobId: job.id,
            queueName,
            historyId: job.data?.history_id,
            batchIndex: job.data?.batch_index,
            contactsCount: job.data?.contacts?.length || 0,
            processingTime: processingTime ? `${processingTime}ms` : undefined,
            result: result
        });
    });
    
    worker.on('failed', (job, err) => {
        logger.error(`[BullMQ] Job ${job?.id} failed in queue ${queueName}:`, {
            jobId: job?.id,
            queueName,
            historyId: job?.data?.history_id,
            batchIndex: job?.data?.batch_index,
            error: err.message,
            stack: err.stack
        });
    });
    
    worker.on('error', (err) => {
        // Tratar erros de lock renewal silenciosamente para jobs delayed que ainda não começaram
        // Esses erros são esperados quando jobs têm delays muito longos
        if (err.message && err.message.includes('could not renew lock')) {
            // Não logar como ERROR - é esperado para jobs delayed com delays longos
            logger.debug(`[BullMQ] Lock renewal error (esperado para jobs delayed): ${queueName}`, {
                queueName,
                error: err.message
            });
            return;
        }
        
        logger.error(`[BullMQ] Worker error in queue ${queueName}:`, {
            queueName,
            error: err.message,
            stack: err.stack
        });
    });
    
    worker.on('stalled', (jobId) => {
        logger.warn(`[BullMQ] Job ${jobId} stalled in queue ${queueName}`, {
            jobId,
            queueName
        });
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
    
    [QUEUE_NAMES.DISPARO_BATCH]: async (data, job) => {
        await processDisparoBatchData(data, job);
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
    
    [QUEUE_NAMES.VALIDATION_DISPARO]: async (data, job) => {
        // Processar validação e disparo (chama processDisparoBatchData)
        await processDisparoBatchData(data, job);
    },
    
    [QUEUE_NAMES.SCHEDULED_DISPARO]: async (data, job) => {
        // Processar disparo agendado (chama processDisparoBatchData)
        await processDisparoBatchData(data, job);
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
    // Inicializar workers das filas principais
    for (const [queueName, processor] of Object.entries(processors)) {
        if (!workers.has(queueName)) {
            const worker = createWorker(queueName, processor);
            workers.set(queueName, worker);
            logger.info(`[BullMQ] Worker initialized for queue: ${queueName}`);
        }
    }
    
    // Inicializar workers de API rate limiter (filas dinâmicas)
    try {
        const { initializeWorkers: initApiWorkers } = require('../worker/api-rate-limiter-worker');
        initApiWorkers().catch(err => {
            logger.warn(`[BullMQ] Erro ao inicializar API rate limiter workers:`, err.message);
        });
    } catch (error) {
        logger.warn(`[BullMQ] Não foi possível carregar API rate limiter workers:`, error.message);
    }
    
    // Telegram rate limiter agora usa Redis diretamente (sem fila BullMQ)
    // Não precisa de worker separado
}

/**
 * Fecha todos os workers
 */
async function closeAllWorkers() {
    // Fechar workers das filas principais
    for (const worker of workers.values()) {
        await worker.close();
    }
    workers.clear();
    
    // Fechar workers de API rate limiter
    try {
        const { closeAll: closeApiWorkers } = require('../worker/api-rate-limiter-worker');
        await closeApiWorkers();
    } catch (error) {
        logger.warn(`[BullMQ] Erro ao fechar API rate limiter workers:`, error.message);
    }
    
    // Telegram rate limiter não tem workers para fechar (usa Redis diretamente)
}

/**
 * Verifica status dos workers
 */
function getWorkersStatus() {
    const status = {};
    for (const [queueName, worker] of workers.entries()) {
        status[queueName] = {
            isRunning: worker.isRunning(),
            name: worker.name
        };
    }
    return status;
}

/**
 * Verifica se um worker específico está rodando
 */
function isWorkerRunning(queueName) {
    const worker = workers.get(queueName);
    return worker ? worker.isRunning() : false;
}

module.exports = {
    initializeWorkers,
    closeAllWorkers,
    createWorker,
    processors,
    getWorkersStatus,
    isWorkerRunning,
    workers, // Exportar para acesso direto se necessário
};
