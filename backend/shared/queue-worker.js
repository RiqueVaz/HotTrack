// Workers BullMQ para processar jobs das filas
const { Worker, Queue, RateLimitError } = require('bullmq');
const { QUEUE_NAMES, QUEUE_CONFIGS, BULLMQ_CONCURRENCY, redisConnection, checkRateLimit, addJobWithDelay } = require('./queue');
const { processTimeoutData } = require('../worker/process-timeout');
const { processDisparoData, processDisparoBatchData } = require('../worker/process-disparo');
const logger = require('../logger');
const { sqlTx, sqlWithRetry } = require('../db');

// Importar métricas Prometheus
let prometheusMetrics = null;
function getPrometheusMetrics() {
    if (!prometheusMetrics) {
        try {
            prometheusMetrics = require('../metrics');
        } catch (e) {
            // Métricas não disponíveis, continuar sem elas
        }
    }
    return prometheusMetrics;
}

/**
 * Cria um worker para uma fila com configurações otimizadas
 */
async function createWorker(queueName, processor, options = {}) {
    // Obter configurações específicas da fila ou usar padrões
    const config = QUEUE_CONFIGS[queueName] || {
        concurrency: BULLMQ_CONCURRENCY,
        limiter: { max: BULLMQ_CONCURRENCY, duration: 1000 },
        stalledInterval: 30000,
        maxStalledCount: 2,
    };
    
    const concurrency = options.concurrency || config.concurrency;
    const limiter = config.limiter;
    const maxStalledCount = config.maxStalledCount || 2;
    const lockDuration = config.lockDuration; // lockDuration específico da fila (opcional)
    
    // Usar stalledInterval fixo da configuração (revertido sistema dinâmico para TIMEOUT)
    // Para outras filas, manter cálculo dinâmico se necessário
    let stalledInterval = config.stalledInterval || 30000; // Usar valor fixo da configuração
    
    // Apenas para filas que não são TIMEOUT, tentar cálculo adaptativo (opcional)
    if (queueName !== QUEUE_NAMES.TIMEOUT && lockDuration) {
        try {
            const queueMetrics = require('./queue-metrics');
            const adaptiveStalled = await queueMetrics.getAdaptiveStalledInterval(lockDuration, queueName, {});
            if (adaptiveStalled && Number.isFinite(adaptiveStalled)) {
                stalledInterval = adaptiveStalled;
                logger.info(`[BullMQ-Worker] StalledInterval adaptativo calculado para ${queueName}: ${Math.round(stalledInterval / 60000)}min`);
            }
        } catch (error) {
            // Em caso de erro, usar valor fixo da configuração
            logger.debug(`[BullMQ-Worker] Usando stalledInterval fixo da configuração para ${queueName}: ${Math.round(stalledInterval / 60000)}min`);
        }
    }
    
    // Criar Queue para obter informações do job quando stalled
    const queue = new Queue(queueName, {
        connection: redisConnection
    });
    
    const worker = new Worker(
        queueName,
        async (job) => {
            const { data } = job;
            const botToken = data._botToken;
            
            // Renovação automática de lock para jobs longos
            // Usar lockDuration calculado dinamicamente se disponível, senão usar padrão da fila
            const jobLockDuration = data._calculatedLockDuration || lockDuration || 300000;
            const lockRenewInterval = Math.min(
                2 * 60 * 1000, // Máximo 2 minutos entre renovações (reduzido de 5min)
                Math.max(60000, Math.floor(jobLockDuration / 4)) // Mínimo 1 minuto, ou 25% do lockDuration
            );
            let lockRenewTimer = null;
            let lastProgress = 0;
            let lastRenewalTime = Date.now();
            
            // Função helper para renovar lock com retry
            const renewLockWithRetry = async (retries = 3) => {
                for (let i = 0; i < retries; i++) {
                    try {
                        const currentProgress = job.progress || lastProgress;
                        await job.updateProgress(Math.max(currentProgress, 1));
                        lastProgress = currentProgress;
                        const renewalDuration = (Date.now() - lastRenewalTime) / 1000;
                        lastRenewalTime = Date.now();
                        
                        // Registrar métrica de renovação bem-sucedida
                        const metrics = getPrometheusMetrics();
                        if (metrics && metrics.metrics && metrics.metrics.queueLockRenewalTotal) {
                            metrics.metrics.queueLockRenewalTotal.inc({ queue_name: queueName, status: 'success' });
                            if (metrics.metrics.queueLockRenewalDuration && renewalDuration > 0) {
                                metrics.metrics.queueLockRenewalDuration.observe({ queue_name: queueName }, renewalDuration);
                            }
                        }
                        
                        logger.debug(`[BullMQ-Worker] Lock renovado automaticamente para job ${job.id} na fila ${queueName} (lockDuration: ${Math.round(jobLockDuration / 60000)}min)`);
                        return true;
                    } catch (renewError) {
                        // Se job foi concluído ou removido, não tentar novamente
                        if (renewError.message?.includes('not found') || 
                            renewError.message?.includes('completed') ||
                            renewError.message?.includes('removed')) {
                            if (lockRenewTimer) {
                                clearInterval(lockRenewTimer);
                                lockRenewTimer = null;
                            }
                            return false;
                        }
                        
                        // Registrar métrica de falha de renovação apenas na última tentativa
                        if (i === retries - 1) {
                            const metrics = getPrometheusMetrics();
                            if (metrics && metrics.metrics && metrics.metrics.queueLockRenewalTotal) {
                                metrics.metrics.queueLockRenewalTotal.inc({ queue_name: queueName, status: 'failed' });
                            }
                            logger.debug(`[BullMQ-Worker] Erro ao renovar lock automaticamente após ${retries} tentativas (não crítico):`, renewError.message);
                        }
                        
                        // Se não é último retry, aguardar antes de tentar novamente (backoff exponencial)
                        if (i < retries - 1) {
                            await new Promise(resolve => setTimeout(resolve, Math.pow(2, i) * 1000));
                        }
                    }
                }
                return false;
            };
            
            // Iniciar renovação automática de lock para jobs que podem demorar muito (> 5 minutos)
            if (job && typeof job.updateProgress === 'function' && jobLockDuration > 300000) {
                lockRenewTimer = setInterval(async () => {
                    await renewLockWithRetry();
                }, lockRenewInterval);
            }
            
            // Função para renovação proativa antes de operações longas (exportada para uso no processor)
            const proactiveRenewLock = async () => {
                const now = Date.now();
                // Renovar se passou mais de 30 segundos desde última renovação
                if (now - lastRenewalTime > 30000) {
                    await renewLockWithRetry();
                }
            };
            
            try {
                // Log detalhado do início do processamento
                if (queueName === QUEUE_NAMES.DISPARO_BATCH) {
                    logger.info(`[BullMQ-Worker] Processando job ${job.id} da fila ${queueName}`, {
                        jobId: job.id,
                        historyId: data.history_id,
                        batchIndex: data.batch_index,
                        totalBatches: data.total_batches,
                        contactsCount: data.contacts?.length || 0,
                        hasBotToken: !!botToken,
                        botTokenLength: botToken?.length || 0,
                        lockRenewInterval: lockRenewTimer ? `${Math.round(lockRenewInterval / 1000)}s` : 'disabled'
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
                    // Passar função de renovação proativa para o processor
                    const result = await processor(data, job, proactiveRenewLock);
                    // Limpar timer de renovação de lock ao concluir
                    if (lockRenewTimer) {
                        clearInterval(lockRenewTimer);
                    }
                    return result;
                } catch (error) {
                    // Limpar timer de renovação de lock em caso de erro
                    if (lockRenewTimer) {
                        clearInterval(lockRenewTimer);
                    }
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
            } catch (error) {
                // Limpar timer de renovação de lock em caso de erro não tratado
                if (lockRenewTimer) {
                    clearInterval(lockRenewTimer);
                }
                throw error;
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
    
    worker.on('completed', async (job, result) => {
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
        
        // Registrar métricas Prometheus
        const metrics = getPrometheusMetrics();
        if (metrics && metrics.metrics) {
            if (metrics.metrics.workerJobsTotal) {
                metrics.metrics.workerJobsTotal.inc({ worker: queueName, job_type: job.name || 'unknown', status: 'completed' });
            }
            if (processingTime && processingTime > 0 && metrics.metrics.workerJobDuration) {
                metrics.metrics.workerJobDuration.observe({ worker: queueName, job_type: job.name || 'unknown', status: 'completed' }, processingTime / 1000);
            }
            
            // Registrar razão de utilização do lockDuration
            if (processingTime && processingTime > 0 && job.data?._calculatedLockDuration) {
                const utilizationRatio = processingTime / job.data._calculatedLockDuration;
                if (metrics.metrics.queueJobUtilizationRatio) {
                    metrics.metrics.queueJobUtilizationRatio.observe({ queue_name: queueName }, utilizationRatio);
                }
                
                // Logar warning se utilização >80%
                if (utilizationRatio > 0.8) {
                    logger.warn(`[BullMQ] Job ${job.id} utilizou ${Math.round(utilizationRatio * 100)}% do lockDuration`, {
                        jobId: job.id,
                        queueName,
                        utilizationRatio: `${Math.round(utilizationRatio * 100)}%`,
                        processingTime: `${Math.round(processingTime / 60000)}min`,
                        lockDuration: `${Math.round(job.data._calculatedLockDuration / 60000)}min`
                    });
                }
            }
        }
        
        // Atualizar métricas históricas para cálculo adaptativo futuro
        if (processingTime && processingTime > 0) {
            try {
                const queueMetrics = require('./queue-metrics');
                const characteristics = queueMetrics.extractJobCharacteristics(queueName, job.data || {});
                await queueMetrics.updateMetrics(queueName, processingTime, characteristics);
            } catch (error) {
                // Não crítico se falhar - apenas logar
                logger.debug(`[BullMQ-Worker] Erro ao atualizar métricas históricas (não crítico):`, error.message);
            }
        }
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
        // Buscar informações do job para log mais detalhado
        // Usar lockDuration calculado dinamicamente se disponível, senão usar padrão da fila
        queue.getJob(jobId).then(async (job) => {
            if (job) {
                const jobLockDuration = job.data?._calculatedLockDuration || lockDuration || 300000;
                const jobStalledInterval = job.data?._calculatedStalledInterval || stalledInterval || 300000;
                const processingTime = job.processedOn ? Date.now() - job.processedOn : null;
                const lockDurationMinutes = Math.round(jobLockDuration / 60000);
                const processingTimeMinutes = processingTime ? Math.round(processingTime / 60000) : null;
                
                // Logar warning se job demorou >80% do lockDuration calculado
                if (processingTime && jobLockDuration) {
                    const utilizationRatio = processingTime / jobLockDuration;
                    if (utilizationRatio > 0.8) {
                        logger.warn(`[BullMQ] Job ${jobId} quase stalled (${Math.round(utilizationRatio * 100)}% do lockDuration usado)`, {
                            jobId,
                            queueName,
                            utilizationRatio: `${Math.round(utilizationRatio * 100)}%`,
                            processingTime: `${processingTimeMinutes}min`,
                            lockDuration: `${lockDurationMinutes}min`
                        });
                    }
                }
                
                // Registrar métricas Prometheus
                const metrics = getPrometheusMetrics();
                if (metrics && metrics.metrics) {
                    if (metrics.metrics.queueJobStalledTotal) {
                        metrics.metrics.queueJobStalledTotal.inc({ queue_name: queueName });
                    }
                    if (processingTime && metrics.metrics.queueJobStalledDuration) {
                        metrics.metrics.queueJobStalledDuration.observe({ queue_name: queueName }, processingTime / 1000);
                    }
                }
                
                logger.warn(`[BullMQ] Job ${jobId} stalled in queue ${queueName}`, {
                    jobId,
                    queueName,
                    historyId: job.data?.history_id,
                    batchIndex: job.data?.batch_index,
                    totalBatches: job.data?.total_batches,
                    contactsCount: job.data?.contacts?.length || 0,
                    processingTime: processingTime ? `${processingTimeMinutes}min (${Math.round(processingTime / 1000)}s)` : 'unknown',
                    lockDuration: `${lockDurationMinutes}min`,
                    stalledInterval: `${Math.round(jobStalledInterval / 60000)}min`,
                    attemptsMade: job.attemptsMade || 0,
                    progress: job.progress || 0,
                    hasDynamicLock: !!job.data?._calculatedLockDuration,
                    timestamp: new Date().toISOString()
                });
            } else {
                logger.warn(`[BullMQ] Job ${jobId} stalled in queue ${queueName} (job not found)`, {
                    jobId,
                    queueName
                });
            }
        }).catch(err => {
            logger.warn(`[BullMQ] Job ${jobId} stalled in queue ${queueName} (error getting job details)`, {
                jobId,
                queueName,
                error: err.message
            });
        });
    });
    
    // Retornar worker e queue para poder fechar ambos quando necessário
    return { worker, queue };
}

// Processadores para cada tipo de job
const processors = {
    [QUEUE_NAMES.TIMEOUT]: async (data, job, proactiveRenewLock = null) => {
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        const result = await processTimeoutData(data, job);
        return result;
    },
    
    [QUEUE_NAMES.DISPARO]: async (data, job, proactiveRenewLock = null) => {
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        await processDisparoData(data, job);
    },
    
    [QUEUE_NAMES.DISPARO_BATCH]: async (data, job, proactiveRenewLock = null) => {
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        await processDisparoBatchData(data, job);
    },
    
    [QUEUE_NAMES.DISPARO_DELAY]: async (data, job, proactiveRenewLock = null) => {
        // Processar disparo após delay
        // Buscar dados do fluxo do banco, pois o payload não inclui flow_nodes, flow_edges e start_node_id
        const { history_id, chat_id, bot_id, current_node_id, variables, remaining_actions } = data;
        
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        
        // Renovação periódica de lock para delays longos
        let lastLockRenewal = Date.now();
        const LOCK_RENEWAL_INTERVAL_MS = 1 * 60 * 1000; // 1 minuto (reduzido de 5min)
        
        // Função para renovar lock durante processamento
        const renewLockIfNeeded = async () => {
            const now = Date.now();
            if ((now - lastLockRenewal) >= LOCK_RENEWAL_INTERVAL_MS && job && typeof job.updateProgress === 'function') {
                try {
                    await job.updateProgress(1);
                    lastLockRenewal = now;
                    logger.debug(`[WORKER-DISPARO-DELAY] Lock renovado para job ${job.id}`);
                } catch (renewError) {
                    if (renewError.message?.includes('not found') || 
                        renewError.message?.includes('completed') ||
                        renewError.message?.includes('removed')) {
                        return;
                    }
                    logger.debug(`[WORKER-DISPARO-DELAY] Erro ao renovar lock (não crítico):`, renewError.message);
                }
            }
        };
        
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
            
            // Renovar lock antes de processar
            await renewLockIfNeeded();
            
            // Processar disparo com dados completos
            // Passar função de renovação para processDisparoData se necessário
            await processDisparoData(completeData, job);
            
            // Renovar lock após processar
            await renewLockIfNeeded();
        } catch (error) {
            logger.error(`[BullMQ] Erro ao processar delay de disparo para history_id ${history_id}:`, error);
            throw error; // Re-throw para que o BullMQ trate como falha e faça retry
        }
    },
    
    [QUEUE_NAMES.VALIDATION_DISPARO]: async (data, job, proactiveRenewLock = null) => {
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        // Processar validação e disparo (chama processDisparoBatchData)
        await processDisparoBatchData(data, job);
    },
    
    [QUEUE_NAMES.SCHEDULED_DISPARO]: async (data, job, proactiveRenewLock = null) => {
        // Processar disparo agendado
        // Primeiro, buscar dados do history e preparar batches
        const { history_id } = data;
        
        if (!history_id) {
            throw new Error('history_id é obrigatório para scheduled-disparo');
        }
        
        logger.info(`[SCHEDULED-DISPARO] Processando disparo agendado ${history_id}`);
        
        // Buscar o histórico do disparo
        const [history] = await sqlWithRetry(
            sqlTx`SELECT * FROM disparo_history WHERE id = ${history_id}`
        );
        
        if (!history) {
            logger.error(`[SCHEDULED-DISPARO] Histórico de disparo ${history_id} não encontrado.`);
            throw new Error(`Histórico de disparo ${history_id} não encontrado.`);
        }
        
        // Validar que está com status SCHEDULED
        if (history.status !== 'SCHEDULED') {
            logger.warn(`[SCHEDULED-DISPARO] Disparo ${history_id} não está agendado (status: ${history.status}). Ignorando.`);
            return;
        }
        
        // Verificar se disparo foi cancelado antes de continuar
        const [disparoCheck] = await sqlWithRetry(
            sqlTx`SELECT status FROM disparo_history WHERE id = ${history_id}`
        );
        
        if (!disparoCheck || disparoCheck.status === 'CANCELLED') {
            logger.info(`[SCHEDULED-DISPARO ${history_id}] Disparo foi cancelado. Abortando processamento.`);
            return;
        }
        
        // Buscar o fluxo de disparo
        const [disparoFlow] = await sqlWithRetry(
            sqlTx`SELECT * FROM disparo_flows WHERE id = ${history.disparo_flow_id}`
        );
        
        if (!disparoFlow) {
            await sqlWithRetry(
                sqlTx`UPDATE disparo_history SET status = 'FAILED' WHERE id = ${history_id}`
            );
            logger.error(`[SCHEDULED-DISPARO] Fluxo de disparo ${history.disparo_flow_id} não encontrado.`);
            throw new Error(`Fluxo de disparo ${history.disparo_flow_id} não encontrado.`);
        }
        
        // Parse do fluxo
        const flowData = typeof disparoFlow.nodes === 'string' ? JSON.parse(disparoFlow.nodes) : disparoFlow.nodes;
        const flowNodes = flowData.nodes || [];
        const flowEdges = flowData.edges || [];
        
        // Buscar contatos dos bots
        const botIds = Array.isArray(history.bot_ids) ? history.bot_ids : JSON.parse(history.bot_ids || '[]');
        
        // Extrair tagIds e tagFilterMode do flow_steps se existir
        let tagIds = null;
        let tagFilterMode = 'include'; // Default
        if (history.flow_steps && typeof history.flow_steps === 'object') {
            const flowSteps = typeof history.flow_steps === 'string' ? JSON.parse(history.flow_steps) : history.flow_steps;
            if (flowSteps.tagIds && Array.isArray(flowSteps.tagIds)) {
                tagIds = flowSteps.tagIds;
            }
            if (flowSteps.tagFilterMode) {
                tagFilterMode = flowSteps.tagFilterMode;
            }
        }
        
        // Encontrar o trigger (nó inicial do disparo) uma vez antes do streaming
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
            await sqlWithRetry(
                sqlTx`UPDATE disparo_history SET status = 'FAILED' WHERE id = ${history_id}`
            );
            logger.error(`[SCHEDULED-DISPARO] Nenhum nó inicial encontrado no fluxo.`);
            throw new Error('Nenhum nó inicial encontrado no fluxo.');
        }
        
        // Buscar bot_tokens uma vez antes do streaming
        const botTokens = await sqlWithRetry(
            sqlTx`SELECT id, bot_token FROM telegram_bots WHERE id = ANY(${botIds}) AND seller_id = ${history.seller_id}`
        );
        const botTokenMap = new Map();
        botTokens.forEach(bot => {
            botTokenMap.set(bot.id, bot.bot_token);
        });
        
        // Usar streaming para processar contatos em páginas e criar batches conforme chegam
        // Isso evita carregar todos os contatos em memória de uma vez
        const serverModule = require('../server');
        const processContactsInStream = serverModule.processContactsInStream;
        
        if (!processContactsInStream) {
            throw new Error('processContactsInStream não está disponível. Verifique se está exportado corretamente.');
        }
        
        const DISPARO_BATCH_SIZE = parseInt(process.env.DISPARO_BATCH_SIZE) || 200;
        const contactsForBatch = [];
        const seenChatIds = new Set(); // Para deduplicação
        let totalContactsProcessed = 0;
        let batchIndex = 0;
        const batchPromises = [];
        
        logger.info(`[SCHEDULED-DISPARO ${history_id}] Iniciando busca de contatos com streaming...`);
        
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
        
        // Renovação periódica de lock durante streaming
        let lastLockRenewal = Date.now();
        const LOCK_RENEWAL_INTERVAL_MS = 1 * 60 * 1000; // 1 minuto
        
        const renewLockIfNeeded = async () => {
            const now = Date.now();
            if ((now - lastLockRenewal) >= LOCK_RENEWAL_INTERVAL_MS && job && typeof job.updateProgress === 'function') {
                try {
                    await job.updateProgress(1);
                    lastLockRenewal = now;
                    logger.debug(`[SCHEDULED-DISPARO] Lock renovado para job ${job.id}`);
                } catch (renewError) {
                    if (renewError.message?.includes('not found') || 
                        renewError.message?.includes('completed') ||
                        renewError.message?.includes('removed')) {
                        return;
                    }
                    logger.debug(`[SCHEDULED-DISPARO] Erro ao renovar lock (não crítico):`, renewError.message);
                }
            }
        };
        
        // Função para criar batch quando atingir tamanho limite
        const createBatchIfReady = async () => {
            if (contactsForBatch.length >= DISPARO_BATCH_SIZE) {
            // Renovar lock periodicamente durante criação de batches
            if (batchIndex > 0 && batchIndex % 10 === 0) {
                await renewLockIfNeeded();
            }
                
                const batchToProcess = contactsForBatch.slice(0, DISPARO_BATCH_SIZE);
                
                // Preparar contatos para batch processing
                const preparedBatch = batchToProcess.map(contact => {
                    const userVariables = {
                        primeiro_nome: contact.first_name || '',
                        nome_completo: `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
                        click_id: contact.click_id ? contact.click_id.replace('/start ', '') : null
                    };
                    
                    return {
                        chat_id: contact.chat_id,
                        bot_id: contact.bot_id_source,
                        variables_json: JSON.stringify(userVariables)
                    };
                });
                
            const SMALL_BATCH_DELAY_SECONDS = 0.01; // 10ms por batch
            const batchDelaySeconds = batchIndex * SMALL_BATCH_DELAY_SECONDS;
            
                const firstContactBotId = preparedBatch[0]?.bot_id;
            const botToken = botTokenMap.get(firstContactBotId) || '';
            
            const batchPayload = {
                history_id: history_id,
                    contacts: preparedBatch,
                flow_nodes: JSON.stringify(flowNodes),
                flow_edges: JSON.stringify(flowEdges),
                start_node_id: startNodeId,
                batch_index: batchIndex,
                    total_batches: null // Será calculado depois
            };
            
            batchPromises.push(
                addJobWithDelay(
                    QUEUE_NAMES.DISPARO_BATCH,
                    'process-disparo-batch',
                    batchPayload,
                    {
                        delay: `${batchDelaySeconds}s`,
                        botToken: botToken
                    }
                )
            );
                
                // Remover batch processado do array
                contactsForBatch.splice(0, DISPARO_BATCH_SIZE);
                batchIndex++;
            }
        };
        
        // Processar contatos usando streaming
        const streamStats = await processContactsInStream(
            botIds,
            history.seller_id,
            tagIds || null,
            tagFilterMode,
            null, // Não excluir chats ainda
            async (pageContacts, pageNumber, totalProcessed) => {
                // Adicionar apenas contatos únicos desta página
                for (const c of pageContacts) {
                    if (!seenChatIds.has(c.chat_id)) {
                        seenChatIds.add(c.chat_id);
                        contactsForBatch.push({
                            chat_id: c.chat_id,
                            first_name: c.first_name,
                            last_name: c.last_name,
                            username: c.username,
                            click_id: c.click_id,
                            bot_id_source: c.bot_id
                        });
                    }
                }
                
                totalContactsProcessed = totalProcessed;
                
                // Criar batches quando atingir tamanho limite
                await createBatchIfReady();
            }
        );
        
        // Processar batch final se houver contatos restantes
        if (contactsForBatch.length > 0) {
            const preparedBatch = contactsForBatch.map(contact => {
                const userVariables = {
                    primeiro_nome: contact.first_name || '',
                    nome_completo: `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
                    click_id: contact.click_id ? contact.click_id.replace('/start ', '') : null
                };
                
                return {
                    chat_id: contact.chat_id,
                    bot_id: contact.bot_id_source,
                    variables_json: JSON.stringify(userVariables)
                };
            });
            
            const SMALL_BATCH_DELAY_SECONDS = 0.01;
            const batchDelaySeconds = batchIndex * SMALL_BATCH_DELAY_SECONDS;
            
            const firstContactBotId = preparedBatch[0]?.bot_id;
            const botToken = botTokenMap.get(firstContactBotId) || '';
            
            const batchPayload = {
                history_id: history_id,
                contacts: preparedBatch,
                flow_nodes: JSON.stringify(flowNodes),
                flow_edges: JSON.stringify(flowEdges),
                start_node_id: startNodeId,
                batch_index: batchIndex,
                total_batches: batchIndex + 1
            };
            
            batchPromises.push(
                addJobWithDelay(
                    QUEUE_NAMES.DISPARO_BATCH,
                    'process-disparo-batch',
                    batchPayload,
                    {
                        delay: `${batchDelaySeconds}s`,
                        botToken: botToken
                    }
                )
            );
        }
        
        const totalBatches = batchPromises.length;
        
        if (totalBatches === 0) {
            logger.info(`[SCHEDULED-DISPARO ${history_id}] Nenhum contato encontrado.`);
            await sqlWithRetry(
                sqlTx`UPDATE disparo_history 
                      SET status = 'COMPLETED', current_step = NULL, total_sent = 0, total_jobs = 0
                      WHERE id = ${history_id}`
            );
            return;
        }
        
        // Publicar todos os batches
        logger.info(`[SCHEDULED-DISPARO ${history_id}] Publicando ${totalBatches} batches no BullMQ...`);
        await Promise.all(batchPromises);
        
        // Atualizar status para RUNNING e current_step para 'sending'
        await sqlWithRetry(
            sqlTx`UPDATE disparo_history 
                  SET status = 'RUNNING', current_step = 'sending', 
                      total_sent = ${totalContactsProcessed}, total_jobs = ${totalContactsProcessed}
                  WHERE id = ${history_id}`
        );
        
        logger.info(`[SCHEDULED-DISPARO ${history_id}] Disparo processado com streaming. ${totalContactsProcessed} contatos em ${streamStats.totalPages} páginas, ${totalBatches} batches criados.`);
        logger.info(`[SCHEDULED-DISPARO] Disparo agendado ${history_id} processado com sucesso.`);
    },
    
    [QUEUE_NAMES.CLEANUP_QRCODES]: async (data, job = null, proactiveRenewLock = null) => {
        // Renovar lock antes de operações longas se função disponível
        if (proactiveRenewLock && typeof proactiveRenewLock === 'function') {
            await proactiveRenewLock();
        }
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
async function initializeWorkers() {
    // Inicializar workers das filas principais
    for (const [queueName, processor] of Object.entries(processors)) {
        if (!workers.has(queueName)) {
            const { worker, queue } = await createWorker(queueName, processor);
            workers.set(queueName, { worker, queue });
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
    for (const { worker, queue } of workers.values()) {
        await worker.close();
        await queue.close();
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
    for (const [queueName, workerData] of workers.entries()) {
        status[queueName] = {
            isRunning: workerData.worker.isRunning(),
            name: workerData.worker.name
        };
    }
    return status;
}

/**
 * Verifica se um worker específico está rodando
 */
function isWorkerRunning(queueName) {
    const workerData = workers.get(queueName);
    return workerData ? workerData.worker.isRunning() : false;
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
