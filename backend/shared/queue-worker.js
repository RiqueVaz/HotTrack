// Workers BullMQ para processar jobs das filas
const { Worker, RateLimitError } = require('bullmq');
const { QUEUE_NAMES, QUEUE_CONFIGS, BULLMQ_CONCURRENCY, redisConnection, checkRateLimit, addJobWithDelay } = require('./queue');
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
            
            // Renovação automática de lock para jobs longos
            // Configurar intervalo baseado no lockDuration da fila (renovar a cada 1/4 do lockDuration)
            const queueLockDuration = lockDuration || 300000; // Default 5 minutos se não especificado
            const lockRenewInterval = Math.max(60000, Math.floor(queueLockDuration / 4)); // Mínimo 1 minuto, máximo 1/4 do lockDuration
            let lockRenewTimer = null;
            
            // Iniciar renovação automática de lock para jobs que podem demorar muito (> 5 minutos)
            if (job && typeof job.updateProgress === 'function' && queueLockDuration > 300000) {
                lockRenewTimer = setInterval(async () => {
                    try {
                        // Atualizar progresso para renovar o lock implicitamente
                        await job.updateProgress(1);
                        logger.debug(`[BullMQ-Worker] Lock renovado automaticamente para job ${job.id} na fila ${queueName}`);
                    } catch (renewError) {
                        // Não é crítico se falhar, apenas logar
                        logger.debug(`[BullMQ-Worker] Erro ao renovar lock automaticamente (não crítico):`, renewError.message);
                    }
                }, lockRenewInterval);
            }
            
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
                    const result = await processor(data, job);
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
        // Buscar informações do job para log mais detalhado
        const queueLockDuration = lockDuration || 300000; // Usar lockDuration do escopo da função
        worker.getJob(jobId).then(job => {
            if (job) {
                const processingTime = job.processedOn ? Date.now() - job.processedOn : null;
                const lockDurationMinutes = Math.round(queueLockDuration / 60000);
                const processingTimeMinutes = processingTime ? Math.round(processingTime / 60000) : null;
                
                logger.warn(`[BullMQ] Job ${jobId} stalled in queue ${queueName}`, {
                    jobId,
                    queueName,
                    historyId: job.data?.history_id,
                    batchIndex: job.data?.batch_index,
                    totalBatches: job.data?.total_batches,
                    contactsCount: job.data?.contacts?.length || 0,
                    processingTime: processingTime ? `${processingTimeMinutes}min (${Math.round(processingTime / 1000)}s)` : 'unknown',
                    lockDuration: `${lockDurationMinutes}min`,
                    stalledInterval: `${Math.round(stalledInterval / 60000)}min`,
                    attemptsMade: job.attemptsMade || 0,
                    progress: job.progress || 0,
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
        
        // Buscar contatos usando getContactsByTags
        // Como a função está em server.js, vamos buscar contatos diretamente (simplificado)
        const MAX_CONTACTS_PER_QUERY = 100000; // Limite alto para não perder contatos
        
        let filteredContacts = [];
        
        // Se não há filtros de tags, buscar todos os contatos
        if (!tagIds || !Array.isArray(tagIds) || tagIds.length === 0) {
            filteredContacts = await sqlWithRetry(
                sqlTx`SELECT DISTINCT ON (tc.chat_id) tc.chat_id, tc.bot_id, tc.first_name, tc.last_name, tc.username, tc.click_id
                    FROM telegram_chats tc
                    LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                    WHERE tc.bot_id = ANY(${botIds}) 
                        AND tc.seller_id = ${history.seller_id}
                        AND tc.chat_id > 0
                        AND bb.chat_id IS NULL
                    ORDER BY tc.chat_id, tc.created_at DESC
                    LIMIT ${MAX_CONTACTS_PER_QUERY}`
            );
        } else {
            // Separar tags custom de automáticas
            let customTagIds = [];
            let automaticTagNames = [];
            tagIds.forEach(tagId => {
                if (typeof tagId === 'number' || (typeof tagId === 'string' && /^\d+$/.test(tagId))) {
                    customTagIds.push(parseInt(tagId));
                } else if (typeof tagId === 'string') {
                    automaticTagNames.push(tagId);
                }
            });
            
            // Validar tags custom
            let validCustomTagIds = [];
            if (customTagIds.length > 0) {
                const validTags = await sqlWithRetry(
                    sqlTx`SELECT id FROM lead_custom_tags 
                          WHERE id = ANY(${customTagIds}) 
                            AND seller_id = ${history.seller_id} 
                            AND bot_id = ANY(${botIds})`
                );
                
                if (validTags && validTags.length > 0) {
                    validCustomTagIds = Array.isArray(validTags) ? validTags.map(t => t.id) : [validTags.id];
                }
            }
            
            // Validar tags automáticas
            const validAutomaticTags = automaticTagNames.filter(name => name === 'Pagante');
            const hasAnyTagFilter = validCustomTagIds.length > 0 || validAutomaticTags.length > 0;
            
            if (!hasAnyTagFilter) {
                // Se não há tags válidas, retornar todos os contatos
                filteredContacts = await sqlWithRetry(
                    sqlTx`SELECT DISTINCT ON (tc.chat_id) tc.chat_id, tc.bot_id, tc.first_name, tc.last_name, tc.username, tc.click_id
                        FROM telegram_chats tc
                        LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                        WHERE tc.bot_id = ANY(${botIds}) 
                            AND tc.seller_id = ${history.seller_id}
                            AND tc.chat_id > 0
                            AND bb.chat_id IS NULL
                        ORDER BY tc.chat_id, tc.created_at DESC
                        LIMIT ${MAX_CONTACTS_PER_QUERY}`
                );
            } else {
                // Aplicar filtros de tags (versão simplificada - para versão completa, criar módulo compartilhado)
                const filterMode = tagFilterMode === 'exclude' ? 'exclude' : 'include';
                const hasPaidTag = validAutomaticTags.includes('Pagante');
                
                // Query com filtros de tags
                if (filterMode === 'include') {
                    // INCLUDE: contatos devem ter TODAS as tags especificadas
                    if (validCustomTagIds.length > 0 && hasPaidTag) {
                        // Contatos com tags custom E pagantes
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            custom_tagged AS (
                                SELECT DISTINCT lcta.chat_id
                                FROM lead_custom_tag_assignments lcta
                                WHERE lcta.bot_id = ANY(${botIds})
                                    AND lcta.seller_id = ${history.seller_id}
                                    AND lcta.tag_id = ANY(${validCustomTagIds})
                                GROUP BY lcta.chat_id
                                HAVING COUNT(DISTINCT lcta.tag_id) = ${validCustomTagIds.length}
                            ),
                            paid_contacts AS (
                                SELECT DISTINCT tc.chat_id
                                FROM telegram_chats tc
                                INNER JOIN clicks c ON c.click_id = tc.click_id AND c.seller_id = tc.seller_id
                                INNER JOIN pix_transactions pt ON pt.click_id_internal = c.id AND pt.status = 'paid'
                                WHERE tc.bot_id = ANY(${botIds})
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id IN (SELECT chat_id FROM base_contacts)
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            INNER JOIN custom_tagged ct ON ct.chat_id = bc.chat_id
                            INNER JOIN paid_contacts pc ON pc.chat_id = bc.chat_id
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    } else if (validCustomTagIds.length > 0) {
                        // Apenas tags custom
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            custom_tagged AS (
                                SELECT DISTINCT lcta.chat_id
                                FROM lead_custom_tag_assignments lcta
                                WHERE lcta.bot_id = ANY(${botIds})
                                    AND lcta.seller_id = ${history.seller_id}
                                    AND lcta.tag_id = ANY(${validCustomTagIds})
                                GROUP BY lcta.chat_id
                                HAVING COUNT(DISTINCT lcta.tag_id) = ${validCustomTagIds.length}
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            INNER JOIN custom_tagged ct ON ct.chat_id = bc.chat_id
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    } else if (hasPaidTag) {
                        // Apenas pagantes
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            paid_contacts AS (
                                SELECT DISTINCT tc.chat_id
                                FROM telegram_chats tc
                                INNER JOIN clicks c ON c.click_id = tc.click_id AND c.seller_id = tc.seller_id
                                INNER JOIN pix_transactions pt ON pt.click_id_internal = c.id AND pt.status = 'paid'
                                WHERE tc.bot_id = ANY(${botIds})
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id IN (SELECT chat_id FROM base_contacts)
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            INNER JOIN paid_contacts pc ON pc.chat_id = bc.chat_id
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    }
                } else {
                    // EXCLUDE: contatos NÃO devem ter nenhuma das tags especificadas
                    if (validCustomTagIds.length > 0 && hasPaidTag) {
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            custom_tagged AS (
                                SELECT DISTINCT lcta.chat_id
                                FROM lead_custom_tag_assignments lcta
                                WHERE lcta.bot_id = ANY(${botIds})
                                    AND lcta.seller_id = ${history.seller_id}
                                    AND lcta.tag_id = ANY(${validCustomTagIds})
                            ),
                            paid_contacts AS (
                                SELECT DISTINCT tc.chat_id
                                FROM telegram_chats tc
                                INNER JOIN clicks c ON c.click_id = tc.click_id AND c.seller_id = tc.seller_id
                                INNER JOIN pix_transactions pt ON pt.click_id_internal = c.id AND pt.status = 'paid'
                                WHERE tc.bot_id = ANY(${botIds})
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id IN (SELECT chat_id FROM base_contacts)
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            LEFT JOIN custom_tagged ct ON ct.chat_id = bc.chat_id
                            LEFT JOIN paid_contacts pc ON pc.chat_id = bc.chat_id
                            WHERE ct.chat_id IS NULL AND pc.chat_id IS NULL
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    } else if (validCustomTagIds.length > 0) {
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            custom_tagged AS (
                                SELECT DISTINCT lcta.chat_id
                                FROM lead_custom_tag_assignments lcta
                                WHERE lcta.bot_id = ANY(${botIds})
                                    AND lcta.seller_id = ${history.seller_id}
                                    AND lcta.tag_id = ANY(${validCustomTagIds})
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            LEFT JOIN custom_tagged ct ON ct.chat_id = bc.chat_id
                            WHERE ct.chat_id IS NULL
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    } else if (hasPaidTag) {
                        filteredContacts = await sqlWithRetry(sqlTx`
                            WITH base_contacts AS (
                                SELECT DISTINCT ON (tc.chat_id) 
                                    tc.chat_id, tc.first_name, tc.last_name, tc.username, tc.click_id, tc.bot_id
                                FROM telegram_chats tc
                                LEFT JOIN bot_blocks bb ON bb.bot_id = tc.bot_id AND bb.chat_id = tc.chat_id
                                WHERE tc.bot_id = ANY(${botIds}) 
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id > 0
                                    AND bb.chat_id IS NULL
                                ORDER BY tc.chat_id, tc.created_at DESC
                                LIMIT ${MAX_CONTACTS_PER_QUERY}
                            ),
                            paid_contacts AS (
                                SELECT DISTINCT tc.chat_id
                                FROM telegram_chats tc
                                INNER JOIN clicks c ON c.click_id = tc.click_id AND c.seller_id = tc.seller_id
                                INNER JOIN pix_transactions pt ON pt.click_id_internal = c.id AND pt.status = 'paid'
                                WHERE tc.bot_id = ANY(${botIds})
                                    AND tc.seller_id = ${history.seller_id}
                                    AND tc.chat_id IN (SELECT chat_id FROM base_contacts)
                            )
                            SELECT bc.chat_id, bc.first_name, bc.last_name, bc.username, bc.click_id, bc.bot_id
                            FROM base_contacts bc
                            LEFT JOIN paid_contacts pc ON pc.chat_id = bc.chat_id
                            WHERE pc.chat_id IS NULL
                            ORDER BY bc.chat_id
                            LIMIT ${MAX_CONTACTS_PER_QUERY}
                        `);
                    }
                }
            }
        }
        
        logger.info(`[SCHEDULED-DISPARO ${history_id}] Filtros aplicados: ${filteredContacts.length} contatos encontrados.`);
        
        // Preparar contatos únicos
        const allContacts = new Map();
        filteredContacts.forEach(c => {
            if (!allContacts.has(c.chat_id)) {
                allContacts.set(c.chat_id, { 
                    chat_id: c.chat_id,
                    first_name: c.first_name,
                    last_name: c.last_name,
                    username: c.username,
                    click_id: c.click_id,
                    bot_id_source: c.bot_id 
                });
            }
        });
        const uniqueContacts = Array.from(allContacts.values());
        
        // Encontrar o trigger (nó inicial do disparo)
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
        
        if (uniqueContacts.length === 0) {
            logger.info(`[SCHEDULED-DISPARO ${history_id}] Nenhum contato encontrado.`);
            await sqlWithRetry(
                sqlTx`UPDATE disparo_history 
                      SET status = 'COMPLETED', current_step = NULL, total_sent = 0, total_jobs = 0
                      WHERE id = ${history_id}`
            );
            return;
        }
        
        // Atualizar status para RUNNING e current_step para 'sending'
        await sqlWithRetry(
            sqlTx`UPDATE disparo_history 
                  SET status = 'RUNNING', current_step = 'sending', 
                      total_sent = ${uniqueContacts.length}, total_jobs = ${uniqueContacts.length}
                  WHERE id = ${history_id}`
        );
        
        // Buscar bot_tokens antes do loop para usar como chave de rate limiting
        const botTokens = await sqlWithRetry(
            sqlTx`SELECT id, bot_token FROM telegram_bots WHERE id = ANY(${botIds}) AND seller_id = ${history.seller_id}`
        );
        const botTokenMap = new Map();
        botTokens.forEach(bot => {
            botTokenMap.set(bot.id, bot.bot_token);
        });
        
        // Preparar contatos para batch processing
        const contactsForBatch = uniqueContacts.map(contact => {
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
        
        // Agrupar contatos em batches
        const DISPARO_BATCH_SIZE = parseInt(process.env.DISPARO_BATCH_SIZE) || 200;
        const batchSize = DISPARO_BATCH_SIZE;
        const totalBatches = Math.ceil(contactsForBatch.length / batchSize);
        const batchPromises = [];
        
        logger.info(`[SCHEDULED-DISPARO ${history_id}] Processando ${contactsForBatch.length} contatos em ${totalBatches} batches de ${batchSize}`);
        
        for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            const batchStart = batchIndex * batchSize;
            const batchEnd = Math.min(batchStart + batchSize, contactsForBatch.length);
            const batchContacts = contactsForBatch.slice(batchStart, batchEnd);
            
            // Delay muito pequeno entre batches (0.01s por batch para evitar sobrecarga inicial)
            const SMALL_BATCH_DELAY_SECONDS = 0.01; // 10ms por batch
            const batchDelaySeconds = batchIndex * SMALL_BATCH_DELAY_SECONDS;
            
            // Obter bot_token do primeiro contato do batch para rate limiting
            const firstContactBotId = batchContacts[0]?.bot_id;
            const botToken = botTokenMap.get(firstContactBotId) || '';
            
            const batchPayload = {
                history_id: history_id,
                contacts: batchContacts,
                flow_nodes: JSON.stringify(flowNodes),
                flow_edges: JSON.stringify(flowEdges),
                start_node_id: startNodeId,
                batch_index: batchIndex,
                total_batches: totalBatches
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
        
        if (batchPromises.length > 0) {
            logger.info(`[SCHEDULED-DISPARO ${history_id}] Publicando ${batchPromises.length} batches no BullMQ...`);
            await Promise.all(batchPromises);
        }
        
        logger.info(`[SCHEDULED-DISPARO] Disparo agendado ${history_id} processado com sucesso.`);
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
