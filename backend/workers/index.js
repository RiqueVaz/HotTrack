const queueManager = require('../shared/queue-manager');
const { processTimeoutData } = require('../worker/process-timeout');
const { processDisparoData } = require('../worker/process-disparo');
const { sqlTx, sqlWithRetry } = require('../db');

console.log('[WORKERS] Inicializando workers BullMQ...');

// ========================================
// Worker 1: Timeouts de Fluxo
// ========================================
queueManager.createWorker('timeouts', async (job) => {
    const { chat_id, bot_id, target_node_id, variables } = job.data;
    
    console.log(`[WORKER-TIMEOUT] Processando timeout para chat ${chat_id}, bot ${bot_id}`);
    
    await processTimeoutData({
        chat_id,
        bot_id,
        target_node_id,
        variables,
        continue_from_delay: false,
        remaining_actions: null
    });
    
    return { success: true, chat_id, bot_id };
}, {
    concurrency: 20, // Processar 20 timeouts simultaneamente
    limiter: {
        max: 100, // Máximo 100 jobs
        duration: 1000, // Por segundo
    }
});

// ========================================
// Worker 2: Delays de Ações
// ========================================
queueManager.createWorker('delays', async (job) => {
    const { chat_id, bot_id, target_node_id, variables, continue_from_delay, remaining_actions } = job.data;
    
    console.log(`[WORKER-DELAY] Processando delay para chat ${chat_id}, bot ${bot_id}`);
    
    await processTimeoutData({
        chat_id,
        bot_id,
        target_node_id,
        variables,
        continue_from_delay: continue_from_delay || true,
        remaining_actions: remaining_actions
    });
    
    return { success: true, chat_id, bot_id };
}, {
    concurrency: 20,
    limiter: {
        max: 100,
        duration: 1000,
    }
});

// ========================================
// Worker 3: Disparos Individuais
// ========================================
queueManager.createWorker('disparos', async (job) => {
    const { history_id, chat_id, bot_id, flow_nodes, flow_edges, start_node_id, variables_json, bot_token } = job.data;
    
    console.log(`[WORKER-DISPARO] Processando disparo para chat ${chat_id}, history ${history_id}`);
    
    await processDisparoData({
        history_id,
        chat_id,
        bot_id,
        flow_nodes,
        flow_edges,
        start_node_id,
        variables_json
    });
    
    return { success: true, chat_id, history_id };
}, {
    concurrency: 10, // 10 disparos simultâneos
    limiter: {
        max: 50, // Máximo 50 jobs por segundo (rate limit do Telegram)
        duration: 1000,
    }
});

// ========================================
// Worker 4: Disparos Agendados
// ========================================
queueManager.createWorker('disparos-agendados', async (job) => {
    const { history_id } = job.data;
    
    console.log(`[WORKER-SCHEDULED-DISPARO] Processando disparo agendado ${history_id}`);
    
    try {
        // Buscar dados do disparo agendado
        const [history] = await sqlWithRetry(
            sqlTx`SELECT * FROM disparo_history WHERE id = ${history_id}`
        );
        
        if (!history) {
            throw new Error(`Disparo ${history_id} não encontrado`);
        }
        
        console.log(`[WORKER-SCHEDULED-DISPARO] Disparo encontrado: ${history.campaign_name}, status: ${history.status}`);
        
        // Atualizar status para RUNNING
        await sqlWithRetry(
            sqlTx`UPDATE disparo_history SET status = 'RUNNING' WHERE id = ${history_id}`
        );
        
        // Buscar contatos e processar (lógica similar ao endpoint /api/worker/process-scheduled-disparo)
        const botIds = Array.isArray(history.bot_ids) ? history.bot_ids : JSON.parse(history.bot_ids || '[]');
        const disparoFlowId = history.disparo_flow_id;
        
        if (!disparoFlowId) {
            throw new Error('Disparo não tem flow_id associado');
        }
        
        // Buscar fluxo de disparo
        const [disparoFlow] = await sqlWithRetry(
            sqlTx`SELECT * FROM disparo_flows WHERE id = ${disparoFlowId}`
        );
        
        if (!disparoFlow || !disparoFlow.nodes) {
            throw new Error(`Fluxo de disparo ${disparoFlowId} não encontrado`);
        }
        
        const flowData = typeof disparoFlow.nodes === 'string' ? JSON.parse(disparoFlow.nodes) : disparoFlow.nodes;
        const flowSteps = flowData.steps || [];
        
        // Buscar contatos únicos dos bots selecionados
        const contactsResult = await sqlWithRetry(
            sqlTx`
                SELECT DISTINCT tc.chat_id, tc.bot_id, tb.bot_token
                FROM telegram_chats tc
                JOIN telegram_bots tb ON tc.bot_id = tb.id
                WHERE tc.bot_id = ANY(${botIds})
                AND tc.chat_id IS NOT NULL
                ORDER BY tc.chat_id
            `
        );
        
        console.log(`[WORKER-SCHEDULED-DISPARO] ${contactsResult.length} contatos encontrados`);
        
        // Criar mapa de bot tokens
        const botTokenMap = new Map();
        for (const contact of contactsResult) {
            botTokenMap.set(contact.bot_id, contact.bot_token);
        }
        
        // Enfileirar disparos individuais
        let messageCounter = 0;
        const delayBetweenMessages = 2; // 2 segundos entre mensagens
        
        for (const contact of contactsResult) {
            const totalDelaySeconds = messageCounter * delayBetweenMessages;
            const botToken = botTokenMap.get(contact.bot_id) || '';
            
            const payload = {
                history_id: history_id,
                chat_id: contact.chat_id,
                bot_id: contact.bot_id,
                flow_nodes: flowSteps,
                flow_edges: [],
                start_node_id: null,
                variables_json: JSON.stringify({}),
                bot_token: botToken
            };
            
            // Adicionar job de disparo individual
            await queueManager.addJob(
                'disparos',
                'process-disparo',
                payload,
                {
                    delay: totalDelaySeconds * 1000,
                    attempts: 3,
                    jobId: `disparo-${contact.chat_id}-${history_id}-${Date.now()}-${messageCounter}`,
                }
            );
            
            messageCounter++;
        }
        
        console.log(`[WORKER-SCHEDULED-DISPARO] ${messageCounter} disparos individuais enfileirados`);
        
        return { success: true, history_id, total_enqueued: messageCounter };
        
    } catch (error) {
        console.error(`[WORKER-SCHEDULED-DISPARO] Erro ao processar disparo agendado ${history_id}:`, error);
        
        // Atualizar status para ERROR
        await sqlWithRetry(
            sqlTx`UPDATE disparo_history SET status = 'ERROR' WHERE id = ${history_id}`
        ).catch(err => console.error('Erro ao atualizar status:', err));
        
        throw error;
    }
}, {
    concurrency: 5, // Menos concorrência para disparos agendados
    limiter: {
        max: 10,
        duration: 1000,
    }
});

console.log('[WORKERS] Todos os workers iniciados com sucesso!');
console.log('[WORKERS] - timeouts: concurrency=20, rate=100/s');
console.log('[WORKERS] - delays: concurrency=20, rate=100/s');
console.log('[WORKERS] - disparos: concurrency=10, rate=50/s');
console.log('[WORKERS] - disparos-agendados: concurrency=5, rate=10/s');

