// /backend/worker/process-disparo.js
// Este worker não tem a função processFlow, a correção não é aplicável aqui. disparo em massa.

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const { Client } = require("@upstash/qstash");
const crypto = require('crypto');
const { createPixService } = require('../shared/pix');
const logger = require('../logger');
const { sqlTx, sqlWithRetry } = require('../db');
const telegramRateLimiter = require('../shared/telegram-rate-limiter');
const { sendEventToUtmify: sendEventToUtmifyShared, sendMetaEvent: sendMetaEventShared } = require('../shared/event-sender');
const { handleSuccessfulPayment: handleSuccessfulPaymentShared } = require('../shared/payment-handler');
const { migrateMediaOnDemand } = require('../shared/migrate-media-on-demand');

// Inicializar QStash client para delays longos
const qstashClient = new Client({
    token: process.env.QSTASH_TOKEN
});

const DEFAULT_INVITE_MESSAGE = 'Seu link exclusivo está pronto! Clique no botão abaixo para acessar.';
const DEFAULT_INVITE_BUTTON_TEXT = 'Acessar convite';

// ==========================================================
//                   INICIALIZAÇÃO
// ==========================================================
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();

// Cleanup automático do cache de tokens SyncPay (evita memory leak)
setInterval(() => {
    const now = Date.now();
    let cleaned = 0;
    for (const [sellerId, tokenData] of syncPayTokenCache.entries()) {
        if (tokenData.expiresAt && now > tokenData.expiresAt) {
            syncPayTokenCache.delete(sellerId);
            cleaned++;
        }
    }
    if (cleaned > 0 && (process.env.NODE_ENV !== 'production' || process.env.ENABLE_VERBOSE_LOGS === 'true')) {
        console.log(`[Memory Cleanup] Removidos ${cleaned} tokens expirados do syncPayTokenCache (worker-disparo)`);
    }
}, 5 * 60 * 1000); // A cada 5 minutos

const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const WIINPAY_SPLIT_USER_ID = process.env.WIINPAY_SPLIT_USER_ID;

const {
    getSyncPayAuthToken,
    generatePixForProvider,
    generatePixWithFallback
} = createPixService({
    sql: sqlTx,
    sqlWithRetry,
    axios,
    uuidv4,
    syncPayTokenCache,
    adminApiKey: ADMIN_API_KEY,
    synPayBaseUrl: SYNCPAY_API_BASE_URL,
    pushinpaySplitAccountId: PUSHINPAY_SPLIT_ACCOUNT_ID,
    cnpaySplitProducerId: CNPAY_SPLIT_PRODUCER_ID,
    oasyfySplitProducerId: OASYFY_SPLIT_PRODUCER_ID,
    brpixSplitRecipientId: BRPIX_SPLIT_RECIPIENT_ID,
    wiinpaySplitUserId: WIINPAY_SPLIT_USER_ID,
    hottrackApiUrl: process.env.HOTTRACK_API_URL,
});


// ==========================================================
//          FUNÇÕES AUXILIARES (Copiadas do backend.js)
// ==========================================================

async function replaceVariables(text, variables) {
    if (!text) return '';
    let processedText = text;
    for (const key in variables) {
        const regex = new RegExp(`{{${key}}}`, 'g');
        processedText = processedText.replace(regex, variables[key]);
    }
    return processedText;
}

const normalizeChatIdentifier = (value) => {
    if (value === null || value === undefined) return null;
    const trimmed = String(value).trim();
    if (!trimmed) return null;
    if (/^-?\d+$/.test(trimmed)) {
        const numericId = Number(trimmed);
        if (Number.isSafeInteger(numericId)) {
            return numericId;
        }
    }
    return trimmed;
};

// Função helper para marcar bloqueio na tabela bot_blocks
async function markBotBlockedInDb(botId, chatId, sellerId) {
    try {
        await sqlWithRetry(
            sqlTx`INSERT INTO bot_blocks (bot_id, chat_id, seller_id, detected_at, last_verified_at)
                  VALUES (${botId}, ${chatId}, ${sellerId}, NOW(), NOW())
                  ON CONFLICT (bot_id, chat_id) 
                  DO UPDATE SET last_verified_at = NOW()`
        );
        // Também atualizar cache em memória (opcional, para performance)
        const dbCache = require('../shared/db-cache');
        dbCache.markBotBlocked(botId, chatId);
    } catch (error) {
        logger.error(`[Bot Blocks] Erro ao marcar bloqueio: ${error.message}`);
    }
}

async function sendTelegramRequest(botToken, method, data, options = {}, retries = 3, delay = 1500, botId = null) {
    const { headers = {}, responseType = 'json', timeout = 30000 } = options;
    const apiUrl = `https://api.telegram.org/bot${botToken}/${method}`;
    
    // Aplicar rate limiting proativo antes de fazer a requisição
    const chatId = data?.chat_id || null;
    
    // VERIFICAR CACHE ANTES DE TENTAR
    if (chatId && chatId !== 'unknown' && chatId !== null) {
        const dbCache = require('../shared/db-cache');
        if (botId && dbCache.isBotBlocked(botId, chatId)) {
            logger.debug(`[CACHE] Chat ${chatId} bloqueou bot ${botId}. Pulando requisição.`);
            return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
        } else if (!botId && dbCache.isBotTokenBlocked(botToken, chatId)) {
            logger.debug(`[CACHE] Chat ${chatId} bloqueou bot (token). Pulando requisição.`);
            return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
        }
    }
    
    await telegramRateLimiter.waitIfNeeded(botToken, chatId);
    
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, { headers, responseType, timeout });
            // Se mensagem foi enviada com sucesso, verificar se havia bloqueio e remover
            if (response.data && response.data.ok && chatId && chatId !== 'unknown' && botId) {
                try {
                    await sqlWithRetry(
                        sqlTx`DELETE FROM bot_blocks WHERE bot_id = ${botId} AND chat_id = ${chatId}`
                    );
                    const dbCache = require('../shared/db-cache');
                    dbCache.unmarkBotBlocked(botId, chatId);
                } catch (unblockError) {
                    // Não crítico, apenas logar
                    logger.debug(`[Bot Blocks] Erro ao remover bloqueio (não crítico): ${unblockError.message}`);
                }
            }
            return response.data;
        } catch (error) {
            // FormData do Node.js não tem .get(), então tenta extrair do erro ou deixa undefined
            const errorChatId = data?.chat_id || 'unknown';
            const description = error.response?.data?.description || error.message;
            if (error.response && error.response.status === 403) {
                const dbCache = require('../shared/db-cache');
                
                // MARCAR NO CACHE E NA TABELA QUANDO RECEBER 403
                if (description.includes('bot was blocked by the user') && errorChatId && errorChatId !== 'unknown') {
                    if (botId) {
                        dbCache.markBotBlocked(botId, errorChatId);
                        // Buscar seller_id do bot e inserir na tabela
                        try {
                            const [bot] = await sqlWithRetry(
                                sqlTx`SELECT seller_id FROM telegram_bots WHERE id = ${botId}`
                            );
                            if (bot) {
                                await markBotBlockedInDb(botId, errorChatId, bot.seller_id);
                            }
                        } catch (dbError) {
                            logger.warn(`[Bot Blocks] Erro ao buscar seller_id para bot ${botId}: ${dbError.message}`);
                        }
                    } else {
                        dbCache.markBotTokenBlocked(botToken, errorChatId);
                    }
                }
                
                logger.debug(`[WORKER-DISPARO] Chat ${errorChatId} bloqueou o bot (method ${method}). Ignorando.`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento para erro 429 (Too Many Requests)
            if (error.response && error.response.status === 429) {
                const retryAfter = parseInt(error.response.headers['retry-after'] || error.response.headers['Retry-After'] || '2');
                const waitTime = retryAfter * 1000; // Converter para milissegundos
                
                // Reduzir logging de rate limits - só logar a cada 20 ocorrências ou na primeira/última tentativa
                if (i === 0 || i === retries - 1 || (i % 20 === 0 && i > 0)) {
                    logger.debug(`[WORKER-DISPARO] Rate limit atingido (429). Aguardando ${retryAfter}s. Method: ${method}`);
                }
                
                if (i < retries - 1) {
                    await new Promise(res => setTimeout(res, waitTime));
                    continue; // Tentar novamente após esperar
                } else {
                    // Se esgotou as tentativas, retornar erro
                    logger.error(`[WORKER-DISPARO] Rate limit persistente após ${retries} tentativas. Method: ${method}`);
                    return { ok: false, error_code: 429, description: 'Too Many Requests: Rate limit exceeded' };
                }
            }

            // Tratamento específico para TOPIC_CLOSED
            if (error.response && error.response.status === 400 && 
                error.response.data?.description?.includes('TOPIC_CLOSED')) {
                logger.warn(`[WORKER-DISPARO] Chat de grupo fechado. ChatID: ${errorChatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }
            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');
            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }
            logger.error(`[WORKER-DISPARO - Telegram API ERROR] Method: ${method}, ChatID: ${errorChatId}:`, error.response?.data || error.message);
            throw error;
        }
    }
}

async function saveMessageToDb(sellerId, botId, message, senderType, variables = {}) {
    const { message_id, chat, from, text, photo, video, voice, reply_markup } = message;
    let mediaType = null;
    let mediaFileId = null;
    let messageText = text;
    if (photo) {
        mediaType = 'photo';
        mediaFileId = photo[photo.length - 1].file_id;
        messageText = message.caption || '[Foto]';
    } else if (video) {
        mediaType = 'video';
        mediaFileId = video.file_id;
        messageText = message.caption || '[Vídeo]';
    } else if (voice) {
        mediaType = 'voice';
        mediaFileId = voice.file_id;
        messageText = '[Mensagem de Voz]';
    }
    
    const fromUser = from || chat;

    // Extrai reply_markup se existir
    const replyMarkupJson = reply_markup ? JSON.stringify(reply_markup) : null;

    // CORREÇÃO FINAL: Salva NULL para os dados do usuário quando o remetente é o bot.
    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id, reply_markup)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET reply_markup = EXCLUDED.reply_markup;
    `, [
        sellerId, botId, chat.id, message_id, fromUser.id, 
        senderType === 'user' ? fromUser.first_name : null, 
        senderType === 'user' ? fromUser.last_name : null, 
        senderType === 'user' ? fromUser.username : null, 
        messageText, senderType, mediaType, mediaFileId, 
        variables.click_id || null,
        replyMarkupJson
    ]);
}


async function sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption) {
    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    if (!storageBotToken) throw new Error('Token do bot de armazenamento não configurado.');
    
    // Arquivos da biblioteca têm file_id do bot de storage, então sempre precisam ser baixados e reenviados
    // Não tentar usar file_id diretamente pois não funcionará com o bot do usuário
    try {
        const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { file_id: fileId });
        if (!fileInfo.ok) {
            // Se getFile falhar com "file is too big", tentar usar file_id diretamente
            if (fileInfo.error_code === 400 && fileInfo.description && fileInfo.description.includes('too big')) {
                const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
                const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
                const method = methodMap[fileType];
                const field = fieldMap[fileType];
                const timeout = fileType === 'video' ? 120000 : 60000;
                try {
                    return await sendTelegramRequest(destinationBotToken, method, { 
                        chat_id: chatId, 
                        [field]: fileId, 
                        caption, 
                        parse_mode: 'HTML' 
                    }, { timeout });
                } catch (bigFileError) {
                    const bigFileErrorMessage = bigFileError.message || bigFileError.description || '';
                    const bigFileErrorResponseDesc = bigFileError.response?.data?.description || '';
                    if (bigFileErrorMessage.includes('wrong remote file identifier') || 
                        bigFileErrorResponseDesc.includes('wrong remote file identifier')) {
                        console.warn(`[Disparo Media] File ID inválido para arquivo grande ${fileType}. Pulando envio.`);
                        return null;
                    }
                    throw bigFileError;
                }
            }
            throw new Error('Não foi possível obter informações do arquivo da biblioteca.');
        }
        
        const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;
        const { data: fileBuffer, headers: fileHeaders } = await axios.get(fileUrl, { responseType: 'arraybuffer' });
        const formData = new FormData();
        formData.append('chat_id', chatId);
        if (caption) {
            formData.append('caption', caption);
            formData.append('parse_mode', 'HTML');
        }
        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
        const fileNameMap = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' };
        const method = methodMap[fileType];
        const field = fieldMap[fileType];
        const fileName = fileNameMap[fileType];
        const timeout = fileType === 'video' ? 120000 : 30000;
        if (!method) throw new Error('Tipo de arquivo não suportado.');

        formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

        return await sendTelegramRequest(destinationBotToken, method, formData, { headers: formData.getHeaders(), timeout });
    } catch (error) {
        // Verificar se é erro de file_id inválido
        const errorMessage = error.message || error.description || '';
        const responseDesc = error.response?.data?.description || '';
        if (errorMessage.includes('wrong remote file identifier') || 
            responseDesc.includes('wrong remote file identifier')) {
            console.warn(`[Disparo Media] File ID inválido em sendMediaAsProxy para ${fileType}. Pulando envio.`);
            return null;
        }
        // Se falhar ao baixar, não tentar file_id direto pois é arquivo da biblioteca (file_id do bot de storage)
        // O file_id não funcionará com o bot do usuário
        throw error;
    }
}

// Funções de PIX (necessárias para o passo 'pix')

// ==========================================================
//           FUNÇÃO PARA PROCESSAR FLUXO COMPLETO DE DISPARO
// ==========================================================

// Função auxiliar para encontrar próximo nó
function findNextNode(nodeId, handle, edges) {
    const edge = edges.find(e => e.source === nodeId && e.sourceHandle === handle);
    return edge ? edge.target : null;
}

// Função simplificada para processar fluxo de disparo
// Esta função processa o fluxo completo começando do start_node_id
async function processDisparoFlow(chatId, botId, botToken, sellerId, startNodeId, initialVariables, flowNodes, flowEdges, historyId) {
    const logPrefix = '[WORKER-DISPARO]';
    const startTime = Date.now();
    logger.debug(`${logPrefix} [Flow Engine] Iniciando processo de disparo para ${chatId}. Nó inicial: ${startNodeId}`);
    
    // ==========================================================
    // LIMPEZA: Deletar estado de fluxo antigo antes de iniciar
    // Isso evita conflitos quando novos leads entram durante disparo
    // ==========================================================
    try {
        const [existingState] = await sqlWithRetry(sqlTx`
            SELECT scheduled_message_id 
            FROM user_flow_states 
            WHERE chat_id = ${chatId} AND bot_id = ${botId}
        `);
        
        if (existingState) {
            // Cancelar tarefa QStash pendente se existir
            if (existingState.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(existingState.scheduled_message_id);
                    logger.debug(`${logPrefix} Tarefa QStash cancelada: ${existingState.scheduled_message_id}`);
                } catch (e) {
                    logger.warn(`${logPrefix} Erro ao cancelar QStash (não crítico):`, e.message);
                }
            }
            
            // Deletar estado antigo
            await sqlWithRetry(sqlTx`
                DELETE FROM user_flow_states 
                WHERE chat_id = ${chatId} AND bot_id = ${botId}
            `);
            logger.debug(`${logPrefix} Estado de fluxo limpo antes de iniciar disparo para chat ${chatId}.`);
        }
    } catch (error) {
        // Não interromper o disparo se falhar ao limpar estado
        logger.warn(`${logPrefix} Erro ao limpar estado de fluxo (não crítico):`, error.message);
    }
    
    let variables = { ...initialVariables };
    let currentNodeId = startNodeId;
    let safetyLock = 0;
    let lastTransactionId = null; // Rastrear transaction_id para salvar no disparo_log
    let logStatus = 'SENT';
    let logDetails = 'Enviado com sucesso.';
    const maxIterations = 50; // Proteção contra loops infinitos
    
    // Otimização: Criar Map para busca O(1) ao invés de O(n) com find
    const nodeMap = new Map(flowNodes.map(node => [node.id, node]));
    
    try {
        // Buscar click completo do chat para obter pressel_id de origem
        let chatClick = null;
        try {
            // Buscar click_id do chat
            const [chatData] = await sqlWithRetry(sqlTx`
                SELECT click_id 
                FROM telegram_chats 
                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
            `);
            
            if (chatData && chatData.click_id) {
                // Buscar click completo através do click_id
                const db_click_id = chatData.click_id.startsWith('/start ') ? chatData.click_id : `/start ${chatData.click_id}`;
                const [foundClick] = await sqlWithRetry(sqlTx`
                    SELECT * FROM clicks 
                    WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                    LIMIT 1
                `);
                
                if (foundClick) {
                    chatClick = foundClick;
                    logger.debug(`${logPrefix} Click encontrado para chat ${chatId}: click_id=${foundClick.click_id}, pressel_id=${foundClick.pressel_id || 'null'}`);
                } else {
                    logger.debug(`${logPrefix} Click não encontrado no banco para click_id=${db_click_id}`);
                }
            } else {
                logger.debug(`${logPrefix} Chat ${chatId} não possui click_id associado`);
            }
        } catch (error) {
            // Não interromper o fluxo se falhar ao buscar click
            logger.warn(`${logPrefix} Erro ao buscar click do chat (não crítico):`, error.message);
        }
        
        // Armazenar click do chat nas variáveis para uso posterior
        if (chatClick) {
            variables._chatClick = chatClick;
        }
        
        // Garantir que variáveis faltantes sejam buscadas do banco
        await ensureVariablesFromDatabase(chatId, botId, sellerId, variables, logPrefix);
        
        // Processar fluxo - usando lógica similar ao processFlow normal
        while (currentNodeId && safetyLock < maxIterations) {
            safetyLock++;
            const currentNode = nodeMap.get(currentNodeId); // Busca O(1) ao invés de O(n)
            
            if (!currentNode) {
                logger.warn(`${logPrefix} Nó ${currentNodeId} não encontrado. Encerrando fluxo.`);
                break;
            }
            
            logger.debug(`${logPrefix} Processando nó ${currentNodeId} (tipo: ${currentNode.type}, iteração: ${safetyLock})`);
            
            if (currentNode.type === 'trigger') {
                // Nó de 'trigger' é apenas um ponto de partida, executa ações aninhadas (se houver) e segue
                if (currentNode.data?.actions && currentNode.data.actions.length > 0) {
                    try {
                        const actionResult = await processDisparoActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, logPrefix, historyId, currentNodeId);
                        
                        // Se delay foi agendado, parar processamento
                        if (actionResult === 'delay_scheduled') {
                            logger.debug(`${logPrefix} [Flow Engine] Delay agendado. Parando processamento atual.`);
                            currentNodeId = null;
                            break;
                        }
                        
                        // Se retornou transactionId, atualizar
                        if (actionResult && actionResult !== 'paid' && actionResult !== 'pending' && actionResult !== 'completed' && actionResult !== 'delay_scheduled') {
                            lastTransactionId = actionResult;
                            logger.debug(`${logPrefix} Transaction ID atualizado: ${actionResult}`);
                        }
                    } catch (error) {
                        logger.error(`${logPrefix} Erro ao processar ações do trigger:`, error);
                        const isCriticalError = error.message?.includes('crítico') || error.message?.includes('critical');
                        if (isCriticalError) {
                            logStatus = 'FAILED';
                            logDetails = error.message.substring(0, 255);
                            break;
                        }
                    }
                }
                currentNodeId = findNextNode(currentNode.id, 'a', flowEdges);
                if (!currentNodeId) {
                    logger.debug(`${logPrefix} Trigger não tem nós conectados. Encerrando fluxo.`);
                    break;
                }
                continue;
            }
            
            if (currentNode.type === 'action') {
                // Processar ações do nó
                const allActions = currentNode.data?.actions || [];
                
                logger.debug(`${logPrefix} [Flow Engine] Processando nó 'action' com ${allActions.length} ação(ões): ${allActions.map(a => a.type).join(', ')}`);
                
                let actionResult;
                try {
                    actionResult = await processDisparoActions(allActions, chatId, botId, botToken, sellerId, variables, logPrefix, historyId, currentNodeId);
                    
                    // Se retornou transactionId (string que não é um código de controle), atualizar
                    if (actionResult && actionResult !== 'paid' && actionResult !== 'pending' && actionResult !== 'completed' && actionResult !== 'delay_scheduled') {
                        lastTransactionId = actionResult;
                        logger.debug(`${logPrefix} Transaction ID atualizado: ${actionResult}`);
                    }
                } catch (actionError) {
                    // Erro crítico durante execução de ação
                    logger.error(`${logPrefix} [Flow Engine] Erro CRÍTICO ao processar ações do nó ${currentNode.id}:`, actionError.message);
                    const isCriticalError = actionError.message?.includes('crítico') || actionError.message?.includes('critical') || 
                                          actionError.message?.includes('não encontrado') || actionError.message?.includes('not found');
                    if (isCriticalError) {
                        logStatus = 'FAILED';
                        logDetails = actionError.message.substring(0, 255);
                        break;
                    }
                    // Para erros não críticos, continua para o próximo nó
                    logger.warn(`${logPrefix} Erro não crítico no nó ${currentNodeId}, continuando para próximo nó.`);
                    actionResult = 'completed'; // Continuar normalmente
                }
                
                // Verifica se delay foi agendado
                if (actionResult === 'delay_scheduled') {
                    logger.debug(`${logPrefix} [Flow Engine] Delay agendado. Parando processamento atual.`);
                    currentNodeId = null;
                    break;
                }
                
                // Ignorar waitForReply em disparos (sempre seguir pelo handle padrão)
                // Em disparos, não há interação do usuário, então sempre seguir pelo handle 'a'
                
                // Verifica se o resultado foi de um 'action_check_pix'
                if (actionResult === 'paid') {
                    logger.debug(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pago. Seguindo handle 'a'.`);
                    currentNodeId = findNextNode(currentNode.id, 'a', flowEdges); // 'a' = Pago
                    continue;
                }
                if (actionResult === 'pending') {
                    logger.debug(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pendente. Seguindo handle 'b'.`);
                    currentNodeId = findNextNode(currentNode.id, 'b', flowEdges); // 'b' = Pendente
                    continue;
                }
                
                // Se nada acima aconteceu, é um nó de ação simples. Segue pelo handle 'a'.
                currentNodeId = findNextNode(currentNode.id, 'a', flowEdges);
                continue;
            }
            
            // Tipo de nó não suportado em disparos
            logger.warn(`${logPrefix} Tipo de nó não suportado em disparos: ${currentNode.type}. Encerrando fluxo.`);
            break;
        }
        
        if (safetyLock >= maxIterations) {
            logger.warn(`${logPrefix} Limite de iterações atingido (${maxIterations}). Encerrando fluxo por segurança.`);
            logStatus = 'FAILED';
            logDetails = 'Limite de iterações atingido';
        }
        
        const processingTime = Date.now() - startTime;
        logger.debug(`${logPrefix} [Flow Engine] Processamento concluído para ${chatId} em ${processingTime}ms. Status: ${logStatus}`);
    } catch (error) {
        logStatus = 'FAILED';
        logDetails = error.message.substring(0, 255);
        logger.error(`${logPrefix} Erro crítico ao processar fluxo:`, error);
        logger.error(`${logPrefix} Stack trace:`, error.stack);
    }
    
    // Atualizar histórico e salvar disparo_log
    if (historyId) {
        try {
            // Atualizar processed_jobs e verificar se concluído
            let updatedHistory = null;
            if (logStatus === 'FAILED') {
                const [result] = await sqlWithRetry(sqlTx`
                    UPDATE disparo_history 
                    SET processed_jobs = processed_jobs + 1,
                        failure_count = failure_count + 1
                    WHERE id = ${historyId}
                    RETURNING processed_jobs, total_jobs, status
                `);
                updatedHistory = result;
            } else {
                const [result] = await sqlWithRetry(sqlTx`
                    UPDATE disparo_history 
                    SET processed_jobs = processed_jobs + 1
                    WHERE id = ${historyId}
                    RETURNING processed_jobs, total_jobs, status
                `);
                updatedHistory = result;
            }
            
            // Verificar se todas as mensagens foram enviadas
            if (updatedHistory && updatedHistory.processed_jobs >= updatedHistory.total_jobs && updatedHistory.total_jobs > 0) {
                if (updatedHistory.status !== 'COMPLETED') {
                    await sqlWithRetry(sqlTx`
                        UPDATE disparo_history 
                        SET status = 'COMPLETED', current_step = NULL
                        WHERE id = ${historyId}
                    `);
                    logger.debug(`${logPrefix} Disparo ${historyId} concluído. Todas as mensagens foram enviadas.`);
                }
            }
        } catch (error) {
            logger.error(`${logPrefix} Erro ao atualizar histórico:`, error);
        }
        
        // Salvar disparo_log
        try {
            await sqlWithRetry(
                sqlTx`INSERT INTO disparo_log (history_id, chat_id, bot_id, status, details, transaction_id) 
                       VALUES (${historyId}, ${chatId}, ${botId}, ${logStatus}, ${logDetails}, ${lastTransactionId})`
            );
        } catch (error) {
            logger.error(`${logPrefix} Erro ao salvar disparo_log:`, error);
        }
    }
}

// As funções compartilhadas são chamadas diretamente com objetos

/**
 * Busca variáveis faltantes do banco de dados quando não estão disponíveis nas variáveis.
 * Similar ao comportamento de fallback usado para last_transaction_id.
 * @param {number} chatId - ID do chat do Telegram
 * @param {number} botId - ID do bot
 * @param {number} sellerId - ID do vendedor
 * @param {Object} variables - Objeto de variáveis (será modificado in-place)
 * @param {string} logPrefix - Prefixo para logs
 */
async function ensureVariablesFromDatabase(chatId, botId, sellerId, variables, logPrefix = '[Variables]') {
    try {
        // Buscar primeiro_nome e nome_completo se não estiverem disponíveis
        if (!variables.primeiro_nome || !variables.nome_completo) {
            const [user] = await sqlWithRetry(sqlTx`
                SELECT first_name, last_name 
                FROM telegram_chats 
                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
                ORDER BY created_at DESC LIMIT 1
            `);
            
            if (user) {
                if (!variables.primeiro_nome) {
                    variables.primeiro_nome = user.first_name || '';
                    logger.debug(`${logPrefix} primeiro_nome buscado do banco: ${variables.primeiro_nome}`);
                }
                if (!variables.nome_completo) {
                    variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
                    logger.debug(`${logPrefix} nome_completo buscado do banco: ${variables.nome_completo}`);
                }
            }
        }
        
        // Buscar click_id se não estiver disponível
        let clickIdToUse = variables.click_id;
        if (!clickIdToUse) {
            const [chatData] = await sqlWithRetry(sqlTx`
                SELECT click_id 
                FROM telegram_chats 
                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
            `);
            
            if (chatData && chatData.click_id) {
                clickIdToUse = chatData.click_id;
                variables.click_id = clickIdToUse;
                logger.debug(`${logPrefix} click_id buscado do banco: ${clickIdToUse}`);
            }
        }
        
        // Buscar last_transaction_id se não estiver disponível
        if (!variables.last_transaction_id) {
            const [chatData] = await sqlWithRetry(sqlTx`
                SELECT last_transaction_id 
                FROM telegram_chats 
                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND last_transaction_id IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
            `);
            
            if (chatData && chatData.last_transaction_id) {
                variables.last_transaction_id = chatData.last_transaction_id;
                logger.debug(`${logPrefix} last_transaction_id buscado do banco: ${chatData.last_transaction_id}`);
            }
        }
        
        // Buscar cidade e estado se não estiverem disponíveis e tivermos click_id
        if ((!variables.cidade || !variables.estado) && clickIdToUse) {
            const db_click_id = clickIdToUse.startsWith('/start ') ? clickIdToUse : `/start ${clickIdToUse}`;
            const [click] = await sqlWithRetry(sqlTx`
                SELECT city, state 
                FROM clicks 
                WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                LIMIT 1
            `);
            
            if (click) {
                if (!variables.cidade) {
                    variables.cidade = click.city || '';
                    logger.debug(`${logPrefix} cidade buscada do banco: ${variables.cidade}`);
                }
                if (!variables.estado) {
                    variables.estado = click.state || '';
                    logger.debug(`${logPrefix} estado buscado do banco: ${variables.estado}`);
                }
            } else {
                // Se não encontrou click, definir valores vazios como fallback
                if (!variables.cidade) variables.cidade = '';
                if (!variables.estado) variables.estado = '';
            }
        } else if (!variables.cidade) {
            variables.cidade = '';
        } else if (!variables.estado) {
            variables.estado = '';
        }
    } catch (error) {
        // Não falhar se houver erro ao buscar variáveis do banco
        logger.warn(`${logPrefix} Erro ao buscar variáveis do banco (não crítico):`, error.message);
    }
}

// Wrapper para handleSuccessfulPayment que passa as dependências necessárias
async function handleSuccessfulPayment(transaction_id, customerData) {
    return await handleSuccessfulPaymentShared({
        transaction_id,
        customerData,
        sqlTx,
        adminSubscription: null, // Worker não tem notificações push
        webpush: null, // Worker não tem notificações push
        sendEventToUtmify: ({ status, clickData, pixData, sellerData, customerData, productData }) => 
            sendEventToUtmifyShared({ status, clickData, pixData, sellerData, customerData, productData, sqlTx }),
        sendMetaEvent: ({ eventName, clickData, transactionData, customerData }) => 
            sendMetaEventShared({ eventName, clickData, transactionData, customerData, sqlTx })
    });
}

/**
 * Extrai checkoutId de uma URL de checkout
 * @param {string} url - URL que pode conter /oferta/cko_xxx
 * @returns {string|null} - checkoutId ou null se não for checkout
 */
function extractCheckoutIdFromUrl(url) {
    try {
        const urlObj = new URL(url.startsWith('http') ? url : `https://${url}`);
        const pathMatch = urlObj.pathname.match(/\/oferta\/(cko_[a-f0-9-]+)/i);
        return pathMatch ? pathMatch[1] : null;
    } catch {
        return null;
    }
}

/**
 * Parse JSON field helper
 */
function parseJsonField(value, context) {
    if (value === null || value === undefined) {
        return value;
    }
    if (typeof value === 'object') {
        return value;
    }
    if (typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch (error) {
            logger.error(`[JSON] Falha ao converter ${context}:`, error);
            throw new Error(`JSON_PARSE_ERROR_${context}`);
        }
    }
    return value;
}

// Função simplificada para processar ações em disparos
async function processDisparoActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix, historyId = null, currentNodeId = null) {
    let lastPixTransactionId = null; // Rastrear último transaction_id gerado
    const actionStartTime = Date.now();
    logger.debug(`${logPrefix} Processando ${actions.length} ação(ões) para chat ${chatId}`);
    
    // Garantir que variáveis faltantes sejam buscadas do banco
    await ensureVariablesFromDatabase(chatId, botId, sellerId, variables, logPrefix);
    
    for (let i = 0; i < actions.length; i++) {
        const action = actions[i];
        const actionIndex = i + 1;
        try {
            const actionData = action.data || {};
            logger.debug(`${logPrefix} [${actionIndex}/${actions.length}] Processando ação tipo: ${action.type}`);
            
            if (action.type === 'message') {
                try {
                    let textToSend = await replaceVariables(actionData.text, variables);
                    
                    // Validação do tamanho do texto (limite do Telegram: 4096 caracteres)
                    if (textToSend.length > 4096) {
                        logger.warn(`${logPrefix} [Flow Message] Texto excede limite de 4096 caracteres. Truncando...`);
                        textToSend = textToSend.substring(0, 4093) + '...';
                    }
                    
                    // Verifica se tem botão para anexar
                    if (actionData.buttonUrl) {
                        // Trata buttonText vazio, null, undefined ou string vazia
                        let rawBtnText = actionData.buttonText;
                        if (!rawBtnText || (typeof rawBtnText === 'string' && rawBtnText.trim() === '')) {
                            rawBtnText = 'Clique aqui';
                            logger.warn(`${logPrefix} [Flow Message] Botão sem texto informado. Aplicando texto padrão '${rawBtnText}'.`);
                        }
                        
                        let btnText = await replaceVariables(rawBtnText, variables);
                        // Garante que após replaceVariables ainda tenha texto válido
                        btnText = btnText && btnText.trim() !== '' ? btnText : 'Clique aqui';

                        let btnUrl = await replaceVariables(actionData.buttonUrl, variables);
                        
                        // Se a URL for um checkout ou thank you page e tivermos click_id, adiciona como parâmetro
                        if (variables.click_id && (btnUrl.includes('/oferta/') || btnUrl.includes('/obrigado/'))) {
                            try {
                                // Adiciona protocolo se não existir
                                const urlWithProtocol = btnUrl.startsWith('http') ? btnUrl : `https://${btnUrl}`;
                                const urlObj = new URL(urlWithProtocol);
                                // Remove prefixo '/start ' se existir
                                const cleanClickId = variables.click_id.replace('/start ', '');
                                urlObj.searchParams.set('click_id', cleanClickId);
                                btnUrl = urlObj.toString();
                                
                                // Log apropriado baseado no tipo de URL
                                const urlType = btnUrl.includes('/obrigado/') ? 'thank you page' : 'checkout hospedado';
                                logger.debug(`${logPrefix} [Flow Message] Adicionando click_id ${cleanClickId} ao botão de ${urlType}`);
                            } catch (urlError) {
                                logger.error(`${logPrefix} [Flow Message] Erro ao processar URL: ${urlError.message}`);
                            }
                        }
                        
                        // Converter HTTP para HTTPS (Telegram não aceita HTTP)
                        if (btnUrl.startsWith('http://')) {
                            btnUrl = btnUrl.replace('http://', 'https://');
                        }
                        
                        // Substituir localhost pela URL de produção (se estiver em produção)
                        const FRONTEND_URL = process.env.FRONTEND_URL || 'https://hottrackerbot.netlify.app';
                        if (btnUrl.includes('localhost')) {
                            try {
                                const urlObj = new URL(btnUrl);
                                if (urlObj.hostname === 'localhost' || urlObj.hostname.includes('localhost')) {
                                    const frontendUrlObj = new URL(FRONTEND_URL);
                                    btnUrl = btnUrl.replace(urlObj.origin, frontendUrlObj.origin);
                                }
                            } catch (urlError) {
                                logger.warn(`${logPrefix} [Flow Message] Erro ao substituir localhost na URL: ${urlError.message}`);
                            }
                        }
                        
                        // Marcar checkout_id quando botão de checkout é enviado (para rastreamento)
                        if (btnUrl.includes('/oferta/')) {
                            try {
                                const checkoutId = extractCheckoutIdFromUrl(btnUrl);
                                if (checkoutId && variables.click_id) {
                                    const db_click_id = variables.click_id.startsWith('/start ') 
                                        ? variables.click_id 
                                        : `/start ${variables.click_id}`;
                                    
                                    // Atualizar checkout_id se necessário
                                    await sqlWithRetry(sqlTx`
                                        UPDATE clicks 
                                        SET checkout_id = ${checkoutId}
                                        WHERE click_id = ${db_click_id} 
                                          AND seller_id = ${sellerId}
                                          AND (checkout_id IS NULL OR checkout_id != ${checkoutId})
                                    `);
                                    
                                    // Sempre atualizar checkout_sent_at quando checkout_id corresponde
                                    await sqlWithRetry(sqlTx`
                                        UPDATE clicks 
                                        SET checkout_sent_at = NOW()
                                        WHERE click_id = ${db_click_id} 
                                          AND seller_id = ${sellerId}
                                          AND checkout_id = ${checkoutId}
                                    `);
                                    
                                    // Buscar o click para obter o click_id_internal
                                    const [clickRecord] = await sqlWithRetry(sqlTx`
                                        SELECT id FROM clicks 
                                        WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                                    `);
                                    
                                    if (clickRecord) {
                                        // Criar uma nova transação PIX pendente toda vez que o checkout for enviado
                                        const tempTransactionId = `checkout_${checkoutId}_${clickRecord.id}_${Date.now()}`;
                                        await sqlWithRetry(sqlTx`
                                            INSERT INTO pix_transactions (
                                                click_id_internal,
                                                checkout_id,
                                                pix_value,
                                                status,
                                                provider,
                                                provider_transaction_id,
                                                pix_id,
                                                created_at
                                            ) VALUES (
                                                ${clickRecord.id},
                                                ${String(checkoutId)},
                                                1.00,
                                                'pending',
                                                'checkout',
                                                ${tempTransactionId},
                                                ${tempTransactionId},
                                                NOW()
                                            )
                                        `);
                                        logger.debug(`${logPrefix} [Checkout Button] Transação PIX pendente criada para checkout ${checkoutId}`);
                                    }
                                    
                                    logger.debug(`${logPrefix} [Checkout Button] checkout_id ${checkoutId} e checkout_sent_at marcados no click para rastreamento`);
                                }
                            } catch (checkoutError) {
                                logger.error(`${logPrefix} [Checkout Button] Erro ao marcar checkout_id:`, checkoutError.message);
                                // Não falhar, continuar enviando mensagem
                            }
                        }
                        
                        // Envia com botão inline
                        const payload = { 
                            chat_id: chatId, 
                            text: textToSend, 
                            parse_mode: 'HTML',
                            reply_markup: { 
                                inline_keyboard: [[{ text: btnText, url: btnUrl }]] 
                            }
                        };
                        
                        await sendTelegramRequest(botToken, 'sendMessage', payload, {}, 3, 1500, botId);
                    } else {
                        // Envia mensagem normal sem botão
                        const payload = { chat_id: chatId, text: textToSend, parse_mode: 'HTML' };
                        await sendTelegramRequest(botToken, 'sendMessage', payload, {}, 3, 1500, botId);
                    }
                } catch (error) {
                    logger.error(`${logPrefix} [Flow Message] Erro ao enviar mensagem: ${error.message}`);
                }
            } else if (action.type === 'typing_action') {
                const duration = actionData.durationInSeconds || 1;
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'typing' }, {}, 3, 1500, botId);
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            } else if (['image', 'video', 'audio'].includes(action.type)) {
                let fileId = actionData.fileId || actionData.file_id || actionData.imageUrl || actionData.videoUrl || actionData.audioUrl;
                const caption = await replaceVariables(actionData.caption || '', variables);
                
                // Se tem mediaLibraryId, buscar da biblioteca (ou usar dados do cache se disponíveis)
                let media = null;
                if (actionData.mediaLibraryId && !fileId) {
                    // Verificar se step já tem storageUrl (otimização - dados vêm do cache)
                    if (action.storageUrl && action.storageType === 'r2') {
                        // Usar storageUrl diretamente do step (cache hit - sem query ao banco!)
                        media = {
                            id: actionData.mediaLibraryId,
                            storage_url: action.storageUrl,
                            storage_type: 'r2',
                            migration_status: action.migrationStatus || 'migrated'
                        };
                        fileId = null; // Não precisa buscar file_id
                    } else {
                        // Fallback: buscar do banco (mantém compatibilidade)
                        const [mediaResult] = await sqlWithRetry(
                            'SELECT id, file_id, storage_url, storage_type, migration_status FROM media_library WHERE id = $1 LIMIT 1',
                            [actionData.mediaLibraryId]
                        );
                        if (mediaResult && mediaResult.file_id) {
                            fileId = mediaResult.file_id;
                            media = mediaResult;
                        }
                    }
                } else if (fileId) {
                    // Buscar mídia pelo file_id se não foi buscado por mediaLibraryId
                    const isLibraryFile = fileId && (fileId.startsWith('BAAC') || fileId.startsWith('AgAC') || fileId.startsWith('AwAC'));
                    if (isLibraryFile && sellerId) {
                        const [mediaResult] = await sqlWithRetry(
                            'SELECT id, file_id, storage_url, storage_type, migration_status FROM media_library WHERE file_id = $1 AND seller_id = $2 LIMIT 1',
                            [fileId, sellerId]
                        );
                        if (mediaResult) {
                            media = mediaResult;
                        }
                    }
                }
                
                // Processar envio de mídia (tanto com fileId quanto com media do cache)
                if (fileId || media) {
                    let sent = false;
                    const isLibraryFile = fileId ? (fileId.startsWith('BAAC') || fileId.startsWith('AgAC') || fileId.startsWith('AwAC')) : (media && media.storage_type === 'r2');
                    
                    // Se tem mídia da biblioteca, tentar usar R2
                    if (media && (isLibraryFile || media.storage_type === 'r2')) {
                        // 1. Tentar usar storage_url se já está migrado
                        if (media.storage_url && media.storage_type === 'r2') {
                            try {
                                const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[action.type];
                                const field = { image: 'photo', video: 'video', audio: 'voice' }[action.type];
                                const timeout = action.type === 'video' ? 120000 : 60000;
                                await sendTelegramRequest(botToken, method, { 
                                    chat_id: chatId, 
                                    [field]: media.storage_url, 
                                    caption, 
                                    parse_mode: 'HTML' 
                                }, { timeout });
                                sent = true;
                            } catch (urlError) {
                                console.warn(`[Disparo Media] Erro ao enviar via R2 URL:`, urlError.message);
                            }
                        }
                        
                        // 2. Se não enviou e não está migrado, tentar migrar sob demanda
                        if (!sent && media.storage_type === 'telegram' && media.migration_status !== 'migrated' && sellerId) {
                            try {
                                console.log(`[Disparo Media] Migrando mídia ${media.id} sob demanda...`);
                                await migrateMediaOnDemand(media.id);
                                
                                const [migratedMedia] = await sqlWithRetry(
                                    'SELECT storage_url FROM media_library WHERE id = $1',
                                    [media.id]
                                );
                                
                                if (migratedMedia?.storage_url) {
                                    const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[action.type];
                                    const field = { image: 'photo', video: 'video', audio: 'voice' }[action.type];
                                    const timeout = action.type === 'video' ? 120000 : 60000;
                                    await sendTelegramRequest(botToken, method, { 
                                        chat_id: chatId, 
                                        [field]: migratedMedia.storage_url, 
                                        caption, 
                                        parse_mode: 'HTML' 
                                    }, { timeout });
                                    sent = true;
                                }
                            } catch (migrationError) {
                                console.error(`[Disparo Media] Erro na migração sob demanda:`, migrationError.message);
                            }
                        }
                    }
                    
                    // 3. Fallback: método antigo
                    if (!sent) {
                        if (fileId) {
                            const isLibraryFileFallback = fileId && (fileId.startsWith('BAAC') || fileId.startsWith('AgAC') || fileId.startsWith('AwAC'));
                            if (isLibraryFileFallback) {
                                await sendMediaAsProxy(botToken, chatId, fileId, action.type, caption);
                            } else {
                                const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[action.type];
                                const field = { image: 'photo', video: 'video', audio: 'voice' }[action.type];
                                const timeout = action.type === 'video' ? 120000 : 60000;
                                await sendTelegramRequest(botToken, method, { chat_id: chatId, [field]: fileId, caption, parse_mode: 'HTML' }, { timeout });
                            }
                        } else if (media && media.file_id) {
                            // Se tem media mas não fileId, usar file_id da mídia
                            const isLibraryFileFallback = media.file_id.startsWith('BAAC') || media.file_id.startsWith('AgAC') || media.file_id.startsWith('AwAC');
                            if (isLibraryFileFallback) {
                                await sendMediaAsProxy(botToken, chatId, media.file_id, action.type, caption);
                            } else {
                                const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[action.type];
                                const field = { image: 'photo', video: 'video', audio: 'voice' }[action.type];
                                const timeout = action.type === 'video' ? 120000 : 60000;
                                await sendTelegramRequest(botToken, method, { chat_id: chatId, [field]: media.file_id, caption, parse_mode: 'HTML' }, { timeout });
                            }
                        }
                    }
                }
            } else if (action.type === 'delay') {
                const delaySeconds = actionData.delayInSeconds || 1;
                
                // Se delay > 60s, agendar via QStash para evitar timeout
                if (delaySeconds > 60) {
                    logger.debug(`${logPrefix} [Delay] Delay longo (${delaySeconds}s) detectado. Agendando via QStash...`);
                    
                    // Buscar history_id se não foi passado como parâmetro
                    let historyIdToUse = historyId;
                    if (!historyIdToUse) {
                        const [historyFromLog] = await sqlWithRetry(sqlTx`
                            SELECT history_id 
                            FROM disparo_log
                            WHERE chat_id = ${chatId} AND bot_id = ${botId}
                            ORDER BY created_at DESC LIMIT 1
                        `);
                        if (historyFromLog) {
                            historyIdToUse = historyFromLog.history_id;
                        }
                    }
                    
                    if (historyIdToUse) {
                        try {
                            // Buscar disparo_flow_id do histórico e depois buscar o fluxo
                            const [history] = await sqlWithRetry(sqlTx`
                                SELECT disparo_flow_id
                                FROM disparo_history
                                WHERE id = ${historyIdToUse}
                            `);
                            
                            if (history && history.disparo_flow_id) {
                                const [disparoFlow] = await sqlWithRetry(sqlTx`
                                    SELECT nodes
                                    FROM disparo_flows
                                    WHERE id = ${history.disparo_flow_id}
                                `);
                                
                                if (disparoFlow && disparoFlow.nodes) {
                                    // Agendar continuação do disparo após delay
                                    // Passar currentNodeId para continuar do mesmo nó
                                    const response = await qstashClient.publishJSON({
                                        url: `${process.env.HOTTRACK_API_URL}/api/worker/process-disparo-delay`,
                                        body: {
                                            history_id: historyIdToUse,
                                            chat_id: chatId,
                                            bot_id: botId,
                                            current_node_id: currentNodeId, // Continuar do mesmo nó após processar ações restantes
                                            variables: variables,
                                            remaining_actions: actions.slice(i + 1).length > 0 ? JSON.stringify(actions.slice(i + 1)) : null
                                        },
                                        delay: `${delaySeconds}s`,
                                        contentBasedDeduplication: true,
                                        method: "POST"
                                    });
                                    
                                    logger.debug(`${logPrefix} [Delay] Delay de ${delaySeconds}s agendado via QStash. Tarefa: ${response.messageId}`);
                                    
                                    // Retornar código especial para processDisparoFlow saber que parou
                                    return 'delay_scheduled';
                                }
                            }
                        } catch (error) {
                            logger.error(`${logPrefix} [Delay] Erro ao agendar delay via QStash:`, error.message);
                            // Fallback: processar delay normalmente (limitado a 60s para evitar timeout)
                            await new Promise(resolve => setTimeout(resolve, Math.min(delaySeconds, 60) * 1000));
                        }
                    } else {
                        // Se não encontrou histórico, processar delay inline (limitado)
                        logger.warn(`${logPrefix} [Delay] Histórico não encontrado. Processando delay inline (limitado a 60s).`);
                        await new Promise(resolve => setTimeout(resolve, Math.min(delaySeconds, 60) * 1000));
                    }
                } else {
                    // Delay curto: processar normalmente
                    logger.debug(`${logPrefix} [Delay] Aguardando ${delaySeconds} segundos...`);
                    await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                    logger.debug(`${logPrefix} [Delay] Delay de ${delaySeconds}s concluído.`);
                }
            } else if (action.type === 'action_pix') {
                // Processar PIX - usar a mesma lógica do process-disparo original
                const valueInCents = actionData.valueInCents || 100;
                const pixMessage = await replaceVariables(actionData.pixMessage || '', variables);
                const pixButtonText = actionData.pixButtonText || '📋 Copiar Código';
                
                const [seller] = await sqlWithRetry(sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`);
                if (!seller) {
                    throw new Error('Vendedor não encontrado para gerar PIX.');
                }
                
                // Buscar click do chat (pressel de origem) ou criar novo
                let click = null;
                let clickIdInternal = null;
                
                // Prioridade 1: Usar click do chat que foi buscado no início do fluxo (tem pressel_id de origem)
                if (variables._chatClick) {
                    click = variables._chatClick;
                    clickIdInternal = click.id;
                    logger.debug(`${logPrefix} Usando click do chat com pressel_id=${click.pressel_id || 'null'}`);
                } 
                // Prioridade 2: Buscar click através do click_id das variáveis
                else if (variables.click_id) {
                    const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
                    const [existingClick] = await sqlWithRetry(sqlTx`
                        SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                    `);
                    
                    if (existingClick) {
                        click = existingClick;
                        clickIdInternal = existingClick.id;
                        logger.debug(`${logPrefix} Click encontrado via variables.click_id com pressel_id=${existingClick.pressel_id || 'null'}`);
                    }
                }
                
                // Se não encontrou click, criar um novo para tracking (sem pressel_id)
                if (!click) {
                    const [newClick] = await sqlWithRetry(sqlTx`
                        INSERT INTO clicks (seller_id, bot_id, ip_address, user_agent)
                        VALUES (${sellerId}, ${botId}, NULL, 'Disparo Massivo')
                        RETURNING *
                    `);
                    click = newClick;
                    clickIdInternal = newClick.id;
                    logger.debug(`${logPrefix} Click criado para disparo (sem pressel_id): ${clickIdInternal}`);
                }
                
                // Gerar PIX usando a função existente com click_id_internal
                const pixResult = await generatePixWithFallback(
                    seller,
                    valueInCents,
                    process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost',
                    seller.api_key,
                    null, // ip_address não disponível em disparos
                    clickIdInternal // Passar click_id_internal para tracking no dashboard
                );
                
                if (pixResult && pixResult.qr_code_text) {
                    // Salvar transaction_id para retornar E atualizar variáveis
                    lastPixTransactionId = pixResult.transaction_id;
                    variables.last_transaction_id = pixResult.transaction_id;
                    
                    const pixText = `${pixMessage}\n\n\`\`\`\n${pixResult.qr_code_text}\n\`\`\``;
                    const payload = {
                        chat_id: chatId,
                        text: pixText,
                        parse_mode: 'Markdown',
                        reply_markup: {
                            inline_keyboard: [[{ text: pixButtonText, copy_text: { text: pixResult.qr_code_text } }]]
                        }
                    };
                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', payload);
                    
                    // Enviar eventos apenas se a mensagem foi enviada com sucesso
                    if (sentMessage && sentMessage.ok) {
                        // Buscar a transação completa para os eventos
                        const [transaction] = await sqlWithRetry(sqlTx`
                            SELECT * FROM pix_transactions WHERE id = ${pixResult.internal_transaction_id}
                        `);
                        
                        if (transaction && click) {
                            // Sempre enviar waiting_payment para Utmify (prioridade)
                            const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                            const productDataForUtmify = { id: "prod_disparo", name: "Produto (Disparo Massivo)" };
                            try {
                                // Buscar click completo do banco para garantir que todos os campos UTM estejam presentes
                                const [fullClick] = await sqlWithRetry(sqlTx`
                                    SELECT * FROM clicks WHERE id = ${click.id}
                                `);
                                
                                // Usar o click completo do banco se encontrado, senão usar o original
                                const clickToUse = fullClick || click;
                                
                                await sendEventToUtmifyShared({
                                    status: 'waiting_payment',
                                    clickData: clickToUse,
                                    pixData: { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date(), id: transaction.id },
                                    sellerData: seller,
                                    customerData: customerDataForUtmify,
                                    productData: productDataForUtmify,
                                    sqlTx: sqlTx
                                });
                                logger.debug(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para transação ${transaction.id}`);
                            } catch (utmifyError) {
                                logger.error(`${logPrefix} Erro ao enviar evento para Utmify:`, utmifyError.message);
                            }
                            
                            // Enviar InitiateCheckout para Meta apenas se o click tiver pressel_id (pixel da pressel de origem)
                            if (click.pressel_id) {
                                try {
                                    await sendMetaEventShared({
                                        eventName: 'InitiateCheckout',
                                        clickData: click,
                                        transactionData: { id: transaction.id, pix_value: valueInCents / 100 },
                                        customerData: null,
                                        sqlTx: sqlTx
                                    });
                                    logger.debug(`${logPrefix} Evento 'InitiateCheckout' enviado para Meta (pressel_id=${click.pressel_id}) para transação ${transaction.id}`);
                                } catch (metaError) {
                                    logger.error(`${logPrefix} Erro ao enviar evento para Meta:`, metaError.message);
                                }
                            } else {
                                logger.debug(`${logPrefix} Evento 'InitiateCheckout' não enviado para Meta (click sem pressel_id)`);
                            }
                        }
                    }
                }
            } else if (action.type === 'action_check_pix') {
                // Verificar PIX - sempre busca o último PIX gerado
                try {
                    if (!variables.click_id) {
                        throw new Error("click_id não encontrado nas variáveis do fluxo.");
                    }
                    
                    const db_click_id = variables.click_id.startsWith('/start ') 
                        ? variables.click_id 
                        : `/start ${variables.click_id}`;
                    
                    // Buscar click
                    const [click] = await sqlWithRetry(sqlTx`
                        SELECT id, checkout_id, checkout_sent_at FROM clicks 
                        WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                    `);
                    
                    if (!click) {
                        throw new Error(`Click não encontrado para click_id: ${variables.click_id}`);
                    }
                    
                    let transaction = null;
                    
                    // Se o click tem checkout_id e checkout_sent_at, buscar último PIX gerado (do checkout OU do fluxo após checkout ser enviado)
                    if (click.checkout_id && click.checkout_sent_at) {
                        logger.debug(`${logPrefix} [action_check_pix] Click tem checkout_id ${click.checkout_id} e checkout_sent_at ${click.checkout_sent_at}. Buscando último PIX gerado (checkout ou fluxo após checkout).`);
                        
                        [transaction] = await sqlWithRetry(sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                              AND (
                                checkout_id = ${click.checkout_id} 
                                OR (checkout_id IS NULL AND created_at > ${click.checkout_sent_at})
                              )
                            ORDER BY created_at DESC
                            LIMIT 1
                        `);
                    } else if (click.checkout_id) {
                        // Tem checkout_id mas não tem checkout_sent_at (compatibilidade com dados antigos)
                        logger.debug(`${logPrefix} [action_check_pix] Click tem checkout_id ${click.checkout_id} mas não tem checkout_sent_at. Buscando último PIX gerado (checkout ou fluxo).`);
                        
                        [transaction] = await sqlWithRetry(sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                              AND (checkout_id = ${click.checkout_id} OR checkout_id IS NULL)
                            ORDER BY created_at DESC
                            LIMIT 1
                        `);
                    } else {
                        // Não tem checkout_id, buscar qualquer PIX do click_id
                        logger.debug(`${logPrefix} [action_check_pix] Click não tem checkout_id. Buscando qualquer PIX do click_id.`);
                        
                        [transaction] = await sqlWithRetry(sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                            ORDER BY created_at DESC
                            LIMIT 1
                        `);
                    }
                    
                    if (!transaction) {
                        throw new Error("Nenhuma transação PIX encontrada para este click_id.");
                    }
                    
                    logger.info(`${logPrefix} [action_check_pix] Transação encontrada: ${transaction.provider_transaction_id}, status: ${transaction.status}`);
                    
                    // Verificar status
                    if (transaction.status === 'paid') {
                        logger.debug(`${logPrefix} [action_check_pix] PIX está pago. Retornando 'paid'.`);
                        return 'paid';
                    } else {
                        logger.debug(`${logPrefix} [action_check_pix] PIX está pendente (status: ${transaction.status}). Retornando 'pending'.`);
                        return 'pending';
                    }
                } catch (error) {
                    logger.error(`${logPrefix} [action_check_pix] Erro: ${error.message}`);
                    return 'pending';
                }
            } else {
                logger.warn(`${logPrefix} [${actionIndex}/${actions.length}] Tipo de ação não reconhecido: ${action.type}. Pulando.`);
            }
        } catch (error) {
            // Log detalhado do erro mas continua processando outras ações
            logger.error(`${logPrefix} [${actionIndex}/${actions.length}] Erro ao processar ação ${action.type}:`, error.message);
            logger.debug(`${logPrefix} Stack trace da ação ${action.type}:`, error.stack);
            
            // Apenas interrompe se for erro crítico (ex: bot não encontrado, token inválido)
            const isCriticalError = error.message?.includes('não encontrado') || 
                                   error.message?.includes('not found') ||
                                   error.message?.includes('token') ||
                                   error.message?.includes('Token');
            
            if (isCriticalError) {
                logger.error(`${logPrefix} Erro crítico detectado. Interrompendo processamento de ações.`);
                throw error; // Propaga erro crítico para ser tratado no nível superior
            }
            
            // Para erros não críticos, continua processando outras ações
            logger.warn(`${logPrefix} Erro não crítico na ação ${action.type}. Continuando com próxima ação.`);
        }
    }
    
    const actionProcessingTime = Date.now() - actionStartTime;
    logger.debug(`${logPrefix} Processamento de ${actions.length} ação(ões) concluído em ${actionProcessingTime}ms`);
    
    // Retornar último transaction_id se houver, ou 'completed' se tudo foi processado normalmente
    // O processDisparoFlow vai verificar se o retorno é 'paid', 'pending', 'delay_scheduled' ou transactionId
    return lastPixTransactionId || 'completed';
}

// ==========================================================
//           LÓGICA DO WORKER
// ==========================================================

// Função pura que processa disparo sem depender de objetos HTTP (req/res)
// Permite reutilização em outros contextos (CLI, jobs, filas, etc.)
async function processDisparoData(data) {
    const { history_id, chat_id, bot_id, flow_nodes, flow_edges, start_node_id, variables_json } = data;
    
    // Validação de dados obrigatórios
    if (!flow_nodes || !flow_edges || !start_node_id) {
        throw new Error('Formato inválido. Requer flow_nodes, flow_edges e start_node_id.');
    }
    
    // Parse dos dados JSON
    const flowNodes = JSON.parse(flow_nodes);
    const flowEdges = JSON.parse(flow_edges);
    const userVariables = JSON.parse(variables_json || '{}');
    
    // Buscar bot no banco de dados
    const [bot] = await sqlWithRetry(sqlTx`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`);
    if (!bot || !bot.bot_token) {
        throw new Error(`Bot com ID ${bot_id} não encontrado ou sem token.`);
    }
    
    // Processar fluxo de disparo (lógica real não depende de res)
    await processDisparoFlow(
        chat_id, 
        bot_id, 
        bot.bot_token, 
        bot.seller_id, 
        start_node_id, 
        userVariables, 
        flowNodes, 
        flowEdges, 
        history_id
    );
}

// Handler HTTP para compatibilidade com código existente
async function handler(req, res) {
    // Verificar se requisição foi abortada antes de processar
    if (req.aborted) {
        return res.status(499).end(); // 499 = Client Closed Request
    }
    
    try {
        // Chamar função pura de processamento
        await processDisparoData(req.body);
        
        // Enviar resposta HTTP de sucesso
        if (!res.headersSent) {
            res.status(200).json({ message: 'Disparo processado com sucesso.' });
        }
    } catch (error) {
        logger.error('[WORKER-DISPARO] Erro ao processar disparo:', error);
        
        // Enviar resposta HTTP de erro apropriada
        if (!res.headersSent) {
            const statusCode = error.message.includes('Formato inválido') ? 400 : 500;
            const message = error.message || 'Erro ao processar disparo.';
            res.status(statusCode).json({ message });
        }
    }
}

// Função para processar múltiplos contatos em batch com controle de concorrência
async function processDisparoBatchData(data) {
    const { history_id, contacts, flow_nodes, flow_edges, start_node_id, batch_index = 0, total_batches = 1 } = data;
    
    // Verificar se disparo foi cancelado antes de processar
    try {
        const [disparoStatus] = await sqlWithRetry(
            sqlTx`SELECT status FROM disparo_history WHERE id = ${history_id}`
        );
        
        if (!disparoStatus || disparoStatus.status === 'CANCELLED') {
            logger.info(`[WORKER-DISPARO-BATCH] Disparo ${history_id} foi cancelado. Ignorando batch ${batch_index + 1}/${total_batches}.`);
            return; // Não processar batch
        }
    } catch (error) {
        logger.warn(`[WORKER-DISPARO-BATCH] Erro ao verificar status do disparo (continuando):`, error.message);
    }
    
    // Validação de dados obrigatórios
    if (!flow_nodes || !flow_edges || !start_node_id || !contacts || !Array.isArray(contacts)) {
        throw new Error('Formato inválido. Requer contacts (array), flow_nodes, flow_edges e start_node_id.');
    }
    
    // Parse dos dados JSON
    const flowNodes = JSON.parse(flow_nodes);
    const flowEdges = JSON.parse(flow_edges);
    
    // Buscar todos os bots únicos necessários
    const uniqueBotIds = [...new Set(contacts.map(c => c.bot_id))];
    const bots = await sqlWithRetry(
        sqlTx`SELECT id, seller_id, bot_token FROM telegram_bots WHERE id = ANY(${uniqueBotIds})`
    );
    
    if (bots.length === 0) {
        throw new Error('Nenhum bot encontrado para os IDs fornecidos.');
    }
    
    const botMap = new Map(bots.map(b => [b.id, b]));
    
    // Configurações de concorrência (usar valores padrão se não disponíveis via env)
    const INTERNAL_CONCURRENCY = parseInt(process.env.DISPARO_INTERNAL_CONCURRENCY) || 15;
    const DELAY_BETWEEN_CONTACTS = parseFloat(process.env.DISPARO_DELAY_BETWEEN_MESSAGES) || 0.3;
    
    logger.info(`[WORKER-DISPARO-BATCH] Processando batch ${batch_index + 1}/${total_batches} com ${contacts.length} contatos (concorrência: ${INTERNAL_CONCURRENCY})`);
    
    // Processar contatos com controle de concorrência
    const queue = [...contacts];
    const results = [];
    let processedCount = 0;
    
    const workers = Array(Math.min(INTERNAL_CONCURRENCY, contacts.length)).fill(null).map(async (_, workerId) => {
        while (queue.length > 0) {
            const contact = queue.shift();
            if (!contact) break;
            
            try {
                const bot = botMap.get(contact.bot_id);
                if (!bot || !bot.bot_token) {
                    logger.warn(`[WORKER-DISPARO-BATCH] Bot ${contact.bot_id} não encontrado para contato ${contact.chat_id}`);
                    continue;
                }
                
                // Parse das variáveis do contato
                const userVariables = JSON.parse(contact.variables_json || '{}');
                
                // Aplicar delay mínimo baseado no índice do contato no batch
                const contactIndex = contacts.indexOf(contact);
                if (contactIndex > 0 && DELAY_BETWEEN_CONTACTS > 0) {
                    await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_CONTACTS * 1000));
                }
                
                // Processar fluxo de disparo para este contato
                await processDisparoFlow(
                    contact.chat_id,
                    contact.bot_id,
                    bot.bot_token,
                    bot.seller_id,
                    start_node_id,
                    userVariables,
                    flowNodes,
                    flowEdges,
                    history_id
                );
                
                processedCount++;
                if (processedCount % 50 === 0) {
                    logger.debug(`[WORKER-DISPARO-BATCH] Processados ${processedCount}/${contacts.length} contatos do batch ${batch_index + 1}`);
                }
            } catch (error) {
                logger.error(`[WORKER-DISPARO-BATCH] Erro ao processar contato ${contact.chat_id}:`, error.message);
                // Continuar processando outros contatos mesmo se um falhar
            }
        }
    });
    
    await Promise.all(workers);
    logger.info(`[WORKER-DISPARO-BATCH] Batch ${batch_index + 1}/${total_batches} concluído: ${processedCount}/${contacts.length} contatos processados`);
    
    return { processed: processedCount, total: contacts.length };
}

// Exportar funções para permitir uso em diferentes contextos
module.exports = { handler, processDisparoData, processDisparoBatchData, findNextNode };