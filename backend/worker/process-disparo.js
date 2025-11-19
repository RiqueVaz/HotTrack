// /backend/worker/process-disparo.js
// Este worker não tem a função processFlow, a correção não é aplicável aqui. disparo em massa.

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const { createPixService } = require('../shared/pix');
const logger = require('../logger');
const { sqlTx, sqlWithRetry } = require('../db');
const telegramRateLimiter = require('../shared/telegram-rate-limiter');
const { sendEventToUtmify: sendEventToUtmifyShared, sendMetaEvent: sendMetaEventShared } = require('../shared/event-sender');

const DEFAULT_INVITE_MESSAGE = 'Seu link exclusivo está pronto! Clique no botão abaixo para acessar.';
const DEFAULT_INVITE_BUTTON_TEXT = 'Acessar convite';

// ==========================================================
//                   INICIALIZAÇÃO
// ==========================================================
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
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

async function sendTelegramRequest(botToken, method, data, options = {}, retries = 3, delay = 1500) {
    const { headers = {}, responseType = 'json', timeout = 30000 } = options;
    const apiUrl = `https://api.telegram.org/bot${botToken}/${method}`;
    
    // Aplicar rate limiting proativo antes de fazer a requisição
    const chatId = data?.chat_id || null;
    await telegramRateLimiter.waitIfNeeded(botToken, chatId);
    
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, { headers, responseType, timeout });
            return response.data;
        } catch (error) {
            // FormData do Node.js não tem .get(), então tenta extrair do erro ou deixa undefined
            const errorChatId = data?.chat_id || 'unknown';
            const description = error.response?.data?.description || error.message;
            if (error.response && error.response.status === 403) {
                logger.debug(`[WORKER-DISPARO] Chat ${errorChatId} bloqueou o bot (method ${method}). Ignorando.`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento para erro 429 (Too Many Requests)
            if (error.response && error.response.status === 429) {
                const retryAfter = parseInt(error.response.headers['retry-after'] || error.response.headers['Retry-After'] || '2');
                const waitTime = retryAfter * 1000; // Converter para milissegundos
                
                logger.warn(`[WORKER-DISPARO] Rate limit atingido (429). Aguardando ${retryAfter}s antes de retry. Method: ${method}, ChatID: ${errorChatId}`);
                
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
    const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { file_id: fileId });
    if (!fileInfo.ok) throw new Error('Não foi possível obter informações do arquivo da biblioteca.');
    const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;
    const { data: fileBuffer, headers: fileHeaders } = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    const formData = new FormData();
    formData.append('chat_id', chatId);
    if (caption) {
        formData.append('caption', caption);
        formData.append('parse_mode', 'HTML'); // Adicionado para consistência
    }
    const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
    const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
    const fileNameMap = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' };
    const method = methodMap[fileType];
    const field = fieldMap[fileType];
    const fileName = fileNameMap[fileType];
    const timeout = fileType === 'video' ? 60000 : 30000;
    if (!method) throw new Error('Tipo de arquivo não suportado.');


    formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

    return await sendTelegramRequest(destinationBotToken, method, formData, { headers: formData.getHeaders(), timeout });

  


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
        // Atualizar variáveis com dados do usuário
        try {
            const [user] = await sqlWithRetry(sqlTx`
                SELECT first_name, last_name 
                FROM telegram_chats 
                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
                ORDER BY created_at DESC LIMIT 1`);
            
            if (user) {
                variables.primeiro_nome = user.first_name || '';
                variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
                logger.debug(`${logPrefix} Variáveis do usuário atualizadas: primeiro_nome=${variables.primeiro_nome}, nome_completo=${variables.nome_completo}`);
            }
        } catch (error) {
            // Não interromper o fluxo se falhar ao buscar dados do usuário
            logger.warn(`${logPrefix} Erro ao buscar dados do usuário (não crítico):`, error.message);
        }
        
        // Processar fluxo
        while (currentNodeId && safetyLock < maxIterations) {
            safetyLock++;
            const currentNode = nodeMap.get(currentNodeId); // Busca O(1) ao invés de O(n)
            
            if (!currentNode) {
                logger.warn(`${logPrefix} Nó ${currentNodeId} não encontrado. Encerrando fluxo.`);
                break;
            }
            
            logger.debug(`${logPrefix} Processando nó ${currentNodeId} (tipo: ${currentNode.type}, iteração: ${safetyLock})`);
            
            if (currentNode.type === 'trigger') {
                // Trigger apenas passa para o próximo nó conectado
                currentNodeId = findNextNode(currentNode.id, 'a', flowEdges);
                if (!currentNodeId) {
                    logger.debug(`${logPrefix} Trigger não tem nós conectados. Encerrando fluxo.`);
                    break;
                }
            } else if (currentNode.type === 'action') {
                // Processar ações do nó
                const actions = currentNode.data?.actions || [];
                if (actions.length > 0) {
                    try {
                        const returnedTransactionId = await processDisparoActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix);
                        if (returnedTransactionId) {
                            lastTransactionId = returnedTransactionId;
                            logger.debug(`${logPrefix} Transaction ID atualizado: ${returnedTransactionId}`);
                        }
                    } catch (error) {
                        // Log erro mas continua processando outras ações/nós se possível
                        logger.error(`${logPrefix} Erro ao processar ações do nó ${currentNodeId}:`, error);
                        // Apenas marca como falha se for erro crítico, caso contrário continua
                        const isCriticalError = error.message?.includes('crítico') || error.message?.includes('critical');
                        if (isCriticalError) {
                            logStatus = 'FAILED';
                            logDetails = error.message.substring(0, 255);
                            break;
                        }
                        // Para erros não críticos, continua para o próximo nó
                        logger.warn(`${logPrefix} Erro não crítico no nó ${currentNodeId}, continuando para próximo nó.`);
                    }
                }
                
                // Encontrar próximo nó
                currentNodeId = findNextNode(currentNode.id, 'a', flowEdges);
            } else {
                // Tipo de nó não suportado em disparos
                logger.warn(`${logPrefix} Tipo de nó não suportado em disparos: ${currentNode.type}. Encerrando fluxo.`);
                break;
            }
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
            if (logStatus === 'FAILED') {
                await sqlWithRetry(sqlTx`
                    UPDATE disparo_history 
                    SET processed_jobs = processed_jobs + 1,
                        failure_count = failure_count + 1
                    WHERE id = ${historyId}
                `);
            } else {
                await sqlWithRetry(sqlTx`
                    UPDATE disparo_history 
                    SET processed_jobs = processed_jobs + 1
                    WHERE id = ${historyId}
                `);
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

// Função simplificada para processar ações em disparos
async function processDisparoActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix) {
    let lastPixTransactionId = null; // Rastrear último transaction_id gerado
    const actionStartTime = Date.now();
    logger.debug(`${logPrefix} Processando ${actions.length} ação(ões) para chat ${chatId}`);
    
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
                        
                        // Envia com botão inline
                        const payload = { 
                            chat_id: chatId, 
                            text: textToSend, 
                            parse_mode: 'HTML',
                            reply_markup: { 
                                inline_keyboard: [[{ text: btnText, url: btnUrl }]] 
                            }
                        };
                        
                        await sendTelegramRequest(botToken, 'sendMessage', payload);
                    } else {
                        // Envia mensagem normal sem botão
                        const payload = { chat_id: chatId, text: textToSend, parse_mode: 'HTML' };
                        await sendTelegramRequest(botToken, 'sendMessage', payload);
                    }
                } catch (error) {
                    logger.error(`${logPrefix} [Flow Message] Erro ao enviar mensagem: ${error.message}`);
                }
            } else if (action.type === 'typing_action') {
                const duration = actionData.durationInSeconds || 1;
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'typing' });
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            } else if (['image', 'video', 'audio'].includes(action.type)) {
                let fileId = actionData.fileId || actionData.file_id || actionData.imageUrl || actionData.videoUrl || actionData.audioUrl;
                const caption = await replaceVariables(actionData.caption || '', variables);
                
                // Se tem mediaLibraryId, buscar da biblioteca
                if (actionData.mediaLibraryId && !fileId) {
                    const [media] = await sqlWithRetry(
                        'SELECT file_id FROM media_library WHERE id = $1 LIMIT 1',
                        [actionData.mediaLibraryId]
                    );
                    if (media && media.file_id) {
                        fileId = media.file_id;
                    }
                }
                
                if (fileId) {
                    const isLibraryFile = fileId && (fileId.startsWith('BAAC') || fileId.startsWith('AgAC') || fileId.startsWith('AwAC'));
                    if (isLibraryFile) {
                        await sendMediaAsProxy(botToken, chatId, fileId, action.type, caption);
                    } else {
                        const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[action.type];
                        const field = { image: 'photo', video: 'video', audio: 'voice' }[action.type];
                        await sendTelegramRequest(botToken, method, { chat_id: chatId, [field]: fileId, caption, parse_mode: 'HTML' });
                    }
                }
            } else if (action.type === 'delay') {
                const delaySeconds = actionData.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
            } else if (action.type === 'action_pix') {
                // Processar PIX - usar a mesma lógica do process-disparo original
                const valueInCents = actionData.valueInCents || 100;
                const pixMessage = await replaceVariables(actionData.pixMessage || '', variables);
                const pixButtonText = actionData.pixButtonText || '📋 Copiar Código';
                
                const [seller] = await sqlWithRetry(sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`);
                if (!seller) {
                    throw new Error('Vendedor não encontrado para gerar PIX.');
                }
                
                // Buscar ou criar click para associar a transação
                let click = null;
                let clickIdInternal = null;
                
                if (variables.click_id) {
                    const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
                    const [existingClick] = await sqlWithRetry(sqlTx`
                        SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                    `);
                    
                    if (existingClick) {
                        click = existingClick;
                        clickIdInternal = existingClick.id;
                    }
                }
                
                // Se não encontrou click, criar um novo para tracking
                if (!click) {
                    const [newClick] = await sqlWithRetry(sqlTx`
                        INSERT INTO clicks (seller_id, bot_id, ip_address, user_agent)
                        VALUES (${sellerId}, ${botId}, NULL, 'Disparo Massivo')
                        RETURNING *
                    `);
                    click = newClick;
                    clickIdInternal = newClick.id;
                    logger.debug(`${logPrefix} Click criado para disparo: ${clickIdInternal}`);
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
                    // Salvar transaction_id para retornar
                    lastPixTransactionId = pixResult.transaction_id;
                    
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
                            // Enviar InitiateCheckout para Meta se o click veio de pressel ou checkout
                            if (click.pressel_id || click.checkout_id) {
                                await sendMetaEventShared({
                                    eventName: 'InitiateCheckout',
                                    clickData: click,
                                    transactionData: { id: transaction.id, pix_value: valueInCents / 100 },
                                    customerData: null,
                                    sqlTx: sqlTx
                                });
                                logger.debug(`${logPrefix} Evento 'InitiateCheckout' enviado para Meta para transação ${transaction.id}`);
                            }
                            
                            // Enviar waiting_payment para Utmify
                            const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                            const productDataForUtmify = { id: "prod_disparo", name: "Produto (Disparo Massivo)" };
                            await sendEventToUtmifyShared({
                                status: 'waiting_payment',
                                clickData: click,
                                pixData: { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date(), id: transaction.id },
                                sellerData: seller,
                                customerData: customerDataForUtmify,
                                productData: productDataForUtmify,
                                sqlTx: sqlTx
                            });
                            logger.debug(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para transação ${transaction.id}`);
                        }
                    }
                }
            } else if (action.type === 'action_check_pix') {
                // Verificar PIX - lógica simplificada (não implementada completamente para disparos)
                logger.debug(`${logPrefix} [${actionIndex}/${actions.length}] Ação check_pix não suportada em disparos. Pulando.`);
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
    
    return lastPixTransactionId; // Retornar último transaction_id ou null
}

// ==========================================================
//           LÓGICA DO WORKER
// ==========================================================

async function handler(req, res) {
    // Verificar se requisição foi abortada antes de processar
    if (req.aborted) {
        return res.status(499).end(); // 499 = Client Closed Request
    }
    
    const { history_id, chat_id, bot_id, flow_nodes, flow_edges, start_node_id, variables_json } = req.body;
    
    if (!flow_nodes || !flow_edges || !start_node_id) {
        return res.status(400).json({ message: 'Formato inválido. Requer flow_nodes, flow_edges e start_node_id.' });
    }
    
    try {
        const flowNodes = JSON.parse(flow_nodes);
        const flowEdges = JSON.parse(flow_edges);
        const userVariables = JSON.parse(variables_json || '{}');
        
        const [bot] = await sqlWithRetry(sqlTx`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`);
        if (!bot || !bot.bot_token) {
            throw new Error(`Bot com ID ${bot_id} não encontrado ou sem token.`);
        }
        
        await processDisparoFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, start_node_id, userVariables, flowNodes, flowEdges, history_id);
        
        if (!res.headersSent) {
            res.status(200).json({ message: 'Disparo processado com sucesso.' });
        }
    } catch (error) {
        logger.error('[WORKER-DISPARO] Erro ao processar disparo:', error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Erro ao processar disparo.' });
        }
    }
}

module.exports = handler;