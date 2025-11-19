// /backend/worker/process-timeout.js

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const { Client } = require("@upstash/qstash");
const crypto = require('crypto');
const { createPixService } = require('../shared/pix');
const { sqlTx, sqlWithRetry } = require('../db');
const { handleSuccessfulPayment: handleSuccessfulPaymentShared } = require('../shared/payment-handler');
const { sendEventToUtmify: sendEventToUtmifyShared, sendMetaEvent: sendMetaEventShared } = require('../shared/event-sender');
const telegramRateLimiter = require('../shared/telegram-rate-limiter');

const DEFAULT_INVITE_MESSAGE = 'Seu link exclusivo está pronto! Clique no botão abaixo para acessar.';
const DEFAULT_INVITE_BUTTON_TEXT = 'Acessar convite';



// ==========================================================
//                     INICIALIZAÇÃO
// ==========================================================
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
// Cache para respeitar rate limit da PushinPay (1/min por transação)
const pushinpayLastCheckAt = new Map();
const wiinpayLastCheckAt = new Map();
const paradiseLastCheckAt = new Map();
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const WIINPAY_SPLIT_USER_ID = process.env.WIINPAY_SPLIT_USER_ID;


const qstashClient = new Client({ // <-- ADICIONE ESTE BLOCO
     token: process.env.QSTASH_TOKEN,
    });

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
//    FUNÇÕES AUXILIARES COMPLETAS PARA AUTONOMIA DO WORKER
// ==========================================================

function getSellerWiinpayApiKey(seller) {
    if (!seller) return null;
    return seller.wiinpay_api_key || seller.wiinpay_token || seller.wiinpay_key || null;
}

function extractWiinpayCustomer(payment) {
    const payer = payment?.payer || payment?.customer || payment?.buyer || {};
    return {
        name: payer?.name || payer?.full_name || payer?.nome || '',
        document: payer?.document || payer?.cpf || payer?.cnpj || payer?.tax_id || '',
        email: payer?.email || payer?.email_address || '',
        phone: payer?.phone || payer?.phone_number || payer?.telefone || ''
    };
}

function parseWiinpayPayment(rawData) {
    if (!rawData) {
        return { id: null, status: null, customer: {} };
    }
    const payment =
        rawData.payment ||
        rawData.data ||
        rawData.payload ||
        rawData.transaction ||
        rawData;

    const id =
        payment?.id ||
        payment?.payment_id ||
        payment?.paymentId ||
        payment?.transaction_id ||
        payment?.transactionId ||
        rawData.payment_id ||
        rawData.paymentId ||
        rawData.id;

    const status = String(
        payment?.status ||
        rawData.status ||
        rawData.payment_status ||
        payment?.payment_status ||
        ''
    ).toLowerCase();

    return {
        id: id || null,
        status,
        customer: extractWiinpayCustomer(payment || rawData || {})
    };
}

async function getWiinpayPaymentStatus(paymentId, apiKey) {
    if (!apiKey) {
        throw new Error('Credenciais da WiinPay não configuradas.');
    }
    const response = await axios.get(`https://api-v2.wiinpay.com.br/payment/list/${paymentId}`, {
        headers: {
            Accept: 'application/json',
            Authorization: `Bearer ${apiKey}`
        }
    });

    const data = Array.isArray(response.data) ? response.data[0] : response.data;
    return parseWiinpayPayment(data);
}

async function getParadisePaymentStatus(transactionId, secretKey) {
    if (!secretKey) {
        throw new Error('Credenciais da Paradise não configuradas.');
    }
    
    try {
        const response = await axios.get(`https://multi.paradisepags.com/api/v1/query.php?action=get_transaction&id=${transactionId}`, {
            headers: {
                'X-API-Key': secretKey,
                'Content-Type': 'application/json',
            },
        });

        const data = response.data;
        const status = String(data?.status || '').toLowerCase();
        const customerData = data?.customer_data?.customer || {};

        return {
            status,
            customer: {
                name: customerData?.name || '',
                document: customerData?.document || '',
                email: customerData?.email || '',
                phone: customerData?.phone || '',
            },
        };
    } catch (error) {
        const errorMessage = error.response?.data?.message || error.response?.data?.error || error.message;
        console.error(`[Paradise Status] Erro ao consultar transação ${transactionId}:`, errorMessage);
        throw new Error(`Erro ao consultar status na Paradise: ${errorMessage || 'Erro desconhecido'}`);
    }
}

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
            const errorChatId = data?.chat_id || 'unknown';
            
            // Tratamento para erro 403 (bot bloqueado)
            if (error.response && error.response.status === 403) {
                console.warn(`[WORKER - Telegram API] O bot foi bloqueado pelo usuário. ChatID: ${errorChatId}`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento para erro 429 (Too Many Requests)
            if (error.response && error.response.status === 429) {
                const retryAfter = parseInt(error.response.headers['retry-after'] || error.response.headers['Retry-After'] || '2');
                const waitTime = retryAfter * 1000; // Converter para milissegundos
                
                console.warn(`[WORKER - Telegram API] Rate limit atingido (429). Aguardando ${retryAfter}s antes de retry. Method: ${method}, ChatID: ${errorChatId}`);
                
                if (i < retries - 1) {
                    await new Promise(res => setTimeout(res, waitTime));
                    continue; // Tentar novamente após esperar
                } else {
                    // Se esgotou as tentativas, retornar erro
                    console.error(`[WORKER - Telegram API ERROR] Rate limit persistente após ${retries} tentativas. Method: ${method}`);
                    return { ok: false, error_code: 429, description: 'Too Many Requests: Rate limit exceeded' };
                }
            }

            // Tratamento específico para TOPIC_CLOSED
            if (error.response && error.response.status === 400 && 
                error.response.data?.description?.includes('TOPIC_CLOSED')) {
                console.warn(`[WORKER - Telegram API] Chat de grupo fechado. ChatID: ${errorChatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }

            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');
            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }

            console.error(`[WORKER - Telegram API ERROR] Method: ${method}, ChatID: ${errorChatId}:`, error.response?.data || error.message);
            if (i < retries - 1) { 
                await new Promise(res => setTimeout(res, delay * (i + 1))); 
            } else { 
                throw error; 
            }
        }
    }
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

async function handleMediaNode(node, botToken, chatId, caption) {
    const type = node.type;
    const nodeData = node.data || {};
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    const fileIdentifier = nodeData[urlMap[type]];

    if (!fileIdentifier) {
        console.warn(`[Flow Media] Nenhum file_id ou URL fornecido para o nó de ${type} ${node.id}`);
        return null;
    }

    const isLibraryFile = fileIdentifier.startsWith('BAAC') || fileIdentifier.startsWith('AgAC') || fileIdentifier.startsWith('AwAC');
    let response;
    const timeout = type === 'video' ? 60000 : 30000;

    if (isLibraryFile) {
        if (type === 'audio') {
            const duration = parseInt(nodeData.durationInSeconds, 10) || 0;
            if (duration > 0) {
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'record_voice' });
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            }
        }
        response = await sendMediaAsProxy(botToken, chatId, fileIdentifier, type, caption);
    } else {
        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
        
        const method = methodMap[type];
        const field = fieldMap[type];
        
        const payload = { chat_id: chatId, [field]: fileIdentifier, caption };
        response = await sendTelegramRequest(botToken, method, payload, { timeout });
    }
    
    return response;
}

async function saveMessageToDb(sellerId, botId, message, senderType) {
    const { message_id, chat, from, text, photo, video, voice, reply_markup } = message;
    let mediaType = null;
    let mediaFileId = null;
    let messageText = text;
    let newClickId = null;

    if (text && text.startsWith('/start ')) {
        newClickId = text.substring(7);
    }

    let finalClickId = newClickId;
    if (!finalClickId) {
        const result = await sqlWithRetry(
            'SELECT click_id FROM telegram_chats WHERE chat_id = $1 AND bot_id = $2 AND click_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
            [chat.id, botId]
        );
        if (result.length > 0) {
            finalClickId = result[0].click_id;
        }
    }

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
    const botInfo = senderType === 'bot' ? { first_name: 'Bot', last_name: '(Automação)' } : {};
    const fromUser = from || chat;

    // Extrai reply_markup se existir
    const replyMarkupJson = reply_markup ? JSON.stringify(reply_markup) : null;

    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id, reply_markup)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET reply_markup = EXCLUDED.reply_markup;
    `, [sellerId, botId, chat.id, message_id, fromUser.id, fromUser.first_name || botInfo.first_name, fromUser.last_name || botInfo.last_name, fromUser.username || null, messageText, senderType, mediaType, mediaFileId, finalClickId, replyMarkupJson]);

    if (newClickId) {
        await sqlWithRetry(
            'UPDATE telegram_chats SET click_id = $1 WHERE chat_id = $2 AND bot_id = $3',
            [newClickId, chat.id, botId]
        );
    }
}

// As funções compartilhadas são chamadas diretamente com objetos

// Wrapper para handleSuccessfulPayment que passa as dependências necessárias
// Worker não tem adminSubscription nem webpush, então passa null
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

function findNextNode(currentNodeId, handleId, edges) {
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null;
}

async function showTypingForDuration(chatId, botToken, durationMs) {
    const endTime = Date.now() + durationMs;
    while (Date.now() < endTime) {
        await sendTypingAction(chatId, botToken);
        const remaining = endTime - Date.now();
        const wait = Math.min(5000, remaining); // envia a cada 5s ou menos se acabar o tempo
        await new Promise(resolve => setTimeout(resolve, wait));
    }
}

async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, { chat_id: chatId, action: 'typing' });
    } catch (error) {
        const errorData = error.response?.data;
        const description = errorData?.description || error.message;
        if (description?.includes('bot was blocked by the user')) {
            console.debug(`[WORKER - Flow Engine] Chat ${chatId} bloqueou o bot (typing). Ignorando.`);
            return;
        }
        console.warn(`[WORKER - Flow Engine] Falha ao enviar ação 'typing':`, errorData || error.message);
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping, typingDelay = 0, variables = {}) {
  if (!text || text.trim() === '') return;
  try {
        if (showTyping) {
            // Use o delay definido no frontend (convertido para ms), ou um fallback se não for definido
            let typingDurationMs = (typingDelay && typingDelay > 0) 
                ? (typingDelay * 1000) 
                : Math.max(500, Math.min(2000, text.length * 50));
            await showTypingForDuration(chatId, botToken, typingDurationMs);
        }
        const response = await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        if (response.data.ok) {
            const sentMessage = response.data.result;
            await sqlTx`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, click_id)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, NULL, NULL, NULL, ${text}, 'bot', ${variables.click_id || null})
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        const errorData = error.response?.data;
        const description = errorData?.description || error.message;
        if (description?.includes('bot was blocked by the user')) {
            console.debug(`[WORKER - Flow Engine] Chat ${chatId} bloqueou o bot (message). Ignorando.`);
            return;
        }
        console.error(`[WORKER - Flow Engine] Erro ao enviar/salvar mensagem:`, errorData || error.message);
    }
}

// /backend/worker/process-timeout.js (Função processActions CORRIGIDA)

/**
 * =================================================================
 * FUNÇÃO 'processActions' (O EXECUTOR) - VERSÃO NOVA
 * =================================================================
 * (Colada da sua resposta anterior)
 */
async function processActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix = '[Actions]', currentNodeId = null, flowId = null, flowNodes = null, flowEdges = null) {
    console.log(`${logPrefix} Iniciando processamento de ${actions.length} ações aninhadas para chat ${chatId}`);
    
    for (let i = 0; i < actions.length; i++) {
        const action = actions[i];
        const actionData = action.data || {};

        switch (action.type) {
            case 'message':
                try {
                    let textToSend = await replaceVariables(actionData.text, variables);
                    
                    // Validação do tamanho do texto (limite do Telegram: 4096 caracteres)
                    if (textToSend.length > 4096) {
                        console.warn(`${logPrefix} [Flow Message] Texto excede limite de 4096 caracteres. Truncando...`);
                        textToSend = textToSend.substring(0, 4093) + '...';
                    }
                    
                    // Verifica se tem botão para anexar
                    if (actionData.buttonText && actionData.buttonUrl) {
                        const btnText = await replaceVariables(actionData.buttonText, variables);
                        let btnUrl = await replaceVariables(actionData.buttonUrl, variables);
                        
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
                                console.warn(`${logPrefix} [Flow Message] Erro ao substituir localhost na URL: ${urlError.message}`);
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
                        
                        const response = await sendTelegramRequest(botToken, 'sendMessage', payload);
                        if (response && response.ok) {
                            await saveMessageToDb(sellerId, botId, response.result, 'bot');
                        }
                    } else {
                        // Envia mensagem normal sem botão
                        await sendMessage(chatId, textToSend, botToken, sellerId, botId, false, variables);
                    }
                } catch (error) {
                    console.error(`${logPrefix} [Flow Message] Erro ao enviar mensagem: ${error.message}`);
                } 
                break;

            case 'image':
            case 'video':
            case 'audio': {
                try {
                    let caption = await replaceVariables(actionData.caption, variables);
                    
                    // Validação do tamanho da legenda (limite do Telegram: 1024 caracteres)
                    if (caption && caption.length > 1024) {
                        console.warn(`${logPrefix} [Flow Media] Legenda excede limite de 1024 caracteres. Truncando...`);
                        caption = caption.substring(0, 1021) + '...';
                    }
                    
                    const response = await handleMediaNode(action, botToken, chatId, caption);

                    if (response && response.ok) {
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    console.error(`${logPrefix} [Flow Media] Erro ao enviar mídia (ação ${action.type}) para o chat ${chatId}: ${e.message}`);
                }
                break;
            }

            case 'delay':
                const delaySeconds = actionData.delayInSeconds || 1;
                
                // Se o delay for maior que 60 segundos, agendar via QStash
                if (delaySeconds > 60) {
                    console.log(`${logPrefix} [Delay] Delay longo detectado (${delaySeconds}s). Agendando via QStash...`);
                    
                    // Usar variáveis locais para poder modificar os valores
                    let resolvedCurrentNodeId = currentNodeId;
                    let resolvedFlowId = flowId;
                    let resolvedFlowNodes = flowNodes;
                    let resolvedFlowEdges = flowEdges;
                    
                    // Verificar se já temos currentNodeId e flowId (necessários para agendar)
                    if (!resolvedCurrentNodeId) {
                        // Se não temos currentNodeId, buscar do estado atual
                        const [currentState] = await sqlWithRetry(sqlTx`
                            SELECT current_node_id, flow_id 
                            FROM user_flow_states 
                            WHERE chat_id = ${chatId} AND bot_id = ${botId}
                        `);
                        if (currentState) {
                            resolvedCurrentNodeId = currentState.current_node_id;
                            resolvedFlowId = currentState.flow_id;
                        }
                    }
                    
                    if (!resolvedCurrentNodeId) {
                        console.error(`${logPrefix} [Delay] Não foi possível determinar currentNodeId. Processando delay normalmente.`);
                        await new Promise(resolve => setTimeout(resolve, Math.min(delaySeconds, 60) * 1000));
                        break;
                    }
                    
                    // Buscar flowNodes e flowEdges se necessário
                    if (!resolvedFlowNodes || !resolvedFlowEdges) {
                        if (resolvedFlowId) {
                            const [flow] = await sqlWithRetry(sqlTx`
                                SELECT nodes FROM flows WHERE id = ${resolvedFlowId}
                            `);
                            if (flow && flow.nodes) {
                                const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
                                resolvedFlowNodes = flowData.nodes || [];
                                resolvedFlowEdges = flowData.edges || [];
                            }
                        }
                    }
                    
                    // Salvar estado atual antes de agendar
                    await sqlWithRetry(sqlTx`
                        UPDATE user_flow_states 
                        SET variables = ${JSON.stringify(variables)},
                            current_node_id = ${resolvedCurrentNodeId},
                            flow_id = ${resolvedFlowId}
                        WHERE chat_id = ${chatId} AND bot_id = ${botId}
                    `);
                    
                    // Agendar continuação após o delay
                    try {
                        const remainingActions = actions.slice(i + 1); // Ações restantes após o delay
                        const response = await qstashClient.publishJSON({
                            url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                            body: {
                                chat_id: chatId,
                                bot_id: botId,
                                target_node_id: resolvedCurrentNodeId, // Continuar do mesmo nó
                                variables: variables,
                                continue_from_delay: true, // Flag para indicar que é continuação após delay
                                remaining_actions: remainingActions.length > 0 ? JSON.stringify(remainingActions) : null
                            },
                            delay: `${delaySeconds}s`,
                            contentBasedDeduplication: true,
                            method: "POST"
                        });
                        
                        // Salvar scheduled_message_id no estado
                        await sqlWithRetry(sqlTx`
                            UPDATE user_flow_states 
                            SET scheduled_message_id = ${response.messageId}
                            WHERE chat_id = ${chatId} AND bot_id = ${botId}
                        `);
                        
                        console.log(`${logPrefix} [Delay] Delay de ${delaySeconds}s agendado via QStash. Tarefa: ${response.messageId}`);
                        
                        // Retornar código especial para processFlow saber que parou
                        return 'delay_scheduled';
                    } catch (error) {
                        console.error(`${logPrefix} [Delay] Erro ao agendar delay via QStash:`, error.message);
                        // Fallback: processar delay normalmente (limitado a 60s para evitar timeout)
                        await new Promise(resolve => setTimeout(resolve, Math.min(delaySeconds, 60) * 1000));
                    }
                } else {
                    // Delay curto: processar normalmente
                    await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                }
                break;
            
            case 'typing_action':
                if (actionData.durationInSeconds && actionData.durationInSeconds > 0) {
                    await showTypingForDuration(chatId, botToken, actionData.durationInSeconds * 1000);
                }
                break;

            case 'action_create_invite_link':
                try {
                    console.log(`${logPrefix} Executando action_create_invite_link para chat ${chatId}`);

                    const [botInvite] = await sqlTx`
                        SELECT telegram_supergroup_id
                        FROM telegram_bots
                        WHERE id = ${botId}
                    `;

                    if (!botInvite?.telegram_supergroup_id) {
                        throw new Error('Supergrupo não configurado para este bot');
                    }

                    const normalizedChatId = normalizeChatIdentifier(botInvite.telegram_supergroup_id);
                    if (!normalizedChatId) {
                        throw new Error('ID do supergrupo inválido para criação de convite');
                    }

                    const userToUnban = actionData.userId || chatId;
                    const normalizedUserId = normalizeChatIdentifier(userToUnban);
                    try {
                        const unbanResponse = await sendTelegramRequest(
                            botToken,
                            'unbanChatMember',
                            {
                                chat_id: normalizedChatId,
                                user_id: normalizedUserId,
                                only_if_banned: true
                            }
                        );
                        if (unbanResponse?.ok) {
                            console.log(`${logPrefix} Usuário ${userToUnban} desbanido antes da criação do convite.`);
                        } else if (unbanResponse && !unbanResponse.ok) {
                            const desc = (unbanResponse.description || '').toLowerCase();
                            if (desc.includes("can't remove chat owner")) {
                                console.info(`${logPrefix} Tentativa de desbanir o proprietário do grupo ignorada.`);
                            } else {
                                console.warn(`${logPrefix} Não foi possível desbanir usuário ${userToUnban}: ${unbanResponse.description}`);
                            }
                        }
                    } catch (unbanError) {
                        const message = (unbanError?.message || '').toLowerCase();
                        if (message.includes("can't remove chat owner")) {
                            console.info(`${logPrefix} Tentativa de desbanir o proprietário do grupo ignorada.`);
                        } else {
                            console.warn(`${logPrefix} Erro ao tentar desbanir usuário ${userToUnban}:`, unbanError.message);
                        }
                    }

                    const expireDate = actionData.expireMinutes
                        ? Math.floor(Date.now() / 1000) + (actionData.expireMinutes * 60)
                        : undefined;

                    const inviteNameRaw = (actionData.linkName || `Convite_${chatId}_${Date.now()}`).toString().trim();
                    const inviteName = inviteNameRaw ? inviteNameRaw.slice(0, 32) : `Convite_${Date.now()}`;

                    const invitePayload = {
                        chat_id: normalizedChatId,
                        name: inviteName,
                        member_limit: 1,
                        creates_join_request: false
                    };

                    if (expireDate) {
                        invitePayload.expire_date = expireDate;
                    }

                    const inviteResponse = await sendTelegramRequest(
                        botToken,
                        'createChatInviteLink',
                        invitePayload
                    );

                    if (inviteResponse.ok) {
                        variables.invite_link = inviteResponse.result.invite_link;
                        variables.invite_link_name = inviteResponse.result.name;
                        variables.invite_link_single_use = true;
                        variables.user_was_banned = false;
                        variables.banned_user_id = undefined;

                        const buttonText = (actionData.buttonText || DEFAULT_INVITE_BUTTON_TEXT).trim() || DEFAULT_INVITE_BUTTON_TEXT;
                        const template = (actionData.messageText || actionData.text || DEFAULT_INVITE_MESSAGE).trim() || DEFAULT_INVITE_MESSAGE;
                        const messageText = await replaceVariables(template, variables);

                        const payload = {
                            chat_id: chatId,
                            text: messageText,
                            parse_mode: 'HTML',
                            reply_markup: {
                                inline_keyboard: [[{ text: buttonText, url: inviteResponse.result.invite_link }]]
                            }
                        };

                        const messageResponse = await sendTelegramRequest(botToken, 'sendMessage', payload);
                        if (messageResponse?.ok) {
                            await saveMessageToDb(sellerId, botId, messageResponse.result, 'bot');
                        } else {
                            throw new Error(messageResponse?.description || 'Falha ao enviar mensagem do convite.');
                        }

                        console.log(`${logPrefix} Link de convite criado com sucesso: ${inviteResponse.result.invite_link}`);
                    } else {
                        throw new Error(`Falha ao criar link de convite: ${inviteResponse.description}`);
                    }
                } catch (error) {
                    console.error(`${logPrefix} Erro ao criar link de convite:`, error.message);
                    throw error;
                }
                break;

            case 'action_remove_user_from_group':
                try {
                    console.log(`${logPrefix} Executando action_remove_user_from_group para chat ${chatId}`);

                    const [bot] = await sqlTx`
                        SELECT telegram_supergroup_id
                        FROM telegram_bots
                        WHERE id = ${botId}
                    `;

                    if (!bot?.telegram_supergroup_id) {
                        throw new Error('Supergrupo não configurado para este bot');
                    }

                    const normalizedChatId = normalizeChatIdentifier(bot.telegram_supergroup_id);
                    if (!normalizedChatId) {
                        throw new Error('ID do supergrupo inválido para banimento');
                    }

                    const handleOwnerBanRestriction = () => {
                        console.info(`${logPrefix} Tentativa de banir o proprietário do grupo ignorada.`);
                        variables.user_was_banned = false;
                        variables.banned_user_id = undefined;
                    };

                    const userToRemove = actionData.userId || chatId;
                    const normalizedUserId = normalizeChatIdentifier(userToRemove);

                    let banResponse;
                    try {
                        banResponse = await sendTelegramRequest(
                            botToken,
                            'banChatMember',
                            {
                                chat_id: normalizedChatId,
                                user_id: normalizedUserId,
                                revoke_messages: actionData.deleteMessages || false
                            }
                        );
                    } catch (banError) {
                        const errorDesc =
                            banError?.response?.data?.description ||
                            banError?.description ||
                            banError?.message ||
                            '';
                        if (errorDesc.toLowerCase().includes("can't remove chat owner")) {
                            handleOwnerBanRestriction();
                            break;
                        }
                        console.error(`${logPrefix} Erro ao remover usuário do grupo:`, banError.message);
                        throw banError;
                    }

                    if (banResponse.ok) {
                        console.log(`${logPrefix} Usuário ${userToRemove} removido e banido do grupo`);
                        variables.user_was_banned = true;
                        variables.banned_user_id = userToRemove;
                        variables.last_ban_at = new Date().toISOString();

                        const linkToRevoke = actionData.inviteLink || variables.invite_link;
                        if (linkToRevoke) {
                            try {
                                const revokeResponse = await sendTelegramRequest(
                                    botToken,
                                    'revokeChatInviteLink',
                                    {
                                        chat_id: normalizedChatId,
                                        invite_link: linkToRevoke
                                    }
                                );
                                if (revokeResponse.ok) {
                                    console.log(`${logPrefix} Link de convite revogado após banimento: ${linkToRevoke}`);
                                    variables.invite_link_revoked = true;
                                    delete variables.invite_link;
                                    delete variables.invite_link_name;
                                } else {
                                    console.warn(`${logPrefix} Falha ao revogar link ${linkToRevoke}: ${revokeResponse.description}`);
                                }
                            } catch (revokeError) {
                                console.warn(`${logPrefix} Erro ao tentar revogar link ${linkToRevoke}:`, revokeError.message);
                            }
                        }

                        if (actionData.sendMessage) {
                            const messageText = await replaceVariables(
                                actionData.messageText || 'Você foi removido do grupo.',
                                variables
                            );
                            await sendMessage(chatId, messageText, botToken, sellerId, botId, false, variables);
                        }
                    } else {
                        const desc =
                            (banResponse?.description ||
                                banResponse?.result?.description ||
                                '').
                                toLowerCase();
                        if (desc.includes("can't remove chat owner")) {
                            handleOwnerBanRestriction();
                        } else {
                            throw new Error(`Falha ao remover usuário: ${banResponse.description}`);
                        }
                    }
                } catch (error) {
                    const message = (error?.response?.data?.description ||
                        error?.description ||
                        error?.message ||
                        '').toLowerCase();
                    if (message.includes("can't remove chat owner")) {
                        console.info(`${logPrefix} Tentativa de banir o proprietário do grupo ignorada.`);
                        variables.user_was_banned = false;
                        variables.banned_user_id = undefined;
                    } else {
                        console.error(`${logPrefix} Erro ao remover usuário do grupo:`, error.message);
                        throw error;
                    }
                }
                break;
            
            case 'action_pix':
                try {
                    console.log(`${logPrefix} Executando action_pix para chat ${chatId}`);
                    const valueInCents = actionData.valueInCents;
                    if (!valueInCents) throw new Error("Valor do PIX não definido na ação do fluxo.");
    
                    const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    if (!seller) throw new Error(`${logPrefix} Vendedor ${sellerId} não encontrado.`);
    
                    let click_id_from_vars = variables.click_id;
                    if (!click_id_from_vars) {
                        const [recentClick] = await sqlTx`
                            SELECT click_id FROM telegram_chats 
                            WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL 
                            ORDER BY created_at DESC LIMIT 1`;
                        if (recentClick?.click_id) {
                            click_id_from_vars = recentClick.click_id;
                        }
                    }
    
                    if (!click_id_from_vars) {
                        throw new Error(`${logPrefix} Click ID não encontrado para gerar PIX.`);
                    }
    
                    const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                    const [click] = await sqlTx`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error(`${logPrefix} Click ID não encontrado para este vendedor.`);
    
                    const ip_address = click.ip_address;
                    const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
                    
                    // Gera PIX e salva no banco
                    const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);
                    console.log(`${logPrefix} PIX gerado com sucesso. Transaction ID: ${pixResult.transaction_id}`);
                    
                    // Atualiza as variáveis do fluxo (IMPORTANTE)
                    variables.last_transaction_id = pixResult.transaction_id;
    
                    let messageText = await replaceVariables(actionData.pixMessageText || "", variables);
                    
                    // Validação do tamanho do texto da mensagem do PIX (limite de 1024 caracteres)
                    if (messageText && messageText.length > 1024) {
                        console.warn(`${logPrefix} [PIX] Texto da mensagem excede limite de 1024 caracteres. Truncando...`);
                        messageText = messageText.substring(0, 1021) + '...';
                    }
                    
                    const buttonText = await replaceVariables(actionData.pixButtonText || "📋 Copiar", variables);
                    const pixToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                    // CRÍTICO: Tenta enviar o PIX para o usuário
                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                        chat_id: chatId, text: pixToSend, parse_mode: 'HTML',
                        reply_markup: { inline_keyboard: [[{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]] }
                    });
    
                    // Verifica se o envio foi bem-sucedido
                    if (!sentMessage.ok) {
                        // Cancela a transação PIX no banco se não conseguiu enviar ao usuário
                        console.error(`${logPrefix} FALHA ao enviar PIX. Cancelando transação ${pixResult.transaction_id}. Motivo: ${sentMessage.description || 'Desconhecido'}`);
                        
                        await sqlTx`
                            UPDATE pix_transactions 
                            SET status = 'canceled' 
                            WHERE provider_transaction_id = ${pixResult.transaction_id}
                        `;
                        
                        throw new Error(`Não foi possível enviar PIX ao usuário. Motivo: ${sentMessage.description || 'Erro desconhecido'}. Transação cancelada.`);
                    }
                    
                    // Salva a mensagem no banco
                    await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot');
                    console.log(`${logPrefix} PIX enviado com sucesso ao usuário ${chatId}`);
                    
                    // Envia eventos para Utmify e Meta SOMENTE APÓS confirmação de entrega ao usuário
                    const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                    const productDataForUtmify = { id: "prod_bot", name: "Produto (Fluxo Bot)" };
                    await sendEventToUtmifyShared({
                        status: 'waiting_payment',
                        clickData: click,
                        pixData: { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date(), id: pixResult.internal_transaction_id },
                        sellerData: seller,
                        customerData: customerDataForUtmify,
                        productData: productDataForUtmify,
                        sqlTx: sqlTx
                    });
                    console.log(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para o clique ${click.id}.`);

                    console.log(`${logPrefix} Eventos adicionais (Meta) serão gerenciados pelo serviço central de geração de PIX.`);
                } catch (error) {
                    console.error(`${logPrefix} Erro no nó action_pix para chat ${chatId}:`, error.message);
                    if (error.response) {
                        console.error(`${logPrefix} Status HTTP:`, error.response.status);
                        console.error(`${logPrefix} Resposta da API:`, JSON.stringify(error.response.data, null, 2));
                    }
                    // Re-lança o erro para que o fluxo seja interrompido
                    throw error;
                }
                break;

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX nas variáveis.");
                    
                    const [transaction] = await sqlTx`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        return 'paid'; // Sinaliza para 'processFlow'
                    }

                    const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
                    let providerStatus = null;
                    let customerData = {};
                    const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;

                    if (transaction.provider === 'pushinpay') {
                        const last = pushinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                        const now = Date.now();
                        if (now - last >= 60_000) {
                            const resp = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`,
                                { headers: { Authorization: `Bearer ${seller.pushinpay_token}`, Accept: 'application/json', 'Content-Type': 'application/json' } });
                            providerStatus = String(resp.data.status || '').toLowerCase();
                            customerData = { name: resp.data.payer_name, document: resp.data.payer_national_registration };
                            pushinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                        }
                    } else if (transaction.provider === 'syncpay') {
                        const syncPayToken = await getSyncPayAuthToken(seller);
                        const resp = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`,
                            { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                        providerStatus = String(resp.data.status || '').toLowerCase();
                        customerData = resp.data.payer || {};
                    } else if (transaction.provider === 'wiinpay') {
                        const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                        if (wiinpayApiKey) {
                            const now = Date.now();
                            const last = wiinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                            if (now - last >= 60_000) {
                                const result = await getWiinpayPaymentStatus(transaction.provider_transaction_id, wiinpayApiKey);
                                providerStatus = result.status || null;
                                customerData = result.customer || {};
                                wiinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                            }
                        }
                    } else if (transaction.provider === 'paradise') {
                        const paradiseSecretKey = seller.paradise_secret_key;
                        if (paradiseSecretKey) {
                            const now = Date.now();
                            const last = paradiseLastCheckAt.get(transaction.provider_transaction_id) || 0;
                            if (now - last >= 60_000) {
                                const result = await getParadisePaymentStatus(transaction.provider_transaction_id, paradiseSecretKey);
                                providerStatus = result.status || null;
                                customerData = result.customer || {};
                                paradiseLastCheckAt.set(transaction.provider_transaction_id, now);
                            }
                        }
                    }
                    
                    // Normalizar providerStatus para lowercase antes de comparar
                    const normalizedProviderStatus = providerStatus ? String(providerStatus).toLowerCase() : null;
                    if (normalizedProviderStatus && paidStatuses.has(normalizedProviderStatus)) {
                        await handleSuccessfulPayment(transaction.id, customerData);
                        return 'paid'; // Sinaliza para 'processFlow'
                    } else {
                        return 'pending'; // Sinaliza para 'processFlow'
                    }
                } catch (error) {
                    console.error(`${logPrefix} Erro ao consultar PIX:`, error);
                    return 'pending'; // Em caso de erro, assume pendente
                }
            
            case 'forward_flow':
                const targetFlowId = actionData.targetFlowId;
                if (!targetFlowId) {
                    console.error(`${logPrefix} 'forward_flow' action não tem targetFlowId.`);
                    break; 
                }
                
                console.log(`${logPrefix} Encaminhando para o fluxo ${targetFlowId} para o chat ${chatId}`);
                
                // Cancela qualquer tarefa de timeout pendente antes de encaminhar para o novo fluxo
                try {
                    const [stateToCancel] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    if (stateToCancel && stateToCancel.scheduled_message_id) {
                        try {
                            await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                            console.log(`${logPrefix} [Forward Flow] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                        } catch (e) {
                            console.warn(`${logPrefix} [Forward Flow] Falha ao cancelar QStash msg ${stateToCancel.scheduled_message_id}:`, e.message);
                        }
                    }
                } catch (e) {
                    console.error(`${logPrefix} [Forward Flow] Erro ao verificar tarefas pendentes:`, e.message);
                }
                
                // Garante que targetFlowId seja um número para a query SQL
                const targetFlowIdNum = parseInt(targetFlowId, 10);
                if (isNaN(targetFlowIdNum)) {
                    console.error(`${logPrefix} 'forward_flow' targetFlowId inválido: ${targetFlowId}`);
                    break;
                }
                
                const [targetFlow] = await sqlTx`SELECT * FROM flows WHERE id = ${targetFlowIdNum} AND bot_id = ${botId}`;
                if (!targetFlow || !targetFlow.nodes) {
                     console.error(`${logPrefix} Fluxo de destino ${targetFlowIdNum} não encontrado.`);
                     break;
                }
                
                const targetFlowData = typeof targetFlow.nodes === 'string' ? JSON.parse(targetFlow.nodes) : targetFlow.nodes;
                const targetNodes = targetFlowData.nodes || [];
                const targetEdges = targetFlowData.edges || [];
                const targetStartNode = targetNodes.find(n => n.type === 'trigger');
                
                if (!targetStartNode) {
                    console.error(`${logPrefix} Fluxo de destino ${targetFlowIdNum} não tem nó de 'trigger'.`);
                    break;
                }
                
                // Encontra o primeiro nó válido (não trigger) após o trigger inicial
                let nextNodeId = findNextNode(targetStartNode.id, 'a', targetEdges);
                let attempts = 0;
                const maxAttempts = 20; // Proteção contra loops infinitos
                
                // Limpa o estado atual antes de iniciar o novo fluxo
                await sqlTx`UPDATE user_flow_states 
                          SET waiting_for_input = false, scheduled_message_id = NULL 
                          WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                
                // Pula nós do tipo 'trigger' até encontrar um nó válido
                while (nextNodeId && attempts < maxAttempts) {
                    const currentNode = targetNodes.find(n => n.id === nextNodeId);
                    if (!currentNode) {
                        console.error(`${logPrefix} Nó ${nextNodeId} não encontrado no fluxo de destino.`);
                        break;
                    }
                    
                    if (currentNode.type !== 'trigger') {
                        // Encontrou um nó válido (não é trigger)
                        console.log(`${logPrefix} Encontrado nó válido para iniciar: ${nextNodeId} (tipo: ${currentNode.type})`);
                        // Passa os dados do fluxo de destino para o processFlow recursivo
                        await processFlow(chatId, botId, botToken, sellerId, nextNodeId, variables, targetNodes, targetEdges, targetFlowIdNum);
                        break;
                    }
                    
                    // Se for trigger, continua procurando o próximo nó
                    console.log(`${logPrefix} Pulando nó trigger ${nextNodeId}, procurando próximo nó...`);
                    nextNodeId = findNextNode(nextNodeId, 'a', targetEdges);
                    attempts++;
                }
                
                if (!nextNodeId || attempts >= maxAttempts) {
                    if (attempts >= maxAttempts) {
                        console.error(`${logPrefix} Limite de tentativas atingido ao procurar nó válido no fluxo ${targetFlowIdNum}.`);
                    } else {
                        console.log(`${logPrefix} Fluxo de destino ${targetFlowIdNum} está vazio (sem nó válido após o trigger).`);
                    }
                }

                return 'flow_forwarded'; // Sinaliza para o 'processFlow' atual PARAR.

            default:
                console.warn(`${logPrefix} Tipo de ação aninhada desconhecida: ${action.type}. Ignorando.`);
                break;
        }
    }
    return 'completed';
}


/**
 * =================================================================
 * FUNÇÃO 'processFlow' (O NAVEGADOR) - VERSÃO NOVA
 * =================================================================
 * (Colada da sua resposta anterior)
 */
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}, flowNodes = null, flowEdges = null, flowId = null) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    console.log(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);

    // ==========================================================
    // PASSO 1: CARREGAR VARIÁVEIS DO USUÁRIO E DO CLIQUE
    // ==========================================================
    let variables = { ...initialVariables };

    const [user] = await sqlWithRetry(sqlTx`
        SELECT first_name, last_name 
        FROM telegram_chats 
        WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
        ORDER BY created_at DESC LIMIT 1`);

    if (user) {
        variables.primeiro_nome = user.first_name || '';
        variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }

    if (variables.click_id) {
        const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
        const [click] = await sqlWithRetry(sqlTx`SELECT city, state FROM clicks WHERE click_id = ${db_click_id}`);
        if (click) {
            variables.cidade = click.city || '';
            variables.estado = click.state || '';
        }else{
            variables.cidade = '';
            variables.estado = '';
        }
    }
    // ==========================================================
    // FIM DO PASSO 1
    // ==========================================================
    // Se os dados do fluxo foram fornecidos (ex: forward_flow), usa eles. Caso contrário, busca do banco.
    let nodes, edges;
    let currentFlowId = null; // Armazena o ID do fluxo atual para rastreamento
    if (flowNodes && flowEdges) {
        // Usa os dados do fluxo fornecido (do forward_flow)
        nodes = flowNodes;
        edges = flowEdges;
        if (flowId) {
            currentFlowId = flowId; // Usa o flowId fornecido para rastreamento
            console.log(`${logPrefix} [Flow Engine] Usando dados do fluxo fornecido (ID: ${flowId}, ${nodes.length} nós, ${edges.length} arestas).`);
        } else {
            // Tenta buscar o flowId do banco usando bot_id e nodes fornecidos
            // Como não temos uma forma direta de identificar o fluxo pelos nodes, deixa null
            // O contador não funcionará neste caso, mas o fluxo continuará funcionando
            console.log(`${logPrefix} [Flow Engine] Usando dados do fluxo fornecido sem flowId (${nodes.length} nós, ${edges.length} arestas). Contador de execução não será atualizado.`);
        }
    } else {
        // Busca o fluxo do banco
        // Primeiro tenta buscar pelo flow_id do estado, se disponível
        let flow = null;
        const [userStateForFlow] = await sqlWithRetry(sqlTx`SELECT flow_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
        
        if (userStateForFlow && userStateForFlow.flow_id) {
            // Busca o fluxo específico usando flow_id do estado
            const [flowResult] = await sqlWithRetry(sqlTx`SELECT * FROM flows WHERE id = ${userStateForFlow.flow_id}`);
            if (flowResult && flowResult.nodes) {
                flow = flowResult;
                console.log(`${logPrefix} [Flow Engine] Usando fluxo do estado (ID: ${userStateForFlow.flow_id}).`);
            }
        }
        
        // Se não encontrou pelo flow_id do estado, busca o fluxo ativo do bot
        if (!flow) {
            const [flowResult] = await sqlWithRetry(sqlTx`SELECT * FROM flows WHERE bot_id = ${botId} AND is_active = TRUE ORDER BY updated_at DESC LIMIT 1`);
            if (flowResult && flowResult.nodes) {
                flow = flowResult;
            }
        }
        
        if (!flow || !flow.nodes) {
            console.log(`${logPrefix} [Flow Engine] Nenhum fluxo encontrado para o bot ID ${botId}.`);
            return;
        }
        currentFlowId = flow.id; // Armazena o ID do fluxo para rastreamento
        const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
        nodes = flowData.nodes || [];
        edges = flowData.edges || [];
    }
    
    let currentNodeId = startNodeId;

    // Se 'currentNodeId' ainda for nulo (início normal), define
    if (!currentNodeId) {
        const isStartCommand = initialVariables.click_id && initialVariables.click_id.startsWith('/start');
        
        if (isStartCommand) {
            console.log(`${logPrefix} [Flow Engine] Comando /start detectado. Reiniciando fluxo.`);
            const [stateToCancel] = await sqlWithRetry(sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            if (stateToCancel && stateToCancel.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                    console.log(`[Flow Engine] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                } catch (e) { console.warn(`[Flow Engine] Falha ao cancelar QStash msg ${stateToCancel.scheduled_message_id}:`, e.message); }
            }
            await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            const startNode = nodes.find(node => node.type === 'trigger');
            currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;

        } else {
            const [userState] = await sqlWithRetry(sqlTx`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            if (userState && userState.waiting_for_input) {
                console.log(`${logPrefix} [Flow Engine] Usuário respondeu. Continuando do nó ${userState.current_node_id} (handle 'a').`);
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
                let parsedVariables = {};
                try { parsedVariables = JSON.parse(userState.variables); } catch (e) { parsedVariables = userState.variables; }
                variables = { ...variables, ...parsedVariables };

            } else {
                console.log(`${logPrefix} [Flow Engine] Nova conversa. Iniciando do gatilho.`);
                await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
                const startNode = nodes.find(node => node.type === 'trigger');
                currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;
            }
        }
    }


    if (!currentNodeId) {
        console.log(`${logPrefix} [Flow Engine] Nenhum nó para processar. Fim do fluxo.`);
        await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
        return;
    }

    // ==========================================================
    // PASSO 3: O NOVO LOOP DE NAVEGAÇÃO
    // ==========================================================
    let safetyLock = 0;
    // currentFlowId já foi determinado acima
    
    while (currentNodeId && safetyLock < 20) {
        safetyLock++;
        let currentNode = nodes.find(node => node.id === currentNodeId);
        
        if (!currentNode) {
            console.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        console.log(`${logPrefix} [Flow Engine] Processando Nó: ${currentNode.id} (Tipo: ${currentNode.type})`);

        // Incrementa contador de execução do node (apenas se tiver flow_id)
        if (currentFlowId && currentNode.id !== 'start') {
            try {
                const [current] = await sqlWithRetry(sqlTx`
                    SELECT COALESCE(node_execution_counts, '{}'::jsonb) as counts 
                    FROM flows 
                    WHERE id = ${currentFlowId}
                `);
                
                if (current) {
                    const currentCount = current.counts[currentNode.id] || 0;
                    const newCount = parseInt(currentCount) + 1;
                    
                    await sqlWithRetry(sqlTx`
                        UPDATE flows 
                        SET node_execution_counts = jsonb_set(
                            COALESCE(node_execution_counts, '{}'::jsonb),
                            ARRAY[${currentNode.id}],
                            ${newCount}::text::jsonb
                        )
                        WHERE id = ${currentFlowId}
                    `);
                }
            } catch (error) {
                console.warn(`[Flow Engine] Erro ao incrementar contador de execução do node ${currentNode.id} no flow ${currentFlowId}:`, error.message);
            }
        }

        // Determina flow_id para salvar no estado
        let flowIdToSave = currentFlowId;
        if (!flowIdToSave) {
            // Se não tem currentFlowId, tenta buscar do fluxo ativo do bot
            try {
                const [activeFlow] = await sqlWithRetry(sqlTx`
                    SELECT id FROM flows 
                    WHERE bot_id = ${botId} AND is_active = TRUE 
                    ORDER BY updated_at DESC LIMIT 1
                `);
                if (activeFlow) {
                    flowIdToSave = activeFlow.id;
                }
            } catch (e) {
                // Ignora erro, deixa flowIdToSave como null
            }
        }

        await sqlWithRetry(sqlTx`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input, scheduled_message_id, flow_id)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false, NULL, ${flowIdToSave})
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET 
                current_node_id = EXCLUDED.current_node_id, 
                variables = EXCLUDED.variables, 
                waiting_for_input = false, 
                scheduled_message_id = NULL,
                flow_id = EXCLUDED.flow_id;
        `);

        if (currentNode.type === 'trigger') {
            if (currentNode.data.actions && currentNode.data.actions.length > 0) {
                 const actionResult = await processActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`, currentNodeId, currentFlowId, nodes, edges);
                 
                 // Se delay foi agendado, parar processamento
                 if (actionResult === 'delay_scheduled') {
                     console.log(`${logPrefix} [Flow Engine] Delay agendado. Parando processamento atual.`);
                     currentNodeId = null;
                     break;
                 }
                 
                 await sqlWithRetry(sqlTx`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            }
            currentNodeId = findNextNode(currentNode.id, 'a', edges);
            continue;
        }

        if (currentNode.type === 'action') {
            const actions = currentNode.data.actions || [];
            const actionResult = await processActions(actions, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`, currentNodeId, currentFlowId, nodes, edges);

            await sqlWithRetry(sqlTx`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`);

            if (actionResult === 'flow_forwarded') {
                console.log(`${logPrefix} [Flow Engine] Fluxo encaminhado. Encerrando o fluxo atual (worker).`);
                currentNodeId = null; // Para o loop atual
                break; // Sai do 'while'
            }
            
            // Se delay foi agendado, parar processamento
            if (actionResult === 'delay_scheduled') {
                console.log(`${logPrefix} [Flow Engine] Delay agendado. Parando processamento atual.`);
                currentNodeId = null;
                break;
            }

            if (currentNode.data.waitForReply) {
                const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                const timeoutMinutes = currentNode.data.replyTimeout || 5;

                try {
                    // Agenda o worker de timeout com uma única chamada
                    const response = await qstashClient.publishJSON({
                        url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                        body: { 
                            chat_id: chatId, 
                            bot_id: botId, 
                            target_node_id: noReplyNodeId, // Pode ser null, e o worker saberá encerrar
                            variables: variables
                        },
                        delay: `${timeoutMinutes}m`,
                        contentBasedDeduplication: true,
                        method: "POST"
                    });
                    
                    // Salva o estado como "esperando" e armazena o ID da tarefa agendada
                    // Mantém o flow_id existente (não atualiza para não perder a referência ao fluxo correto)
                    await sqlWithRetry(sqlTx`
                        UPDATE user_flow_states 
                        SET waiting_for_input = true, scheduled_message_id = ${response.messageId} 
                        WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
                    
                    console.log(`${logPrefix} [Flow Engine] Fluxo pausado no nó ${currentNode.id}. Esperando ${timeoutMinutes} min. Tarefa QStash: ${response.messageId}`);
                
                } catch (error) {
                    console.error(`${logPrefix} [Flow Engine] Erro CRÍTICO ao agendar timeout no QStash:`, error);
                }

                currentNodeId = null; // PARA o loop
                break; // Sai do 'while'
            }
            
            if (actionResult === 'paid') {
                console.log(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pago. Seguindo handle 'a'.`);
                currentNodeId = findNextNode(currentNode.id, 'a', edges);
                continue;
            }
            if (actionResult === 'pending') {
                console.log(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pendente. Seguindo handle 'b'.`);
                currentNodeId = findNextNode(currentNode.id, 'b', edges);
                continue;
            }
            
            currentNodeId = findNextNode(currentNode.id, 'a', edges);
            continue;
        }

        console.warn(`${logPrefix} [Flow Engine] Tipo de nó desconhecido: ${currentNode.type}. Encerrando fluxo.`);
        currentNodeId = null;
    }
    // ==========================================================
    // FIM DO PASSO 3
    // ==========================================================

    // Limpeza final: Se o fluxo terminou (não está esperando input), limpa o estado.
    // IMPORTANTE: Não limpar se há scheduled_message_id (delay agendado) ou waiting_for_input (aguardando resposta)
    if (!currentNodeId) {
        const [state] = await sqlWithRetry(sqlTx`
            SELECT waiting_for_input, scheduled_message_id 
            FROM user_flow_states 
            WHERE chat_id = ${chatId} AND bot_id = ${botId}
        `);
        
        if (!state) {
            // Estado não existe, nada a fazer
            console.log(`${logPrefix} [Flow Engine] Nenhum estado encontrado para ${chatId}.`);
        } else if (state.waiting_for_input) {
            // Fluxo pausado aguardando resposta do usuário
            console.log(`${logPrefix} [Flow Engine] Fluxo pausado (waiting for input). Estado preservado para ${chatId}.`);
        } else if (state.scheduled_message_id) {
            // Delay agendado via QStash - estado deve ser preservado
            console.log(`${logPrefix} [Flow Engine] Delay agendado detectado (scheduled_message_id: ${state.scheduled_message_id}). Estado preservado para ${chatId}.`);
        } else {
            // Fluxo realmente terminou - pode limpar o estado
            console.log(`${logPrefix} [Flow Engine] Fim do fluxo para ${chatId}. Limpando estado.`);
            await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
        }
    }
}
// ==========================================================

async function handler(req, res) {
    // Verificar se requisição foi abortada antes de processar
    if (req.aborted) {
        return res.status(499).end(); // 499 = Client Closed Request
    }

    if (req.method !== 'POST') {
        return res.status(405).json({ message: 'Method Not Allowed' });


    }

    try {
        // 1. Recebe os dados agendados pelo QStash
        const { chat_id, bot_id, target_node_id, variables, continue_from_delay } = req.body;
        const logPrefix = '[WORKER]';

        console.log(`${logPrefix} [Timeout] Recebido para chat ${chat_id}, bot ${bot_id}. Nó de destino: ${target_node_id || 'NONE'}. Continue from delay: ${continue_from_delay || false}`);

        // 2. Busca o bot para obter o token e sellerId
        const [bot] = await sqlWithRetry(sqlTx`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`);
        if (!bot || !bot.bot_token) {
            throw new Error(`[WORKER] Bot ${bot_id} ou token não encontrado.`);
        }
        const botToken = bot.bot_token;
        const sellerId = bot.seller_id;

        // 3. *** VERIFICAÇÃO CRÍTICA ***
        // Verifica se o estado atual corresponde ao esperado
        const [currentState] = await sqlWithRetry(sqlTx`
            SELECT waiting_for_input, scheduled_message_id, current_node_id, flow_id 
            FROM user_flow_states 
            WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);

        // Verificações para determinar se este timeout deve ser processado
        if (!currentState) {
            console.log(`${logPrefix} [Timeout] Ignorado: Nenhum estado encontrado para o usuário ${chat_id}.`);
            return res.status(200).json({ message: 'Timeout ignored, no user state found.' });
        }
        
        // Se é continuação após delay, não verificar waiting_for_input
        if (!continue_from_delay) {
            if (!currentState.waiting_for_input) {
                console.log(`${logPrefix} [Timeout] Ignorado: Usuário ${chat_id} já respondeu ou o fluxo foi reiniciado.`);
                return res.status(200).json({ message: 'Timeout ignored, user already proceeded.' });
            }

            // 4. O usuário NÃO respondeu a tempo.
            console.log(`${logPrefix} [Timeout] Usuário ${chat_id} não respondeu. Processando caminho de timeout.`);
            
            // Limpa o estado de 'espera' ANTES de processar o próximo nó
            await sqlWithRetry(sqlTx`
                UPDATE user_flow_states 
                SET waiting_for_input = false, scheduled_message_id = NULL 
                WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);
        } else {
            console.log(`${logPrefix} [Timeout] Continuando após delay agendado para ${chat_id}.`);
        }

        // 5. Inicia o 'processFlow' a partir do nó de timeout (handle 'b')
        // Se target_node_id for 'null' (porque o handle 'b' não estava conectado),
        // o 'processFlow' saberá que deve encerrar o fluxo.
        if (target_node_id) {
            try {
                // Verificar se é continuação após delay agendado
                const continueFromDelay = req.body.continue_from_delay === true;
                const remainingActionsJson = req.body.remaining_actions;
                
                // Busca o fluxo correto usando flow_id do estado, se disponível
                let flowNodes = null;
                let flowEdges = null;
                let flowIdForProcess = null;
                
                if (currentState.flow_id) {
                    const [flow] = await sqlWithRetry(sqlTx`
                        SELECT nodes FROM flows WHERE id = ${currentState.flow_id}
                    `);
                    if (flow && flow.nodes) {
                        const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
                        flowNodes = flowData.nodes || [];
                        flowEdges = flowData.edges || [];
                        flowIdForProcess = currentState.flow_id;
                    }
                }
                
                // Se é continuação após delay e há ações restantes, processar apenas as ações restantes
                if (continueFromDelay && remainingActionsJson) {
                    try {
                        const remainingActions = JSON.parse(remainingActionsJson);
                        console.log(`${logPrefix} [Timeout] Continuando após delay. Processando ${remainingActions.length} ação(ões) restante(s).`);
                        
                        // Buscar o nó atual para obter contexto
                        const currentNode = flowNodes.find(n => n.id === target_node_id);
                        if (currentNode && currentNode.type === 'action') {
                            // Processar apenas as ações restantes
                            const actionResult = await processActions(
                                remainingActions, 
                                chat_id, 
                                bot_id, 
                                botToken, 
                                sellerId, 
                                variables, 
                                `[FlowNode ${target_node_id}]`,
                                target_node_id,
                                flowIdForProcess,
                                flowNodes,
                                flowEdges
                            );
                            
                            // Atualizar variáveis
                            await sqlWithRetry(sqlTx`
                                UPDATE user_flow_states 
                                SET variables = ${JSON.stringify(variables)},
                                    scheduled_message_id = NULL
                                WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}
                            `);
                            
                            // Tratar todos os possíveis resultados das ações restantes
                            // Seguindo a mesma lógica do processFlow
                            
                            // 1. Verifica se uma ação 'forward_flow' foi executada
                            if (actionResult === 'flow_forwarded') {
                                console.log(`${logPrefix} [Timeout] Fluxo encaminhado após delay. Encerrando o fluxo atual.`);
                                // Fluxo foi encaminhado, não precisa continuar
                                return;
                            }
                            
                            // 2. Se delay foi agendado novamente, não continuar (já agendado)
                            if (actionResult === 'delay_scheduled') {
                                console.log(`${logPrefix} [Timeout] Outro delay agendado após delay. Parando processamento.`);
                                return;
                            }
                            
                            // 3. Verifica se o resultado foi de um 'action_check_pix'
                            if (actionResult === 'paid') {
                                console.log(`${logPrefix} [Timeout] Resultado após delay: PIX Pago. Seguindo handle 'a'.`);
                                const nextNodeId = findNextNode(target_node_id, 'a', flowEdges);
                                if (nextNodeId) {
                                    await processFlow(
                                        chat_id, 
                                        bot_id, 
                                        botToken, 
                                        sellerId, 
                                        nextNodeId,
                                        variables,
                                        flowNodes,
                                        flowEdges,
                                        flowIdForProcess
                                    );
                                } else {
                                    console.log(`${logPrefix} [Timeout] Nenhum próximo nó após delay (paid). Fluxo concluído.`);
                                }
                                return;
                            }
                            
                            if (actionResult === 'pending') {
                                console.log(`${logPrefix} [Timeout] Resultado após delay: PIX Pendente. Seguindo handle 'b'.`);
                                const nextNodeId = findNextNode(target_node_id, 'b', flowEdges);
                                if (nextNodeId) {
                                    await processFlow(
                                        chat_id, 
                                        bot_id, 
                                        botToken, 
                                        sellerId, 
                                        nextNodeId,
                                        variables,
                                        flowNodes,
                                        flowEdges,
                                        flowIdForProcess
                                    );
                                } else {
                                    console.log(`${logPrefix} [Timeout] Nenhum próximo nó após delay (pending). Fluxo concluído.`);
                                }
                                return;
                            }
                            
                            // 4. Se nada acima aconteceu, é um nó de ação simples. Segue pelo handle 'a'.
                            const nextNodeId = findNextNode(target_node_id, 'a', flowEdges);
                            if (nextNodeId) {
                                // Continuar processando o fluxo a partir do próximo nó
                                await processFlow(
                                    chat_id, 
                                    bot_id, 
                                    botToken, 
                                    sellerId, 
                                    nextNodeId,
                                    variables,
                                    flowNodes,
                                    flowEdges,
                                    flowIdForProcess
                                );
                            } else {
                                // Não há próximo nó, fluxo terminou
                                console.log(`${logPrefix} [Timeout] Nenhum próximo nó após delay. Fluxo concluído.`);
                            }
                        } else {
                            // Se não é um nó de ação, continuar normalmente
                            await processFlow(
                                chat_id, 
                                bot_id, 
                                botToken, 
                                sellerId, 
                                target_node_id,
                                variables,
                                flowNodes,
                                flowEdges,
                                flowIdForProcess
                            );
                        }
                    } catch (parseError) {
                        console.error(`${logPrefix} [Timeout] Erro ao processar ações restantes após delay:`, parseError.message);
                        // Fallback: continuar normalmente
                        await processFlow(
                            chat_id, 
                            bot_id, 
                            botToken, 
                            sellerId, 
                            target_node_id,
                            variables,
                            flowNodes,
                            flowEdges,
                            flowIdForProcess
                        );
                    }
                } else if (continueFromDelay && !remainingActionsJson) {
                    // Delay foi a última ação do nó - continuar para o próximo nó
                    console.log(`${logPrefix} [Timeout] Continuando após delay. Delay foi a última ação do nó. Continuando para o próximo nó.`);
                    
                    // Atualizar scheduled_message_id para NULL já que o delay foi processado
                    await sqlWithRetry(sqlTx`
                        UPDATE user_flow_states 
                        SET scheduled_message_id = NULL
                        WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}
                    `);
                    
                    // Encontrar o próximo nó pelo handle 'a' e continuar de lá
                    const nextNodeId = findNextNode(target_node_id, 'a', flowEdges);
                    if (nextNodeId) {
                        await processFlow(
                            chat_id, 
                            bot_id, 
                            botToken, 
                            sellerId, 
                            nextNodeId,
                            variables,
                            flowNodes,
                            flowEdges,
                            flowIdForProcess
                        );
                    } else {
                        console.log(`${logPrefix} [Timeout] Nenhum próximo nó após delay. Fluxo concluído.`);
                    }
                } else {
                    // Processamento normal (timeout ou continuação sem ações restantes)
                    await processFlow(
                        chat_id, 
                        bot_id, 
                        botToken, 
                        sellerId, 
                        target_node_id, // Este é o nó da saída 'b' (Sem Resposta) ou continuação
                        variables,
                        flowNodes,
                        flowEdges,
                        flowIdForProcess
                    );
                }
                // Verificar se resposta já foi enviada antes de enviar
                if (!res.headersSent) {
                    return res.status(200).json({ message: 'Timeout processed successfully.' });
                }
                return;
            } catch (flowError) {
                // Erro durante processFlow - logar mas não quebrar o handler
                console.error(`[WORKER] Erro durante processFlow para timeout:`, flowError.message);
                // Verificar se resposta já foi enviada antes de enviar
                if (!res.headersSent) {
                    return res.status(200).json({ message: 'Timeout processed with errors.' });
                }
                return;
            }
        } else {
            console.log(`${logPrefix} [Timeout] Nenhum nó de destino definido. Encerrando fluxo para ${chat_id}.`);
            await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);
            // Verificar se resposta já foi enviada antes de enviar
            if (!res.headersSent) {
                return res.status(200).json({ message: 'Timeout processed, flow ended (no target node).' });
            }
            return;
        }

    } catch (error) {
        // Verificar se resposta já foi enviada antes de tentar enviar qualquer resposta
        if (res.headersSent) {
            console.error('[WORKER] Erro após resposta já enviada:', error.message);
            return;
        }
        
        // Tratar requisições abortadas silenciosamente
        if (error.message?.includes('request aborted') || 
            error.message?.includes('aborted') ||
            req.aborted ||
            error.code === 'ECONNRESET' ||
            error.code === 'EPIPE') {
            return res.status(499).end(); // 499 = Client Closed Request
        }
        
        // Tratar especificamente CONNECT_TIMEOUT
        if (error.message?.includes('CONNECT_TIMEOUT') || error.message?.includes('write CONNECT_TIMEOUT')) {
            console.error(`[WORKER] CONNECT_TIMEOUT ao processar timeout para chat ${chat_id}. Pool pode estar esgotado.`);
            // Retornar 500 para que o QStash tente novamente mais tarde
            return res.status(500).json({ 
                error: `Database connection timeout: ${error.message}`,
                retry: true 
            });
        }
        
        console.error('[WORKER] Erro fatal ao processar timeout:', error.message, error.stack);
        // Retornamos 200 para que o QStash NÃO tente re-executar um fluxo que falhou logicamente.
        return res.status(200).json({ error: `Failed to process timeout: ${error.message}` });
    
    }
}

// Exporta o handler com a verificação de segurança do QStash
module.exports = handler;