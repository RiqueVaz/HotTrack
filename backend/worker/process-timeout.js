// /backend/worker/process-timeout.js

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
// QStash removido - usando BullMQ agora
// const { Client } = require("@upstash/qstash");
const crypto = require('crypto');
const { createPixService } = require('../shared/pix');
const { sqlTx, sqlWithRetry } = require('../db');
const { handleSuccessfulPayment: handleSuccessfulPaymentShared } = require('../shared/payment-handler');
const { sendEventToUtmify: sendEventToUtmifyShared, sendMetaEvent: sendMetaEventShared } = require('../shared/event-sender');
const telegramRateLimiter = require('../shared/telegram-rate-limiter-bullmq');
const apiRateLimiterBullMQ = require('../shared/api-rate-limiter-bullmq');
const dbCache = require('../shared/db-cache');
const { shouldLogDebug, shouldLogOccasionally } = require('../shared/logger-helper');
const { addJobWithDelay, removeJob, QUEUE_NAMES } = require('../shared/queue');

const DEFAULT_INVITE_MESSAGE = 'Seu link exclusivo está pronto! Clique no botão abaixo para acessar.';
const DEFAULT_INVITE_BUTTON_TEXT = 'Acessar convite';

// Cache global de file_ids do Telegram (evita queries repetidas durante batches)
if (!global.mediaFileIdCache) {
    global.mediaFileIdCache = new Map();
    // Limpar cache a cada 10 minutos
    setInterval(() => {
        if (global.mediaFileIdCache.size > 1000) {
            global.mediaFileIdCache.clear();
        }
    }, 10 * 60 * 1000);
}

// ==========================================================
//                     INICIALIZAÇÃO
// ==========================================================
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
const MAX_SYNCPAY_TOKEN_CACHE_SIZE = 100; // Limite máximo de tokens no cache

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
    
    // Se cache ainda estiver acima do limite, remover 20% das entradas mais antigas
    if (syncPayTokenCache.size >= MAX_SYNCPAY_TOKEN_CACHE_SIZE) {
        const entries = Array.from(syncPayTokenCache.entries())
            .sort((a, b) => (a[1].expiresAt || 0) - (b[1].expiresAt || 0));
        const toRemove = Math.floor(MAX_SYNCPAY_TOKEN_CACHE_SIZE * 0.2);
        for (let i = 0; i < toRemove && i < entries.length; i++) {
            syncPayTokenCache.delete(entries[i][0]);
        }
        cleaned += toRemove;
    }
    
    // Removido log de memory cleanup - não é necessário em produção
}, 5 * 60 * 1000); // A cada 5 minutos

// Rate limiting agora é gerenciado pelo módulo api-rate-limiter
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const WIINPAY_SPLIT_USER_ID = process.env.WIINPAY_SPLIT_USER_ID;


// QStash removido - usando BullMQ agora
// const qstashClient = new Client({
//     token: process.env.QSTASH_TOKEN,
// });

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

async function getWiinpayPaymentStatus(paymentId, apiKey, sellerId = null) {
    if (!apiKey) {
        throw new Error('Credenciais da WiinPay não configuradas.');
    }
    // Usar rate limiter se sellerId fornecido, senão usar axios direto (compatibilidade)
    if (sellerId) {
        const response = await apiRateLimiterBullMQ.request({
            provider: 'wiinpay',
            sellerId: sellerId,
            method: 'get',
            url: `https://api-v2.wiinpay.com.br/payment/list/${paymentId}`,
            headers: {
                Accept: 'application/json',
                Authorization: `Bearer ${apiKey}`
            }
        });
        const data = Array.isArray(response) ? response[0] : response;
        return parseWiinpayPayment(data);
    } else {
        const response = await axios.get(`https://api-v2.wiinpay.com.br/payment/list/${paymentId}`, {
            headers: {
                Accept: 'application/json',
                Authorization: `Bearer ${apiKey}`
            }
        });
        const data = Array.isArray(response.data) ? response.data[0] : response.data;
        return parseWiinpayPayment(data);
    }
}

async function getParadisePaymentStatus(transactionId, secretKey, sellerId = null) {
    if (!secretKey) {
        throw new Error('Credenciais da Paradise não configuradas.');
    }
    
    try {
        // Usar rate limiter se sellerId fornecido, senão usar axios direto (compatibilidade)
        let data;
        if (sellerId) {
            data = await apiRateLimiterBullMQ.request({
                provider: 'paradise',
                sellerId: sellerId,
                method: 'get',
                url: `https://multi.paradisepags.com/api/v1/query.php?action=get_transaction&id=${transactionId}`,
                headers: {
                    'X-API-Key': secretKey,
                    'Content-Type': 'application/json',
                }
            });
        } else {
            const response = await axios.get(`https://multi.paradisepags.com/api/v1/query.php?action=get_transaction&id=${transactionId}`, {
                headers: {
                    'X-API-Key': secretKey,
                    'Content-Type': 'application/json',
                },
            });
            data = response.data;
        }

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
        await dbCache.markBotBlocked(botId, chatId);
    } catch (error) {
        console.error(`[Bot Blocks] Erro ao marcar bloqueio: ${error.message}`);
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
        if (botId && await dbCache.isBotBlocked(botId, chatId)) {
            // Removido log de cache hit
            return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
        } else if (!botId && await dbCache.isBotTokenBlocked(botToken, chatId)) {
            // Removido log de cache hit
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
                    await dbCache.unmarkBotBlocked(botId, chatId);
                } catch (unblockError) {
                    // Não crítico, apenas logar
                    console.debug(`[Bot Blocks] Erro ao remover bloqueio (não crítico): ${unblockError.message}`);
                }
            }
            return response.data;
        } catch (error) {
            const errorChatId = data?.chat_id || 'unknown';
            
            // Tratamento para erro 403 (bot bloqueado)
            if (error.response && error.response.status === 403) {
                const dbCache = require('../shared/db-cache');
                const description = error.response?.data?.description || 'Forbidden: bot was blocked by the user';
                
                // MARCAR NO CACHE E NA TABELA QUANDO RECEBER 403
                if (description.includes('bot was blocked by the user') && errorChatId && errorChatId !== 'unknown') {
                    if (botId) {
                        await dbCache.markBotBlocked(botId, errorChatId);
                        // Buscar seller_id do bot e inserir na tabela
                        try {
                            const [bot] = await sqlWithRetry(
                                sqlTx`SELECT seller_id FROM telegram_bots WHERE id = ${botId}`
                            );
                            if (bot) {
                                await markBotBlockedInDb(botId, errorChatId, bot.seller_id);
                            }
                        } catch (dbError) {
                            console.warn(`[Bot Blocks] Erro ao buscar seller_id para bot ${botId}: ${dbError.message}`);
                        }
                    } else {
                        await dbCache.markBotTokenBlocked(botToken, errorChatId);
                    }
                }
                
                // Logar apenas ocasionalmente (1% das vezes) para evitar spam
                if (shouldLogOccasionally(0.01)) {
                    console.warn(`[WORKER - Telegram API] O bot foi bloqueado pelo usuário. ChatID: ${errorChatId}`);
                }
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


async function sendMediaFromR2(botToken, chatId, storageUrl, fileType, caption, mediaId = null, botId = null) {
    // Se temos mediaId e botId, verificar se já existe file_id cacheado
    if (mediaId && botId) {
        // Verificar cache em memória primeiro (se disponível)
        const cacheKey = `telegram_file_id_${mediaId}_${botId}`;
        if (global.mediaFileIdCache && global.mediaFileIdCache.has(cacheKey)) {
            const cachedFileId = global.mediaFileIdCache.get(cacheKey);
            if (cachedFileId) {
                // Usar file_id cacheado diretamente
                const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[fileType];
                const field = { image: 'photo', video: 'video', audio: 'voice' }[fileType];
                const timeout = fileType === 'video' ? 120000 : 60000;
                const payload = normalizeTelegramPayload({
                    chat_id: chatId,
                    [field]: cachedFileId,
                    caption: caption || "",
                    parse_mode: 'HTML'
                });
                try {
                    return await sendTelegramRequest(botToken, method, payload, { timeout }, 3, 1500, botId);
                } catch (error) {
                    // Se file_id expirou, remover do cache e continuar para download
                    if (error.response?.data?.description?.includes('wrong remote file identifier')) {
                        global.mediaFileIdCache.delete(cacheKey);
                        // Continuar para baixar do R2
                    } else {
                        throw error;
                    }
                }
            }
        }
        
        // Verificar no banco
        try {
            const [media] = await sqlWithRetry(
                'SELECT telegram_file_ids FROM media_library WHERE id = $1',
                [mediaId]
            );
            
            const botIdStr = String(botId);
            if (botIdStr && botIdStr !== 'null' && botIdStr !== 'undefined' && media?.telegram_file_ids?.[botIdStr]) {
                const fileId = media.telegram_file_ids[botIdStr];
                // Adicionar ao cache em memória
                if (global.mediaFileIdCache) {
                    global.mediaFileIdCache.set(cacheKey, fileId);
                }
                
                // Usar file_id do banco
                const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[fileType];
                const field = { image: 'photo', video: 'video', audio: 'voice' }[fileType];
                const timeout = fileType === 'video' ? 120000 : 60000;
                const payload = normalizeTelegramPayload({
                    chat_id: chatId,
                    [field]: fileId,
                    caption: caption || "",
                    parse_mode: 'HTML'
                });
                try {
                    return await sendTelegramRequest(botToken, method, payload, { timeout }, 3, 1500, botId);
                } catch (error) {
                    // Se file_id expirou, remover do banco e continuar para download
                    if (error.response?.data?.description?.includes('wrong remote file identifier')) {
                        await sqlWithRetry(
                            'UPDATE media_library SET telegram_file_ids = telegram_file_ids - $1 WHERE id = $2',
                            [botIdStr, mediaId]
                        );
                        if (global.mediaFileIdCache) {
                            global.mediaFileIdCache.delete(cacheKey);
                        }
                        // Continuar para baixar do R2
                    } else {
                        throw error;
                    }
                }
            }
        } catch (dbError) {
            // Se erro ao buscar do banco, continuar para download
            console.warn(`[Timeout Media] Erro ao buscar file_id do banco:`, dbError.message);
        }
    }
    
    // Se não tem cache ou file_id expirou, baixar do R2 e fazer upload
    const axios = require('axios');
    const FormData = require('form-data');
    
    // Baixar arquivo do R2
    const fileResponse = await axios.get(storageUrl, {
        responseType: 'arraybuffer',
        timeout: 120000
    });
    
    const fileBuffer = Buffer.from(fileResponse.data);
    const contentType = fileResponse.headers['content-type'] || 
        (fileType === 'image' ? 'image/jpeg' : 
         fileType === 'video' ? 'video/mp4' : 
         'audio/ogg');
    
    // Criar FormData para upload
    const formData = new FormData();
    formData.append('chat_id', chatId);
    
    const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[fileType];
    const field = { image: 'photo', video: 'video', audio: 'voice' }[fileType];
    const fileName = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' }[fileType];
    
    formData.append(field, fileBuffer, {
        filename: fileName,
        contentType: contentType
    });
    
    if (caption) {
        formData.append('caption', caption);
        formData.append('parse_mode', 'HTML');
    }
    
    const timeout = fileType === 'video' ? 120000 : 60000;
    const response = await sendTelegramRequest(botToken, method, formData, {
        headers: formData.getHeaders(),
        timeout
    }, 3, 1500, null);
    
    // Extrair file_id da resposta e salvar no banco
    if (response.ok && response.result && mediaId && botId && botId !== null && botId !== undefined) {
        let fileId = null;
        if (fileType === 'image' && response.result.photo) {
            fileId = Array.isArray(response.result.photo) 
                ? response.result.photo[response.result.photo.length - 1].file_id 
                : response.result.photo.file_id;
        } else if (fileType === 'video' && response.result.video) {
            fileId = response.result.video.file_id;
        } else if (fileType === 'audio' && response.result.voice) {
            fileId = response.result.voice.file_id;
        }
        
        if (fileId && fileId !== null && fileId !== undefined) {
            // Salvar no banco
            try {
                const botIdStr = String(botId);
                const fileIdStr = String(fileId);
                if (!botIdStr || botIdStr === 'null' || botIdStr === 'undefined') {
                    console.warn(`[Timeout Media] botId inválido ao salvar file_id: ${botId}`);
                    return response;
                }
                if (!fileIdStr || fileIdStr === 'null' || fileIdStr === 'undefined') {
                    console.warn(`[Timeout Media] fileId inválido ao salvar: ${fileId}`);
                    return response;
                }
                await sqlWithRetry(
                    'UPDATE media_library SET telegram_file_ids = COALESCE(telegram_file_ids, \'{}\'::jsonb) || jsonb_build_object($1::text, $2::text) WHERE id = $3',
                    [botIdStr, fileIdStr, mediaId]
                );
                
                // Adicionar ao cache em memória
                if (global.mediaFileIdCache) {
                    const cacheKey = `telegram_file_id_${mediaId}_${botId}`;
                    global.mediaFileIdCache.set(cacheKey, fileId);
                }
            } catch (saveError) {
                // Não crítico se não conseguir salvar
                console.warn(`[Timeout Media] Erro ao salvar file_id no banco:`, saveError.message);
            }
        }
    }
    
    return response;
}

/**
 * Normaliza payload do Telegram removendo undefined e garantindo tipos corretos
 */
function normalizeTelegramPayload(payload) {
    const normalized = {};
    for (const [key, value] of Object.entries(payload)) {
        if (value !== undefined) {
            // Converter null para string vazia em campos opcionais de texto
            if (value === null && (key === 'caption' || key === 'text')) {
                normalized[key] = '';
            } else {
                normalized[key] = value;
            }
        }
    }
    
    // Remover parse_mode se caption estiver vazio ou ausente
    // Telegram não aceita parse_mode sem caption
    if ((!normalized.caption || normalized.caption === '') && normalized.parse_mode) {
        delete normalized.parse_mode;
    }
    
    return normalized;
}

async function handleMediaNode(node, botToken, chatId, caption, botId = null, sellerId = null) {
    // Normalizar caption para evitar UNDEFINED_VALUE (Telegram não aceita undefined)
    caption = caption || "";
    
    // Aceitar tanto node quanto action (action pode não ter id)
    const nodeId = node.id || node.nodeId || 'unknown';
    const type = node.type;
    const nodeData = node.data || {};
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    const fileIdentifier = nodeData[urlMap[type]];

    if (!fileIdentifier) {
        console.warn(`[Flow Media] Nenhum file_id ou URL fornecido para o nó de ${type} ${nodeId}`);
        return null;
    }

    // Validar file_id antes de usar
    if (typeof fileIdentifier !== 'string' || fileIdentifier.trim() === '') {
        console.warn(`[Timeout Media] File ID inválido ou vazio para o nó de ${type} ${nodeId}`);
        return null;
    }

    // Verificar cache antes de processar mídia
    if (chatId && chatId !== 'unknown' && chatId !== null) {
        if (botId && await dbCache.isBotBlocked(botId, chatId)) {
            // Removido log de cache hit
            return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
        } else if (!botId && await dbCache.isBotTokenBlocked(botToken, chatId)) {
            // Removido log de cache hit
            return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
        }
    }

    // Validar formato de file_id (prefixos conhecidos ou R2)
    const isLibraryFile = fileIdentifier.startsWith('BAAC') || fileIdentifier.startsWith('AgAC') || 
                         fileIdentifier.startsWith('AwAC') || fileIdentifier.startsWith('R2_');
    let response;
    const timeout = type === 'video' ? 120000 : 30000; // Timeout maior para vídeos

    if (isLibraryFile) {
        if (type === 'audio') {
            const duration = parseInt(nodeData.durationInSeconds, 10) || 0;
            if (duration > 0) {
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'record_voice' }, {}, 3, 1500, botId);
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            }
        }
        
        // Buscar mídia no banco para verificar se tem storage_url (ou usar dados do cache se disponíveis)
        let media = null;
        // Verificar se node já tem storageUrl (otimização - dados vêm do cache/step processado)
        if (nodeData.storageUrl && nodeData.storageType === 'r2') {
            // Usar dados já disponíveis no node (cache hit - sem query ao banco!)
            media = {
                id: nodeData.mediaLibraryId,
                storage_url: nodeData.storageUrl,
                storage_type: 'r2',
            };
        } else if (sellerId) {
            // Fallback: buscar do banco (mantém compatibilidade)
            try {
                const [mediaResult] = await sqlWithRetry(
                    'SELECT id, file_id, storage_url, storage_type FROM media_library WHERE file_id = $1 AND seller_id = $2 LIMIT 1',
                    [fileIdentifier, sellerId]
                );
                if (mediaResult) {
                    media = mediaResult;
                }
            } catch (error) {
                console.warn(`[Timeout Media] Erro ao buscar mídia no banco:`, error.message);
            }
        }
        
        // Tentar usar storage_url se disponível (apenas R2)
        if (media && media.storage_url && media.storage_type === 'r2') {
            response = await sendMediaFromR2(botToken, chatId, media.storage_url, type, caption, media.id, botId);
        } else if (media && media.storage_type !== 'r2') {
            // Mídia não está no R2 - não suportado mais
            console.warn(`[Timeout Media] Mídia ${media.id} não está armazenada no R2. Apenas mídias no R2 são suportadas.`);
            return null;
        } else {
            // Mídia não encontrada
            console.warn(`[Timeout Media] Mídia não encontrada na biblioteca para file_id: ${fileIdentifier}`);
            return null;
        }
    } else {
        // Se não é da biblioteca, pode ser URL direta ou file_id de outro bot
        if (fileIdentifier.startsWith('http://') || fileIdentifier.startsWith('https://')) {
            // URL direta - Telegram pode baixar e enviar
            const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
            const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
            const method = methodMap[type];
            const field = fieldMap[type];
            
            if (!method) {
                console.warn(`[Timeout Media] Tipo de mídia não suportado: ${type}`);
                return null;
            }
            
            const payload = normalizeTelegramPayload({ 
                chat_id: chatId, 
                [field]: fileIdentifier, 
                caption: caption || "" 
            });
            try {
            response = await sendTelegramRequest(botToken, method, payload, { timeout }, 3, 1500, botId);
            } catch (urlError) {
                const urlErrorMessage = urlError.message || urlError.description || '';
                const urlErrorResponseDesc = urlError.response?.data?.description || '';
                if (urlErrorMessage.includes('wrong remote file identifier') || 
                    urlErrorResponseDesc.includes('wrong remote file identifier')) {
                    console.warn(`[Timeout Media] File ID inválido para URL ${type} (nó ${node.id}). Pulando envio.`);
                    return null;
                }
                throw urlError;
            }
        } else {
            // file_id direto de outro bot
            const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
            const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
            const method = methodMap[type];
            const field = fieldMap[type];
            
            const payload = normalizeTelegramPayload({ 
                chat_id: chatId, 
                [field]: fileIdentifier, 
                caption: caption || "" 
            });
            try {
            response = await sendTelegramRequest(botToken, method, payload, { timeout }, 3, 1500, botId);
            } catch (fileIdError) {
                const fileIdErrorMessage = fileIdError.message || fileIdError.description || '';
                const fileIdErrorResponseDesc = fileIdError.response?.data?.description || '';
                if (fileIdErrorMessage.includes('wrong remote file identifier') || 
                    fileIdErrorResponseDesc.includes('wrong remote file identifier')) {
                    console.warn(`[Timeout Media] File ID inválido para ${type} (nó ${node.id}). Pulando envio.`);
                    return null;
                }
                throw fileIdError;
            }
        }
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
    // Verificar cache antes de enviar
    if (chatId && chatId !== 'unknown' && chatId !== null) {
        if (await dbCache.isBotTokenBlocked(botToken, chatId)) {
            // Removido log de cache hit
            return;
        }
    }
    
    try {
        const response = await sendTelegramRequest(botToken, 'sendChatAction', { 
            chat_id: chatId, 
            action: 'typing' 
        }, {}, 3, 1500, null);
        
        // Se retornou erro 403, o cache já foi atualizado pela sendTelegramRequest
        if (response && !response.ok && response.error_code === 403) {
            // Removido log - não é crítico
            return;
        }
    } catch (error) {
        const errorData = error.response?.data;
        const description = errorData?.description || error.message;
        if (description?.includes('bot was blocked by the user')) {
            // Removido log - não é crítico
            return;
        }
        // Logar apenas em desenvolvimento ou se for erro crítico
        if (shouldLogDebug() && error.response?.status !== 403) {
            console.warn(`[WORKER - Flow Engine] Falha ao enviar ação 'typing':`, errorData || error.message);
        }
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping, typingDelay = 0, variables = {}) {
  if (!text || text.trim() === '') return;
  
  // Verificar cache antes de enviar
  if (chatId && chatId !== 'unknown' && chatId !== null) {
      if (botId && await dbCache.isBotBlocked(botId, chatId)) {
          // Removido log de cache hit
          return;
      } else if (!botId && await dbCache.isBotTokenBlocked(botToken, chatId)) {
          // Removido log de cache hit
          return;
      }
  }
  
  try {
        if (showTyping) {
            // Use o delay definido no frontend (convertido para ms), ou um fallback se não for definido
            let typingDurationMs = (typingDelay && typingDelay > 0) 
                ? (typingDelay * 1000) 
                : Math.max(500, Math.min(2000, text.length * 50));
            await showTypingForDuration(chatId, botToken, typingDurationMs);
        }
        
        const response = await sendTelegramRequest(botToken, 'sendMessage', { 
            chat_id: chatId, 
            text: text, 
            parse_mode: 'HTML' 
        }, {}, 3, 1500, botId);
        
        if (response && response.ok && response.result) {
            const sentMessage = response.result;
            await sqlTx`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, click_id)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, NULL, NULL, NULL, ${text}, 'bot', ${variables.click_id || null})
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        } else if (response && !response.ok && response.error_code === 403) {
            // Se retornou erro 403, o cache já foi atualizado pela sendTelegramRequest
            // Removido log - não é crítico
            return;
        }
    } catch (error) {
        const errorData = error.response?.data;
        const description = errorData?.description || error.message;
        if (description?.includes('bot was blocked by the user')) {
            // Removido log - não é crítico
            return;
        }
        // Logar apenas erros críticos
        if (shouldLogDebug() && error.response?.status !== 403) {
            console.error(`[WORKER - Flow Engine] Erro ao enviar/salvar mensagem:`, errorData || error.message);
        }
    }
}

// /backend/worker/process-timeout.js (Função processActions CORRIGIDA)

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
                    // Removido log de debug
                }
                if (!variables.nome_completo) {
                    variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
                    // Removido log de debug
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
                // Removido log de debug
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
                // Removido log de debug
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
                    // Removido log de debug
                }
                if (!variables.estado) {
                    variables.estado = click.state || '';
                    // Removido log de debug
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
        console.warn(`${logPrefix} Erro ao buscar variáveis do banco (não crítico):`, error.message);
    }
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
            console.error(`[JSON] Falha ao converter ${context}:`, error);
            throw new Error(`JSON_PARSE_ERROR_${context}`);
        }
    }
    return value;
}

/**
 * =================================================================
 * FUNÇÃO 'processActions' (O EXECUTOR) - VERSÃO NOVA
 * =================================================================
 * (Colada da sua resposta anterior)
 */
async function processActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix = '[Actions]', currentNodeId = null, flowId = null, flowNodes = null, flowEdges = null) {
    // Removido log de debug - não é necessário em produção
    
    // Garantir que variáveis faltantes sejam buscadas do banco
    await ensureVariablesFromDatabase(chatId, botId, sellerId, variables, logPrefix);
    
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
                                console.log(`${logPrefix} [Flow Message] Adicionando click_id ${cleanClickId} ao botão de ${urlType}`);
                            } catch (urlError) {
                                console.error(`${logPrefix} [Flow Message] Erro ao processar URL: ${urlError.message}`);
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
                                console.warn(`${logPrefix} [Flow Message] Erro ao substituir localhost na URL: ${urlError.message}`);
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
                                    await sqlTx`
                                        UPDATE clicks 
                                        SET checkout_id = ${checkoutId}
                                        WHERE click_id = ${db_click_id} 
                                          AND seller_id = ${sellerId}
                                          AND (checkout_id IS NULL OR checkout_id != ${checkoutId})
                                    `;
                                    
                                    // Sempre atualizar checkout_sent_at quando checkout_id corresponde
                                    await sqlTx`
                                        UPDATE clicks 
                                        SET checkout_sent_at = NOW()
                                        WHERE click_id = ${db_click_id} 
                                          AND seller_id = ${sellerId}
                                          AND checkout_id = ${checkoutId}
                                    `;
                                    
                                    // Buscar o click para obter o click_id_internal
                                    const [clickRecord] = await sqlTx`
                                        SELECT id FROM clicks 
                                        WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                                    `;
                                    
                                    if (clickRecord) {
                                        // Criar uma nova transação PIX pendente toda vez que o checkout for enviado
                                        const tempTransactionId = `checkout_${checkoutId}_${clickRecord.id}_${Date.now()}`;
                                        await sqlTx`
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
                                        `;
                                        console.log(`${logPrefix} [Checkout Button] Transação PIX pendente criada para checkout ${checkoutId}`);
                                    }
                                    
                                    console.log(`${logPrefix} [Checkout Button] checkout_id ${checkoutId} e checkout_sent_at marcados no click para rastreamento`);
                                }
                            } catch (checkoutError) {
                                console.error(`${logPrefix} [Checkout Button] Erro ao marcar checkout_id:`, checkoutError.message);
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
                        
                        const response = await sendTelegramRequest(botToken, 'sendMessage', payload, {}, 3, 1500, botId);
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
                    
                    // Normalizar caption para evitar UNDEFINED_VALUE (Telegram não aceita undefined)
                    caption = caption || "";
                    
                    // Validação do tamanho da legenda (limite do Telegram: 1024 caracteres)
                    if (caption && caption.length > 1024) {
                        console.warn(`${logPrefix} [Flow Media] Legenda excede limite de 1024 caracteres. Truncando...`);
                        caption = caption.substring(0, 1021) + '...';
                    }
                    
                    const response = await handleMediaNode(action, botToken, chatId, caption, botId, sellerId);

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
                
                // Se o delay for maior que 60 segundos, agendar via BullMQ
                if (delaySeconds > 60) {
                    console.log(`${logPrefix} [Delay] Delay longo detectado (${delaySeconds}s). Agendando via BullMQ...`);
                    
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
                        const response = await addJobWithDelay(
                            QUEUE_NAMES.TIMEOUT,
                            'process-timeout',
                            {
                                chat_id: chatId,
                                bot_id: botId,
                                target_node_id: resolvedCurrentNodeId, // Continuar do mesmo nó
                                variables: variables,
                                continue_from_delay: true, // Flag para indicar que é continuação após delay
                                remaining_actions: remainingActions.length > 0 ? JSON.stringify(remainingActions) : null,
                                flow_id: flowId,
                                flow_nodes: JSON.stringify({ nodes: resolvedFlowNodes, edges: resolvedFlowEdges })
                            },
                            {
                                delay: `${delaySeconds}s`,
                                jobId: `timeout-delay-${chatId}-${botId}-${Date.now()}`
                            }
                        );
                        
                        // Salvar scheduled_message_id no estado
                        await sqlWithRetry(sqlTx`
                            UPDATE user_flow_states 
                            SET scheduled_message_id = ${response.jobId}
                            WHERE chat_id = ${chatId} AND bot_id = ${botId}
                        `);
                        
                        console.log(`${logPrefix} [Delay] Delay de ${delaySeconds}s agendado via BullMQ. Tarefa: ${response.jobId}`);
                        
                        // Retornar código especial para processFlow saber que parou
                        return 'delay_scheduled';
                    } catch (error) {
                        console.error(`${logPrefix} [Delay] Erro ao agendar delay via BullMQ:`, error.message);
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
                    // Removido log de debug

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
                            },
                            {},
                            3,
                            1500,
                            botId
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
                        invitePayload,
                        {},
                        3,
                        1500,
                        botId
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
                    // Removido log de debug

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
                            },
                            {},
                            3,
                            1500,
                            botId
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
                                    },
                                    {},
                                    3,
                                    1500,
                                    botId
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
                    // Removido log de debug
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
                    // Removido log de debug - não é necessário em produção
                    
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
                    // Removido log de debug - não é necessário em produção
                    
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
                    // Removido log de debug - não é necessário em produção
    
                    // Envia InitiateCheckout para Meta se o click veio de pressel ou checkout
                    if (click.pressel_id || click.checkout_id) {
                        await sendMetaEventShared({
                            eventName: 'InitiateCheckout',
                            clickData: click,
                            transactionData: { id: pixResult.internal_transaction_id, pix_value: valueInCents / 100 },
                            customerData: null,
                            sqlTx: sqlTx
                        });
                        // Removido log de debug - não é necessário em produção
                    }
                } catch (error) {
                    console.error(`${logPrefix} Erro no nó action_pix para chat ${chatId}:`, error.message);
                    // Re-lança o erro para que o fluxo seja interrompido
                    throw error;
                }
                break;

            case 'action_check_pix':
                try {
                    if (!variables.click_id) {
                        throw new Error("click_id não encontrado nas variáveis do fluxo.");
                    }
                    
                    const db_click_id = variables.click_id.startsWith('/start ') 
                        ? variables.click_id 
                        : `/start ${variables.click_id}`;
                    
                    // Buscar click
                    const [click] = await sqlTx`
                        SELECT id, checkout_id, checkout_sent_at FROM clicks 
                        WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}
                    `;
                    
                    if (!click) {
                        throw new Error(`Click não encontrado para click_id: ${variables.click_id}`);
                    }
                    
                    let transaction = null;
                    
                    // Se o click tem checkout_id e checkout_sent_at, buscar último PIX gerado (do checkout OU do fluxo após checkout ser enviado)
                    if (click.checkout_id && click.checkout_sent_at) {
                        // Removido log de debug
                        
                        [transaction] = await sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                              AND (
                                checkout_id = ${click.checkout_id} 
                                OR (checkout_id IS NULL AND created_at > ${click.checkout_sent_at})
                              )
                            ORDER BY created_at DESC
                            LIMIT 1
                        `;
                    } else if (click.checkout_id) {
                        // Tem checkout_id mas não tem checkout_sent_at (compatibilidade com dados antigos)
                        // Removido log de debug
                        
                        [transaction] = await sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                              AND (checkout_id = ${click.checkout_id} OR checkout_id IS NULL)
                            ORDER BY created_at DESC
                            LIMIT 1
                        `;
                    } else {
                        // Não tem checkout_id, buscar qualquer PIX do click_id
                        console.log(`${logPrefix} [action_check_pix] Click não tem checkout_id. Buscando qualquer PIX do click_id.`);
                        
                        [transaction] = await sqlTx`
                            SELECT * FROM pix_transactions 
                            WHERE click_id_internal = ${click.id}
                            ORDER BY created_at DESC
                            LIMIT 1
                        `;
                    }
                    
                    if (!transaction) {
                        throw new Error("Nenhuma transação PIX encontrada para este click_id.");
                    }
                    
                    console.log(`${logPrefix} [action_check_pix] Transação encontrada: ${transaction.provider_transaction_id}, status: ${transaction.status}`);
                    
                    // Verificar status
                    if (transaction.status === 'paid') {
                        // Tentar enviar eventos (handleSuccessfulPayment é idempotente)
                        await handleSuccessfulPayment(transaction.id, {});
                        return 'paid';
                    }
                    
                    return 'pending';
                } catch (error) {
                    console.error(`${logPrefix} [action_check_pix] Erro: ${error.message}`);
                    return 'pending';
                }
            
            case 'forward_flow':
                const targetFlowId = actionData.targetFlowId;
                if (!targetFlowId) {
                    console.error(`${logPrefix} 'forward_flow' action não tem targetFlowId.`);
                    break; 
                }
                
                // Removido log de debug
                
                // Cancela qualquer tarefa de timeout pendente antes de encaminhar para o novo fluxo
                try {
                    const [stateToCancel] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    if (stateToCancel && stateToCancel.scheduled_message_id) {
                        try {
                            await removeJob(QUEUE_NAMES.TIMEOUT, stateToCancel.scheduled_message_id);
                            console.log(`${logPrefix} [Forward Flow] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                        } catch (e) {
                            console.warn(`${logPrefix} [Forward Flow] Falha ao cancelar job BullMQ ${stateToCancel.scheduled_message_id}:`, e.message);
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
                        // Removido log de debug
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
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}, flowNodes = null, flowEdges = null, flowId = null, renewLockCallback = null) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    // Removido log de debug - não é necessário em produção

    // ==========================================================
    // PASSO 1: CARREGAR VARIÁVEIS DO USUÁRIO E DO CLIQUE
    // ==========================================================
    let variables = { ...initialVariables };

    // Garantir que variáveis faltantes sejam buscadas do banco
    await ensureVariablesFromDatabase(chatId, botId, sellerId, variables, logPrefix);
    
    // ==========================================================
    // CARREGAR VARIÁVEIS DO BANCO DE DADOS (se existir estado)
    // Isso garante que variáveis atualizadas (ex: last_transaction_id)
    // estejam disponíveis mesmo quando processFlow é chamado com startNodeId
    // ==========================================================
    const [userStateForVars] = await sqlWithRetry(sqlTx`
        SELECT variables 
        FROM user_flow_states 
        WHERE chat_id = ${chatId} AND bot_id = ${botId}
    `);
    
    if (userStateForVars && userStateForVars.variables) {
        let parsedDbVariables = {};
        try {
            parsedDbVariables = typeof userStateForVars.variables === 'string' 
                ? JSON.parse(userStateForVars.variables) 
                : userStateForVars.variables;
        } catch (e) {
            parsedDbVariables = userStateForVars.variables || {};
        }
        // Mescla variáveis do banco com as iniciais (variáveis iniciais têm prioridade)
        variables = { ...parsedDbVariables, ...variables };
        // Removido log de debug - não é necessário em produção
    }
    // ==========================================================
    // FIM DO PASSO 1
    // ==========================================================
    // Se os dados do fluxo foram fornecidos (ex: forward_flow), usa eles. Caso contrário, busca do banco.
    let nodes, edges;
    let currentFlowId = null; // Armazena o ID do fluxo atual para rastreamento
    if (flowNodes && flowEdges && Array.isArray(flowNodes) && flowNodes.length > 0 && Array.isArray(flowEdges) && flowEdges.length > 0) {
        // Usa os dados do fluxo fornecido (do forward_flow)
        nodes = flowNodes;
        edges = flowEdges;
        if (flowId) {
            currentFlowId = flowId; // Usa o flowId fornecido para rastreamento
            // Removido log de debug
        } else {
            // Tenta buscar o flowId do banco usando bot_id e nodes fornecidos
            // Como não temos uma forma direta de identificar o fluxo pelos nodes, deixa null
            // O contador não funcionará neste caso, mas o fluxo continuará funcionando
            // Removido log de debug
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
                // Removido log de debug
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
            // Removido log de debug
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
            // Removido log de debug
            const [stateToCancel] = await sqlWithRetry(sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            if (stateToCancel && stateToCancel.scheduled_message_id) {
                try {
                    await removeJob(QUEUE_NAMES.TIMEOUT, stateToCancel.scheduled_message_id);
                    // Removido log de debug
                } catch (e) { 
                    // Removido log de warn - não é crítico
                }
            }
            await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            const startNode = nodes.find(node => node.type === 'trigger');
            currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;

        } else {
            const [userState] = await sqlWithRetry(sqlTx`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
            if (userState && userState.waiting_for_input) {
                // Removido log de debug
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
                let parsedVariables = {};
                try { parsedVariables = JSON.parse(userState.variables); } catch (e) { parsedVariables = userState.variables; }
                variables = { ...variables, ...parsedVariables };

            } else {
                // Removido log de debug
                await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
                const startNode = nodes.find(node => node.type === 'trigger');
                currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;
            }
        }
    }


    if (!currentNodeId) {
        // Removido log de debug
        await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
        return;
    }

    // ==========================================================
    // PASSO 3: O NOVO LOOP DE NAVEGAÇÃO
    // ==========================================================
    let safetyLock = 0;
    let lastLockRenewal = Date.now();
    const LOCK_RENEWAL_INTERVAL_MS = 1 * 60 * 1000; // 1 minuto - renovação mais frequente (reduzido de 3min)
    // currentFlowId já foi determinado acima
    
    while (currentNodeId && safetyLock < 20) {
        safetyLock++;
        let currentNode = nodes.find(node => node.id === currentNodeId);
        
        if (!currentNode) {
            console.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        // Renovar lock durante processamento de flows longos
        // Renovar a cada 2 nós processados OU a cada 1 minuto (o que ocorrer primeiro)
        const now = Date.now();
        const shouldRenewByTime = (now - lastLockRenewal) >= LOCK_RENEWAL_INTERVAL_MS;
        const shouldRenewByProgress = safetyLock % 2 === 0; // A cada 2 nós (mais frequente)
        
        if (renewLockCallback && (shouldRenewByTime || shouldRenewByProgress)) {
            await renewLockCallback(safetyLock);
            lastLockRenewal = now;
        }

        // Removido log de debug

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
                     // Removido log de debug
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
                // Removido log de debug
                currentNodeId = null; // Para o loop atual
                break; // Sai do 'while'
            }
            
            // Se delay foi agendado, parar processamento
            if (actionResult === 'delay_scheduled') {
                // Removido log de debug
                currentNodeId = null;
                break;
            }

            if (currentNode.data.waitForReply) {
                const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                const timeoutMinutes = currentNode.data.replyTimeout || 5;

                try {
                    // Agenda o worker de timeout com uma única chamada
                    const response = await addJobWithDelay(
                        QUEUE_NAMES.TIMEOUT,
                        'process-timeout',
                        {
                            chat_id: chatId,
                            bot_id: botId,
                            target_node_id: noReplyNodeId, // Pode ser null, e o worker saberá encerrar
                            variables: variables,
                            flow_id: currentFlowId,
                            flow_nodes: JSON.stringify({ nodes, edges })
                        },
                        {
                            delay: `${timeoutMinutes}m`,
                            jobId: `timeout-${chatId}-${botId}-${Date.now()}`
                        }
                    );

                    // Salva o estado como "esperando" e armazena o ID da tarefa agendada
                    // Mantém o flow_id existente (não atualiza para não perder a referência ao fluxo correto)
                    await sqlWithRetry(sqlTx`
                        UPDATE user_flow_states
                        SET waiting_for_input = true, scheduled_message_id = ${response.jobId}
                        WHERE chat_id = ${chatId} AND bot_id = ${botId}`);

                    // Removido log de debug

                } catch (error) {
                    console.error(`${logPrefix} [Flow Engine] Erro CRÍTICO ao agendar timeout no BullMQ:`, error);
                }

                currentNodeId = null; // PARA o loop
                break; // Sai do 'while'
            }
            
            if (actionResult === 'paid') {
                // Removido log de debug
                currentNodeId = findNextNode(currentNode.id, 'a', edges);
                continue;
            }
            if (actionResult === 'pending') {
                // Removido log de debug
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
            // Removido log de debug
        } else if (state.waiting_for_input) {
            // Fluxo pausado aguardando resposta do usuário
            // Removido log de debug
        } else if (state.scheduled_message_id) {
            // Delay agendado via BullMQ - estado deve ser preservado
            // Removido log de debug
        } else {
            // Fluxo realmente terminou - pode limpar o estado
            // Removido log de debug
            await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`);
        }
    }
}
// ==========================================================

// Função pura que processa timeout sem depender de objetos HTTP (req/res)
// Permite reutilização em outros contextos (CLI, jobs, filas, etc.)
async function processTimeoutData(data, job = null) {
    try {
        const { chat_id, bot_id, target_node_id, variables, continue_from_delay, remaining_actions, is_disparo, history_id, disparo_flow_id, flow_nodes, flow_id } = data;
        const logPrefix = '[WORKER]';
        
        // Renovação periódica de lock para evitar jobs stalled em flows longos
        let lastLockRenewal = Date.now();
        const LOCK_RENEWAL_INTERVAL_MS = 1 * 60 * 1000; // 1 minuto - renovação mais frequente (reduzido de 3min)
        let nodesProcessed = 0;
        
        // Função para renovar lock durante processamento de flows longos
        const renewLockIfNeeded = async (currentNodesProcessed = 0) => {
            const now = Date.now();
            const shouldRenewByTime = (now - lastLockRenewal) >= LOCK_RENEWAL_INTERVAL_MS;
            const shouldRenewByProgress = currentNodesProcessed > 0 && currentNodesProcessed % 2 === 0; // A cada 2 nós (mais frequente)
            
            if ((shouldRenewByTime || shouldRenewByProgress) && job && typeof job.updateProgress === 'function') {
                try {
                    // Estimar progresso baseado em nós processados (se flowNodes disponível)
                    const progress = Math.min(currentNodesProcessed * 10, 90); // Máximo 90% até concluir
                    await job.updateProgress(Math.max(progress, 1));
                    lastLockRenewal = now;
                    console.debug(`[WORKER-TIMEOUT] Lock renovado para job ${job.id} (nós processados: ${currentNodesProcessed})`);
                } catch (renewError) {
                    // Se job foi concluído ou removido, não é erro crítico
                    if (renewError.message?.includes('not found') || 
                        renewError.message?.includes('completed') ||
                        renewError.message?.includes('removed')) {
                        return;
                    }
                    console.debug(`[WORKER-TIMEOUT] Erro ao renovar lock (não crítico):`, renewError.message);
                }
            }
        };
        
        // Renovação durante delays longos (se job está aguardando delay antes de processar)
        // Detectar se job está em delay: quando não há continue_from_delay mas há remaining_actions com delay
        let delayRenewTimer = null;
        if (!continue_from_delay && remaining_actions && job && typeof job.updateProgress === 'function') {
            try {
                const remainingActionsJson = typeof remaining_actions === 'string' 
                    ? JSON.parse(remaining_actions) 
                    : remaining_actions;
                // Verificar se há delay nas ações restantes
                const hasDelay = Array.isArray(remainingActionsJson) && remainingActionsJson.some(action => 
                    action.type === 'delay' && action.data && action.data.delaySeconds > 60
                );
                
                if (hasDelay) {
                    // Iniciar renovação periódica durante delay
                    delayRenewTimer = setInterval(async () => {
                        try {
                            await job.updateProgress(1);
                            lastLockRenewal = Date.now();
                            console.debug(`[WORKER-TIMEOUT] Lock renovado durante delay para job ${job.id}`);
                        } catch (renewError) {
                            if (renewError.message?.includes('not found') || 
                                renewError.message?.includes('completed') ||
                                renewError.message?.includes('removed')) {
                                if (delayRenewTimer) {
                                    clearInterval(delayRenewTimer);
                                    delayRenewTimer = null;
                                }
                                return;
                            }
                            console.debug(`[WORKER-TIMEOUT] Erro ao renovar lock durante delay (não crítico):`, renewError.message);
                        }
                    }, LOCK_RENEWAL_INTERVAL_MS);
                }
            } catch (e) {
                // Ignorar erro ao parsear remaining_actions
            }
        }

        // Validação de dados obrigatórios
        if (!chat_id || !bot_id) {
            throw new Error('chat_id e bot_id são obrigatórios.');
        }

        // Removido log de debug - não é necessário em produção

        // Buscar bot para obter o token e sellerId
        const [bot] = await sqlWithRetry(sqlTx`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`);
        if (!bot || !bot.bot_token) {
            throw new Error(`Bot ${bot_id} ou token não encontrado.`);
        }
        const botToken = bot.bot_token;
        const sellerId = bot.seller_id;

        // Verificar estado atual do usuário
        const [currentState] = await sqlWithRetry(sqlTx`
            SELECT waiting_for_input, scheduled_message_id, current_node_id, flow_id, variables as state_variables
            FROM user_flow_states 
            WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);

        // #region agent log
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2306',message:'Estado do usuário verificado antes de processar timeout',data:{chatId:chat_id,botId:bot_id,hasState:!!currentState,waitingForInput:currentState?.waiting_for_input,scheduledMessageId:currentState?.scheduled_message_id,continueFromDelay:continue_from_delay,jobId:job?.id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'C'}));
        // #endregion

        // Verificações para determinar se este timeout deve ser processado
        if (!currentState) {
            // Limpar timer de renovação durante delay se ainda estiver ativo
            if (delayRenewTimer) {
                clearInterval(delayRenewTimer);
                delayRenewTimer = null;
            }
            // #region agent log
            console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2323',message:'RETORNANDO: no_user_state',data:{chatId:chat_id,botId:bot_id,reason:'no_user_state'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
            // #endregion
            return { ignored: true, reason: 'no_user_state' };
        }
        
        // Verificar se é disparo (pode vir do body ou das variables do estado)
        let isDisparoTimeout = is_disparo === true;
        let disparoHistoryId = history_id;
        let disparoFlowId = disparo_flow_id;
        
        if (!isDisparoTimeout && currentState.state_variables) {
            try {
                const stateVars = typeof currentState.state_variables === 'string' 
                    ? JSON.parse(currentState.state_variables) 
                    : currentState.state_variables;
                if (stateVars._disparo_is_waiting === true || stateVars._disparo_history_id) {
                    isDisparoTimeout = true;
                    disparoHistoryId = stateVars._disparo_history_id || history_id;
                    disparoFlowId = stateVars._disparo_flow_id || disparo_flow_id || currentState.flow_id;
                }
            } catch (e) {
                console.warn(`${logPrefix} [Timeout] Erro ao parsear variables do estado:`, e.message);
            }
        }
        
        // #region agent log
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2345',message:'Depois de definir isDisparoTimeout',data:{chatId:chat_id,botId:bot_id,isDisparoTimeout,is_disparo:is_disparo,target_node_id,disparoHistoryId,disparoFlowId,flow_nodes_exists:!!flow_nodes,flow_nodes_type:typeof flow_nodes},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
        // #endregion
        
        // Se é continuação após delay, não verificar waiting_for_input
        if (!continue_from_delay) {
            // #region agent log
            console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2343',message:'Verificando waiting_for_input antes de processar timeout',data:{chatId:chat_id,botId:bot_id,waitingForInput:currentState.waiting_for_input,willIgnore:!currentState.waiting_for_input,jobId:job?.id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'C'}));
            // #endregion
            
            if (!currentState.waiting_for_input) {
                // Limpar timer de renovação durante delay se ainda estiver ativo
                if (delayRenewTimer) {
                    clearInterval(delayRenewTimer);
                    delayRenewTimer = null;
                }
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2351',message:'Timeout ignorado - usuário já respondeu',data:{chatId:chat_id,botId:bot_id,reason:'user_already_proceeded',jobId:job?.id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'C'}));
                // #endregion
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2361',message:'RETORNANDO: user_already_proceeded',data:{chatId:chat_id,botId:bot_id,reason:'user_already_proceeded'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
                // #endregion
                return { ignored: true, reason: 'user_already_proceeded' };
            }

            // O usuário NÃO respondeu a tempo
            // Removido log de debug - não é necessário em produção
            
            // Limpa o estado de 'espera' ANTES de processar o próximo nó
            await sqlWithRetry(sqlTx`
                UPDATE user_flow_states 
                SET waiting_for_input = false, scheduled_message_id = NULL
                WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);
        } else {
            // Removido log of debug
        }
        
        // #region agent log
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2379',message:'ANTES verificar se é disparo',data:{chatId:chat_id,botId:bot_id,isDisparoTimeout,is_disparo:is_disparo,target_node_id,willEnterDisparoBlock:isDisparoTimeout && target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
        // #endregion
        
        // Se é disparo, processar usando processDisparoFlow
        if (isDisparoTimeout && target_node_id) {
            // #region agent log
            console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2385',message:'ENTRANDO no bloco de disparo',data:{chatId:chat_id,botId:bot_id,disparoHistoryId,disparoFlowId},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
            // #endregion
            try {
                if (!disparoFlowId) {
                    throw new Error('disparo_flow_id não encontrado para continuar disparo após timeout.');
                }
                
                const [disparoFlow] = await sqlWithRetry(sqlTx`
                    SELECT nodes FROM disparo_flows WHERE id = ${disparoFlowId}
                `);
                
                if (!disparoFlow || !disparoFlow.nodes) {
                    throw new Error(`Fluxo de disparo ${disparoFlowId} não encontrado.`);
                }
                
                const flowData = typeof disparoFlow.nodes === 'string' ? JSON.parse(disparoFlow.nodes) : disparoFlow.nodes;
                const flowNodes = flowData.nodes || [];
                const flowEdges = flowData.edges || [];
                
                // Limpar flags de disparo das variables
                const cleanVariables = { ...variables };
                if (cleanVariables._disparo_is_waiting !== undefined) delete cleanVariables._disparo_is_waiting;
                if (cleanVariables._disparo_history_id !== undefined) delete cleanVariables._disparo_history_id;
                if (cleanVariables._disparo_flow_id !== undefined) delete cleanVariables._disparo_flow_id;
                
                // Importar e chamar processDisparoFlow
                const { processDisparoFlow } = require('./process-disparo');
                await processDisparoFlow(chat_id, bot_id, botToken, sellerId, target_node_id, cleanVariables, flowNodes, flowEdges, disparoHistoryId);
                
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2420',message:'RETORNANDO: disparo processado',data:{chatId:chat_id,botId:bot_id,is_disparo:true},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
                // #endregion
                return { processed: true, is_disparo: true };
            } catch (error) {
                console.error(`${logPrefix} [Timeout] Erro ao processar timeout de disparo:`, error);
                throw error;
            }
        }

        // #region agent log
        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2412',message:'ANTES do if(target_node_id) - fluxo normal',data:{chatId:chat_id,botId:bot_id,target_node_id,target_node_id_exists:!!target_node_id,isDisparoTimeout,flow_nodes_exists:!!flow_nodes,flow_nodes_type:typeof flow_nodes,flow_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
        // #endregion

        // Inicia o 'processFlow' a partir do nó de timeout (handle 'b')
        // Se target_node_id for 'null' (porque o handle 'b' não estava conectado),
        // o 'processFlow' saberá que deve encerrar o fluxo.
        if (target_node_id) {
            try {
            // Verificar se é continuação após delay agendado
            const continueFromDelay = continue_from_delay === true;
            const remainingActionsJson = remaining_actions;
                
                // Busca o fluxo correto - PRIORIDADE: usar flow_nodes do job data quando disponível
                let flowNodes = null;
                let flowEdges = null;
                let flowIdForProcess = null;
                
                // PRIORIDADE 1: Usar flow_nodes do job data (mais confiável, evita buscar do banco)
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2428',message:'ANTES verificar flow_nodes',data:{chatId:chat_id,botId:bot_id,flow_nodes_exists:!!flow_nodes,flow_nodes_type:typeof flow_nodes,flow_nodes_length:typeof flow_nodes === 'string' ? flow_nodes.length : (flow_nodes ? 'not-string' : 'null/undefined'),flow_id:flow_id,currentState_flow_id:currentState?.flow_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
                // #endregion
                if (flow_nodes) {
                    try {
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2430',message:'ENTRANDO no try - flow_nodes existe',data:{chatId:chat_id,botId:bot_id,flow_nodes_type:typeof flow_nodes,willParse:typeof flow_nodes === 'string'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
                        // #endregion
                        const flowData = typeof flow_nodes === 'string' ? JSON.parse(flow_nodes) : flow_nodes;
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2431',message:'DEPOIS do parse',data:{chatId:chat_id,botId:bot_id,flowData_type:typeof flowData,has_nodes:'nodes' in flowData,has_edges:'edges' in flowData,nodes_length:Array.isArray(flowData.nodes) ? flowData.nodes.length : 'not-array',edges_length:Array.isArray(flowData.edges) ? flowData.edges.length : 'not-array'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
                        // #endregion
                        flowNodes = flowData.nodes || [];
                        flowEdges = flowData.edges || [];
                        // Usar flow_id do job data ou do estado
                        flowIdForProcess = flow_id || currentState?.flow_id || null;
                        
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2442',message:'Usando flow_nodes do job data',data:{chatId:chat_id,botId:bot_id,flowNodesCount:flowNodes.length,flowEdgesCount:flowEdges.length,flowIdForProcess,targetNodeId:target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'F'}));
                        // #endregion
                    } catch (parseError) {
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2440',message:'ERRO no parse',data:{chatId:chat_id,botId:bot_id,errorMessage:parseError.message},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
                        // #endregion
                        console.error(`[WORKER-TIMEOUT] Erro ao parsear flow_nodes do job data:`, parseError.message);
                    }
                } else {
                    // #region agent log
                    console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2443',message:'flow_nodes é FALSY - não entrou no if',data:{chatId:chat_id,botId:bot_id,flow_nodes_value:flow_nodes},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A'}));
                    // #endregion
                }
                
                // PRIORIDADE 2: Se não tem flow_nodes no job data ou está vazio, buscar do banco usando flow_id
                if ((!flowNodes || (Array.isArray(flowNodes) && flowNodes.length === 0)) && (flow_id || currentState?.flow_id)) {
                    const flowIdToUse = flow_id || currentState.flow_id;
                    const [flow] = await sqlWithRetry(sqlTx`
                        SELECT nodes FROM flows WHERE id = ${flowIdToUse}
                    `);
                    if (flow && flow.nodes) {
                        const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
                        flowNodes = flowData.nodes || [];
                        flowEdges = flowData.edges || [];
                        flowIdForProcess = flowIdToUse;
                        
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2443',message:'Usando flow_nodes do banco de dados',data:{chatId:chat_id,botId:bot_id,flowId:flowIdToUse,flowNodesCount:flowNodes.length,flowEdgesCount:flowEdges.length,targetNodeId:target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'F'}));
                        // #endregion
                    }
                }
                
                // Se ainda não tem flowNodes ou está vazio, processFlow vai buscar do banco (fallback)
                if (!flowNodes || (Array.isArray(flowNodes) && flowNodes.length === 0)) {
                    // #region agent log
                    console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2453',message:'Nenhum flow_nodes encontrado - processFlow vai buscar do banco',data:{chatId:chat_id,botId:bot_id,targetNodeId:target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'F'}));
                    // #endregion
                }
                
                // Se é continuação após delay e há ações restantes, processar apenas as ações restantes
                if (continueFromDelay && remainingActionsJson) {
                    try {
                        const remainingActions = JSON.parse(remainingActionsJson);
                        // Removido log de debug
                        
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
                                // Removido log de debug
                                // Fluxo foi encaminhado, não precisa continuar
                                return;
                            }
                            
                            // 2. Se delay foi agendado novamente, não continuar (já agendado)
                            if (actionResult === 'delay_scheduled') {
                                // Removido log de debug
                                return;
                            }
                            
                            // 3. Verifica se o resultado foi de um 'action_check_pix'
                            if (actionResult === 'paid') {
                                // Removido log de debug
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
                                        flowIdForProcess,
                                        renewLockIfNeeded // Passar callback de renovação
                                    );
                                } else {
                                    // Removido log de debug
                                }
                                return;
                            }
                            
                            if (actionResult === 'pending') {
                                // Removido log de debug
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
                                        flowIdForProcess,
                                        renewLockIfNeeded // Passar callback de renovação
                                    );
                                } else {
                                    // Removido log de debug
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
                                    flowIdForProcess,
                                    renewLockIfNeeded // Passar callback de renovação
                                );
                            } else {
                                // Não há próximo nó, fluxo terminou
                                // Removido log de debug
                            }
                        } else {
                            // Se não é um nó de ação, continuar normalmente
                            // #region agent log
                            console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2615',message:'ANTES processFlow (continueFromDelay, não é action)',data:{chatId:chat_id,botId:bot_id,target_node_id,flowIdForProcess},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                            // #endregion
                            await processFlow(
                                chat_id, 
                                bot_id, 
                                botToken, 
                                sellerId, 
                                target_node_id,
                                variables,
                                flowNodes,
                                flowEdges,
                                flowIdForProcess,
                                renewLockIfNeeded // Passar callback de renovação
                            );
                            // #region agent log
                            console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2627',message:'DEPOIS processFlow (continueFromDelay, não é action)',data:{chatId:chat_id,botId:bot_id,target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                            // #endregion
                        }
                    } catch (parseError) {
                        console.error(`${logPrefix} [Timeout] Erro ao processar ações restantes após delay:`, parseError.message);
                        // Fallback: continuar normalmente
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2631',message:'ANTES processFlow (fallback após parseError)',data:{chatId:chat_id,botId:bot_id,target_node_id,flowIdForProcess,parseError:parseError.message},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                        // #endregion
                        await processFlow(
                            chat_id, 
                            bot_id, 
                            botToken, 
                            sellerId, 
                            target_node_id,
                            variables,
                            flowNodes,
                            flowEdges,
                            flowIdForProcess,
                            renewLockIfNeeded // Passar callback de renovação
                        );
                        // #region agent log
                        console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2643',message:'DEPOIS processFlow (fallback após parseError)',data:{chatId:chat_id,botId:bot_id,target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                        // #endregion
                    }
                } else if (continueFromDelay && !remainingActionsJson) {
                    // Delay foi a última ação do nó - continuar para o próximo nó
                    // Removido log de debug
                    
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
                            flowIdForProcess,
                            renewLockIfNeeded // Passar callback de renovação
                        );
                    } else {
                        // Removido log de debug
                    }
                } else {
                    // Processamento normal (timeout ou continuação sem ações restantes)
                    // #region agent log
                    console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2675',message:'ANTES processFlow (processamento normal - timeout)',data:{chatId:chat_id,botId:bot_id,target_node_id,flowIdForProcess},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                    // #endregion
                    await processFlow(
                        chat_id, 
                        bot_id, 
                        botToken, 
                        sellerId, 
                        target_node_id, // Este é o nó da saída 'b' (Sem Resposta) ou continuação
                        variables,
                        flowNodes,
                        flowEdges,
                        flowIdForProcess,
                        renewLockIfNeeded // Passar callback de renovação
                    );
                    // #region agent log
                    console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2687',message:'DEPOIS processFlow (processamento normal - timeout)',data:{chatId:chat_id,botId:bot_id,target_node_id},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'PROCESSFLOW'}));
                    // #endregion
                }
                // Timeout processado com sucesso
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2689',message:'RETORNANDO: success true',data:{chatId:chat_id,botId:bot_id,success:true},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
                // #endregion
                return { success: true };
            } catch (flowError) {
                // Erro durante processFlow - logar mas não interromper
                console.error(`[WORKER] Erro durante processFlow para timeout:`, flowError.message);
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2693',message:'RETORNANDO: success true com errors',data:{chatId:chat_id,botId:bot_id,success:true,errors:[flowError.message]},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
                // #endregion
                return { success: true, errors: [flowError.message] };
            }
            } else {
                // Nenhum nó de destino definido - encerrar fluxo
                // Removido log de debug
                await sqlWithRetry(sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`);
                // #region agent log
                console.log('[DEBUG-TIMEOUT]', JSON.stringify({location:'process-timeout.js:2730',message:'RETORNANDO: no_target_node',data:{chatId:chat_id,botId:bot_id,success:true,flowEnded:true,reason:'no_target_node'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'RETURN'}));
                // #endregion
                return { success: true, flowEnded: true, reason: 'no_target_node' };
            }
    } catch (error) {
        // Tratar especificamente CONNECT_TIMEOUT - re-lançar para tratamento especial no handler
        if (error.message?.includes('CONNECT_TIMEOUT') || error.message?.includes('write CONNECT_TIMEOUT')) {
            console.error(`[WORKER] CONNECT_TIMEOUT ao processar timeout para chat ${chat_id}. Pool pode estar esgotado.`);
            error.isConnectTimeout = true;
            throw error;
        }
        
        // Re-lançar outros erros
        console.error('[WORKER] Erro ao processar timeout:', error.message, error.stack);
        throw error;
    }
}

// Handler HTTP para compatibilidade com código existente
async function handler(req, res) {
    // Verificar se requisição foi abortada antes de processar
    if (req.aborted) {
        return res.status(499).end(); // 499 = Client Closed Request
    }

    if (req.method !== 'POST') {
        return res.status(405).json({ message: 'Method Not Allowed' });
    }

    try {
        // Extrair dados do corpo da requisição
        const { chat_id, bot_id, target_node_id, variables, continue_from_delay, remaining_actions } = req.body;
        
        // Chamar função pura de processamento
        const result = await processTimeoutData({
            chat_id,
            bot_id,
            target_node_id,
            variables,
            continue_from_delay,
            remaining_actions
        });
        
        // Tratar resultados
        if (result.ignored) {
            if (result.reason === 'no_user_state') {
                return res.status(200).json({ message: 'Timeout ignored, no user state found.' });
            } else if (result.reason === 'user_already_proceeded') {
                return res.status(200).json({ message: 'Timeout ignored, user already proceeded.' });
            }
        }
        
        if (result.flowEnded) {
            return res.status(200).json({ message: 'Timeout processed, flow ended (no target node).' });
        }
        
        if (result.errors && result.errors.length > 0) {
            return res.status(200).json({ message: 'Timeout processed with errors.' });
        }
        
        // Sucesso
        if (!res.headersSent) {
            return res.status(200).json({ message: 'Timeout processed successfully.' });
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
        if (error.isConnectTimeout) {
            // Retornar erro para que o BullMQ tente novamente mais tarde
            return res.status(500).json({ 
                error: `Database connection timeout: ${error.message}`,
                retry: true 
            });
        }
        
        // Erros genéricos - não re-executar automaticamente
        console.error('[WORKER] Erro fatal ao processar timeout:', error.message, error.stack);
        return res.status(200).json({ error: `Failed to process timeout: ${error.message}` });
    }
}

// Exportar ambas as funções para permitir uso em diferentes contextos
module.exports = { handler, processTimeoutData };