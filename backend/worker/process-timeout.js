// /backend/worker/process-timeout.js

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const { neon } = require('@neondatabase/serverless');
const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const { Client } = require("@upstash/qstash");
const crypto = require('crypto');
const { createPixService } = require('../shared/pix');
const {
    metrics: prometheusMetrics,
    isEnabled: isPrometheusEnabled,
} = require('../metrics');

const METRICS_SOURCE = 'worker_process_timeout';

// ==========================================================
//                     INICIALIZAÇÃO
// ==========================================================
const sql = neon(process.env.DATABASE_URL);
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
// Cache para respeitar rate limit da PushinPay (1/min por transação)
const pushinpayLastCheckAt = new Map();
const wiinpayLastCheckAt = new Map();
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
    sql,
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

async function sqlWithRetry(query, params = [], retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            if (typeof query === 'string') { return await sql(query, params); }
            return await query;
        } catch (error) {
            const isRetryable = error.message.includes('fetch failed') || (error.sourceError && error.sourceError.code === 'UND_ERR_SOCKET');
            if (isRetryable && i < retries - 1) { await new Promise(res => setTimeout(res, delay)); } else { throw error; }
        }
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
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, { headers, responseType, timeout });
            return response.data;
        } catch (error) {
            console.error(`[WORKER - Telegram API ERROR] Method: ${method}:`, error.response?.data || error.message);
            if (i < retries - 1) { await new Promise(res => setTimeout(res, delay * (i + 1))); } else { throw error; }
        }
    }
}

async function sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption) {
    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    if (!storageBotToken) throw new Error('Token do bot de armazenamento não configurado.');

    const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { file_id: fileId });
    if (!fileInfo.ok) throw new Error('Não foi possível obter informações do arquivo da biblioteca.');

    const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;

    const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
    const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
    const fileNameMap = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' };
    const method = methodMap[fileType];
    const field = fieldMap[fileType];
    const timeout = fileType === 'video' ? 60000 : 30000;

    if (!method) throw new Error('Tipo de arquivo não suportado.');

    const recordRequest = (status) => {
        if (isPrometheusEnabled && prometheusMetrics.telegramMediaRequests) {
            prometheusMetrics.telegramMediaRequests.inc({
                source: METRICS_SOURCE,
                type: fileType,
                status,
            });
        }
    };

    const recordBytes = (direction, bytes) => {
        if (isPrometheusEnabled && prometheusMetrics.telegramMediaBytes && bytes > 0) {
            prometheusMetrics.telegramMediaBytes.observe(
                {
                    source: METRICS_SOURCE,
                    type: fileType,
                    direction,
                },
                bytes
            );
        }
    };

    try {
        const directPayload = {
            chat_id: chatId,
            [field]: fileUrl,
        };
        if (caption) {
            directPayload.caption = caption;
        }
        const directResponse = await sendTelegramRequest(destinationBotToken, method, directPayload, { timeout });
        if (directResponse?.ok) {
            recordRequest('success_direct');
            return directResponse;
        }
        recordRequest('retry_upload');
    } catch (error) {
        recordRequest('retry_upload');
    }

    const { data: fileBuffer, headers: fileHeaders } = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    recordBytes('download', fileBuffer.length);

    const formData = new FormData();
    formData.append('chat_id', chatId);
    if (caption) {
        formData.append('caption', caption);
    }

    const fileName = fileNameMap[fileType];
    formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

    recordBytes('upload', fileBuffer.length);

    try {
        const uploadResponse = await sendTelegramRequest(destinationBotToken, method, formData, {
            headers: formData.getHeaders(),
            timeout,
        });
        if (uploadResponse?.ok) {
            recordRequest('success_upload');
        } else {
            recordRequest('failure');
        }
        return uploadResponse;
    } catch (error) {
        recordRequest('failure');
        throw error;
    }
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
    const { message_id, chat, from, text, photo, video, voice } = message;
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

    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (chat_id, message_id) DO NOTHING;
    `, [sellerId, botId, chat.id, message_id, fromUser.id, fromUser.first_name || botInfo.first_name, fromUser.last_name || botInfo.last_name, fromUser.username || null, messageText, senderType, mediaType, mediaFileId, finalClickId]);

    if (newClickId) {
        await sqlWithRetry(
            'UPDATE telegram_chats SET click_id = $1 WHERE chat_id = $2 AND bot_id = $3',
            [newClickId, chat.id, botId]
        );
    }
}

async function sendEventToUtmify(status, clickData, pixData, sellerData, customerData, productData) {
    console.log(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            console.log(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sql`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            console.log(`[Utmify] Clique originado do Checkout ID: ${clickData.checkout_id}. Lógica de associação não implementada para checkouts.`);
        }

        if (!integrationId) {
            console.log(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        console.log(`[Utmify] Integração vinculada ID: ${integrationId}. Buscando token...`);
        const [integration] = await sql`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            console.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        console.log(`[Utmify] Token encontrado. Montando payload...`);
        
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id, platform: "HotTrack", paymentMethod: 'pix',
            status: status, createdAt: createdAt, approvedDate: approvedDate, refundedAt: null,
            customer: { name: customerData?.name || "Não informado", email: customerData?.email || "naoinformado@email.com", phone: customerData?.phone || null, document: customerData?.document || null, },
            products: [{ id: productData?.id || "default_product", name: productData?.name || "Produto Digital", planId: null, planName: null, quantity: 1, priceInCents: Math.round(pixData.pix_value * 100) }],
            trackingParameters: { src: null, sck: null, utm_source: clickData.utm_source, utm_campaign: clickData.utm_campaign, utm_medium: clickData.utm_medium, utm_content: clickData.utm_content, utm_term: clickData.utm_term },
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0500)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0500))) },
            isTest: false
        };

        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        console.log(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        console.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}
async function sendMetaEvent(eventName, clickData, transactionData, customerData = null) {
    try {
        let presselPixels = [];
        if (clickData.pressel_id) {
            presselPixels = await sql`SELECT pixel_config_id FROM pressel_pixels WHERE pressel_id = ${clickData.pressel_id}`;
        } else if (clickData.checkout_id) {
            presselPixels = await sql`SELECT pixel_config_id FROM checkout_pixels WHERE checkout_id = ${clickData.checkout_id}`;
        }

        if (presselPixels.length === 0) {
            console.log(`Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}.`);
            return;
        }

        const userData = {
            fbp: clickData.fbp || undefined,
            fbc: clickData.fbc || undefined,
            external_id: clickData.click_id ? clickData.click_id.replace('/start ', '') : undefined
        };

        if (clickData.ip_address && clickData.ip_address !== '::1' && !clickData.ip_address.startsWith('127.0.0.1')) {
            userData.client_ip_address = clickData.ip_address;
        }
        if (clickData.user_agent && clickData.user_agent.length > 10) { 
            userData.client_user_agent = clickData.user_agent;
        }

        if (customerData?.name) {
            const nameParts = customerData.name.trim().split(' ');
            const firstName = nameParts[0].toLowerCase();
            const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1].toLowerCase() : undefined;
            userData.fn = crypto.createHash('sha256').update(firstName).digest('hex');
            if (lastName) {
                userData.ln = crypto.createHash('sha256').update(lastName).digest('hex');
            }
        }

        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        for (const { pixel_config_id } of presselPixels) {
            const [pixelConfig] = await sql`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE id = ${pixel_config_id}`;
            if (pixelConfig) {
                const { pixel_id, meta_api_token } = pixelConfig;
                const event_id = `${eventName}.${transactionData.id || clickData.id}.${pixel_id}`;
                
                const payload = {
                    data: [{
                        event_name: eventName,
                        event_time: Math.floor(Date.now() / 1000),
                        event_id,
                        user_data: userData,
                        custom_data: {
                            currency: 'BRL',
                            value: transactionData.pix_value
                        },
                    }]
                };
                
                if (eventName !== 'Purchase') {
                    delete payload.data[0].custom_data.value;
                }

                console.log(`[Meta Pixel] Enviando payload para o pixel ${pixel_id}:`, JSON.stringify(payload, null, 2));
                await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                console.log(`Evento '${eventName}' enviado para o Pixel ID ${pixel_id}.`);

                if (eventName === 'Purchase') {
                     await sql`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                }
            }
        }
    } catch (error) {
        console.error(`Erro ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}

async function handleSuccessfulPayment(transaction_id, customerData) {
    try {
        const [transaction] = await sql`UPDATE pix_transactions SET status = 'paid', paid_at = NOW() WHERE id = ${transaction_id} AND status != 'paid' RETURNING *`;
        if (!transaction) { 
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já processada ou não encontrada.`);
            return; 
        }

        console.log(`[handleSuccessfulPayment] Processando pagamento para transação ${transaction_id}.`);

        if (adminSubscription && webpush) {
            const payload = JSON.stringify({
                title: 'Nova Venda Paga!',
                body: `Venda de R$ ${parseFloat(transaction.pix_value).toFixed(2)} foi confirmada.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(error => {
                if (error.statusCode === 410) {
                    console.log("Inscrição de notificação expirada. Removendo.");
                    adminSubscription = null;
                } else {
                    console.warn("Falha ao enviar notificação push (não-crítico):", error.message);
                }
            });
        }
        
        const [click] = await sql`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${click.seller_id}`;

        if (click && seller) {
            const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
            const productData = { id: "prod_final", name: "Produto Vendido" };

            await sendEventToUtmify('paid', click, transaction, seller, finalCustomerData, productData);
            await sendMetaEvent('Purchase', click, transaction, finalCustomerData);
        } else {
            console.error(`[handleSuccessfulPayment] ERRO: Não foi possível encontrar dados do clique ou vendedor para a transação ${transaction_id}`);
        }
    } catch(error) {
        console.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
    }
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
        console.warn(`[WORKER - Flow Engine] Falha ao enviar ação 'typing':`, error.response?.data || error.message);
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
            // CORREÇÃO FINAL: Salva NULL para os dados do usuário quando o remetente é o bot.
            await sql`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, click_id)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, NULL, NULL, NULL, ${text}, 'bot', ${variables.click_id || null})
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        console.error(`[WORKER - Flow Engine] Erro ao enviar/salvar mensagem:`, error.response?.data || error.message);
    }
}

// /backend/worker/process-timeout.js (Função processActions CORRIGIDA)

/**
 * =================================================================
 * FUNÇÃO 'processActions' (O EXECUTOR) - VERSÃO NOVA
 * =================================================================
 * (Colada da sua resposta anterior)
 */
async function processActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix = '[Actions]') {
    console.log(`${logPrefix} Iniciando processamento de ${actions.length} ações aninhadas para chat ${chatId}`);
    
    for (const action of actions) {
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
                        const btnUrl = await replaceVariables(actionData.buttonUrl, variables);
                        
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
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                break;
            
            case 'typing_action':
                if (actionData.durationInSeconds && actionData.durationInSeconds > 0) {
                    await showTypingForDuration(chatId, botToken, actionData.durationInSeconds * 1000);
                }
                break;

            case 'action_create_invite_link':
                try {
                    console.log(`${logPrefix} Executando action_create_invite_link para chat ${chatId}`);

                    const [botInvite] = await sql`
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

                        if (actionData.sendMessage) {
                            const messageText = await replaceVariables(
                                actionData.messageText || `Link de convite criado: ${inviteResponse.result.invite_link}`,
                                variables
                            );
                            await sendMessage(chatId, messageText, botToken, sellerId, botId, false, variables);
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

                    const [bot] = await sql`
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
    
                    const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    if (!seller) throw new Error(`${logPrefix} Vendedor ${sellerId} não encontrado.`);
    
                    let click_id_from_vars = variables.click_id;
                    if (!click_id_from_vars) {
                        const [recentClick] = await sql`
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
                    const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
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
                        
                        await sql`
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
                    await sendEventToUtmify(
                        'waiting_payment', 
                        click, 
                        { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() }, 
                        seller, customerDataForUtmify, productDataForUtmify
                    );
                    console.log(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para o clique ${click.id}.`);

                    // Envia evento Meta se veio de pressel ou checkout
                    if (click.pressel_id || click.checkout_id) {
                        await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: valueInCents / 100 }, null);
                        console.log(`${logPrefix} Evento 'InitiateCheckout' enviado para Meta para o clique ${click.id}.`);
                    }
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
                    
                    const [transaction] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        return 'paid'; // Sinaliza para 'processFlow'
                    }

                    const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
                    let providerStatus = null;
                    let customerData = {};
                    const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;

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
                    }
                    
                    if (providerStatus && paidStatuses.has(providerStatus)) {
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
                    const [stateToCancel] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
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
                
                const [targetFlow] = await sql`SELECT * FROM flows WHERE id = ${targetFlowIdNum} AND bot_id = ${botId}`;
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
                let currentNodeId = findNextNode(targetStartNode.id, 'a', targetEdges);
                let attempts = 0;
                const maxAttempts = 20; // Proteção contra loops infinitos
                
                // Limpa o estado atual antes de iniciar o novo fluxo
                await sql`UPDATE user_flow_states 
                          SET waiting_for_input = false, scheduled_message_id = NULL 
                          WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                
                // Pula nós do tipo 'trigger' até encontrar um nó válido
                while (currentNodeId && attempts < maxAttempts) {
                    const currentNode = targetNodes.find(n => n.id === currentNodeId);
                    if (!currentNode) {
                        console.error(`${logPrefix} Nó ${currentNodeId} não encontrado no fluxo de destino.`);
                        break;
                    }
                    
                    if (currentNode.type !== 'trigger') {
                        // Encontrou um nó válido (não é trigger)
                        console.log(`${logPrefix} Encontrado nó válido para iniciar: ${currentNodeId} (tipo: ${currentNode.type})`);
                        // Passa os dados do fluxo de destino para o processFlow recursivo
                        await processFlow(chatId, botId, botToken, sellerId, currentNodeId, variables, targetNodes, targetEdges);
                        break;
                    }
                    
                    // Se for trigger, continua procurando o próximo nó
                    console.log(`${logPrefix} Pulando nó trigger ${currentNodeId}, procurando próximo nó...`);
                    currentNodeId = findNextNode(currentNodeId, 'a', targetEdges);
                    attempts++;
                }
                
                if (!currentNodeId || attempts >= maxAttempts) {
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
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}, flowNodes = null, flowEdges = null) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    console.log(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);

    // ==========================================================
    // PASSO 1: CARREGAR VARIÁVEIS DO USUÁRIO E DO CLIQUE
    // ==========================================================
    let variables = { ...initialVariables };

    const [user] = await sql`
        SELECT first_name, last_name 
        FROM telegram_chats 
        WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
        ORDER BY created_at DESC LIMIT 1`;

    if (user) {
        variables.primeiro_nome = user.first_name || '';
        variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }

    if (variables.click_id) {
        const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
        const [click] = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id}`;
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
    
        if (variables._scheduled_media_actions && Array.isArray(variables._scheduled_media_actions)) {
        console.log(`[WORKER - Timeout] Processando ${variables._scheduled_media_actions.length} ação(ões) de mídia agendadas.`);
        
        for (const mediaAction of variables._scheduled_media_actions) {
            try {
                const actionData = mediaAction.data || {};
                let caption = await replaceVariables(actionData.caption || '', variables);
                
                // Validação do tamanho da legenda
                if (caption && caption.length > 1024) {
                    console.warn(`[WORKER - Media] Legenda excede limite de 1024 caracteres. Truncando...`);
                    caption = caption.substring(0, 1021) + '...';
                }
                
                const response = await handleMediaNode(mediaAction, botToken, chatId, caption);
                
                if (response && response.ok) {
                    await saveMessageToDb(sellerId, botId, response.result, 'bot');
                }
            } catch (e) {
                console.error(`[WORKER - Media] Erro ao enviar mídia agendada (${mediaAction.type}):`, e.message);
            }
        }
        
        // Remove as ações de mídia processadas das variáveis
        delete variables._scheduled_media_actions;
    }
    
    // Se os dados do fluxo foram fornecidos (ex: forward_flow), usa eles. Caso contrário, busca do banco.
    let nodes, edges;
    if (flowNodes && flowEdges) {
        // Usa os dados do fluxo fornecido (do forward_flow)
        nodes = flowNodes;
        edges = flowEdges;
        console.log(`${logPrefix} [Flow Engine] Usando dados do fluxo fornecido (${nodes.length} nós, ${edges.length} arestas).`);
    } else {
        // Busca o fluxo ativo do banco
        const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} AND is_active = TRUE ORDER BY updated_at DESC LIMIT 1`;
        if (!flow || !flow.nodes) {
            console.log(`${logPrefix} [Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
            return;
        }
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
            const [stateToCancel] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (stateToCancel && stateToCancel.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                    console.log(`[Flow Engine] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                } catch (e) { console.warn(`[Flow Engine] Falha ao cancelar QStash msg ${stateToCancel.scheduled_message_id}:`, e.message); }
            }
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            const startNode = nodes.find(node => node.type === 'trigger');
            currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;

        } else {
            const [userState] = await sql`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (userState && userState.waiting_for_input) {
                console.log(`${logPrefix} [Flow Engine] Usuário respondeu. Continuando do nó ${userState.current_node_id} (handle 'a').`);
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
                let parsedVariables = {};
                try { parsedVariables = JSON.parse(userState.variables); } catch (e) { parsedVariables = userState.variables; }
                variables = { ...variables, ...parsedVariables };

            } else {
                console.log(`${logPrefix} [Flow Engine] Nova conversa. Iniciando do gatilho.`);
                await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                const startNode = nodes.find(node => node.type === 'trigger');
                currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;
            }
        }
    }


    if (!currentNodeId) {
        console.log(`${logPrefix} [Flow Engine] Nenhum nó para processar. Fim do fluxo.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        return;
    }

    // ==========================================================
    // PASSO 3: O NOVO LOOP DE NAVEGAÇÃO
    // ==========================================================
    let safetyLock = 0;
    while (currentNodeId && safetyLock < 20) {
        safetyLock++;
        let currentNode = nodes.find(node => node.id === currentNodeId);
        
        if (!currentNode) {
            console.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        console.log(`${logPrefix} [Flow Engine] Processando Nó: ${currentNode.id} (Tipo: ${currentNode.type})`);

        await sql`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input, scheduled_message_id)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false, NULL)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET 
                current_node_id = EXCLUDED.current_node_id, 
                variables = EXCLUDED.variables, 
                waiting_for_input = false, 
                scheduled_message_id = NULL;
        `;

        if (currentNode.type === 'trigger') {
            if (currentNode.data.actions && currentNode.data.actions.length > 0) {
                 await processActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`);
                 await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            }
            currentNodeId = findNextNode(currentNode.id, 'a', edges);
            continue;
        }

        if (currentNode.type === 'action') {
            const actions = currentNode.data.actions || [];
            const actionResult = await processActions(actions, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`);

            await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;

            if (actionResult === 'flow_forwarded') {
                console.log(`${logPrefix} [Flow Engine] Fluxo encaminhado. Encerrando o fluxo atual (worker).`);
                currentNodeId = null; // Para o loop atual
                break; // Sai do 'while'
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
                    
                    await sql`
                        UPDATE user_flow_states 
                        SET waiting_for_input = true, scheduled_message_id = ${response.messageId} 
                        WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    
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

    if (!currentNodeId) {
        const [state] = await sql`SELECT 1 FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId} AND waiting_for_input = true`;
        if (!state) {
            console.log(`${logPrefix} [Flow Engine] Fim do fluxo para ${chatId}. Limpando estado.`);
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        } else {
            console.log(`${logPrefix} [Flow Engine] Fluxo pausado (waiting for input). Estado preservado para ${chatId}.`);
        }
    }
}
// ==========================================================

async function handler(req, res) {
    if (req.method !== 'POST') {
        return res.status(405).json({ message: 'Method Not Allowed' });
    }

    try {
        // 1. Recebe os dados agendados pelo QStash
        const { chat_id, bot_id, target_node_id, variables } = req.body;
        const logPrefix = '[WORKER]';

        console.log(`${logPrefix} [Timeout] Recebido para chat ${chat_id}, bot ${bot_id}. Nó de destino: ${target_node_id || 'NONE'}`);

        // 2. Busca o bot para obter o token e sellerId
        const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;
        if (!bot || !bot.bot_token) {
            throw new Error(`[WORKER] Bot ${bot_id} ou token não encontrado.`);
        }
        const botToken = bot.bot_token;
        const sellerId = bot.seller_id;

        // 3. *** VERIFICAÇÃO CRÍTICA ***
        // Verifica se o estado atual corresponde ao esperado
        const [currentState] = await sql`
            SELECT waiting_for_input, scheduled_message_id, current_node_id 
            FROM user_flow_states 
            WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

        // Verificações para determinar se este timeout deve ser processado
        if (!currentState) {
            console.log(`${logPrefix} [Timeout] Ignorado: Nenhum estado encontrado para o usuário ${chat_id}.`);
            return res.status(200).json({ message: 'Timeout ignored, no user state found.' });
        }
        
        if (!currentState.waiting_for_input) {
            console.log(`${logPrefix} [Timeout] Ignorado: Usuário ${chat_id} já respondeu ou o fluxo foi reiniciado.`);
            return res.status(200).json({ message: 'Timeout ignored, user already proceeded.' });
        }

        // 4. O usuário NÃO respondeu a tempo.
        console.log(`${logPrefix} [Timeout] Usuário ${chat_id} não respondeu. Processando caminho de timeout.`);
        
        // Limpa o estado de 'espera' ANTES de processar o próximo nó
        await sql`
            UPDATE user_flow_states 
            SET waiting_for_input = false, scheduled_message_id = NULL 
            WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

        // 5. Inicia o 'processFlow' a partir do nó de timeout (handle 'b')
        // Se target_node_id for 'null' (porque o handle 'b' não estava conectado),
        // o 'processFlow' saberá que deve encerrar o fluxo.
        if (target_node_id) {
            await processFlow(
                chat_id, 
                bot_id, 
                botToken, 
                sellerId, 
                target_node_id, // Este é o nó da saída 'b' (Sem Resposta)
                variables
            );
            return res.status(200).json({ message: 'Timeout processed successfully.' });
        } else {
            console.log(`${logPrefix} [Timeout] Nenhum nó de destino definido. Encerrando fluxo para ${chat_id}.`);
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;
            return res.status(200).json({ message: 'Timeout processed, flow ended (no target node).' });
        }

    } catch (error) {
        console.error('[WORKER] Erro fatal ao processar timeout:', error.message, error.stack);
        // Retornamos 200 para que o QStash NÃO tente re-executar um fluxo que falhou logicamente.
        return res.status(200).json({ error: `Failed to process timeout: ${error.message}` });
    }
}

// Exporta o handler com a verificação de segurança do QStash
module.exports = handler;