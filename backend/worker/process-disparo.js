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
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, { headers, responseType, timeout });
            return response.data;
        } catch (error) {
            // FormData do Node.js não tem .get(), então tenta extrair do erro ou deixa undefined
            const chatId = data?.chat_id || 'unknown';
            const description = error.response?.data?.description || error.message;
            if (error.response && error.response.status === 403) {
                logger.debug(`[WORKER-DISPARO] Chat ${chatId} bloqueou o bot (method ${method}). Ignorando.`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento para erro 429 (Too Many Requests)
            if (error.response && error.response.status === 429) {
                const retryAfter = parseInt(error.response.headers['retry-after'] || error.response.headers['Retry-After'] || '2');
                const waitTime = retryAfter * 1000; // Converter para milissegundos
                
                logger.warn(`[WORKER-DISPARO] Rate limit atingido (429). Aguardando ${retryAfter}s antes de retry. Method: ${method}, ChatID: ${chatId}`);
                
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
                logger.warn(`[WORKER-DISPARO] Chat de grupo fechado. ChatID: ${chatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }
            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');
            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }
            logger.error(`[WORKER-DISPARO - Telegram API ERROR] Method: ${method}, ChatID: ${chatId}:`, error.response?.data || error.message);
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
    logger.debug(`${logPrefix} [Flow Engine] Iniciando processo de disparo para ${chatId}. Nó inicial: ${startNodeId}`);
    
    let variables = { ...initialVariables };
    let currentNodeId = startNodeId;
    let safetyLock = 0;
    let lastTransactionId = null; // Rastrear transaction_id para salvar no disparo_log
    let logStatus = 'SENT';
    let logDetails = 'Enviado com sucesso.';
    const maxIterations = 50; // Proteção contra loops infinitos
    
    try {
        // Atualizar variáveis com dados do usuário
        const [user] = await sqlWithRetry(sqlTx`
            SELECT first_name, last_name 
            FROM telegram_chats 
            WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
            ORDER BY created_at DESC LIMIT 1`);
        
        if (user) {
            variables.primeiro_nome = user.first_name || '';
            variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
        }
        
        // Processar fluxo
        while (currentNodeId && safetyLock < maxIterations) {
            safetyLock++;
            const currentNode = flowNodes.find(n => n.id === currentNodeId);
            
            if (!currentNode) {
                logger.warn(`${logPrefix} Nó ${currentNodeId} não encontrado. Encerrando fluxo.`);
                break;
            }
            
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
                        }
                    } catch (error) {
                        logger.error(`${logPrefix} Erro ao processar ações do nó ${currentNodeId}:`, error);
                        logStatus = 'FAILED';
                        logDetails = error.message.substring(0, 255);
                        break;
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
    } catch (error) {
        logStatus = 'FAILED';
        logDetails = error.message.substring(0, 255);
        logger.error(`${logPrefix} Erro crítico ao processar fluxo:`, error);
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

// Funções para envio de eventos Meta e Utmify
async function sendEventToUtmify(status, clickData, pixData, sellerData, customerData, productData) {
    logger.debug(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            logger.debug(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sqlWithRetry(sqlTx`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`);
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            logger.debug(`[Utmify] Clique originado do Checkout ID: ${clickData.checkout_id}. Lógica de associação não implementada para checkouts.`);
        }

        if (!integrationId) {
            logger.debug(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        logger.debug(`[Utmify] Integração vinculada ID: ${integrationId}. Buscando token...`);
        const [integration] = await sqlTx`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            logger.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        logger.debug(`[Utmify] Token encontrado. Montando payload...`);
        
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
        logger.debug(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        logger.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}

async function sendMetaEvent(eventName, clickData, transactionData, customerData = null) {
    try {
        let presselPixels = [];
        if (clickData.pressel_id) {
            presselPixels = await sqlTx`SELECT pixel_config_id FROM pressel_pixels WHERE pressel_id = ${clickData.pressel_id}`;
        } else if (clickData.checkout_id) {
            // Verificar se é checkout hospedado (string) ou checkout antigo (integer)
            const checkoutId = clickData.checkout_id;
            if (String(checkoutId).startsWith('cko_')) {
                // Buscar pixel_id do config do hosted_checkout
                const [hostedCheckout] = await sqlTx`
                    SELECT config->'tracking'->>'pixel_id' as pixel_id 
                    FROM hosted_checkouts 
                    WHERE id = ${checkoutId}
                `;
                
                if (hostedCheckout?.pixel_id) {
                    // Converter pixel_id para o formato esperado
                    presselPixels = [{ pixel_config_id: parseInt(hostedCheckout.pixel_id) }];
                }
            } else {
                // É um checkout antigo (integer), usar a tabela checkout_pixels
                presselPixels = await sqlTx`SELECT pixel_config_id FROM checkout_pixels WHERE checkout_id = ${checkoutId}`;
            }
        }

        if (presselPixels.length === 0) {
            logger.debug(`Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}.`);
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
            const [pixelConfig] = await sqlTx`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE id = ${pixel_config_id}`;
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

                logger.debug(`[Meta Pixel] Enviando payload para o pixel ${pixel_id}:`, JSON.stringify(payload, null, 2));
                await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                logger.debug(`Evento '${eventName}' enviado para o Pixel ID ${pixel_id}.`);

                if (eventName === 'Purchase') {
                     await sqlTx`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                }
            }
        }
    } catch (error) {
        logger.error(`Erro ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}

// Função simplificada para processar ações em disparos
async function processDisparoActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix) {
    let lastPixTransactionId = null; // Rastrear último transaction_id gerado
    
    for (const action of actions) {
        try {
            const actionData = action.data || {};
            
            if (action.type === 'message') {
                const text = await replaceVariables(actionData.text || '', variables);
                if (text && text.trim()) {
                    const payload = { chat_id: chatId, text, parse_mode: 'HTML' };
                    if (actionData.buttonText && actionData.buttonUrl) {
                        payload.reply_markup = { inline_keyboard: [[{ text: actionData.buttonText, url: actionData.buttonUrl }]] };
                    }
                    await sendTelegramRequest(botToken, 'sendMessage', payload);
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
                                await sendMetaEvent('InitiateCheckout', click, { id: transaction.id, pix_value: valueInCents / 100 }, null);
                                logger.debug(`${logPrefix} Evento 'InitiateCheckout' enviado para Meta para transação ${transaction.id}`);
                            }
                            
                            // Enviar waiting_payment para Utmify
                            const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                            const productDataForUtmify = { id: "prod_disparo", name: "Produto (Disparo Massivo)" };
                            await sendEventToUtmify(
                                'waiting_payment',
                                click,
                                { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() },
                                seller,
                                customerDataForUtmify,
                                productDataForUtmify
                            );
                            logger.debug(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para transação ${transaction.id}`);
                        }
                    }
                }
            } else if (action.type === 'action_check_pix') {
                // Verificar PIX - lógica simplificada (não implementada completamente para disparos)
                logger.debug(`${logPrefix} Ação check_pix não suportada em disparos. Pulando.`);
            }
            // Outros tipos de ação podem ser adicionados aqui conforme necessário
        } catch (error) {
            logger.error(`${logPrefix} Erro ao processar ação ${action.type}:`, error);
            // Continua processando outras ações mesmo se uma falhar
        }
    }
    
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