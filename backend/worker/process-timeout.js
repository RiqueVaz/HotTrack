// /backend/worker/process-timeout.js

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const { neon } = require('@neondatabase/serverless');
const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const { Client } = require("@upstash/qstash");

// ==========================================================
//                     INICIALIZAÇÃO
// ==========================================================
const sql = neon(process.env.DATABASE_URL);
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
// Cache para respeitar rate limit da PushinPay (1/min por transação)
const pushinpayLastCheckAt = new Map();
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;


const qstashClient = new Client({ // <-- ADICIONE ESTE BLOCO
     token: process.env.QSTASH_TOKEN,
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

async function generatePixWithFallback(seller, value_cents, host, apiKey, ip_address, click_id_internal) {
    const providerOrder = [
        seller.pix_provider_primary,
        seller.pix_provider_secondary,
        seller.pix_provider_tertiary
    ].filter(Boolean); // Remove nulos ou vazios

    if (providerOrder.length === 0) {
        throw new Error('Nenhum provedor de PIX configurado para este vendedor.');
    }

    let lastError = null;

    for (const provider of providerOrder) {
        try {
            console.log(`[PIX Fallback] Tentando gerar PIX com ${provider.toUpperCase()} para ${value_cents} centavos.`);
            const pixResult = await generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address);
            console.log(`[PIX Fallback] SUCESSO com ${provider.toUpperCase()}. Transaction ID: ${pixResult.transaction_id}`);

            // Salvar a transação no banco AQUI DENTRO da função de fallback
            // Isso garante que a transação só é salva se a geração for bem-sucedida
            const [transaction] = await sql`
                INSERT INTO pix_transactions (
                    click_id_internal, pix_value, qr_code_text, qr_code_base64,
                    provider, provider_transaction_id, pix_id
                ) VALUES (
                    ${click_id_internal}, ${value_cents / 100}, ${pixResult.qr_code_text},
                    ${pixResult.qr_code_base64}, ${pixResult.provider},
                    ${pixResult.transaction_id}, ${pixResult.transaction_id}
                ) RETURNING id`;

             // Adiciona o ID interno da transação salva ao resultado para uso posterior
            pixResult.internal_transaction_id = transaction.id;

            return pixResult; // Retorna o resultado SUCESSO

        } catch (error) {
            console.error(`[PIX Fallback] FALHA ao gerar PIX com ${provider.toUpperCase()}:`, error.response?.data?.message || error.message);
            lastError = error; // Guarda o erro para o caso de todos falharem
        }
    }

    // Se o loop terminar sem sucesso, lança o último erro ocorrido
    console.error(`[PIX Fallback FINAL ERROR] Seller ID: ${seller?.id} - Todas as tentativas de geração PIX falharam.`);
    // Tenta repassar a mensagem de erro mais específica do provedor, se disponível
    const specificMessage = lastError.response?.data?.message || lastError.message || 'Todos os provedores de PIX falharam.';
    throw new Error(`Não foi possível gerar o PIX: ${specificMessage}`);
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

async function getSyncPayAuthToken(seller) {
    const cachedToken = syncPayTokenCache.get(seller.id);
    if (cachedToken && cachedToken.expiresAt > Date.now() + 60000) { return cachedToken.accessToken; }
    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) { throw new Error('Credenciais da SyncPay não configuradas.'); }
    const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/auth-token`, { client_id: seller.syncpay_client_id, client_secret: seller.syncpay_client_secret });
    const { access_token, expires_in } = response.data;
    const expiresAt = Date.now() + (expires_in * 1000);
    syncPayTokenCache.set(seller.id, { accessToken: access_token, expiresAt });
    return access_token;
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

async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address) {
    let pixData;
    let acquirer = 'Não identificado';
    const commission_rate = seller.commission_rate || 0.0500;
    
    const clientPayload = {
        document: { number: "21376710773", type: "CPF" },
        name: "Cliente Padrão",
        email: "gabriel@email.com",
        phone: "27995310379"
    };
    
    if (provider === 'brpix') {
        if (!seller.brpix_secret_key || !seller.brpix_company_id) {
            throw new Error('Credenciais da BR PIX não configuradas para este vendedor.');
        }
        const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');
        
        const payload = {
            customer: clientPayload,
            items: [{ title: "Produto Digital", unitPrice: parseInt(value_cents, 10), quantity: 1 }],
            paymentMethod: "PIX",
            amount: parseInt(value_cents, 10),
            pix: { expiresInDays: 1 },
            ip: ip_address
        };

        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && BRPIX_SPLIT_RECIPIENT_ID) {
            payload.split = [{ recipientId: BRPIX_SPLIT_RECIPIENT_ID, amount: commission_cents }];
        }
        const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, {
            headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/json' }
        });
        pixData = response.data;
        acquirer = "BRPix";
        return {
            qr_code_text: pixData.pix.qrcode, // Alterado de pixData.pix.qrcodeText
            qr_code_base64: pixData.pix.qrcode, // Mantido para consistência com a resposta atual
            transaction_id: pixData.id,
            acquirer,
            provider
        };
    } else if (provider === 'syncpay') {
        const token = await getSyncPayAuthToken(seller);
        const payload = { 
            amount: value_cents / 100, 
            payer: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" },
            callbackUrl: `https://${host}/api/webhook/syncpay`
        };
        const commission_percentage = commission_rate * 100;
        if (apiKey !== ADMIN_API_KEY && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) {
            payload.split = [{
                percentage: Math.round(commission_percentage), 
                user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID 
            }];
        }
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/cash-in`, payload, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        pixData = response.data;
        acquirer = "SyncPay";
        return { 
            qr_code_text: pixData.pix_code, 
            qr_code_base64: null, 
            transaction_id: pixData.identifier, 
            acquirer, 
            provider 
        };
    } else if (provider === 'cnpay' || provider === 'oasyfy') {
        const isCnpay = provider === 'cnpay';
        const publicKey = isCnpay ? seller.cnpay_public_key : seller.oasyfy_public_key;
        const secretKey = isCnpay ? seller.cnpay_secret_key : seller.oasyfy_secret_key;
        if (!publicKey || !secretKey) throw new Error(`Credenciais para ${provider.toUpperCase()} não configuradas.`);
        const apiUrl = isCnpay ? 'https://painel.appcnpay.com/api/v1/gateway/pix/receive' : 'https://app.oasyfy.com/api/v1/gateway/pix/receive';
        const splitId = isCnpay ? CNPAY_SPLIT_PRODUCER_ID : OASYFY_SPLIT_PRODUCER_ID;
        const payload = {
            identifier: uuidv4(),
            amount: value_cents / 100,
            client: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" },
            callbackUrl: `https://${host}/api/webhook/${provider}`
        };
        const commission = parseFloat(((value_cents / 100) * commission_rate).toFixed(2));
        if (apiKey !== ADMIN_API_KEY && commission > 0 && splitId) {
            payload.splits = [{ producerId: splitId, amount: commission }];
        }
        const response = await axios.post(apiUrl, payload, { headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey } });
        pixData = response.data;
        acquirer = isCnpay ? "CNPay" : "Oasy.fy";
        return { qr_code_text: pixData.pix.code, qr_code_base64: pixData.pix.base64, transaction_id: pixData.transactionId, acquirer, provider };
    } else { // Padrão é PushinPay
        if (!seller.pushinpay_token) throw new Error(`Token da PushinPay não configurado.`);
        const payload = {
            value: value_cents,
            webhook_url: `https://${host}/api/webhook/pushinpay`,
        };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && PUSHINPAY_SPLIT_ACCOUNT_ID) {
            payload.split_rules = [{ value: commission_cents, account_id: PUSHINPAY_SPLIT_ACCOUNT_ID }];
        }
        const pushinpayResponse = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
        pixData = pushinpayResponse.data;
        acquirer = "Woovi";
        return { qr_code_text: pixData.qr_code, qr_code_base64: pixData.qr_code_base64, transaction_id: pixData.id, acquirer, provider: 'pushinpay' };
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
                const textToSend = await replaceVariables(actionData.text, variables);
                // Corrigido: Removido o '0' extra da chamada sendMessage
                await sendMessage(chatId, textToSend, botToken, sellerId, botId, false, variables); 
                break;

            case 'image':
            case 'video':
            case 'audio': {
                try {
                    const caption = await replaceVariables(actionData.caption, variables);
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
                    
                    const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);
                    
                    const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                    const productDataForUtmify = { id: "prod_bot", name: "Produto (Fluxo Bot)" };
                    await sendEventToUtmify(
                        'waiting_payment', 
                        click, 
                        { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() }, 
                        seller, customerDataForUtmify, productDataForUtmify
                    );
                    console.log(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para o clique ${click.id}.`);

                    variables.last_transaction_id = pixResult.transaction_id;
    
                    const messageText = await replaceVariables(actionData.pixMessageText || "", variables); // Corrigido
                    const buttonText = await replaceVariables(actionData.pixButtonText || "📋 Copiar", variables);
                    const pixToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                        chat_id: chatId, text: pixToSend, parse_mode: 'HTML',
                        reply_markup: { inline_keyboard: [[{ text: buttonText, callback_data: `copy_${pixResult.transaction_id}` }]] }
                    });
    
                    if (sentMessage.ok) {
                        await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot');
                    }
                } catch (error) {
                    console.error(`${logPrefix} Erro no nó action_pix para chat ${chatId}:`, error);
                    console.log(chatId, "Desculpe, não consegui gerar o PIX neste momento.", botToken, sellerId, botId, true, variables);
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
                            customerData = { name: resp.data.payer_name, document: resp.data.payer_document };
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
                // Inicia o novo fluxo (passando o ID do fluxo como 'startNodeId' do novo fluxo)
                // O 'startNodeId' do processFlow é o NÓ, mas como fluxos antigos não têm 'start node',
                // precisamos adaptar processFlow para aceitar um ID de FLUXO.
                
                // *** CORREÇÃO DE LÓGICA ***
                // O 'processFlow' que você tem busca o fluxo pelo bot_id.
                // Precisamos que o 'processFlow' busque pelo 'flow.id' se 'startNodeId' for um ID de fluxo.
                
                // ASSUMINDO que 'processFlow' foi ajustado para receber um 'flowId'
                // Esta é uma chamada RECURSIVA para o 'processFlow' deste worker.
                
                // CORREÇÃO: A função 'processFlow' precisa saber qual fluxo carregar.
                // Se 'startNodeId' for um ID de fluxo (ex: 'flow_123'), 'processFlow' deve carregá-lo.
                // Se 'startNodeId' for um ID de nó (ex: 'node_abc'), 'processFlow' deve continuar.
                
                // Simplesmente chamar 'processFlow' com o ID do *fluxo* não funciona.
                // Devemos chamar 'processFlow' com o 'startNodeId' (trigger) *daquele* fluxo.
                
                const [targetFlow] = await sql`SELECT * FROM flows WHERE id = ${targetFlowId} AND bot_id = ${botId}`;
                if (!targetFlow || !targetFlow.nodes) {
                     console.error(`${logPrefix} Fluxo de destino ${targetFlowId} não encontrado.`);
                     break;
                }
                
                const targetFlowData = typeof targetFlow.nodes === 'string' ? JSON.parse(targetFlow.nodes) : targetFlow.nodes;
                const targetStartNode = (targetFlowData.nodes || []).find(n => n.type === 'trigger');
                
                if (!targetStartNode) {
                    console.error(`${logPrefix} Fluxo de destino ${targetFlowId} não tem nó de 'trigger'.`);
                    break;
                }
                
                // Encontra o primeiro nó *depois* do trigger
                const firstNodeId = findNextNode(targetStartNode.id, 'a', targetFlowData.edges || []);
                
                if (firstNodeId) {
                    // Chama o 'processFlow' recursivamente para o *novo* fluxo.
                    await processFlow(chatId, botId, botToken, sellerId, firstNodeId, variables);
                } else {
                    console.log(`${logPrefix} Fluxo de destino ${targetFlowId} está vazio (sem nó após o trigger).`);
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
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
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
    
    // Lógica de Carregamento do Fluxo (Adaptada para 'forward_flow')
    let flow;
    let currentNodeId = startNodeId;


        // Comportamento normal: Carrega o fluxo principal (mais recente) do bot
    [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    

    if (!flow || !flow.nodes) {
        console.log(`${logPrefix} [Flow Engine] Nenhum fluxo encontrado (Bot: ${botId}, Fluxo/Nó: ${startNodeId}).`);
        return;
    }

    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

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
        const currentNode = nodes.find(node => node.id === currentNodeId);
        
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
                    const response = await qstashClient.publishJSON({
                        url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                        body: { 
                            chat_id: chatId, 
                            bot_id: botId, 
                            target_node_id: noReplyNodeId,
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
        // O usuário AINDA está esperando? (Se 'waiting_for_input' for false,
        // significa que ele respondeu e o fluxo principal já continuou).
        const [currentState] = await sql`
            SELECT 1 FROM user_flow_states 
            WHERE chat_id = ${chat_id} 
              AND bot_id = ${bot_id} 
              AND waiting_for_input = true`;

        if (!currentState) {
            // O usuário já respondeu. O fluxo principal já limpou o 'waiting_for_input'.
            // Este timeout chegou atrasado e deve ser ignorado.
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
        await processFlow(
            chat_id, 
            bot_id, 
            botToken, 
            sellerId, 
            target_node_id, // Este é o nó da saída 'b' (Sem Resposta)
            variables
        );

        return res.status(200).json({ message: 'Timeout processed successfully.' });

    } catch (error) {
        console.error('[WORKER] Erro fatal ao processar timeout:', error.message, error.stack);
        // Retornamos 200 para que o QStash NÃO tente re-executar um fluxo que falhou logicamente.
        return res.status(200).json({ error: `Failed to process timeout: ${error.message}` });
    }
}

// Exporta o handler com a verificação de segurança do QStash
module.exports = handler;