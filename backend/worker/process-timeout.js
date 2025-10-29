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

async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address) {
    let pixData;
    let acquirer = 'Não identificado';
    const commission_rate = seller.commission_rate || 0.0299;
    
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

async function processActions(actions, chatId, botId, botToken, sellerId, variables, edges) {
    // A variável 'logPrefix' não está definida aqui, mas pode ser adicionada se necessário para logs
    // const logPrefix = '[WORKER]';

    for (const action of actions) {
        // Assume que 'action' tem a estrutura { type: '...', data: { ... } }
        const actionData = action.data || {}; // Garante que actionData exista

        switch (action.type) {
            case 'message':

                const textToSend = await replaceVariables(actionData.text, variables);
                // Passa actionData.showTyping
                await sendMessage(chatId, textToSend, botToken, sellerId, botId, actionData.showTyping, variables);

                // Processa recursivamente se ESTA ação tiver ações aninhadas
                if (actionData.actions && actionData.actions.length > 0) {
                    await processActions(actionData.actions, chatId, botId, botToken, sellerId, variables, edges);
                }

                // Lógica 'waitForReply' NÃO pertence aqui, pois é uma característica do NÓ principal.
                // O fluxo continua após processar TODAS as ações aninhadas.
                break; // Fim do case 'message'

            case 'image':
            case 'video':
            case 'audio': { // Usar {} para criar um escopo para 'caption' e 'response'
                try {
                    const caption = await replaceVariables(actionData.caption, variables);
                    // handleMediaNode precisa do objeto 'action' que contém 'type' e 'data'
                    const response = await handleMediaNode(action, botToken, chatId, caption);

                    if (response && response.ok) {
                        // Passa sellerId e botId corretamente
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    // action.id não existe no objeto action padrão, use o contexto disponível
                    console.error(`[WORKER - Flow Media] Erro ao enviar mídia (ação aninhada tipo ${action.type}) para o chat ${chatId}: ${e.message}`);
                }
                // Não determinar o próximo nó aqui
                break;
            } // Fim do case 'image'/'video'/'audio'

            case 'delay':
                const delaySeconds = actionData.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                // Não determinar o próximo nó aqui
                break;

            case 'typing_action':
                if (actionData.durationInSeconds && actionData.durationInSeconds > 0) {
                    await showTypingForDuration(chatId, botToken, actionData.durationInSeconds * 1000);
                }
                break;

            case 'action_pix':
                try {
                    const valueInCents = actionData.valueInCents;
                    if (!valueInCents) throw new Error("Valor do PIX não definido na ação do fluxo.");

                    const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    if (!seller) throw new Error(`[WORKER] Vendedor ${sellerId} não encontrado na ação PIX.`);

                    // Busca click_id das variáveis ou do banco
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
                        throw new Error("[WORKER] Click ID não encontrado para gerar PIX na ação aninhada.");
                    }

                    const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                    const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error("[WORKER] Dados do clique não encontrados para gerar PIX na ação aninhada.");

                    const ip_address = click.ip_address;
                    const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost'; // Usa a URL da env var
                    const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);

                    // Atualiza a variável 'last_transaction_id' nas variáveis do fluxo
                    variables.last_transaction_id = pixResult.transaction_id;
                     // IMPORTANTE: Persistir as variáveis atualizadas no estado do usuário, se necessário
                     // Esta função 'processActions' não tem acesso direto para atualizar user_flow_states
                     // O ideal seria que 'processFlow' atualizasse as variáveis após chamar processActions
                     // await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;


                    // Envia o PIX para o usuário
                    const messageText = await replaceVariables(actionData.pixMessage || "", variables);
                    const buttonText = await replaceVariables(actionData.pixButtonText || "📋 Copiar Código PIX", variables);
                    const textToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;

                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                        chat_id: chatId,
                        text: textToSend,
                        parse_mode: 'HTML',
                        reply_markup: {
                            inline_keyboard: [
                                [{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]
                            ]
                        }
                    });

                    if (sentMessage.ok) {
                        await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot');
                    }

                } catch (error) {
                    console.error(`[WORKER - Flow Engine] Erro na ação action_pix para chat ${chatId}:`, error);
                    console.log(chatId, "Desculpe, não consegui gerar o PIX neste momento.", botToken, sellerId, botId, true);
                    // Não determinar o próximo nó aqui ou parar o fluxo principal
                }
                break; // Fim do case 'action_pix'

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("[WORKER] Nenhum ID de transação PIX encontrado para consultar na ação aninhada.");

                    const [transaction] = await sql`SELECT status FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    if (!transaction) throw new Error(`[WORKER] Transação ${transactionId} não encontrada na ação aninhada.`);

                    // Apenas envia a mensagem de status, não muda o fluxo principal
                    if (transaction.status === 'paid') {
                        console.log(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true);
                    } else {
                        console.log(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true);
                    }
                } catch (error) {
                    console.error("[WORKER - Flow Engine] Erro ao consultar PIX na ação aninhada:", error);
                }
                break; // Fim do case 'action_check_pix'

            default:
                console.warn(`[WORKER - Flow Engine] Tipo de ação aninhada desconhecida: ${action.type}. Ignorando.`);
                break;
        }
        // Não adicionar lógica de 'findNextNode' ou 'currentNodeId = null' aqui.
        // O loop 'for...of' continuará para a próxima ação aninhada.
    }
    // A função termina após processar todas as 'actions' fornecidas.
}

async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    
    console.log(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);

    // ==========================================================
    // PASSO 1: CARREGAR AS VARIÁVEIS NO INÍCIO
    // ==========================================================
    let variables = { ...initialVariables };

    // Pega os dados do usuário da última mensagem ENVIADA PELO USUÁRIO
    const [user] = await sql`
        SELECT first_name, last_name 
        FROM telegram_chats 
        WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
        ORDER BY created_at DESC LIMIT 1`;

    if (user) {
        variables.primeiro_nome = user.first_name || '';
        variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }

    // Se tiver um click_id, busca os dados de geolocalização (cidade)
    if (variables.click_id) {
        const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
        const [click] = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id}`;
        if (click) {
            variables.cidade = click.city || 'Desconhecida';
            variables.estado = click.state || 'Desconhecido';
        }
    }
    // ==========================================================
    // FIM DO PASSO 1
    // ==========================================================
    
    const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    if (!flow || !flow.nodes) {
        console.log(`${logPrefix} [Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
        return;
    }

    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

    let currentNodeId = startNodeId;
    const isStartCommand = initialVariables.click_id && initialVariables.click_id.startsWith('/start');

    if (!currentNodeId) {
        if (isStartCommand) {
            console.log(`${logPrefix} [Flow Engine] Comando /start detectado. Reiniciando fluxo.`);
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            const startNode = nodes.find(node => node.type === 'trigger');
            if (startNode) {
                currentNodeId = findNextNode(startNode.id, null, edges);
            }
        } else {
            const [userState] = await sql`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (userState && userState.waiting_for_input) {
                console.log(`${logPrefix} [Flow Engine] Usuário respondeu. Continuando.`);
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
                
                let parsedVariables = {};
                if (userState.variables) {
                    try {
                        parsedVariables = JSON.parse(userState.variables);
                    } catch (e) {
                        parsedVariables = userState.variables;
                    }
                }
                // Une as variáveis já carregadas com as salvas no estado
                variables = { ...variables, ...parsedVariables };

            } else {
                console.log(`${logPrefix} [Flow Engine] Nova conversa sem /start. Iniciando do gatilho.`);
                const startNode = nodes.find(node => node.type === 'trigger');
                if (startNode) {
                    currentNodeId = findNextNode(startNode.id, null, edges);
                }
            }
        }
    }

    if (!currentNodeId) {
        console.log(`${logPrefix} [Flow Engine] Nenhum nó para processar. Fim do fluxo.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        return;
    }

    let safetyLock = 0;
    while (currentNodeId && safetyLock < 20) {
        const currentNode = nodes.find(node => node.id === currentNodeId);
        if (!currentNode) {
            console.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        await sql`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET current_node_id = EXCLUDED.current_node_id, variables = EXCLUDED.variables, waiting_for_input = false, scheduled_message_id = NULL;
        `;

        switch (currentNode.type) {
            case 'message':

                // ==========================================================
                // PASSO 2: USAR A VARIÁVEL CORRETA AO ENVIAR A MENSAGEM
                // ==========================================================
                const textToSend = await replaceVariables(currentNode.data.text, variables);
                await sendMessage(chatId, textToSend, botToken, sellerId, botId, currentNode.data.showTyping, variables);
                // ==========================================================
                // FIM DO PASSO 2
                // ==========================================================

                // Execute nested actions if any
                if (currentNode.data.actions && currentNode.data.actions.length > 0) {
                    await processActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, edges);
                }
                
                if (currentNode.data.waitForReply) {
                    await sql`UPDATE user_flow_states SET waiting_for_input = true WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                    
                    if (noReplyNodeId) {
                        const timeoutMinutes = currentNode.data.replyTimeout || 5;
                        console.log(`${logPrefix} [Flow Engine] Agendando worker em ${timeoutMinutes} min para o nó ${noReplyNodeId}`);

                        try {
                            // Cancela qualquer tarefa antiga antes de agendar uma nova.
                            const [existingState] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                            if (existingState && existingState.scheduled_message_id) {
                                try {
                                    await qstashClient.messages.delete(existingState.scheduled_message_id);
                                    console.log(`[Flow Engine] Tarefa de timeout antiga ${existingState.scheduled_message_id} cancelada antes de agendar a nova.`);
                                } catch (e) {
                                    console.warn(`[Flow Engine] Não foi possível cancelar a tarefa antiga ${existingState.scheduled_message_id} (pode já ter sido executada):`, e.message);
                                }
                            }

                            JSON.stringify(variables);
                            const response = await qstashClient.publishJSON({
                                url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                                body: { chat_id: chatId, bot_id: botId, target_node_id: noReplyNodeId, variables: variables },
                                delay: `${timeoutMinutes}m`,
                                contentBasedDeduplication: true,
                                method: "POST"
                            });
                            await sql`UPDATE user_flow_states SET scheduled_message_id = ${response.messageId} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                        } catch (error) {
                            console.error("--- ERRO FATAL DE SERIALIZAÇÃO ---");
                            console.error(`O objeto 'variables' para o chat ${chatId} não pôde ser convertido para JSON.`);
                            console.error("Conteúdo do objeto problemático:", variables);
                            console.error("Erro original:", error.message);
                            console.error("--- FIM DO ERRO ---");
                        }
                    }
                    currentNodeId = null;
                } else {
                    currentNodeId = findNextNode(currentNodeId, 'a', edges);
                }
                break;

            // ===== IMPLEMENTAÇÃO DOS NÓS DE MÍDIA =====
            case 'image':
            case 'video':
            case 'audio': {
                try {
                    const caption = await replaceVariables(currentNode.data.caption, variables);
                    const response = await handleMediaNode(currentNode, botToken, chatId, caption);

                    if (response && response.ok) {
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    console.error(`[Flow Media] Erro ao enviar mídia no nó ${currentNode.id} para o chat ${chatId}: ${e.message}`);
                }
                currentNodeId = findNextNode(currentNodeId, 'a', edges);
                break;
            }
            // ===========================================

            case 'delay':
                const delaySeconds = currentNode.data.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                currentNodeId = findNextNode(currentNodeId, null, edges);
                break;
            
                case 'action_pix':
                    try {
                        const valueInCents = currentNode.data.valueInCents;
                        if (!valueInCents) throw new Error("Valor do PIX não definido no nó do fluxo.");
    
                        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                         // Adiciona verificação do seller
                        if (!seller) throw new Error(`Vendedor ${sellerId} não encontrado no processFlow.`);
    
                        // Busca o click_id das variáveis do fluxo
                        const click_id_from_vars = variables.click_id;
                        // Se não encontrou nas variáveis, tenta buscar do banco de dados
                        if (!click_id_from_vars) {
                            const [recentClick] = await sql`
                                SELECT click_id FROM telegram_chats 
                                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL 
                                ORDER BY created_at DESC LIMIT 1
                            `;
                            if (recentClick?.click_id) {
                                click_id_from_vars = recentClick.click_id;
                                console.log(`[WORKER] Click ID recuperado do banco: ${click_id_from_vars}`);
                            }
                        }
                        
                        if (!click_id_from_vars) {
                            console.error(`[WORKER] Click ID não encontrado para chat ${chatId}, bot ${botId}. Variáveis:`, variables);
                            throw new Error("Click ID não encontrado nas variáveis do fluxo nem no histórico do chat.");
                        }
    
                        const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                        const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                        if (!click) throw new Error("Dados do clique não encontrados ou não pertencem ao vendedor para gerar o PIX no fluxo.");
    
                        const ip_address = click.ip_address; // IP do clique original
    
                        // *** SUBSTITUIÇÃO DA CHAMADA DIRETA PELA NOVA FUNÇÃO ***
                        // Usar 'localhost' ou um placeholder se req.headers.host não estiver disponível aqui
                        const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
                        const pixResult = await generatePixForProvider('brpix', seller, valueInCents, hostPlaceholder, seller.api_key, ip_address); // Passa click.id
    
                        // O INSERT já foi feito dentro de generatePixWithFallback
    
                        variables.last_transaction_id = pixResult.transaction_id;
                        // Salva as variáveis atualizadas (com o last_transaction_id)
                        await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
    
                        // Envia o PIX para o usuário
                        const messageText = await replaceVariables(currentNode.data.pixMessage || "", variables);
                        const buttonText = await replaceVariables(currentNode.data.pixButtonText || "📋 Copiar Código PIX", variables);
                        const textToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                        const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                            chat_id: chatId,
                            text: textToSend,
                            parse_mode: 'HTML',
                            reply_markup: {
                                inline_keyboard: [
                                    [{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]
                                ]
                            }
                        });
    
                         if (sentMessage.ok) {
                             await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot'); // Salva como 'bot'
                         }
    
                    } catch (error) {
                        console.error(`[Flow Engine] Erro no nó action_pix para chat ${chatId}:`, error);
                        // Informa o usuário sobre o erro
                        console.log(chatId, "Desculpe, não consegui gerar o PIX neste momento. Tente novamente mais tarde.", botToken, sellerId, botId, true);
                        // Decide se o fluxo deve parar ou seguir por um caminho de erro (se houver)
                        // Por enquanto, vamos parar aqui para evitar loops
                        currentNodeId = null; // Para o fluxo neste ponto em caso de erro no PIX
                        break; // Sai do switch
                    }
                    // Se chegou aqui, o PIX foi gerado e enviado com sucesso
                    currentNodeId = findNextNode(currentNodeId, 'a', edges); // Assume que a saída 'a' é o caminho de sucesso
                    break; // Sai do switch

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX encontrado para consultar.");
                    
                    const [transaction] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        console.log(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'a', edges); // Caminho 'Pago'
                    } else {
                        console.log(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'b', edges); // Caminho 'Pendente'
                    }
                } catch (error) {
                    console.error("[Flow Engine] Erro ao consultar PIX:", error);
                    currentNodeId = findNextNode(currentNodeId, 'b', edges);
                }
                break;

                default:
                    console.warn(`${logPrefix} [Flow Engine] Tipo de nó desconhecido: ${currentNode.type}. Parando fluxo.`);
                    currentNodeId = null;
                    break;
            }

            if (!currentNodeId) {
                const [state] = await sql`SELECT 1 FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId} AND waiting_for_input = true`;
                if(!state){
                    console.log(`${logPrefix} [Flow Engine] Fim do fluxo para ${chatId}. Limpando estado.`);
                    await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                }
            }
            safetyLock++;
    }
}


// ==========================================================
// ==========================================================

async function handler(req, res) {
    try {
        // 1. Extrai os dados do corpo da requisição E o ID da mensagem do header do QStash
        const { chat_id, bot_id, target_node_id, variables } = req.body;
        const messageId = req.headers['upstash-message-id'];
        console.log(`[WORKER] Recebido job de timeout para chat: ${chat_id}, bot: ${bot_id}, msg: ${messageId}`);

        // 2. Verifica se o usuário ainda está no estado de "aguardando input"
        const [userState] = await sql`
            SELECT waiting_for_input, scheduled_message_id 
            FROM user_flow_states 
            WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}
        `;

        // 3. Se o estado não existe ou o usuário já respondeu, o trabalho do worker termina.
        if (!userState || !userState.waiting_for_input) {
            console.log(`[WORKER] Job para chat ${chat_id} ignorado. O usuário já respondeu ou o fluxo foi resetado.`);
            return res.status(200).json({ message: 'Ignorado: Estado não encontrado ou não está aguardando.' });
        }

        // 4. VERIFICAÇÃO CRÍTICA: O ID da tarefa no banco é o mesmo desta execução?
        if (userState.scheduled_message_id !== messageId) {
            console.log(`[WORKER] Job (Msg ID: ${messageId}) obsoleto. Uma nova tarefa (${userState.scheduled_message_id}) já está agendada. Abortando.`);
            return res.status(200).json({ message: 'Ignorado: Tarefa obsoleta.' });
        }
        
        // 5. Se a tarefa é válida, continua o fluxo de timeout
        console.log(`[WORKER] Timeout confirmado! Processando fluxo a partir do nó ${target_node_id}`);
        
        const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;
        if (!bot) {
            console.error(`[WORKER] Bot ${bot_id} não encontrado. Abortando.`);
            return res.status(500).json({ message: 'Bot não encontrado.' });
        }

        // Garante que as variáveis existam antes de chamar o processFlow
        const finalVariables = variables || {};

        // Chama o motor de fluxo para executar o nó de timeout
        if (target_node_id) {
            // Chama o motor de fluxo para executar o nó de timeout
            await processFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, target_node_id, finalVariables);
        } else {
            console.log(`[WORKER] Timeout para chat ${chat_id}. Encerrando fluxo.`);
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;
        }
        
        res.status(200).json({ message: 'Timeout processado com sucesso.' });

    } catch (error) {
        console.error('[WORKER] Erro fatal ao processar timeout:', error);
        res.status(500).json({ message: 'Erro interno no worker.' });
    }
}

// Exporta o handler com a verificação de segurança do QStash
module.exports = handler;