// /backend/worker/process-timeout.js

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const { neon } = require('@neondatabase/serverless');
const { verifySignature } = require("@upstash/qstash/nextjs");
const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');

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
    // ESTA É A CÓPIA COMPLETA DA SUA FUNÇÃO
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
        if (!seller.brpix_secret_key || !seller.brpix_company_id) { throw new Error('Credenciais da BR PIX não configuradas.'); }
        const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');
        const payload = { customer: clientPayload, items: [{ title: "Produto Digital", unitPrice: parseInt(value_cents, 10), quantity: 1 }], paymentMethod: "PIX", amount: parseInt(value_cents, 10), pix: { expiresInDays: 1 }, ip: ip_address };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && BRPIX_SPLIT_RECIPIENT_ID) { payload.split = [{ recipientId: BRPIX_SPLIT_RECIPIENT_ID, amount: commission_cents }]; }
        const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, { headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/json' } });
        pixData = response.data;
        return { qr_code_text: pixData.pix.qrcode, qr_code_base64: pixData.pix.qrcode, transaction_id: pixData.id, acquirer: "BRPix", provider };
    } else if (provider === 'syncpay') {
        const token = await getSyncPayAuthToken(seller);
        const payload = { amount: value_cents / 100, payer: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" }, callbackUrl: `https://${host}/api/webhook/syncpay` };
        const commission_percentage = commission_rate * 100;
        if (apiKey !== ADMIN_API_KEY && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) { payload.split = [{ percentage: Math.round(commission_percentage), user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID }]; }
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/cash-in`, payload, { headers: { 'Authorization': `Bearer ${token}` } });
        pixData = response.data;
        return { qr_code_text: pixData.pix_code, qr_code_base64: null, transaction_id: pixData.identifier, acquirer: "SyncPay", provider };
    } else if (provider === 'cnpay' || provider === 'oasyfy') {
        const isCnpay = provider === 'cnpay';
        const publicKey = isCnpay ? seller.cnpay_public_key : seller.oasyfy_public_key;
        const secretKey = isCnpay ? seller.cnpay_secret_key : seller.oasyfy_secret_key;
        if (!publicKey || !secretKey) throw new Error(`Credenciais para ${provider.toUpperCase()} não configuradas.`);
        const apiUrl = isCnpay ? 'https://painel.appcnpay.com/api/v1/gateway/pix/receive' : 'https://app.oasyfy.com/api/v1/gateway/pix/receive';
        const splitId = isCnpay ? CNPAY_SPLIT_PRODUCER_ID : OASYFY_SPLIT_PRODUCER_ID;
        const payload = { identifier: uuidv4(), amount: value_cents / 100, client: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" }, callbackUrl: `https://${host}/api/webhook/${provider}` };
        const commission = parseFloat(((value_cents / 100) * commission_rate).toFixed(2));
        if (apiKey !== ADMIN_API_KEY && commission > 0 && splitId) { payload.splits = [{ producerId: splitId, amount: commission }]; }
        const response = await axios.post(apiUrl, payload, { headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey } });
        pixData = response.data;
        return { qr_code_text: pixData.pix.code, qr_code_base64: pixData.pix.base64, transaction_id: pixData.transactionId, acquirer: isCnpay ? "CNPay" : "Oasy.fy", provider };
    } else { 
        if (!seller.pushinpay_token) throw new Error(`Token da PushinPay não configurado.`);
        const payload = { value: value_cents, webhook_url: `https://${host}/api/webhook/pushinpay` };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && PUSHINPAY_SPLIT_ACCOUNT_ID) { payload.split_rules = [{ value: commission_cents, account_id: PUSHINPAY_SPLIT_ACCOUNT_ID }]; }
        const response = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
        pixData = response.data;
        return { qr_code_text: pixData.qr_code, qr_code_base64: pixData.qr_code_base64, transaction_id: pixData.id, acquirer: "Woovi", provider: 'pushinpay' };
    }
}

function findNextNode(currentNodeId, handleId, edges) {
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null;
}

async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, { chat_id: chatId, action: 'typing' });
    } catch (error) {
        console.warn(`[WORKER - Flow Engine] Falha ao enviar ação 'typing':`, error.response?.data || error.message);
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping) {
    if (!text || text.trim() === '') return;
    try {
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            let typingDuration = Math.max(500, Math.min(2000, text.length * 50));
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }
        const response = await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        if (response.data.ok) {
            const sentMessage = response.data.result;
            await sql`INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, message_text, sender_type) VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, 'Bot', '(Fluxo)', ${text}, 'bot') ON CONFLICT (chat_id, message_id) DO NOTHING;`;
        }
    } catch (error) {
        console.error(`[WORKER - Flow Engine] Erro ao enviar/salvar mensagem:`, error.response?.data || error.message);
    }
}

async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    
    console.log(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);
    const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    if (!flow || !flow.nodes) {
        console.log(`${logPrefix} [Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
        return;
    }

    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

    let currentNodeId = startNodeId;
    let variables = initialVariables;
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
                variables = { ...initialVariables, ...(userState.variables || {}) };
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
                if (currentNode.data.typingDelay && currentNode.data.typingDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, currentNode.data.typingDelay * 1000));
                }
                await sendMessage(chatId, currentNode.data.text, botToken, sellerId, botId, currentNode.data.showTyping);
                if (currentNode.data.waitForReply) {
                    await sql`UPDATE user_flow_states SET waiting_for_input = true WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                    if (noReplyNodeId) {
                        const timeoutMinutes = currentNode.data.replyTimeout || 5;
                        console.log(`${logPrefix} [Flow Engine] Agendando worker via QStash em ${timeoutMinutes} min para o nó ${noReplyNodeId}`);
                        const response = await qstashClient.publishJSON({
                            url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                            body: { chat_id: chatId, bot_id: botId, target_node_id: noReplyNodeId, variables: variables },
                            delay: `${timeoutMinutes}m`,
                            contentBasedDeduplication: true,
                            "Upstash-Method": "POST"
                        });
                        // Remova as aspas simples ao redor de ${response.messageId}
                        await sql`UPDATE user_flow_states SET scheduled_message_id = ${response.messageId} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;                    }
                    currentNodeId = null;
                } else {
                    currentNodeId = findNextNode(currentNodeId, 'a', edges);
                }
                break;

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
                    const [userFlowState] = await sql`SELECT variables FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const click_id = userFlowState.variables.click_id;
                    if (!click_id) throw new Error("Click ID não encontrado nas variáveis do fluxo.");
                    
                    const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error("Dados do clique não encontrados para gerar o PIX.");

                    const provider = seller.pix_provider_primary || 'pushinpay';
                    const ip_address = click.ip_address;
                    const pixResult = await generatePixForProvider(provider, seller, valueInCents, HOTTRACK_API_URL.replace('/api', ''), seller.api_key, ip_address);
                    
                    await sql`INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, provider, provider_transaction_id, pix_id) VALUES (${click.id}, ${valueInCents / 100}, ${pixResult.qr_code_text}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id})`;
                    
                    variables.last_transaction_id = pixResult.transaction_id;
                    await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    
                    await sendMessage(chatId, `Pix copia e cola gerado:\n\n\`${pixResult.qr_code_text}\``, botToken, sellerId, botId, true);
                } catch (error) {
                    console.error("[Flow Engine] Erro ao gerar PIX:", error);
                    await sendMessage(chatId, "Desculpe, não consegui gerar o PIX neste momento. Tente novamente mais tarde.", botToken, sellerId, botId, true);
                }
                currentNodeId = findNextNode(currentNodeId, null, edges);
                break;

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX encontrado para consultar.");
                    
                    const [transaction] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        await sendMessage(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'a', edges); // Caminho 'Pago'
                    } else {
                         await sendMessage(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'b', edges); // Caminho 'Pendente'
                    }
                } catch (error) {
                     console.error("[Flow Engine] Erro ao consultar PIX:", error);
                     await sendMessage(chatId, "Não consegui consultar o status do PIX agora.", botToken, sellerId, botId, true);
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
//                      LÓGICA DO WORKER
// ==========================================================

async function handler(req, res) {
    try {
        const { chat_id, bot_id, target_node_id, variables } = req.body;
        console.log(`[WORKER] Recebido job de timeout para chat: ${chat_id}, bot: ${bot_id}`);

        const [userState] = await sql`SELECT waiting_for_input FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

        if (userState && userState.waiting_for_input) {
            console.log(`[WORKER] Timeout confirmado! Processando fluxo a partir do nó ${target_node_id}`);
            
            const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;

            if (bot) {
                await sql`UPDATE user_flow_states SET waiting_for_input = false, scheduled_message_id = NULL WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;
                await processFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, target_node_id, variables);
            } else {
                console.error(`[WORKER] Bot com ID ${bot_id} não encontrado.`);
            }
        } else {
            console.log(`[WORKER] Tarefa para ${chat_id} ignorada, pois o usuário já respondeu.`);
        }
        res.status(200).send('Worker finished successfully.');
    } catch (error) {
        console.error('[WORKER] Erro crítico ao processar timeout:', error);
        res.status(500).send('Erro interno no worker.');
    }
}

// Exporta o handler com a verificação de segurança do QStash
module.exports = verifySignature(handler);