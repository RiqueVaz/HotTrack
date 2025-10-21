// VERSÃO FINAL E COMPLETA - PRONTA PARA PRODUÇÃO
const express = require('express');
const cors = require('cors');
const { neon } = require('@neondatabase/serverless');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const webpush = require('web-push');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' })); // Middleware global para processar JSON e aumentar limite

// --- OTIMIZAÇÃO CRÍTICA: A conexão com o banco é inicializada UMA VZ e reutilizada ---
const sql = neon(process.env.DATABASE_URL);

// --- ROTA DO CRON JOB ---
app.post('/api/cron/process-timeouts', async (req, res) => {
    const cronSecret = process.env.CRON_SECRET;
    if (req.headers['authorization'] !== `Bearer ${cronSecret}`) {
        return res.status(401).send('Unauthorized');
    }
    try {
        const pendingTimeouts = await sql`SELECT * FROM flow_timeouts WHERE execute_at <= NOW()`;
        if (pendingTimeouts.length > 0) {
            console.log(`[CRON] Encontrados ${pendingTimeouts.length} timeouts para processar.`);
            for (const timeout of pendingTimeouts) {
                const { chat_id, bot_id, target_node_id, variables } = timeout;
                await sql`DELETE FROM flow_timeouts WHERE id = ${timeout.id}`;
                const [userState] = await sql`SELECT waiting_for_input FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;
                if (userState && userState.waiting_for_input) {
                    const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;
                    if (bot) {
                        processFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, target_node_id, variables);
                    }
                }
            }
        }
        res.status(200).send(`Processados ${pendingTimeouts.length} timeouts.`);
    } catch (error) {
        console.error('[CRON] Erro ao processar timeouts:', error);
        res.status(500).send('Erro interno no servidor.');
    }
});

// --- CONFIGURAÇÃO DAS NOTIFICAÇÕES ---
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    webpush.setVapidDetails(
        process.env.VAPID_SUBJECT,
        process.env.VAPID_PUBLIC_KEY,
        process.env.VAPID_PRIVATE_KEY
    );
}
let adminSubscription = null;

// --- CONFIGURAÇÃO ---
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();

// --- MIDDLEWARE DE AUTENTICAÇÃO ---
async function authenticateJwt(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ message: 'Token não fornecido.' });

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ message: 'Token inválido ou expirado.' });
        req.user = user;
        next();
    });
}

// --- MIDDLEWARE DE AUTENTICAÇÃO POR API KEY ---
async function authenticateApiKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) {
        return res.status(401).json({ message: 'Chave de API não fornecida.' });
    }
    try {
        const sellerResult = await sql`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            return res.status(401).json({ message: 'Chave de API inválida.' });
        }
        req.sellerId = sellerResult[0].id;
        next();
    } catch (error) {
        console.error("Erro na autenticação por API Key:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
}


// --- MIDDLEWARE DE LOG DE REQUISIÇÕES ---
async function logApiRequest(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) return next();
    try {
        const sellerResult = await sql`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length > 0) {
            sql`INSERT INTO api_requests (seller_id, endpoint) VALUES (${sellerResult[0].id}, ${req.path})`.catch(err => console.error("Falha ao logar requisição:", err));
        }
    } catch (error) {
        console.error("Erro no middleware de log:", error);
    }
    next();
}

// --- FUNÇÕES DE LÓGICA DE NEGÓCIO ---
async function getSyncPayAuthToken(seller) {
    const cachedToken = syncPayTokenCache.get(seller.id);
    if (cachedToken && cachedToken.expiresAt > Date.now() + 60000) { // Add 60s buffer
        return cachedToken.accessToken;
    }

    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) {
        throw new Error('Credenciais da SyncPay não configuradas para este vendedor.');
    }
    console.log(`[SyncPay] Solicitando novo token para o vendedor ID: ${seller.id}`);
    try {
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/auth-token`, {
            client_id: seller.syncpay_client_id,
            client_secret: seller.syncpay_client_secret,
        });
        const { access_token, expires_in } = response.data;
        const expiresAt = Date.now() + (expires_in * 1000); // Convert expires_in seconds to milliseconds
        syncPayTokenCache.set(seller.id, { accessToken: access_token, expiresAt });
        return access_token;
    } catch (error) {
        console.error(`[SyncPay Auth Error] Seller ID: ${seller.id}`, error.response?.data || error.message);
        throw new Error(`Falha ao obter token da SyncPay: ${error.response?.data?.message || error.message}`);
    }
}

async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address) {
    let pixData;
    let acquirer = 'Não identificado';
    const commission_rate = seller.commission_rate || 0.0299; // Default 2.99% commission

    // Default customer data if not provided
    const clientPayload = {
        document: { number: "21376710773", type: "CPF" }, // Use generic or request from user
        name: "Cliente Padrão",
        email: "gabriel@email.com", // Use generic or request from user
        phone: "27995310379" // Use generic or request from user
    };

    if (provider === 'brpix') {
        if (!seller.brpix_secret_key || !seller.brpix_company_id) {
            throw new Error('Credenciais da BR PIX não configuradas para este vendedor.');
        }
        // Basic Auth: base64(secret_key:company_id)
        const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');

        const payload = {
            customer: clientPayload,
            items: [{ title: "Produto Digital", unitPrice: parseInt(value_cents, 10), quantity: 1 }],
            paymentMethod: "PIX",
            amount: parseInt(value_cents, 10), // Amount in cents
            pix: { expiresInDays: 1 }, // PIX expiry
            ip: ip_address
        };

        // Add split rule if applicable
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && BRPIX_SPLIT_RECIPIENT_ID) {
            payload.split = [{ recipientId: BRPIX_SPLIT_RECIPIENT_ID, amount: commission_cents }];
        }
        // Make the API call to BRPix
        const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, {
            headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/json' }
        });
        pixData = response.data;
        acquirer = "BRPix";
        return {
            qr_code_text: pixData.pix.qrcode, // The PIX copy-paste code
            qr_code_base64: pixData.pix.qrcode_base64, // BRPix now provides base64
            transaction_id: pixData.id, // BRPix's transaction ID
            acquirer,
            provider
        };
    } else if (provider === 'syncpay') {
        const token = await getSyncPayAuthToken(seller);
        const payload = {
            amount: value_cents / 100, // SyncPay expects amount in Reais
            payer: {
                name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379"
            },
            callbackUrl: `https://${host}/api/webhook/syncpay` // Webhook URL for payment confirmation
        };
        // Add split rule if applicable (SyncPay uses percentage)
        const commission_percentage = commission_rate * 100;
        if (apiKey !== ADMIN_API_KEY && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) {
            payload.split = [{
                percentage: Math.round(commission_percentage), // Rounded percentage
                user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID // Your partner/split account ID
            }];
        }
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/cash-in`, payload, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        pixData = response.data;
        acquirer = "SyncPay";
        return {
            qr_code_text: pixData.pix_code,
            qr_code_base64: pixData.base64image || null, // Check if base64image is returned
            transaction_id: pixData.identifier, // SyncPay's transaction ID
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
            identifier: uuidv4(), // Unique identifier for the transaction
            amount: value_cents / 100, // Amount in Reais
            client: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" },
            callbackUrl: `https://${host}/api/webhook/${provider}` // Webhook URL
        };
        // Add split rule (amount in Reais)
        const commission = parseFloat(((value_cents / 100) * commission_rate).toFixed(2));
        if (apiKey !== ADMIN_API_KEY && commission > 0 && splitId) {
            payload.splits = [{ producerId: splitId, amount: commission }];
        }
        const response = await axios.post(apiUrl, payload, { headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey } });
        pixData = response.data;
        acquirer = isCnpay ? "CNPay" : "Oasy.fy";
        return { qr_code_text: pixData.pix.code, qr_code_base64: pixData.pix.base64, transaction_id: pixData.transactionId, acquirer, provider };
    } else { // Default is PushinPay
        if (!seller.pushinpay_token) throw new Error(`Token da PushinPay não configurado.`);
        const payload = {
            value: value_cents, // Amount in cents
            webhook_url: `https://${host}/api/webhook/pushinpay`, // Webhook URL
        };
        // Add split rule (amount in cents)
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && PUSHINPAY_SPLIT_ACCOUNT_ID) {
            payload.split_rules = [{ value: commission_cents, account_id: PUSHINPAY_SPLIT_ACCOUNT_ID }];
        }
        const pushinpayResponse = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
        pixData = pushinpayResponse.data;
        acquirer = "Woovi"; // PushinPay uses Woovi backend
        return { qr_code_text: pixData.qr_code, qr_code_base64: pixData.qr_code_base64, transaction_id: pixData.id, acquirer, provider: 'pushinpay' };
    }
}


async function handleSuccessfulPayment(transaction_id, customerData) {
    try {
        // Update transaction status to 'paid' and get the updated record
        const [transaction] = await sql`UPDATE pix_transactions SET status = 'paid', paid_at = NOW() WHERE id = ${transaction_id} AND status != 'paid' RETURNING *`;

        // If transaction was already paid or not found, do nothing
        if (!transaction) {
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já processada ou não encontrada.`);
            return;
        }

        console.log(`[handleSuccessfulPayment] Processando pagamento para transação ${transaction_id}.`);

        // Send push notification to admin if subscribed
        if (adminSubscription && webpush) {
            const payload = JSON.stringify({
                title: 'Nova Venda Paga!',
                body: `Venda de R$ ${parseFloat(transaction.pix_value).toFixed(2)} foi confirmada.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(error => {
                if (error.statusCode === 410) { // Gone error means subscription is no longer valid
                    console.log("Inscrição de notificação expirada. Removendo.");
                    adminSubscription = null;
                } else {
                    console.warn("Falha ao enviar notificação push (não-crítico):", error.message);
                }
            });
        }

        // Get click data and seller data associated with the transaction
        const [click] = await sql`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${click.seller_id}`;

        if (click && seller) {
            const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
            const productData = { id: "prod_final", name: "Produto Vendido" }; // Or derive from checkout config if available

            // Send events to external services
            await sendEventToUtmify('paid', click, transaction, seller, finalCustomerData, productData);
            await sendMetaEvent('Purchase', click, transaction, finalCustomerData);
        } else {
            console.error(`[handleSuccessfulPayment] ERRO: Não foi possível encontrar dados do clique ou vendedor para a transação ${transaction_id}`);
        }
    } catch(error) {
        console.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
    }
}


// --- ROTAS DO PAINEL ADMINISTRATIVO ---
// Middleware to authenticate admin requests using a specific API key
function authenticateAdmin(req, res, next) {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ message: 'Acesso negado. Chave de administrador inválida.' });
    }
    next();
}

// Function to resend historical Purchase events to Meta
async function sendHistoricalMetaEvent(eventName, clickData, transactionData, targetPixel) {
    let payload_sent = null; // Store the payload for logging
    try {
        // Prepare user data for Meta event, hashing sensitive fields
        const userData = {};
        if (clickData.ip_address) userData.client_ip_address = clickData.ip_address;
        if (clickData.user_agent) userData.client_user_agent = clickData.user_agent;
        if (clickData.fbp) userData.fbp = clickData.fbp;
        if (clickData.fbc) userData.fbc = clickData.fbc;
        // Hash names if available from Telegram chat data
        if (clickData.firstName) userData.fn = crypto.createHash('sha256').update(clickData.firstName.toLowerCase()).digest('hex');
        if (clickData.lastName) userData.ln = crypto.createHash('sha256').update(clickData.lastName.toLowerCase()).digest('hex');

        // Hash city and state if available
        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        // Remove undefined fields
        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);

        const { pixelId, accessToken } = targetPixel;
        const event_time = Math.floor(new Date(transactionData.paid_at).getTime() / 1000); // Unix timestamp of payment
        const event_id = `${eventName}.${transactionData.id}.${pixelId}`; // Unique event ID

        // Construct the Meta event payload
        const payload = {
            data: [{
                event_name: eventName,
                event_time: event_time,
                event_id,
                action_source: 'other', // Since it's server-side
                user_data: userData,
                custom_data: {
                    currency: 'BRL',
                    value: parseFloat(transactionData.pix_value) // Purchase value
                },
            }]
        };
        payload_sent = payload; // Store for logging

        // Basic validation: requires IP or User Agent
        if (!userData.client_ip_address && !userData.client_user_agent) {
             throw new Error('Dados de usuário insuficientes para envio (IP/UserAgent faltando).');
        }

        // Send the event to Meta Conversion API
        await axios.post(`https://graph.facebook.com/v19.0/${pixelId}/events`, payload, { params: { access_token: accessToken } });

        return { success: true, payload: payload_sent };

    } catch (error) {
        // Log detailed error from Meta or Axios
        const metaError = error.response?.data?.error || { message: error.message };
        console.error(`Erro ao reenviar evento (Transação ID: ${transactionData.id}):`, metaError.message);
        return { success: false, error: metaError, payload: payload_sent };
    }
}

// Admin route to resend Purchase events in batches
app.post('/api/admin/resend-events', authenticateAdmin, async (req, res) => {
    const {
        target_pixel_id, target_meta_api_token, seller_id,
        start_date, end_date, page = 1, limit = 50
    } = req.body;

    if (!target_pixel_id || !target_meta_api_token || !start_date || !end_date) {
        return res.status(400).json({ message: 'Todos os campos obrigatórios devem ser preenchidos.' });
    }

    try {
        // Build the query based on whether a specific seller_id is provided
        const query = seller_id
            ? sql`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND c.seller_id = ${seller_id} AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`
            : sql`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`;

        const allPaidTransactions = await query;

        if (allPaidTransactions.length === 0) {
            return res.status(200).json({
                total_events: 0,
                total_pages: 0,
                message: 'Nenhuma transação paga encontrada para os filtros fornecidos.'
            });
        }

        // Fetch Telegram user names associated with the clicks for better hashing
        const clickIds = allPaidTransactions.map(t => t.click_id).filter(Boolean);
        let userDataMap = new Map();
        if (clickIds.length > 0) {
            const telegramUsers = await sql`
                SELECT click_id, first_name, last_name
                FROM telegram_chats
                WHERE click_id = ANY(${clickIds})
            `;
            telegramUsers.forEach(user => {
                // Ensure click_id format matches the one stored in clicks table
                const cleanClickId = user.click_id.startsWith('/start ') ? user.click_id : `/start ${user.click_id}`;
                userDataMap.set(cleanClickId, { firstName: user.first_name, lastName: user.last_name });
            });
        }

        // Pagination logic
        const totalEvents = allPaidTransactions.length;
        const totalPages = Math.ceil(totalEvents / limit);
        const offset = (page - 1) * limit;
        const batch = allPaidTransactions.slice(offset, offset + limit);

        const detailedResults = [];
        const targetPixel = { pixelId: target_pixel_id, accessToken: target_meta_api_token };

        console.log(`[ADMIN] Processando página ${page}/${totalPages}. Lote com ${batch.length} eventos.`);

        // Process the batch, sending events one by one with a small delay
        for (const transaction of batch) {
            const extraUserData = userDataMap.get(transaction.click_id); // Get names if available
            const enrichedTransactionData = { ...transaction, ...extraUserData }; // Combine data
            const result = await sendHistoricalMetaEvent('Purchase', enrichedTransactionData, transaction, targetPixel);

            detailedResults.push({
                transaction_id: transaction.id,
                status: result.success ? 'success' : 'failure',
                payload_sent: result.payload,
                meta_response: result.error || 'Enviado com sucesso.'
            });
            await new Promise(resolve => setTimeout(resolve, 100)); // Small delay between events
        }

        res.status(200).json({
            total_events: totalEvents,
            total_pages: totalPages,
            current_page: page,
            limit: limit,
            results: detailedResults
        });

    } catch (error) {
        console.error("Erro geral na rota de reenviar eventos:", error);
        res.status(500).json({ message: 'Erro interno do servidor ao processar o reenvio.' });
    }
});

// Admin route to get the VAPID public key for push notifications
app.get('/api/admin/vapidPublicKey', authenticateAdmin, (req, res) => {
    if (!process.env.VAPID_PUBLIC_KEY) {
        return res.status(500).send('VAPID Public Key não configurada no servidor.');
    }
    res.type('text/plain').send(process.env.VAPID_PUBLIC_KEY);
});

// Admin route to save the push notification subscription object
app.post('/api/admin/save-subscription', authenticateAdmin, (req, res) => {
    adminSubscription = req.body; // Store the subscription object in memory
    console.log("Inscrição de admin para notificações recebida e guardada.");
    res.status(201).json({});
});

// Admin route to get dashboard summary statistics
app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
    try {
        const totalSellers = await sql`SELECT COUNT(*) FROM sellers;`;
        const paidTransactions = await sql`SELECT COUNT(*) as count, SUM(pix_value) as total_revenue FROM pix_transactions WHERE status = 'paid';`;
        const total_sellers = parseInt(totalSellers[0].count);
        const total_paid_transactions = parseInt(paidTransactions[0].count);
        const total_revenue = parseFloat(paidTransactions[0].total_revenue || 0);
        const saas_profit = total_revenue * 0.0299; // Assuming a 2.99% fee
        res.json({
            total_sellers, total_paid_transactions,
            total_revenue: total_revenue.toFixed(2),
            saas_profit: saas_profit.toFixed(2)
        });
    } catch (error) {
        console.error("Erro no dashboard admin:", error);
        res.status(500).json({ message: 'Erro ao buscar dados do dashboard.' });
    }
});
// Admin route to get seller ranking by revenue
app.get('/api/admin/ranking', authenticateAdmin, async (req, res) => {
    try {
        const ranking = await sql`
            SELECT s.id, s.name, s.email, COUNT(pt.id) AS total_sales, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name, s.email ORDER BY total_revenue DESC LIMIT 20;`;
        res.json(ranking);
    } catch (error) {
        console.error("Erro no ranking de sellers:", error);
        res.status(500).json({ message: 'Erro ao buscar ranking.' });
    }
});
// Admin route to list all sellers
app.get('/api/admin/sellers', authenticateAdmin, async (req, res) => {
    try {
        const sellers = await sql`SELECT id, name, email, created_at, is_active FROM sellers ORDER BY created_at DESC;`;
        res.json(sellers);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao listar vendedores.' });
    }
});
// Admin route to activate/deactivate a seller
app.post('/api/admin/sellers/:id/toggle-active', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { isActive } = req.body;
    try {
        await sql`UPDATE sellers SET is_active = ${isActive} WHERE id = ${id};`;
        res.status(200).json({ message: `Usuário ${isActive ? 'ativado' : 'bloqueado'} com sucesso.` });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar status do usuário.' });
    }
});
// Admin route to change a seller's password
app.put('/api/admin/sellers/:id/password', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { newPassword } = req.body;
    if (!newPassword || newPassword.length < 8) return res.status(400).json({ message: 'A nova senha deve ter pelo menos 8 caracteres.' });
    try {
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        await sql`UPDATE sellers SET password_hash = ${hashedPassword} WHERE id = ${id};`;
        res.status(200).json({ message: 'Senha alterada com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar senha.' });
    }
});
// Admin route to update a seller's payment provider credentials
app.put('/api/admin/sellers/:id/credentials', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { pushinpay_token, cnpay_public_key, cnpay_secret_key } = req.body; // Add other providers if needed
    try {
        await sql`
            UPDATE sellers
            SET pushinpay_token = ${pushinpay_token}, cnpay_public_key = ${cnpay_public_key}, cnpay_secret_key = ${cnpay_secret_key}
            -- Add other provider fields here
            WHERE id = ${id};`;
        res.status(200).json({ message: 'Credenciais alteradas com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar credenciais.' });
    }
});
// Admin route to list recent transactions with pagination
app.get('/api/admin/transactions', authenticateAdmin, async (req, res) => {
    try {
        const page = parseInt(req.query.page || 1);
        const limit = parseInt(req.query.limit || 20);
        const offset = (page - 1) * limit;
        const transactions = await sql`
            SELECT pt.id, pt.status, pt.pix_value, pt.provider, pt.created_at, s.name as seller_name, s.email as seller_email
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            JOIN sellers s ON c.seller_id = s.id ORDER BY pt.created_at DESC
            LIMIT ${limit} OFFSET ${offset};`;
         const totalTransactionsResult = await sql`SELECT COUNT(*) FROM pix_transactions;`;
         const total = parseInt(totalTransactionsResult[0].count);
        res.json({ transactions, total, page, pages: Math.ceil(total / limit), limit });
    } catch (error) {
        console.error("Erro ao buscar transações admin:", error);
        res.status(500).json({ message: 'Erro ao buscar transações.' });
    }
});
// Admin route to analyze API usage per seller
app.get('/api/admin/usage-analysis', authenticateAdmin, async (req, res) => {
    try {
        const usageData = await sql`
            SELECT
                s.id, s.name, s.email,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '1 hour') AS requests_last_hour,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '24 hours') AS requests_last_24_hours
            FROM sellers s
            LEFT JOIN api_requests ar ON s.id = ar.seller_id
            GROUP BY s.id, s.name, s.email
            ORDER BY requests_last_24_hours DESC, requests_last_hour DESC;
        `;
        res.json(usageData);
    } catch (error) {
        console.error("Erro na análise de uso:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de uso.' });
    }
});
// Admin route to set a seller's commission rate
app.put('/api/admin/sellers/:id/commission', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { commission_rate } = req.body;

    // Validate commission rate (must be between 0 and 1)
    if (typeof commission_rate !== 'number' || commission_rate < 0 || commission_rate > 1) {
        return res.status(400).json({ message: 'A taxa de comissão deve ser um número entre 0 e 1 (ex: 0.0299 para 2.99%).' });
    }

    try {
        await sql`UPDATE sellers SET commission_rate = ${commission_rate} WHERE id = ${id};`;
        res.status(200).json({ message: 'Comissão do usuário atualizada com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar comissão:", error);
        res.status(500).json({ message: 'Erro ao atualizar a comissão.' });
    }
});

// --- ROTAS GERAIS DE USUÁRIO ---
// Seller registration
app.post('/api/sellers/register', async (req, res) => {
    const { name, email, password } = req.body;

    // Basic validation
    if (!name || !email || !password || password.length < 8) {
        return res.status(400).json({ message: 'Dados inválidos. Nome, email e senha (mínimo 8 caracteres) são obrigatórios.' });
    }

    try {
        const normalizedEmail = email.trim().toLowerCase();
        // Check if email already exists
        const existingSeller = await sql`SELECT id FROM sellers WHERE LOWER(email) = ${normalizedEmail}`;
        if (existingSeller.length > 0) {
            return res.status(409).json({ message: 'Este email já está em uso.' });
        }
        // Hash the password
        const hashedPassword = await bcrypt.hash(password, 10);
        // Generate a unique API key
        const apiKey = uuidv4();

        // Insert new seller (active by default)
        await sql`INSERT INTO sellers (name, email, password_hash, api_key, is_active) VALUES (${name}, ${normalizedEmail}, ${hashedPassword}, ${apiKey}, TRUE)`;

        res.status(201).json({ message: 'Vendedor cadastrado com sucesso!' });
    } catch (error) {
        console.error("Erro no registro:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Seller login
app.post('/api/sellers/login', async (req, res) => {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email e senha são obrigatórios.' });
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const sellerResult = await sql`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        if (sellerResult.length === 0) {
             console.warn(`[LOGIN FAILURE] Usuário não encontrado no banco de dados para o email: "${normalizedEmail}"`);
            return res.status(404).json({ message: 'Usuário não encontrado.' });
        }

        const seller = sellerResult[0];

        // Check if account is active
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }

        // Compare password hash
        const isPasswordCorrect = await bcrypt.compare(password, seller.password_hash);
        if (!isPasswordCorrect) return res.status(401).json({ message: 'Senha incorreta.' });

        // Generate JWT token
        const tokenPayload = { id: seller.id, email: seller.email };
        const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '1d' }); // Token expires in 1 day

        // Return token and seller data (excluding password hash)
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({ message: 'Login bem-sucedido!', token, seller: sellerData });

    } catch (error) {
        console.error("ERRO DETALHADO NO LOGIN:", error); // Log the full error for debugging
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Fetch initial data for the dashboard (pixels, bots, settings, etc.)
app.get('/api/dashboard/data', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        // Fetch various configurations in parallel
        const settingsPromise = sql`SELECT api_key, pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key, syncpay_client_id, syncpay_client_secret, brpix_secret_key, brpix_company_id, pix_provider_primary, pix_provider_secondary, pix_provider_tertiary, commission_rate FROM sellers WHERE id = ${sellerId}`;
        const pixelsPromise = sql`SELECT * FROM pixel_configurations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const presselsPromise = sql`
            SELECT p.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids, b.bot_name
            FROM pressels p
            LEFT JOIN ( SELECT pressel_id, array_agg(pixel_config_id) as pixel_ids FROM pressel_pixels GROUP BY pressel_id ) px ON p.id = px.pressel_id
            JOIN telegram_bots b ON p.bot_id = b.id
            WHERE p.seller_id = ${sellerId} ORDER BY p.created_at DESC`;
        const botsPromise = sql`SELECT * FROM telegram_bots WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const utmifyIntegrationsPromise = sql`SELECT id, account_name FROM utmify_integrations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;

        const [settingsResult, pixels, pressels, bots, utmifyIntegrations] = await Promise.all([
            settingsPromise, pixelsPromise, presselsPromise, botsPromise, utmifyIntegrationsPromise
        ]);

        const settings = settingsResult[0] || {};
        res.json({ settings, pixels, pressels, bots, utmifyIntegrations });
    } catch (error) {
        console.error("Erro ao buscar dados do dashboard:", error);
        res.status(500).json({ message: 'Erro ao buscar dados.' });
    }
});
// Fetch user achievements and sales ranking
app.get('/api/dashboard/achievements-and-ranking', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;

        // Fetch user's achievements status
        const userAchievements = await sql`
            SELECT a.title, a.description, ua.is_completed, a.sales_goal
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id
            WHERE ua.seller_id = ${sellerId}
            ORDER BY a.sales_goal ASC;
        `;

        // Fetch top 5 sellers by paid revenue
        const topSellersRanking = await sql`
            SELECT s.name, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        `;

        // Fetch current user's total paid revenue
        const [userRevenue] = await sql`
            SELECT COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            WHERE s.id = ${sellerId}
            GROUP BY s.id;
        `;

        // Calculate user's rank based on revenue
        const userRankResult = await sql`
            SELECT COUNT(T1.id) + 1 AS rank
            FROM (
                SELECT s.id
                FROM sellers s
                LEFT JOIN clicks c ON s.id = c.seller_id
                LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
                GROUP BY s.id
                HAVING COALESCE(SUM(pt.pix_value), 0) > ${userRevenue.total_revenue}
            ) AS T1;
        `;

        const userRank = userRankResult[0].rank;

        res.json({
            userAchievements,
            topSellersRanking,
            currentUserRank: userRank
        });
    } catch (error) {
        console.error("Erro ao buscar conquistas e ranking:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de ranking.' });
    }
});
// Add a new Meta Pixel configuration
app.post('/api/pixels', authenticateJwt, async (req, res) => {
    const { account_name, pixel_id, meta_api_token } = req.body;
    if (!account_name || !pixel_id || !meta_api_token) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    try {
        const newPixel = await sql`INSERT INTO pixel_configurations (seller_id, account_name, pixel_id, meta_api_token) VALUES (${req.user.id}, ${account_name}, ${pixel_id}, ${meta_api_token}) RETURNING *;`;
        res.status(201).json(newPixel[0]);
    } catch (error) {
        // Handle unique constraint violation for pixel_id
        if (error.code === '23505') { return res.status(409).json({ message: 'Este ID de Pixel já foi cadastrado.' }); }
        console.error("Erro ao salvar pixel:", error);
        res.status(500).json({ message: 'Erro ao salvar o pixel.' });
    }
});
// Delete a Meta Pixel configuration
app.delete('/api/pixels/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM pixel_configurations WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send(); // No content on successful deletion
    } catch (error) {
        console.error("Erro ao excluir pixel:", error);
        res.status(500).json({ message: 'Erro ao excluir o pixel.' });
    }
});

// Add a new Telegram Bot
app.post('/api/bots', authenticateJwt, async (req, res) => {
    const { bot_name } = req.body;
    if (!bot_name) {
        return res.status(400).json({ message: 'O nome do bot é obrigatório.' });
    }
    try {
        // Insert with a placeholder token initially
        const placeholderToken = uuidv4();

        const [newBot] = await sql`
            INSERT INTO telegram_bots (seller_id, bot_name, bot_token)
            VALUES (${req.user.id}, ${bot_name}, ${placeholderToken})
            RETURNING *;
        `;
        res.status(201).json(newBot);
    } catch (error) {
        // Handle unique constraint violation for bot_name
        if (error.code === '23505' && error.constraint_name === 'telegram_bots_bot_name_key') {
            return res.status(409).json({ message: 'Um bot com este nome de usuário já existe.' });
        }
        console.error("Erro ao salvar bot:", error);
        res.status(500).json({ message: 'Erro ao salvar o bot.' });
    }
});

// Delete a Telegram Bot
app.delete('/api/bots/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM telegram_bots WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir bot:", error);
        res.status(500).json({ message: 'Erro ao excluir o bot.' });
    }
});

// Update a Telegram Bot's token
app.put('/api/bots/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    let { bot_token } = req.body;
    if (!bot_token) {
        return res.status(400).json({ message: 'O token do bot é obrigatório.' });
    }
    bot_token = bot_token.trim(); // Remove leading/trailing whitespace
    try {
        await sql`
            UPDATE telegram_bots
            SET bot_token = ${bot_token}
            WHERE id = ${id} AND seller_id = ${req.user.id}`;
        res.status(200).json({ message: 'Token do bot atualizado com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar token do bot:", error);
        res.status(500).json({ message: 'Erro ao atualizar o token do bot.' });
    }
});

// Set the Telegram Bot's webhook URL
app.post('/api/bots/:id/set-webhook', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    try {
        // Fetch the bot token
        const [bot] = await sql`
            SELECT bot_token FROM telegram_bots
            WHERE id = ${id} AND seller_id = ${sellerId}`;

        // Validate if token exists
        if (!bot || !bot.bot_token || bot.bot_token.trim() === '') {
            return res.status(400).json({ message: 'O token do bot não está configurado. Salve um token válido primeiro.' });
        }

        const token = bot.bot_token.trim();
        // Construct the webhook URL dynamically based on the current environment/host
        const webhookUrl = `https://${req.headers.host}/api/webhook/telegram/${id}`; // Use req.headers.host
        const telegramApiUrl = `https://api.telegram.org/bot${token}/setWebhook?url=${webhookUrl}`;

        // Call Telegram API to set the webhook
        const response = await axios.get(telegramApiUrl);

        if (response.data.ok) {
            res.status(200).json({ message: 'Webhook configurado com sucesso!' });
        } else {
            // Throw error with Telegram's description if setup failed
            throw new Error(response.data.description);
        }
    } catch (error) {
        console.error("Erro ao configurar webhook:", error);
        // Handle specific Telegram API errors
        if (error.isAxiosError && error.response) {
            const status = error.response.status;
            const telegramMessage = error.response.data?.description || 'Resposta inválida do Telegram.';
            if (status === 401 || status === 404) { // Unauthorized or Not Found often mean invalid token
                return res.status(400).json({ message: `O Telegram rejeitou seu token: "${telegramMessage}". Verifique se o token está correto.` });
            }
            return res.status(500).json({ message: `Erro de comunicação com o Telegram: ${telegramMessage}` });
        }
        // Handle generic server errors
        res.status(500).json({ message: `Erro interno no servidor: ${error.message}` });
    }
});

// Test the connection to a Telegram Bot using its token
app.post('/api/bots/test-connection', authenticateJwt, async (req, res) => {
    const { bot_id } = req.body;
    if (!bot_id) return res.status(400).json({ message: 'ID do bot é obrigatório.' });

    try {
        const [bot] = await sql`SELECT bot_token, bot_name FROM telegram_bots WHERE id = ${bot_id} AND seller_id = ${req.user.id}`;
        if (!bot) {
            return res.status(404).json({ message: 'Bot não encontrado ou não pertence a este usuário.' });
        }
        if (!bot.bot_token) {
            return res.status(400).json({ message: 'Token do bot não configurado. Impossível testar.'})
        }

        // Call Telegram's getMe method to verify the token
        const response = await axios.get(`https://api.telegram.org/bot${bot.bot_token}/getMe`);

        if (response.data.ok) {
            res.status(200).json({
                message: `Conexão com o bot @${response.data.result.username} bem-sucedida!`,
                bot_info: response.data.result
            });
        } else {
            throw new Error('A API do Telegram retornou um erro.');
        }

    } catch (error) {
        console.error(`[BOT TEST ERROR] Bot ID: ${bot_id} - Erro:`, error.response?.data || error.message);
        let errorMessage = 'Falha ao conectar com o bot. Verifique o token e tente novamente.';
        if (error.response?.status === 401) {
            errorMessage = 'Token inválido. Verifique se o token do bot foi copiado corretamente do BotFather.';
        } else if (error.response?.status === 404) {
            errorMessage = 'Bot não encontrado. O token pode estar incorreto ou o bot foi deletado.';
        }
        res.status(500).json({ message: errorMessage });
    }
});

// Count unique users across selected bots
app.get('/api/bots/users', authenticateJwt, async (req, res) => {
    const { botIds } = req.query; // Expecting comma-separated IDs like "1,2,3"

    if (!botIds) {
        return res.status(400).json({ message: 'IDs dos bots são obrigatórios.' });
    }
    // Parse the string into an array of integers
    const botIdArray = botIds.split(',').map(id => parseInt(id.trim(), 10));

    try {
        // Select distinct chat_ids belonging to the specified bots and the current seller
        const users = await sql`
            SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username
            FROM telegram_chats
            WHERE bot_id = ANY(${botIdArray}) AND seller_id = ${req.user.id};
        `;
        res.status(200).json({ total_users: users.length });
    } catch (error) {
        console.error("Erro ao buscar contagem de usuários do bot:", error);
        res.status(500).json({ message: 'Erro interno ao buscar usuários.' });
    }
});
// Create a new Pressel page configuration
app.post('/api/pressels', authenticateJwt, async (req, res) => {
    const { name, bot_id, white_page_url, pixel_ids, utmify_integration_id } = req.body;
    // Validate required fields
    if (!name || !bot_id || !white_page_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });

    try {
        const numeric_bot_id = parseInt(bot_id, 10);
        const numeric_pixel_ids = pixel_ids.map(id => parseInt(id, 10));

        // Verify the selected bot belongs to the user
        const botResult = await sql`SELECT bot_name FROM telegram_bots WHERE id = ${numeric_bot_id} AND seller_id = ${req.user.id}`;
        if (botResult.length === 0) {
            return res.status(404).json({ message: 'Bot não encontrado.' });
        }
        const bot_name = botResult[0].bot_name;

        // Use a transaction to ensure both inserts succeed or fail together
        await sql`BEGIN`;
        try {
            // Insert the main pressel configuration
            const [newPressel] = await sql`
                INSERT INTO pressels (seller_id, name, bot_id, bot_name, white_page_url, utmify_integration_id)
                VALUES (${req.user.id}, ${name}, ${numeric_bot_id}, ${bot_name}, ${white_page_url}, ${utmify_integration_id || null})
                RETURNING *;
            `;

            // Insert associations into the pressel_pixels join table
            for (const pixelId of numeric_pixel_ids) {
                await sql`INSERT INTO pressel_pixels (pressel_id, pixel_config_id) VALUES (${newPressel.id}, ${pixelId})`;
            }
            await sql`COMMIT`; // Commit the transaction

            // Return the created pressel data along with associated pixel IDs
            res.status(201).json({ ...newPressel, pixel_ids: numeric_pixel_ids, bot_name });
        } catch (transactionError) {
            await sql`ROLLBACK`; // Rollback on error
            throw transactionError; // Re-throw the error
        }
    } catch (error) {
        console.error("Erro ao salvar pressel:", error);
        res.status(500).json({ message: 'Erro ao salvar a pressel.' });
    }
});
// Delete a Pressel page configuration
app.delete('/api/pressels/:id', authenticateJwt, async (req, res) => {
    try {
        // Deleting from 'pressels' will cascade delete related 'pressel_pixels' entries due to FK constraint
        await sql`DELETE FROM pressels WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pressel:", error);
        res.status(500).json({ message: 'Erro ao excluir a pressel.' });
    }
});

// Save PIX provider settings and priority order
app.post('/api/settings/pix', authenticateJwt, async (req, res) => {
    const {
        pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key,
        syncpay_client_id, syncpay_client_secret,
        brpix_secret_key, brpix_company_id,
        pix_provider_primary, pix_provider_secondary, pix_provider_tertiary
    } = req.body;
    try {
        await sql`UPDATE sellers SET
            pushinpay_token = ${pushinpay_token || null},
            cnpay_public_key = ${cnpay_public_key || null},
            cnpay_secret_key = ${cnpay_secret_key || null},
            oasyfy_public_key = ${oasyfy_public_key || null},
            oasyfy_secret_key = ${oasyfy_secret_key || null},
            syncpay_client_id = ${syncpay_client_id || null},
            syncpay_client_secret = ${syncpay_client_secret || null},
            brpix_secret_key = ${brpix_secret_key || null},
            brpix_company_id = ${brpix_company_id || null},
            pix_provider_primary = ${pix_provider_primary || 'pushinpay'}, -- Default to PushinPay if not set
            pix_provider_secondary = ${pix_provider_secondary || null},
            pix_provider_tertiary = ${pix_provider_tertiary || null}
            WHERE id = ${req.user.id}`;
        res.status(200).json({ message: 'Configurações de PIX salvas com sucesso.' });
    } catch (error) {
        console.error("Erro ao salvar configurações de PIX:", error);
        res.status(500).json({ message: 'Erro ao salvar as configurações.' });
    }
});
// List Utmify integrations for the user
app.get('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    try {
        const integrations = await sql`
            SELECT id, account_name, created_at
            FROM utmify_integrations
            WHERE seller_id = ${req.user.id}
            ORDER BY created_at DESC
        `;
        res.status(200).json(integrations);
    } catch (error) {
        console.error("Erro ao buscar integrações Utmify:", error);
        res.status(500).json({ message: 'Erro ao buscar integrações.' });
    }
});
// Add a new Utmify integration
app.post('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    const { account_name, api_token } = req.body;
    if (!account_name || !api_token) {
        return res.status(400).json({ message: 'Nome da conta e token da API são obrigatórios.' });
    }
    try {
        const [newIntegration] = await sql`
            INSERT INTO utmify_integrations (seller_id, account_name, api_token)
            VALUES (${req.user.id}, ${account_name}, ${api_token})
            RETURNING id, account_name, created_at
        `;
        res.status(201).json(newIntegration);
    } catch (error) {
        console.error("Erro ao adicionar integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao salvar integração.' });
    }
});
// Delete a Utmify integration
app.delete('/api/integrations/utmify/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        await sql`
            DELETE FROM utmify_integrations
            WHERE id = ${id} AND seller_id = ${req.user.id}
        `;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao excluir integração.' });
    }
});
// Register a click (from Pressel or Checkout)
app.post('/api/registerClick', logApiRequest, async (req, res) => {
    const { sellerApiKey, presselId, checkoutId, referer, fbclid, fbp, fbc, user_agent, utm_source, utm_campaign, utm_medium, utm_content, utm_term } = req.body;

    if (!sellerApiKey || (!presselId && !checkoutId)) {
        return res.status(400).json({ message: 'Dados insuficientes.' });
    }

    // Get IP address, handling potential proxies
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    try {
        // Insert the click record, finding the seller_id based on the API key
        const result = await sql`INSERT INTO clicks (
            seller_id, pressel_id, checkout_id, ip_address, user_agent, referer, fbclid, fbp, fbc,
            utm_source, utm_campaign, utm_medium, utm_content, utm_term
        )
        SELECT
            s.id, ${presselId || null}, ${checkoutId || null}, ${ip_address}, ${user_agent}, ${referer}, ${fbclid}, ${fbp}, ${fbc},
            ${utm_source || null}, ${utm_campaign || null}, ${utm_medium || null}, ${utm_content || null}, ${utm_term || null}
        FROM sellers s WHERE s.api_key = ${sellerApiKey} RETURNING *;`;

        if (result.length === 0) {
            return res.status(404).json({ message: 'API Key inválida.' });
        }

        // Generate a user-friendly click ID (e.g., lead000123)
        const newClick = result[0];
        const click_record_id = newClick.id;
        const clean_click_id = `lead${click_record_id.toString().padStart(6, '0')}`;
        // Store the format expected by Telegram (/start command)
        const db_click_id = `/start ${clean_click_id}`;

        // Update the click record with the generated click_id
        await sql`UPDATE clicks SET click_id = ${db_click_id} WHERE id = ${click_record_id}`;

        // Return the clean click ID to the frontend
        res.status(200).json({ status: 'success', click_id: clean_click_id });

        // --- Background tasks (GeoIP lookup and Meta event) ---
        (async () => {
            try {
                // GeoIP lookup using ip-api.com
                let city = 'Desconhecida', state = 'Desconhecido';
                // Avoid looking up local/private IPs
                if (ip_address && ip_address !== '::1' && !ip_address.startsWith('192.168.') && !ip_address.startsWith('10.') && !ip_address.startsWith('172.')) {
                    try {
                        const geo = await axios.get(`http://ip-api.com/json/${ip_address}?fields=status,city,regionName`);
                        if(geo.data.status === 'success'){
                            city = geo.data.city || city;
                            state = geo.data.regionName || state;
                        }
                    } catch (geoError) {
                        console.warn(`[GeoIP] Falha ao buscar geolocalização para IP ${ip_address}:`, geoError.message);
                    }
                }
                await sql`UPDATE clicks SET city = ${city}, state = ${state} WHERE id = ${click_record_id}`;
                console.log(`[BACKGROUND] Geolocalização atualizada para o clique ${click_record_id}.`);

                // If the click came from a checkout, send InitiateCheckout event
                if (checkoutId) {
                    await sendMetaEvent('InitiateCheckout', { ...newClick, checkout_id: checkoutId }, { id: click_record_id }); // Pass minimal data needed
                    console.log(`[BACKGROUND] Evento InitiateCheckout enviado para o clique ${click_record_id}.`);
                }
            } catch (backgroundError) {
                console.error("Erro em tarefa de segundo plano (registerClick):", backgroundError.message);
            }
        })();
        // --- End of background tasks ---

    } catch (error) {
        console.error("Erro ao registrar clique:", error);
        if (!res.headersSent) { // Avoid sending response if headers already sent
            res.status(500).json({ message: 'Erro interno do servidor.' });
        }
    }
});
// Get City/State info based on a click_id (used by Telegram bot potentially)
app.post('/api/click/info', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id } = req.body;

    if (!apiKey) return res.status(401).json({ message: 'API Key não fornecida.' });
    // If only API key is sent, just validate it
    if (!click_id) {
        const sellerResult = await sql`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        return sellerResult.length > 0 ? res.status(200).json({ message: 'API Key válida.' }) : res.status(401).json({ message: 'API Key inválida.' });
    }

    try {
        // Validate API key
        const sellerResult = await sql`SELECT id, email FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            return res.status(401).json({ message: 'API Key inválida.' });
        }

        const seller_id = sellerResult[0].id;
        // Ensure click_id has the /start prefix for DB lookup
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;

        // Find the click record
        const clickResult = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller_id}`;

        if (clickResult.length === 0) {
            return res.status(404).json({ message: 'Click ID não encontrado para este vendedor.' });
        }

        const clickInfo = clickResult[0];
        res.status(200).json({ status: 'success', city: clickInfo.city, state: clickInfo.state });

    } catch (error) {
        console.error("Erro ao consultar informações do clique:", error);
        res.status(500).json({ message: 'Erro interno ao consultar informações do clique.' });
    }
});
// Fetch dashboard metrics, optionally filtered by date
app.get('/api/dashboard/metrics', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        let { startDate, endDate } = req.query;
        // Check if valid start and end dates are provided
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        // Build queries conditionally based on date filter
        const totalClicksQuery = hasDateFilter
            ? sql`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId} AND created_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId}`;

        const pixGeneratedQuery = hasDateFilter
            ? sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.created_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId}`;

        const pixPaidQuery = hasDateFilter
            ? sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid'`;

        const botsPerformanceQuery = hasDateFilter
            ? sql`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON c.pressel_id = p.id AND c.seller_id = ${sellerId} AND c.created_at BETWEEN ${startDate} AND ${endDate} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`
            : sql`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON c.pressel_id = p.id AND c.seller_id = ${sellerId} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`;

        const clicksByStateQuery = hasDateFilter
             ? sql`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' AND c.created_at BETWEEN ${startDate} AND ${endDate} GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`
             : sql`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`;

        const userTimezone = 'America/Sao_Paulo'; // Define the target timezone for daily aggregation
        const dailyRevenueQuery = hasDateFilter
             ? sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate} GROUP BY 1 ORDER BY 1 ASC`
             : sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' GROUP BY 1 ORDER BY 1 ASC`;

        // Execute all queries in parallel
        const [
               totalClicksResult, pixGeneratedResult, pixPaidResult, botsPerformance,
               clicksByState, dailyRevenue
        ] = await Promise.all([
              totalClicksQuery, pixGeneratedQuery, pixPaidQuery, botsPerformanceQuery,
              clicksByStateQuery, dailyRevenueQuery
        ]);

        // Extract results
        const totalClicks = totalClicksResult[0].count;
        const totalPixGenerated = pixGeneratedResult[0].total;
        const totalRevenue = pixGeneratedResult[0].revenue;
        const totalPixPaid = pixPaidResult[0].total;
        const paidRevenue = pixPaidResult[0].revenue;

        // Format and return the combined metrics
        res.status(200).json({
            total_clicks: parseInt(totalClicks),
            total_pix_generated: parseInt(totalPixGenerated),
            total_pix_paid: parseInt(totalPixPaid),
            total_revenue: parseFloat(totalRevenue), // Revenue comes as string/numeric from DB, ensure float
            paid_revenue: parseFloat(paidRevenue),
            bots_performance: botsPerformance.map(b => ({ ...b, total_clicks: parseInt(b.total_clicks), total_pix_paid: parseInt(b.total_pix_paid), paid_revenue: parseFloat(b.paid_revenue) })),
            clicks_by_state: clicksByState.map(s => ({ ...s, total_clicks: parseInt(s.total_clicks) })),
            daily_revenue: dailyRevenue.map(d => ({ date: d.date.toISOString().split('T')[0], revenue: parseFloat(d.revenue) })) // Format date as YYYY-MM-DD
        });
    } catch (error) {
        console.error("Erro ao buscar métricas do dashboard:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// ########## ROTA DE TRANSAÇÕES CORRIGIDA ##########
app.get('/api/transactions', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const { startDate, endDate } = req.query; // Pega as datas da query string
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        // 1. Iniciar a string da query e o array de parâmetros
        let queryString = `
            SELECT
                pt.status,
                pt.pix_value,
                COALESCE(tb.bot_name, hc.config->'content'->>'main_title', 'Checkout Desconhecido') as source_name,
                pt.provider,
                pt.created_at
            FROM pix_transactions pt
            JOIN clicks c ON pt.click_id_internal = c.id
            LEFT JOIN pressels p ON c.pressel_id = p.id
            LEFT JOIN telegram_bots tb ON p.bot_id = tb.id
            LEFT JOIN hosted_checkouts hc ON c.checkout_id = hc.id
            WHERE c.seller_id = $1
        `;
        const queryParams = [sellerId]; // $1 é sellerId

        if (hasDateFilter) {
            // 2. Adicionar o filtro de data à string e os parâmetros ao array
            queryString += ` AND pt.created_at BETWEEN $2 AND $3`;
            queryParams.push(startDate); // $2 é startDate
            queryParams.push(endDate);   // $3 é endDate
        }

        // 3. Adicionar a ordenação
        queryString += ` ORDER BY pt.created_at DESC;`;

        // 4. Executar a consulta usando a sintaxe de função sql(query, params)
        //    Isto é diferente do "sql`...`" (template tag) e é o que resolve o erro.
        const transactions = await sql(queryString, queryParams);

        res.status(200).json(transactions);
    } catch (error) {
        console.error("Erro ao buscar transações:", error); // Loga o erro completo no console
        res.status(500).json({ message: 'Erro ao buscar dados das transações.' });
    }
});
// ########## FIM DA CORREÇÃO ##########


// Generate PIX via API key (typically used by Telegram bot or other integrations)
app.post('/api/pix/generate', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id, value_cents, customer, product } = req.body;

    if (!apiKey || !click_id || !value_cents) return res.status(400).json({ message: 'API Key, click_id e value_cents são obrigatórios.' });

    try {
        // Validate API key and get seller data
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) return res.status(401).json({ message: 'API Key inválida.' });

        // Send admin notification if subscribed
        if (adminSubscription) {
            const payload = JSON.stringify({
                title: 'PIX Gerado',
                body: `Um PIX de R$ ${(value_cents / 100).toFixed(2)} foi gerado por ${seller.name}.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(err => console.error(err));
        }

        // Ensure click_id format matches DB
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;

        // Find the corresponding click record
        const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller.id}`;
        if (!click) return res.status(404).json({ message: 'Click ID não encontrado.' });

        const ip_address = click.ip_address; // Use IP from the original click

        // Determine provider order based on seller settings
        const providerOrder = [ seller.pix_provider_primary, seller.pix_provider_secondary, seller.pix_provider_tertiary ].filter(Boolean); // Filter out null/empty providers
        let lastError = null;

        // Try generating PIX with providers in order of priority
        for (const provider of providerOrder) {
            try {
                // Call the internal function to generate PIX
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, apiKey, ip_address);
                // Save the transaction record
                const [transaction] = await sql`INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, qr_code_base64, provider, provider_transaction_id, pix_id) VALUES (${click.id}, ${value_cents / 100}, ${pixResult.qr_code_text}, ${pixResult.qr_code_base64}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id}) RETURNING id`;

                // Send InitiateCheckout event to Meta if applicable
                if (click.pressel_id || click.checkout_id) {
                    await sendMetaEvent('InitiateCheckout', click, { id: transaction.id, pix_value: value_cents / 100 }, null);
                }

                // Send waiting_payment event to Utmify
                const customerDataForUtmify = customer || { name: "Cliente Interessado", email: "cliente@email.com" };
                const productDataForUtmify = product || { id: "prod_1", name: "Produto Ofertado" };
                await sendEventToUtmify('waiting_payment', click, { provider_transaction_id: pixResult.transaction_id, pix_value: value_cents / 100, created_at: new Date() }, seller, customerDataForUtmify, productDataForUtmify);

                // Return successful PIX data
                return res.status(200).json(pixResult);
            } catch (error) {
                // Log the error and try the next provider
                console.error(`[PIX GENERATE FALLBACK] Falha ao gerar PIX com ${provider}:`, error.response?.data || error.message);
                lastError = error;
            }
        }

        // If all providers failed
        console.error(`[PIX GENERATE FINAL ERROR] Seller ID: ${seller?.id}, Email: ${seller?.email} - Todas as tentativas falharam. Último erro:`, lastError?.message || lastError);
        return res.status(500).json({ message: 'Não foi possível gerar o PIX. Todos os provedores falharam.' });

    } catch (error) {
        console.error(`[PIX GENERATE ERROR] Erro geral na rota:`, error.message);
        res.status(500).json({ message: 'Erro interno ao processar a geração de PIX.' });
    }
});

// Check PIX status via API key (MODIFICADO PARA RETORNAR REDIRECT URL)
app.get('/api/pix/status/:transaction_id', async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { transaction_id } = req.params; // Can be provider's ID or our internal pix_id

    if (!apiKey) return res.status(401).json({ message: 'API Key não fornecida.' });
    if (!transaction_id) return res.status(400).json({ message: 'ID da transação é obrigatório.' });

    try {
        // Validate API Key and get seller
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) {
            return res.status(401).json({ message: 'API Key inválida.' });
        }

        // Find the transaction using either provider_transaction_id or pix_id
        const [transaction] = await sql`
            SELECT pt.*, c.checkout_id, c.click_id -- Inclui checkout_id e click_id do clique original
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            WHERE (pt.provider_transaction_id = ${transaction_id} OR pt.pix_id = ${transaction_id}) AND c.seller_id = ${seller.id}`;

        if (!transaction) {
            return res.status(404).json({ status: 'not_found', message: 'Transação não encontrada.' });
        }

        let currentStatus = transaction.status;
        let customerData = {}; // Para armazenar dados do cliente se consultarmos o provedor

        // Se o status no DB ainda é 'pending', verifica com o provedor (se aplicável)
        if (currentStatus === 'pending' && transaction.provider !== 'oasyfy' && transaction.provider !== 'cnpay' && transaction.provider !== 'brpix') {
            let providerStatus;
            try {
                if (transaction.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                        headers: { 'Authorization': `Bearer ${syncPayToken}` }
                    });
                    providerStatus = response.data.status;
                    customerData = response.data.payer; // Pega dados do cliente da SyncPay
                } else if (transaction.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    customerData = { name: response.data.payer_name, document: response.data.payer_document }; // Pega dados do cliente da PushinPay
                }

                // Se o provedor confirmar o pagamento, atualiza nosso status
                if (providerStatus === 'paid' || providerStatus === 'COMPLETED') {
                    await handleSuccessfulPayment(transaction.id, customerData); // Processa pagamento (envia eventos, etc.)
                    currentStatus = 'paid'; // Atualiza o status localmente para a resposta
                }
            } catch (providerError) {
                // Loga erro mas continua, retornando 'pending' se não puder confirmar
                if (!providerError.response || providerError.response.status !== 404) { // Ignora erros 404 (transação talvez não exista ainda no provedor)
                    console.error(`Falha ao consultar o provedor (${transaction.provider}) para a transação ${transaction.id}:`, providerError.message);
                }
            }
        }

        // Se o status final (do DB ou após consulta) for 'paid'
        if (currentStatus === 'paid') {
            let redirectUrl = null;
            // Busca a URL de redirecionamento do checkout associado
            if (transaction.checkout_id && transaction.checkout_id.startsWith('cko_')) {
                const [checkoutConfig] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${transaction.checkout_id}`;
                // *** AJUSTE AQUI o caminho se necessário ***
                redirectUrl = checkoutConfig?.config?.redirects?.success_url || null; // Exemplo de caminho
                 if (!redirectUrl) {
                     console.warn(`[PIX Status] Checkout ${transaction.checkout_id} não possui 'config.redirects.success_url' definida.`);
                 }
            }
            // Retorna status 'paid' e a URL encontrada (ou null) e o click_id original
            return res.status(200).json({
                status: 'paid',
                redirectUrl: redirectUrl,
                click_id: transaction.click_id // Retorna o click_id original
             });
        }

        // Se não for 'paid', retorna o status atual ('pending' ou outro)
        res.status(200).json({ status: currentStatus });

    } catch (error) {
        console.error("Erro ao consultar status da transação:", error);
        res.status(500).json({ message: 'Erro interno ao consultar o status.' });
    }
});

// Test connection and generate a sample PIX for a specific provider
app.post('/api/pix/test-provider', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { provider } = req.body;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    if (!provider) {
        return res.status(400).json({ message: 'O nome do provedor é obrigatório.' });
    }

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });

        const value_cents = 3333; // Use a fixed, non-standard value for testing (R$ 33,33)

        const startTime = Date.now();
        const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
        const endTime = Date.now();
        const responseTime = ((endTime - startTime) / 1000).toFixed(2); // Calculate response time

        res.status(200).json({
            provider: provider.toUpperCase(),
            acquirer: pixResult.acquirer,
            responseTime: responseTime,
            qr_code_text: pixResult.qr_code_text // Return the PIX code for manual testing
        });

    } catch (error) {
        // Log detailed error and return user-friendly message
        console.error(`[PIX TEST ERROR] Seller ID: ${sellerId}, Provider: ${provider} - Erro:`, error.response?.data || error.message);
        res.status(500).json({
            message: `Falha ao gerar PIX de teste com ${provider.toUpperCase()}. Verifique as credenciais.`,
            details: error.response?.data ? JSON.stringify(error.response.data) : error.message
        });
    }
});
// Test the configured PIX provider priority route
app.post('/api/pix/test-priority-route', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    let testLog = []; // Log attempts

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });

        // Get the configured provider order
        const providerOrder = [
            { name: seller.pix_provider_primary, position: 'Primário' },
            { name: seller.pix_provider_secondary, position: 'Secundário' },
            { name: seller.pix_provider_tertiary, position: 'Terciário' }
        ].filter(p => p.name); // Filter out unconfigured slots

        if (providerOrder.length === 0) {
            return res.status(400).json({ message: 'Nenhuma ordem de prioridade de provedores foi configurada.' });
        }

        const value_cents = 3333; // Test value R$ 33,33

        // Iterate through providers in order
        for (const providerInfo of providerOrder) {
            const provider = providerInfo.name;
            const position = providerInfo.position;

            try {
                const startTime = Date.now();
                // Attempt to generate PIX with the current provider
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
                const endTime = Date.now();
                const responseTime = ((endTime - startTime) / 1000).toFixed(2);

                // If successful, log and return immediately
                testLog.push(`SUCESSO com Provedor ${position} (${provider.toUpperCase()}).`);
                return res.status(200).json({
                    success: true, position: position, provider: provider.toUpperCase(),
                    acquirer: pixResult.acquirer, responseTime: responseTime,
                    qr_code_text: pixResult.qr_code_text, log: testLog
                });

            } catch (error) {
                // Log the failure and continue to the next provider
                let errorMessage = error.message;
                if (error.response && error.response.data) { // Get more details if available
                    errorMessage = JSON.stringify(error.response.data);
                }
                console.error(`Falha no provedor ${position} (${provider}):`, error.response?.data || error.message);
                testLog.push(`FALHA com Provedor ${position} (${provider.toUpperCase()}): ${errorMessage}`);
            }
        }

        // If loop completes, all providers failed
        console.error("Todos os provedores na rota de prioridade falharam.");
        return res.status(500).json({
            success: false, message: 'Todos os provedores configurados na sua rota de prioridade falharam.',
            log: testLog // Return the log of attempts
        });

    } catch (error) {
        console.error(`[PIX PRIORITY TEST ERROR] Erro geral:`, error.message);
        res.status(500).json({
            success: false, message: 'Ocorreu um erro inesperado ao testar a rota de prioridade.',
            log: testLog
        });
    }
});

// ==========================================================
//          MOTOR DE FLUXO E WEBHOOK DO TELEGRAM
// ==========================================================
// Finds the next node ID based on the source node and handle
function findNextNode(currentNodeId, handleId, edges) {
    // Find an edge originating from the currentNodeId
    // Match the sourceHandle if handleId is provided, otherwise accept edges without a sourceHandle or if handleId is null
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null; // Return the target node ID or null if no edge found
}

// Sends the 'typing' action to Telegram chat
async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, {
            chat_id: chatId,
            action: 'typing',
        });
    } catch (error) {
        // Log warning but don't stop the flow
        console.warn(`[Flow Engine] Falha ao enviar ação 'typing' para ${chatId}:`, error.response?.data || error.message);
    }
}

// Sends a text message via Telegram and logs it
async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping) {
    if (!text || text.trim() === '') return; // Don't send empty messages
    const apiUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    try {
        // Optionally send 'typing' action and wait briefly
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            // Calculate a plausible typing duration based on message length
            let typingDuration = text.length * 50; // 50ms per character
            typingDuration = Math.max(500, typingDuration); // Minimum 500ms
            typingDuration = Math.min(2000, typingDuration); // Maximum 2000ms
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }

        // Send the message using Telegram API
        const response = await axios.post(apiUrl, {
            chat_id: chatId,
            text: text,
            parse_mode: 'HTML' // Allow basic HTML formatting like <b>, <i>, <a>
        });

        // If message sent successfully, log it in our database
        if (response.data.ok) {
            const sentMessage = response.data.result;
            // Get bot name for logging clarity
            const [botInfo] = await sql`SELECT bot_name FROM telegram_bots WHERE id = ${botId}`;
            const botName = botInfo ? botInfo.bot_name : 'Bot';

            // Insert into telegram_chats, ignoring duplicates (based on chat_id, message_id)
            await sql`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, message_text, sender_type)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, ${botName}, '(Fluxo)', ${text}, 'bot')
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        console.error(`[Flow Engine] Erro ao enviar/salvar mensagem para ${chatId}:`, error.response?.data || error.message);
    }
}

// Main function to process the conversation flow for a user
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    console.log(`[Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);
    // Find the latest active flow for this bot
    const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    if (!flow || !flow.nodes) {
        console.log(`[Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
        return; // No flow to execute
    }

    // Parse flow data (nodes and edges) from JSON
    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

    let currentNodeId = startNodeId; // Use provided start node if available
    let variables = initialVariables; // Use provided variables if available

    // Determine the starting point if not explicitly given
    if (!currentNodeId) {
        // Check if the user was waiting for input (replying to a message node)
        const [userState] = await sql`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        if (userState && userState.waiting_for_input) {
            console.log(`[Flow Engine] Usuário ${chatId} respondeu. Continuando do nó ${userState.current_node_id} pelo caminho 'com resposta'.`);
            // Find the node connected to the 'a' handle (response received)
            currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
            variables = userState.variables; // Restore variables from the previous state
        } else {
            // Start a new flow from the 'trigger' node
            console.log(`[Flow Engine] Iniciando novo fluxo para ${chatId} a partir do gatilho.`);
            const startNode = nodes.find(node => node.type === 'trigger');
            if (startNode) {
                // Find the node connected to the trigger node's output handle
                currentNodeId = findNextNode(startNode.id, null, edges); // Trigger usually has null or 'a' handle
            }
        }
    }

    // If no valid starting node, end the flow
    if (!currentNodeId) {
        console.log(`[Flow Engine] Fim do fluxo ou nenhum nó inicial encontrado para ${chatId}.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`; // Clean up state
        return;
    }

    let safetyLock = 0; // Prevent infinite loops
    // Loop through the flow nodes
    while (currentNodeId && safetyLock < 20) { // Limit to 20 steps to prevent runaway loops
        const currentNode = nodes.find(node => node.id === currentNodeId);
        if (!currentNode) {
            console.error(`[Flow Engine] Erro: Nó ${currentNodeId} não encontrado no fluxo.`);
            break; // Stop if node definition is missing
        }

        // Update the user's current state in the database
        await sql`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET current_node_id = EXCLUDED.current_node_id, variables = EXCLUDED.variables, waiting_for_input = false;
        `;

        // Process the current node based on its type
        switch (currentNode.type) {
            case 'message':
                // Optional typing delay before sending message
                if (currentNode.data.addTypingAction && currentNode.data.typingDuration > 0) {
                     await sendTypingAction(chatId, botToken);
                     await new Promise(resolve => setTimeout(resolve, currentNode.data.typingDuration * 1000));
                }
                // Send the message text
                await sendMessage(chatId, currentNode.data.text, botToken, sellerId, botId, false); // showTyping handled above

                // If waiting for reply, set state and schedule timeout
                if (currentNode.data.waitForReply) {
                    await sql`UPDATE user_flow_states SET waiting_for_input = true WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const timeoutMinutes = currentNode.data.replyTimeout || 5; // Default 5 mins timeout
                    // Find the node connected to the 'b' handle (no response timeout)
                    const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);

                    if(noReplyNodeId){
                        console.log(`[Flow Engine] Agendando timeout de ${timeoutMinutes} min para o nó ${noReplyNodeId}`);
                        // Schedule the timeout action in the database
                        await sql`
                            INSERT INTO flow_timeouts (chat_id, bot_id, execute_at, target_node_id, variables)
                            VALUES (${chatId}, ${botId}, NOW() + INTERVAL '${timeoutMinutes} minutes', ${noReplyNodeId}, ${JSON.stringify(variables)})
                        `;
                    }
                    currentNodeId = null; // Stop the loop, wait for user input or timeout
                } else {
                    // Continue to the next node connected to handle 'a' (or default)
                    currentNodeId = findNextNode(currentNodeId, 'a', edges);
                }
                break;

            case 'delay':
                const delaySeconds = currentNode.data.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                currentNodeId = findNextNode(currentNodeId, null, edges); // Continue to the next node
                break;

            case 'action_pix':
                try {
                    const valueInCents = currentNode.data.valueInCents;
                    if (!valueInCents) throw new Error("Valor do PIX não definido no nó do fluxo.");

                    // Fetch required data (seller, click)
                    const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    const [userFlowState] = await sql`SELECT variables FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const click_id = userFlowState?.variables?.click_id; // Get click_id from stored variables
                    if (!click_id) throw new Error("Click ID não encontrado nas variáveis do fluxo.");

                    const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error("Dados do clique não encontrados para gerar o PIX.");

                    // Determine provider and generate PIX
                    const provider = seller.pix_provider_primary || 'pushinpay'; // Use seller's primary provider
                    const ip_address = click.ip_address;
                    const pixResult = await generatePixForProvider(provider, seller, valueInCents, 'novaapi-one.vercel.app', seller.api_key, ip_address); // Use actual host if possible

                    // Save the PIX transaction record
                    await sql`INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, provider, provider_transaction_id, pix_id) VALUES (${click.id}, ${valueInCents / 100}, ${pixResult.qr_code_text}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id})`;

                    // Store the transaction ID in flow variables for later checking
                    variables.last_transaction_id = pixResult.transaction_id;
                    await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;

                    // Send the PIX code to the user
                    await sendMessage(chatId, `Pix copia e cola gerado:\n\n\`${pixResult.qr_code_text}\``, botToken, sellerId, botId, true);
                } catch (error) {
                    console.error("[Flow Engine] Erro ao gerar PIX:", error);
                    await sendMessage(chatId, "Desculpe, não consegui gerar o PIX neste momento. Tente novamente mais tarde.", botToken, sellerId, botId, true);
                }
                // Continue to the next node regardless of PIX generation success/failure
                currentNodeId = findNextNode(currentNodeId, null, edges);
                break;

            case 'action_check_pix':
                try {
                    // Retrieve the last transaction ID stored in variables
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX encontrado para consultar.");

                    // Check transaction status in our database
                    const [transaction] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;

                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        // If paid, proceed via 'a' handle (Paid)
                        await sendMessage(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'a', edges);
                    } else {
                        // If not paid, proceed via 'b' handle (Pending)
                         await sendMessage(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'b', edges);
                    }
                } catch (error) {
                     console.error("[Flow Engine] Erro ao consultar PIX:", error);
                     await sendMessage(chatId, "Não consegui consultar o status do PIX agora.", botToken, sellerId, botId, true);
                     // Proceed via 'b' handle (Pending) on error
                     currentNodeId = findNextNode(currentNodeId, 'b', edges);
                }
                break;

            // Handle other node types or unknown types
            default:
                console.warn(`[Flow Engine] Tipo de nó desconhecido ou não implementado: ${currentNode.type}. Parando fluxo.`);
                currentNodeId = null; // Stop the flow
                break;
        }

        // If flow execution stopped (e.g., waiting for input, error, end of flow)
        if (!currentNodeId) {
            // Check if there are any pending timeouts for this user/bot
            const pendingTimeouts = await sql`SELECT 1 FROM flow_timeouts WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            // If no timeouts are scheduled, clean up the user's flow state
            if(pendingTimeouts.length === 0){
                 await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            }
        }
        safetyLock++; // Increment safety lock counter
    }

    if (safetyLock >= 20) {
        console.error(`[Flow Engine] Safety lock triggered for chat ${chatId}. Flow terminated.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`; // Clean up state on safety lock
    }
}

// Telegram Webhook Handler
app.post('/api/webhook/telegram/:botId', async (req, res) => {
    const { botId } = req.params;
    const body = req.body;
    res.sendStatus(200); // Respond immediately to Telegram

    try {
        const message = body.message;
        const chatId = message?.chat?.id;
        // Ignore updates without a message or chat ID or text
        if (!chatId || !message || !message.text) return;

        // --- Important: Cancel any pending timeouts for this user ---
        await sql`DELETE FROM flow_timeouts WHERE chat_id = ${chatId} AND bot_id = ${botId}`;

        // Find the bot and seller associated with this webhook
        const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${botId}`;
        if (!bot) {
            console.warn(`[Webhook] Webhook recebido para botId não encontrado: ${botId}`);
            return; // Ignore if bot doesn't exist
        }

        const { seller_id: sellerId, bot_token: botToken } = bot;

        const text = message.text;
        const isStartCommand = text.startsWith('/start ');
        // Extract click_id only if it's a start command
        const clickIdValue = isStartCommand ? text : null;

        // Log the incoming user message
        await sql`
            INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, click_id, message_text, sender_type)
            VALUES (${sellerId}, ${botId}, ${chatId}, ${message.message_id}, ${message.from.id}, ${message.from.first_name}, ${message.from.last_name || null}, ${message.from.username || null}, ${clickIdValue}, ${text}, 'user')
            ON CONFLICT (chat_id, message_id) DO NOTHING; -- Avoid duplicates
        `;

        // Prepare initial variables for the flow if it's a start command
        let initialVars = {};
        if (isStartCommand) {
            initialVars.click_id = clickIdValue;
            // Potentially add other initial variables here based on the start payload if needed
        }

        // Start or continue the flow processing
        await processFlow(chatId, botId, botToken, sellerId, null, initialVars); // Let processFlow determine the start node

    } catch (error) {
        console.error("Erro CRÍTICO ao processar webhook do Telegram:", error);
        // Avoid sending response here as it might have already been sent
    }
});
// ==========================================================
//                   FIM DO MOTOR DE FLUXO
// ==========================================================


// ROTA DE DISPAROS (/api/dispatches, /api/dispatches/:id, /api/bots/mass-send)
app.get('/api/dispatches', authenticateJwt, async (req, res) => {
    try {
        const dispatches = await sql`SELECT * FROM mass_sends WHERE seller_id = ${req.user.id} ORDER BY created_at DESC;`; // Changed sent_at to created_at if that's the column name
        res.status(200).json(dispatches);
    } catch (error) {
        console.error("Erro ao buscar histórico de disparos:", error);
        res.status(500).json({ message: 'Erro ao buscar histórico.' });
    }
});
app.get('/api/dispatches/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        // Fetch details joining with telegram_chats to get user info
        const details = await sql`
            SELECT d.*, u.first_name, u.username
            FROM mass_send_details d
            -- Use LEFT JOIN in case chat info is missing for a chat_id
            LEFT JOIN telegram_chats u ON d.chat_id = u.chat_id AND d.mass_send_id = ${id} -- May need to adjust join condition if chat_id isn't unique across bots/sellers
            WHERE d.mass_send_id = ${id}
            ORDER BY d.created_at; -- Assuming created_at exists in mass_send_details
        `;
        res.status(200).json(details);
    } catch (error) {
        console.error("Erro ao buscar detalhes do disparo:", error);
        res.status(500).json({ message: 'Erro ao buscar detalhes.' });
    }
});
app.post('/api/bots/mass-send', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    // Adapt payload based on frontend sending flowSteps object
    const { campaignName, botIds, flowSteps } = req.body;

    if (!campaignName || !botIds || botIds.length === 0 || !Array.isArray(flowSteps) || flowSteps.length === 0) {
        return res.status(400).json({ message: 'Nome da campanha, IDs dos bots e pelo menos um passo no fluxo são obrigatórios.' });
    }

    try {
        // Validate selected bots belong to the user
        const bots = await sql`SELECT id, bot_token FROM telegram_bots WHERE id = ANY(${botIds}) AND seller_id = ${sellerId}`;
        if (bots.length === 0) return res.status(404).json({ message: 'Nenhum bot válido selecionado.' });

        // Get unique users for the selected bots
        const users = await sql`SELECT DISTINCT ON (chat_id) chat_id, bot_id FROM telegram_chats WHERE bot_id = ANY(${botIds}) AND seller_id = ${sellerId}`;
        if (users.length === 0) return res.status(404).json({ message: 'Nenhum usuário encontrado para os bots selecionados.' });

        // Log the main dispatch job as PENDING
        const [log] = await sql`INSERT INTO mass_sends (seller_id, campaign_name, status, total_users) VALUES (${sellerId}, ${campaignName}, 'PENDING', ${users.length}) RETURNING id;`;
        const logId = log.id;

        // Respond immediately that the job is scheduled
        res.status(202).json({ message: `Disparo "${campaignName}" agendado para ${users.length} usuários. Acompanhe o progresso no histórico.`, logId });

        // --- Start background processing (no await here) ---
        (async () => {
            await sql`UPDATE mass_sends SET status = 'RUNNING' WHERE id = ${logId}`;
            let successCount = 0, failureCount = 0;
            const botTokenMap = new Map(bots.map(b => [b.id, b.bot_token]));

            // Iterate through each unique user
            for (const user of users) {
                const botToken = botTokenMap.get(user.bot_id);
                if (!botToken) { failureCount++; continue; } // Skip if bot token not found

                let stepSuccess = true;
                // Iterate through each step defined in the flow
                for (const step of flowSteps) {
                    try {
                        let response;
                        let payload = { chat_id: user.chat_id, parse_mode: 'HTML' };
                        let endpoint = 'sendMessage';

                        // Build payload based on step type
                        switch (step.type) {
                            case 'message':
                                payload.text = step.text; // Consider adding variable replacement here: replacePlaceholders(step.text, user);
                                if (step.buttonText && step.buttonUrl) {
                                    payload.reply_markup = { inline_keyboard: [[{ text: step.buttonText, url: step.buttonUrl }]] };
                                }
                                endpoint = 'sendMessage';
                                break;
                            case 'image':
                                payload.photo = step.fileUrl; // Expects a Telegram file_id or public URL
                                if (step.caption) payload.caption = step.caption; // Add variable replacement: replacePlaceholders(step.caption, user);
                                endpoint = 'sendPhoto';
                                break;
                            case 'video':
                                payload.video = step.fileUrl;
                                if (step.caption) payload.caption = step.caption; // Add variable replacement: replacePlaceholders(step.caption, user);
                                endpoint = 'sendVideo';
                                break;
                            case 'audio':
                                payload.audio = step.fileUrl;
                                if (step.caption) payload.caption = step.caption; // Add variable replacement: replacePlaceholders(step.caption, user);
                                endpoint = 'sendAudio';
                                break;
                            // PIX steps are complex for mass send, skipping for now
                            case 'pix':
                            case 'check_pix':
                                console.warn(`[Mass Send ${logId}] PIX steps not supported in direct mass send. Skipping for chat ${user.chat_id}.`);
                                continue; // Skip PIX steps
                            default:
                                console.warn(`[Mass Send ${logId}] Unknown step type: ${step.type}`);
                                continue; // Skip unknown steps
                        }

                        // Send the message/media via Telegram API
                        const apiUrl = `https://api.telegram.org/bot${botToken}/${endpoint}`;
                        await axios.post(apiUrl, payload, { timeout: 15000 }); // Increased timeout for potentially large media
                        // Small delay to respect Telegram rate limits
                        await new Promise(resolve => setTimeout(resolve, 350)); // ~3 messages per second

                    } catch (stepError) {
                        stepSuccess = false; // Mark failure for this user
                        const errorMessage = stepError.response?.data?.description || stepError.message;
                        console.error(`[Mass Send ${logId}] Falha no passo ${step.type} para ${user.chat_id}: ${errorMessage}`);
                        // Log detailed failure
                        await sql`INSERT INTO mass_send_details (mass_send_id, chat_id, status, details) VALUES (${logId}, ${user.chat_id}, 'failure', ${`Passo ${step.type}: ${errorMessage}`})`;
                        break; // Stop sending subsequent steps to this user on failure
                    }
                } // End loop through steps for one user

                // Log success or update failure count
                if (stepSuccess) {
                    successCount++;
                    await sql`INSERT INTO mass_send_details (mass_send_id, chat_id, status) VALUES (${logId}, ${user.chat_id}, 'success')`;
                } else {
                    failureCount++;
                }
                // Small delay between users
                await new Promise(resolve => setTimeout(resolve, 50)); // Adjust as needed, e.g., based on Telegram limits (~30/sec)
            } // End loop through users

            // Update the main dispatch log with final counts and status
            await sql`UPDATE mass_sends SET success_count = ${successCount}, failure_count = ${failureCount}, status = 'COMPLETED', finished_at = NOW() WHERE id = ${logId};`;
            console.log(`Disparo ${logId} concluído. Sucessos: ${successCount}, Falhas: ${failureCount}`);
        })(); // --- End background processing ---

    } catch (error) {
        console.error("Erro no disparo em massa:", error);
        if (!res.headersSent) res.status(500).json({ message: 'Erro ao iniciar o disparo.' });
    }
});
// WEBHOOKS DOS PROVEDORES (pushinpay, cnpay, oasyfy, syncpay, brpix)
app.post('/api/webhook/pushinpay', async (req, res) => {
    const { id, status, payer_name, payer_document } = req.body;
    if (status === 'paid') {
        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${id} AND provider = 'pushinpay'`;
            if (tx && tx.status !== 'paid') {
                await handleSuccessfulPayment(tx.id, { name: payer_name, document: payer_document });
            }
        } catch (error) { console.error("Erro no webhook da PushinPay:", error); }
    }
    res.sendStatus(200);
});
app.post('/api/webhook/cnpay', async (req, res) => {
    const { transactionId, status, customer } = req.body;
    if (status === 'COMPLETED') {
        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'cnpay'`;
            if (tx && tx.status !== 'paid') {
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.taxID?.taxID });
            }
        } catch (error) { console.error("Erro no webhook da CNPay:", error); }
    }
    res.sendStatus(200);
});
app.post('/api/webhook/oasyfy', async (req, res) => {
    console.log('[Webhook Oasy.fy] Corpo completo do webhook recebido:', JSON.stringify(req.body, null, 2));
    const transactionData = req.body.transaction;
    const customer = req.body.client;
    if (!transactionData || !transactionData.status) {
        console.log("[Webhook Oasy.fy] Webhook ignorado: objeto 'transaction' ou 'status' ausente.");
        return res.sendStatus(200);
    }
    const { id: transactionId, status } = transactionData;
    if (status === 'COMPLETED') {
        try {
            console.log(`[Webhook Oasy.fy] Processando pagamento para transactionId: ${transactionId}`);
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'oasyfy'`;
            if (tx) {
                console.log(`[Webhook Oasy.fy] Transação encontrada no banco. ID interno: ${tx.id}, Status atual: ${tx.status}`);
                if (tx.status !== 'paid') {
                    console.log(`[Webhook Oasy.fy] Status não é 'paid'. Chamando handleSuccessfulPayment...`);
                    await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
                    console.log(`[Webhook Oasy.fy] handleSuccessfulPayment concluído para transação ID ${tx.id}.`);
                } else {
                    console.log(`[Webhook Oasy.fy] Transação ${transactionId} já está marcada como 'paid'. Nenhuma ação necessária.`);
                }
            } else {
                console.error(`[Webhook Oasy.fy] ERRO CRÍTICO: Transação com provider_transaction_id = '${transactionId}' NÃO FOI ENCONTRADA no banco de dados.`);
            }
        } catch (error) {
            console.error("[Webhook Oasy.fy] ERRO DURANTE O PROCESSAMENTO:", error);
        }
    } else {
        console.log(`[Webhook Oasy.fy] Recebido webhook com status '${status}', que não é 'COMPLETED'. Ignorando.`);
    }
    res.sendStatus(200);
});

app.post('/api/webhook/syncpay', async (req, res) => {
    try {
        const notification = req.body;
        console.log('[Webhook SyncPay] Notificação recebida:', JSON.stringify(notification, null, 2));

        if (!notification.data) {
            console.log('[Webhook SyncPay] Webhook ignorado: formato inesperado, objeto "data" não encontrado.');
            return res.sendStatus(200);
        }

        const transactionData = notification.data;
        const transactionId = transactionData.id; // Correct ID field for SyncPay webhook
        const status = transactionData.status;
        const customer = transactionData.client; // Payer info might be in 'client'

        if (!transactionId || !status) {
            console.log('[Webhook SyncPay] Ignorado: "id" ou "status" não encontrados dentro do objeto "data".');
            return res.sendStatus(200);
        }

        if (String(status).toLowerCase() === 'completed') { // SyncPay uses 'completed'

            console.log(`[Webhook SyncPay] Processando pagamento para transação: ${transactionId}`);

            // Use the correct transaction ID field from SyncPay ('identifier' was used during creation, 'id' comes in webhook)
            const [tx] = await sql`
                SELECT * FROM pix_transactions
                WHERE provider_transaction_id = ${transactionId} AND provider = 'syncpay'
            `;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook SyncPay] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
                // Map customer data correctly if available
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document });
            } else if (tx) {
                console.log(`[Webhook SyncPay] Transação ${tx.id} já estava como 'paga'.`);
            } else {
                console.warn(`[Webhook SyncPay] AVISO: Transação com ID ${transactionId} não foi encontrada no banco de dados.`);
            }
        }

        res.sendStatus(200);

    } catch (error) {
        console.error("Erro CRÍTICO no webhook da SyncPay:", error);
        res.sendStatus(500);
    }
});

app.post('/api/webhook/brpix', async (req, res) => {
    const { type, data } = req.body;
    console.log('[Webhook BRPix] Notificação recebida:', JSON.stringify(req.body, null, 2));

    if (type === 'transaction' && data?.status === 'paid') {
        const transactionId = data.id;
        const customer = data.customer;

        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'brpix'`;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook BRPix] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document?.number }); // Adapt based on actual customer object structure
            } else if (tx) {
                console.log(`[Webhook BRPix] Transação ${tx.id} já estava como 'paga'. Nenhuma ação necessária.`);
            } else {
                 console.warn(`[Webhook BRPix] AVISO: Transação com ID ${transactionId} não foi encontrada no banco de dados.`);
            }
        } catch (error) {
            console.error("Erro CRÍTICO no webhook da BRPix:", error);
        }
    }

    res.sendStatus(200);
});
// ROTA UTMIFY (sendEventToUtmify)
async function sendEventToUtmify(status, clickData, pixData, sellerData, customerData, productData) {
    console.log(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        // Determine Utmify integration based on click origin (Pressel or Checkout)
        if (clickData.pressel_id) {
            console.log(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sql`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id && clickData.checkout_id.startsWith('cko_')) {
            // Find Utmify integration linked to the hosted checkout (assuming it's stored in config)
             const [checkout] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${clickData.checkout_id}`;
             // Adjust the path according to where you store the utmify_integration_id in the config JSON
             integrationId = checkout?.config?.tracking?.utmify_integration_id;
             if (integrationId) {
                console.log(`[Utmify] Clique originado do Checkout Hospedado ID: ${clickData.checkout_id}. Integração Utmify ID: ${integrationId}`);
             } else {
                console.log(`[Utmify] Clique originado do Checkout Hospedado ID: ${clickData.checkout_id}, mas nenhuma integração Utmify configurada.`);
             }
        } else {
             console.log(`[Utmify] Clique ID ${clickData.id} não originado de Pressel ou Checkout Hospedado conhecido.`);
        }


        if (!integrationId) {
            console.log(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        console.log(`[Utmify] Integração vinculada ID: ${integrationId}. Buscando token...`);
        // Fetch the API token for the selected integration
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

        // Prepare payload for Utmify API
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id || `ht_${pixData.id}`, // Use provider ID or internal ID as fallback
            platform: "HotTrack", paymentMethod: 'pix',
            status: status, createdAt: createdAt, approvedDate: approvedDate, refundedAt: null,
            customer: { name: customerData?.name || "Não informado", email: customerData?.email || "naoinformado@email.com", phone: customerData?.phone || null, document: customerData?.document || null, },
            products: [{ id: productData?.id || "default_product", name: productData?.name || "Produto Digital", planId: null, planName: null, quantity: 1, priceInCents: Math.round(pixData.pix_value * 100) }],
            trackingParameters: { src: null, sck: null, utm_source: clickData.utm_source, utm_campaign: clickData.utm_campaign, utm_medium: clickData.utm_medium, utm_content: clickData.utm_content, utm_term: clickData.utm_term, click_id: clickData.click_id?.replace('/start ', '') || null }, // Adiciona click_id aqui
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0299)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0299))) },
            isTest: false // Set to true for testing if needed
        };

        // Send event to Utmify
        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        console.log(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        console.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}
// ROTA META (sendMetaEvent)
async function sendMetaEvent(eventName, clickData, transactionData, customerData = null) {
    try {
        let pixelConfigs = []; // Will store { pixel_id, meta_api_token }

        // Determine the source (Pressel or Checkout) and get associated pixels
        if (clickData.pressel_id) {
            // 1. Fetch pixels linked to Pressels
            pixelConfigs = await sql`
                SELECT pc.pixel_id, pc.meta_api_token
                FROM pixel_configurations pc
                JOIN pressel_pixels pp ON pc.id = pp.pixel_config_id
                WHERE pp.pressel_id = ${clickData.pressel_id} AND pc.seller_id = ${clickData.seller_id}
            `;
        } else if (clickData.checkout_id && clickData.checkout_id.startsWith('cko_')) {
            // 2. Fetch pixel from Hosted Checkouts (cko_) config
            const [hostedCheckout] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${clickData.checkout_id}`;
            // Extract pixel_id from the JSON config (adjust path if needed)
            const pixelId = hostedCheckout?.config?.tracking?.pixel_id;
            if (pixelId) {
                // Find the full pixel configuration using the pixel_id
                const [pixelConfig] = await sql`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE pixel_id = ${pixelId} AND seller_id = ${clickData.seller_id}`;
                if (pixelConfig) pixelConfigs.push(pixelConfig);
            }
        }
        // Add handling for old numeric checkouts if necessary

        // If no pixels are configured for this source, log and exit
        if (pixelConfigs.length === 0) {
            console.log(`Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}. Origem: pressel_id=${clickData.pressel_id}, checkout_id=${clickData.checkout_id}`);
            return;
        }

        // --- Prepare UserData object for Meta ---
        const userData = {
            fbp: clickData.fbp || undefined, // Facebook browser ID cookie
            fbc: clickData.fbc || undefined, // Facebook click ID cookie
            // Use clean click ID as external_id
            external_id: clickData.click_id ? clickData.click_id.replace('/start ', '') : undefined
        };

        // Add IP and User Agent if available and valid
        if (clickData.ip_address && clickData.ip_address !== '::1' && !clickData.ip_address.startsWith('127.0.0.1')) {
            userData.client_ip_address = clickData.ip_address;
        }
        if (clickData.user_agent && clickData.user_agent.length > 10) { // Basic check for validity
            userData.client_user_agent = clickData.user_agent;
        }

        // Add hashed customer PII if available (from payment confirmation or Telegram)
        if (customerData?.name) {
            const nameParts = customerData.name.trim().split(' ');
            const firstName = nameParts[0].toLowerCase();
            const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1].toLowerCase() : undefined;
            userData.fn = crypto.createHash('sha256').update(firstName).digest('hex'); // Hashed first name
            if (lastName) {
                userData.ln = crypto.createHash('sha256').update(lastName).digest('hex'); // Hashed last name
            }
        }
        // Add hashed City/State from GeoIP if available
        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex'); // Hashed city
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex'); // Hashed state

        // Remove any undefined fields from userData
        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);

        // --- Loop through each configured pixel and send the event ---
        for (const pixelConfig of pixelConfigs) {
            if (pixelConfig && pixelConfig.meta_api_token) {
                const { pixel_id, meta_api_token } = pixelConfig;
                // Generate a unique event ID using transaction/click ID and pixel ID
                const event_id = `${eventName}.${transactionData.id || clickData.id}.${pixel_id}`;

                // Construct the event payload
                const payload = {
                    data: [{
                        event_name: eventName,
                        event_time: Math.floor(Date.now() / 1000), // Current Unix timestamp
                        event_id,
                        action_source: 'other', // Indicate server-side event
                        user_data: userData,
                        custom_data: {
                            currency: 'BRL',
                            value: transactionData.pix_value // Purchase value (in Reais)
                        },
                    }]
                };

                // Remove 'value' for non-Purchase events
                if (eventName !== 'Purchase') {
                    delete payload.data[0].custom_data.value;
                }

                try {
                    // Send event to Meta Conversion API
                    console.log(`[Meta Pixel] Enviando payload para o pixel ${pixel_id}:`, JSON.stringify(payload, null, 2));
                    await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                    console.log(`Evento '${eventName}' enviado para o Pixel ID ${pixel_id}.`);

                    // Store the Meta event ID in the transaction record for reference
                    if (eventName === 'Purchase') {
                         await sql`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                    }
                } catch (pixelError) {
                    // Log specific errors from Meta API
                    console.error(`Erro ao enviar evento '${eventName}' para o Pixel ID ${pixel_id}:`, pixelError.response?.data || pixelError.message);
                }

            } else {
                 console.warn(`[Meta Pixel] Token da API não encontrado para o Pixel ID ${pixelConfig?.pixel_id} do vendedor ${clickData.seller_id}.`);
            }
        } // End loop through pixels

    } catch (error) {
        // Log general errors during event preparation or sending
        console.error(`Erro geral ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}
// CHECK TRANSAÇÕES PENDENTES (checkPendingTransactions)
async function checkPendingTransactions() {
    try {
        // Find recent pending transactions (e.g., within the last 30 minutes)
        const pendingTransactions = await sql`
            SELECT id, provider, provider_transaction_id, click_id_internal, status
            FROM pix_transactions WHERE status = 'pending' AND created_at > NOW() - INTERVAL '30 minutes'`;

        if (pendingTransactions.length === 0) return; // No pending transactions to check

        // Iterate through pending transactions
        for (const tx of pendingTransactions) {
            // Skip providers that rely solely on webhooks
            if (tx.provider === 'oasyfy' || tx.provider === 'cnpay' || tx.provider === 'brpix') {
                continue;
            }

            try {
                // Get seller associated with the transaction's click
                const [seller] = await sql`
                    SELECT *
                    FROM sellers s JOIN clicks c ON c.seller_id = s.id
                    WHERE c.id = ${tx.click_id_internal}`;
                if (!seller) continue; // Skip if seller not found

                let providerStatus, customerData = {};
                // Check status with the specific provider's API
                if (tx.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${tx.provider_transaction_id}`, { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                    providerStatus = response.data.status;
                    customerData = response.data.payer;
                } else if (tx.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${tx.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    customerData = { name: response.data.payer_name, document: response.data.payer_document };
                }

                // If provider confirms payment and our DB status is still pending, process payment
                if ((providerStatus === 'paid' || providerStatus === 'COMPLETED') && tx.status !== 'paid') {
                     await handleSuccessfulPayment(tx.id, customerData);
                }
            } catch (error) {
                // Log errors unless it's a 404 (transaction not found at provider yet)
                if (!error.response || error.response.status !== 404) {
                    console.error(`Erro ao verificar transação ${tx.id} (${tx.provider}):`, error.response?.data || error.message);
                }
            }
            // Small delay between checks to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 200));
        }
    } catch (error) {
        console.error("Erro na rotina de verificação geral:", error.message);
    }
}
// ROTA CRIAÇÃO CHECKOUT HOSPEDADO (create-hosted)
app.post('/api/checkouts/create-hosted', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects the full config object from the frontend

    // Generate a unique ID for the new checkout, prefixed for easy identification
    const checkoutId = `cko_${uuidv4()}`;

    try {
        // Insert into the hosted_checkouts table
        await sql`
            INSERT INTO hosted_checkouts (id, seller_id, config)
            VALUES (${checkoutId}, ${sellerId}, ${JSON.stringify(config)});
        `;

        // Return the generated ID to the frontend
        res.status(201).json({
            message: 'Checkout hospedado criado com sucesso!',
            checkoutId: checkoutId
        });

    } catch (error) {
        console.error("Erro ao criar checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno ao criar o checkout.' });
    }
});
// ROTA PÁGINA DE OFERTA (/api/oferta/:checkoutId)
app.get('/api/oferta/:checkoutId', async (req, res) => {
    const { checkoutId } = req.params;

    try {
        // Fetch the configuration JSON for the given checkout ID
        const [checkout] = await sql`
            SELECT config FROM hosted_checkouts WHERE id = ${checkoutId}
        `;

        if (!checkout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        // Return the configuration object
        res.status(200).json(checkout); // Returns { config: { ... } }

    } catch (error) {
        console.error("Erro ao buscar dados do checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});

// ==========================================================
// ROTA GERAR PIX DA PÁGINA DE OFERTA (MODIFICADA COM CLICK_ID)
// ==========================================================
app.post('/api/oferta/generate-pix', async (req, res) => {
    // 1. Capture o click_id do corpo da requisição
    const { checkoutId, value_cents, click_id } = req.body;

    if (!checkoutId || !value_cents) {
        return res.status(400).json({ message: 'Dados insuficientes para gerar o PIX.' });
    }

    try {
        // 1. Find the seller associated with this checkout
        const [hostedCheckout] = await sql`
            SELECT seller_id, config FROM hosted_checkouts WHERE id = ${checkoutId}
        `;

        if (!hostedCheckout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        const sellerId = hostedCheckout.seller_id;
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;

        if (!seller) {
            return res.status(404).json({ message: 'Vendedor associado a este checkout não foi encontrado.' });
        }

        // 2. Lógica de Rastreamento de Clique (Modificada)
        const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
        const user_agent = req.headers['user-agent'];

        let clickData;
        let clickIdInternal;

        if (click_id) {
            // Se um click_id foi fornecido, encontre-o
            // Aceita tanto "/start lead..." quanto apenas "lead..."
            const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
            const [existingClick] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;

            if (existingClick) {
                clickData = existingClick;
                clickIdInternal = existingClick.id;
                console.log(`[Checkout PIX] Usando click_id existente: ${click_id} (ID: ${clickIdInternal})`);
                // Opcional: Atualiza o checkout_id se for o primeiro PIX desse clique OU se for diferente
                if (!existingClick.checkout_id || existingClick.checkout_id !== checkoutId) {
                    await sql`UPDATE clicks SET checkout_id = ${checkoutId} WHERE id = ${clickIdInternal}`;
                    clickData.checkout_id = checkoutId; // Atualiza o objeto em memória
                }
            } else {
                // Fallback: Click ID fornecido não foi encontrado. Cria um novo.
                console.warn(`[Checkout PIX] Click ID ${db_click_id} não encontrado. Criando um novo clique.`);
                [clickData] = await sql`
                    INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent, click_id)
                    VALUES (${sellerId}, ${checkoutId}, ${ip_address}, ${user_agent}, ${db_click_id})
                    RETURNING *;
                `;
                clickIdInternal = clickData.id;
            }
        } else {
            // Nenhum click_id fornecido. Cria um novo clique.
            console.log(`[Checkout PIX] Nenhum click_id. Criando novo clique.`);
            [clickData] = await sql`
                INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent)
                VALUES (${sellerId}, ${checkoutId}, ${ip_address}, ${user_agent})
                RETURNING *;
            `;
            clickIdInternal = clickData.id;

            // Gera e salva o click_id 'lead...' no banco
            const clean_click_id = `lead${clickIdInternal.toString().padStart(6, '0')}`;
            const db_click_id_new = `/start ${clean_click_id}`;
            await sql`UPDATE clicks SET click_id = ${db_click_id_new} WHERE id = ${clickIdInternal}`;
            clickData.click_id = db_click_id_new; // Atualiza o objeto em memória
        }

        // 3. Generate PIX (Lógica existente)
        const provider = seller.pix_provider_primary || 'pushinpay';
        const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);

        // 4. Save the PIX transaction, linking it to the click (novo ou existente)
        const [transaction] = await sql`
            INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, qr_code_base64, provider, provider_transaction_id, pix_id)
            VALUES (${clickIdInternal}, ${value_cents / 100}, ${pixResult.qr_code_text}, ${pixResult.qr_code_base64}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id})
            RETURNING id;
        `;

        // 5. Send 'InitiateCheckout' event to Meta Pixel API
        // Passa o clickData (seja novo ou existente, já contém todos os dados de rastreamento)
        await sendMetaEvent('InitiateCheckout', clickData, { id: transaction.id, pix_value: value_cents / 100 }, null);


        // 6. Return PIX details to the frontend (incluindo transaction_id e qr_code_base64)
        res.status(200).json(pixResult);

    } catch (error) {
        console.error("Erro ao gerar PIX da página de oferta:", error);
        res.status(500).json({ message: 'Não foi possível gerar o PIX no momento.' });
    }
});
// ==========================================================
// FIM DA ROTA MODIFICADA
// ==========================================================


// ROTAS PÁGINAS DE OBRIGADO
// Create a new Thank You Page configuration
app.post('/api/thank-you-pages/create', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects config object { page_name, purchase_value, pixel_id, redirect_url, utmify_integration_id? }

    // Validate essential fields
    if (!config.page_name || !config.purchase_value || !config.pixel_id || !config.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para criar a página.' });
    }

    // Generate unique ID for the page
    const pageId = `ty_${uuidv4()}`;

    try {
        // Insert the configuration into the database
        await sql`
            INSERT INTO thank_you_pages (id, seller_id, config)
            VALUES (${pageId}, ${sellerId}, ${JSON.stringify(config)});
        `;

        res.status(201).json({
            message: 'Página de obrigado criada com sucesso!',
            pageId: pageId // Return the generated ID
        });

    } catch (error) {
        console.error("Erro ao criar página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno ao criar a página.' });
    }
});

// Fetch configuration for a specific Thank You Page (used by the page itself)
app.get('/api/obrigado/:pageId', async (req, res) => {
    const { pageId } = req.params;

    try {
        const [page] = await sql`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada.' });
        }

        // Return only the configuration needed by the frontend page
        res.status(200).json({
            config: page.config,
        });

    } catch (error) {
        console.error("Erro ao buscar dados da página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});

// Trigger Utmify event from the Thank You Page frontend - ROTA ATUALIZADA
app.post('/api/thank-you-pages/fire-utmify', async (req, res) => {
    const { pageId, trackingParameters, customerData } = req.body;

    try {
        // Fetch page config to get Utmify integration ID and seller ID
        const [page] = await sql`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        // Check if page exists and has Utmify configured
        if (!page || !page.config.utmify_integration_id) {
            return res.status(404).json({ message: 'Página ou integração Utmify não configurada.' });
        }

        // Fetch the Utmify API token using the integration ID
        const [integration] = await sql`
            SELECT api_token FROM utmify_integrations WHERE id = ${page.config.utmify_integration_id}
        `;

        if (!integration || !integration.api_token) {
             return res.status(401).json({ message: 'Token da integração Utmify não encontrado.' });
        }

        // Fetch seller commission rate
        const [seller] = await sql`SELECT commission_rate FROM sellers WHERE id = ${page.seller_id}`;

        const purchaseValueCents = Math.round(page.config.purchase_value * 100);
        const commission_rate = seller.commission_rate || 0.0299;

        // Prepare payload for Utmify - CORRIGIDO e com click_id
        const payload = {
            orderId: `ty_${pageId}_${Date.now()}`, // Generate a unique order ID
            platform: "HotTrack TY Page",
            paymentMethod: 'unknown', // Changed from 'card' to 'unknown'
            status: 'paid', // Assume payment is confirmed if they reach TY page
            createdAt: new Date().toISOString().replace('T', ' ').substring(0, 19),
            approvedDate: new Date().toISOString().replace('T', ' ').substring(0, 19),
            refundedAt: null, // Add refundedAt as null
            customer: { // Use data passed from frontend, with fallbacks
                name: customerData?.name || "Cliente",
                email: customerData?.email || "email@desconhecido.com",
                phone: customerData?.phone || null,
                document: null, // TY page usually doesn't have document
            },
            products: [{
                id: `prod_${page.config.page_name.replace(/\s/g, '_')}`,
                name: page.config.page_name,
                planId: null, // Added, defaulting to null
                planName: null, // Added, defaulting to null
                quantity: 1,
                priceInCents: purchaseValueCents
            }],
            trackingParameters: { // Ensure all UTM parameters exist, defaulting to null
                src: trackingParameters?.src || null,
                sck: trackingParameters?.sck || null,
                utm_source: trackingParameters?.utm_source || null,
                utm_campaign: trackingParameters?.utm_campaign || null,
                utm_medium: trackingParameters?.utm_medium || null,
                utm_content: trackingParameters?.utm_content || null,
                utm_term: trackingParameters?.utm_term || null,
                click_id: trackingParameters?.click_id || null // Adiciona o click_id capturado
            },
            commission: {
                totalPriceInCents: purchaseValueCents,
                gatewayFeeInCents: Math.round(purchaseValueCents * commission_rate),
                userCommissionInCents: Math.round(purchaseValueCents * (1 - commission_rate))
            },
            isTest: false
        };

        // Send event to Utmify
        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, {
            headers: { 'x-api-token': integration.api_token }
        });

        res.status(200).json({ message: 'Evento Utmify enviado.' });

    } catch (error) {
        console.error(`[Utmify TY Page Error]`, error.response?.data || error.message);
        res.status(500).json({ message: 'Erro ao enviar evento para Utmify.' });
    }
});

// --- ROTAS PÁGINAS DE OBRIGADO (GERENCIAMENTO) ADICIONADAS ---

// LISTAR PÁGINAS DE OBRIGADO DO VENDEDOR
app.get('/api/thank-you-pages', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        // Seleciona ID, nome (extraído do config) e data de criação
        const pages = await sql`
            SELECT
                id,
                config->>'page_name' as name, -- Extrai 'page_name' do JSON config
                created_at
            FROM thank_you_pages
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(pages);
    } catch (error) {
        console.error("Erro ao listar páginas de obrigado:", error);
        res.status(500).json({ message: 'Erro ao buscar suas páginas de obrigado.' });
    }
});

// BUSCAR UMA PÁGINA DE OBRIGADO ESPECÍFICA (PARA EDIÇÃO)
app.get('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;
    try {
        const [page] = await sql`
            SELECT id, config FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId};
        `;
        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada ou não pertence a você.' });
        }
        res.status(200).json(page); // Retorna { id: 'ty_...', config: { ... } }
    } catch (error) {
        console.error(`Erro ao buscar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao buscar a página.' });
    }
});


// ATUALIZAR UMA PÁGINA DE OBRIGADO
app.put('/api/thank-you-pages/:pageId', authenticateApiKey, async (req, res) => { // Usando ApiKey para consistência com create
    const { pageId } = req.params;
    const sellerId = req.sellerId;
    const newConfig = req.body; // Espera o objeto config atualizado

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }
     // Valida campos essenciais no newConfig
    if (!newConfig.page_name || !newConfig.purchase_value || !newConfig.pixel_id || !newConfig.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para atualizar a página.' });
    }

    try {
        const result = await sql`
            UPDATE thank_you_pages
            SET config = ${JSON.stringify(newConfig)}, updated_at = NOW()
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
            return res.status(404).json({ message: 'Página não encontrada ou você não tem permissão para editá-la.' });
        }
        res.status(200).json({ message: 'Página de obrigado atualizada com sucesso!', pageId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar a página.' });
    }
});

// DELETAR UMA PÁGINA DE OBRIGADO
app.delete('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }

    try {
        // Deleta diretamente, não há dependências complexas como nos checkouts
        const result = await sql`
            DELETE FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
             console.warn(`Tentativa de excluir página TY não encontrada ou não pertencente ao seller: ${pageId}, Seller: ${sellerId}`);
        }
        res.status(200).json({ message: 'Página de obrigado excluída com sucesso!' });
    } catch (error) {
        console.error(`Erro ao excluir página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao excluir a página.' });
    }
});

// --- FIM DAS ROTAS DE GERENCIAMENTO DE PÁGINAS DE OBRIGADO ---

// ########## NOVAS ROTAS PARA GERENCIAR CHECKOUTS HOSPEDADOS ##########

// LISTAR CHECKOUTS
app.get('/api/checkouts', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        // Select the ID, extract the main title from the config JSON, and the creation date
        const checkouts = await sql`
            SELECT
                id,
                config->'content'->>'main_title' as name, -- Extracts 'main_title' from the 'content' object within 'config'
                created_at
            FROM hosted_checkouts
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(checkouts);
    } catch (error) {
        console.error("Erro ao listar checkouts hospedados:", error);
        res.status(500).json({ message: 'Erro ao buscar seus checkouts.' });
    }
});

// EDITAR (ATUALIZAR) CHECKOUT
app.put('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;
    const newConfig = req.body; // Expects the full updated config object from the frontend

    // Basic validation
    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }

    try {
        // Update the config JSON and updated_at timestamp for the specific checkout ID and seller ID
        const result = await sql`
            UPDATE hosted_checkouts
            SET config = ${JSON.stringify(newConfig)}, updated_at = NOW()
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Return the ID to confirm update occurred
        `;

        // Check if any row was updated
        if (result.length === 0) {
            return res.status(404).json({ message: 'Checkout não encontrado ou você não tem permissão para editá-lo.' });
        }

        res.status(200).json({ message: 'Checkout atualizado com sucesso!', checkoutId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar checkout ${checkoutId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar o checkout.' });
    }
});

// EXCLUIR CHECKOUT
app.delete('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;

    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }

    try {
        // IMPORTANT: First, delete associated clicks to avoid foreign key constraint errors
        await sql `DELETE FROM clicks WHERE checkout_id = ${checkoutId} AND seller_id = ${sellerId}`;

        // Now, delete the checkout itself
        const result = await sql`
            DELETE FROM hosted_checkouts
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Return ID to confirm deletion
        `;

        if (result.length === 0) {
            console.warn(`Tentativa de excluir checkout não encontrado ou não pertencente ao seller: ${checkoutId}, Seller: ${sellerId}`);
            // Still return success as the end state (checkout doesn't exist) is achieved
        }

        res.status(200).json({ message: 'Checkout excluído com sucesso!' }); // Use 200 with message

    } catch (error) {
        console.error(`Erro ao excluir checkout ${checkoutId}:`, error);
        // Specifically handle foreign key violations if pix_transactions block deletion
        if (error.code === '23503') { // PostgreSQL foreign key violation error code
             console.error(`Erro de chave estrangeira ao excluir checkout ${checkoutId}. Pode haver transações PIX associadas.`);
             return res.status(409).json({ message: 'Não é possível excluir este checkout pois existem transações PIX associadas a ele através de cliques. Contacte o suporte se necessário.' });
        }
        res.status(500).json({ message: 'Erro interno ao excluir o checkout.' });
    }
});

// ########## FIM DAS NOVAS ROTAS ##########


module.exports = app; // Export the app for Vercel