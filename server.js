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
app.use(express.json()); // Middleware global para processar JSON

// --- OTIMIZAÇÃO CRÍTICA: A conexão com o banco é inicializada UMA VEZ e reutilizada ---
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
    if (cachedToken && cachedToken.expiresAt > Date.now() + 60000) {
        return cachedToken.accessToken;
    }
    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) {
        throw new Error('Credenciais da SyncPay não configuradas para este vendedor.');
    }
    console.log(`[SyncPay] Solicitando novo token para o vendedor ID: ${seller.id}`);
    const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/auth-token`, {
        client_id: seller.syncpay_client_id,
        client_secret: seller.syncpay_client_secret,
    });
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

// --- ROTAS DO PAINEL ADMINISTRATIVO ---
function authenticateAdmin(req, res, next) {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ message: 'Acesso negado. Chave de administrador inválida.' });
    }
    next();
}

async function sendHistoricalMetaEvent(eventName, clickData, transactionData, targetPixel) {
    let payload_sent = null;
    try {
        const userData = {};
        if (clickData.ip_address) userData.client_ip_address = clickData.ip_address;
        if (clickData.user_agent) userData.client_user_agent = clickData.user_agent;
        if (clickData.fbp) userData.fbp = clickData.fbp;
        if (clickData.fbc) userData.fbc = clickData.fbc;
        if (clickData.firstName) userData.fn = crypto.createHash('sha256').update(clickData.firstName.toLowerCase()).digest('hex');
        if (clickData.lastName) userData.ln = crypto.createHash('sha256').update(clickData.lastName.toLowerCase()).digest('hex');
        
        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        const { pixelId, accessToken } = targetPixel;
        const event_time = Math.floor(new Date(transactionData.paid_at).getTime() / 1000);
        const event_id = `${eventName}.${transactionData.id}.${pixelId}`;

        const payload = {
            data: [{
                event_name: eventName,
                event_time: event_time,
                event_id,
                action_source: 'other',
                user_data: userData,
                custom_data: {
                    currency: 'BRL',
                    value: parseFloat(transactionData.pix_value)
                },
            }]
        };
        payload_sent = payload;

        if (Object.keys(userData).length === 0) {
            throw new Error('Dados de usuário insuficientes para envio (IP/UserAgent faltando).');
        }

        await axios.post(`https://graph.facebook.com/v19.0/${pixelId}/events`, payload, { params: { access_token: accessToken } });
        
        return { success: true, payload: payload_sent };

    } catch (error) {
        const metaError = error.response?.data?.error || { message: error.message };
        console.error(`Erro ao reenviar evento (Transação ID: ${transactionData.id}):`, metaError.message);
        return { success: false, error: metaError, payload: payload_sent };
    }
}

app.post('/api/admin/resend-events', authenticateAdmin, async (req, res) => {
    const { 
        target_pixel_id, target_meta_api_token, seller_id, 
        start_date, end_date, page = 1, limit = 50
    } = req.body;

    if (!target_pixel_id || !target_meta_api_token || !start_date || !end_date) {
        return res.status(400).json({ message: 'Todos os campos obrigatórios devem ser preenchidos.' });
    }

    try {
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

        const clickIds = allPaidTransactions.map(t => t.click_id).filter(Boolean);
        let userDataMap = new Map();
        if (clickIds.length > 0) {
            const telegramUsers = await sql`
                SELECT click_id, first_name, last_name 
                FROM telegram_chats 
                WHERE click_id = ANY(${clickIds})
            `;
            telegramUsers.forEach(user => {
                const cleanClickId = user.click_id.startsWith('/start ') ? user.click_id : `/start ${user.click_id}`;
                userDataMap.set(cleanClickId, { firstName: user.first_name, lastName: user.last_name });
            });
        }

        const totalEvents = allPaidTransactions.length;
        const totalPages = Math.ceil(totalEvents / limit);
        const offset = (page - 1) * limit;
        const batch = allPaidTransactions.slice(offset, offset + limit);
        
        const detailedResults = [];
        const targetPixel = { pixelId: target_pixel_id, accessToken: target_meta_api_token };

        console.log(`[ADMIN] Processando página ${page}/${totalPages}. Lote com ${batch.length} eventos.`);

        for (const transaction of batch) {
            const extraUserData = userDataMap.get(transaction.click_id);
            const enrichedTransactionData = { ...transaction, ...extraUserData };
            const result = await sendHistoricalMetaEvent('Purchase', enrichedTransactionData, transaction, targetPixel);
            
            detailedResults.push({
                transaction_id: transaction.id,
                status: result.success ? 'success' : 'failure',
                payload_sent: result.payload,
                meta_response: result.error || 'Enviado com sucesso.'
            });
            await new Promise(resolve => setTimeout(resolve, 100));
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

app.get('/api/admin/vapidPublicKey', authenticateAdmin, (req, res) => {
    if (!process.env.VAPID_PUBLIC_KEY) {
        return res.status(500).send('VAPID Public Key não configurada no servidor.');
    }
    res.type('text/plain').send(process.env.VAPID_PUBLIC_KEY);
});

app.post('/api/admin/save-subscription', authenticateAdmin, (req, res) => {
    adminSubscription = req.body;
    console.log("Inscrição de admin para notificações recebida e guardada.");
    res.status(201).json({});
});

app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
    try {
        const totalSellers = await sql`SELECT COUNT(*) FROM sellers;`;
        const paidTransactions = await sql`SELECT COUNT(*) as count, SUM(pix_value) as total_revenue FROM pix_transactions WHERE status = 'paid';`;
        const total_sellers = parseInt(totalSellers[0].count);
        const total_paid_transactions = parseInt(paidTransactions[0].count);
        const total_revenue = parseFloat(paidTransactions[0].total_revenue || 0);
        const saas_profit = total_revenue * 0.0299;
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
app.get('/api/admin/sellers', authenticateAdmin, async (req, res) => {
    try {
        const sellers = await sql`SELECT id, name, email, created_at, is_active FROM sellers ORDER BY created_at DESC;`;
        res.json(sellers);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao listar vendedores.' });
    }
});
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
app.put('/api/admin/sellers/:id/credentials', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { pushinpay_token, cnpay_public_key, cnpay_secret_key } = req.body;
    try {
        await sql`
            UPDATE sellers 
            SET pushinpay_token = ${pushinpay_token}, cnpay_public_key = ${cnpay_public_key}, cnpay_secret_key = ${cnpay_secret_key}
            WHERE id = ${id};`;
        res.status(200).json({ message: 'Credenciais alteradas com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar credenciais.' });
    }
});
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

app.put('/api/admin/sellers/:id/commission', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { commission_rate } = req.body;

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
app.post('/api/sellers/register', async (req, res) => {
    const { name, email, password } = req.body;

    if (!name || !email || !password || password.length < 8) {
        return res.status(400).json({ message: 'Dados inválidos. Nome, email e senha (mínimo 8 caracteres) são obrigatórios.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const existingSeller = await sql`SELECT id FROM sellers WHERE LOWER(email) = ${normalizedEmail}`;
        if (existingSeller.length > 0) {
            return res.status(409).json({ message: 'Este email já está em uso.' });
        }
        const hashedPassword = await bcrypt.hash(password, 10);
        const apiKey = uuidv4();
        
        await sql`INSERT INTO sellers (name, email, password_hash, api_key, is_active) VALUES (${name}, ${normalizedEmail}, ${hashedPassword}, ${apiKey}, TRUE)`;
        
        res.status(201).json({ message: 'Vendedor cadastrado com sucesso!' });
    } catch (error) {
        console.error("Erro no registro:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

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
        
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }
        
        const isPasswordCorrect = await bcrypt.compare(password, seller.password_hash);
        if (!isPasswordCorrect) return res.status(401).json({ message: 'Senha incorreta.' });
        
        const tokenPayload = { id: seller.id, email: seller.email };
        const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '1d' });
        
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({ message: 'Login bem-sucedido!', token, seller: sellerData });

    } catch (error) {
        console.error("ERRO DETALHADO NO LOGIN:", error); 
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.get('/api/dashboard/data', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const settingsPromise = sql`SELECT api_key, pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key, syncpay_client_id, syncpay_client_secret, brpix_secret_key, brpix_company_id, pix_provider_primary, pix_provider_secondary, pix_provider_tertiary, commission_rate FROM sellers WHERE id = ${sellerId}`;
        const pixelsPromise = sql`SELECT * FROM pixel_configurations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const presselsPromise = sql`
            SELECT p.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids, b.bot_name
            FROM pressels p
            LEFT JOIN ( SELECT pressel_id, array_agg(pixel_config_id) as pixel_ids FROM pressel_pixels GROUP BY pressel_id ) px ON p.id = px.pressel_id
            JOIN telegram_bots b ON p.bot_id = b.id
            WHERE p.seller_id = ${sellerId} ORDER BY p.created_at DESC`;
        const botsPromise = sql`SELECT * FROM telegram_bots WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const checkoutsPromise = sql`
            SELECT c.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids
            FROM checkouts c
            LEFT JOIN ( SELECT checkout_id, array_agg(pixel_config_id) as pixel_ids FROM checkout_pixels GROUP BY checkout_id ) px ON c.id = px.checkout_id
            WHERE c.seller_id = ${sellerId} ORDER BY c.created_at DESC`;
        const utmifyIntegrationsPromise = sql`SELECT id, account_name FROM utmify_integrations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;

        const [settingsResult, pixels, pressels, bots, checkouts, utmifyIntegrations] = await Promise.all([
            settingsPromise, pixelsPromise, presselsPromise, botsPromise, checkoutsPromise, utmifyIntegrationsPromise
        ]);
        
        const settings = settingsResult[0] || {};
        res.json({ settings, pixels, pressels, bots, checkouts, utmifyIntegrations });
    } catch (error) {
        console.error("Erro ao buscar dados do dashboard:", error);
        res.status(500).json({ message: 'Erro ao buscar dados.' });
    }
});
app.get('/api/dashboard/achievements-and-ranking', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        
        const userAchievements = await sql`
            SELECT a.title, a.description, ua.is_completed, a.sales_goal
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id
            WHERE ua.seller_id = ${sellerId}
            ORDER BY a.sales_goal ASC;
        `;

        const topSellersRanking = await sql`
            SELECT s.name, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        `;
        
        const [userRevenue] = await sql`
            SELECT COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            WHERE s.id = ${sellerId}
            GROUP BY s.id;
        `;

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
app.post('/api/pixels', authenticateJwt, async (req, res) => {
    const { account_name, pixel_id, meta_api_token } = req.body;
    if (!account_name || !pixel_id || !meta_api_token) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    try {
        const newPixel = await sql`INSERT INTO pixel_configurations (seller_id, account_name, pixel_id, meta_api_token) VALUES (${req.user.id}, ${account_name}, ${pixel_id}, ${meta_api_token}) RETURNING *;`;
        res.status(201).json(newPixel[0]);
    } catch (error) {
        if (error.code === '23505') { return res.status(409).json({ message: 'Este ID de Pixel já foi cadastrado.' }); }
        console.error("Erro ao salvar pixel:", error);
        res.status(500).json({ message: 'Erro ao salvar o pixel.' });
    }
});
app.delete('/api/pixels/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM pixel_configurations WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pixel:", error);
        res.status(500).json({ message: 'Erro ao excluir o pixel.' });
    }
});

app.post('/api/bots', authenticateJwt, async (req, res) => {
    const { bot_name } = req.body;
    if (!bot_name) {
        return res.status(400).json({ message: 'O nome do bot é obrigatório.' });
    }
    try {
        const placeholderToken = uuidv4();

        const [newBot] = await sql`
            INSERT INTO telegram_bots (seller_id, bot_name, bot_token) 
            VALUES (${req.user.id}, ${bot_name}, ${placeholderToken}) 
            RETURNING *;
        `;
        res.status(201).json(newBot);
    } catch (error) {
        if (error.code === '23505' && error.constraint_name === 'telegram_bots_bot_name_key') {
            return res.status(409).json({ message: 'Um bot com este nome de usuário já existe.' });
        }
        console.error("Erro ao salvar bot:", error);
        res.status(500).json({ message: 'Erro ao salvar o bot.' });
    }
});

app.delete('/api/bots/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM telegram_bots WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir bot:", error);
        res.status(500).json({ message: 'Erro ao excluir o bot.' });
    }
});

app.put('/api/bots/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    let { bot_token } = req.body;
    if (!bot_token) {
        return res.status(400).json({ message: 'O token do bot é obrigatório.' });
    }
    bot_token = bot_token.trim();
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

app.post('/api/bots/:id/set-webhook', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    try {
        const [bot] = await sql`
            SELECT bot_token FROM telegram_bots 
            WHERE id = ${id} AND seller_id = ${sellerId}`;

        if (!bot || !bot.bot_token || bot.bot_token.trim() === '') {
            return res.status(400).json({ message: 'O token do bot não está configurado. Salve um token válido primeiro.' });
        }

        const token = bot.bot_token.trim();
        const webhookUrl = `https://novaapi-one.vercel.app/api/webhook/telegram/${id}`;
        const telegramApiUrl = `https://api.telegram.org/bot${token}/setWebhook?url=${webhookUrl}`;
        
        const response = await axios.get(telegramApiUrl);

        if (response.data.ok) {
            res.status(200).json({ message: 'Webhook configurado com sucesso!' });
        } else {
            throw new Error(response.data.description);
        }
    } catch (error) {
        console.error("Erro ao configurar webhook:", error);
        if (error.isAxiosError && error.response) {
            const status = error.response.status;
            const telegramMessage = error.response.data?.description || 'Resposta inválida do Telegram.';
            if (status === 401 || status === 404) {
                return res.status(400).json({ message: `O Telegram rejeitou seu token: "${telegramMessage}". Verifique se o token está correto.` });
            }
            return res.status(500).json({ message: `Erro de comunicação com o Telegram: ${telegramMessage}` });
        }
        res.status(500).json({ message: `Erro interno no servidor: ${error.message}` });
    }
});

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

app.get('/api/bots/users', authenticateJwt, async (req, res) => {
    const { botIds } = req.query; 

    if (!botIds) {
        return res.status(400).json({ message: 'IDs dos bots são obrigatórios.' });
    }
    const botIdArray = botIds.split(',').map(id => parseInt(id.trim(), 10));

    try {
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
app.post('/api/pressels', authenticateJwt, async (req, res) => {
    const { name, bot_id, white_page_url, pixel_ids, utmify_integration_id } = req.body;
    if (!name || !bot_id || !white_page_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    
    try {
        const numeric_bot_id = parseInt(bot_id, 10);
        const numeric_pixel_ids = pixel_ids.map(id => parseInt(id, 10));

        const botResult = await sql`SELECT bot_name FROM telegram_bots WHERE id = ${numeric_bot_id} AND seller_id = ${req.user.id}`;
        if (botResult.length === 0) {
            return res.status(404).json({ message: 'Bot não encontrado.' });
        }
        const bot_name = botResult[0].bot_name;

        await sql`BEGIN`;
        try {
            const [newPressel] = await sql`
                INSERT INTO pressels (seller_id, name, bot_id, bot_name, white_page_url, utmify_integration_id) 
                VALUES (${req.user.id}, ${name}, ${numeric_bot_id}, ${bot_name}, ${white_page_url}, ${utmify_integration_id || null}) 
                RETURNING *;
            `;
            
            for (const pixelId of numeric_pixel_ids) {
                await sql`INSERT INTO pressel_pixels (pressel_id, pixel_config_id) VALUES (${newPressel.id}, ${pixelId})`;
            }
            await sql`COMMIT`;
            
            res.status(201).json({ ...newPressel, pixel_ids: numeric_pixel_ids, bot_name });
        } catch (transactionError) {
            await sql`ROLLBACK`;
            throw transactionError;
        }
    } catch (error) {
        console.error("Erro ao salvar pressel:", error);
        res.status(500).json({ message: 'Erro ao salvar a pressel.' });
    }
});
app.delete('/api/pressels/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM pressels WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pressel:", error);
        res.status(500).json({ message: 'Erro ao excluir a pressel.' });
    }
});
app.post('/api/checkouts', authenticateJwt, async (req, res) => {
    const { name, product_name, redirect_url, value_type, fixed_value_cents, pixel_ids } = req.body;

    if (!name || !product_name || !redirect_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) {
        return res.status(400).json({ message: 'Nome, Nome do Produto, URL de Redirecionamento e ao menos um Pixel são obrigatórios.' });
    }
    if (value_type === 'fixed' && (!fixed_value_cents || fixed_value_cents <= 0)) {
        return res.status(400).json({ message: 'Para valor fixo, o valor em centavos deve ser maior que zero.' });
    }

    try {
        await sql`BEGIN`;

        const [newCheckout] = await sql`
            INSERT INTO checkouts (seller_id, name, product_name, redirect_url, value_type, fixed_value_cents)
            VALUES (${req.user.id}, ${name}, ${product_name}, ${redirect_url}, ${value_type}, ${value_type === 'fixed' ? fixed_value_cents : null})
            RETURNING *;
        `;

        for (const pixelId of pixel_ids) {
            await sql`INSERT INTO checkout_pixels (checkout_id, pixel_config_id) VALUES (${newCheckout.id}, ${pixelId})`;
        }
        
        await sql`COMMIT`;

        res.status(201).json({ ...newCheckout, pixel_ids: pixel_ids.map(id => parseInt(id)) });
    } catch (error) {
        await sql`ROLLBACK`;
        console.error("Erro ao salvar checkout:", error);
        res.status(500).json({ message: 'Erro interno ao salvar o checkout.' });
    }
});
app.delete('/api/checkouts/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM checkouts WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir checkout:", error);
        res.status(500).json({ message: 'Erro ao excluir o checkout.' });
    }
});
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
            pix_provider_primary = ${pix_provider_primary || 'pushinpay'},
            pix_provider_secondary = ${pix_provider_secondary || null},
            pix_provider_tertiary = ${pix_provider_tertiary || null}
            WHERE id = ${req.user.id}`;
        res.status(200).json({ message: 'Configurações de PIX salvas com sucesso.' });
    } catch (error) {
        console.error("Erro ao salvar configurações de PIX:", error);
        res.status(500).json({ message: 'Erro ao salvar as configurações.' });
    }
});
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
app.post('/api/registerClick', logApiRequest, async (req, res) => {
    const { sellerApiKey, presselId, checkoutId, referer, fbclid, fbp, fbc, user_agent, utm_source, utm_campaign, utm_medium, utm_content, utm_term } = req.body;

    if (!sellerApiKey || (!presselId && !checkoutId)) {
        return res.status(400).json({ message: 'Dados insuficientes.' });
    }

    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    try {
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

        const newClick = result[0];
        const click_record_id = newClick.id;
        const clean_click_id = `lead${click_record_id.toString().padStart(6, '0')}`;
        const db_click_id = `/start ${clean_click_id}`;
        
        await sql`UPDATE clicks SET click_id = ${db_click_id} WHERE id = ${click_record_id}`;

        res.status(200).json({ status: 'success', click_id: clean_click_id });

        (async () => {
            try {
                let city = 'Desconhecida', state = 'Desconhecido';
                if (ip_address && ip_address !== '::1' && !ip_address.startsWith('192.168.')) {
                    const geo = await axios.get(`http://ip-api.com/json/${ip_address}?fields=city,regionName`);
                    city = geo.data.city || city;
                    state = geo.data.regionName || state;
                }
                await sql`UPDATE clicks SET city = ${city}, state = ${state} WHERE id = ${click_record_id}`;
                console.log(`[BACKGROUND] Geolocalização atualizada para o clique ${click_record_id}.`);

                if (checkoutId) {
                    const [checkoutDetails] = await sql`SELECT fixed_value_cents FROM checkouts WHERE id = ${checkoutId}`;
                    const eventValue = checkoutDetails ? (checkoutDetails.fixed_value_cents / 100) : 0;
                    await sendMetaEvent('InitiateCheckout', { ...newClick, click_id: clean_click_id }, { pix_value: eventValue, id: click_record_id });
                    console.log(`[BACKGROUND] Evento InitiateCheckout enviado para o clique ${click_record_id}.`);
                }
            } catch (backgroundError) {
                console.error("Erro em tarefa de segundo plano (registerClick):", backgroundError.message);
            }
        })();

    } catch (error) {
        console.error("Erro ao registrar clique:", error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Erro interno do servidor.' });
        }
    }
});
app.post('/api/click/info', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id } = req.body;
    if (!apiKey || !click_id) return res.status(400).json({ message: 'API Key e click_id são obrigatórios.' });
    
    try {
        const sellerResult = await sql`SELECT id, email FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            console.warn(`[CLICK INFO] Tentativa de consulta com API Key inválida: ${apiKey}`);
            return res.status(401).json({ message: 'API Key inválida.' });
        }
        
        const seller_id = sellerResult[0].id;
        const seller_email = sellerResult[0].email;
        
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        
        const clickResult = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller_id}`;
        
        if (clickResult.length === 0) {
            console.warn(`[CLICK INFO NOT FOUND] Vendedor (ID: ${seller_id}, Email: ${seller_email}) tentou consultar o click_id "${click_id}", mas não foi encontrado.`);
            return res.status(404).json({ message: 'Click ID não encontrado para este vendedor.' });
        }
        
        const clickInfo = clickResult[0];
        res.status(200).json({ status: 'success', city: clickInfo.city, state: clickInfo.state });

    } catch (error) {
        console.error("Erro ao consultar informações do clique:", error);
        res.status(500).json({ message: 'Erro interno ao consultar informações do clique.' });
    }
});
app.get('/api/dashboard/metrics', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        let { startDate, endDate } = req.query;
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

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

        const userTimezone = 'America/Sao_Paulo'; 
        const dailyRevenueQuery = hasDateFilter
             ? sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate} GROUP BY 1 ORDER BY 1 ASC`
             : sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' GROUP BY 1 ORDER BY 1 ASC`;
        
        const [
               totalClicksResult, pixGeneratedResult, pixPaidResult, botsPerformance,
               clicksByState, dailyRevenue
        ] = await Promise.all([
              totalClicksQuery, pixGeneratedQuery, pixPaidQuery, botsPerformanceQuery,
              clicksByStateQuery, dailyRevenueQuery
        ]);

        const totalClicks = totalClicksResult[0].count;
        const totalPixGenerated = pixGeneratedResult[0].total;
        const totalRevenue = pixGeneratedResult[0].revenue;
        const totalPixPaid = pixPaidResult[0].total;
        const paidRevenue = pixPaidResult[0].revenue;
        
        res.status(200).json({
            total_clicks: parseInt(totalClicks),
            total_pix_generated: parseInt(totalPixGenerated),
            total_pix_paid: parseInt(totalPixPaid),
            total_revenue: parseFloat(totalRevenue),
            paid_revenue: parseFloat(paidRevenue),
            bots_performance: botsPerformance.map(b => ({ ...b, total_clicks: parseInt(b.total_clicks), total_pix_paid: parseInt(b.total_pix_paid), paid_revenue: parseFloat(b.paid_revenue) })),
            clicks_by_state: clicksByState.map(s => ({ ...s, total_clicks: parseInt(s.total_clicks) })),
            daily_revenue: dailyRevenue.map(d => ({ date: d.date.toISOString().split('T')[0], revenue: parseFloat(d.revenue) }))
        });
    } catch (error) {
        console.error("Erro ao buscar métricas do dashboard:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
app.get('/api/transactions', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const transactions = await sql`
            SELECT pt.status, pt.pix_value, COALESCE(tb.bot_name, ch.name, 'Checkout') as source_name, pt.provider, pt.created_at
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            LEFT JOIN pressels p ON c.pressel_id = p.id LEFT JOIN telegram_bots tb ON p.bot_id = tb.id
            LEFT JOIN checkouts ch ON c.checkout_id = ch.id WHERE c.seller_id = ${sellerId}
            ORDER BY pt.created_at DESC;`;
        res.status(200).json(transactions);
    } catch (error) {
        console.error("Erro ao buscar transações:", error);
        res.status(500).json({ message: 'Erro ao buscar dados das transações.' });
    }
});
app.post('/api/pix/generate', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id, value_cents, customer, product } = req.body;
    
    if (!apiKey || !click_id || !value_cents) return res.status(400).json({ message: 'API Key, click_id e value_cents são obrigatórios.' });

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) return res.status(401).json({ message: 'API Key inválida.' });

        if (adminSubscription) {
            const payload = JSON.stringify({
                title: 'PIX Gerado',
                body: `Um PIX de R$ ${(value_cents / 100).toFixed(2)} foi gerado por ${seller.name}.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(err => console.error(err));
        }

        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        
        const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller.id}`;
        if (!click) return res.status(404).json({ message: 'Click ID não encontrado.' });
        
        const ip_address = click.ip_address;
        
        const providerOrder = [ seller.pix_provider_primary, seller.pix_provider_secondary, seller.pix_provider_tertiary ].filter(Boolean);
        let lastError = null;

        for (const provider of providerOrder) {
            try {
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, apiKey, ip_address);
                const [transaction] = await sql`INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, qr_code_base64, provider, provider_transaction_id, pix_id) VALUES (${click.id}, ${value_cents / 100}, ${pixResult.qr_code_text}, ${pixResult.qr_code_base64}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id}) RETURNING id`;
                
                if (click.pressel_id) {
                    await sendMetaEvent('InitiateCheckout', click, { id: transaction.id, pix_value: value_cents / 100 }, null);
                }

                const customerDataForUtmify = customer || { name: "Cliente Interessado", email: "cliente@email.com" };
                const productDataForUtmify = product || { id: "prod_1", name: "Produto Ofertado" };
                await sendEventToUtmify('waiting_payment', click, { provider_transaction_id: pixResult.transaction_id, pix_value: value_cents / 100, created_at: new Date() }, seller, customerDataForUtmify, productDataForUtmify);
                
                return res.status(200).json(pixResult);
            } catch (error) {
                console.error(`[PIX GENERATE FALLBACK] Falha ao gerar PIX com ${provider}:`, error.response?.data || error.message);
                lastError = error;
            }
        }

        console.error(`[PIX GENERATE FINAL ERROR] Seller ID: ${seller?.id}, Email: ${seller?.email} - Todas as tentativas falharam. Último erro:`, lastError?.message || lastError);
        return res.status(500).json({ message: 'Não foi possível gerar o PIX. Todos os provedores falharam.' });

    } catch (error) {
        console.error(`[PIX GENERATE ERROR] Erro geral na rota:`, error.message);
        res.status(500).json({ message: 'Erro interno ao processar a geração de PIX.' });
    }
});
app.get('/api/pix/status/:transaction_id', async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { transaction_id } = req.params;

    if (!apiKey) return res.status(401).json({ message: 'API Key não fornecida.' });
    if (!transaction_id) return res.status(400).json({ message: 'ID da transação é obrigatório.' });

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) {
            return res.status(401).json({ message: 'API Key inválida.' });
        }
        
        const [transaction] = await sql`
            SELECT pt.* FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            WHERE (pt.provider_transaction_id = ${transaction_id} OR pt.pix_id = ${transaction_id}) AND c.seller_id = ${seller.id}`;

        if (!transaction) {
            return res.status(404).json({ status: 'not_found', message: 'Transação não encontrada.' });
        }
        
        if (transaction.status === 'paid') {
            return res.status(200).json({ status: 'paid' });
        }
        
        if (transaction.provider === 'oasyfy' || transaction.provider === 'cnpay' || transaction.provider === 'brpix') {
            return res.status(200).json({ status: 'pending', message: 'Aguardando confirmação via webhook.' });
        }

        let providerStatus, customerData = {};
        try {
            if (transaction.provider === 'syncpay') {
                const syncPayToken = await getSyncPayAuthToken(seller);
                const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                    headers: { 'Authorization': `Bearer ${syncPayToken}` }
                });
                providerStatus = response.data.status;
                customerData = response.data.payer;
            } else if (transaction.provider === 'pushinpay') {
                const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                providerStatus = response.data.status;
                customerData = { name: response.data.payer_name, document: response.data.payer_document };
            }
        } catch (providerError) {
             console.error(`Falha ao consultar o provedor para a transação ${transaction.id}:`, providerError.message);
             return res.status(200).json({ status: 'pending' });
        }

        if (providerStatus === 'paid' || providerStatus === 'COMPLETED') {
            await handleSuccessfulPayment(transaction.id, customerData);
            return res.status(200).json({ status: 'paid' });
        }

        res.status(200).json({ status: 'pending' });

    } catch (error) {
        console.error("Erro ao consultar status da transação:", error);
        res.status(500).json({ message: 'Erro interno ao consultar o status.' });
    }
});
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
        
        const value_cents = 3333;
        
        const startTime = Date.now();
        const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
        const endTime = Date.now();
        const responseTime = ((endTime - startTime) / 1000).toFixed(2);

        res.status(200).json({
            provider: provider.toUpperCase(),
            acquirer: pixResult.acquirer,
            responseTime: responseTime,
            qr_code_text: pixResult.qr_code_text
        });

    } catch (error) {
        console.error(`[PIX TEST ERROR] Seller ID: ${sellerId}, Provider: ${provider} - Erro:`, error.response?.data || error.message);
        res.status(500).json({ 
            message: `Falha ao gerar PIX de teste com ${provider.toUpperCase()}. Verifique as credenciais.`, 
            details: error.response?.data ? JSON.stringify(error.response.data) : error.message 
        });
    }
});
app.post('/api/pix/test-priority-route', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    let testLog = [];

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        const providerOrder = [
            { name: seller.pix_provider_primary, position: 'Primário' },
            { name: seller.pix_provider_secondary, position: 'Secundário' },
            { name: seller.pix_provider_tertiary, position: 'Terciário' }
        ].filter(p => p.name); 

        if (providerOrder.length === 0) {
            return res.status(400).json({ message: 'Nenhuma ordem de prioridade de provedores foi configurada.' });
        }

        const value_cents = 3333;

        for (const providerInfo of providerOrder) {
            const provider = providerInfo.name;
            const position = providerInfo.position;
            
            try {
                const startTime = Date.now();
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
                const endTime = Date.now();
                const responseTime = ((endTime - startTime) / 1000).toFixed(2);

                testLog.push(`SUCESSO com Provedor ${position} (${provider.toUpperCase()}).`);
                return res.status(200).json({
                    success: true, position: position, provider: provider.toUpperCase(),
                    acquirer: pixResult.acquirer, responseTime: responseTime,
                    qr_code_text: pixResult.qr_code_text, log: testLog
                });

            } catch (error) {
                let errorMessage = error.message;
                if (error.response && error.response.data) {
                    errorMessage = JSON.stringify(error.response.data);
                }
                console.error(`Falha no provedor ${position} (${provider}):`, error.response?.data || error.message);
                testLog.push(`FALHA com Provedor ${position} (${provider.toUpperCase()}): ${errorMessage}`);
            }
        }

        console.error("Todos os provedores na rota de prioridade falharam.");
        return res.status(500).json({
            success: false, message: 'Todos os provedores configurados na sua rota de prioridade falharam.',
            log: testLog
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
//          MOTOR DE FLUXO E WEBHOOK DO TELEGRAM (VERSÃO FINAL)
// ==========================================================
function findNextNode(currentNodeId, handleId, edges) {
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null;
}

async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, {
            chat_id: chatId,
            action: 'typing',
        });
    } catch (error) {
        console.warn(`[Flow Engine] Falha ao enviar ação 'typing' para ${chatId}:`, error.response?.data || error.message);
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping) {
    if (!text || text.trim() === '') return;
    const apiUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    try {
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            let typingDuration = text.length * 50;
            typingDuration = Math.max(500, typingDuration);
            typingDuration = Math.min(2000, typingDuration);
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }

        const response = await axios.post(apiUrl, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        
        if (response.data.ok) {
            const sentMessage = response.data.result;
            const [botInfo] = await sql`SELECT bot_name FROM telegram_bots WHERE id = ${botId}`;
            const botName = botInfo ? botInfo.bot_name : 'Bot';

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

async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    console.log(`[Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);
    const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    if (!flow || !flow.nodes) {
        console.log(`[Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
        return;
    }

    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

    let currentNodeId = startNodeId;
    let variables = initialVariables;

    if (!currentNodeId) {
        const [userState] = await sql`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        if (userState && userState.waiting_for_input) {
            console.log(`[Flow Engine] Usuário ${chatId} respondeu. Continuando do nó ${userState.current_node_id} pelo caminho 'com resposta'.`);
            currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
            variables = userState.variables;
        } else {
            console.log(`[Flow Engine] Iniciando novo fluxo para ${chatId} a partir do gatilho.`);
            const startNode = nodes.find(node => node.type === 'trigger');
            if (startNode) {
                currentNodeId = findNextNode(startNode.id, null, edges);
            }
        }
    }

    if (!currentNodeId) {
        console.log(`[Flow Engine] Fim do fluxo ou nenhum nó inicial encontrado para ${chatId}.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        return;
    }

    let safetyLock = 0;
    while (currentNodeId && safetyLock < 20) {
        const currentNode = nodes.find(node => node.id === currentNodeId);
        if (!currentNode) {
            console.error(`[Flow Engine] Erro: Nó ${currentNodeId} não encontrado no fluxo.`);
            break;
        }

        await sql`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET current_node_id = EXCLUDED.current_node_id, variables = EXCLUDED.variables, waiting_for_input = false;
        `;

        switch (currentNode.type) {
            case 'message':
                if (currentNode.data.typingDelay && currentNode.data.typingDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, currentNode.data.typingDelay * 1000));
                }
                await sendMessage(chatId, currentNode.data.text, botToken, sellerId, botId, currentNode.data.showTyping);

                if (currentNode.data.waitForReply) {
                    await sql`UPDATE user_flow_states SET waiting_for_input = true WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    const timeoutMinutes = currentNode.data.replyTimeout || 5;
                    const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                    
                    if(noReplyNodeId){
                        console.log(`[Flow Engine] Agendando timeout de ${timeoutMinutes} min para o nó ${noReplyNodeId}`);
                        await sql`
                            INSERT INTO flow_timeouts (chat_id, bot_id, execute_at, target_node_id, variables)
                            VALUES (${chatId}, ${botId}, NOW() + INTERVAL '${timeoutMinutes} minutes', ${noReplyNodeId}, ${JSON.stringify(variables)})
                        `;
                    }
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
                    const pixResult = await generatePixForProvider(provider, seller, valueInCents, 'novaapi-one.vercel.app', seller.api_key, ip_address);
                    
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
                console.warn(`[Flow Engine] Tipo de nó desconhecido: ${currentNode.type}. Parando fluxo.`);
                currentNodeId = null;
                break;
        }

        if (!currentNodeId) {
            const pendingTimeouts = await sql`SELECT 1 FROM flow_timeouts WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if(pendingTimeouts.length === 0){
                 await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            }
        }
        safetyLock++;
    }
}

app.post('/api/webhook/telegram/:botId', async (req, res) => {
    const { botId } = req.params;
    const body = req.body;
    res.sendStatus(200);

    try {
        const message = body.message;
        const chatId = message?.chat?.id;
        if (!chatId || !message.text) return;
        
        await sql`DELETE FROM flow_timeouts WHERE chat_id = ${chatId} AND bot_id = ${botId}`;

        const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${botId}`;
        if (!bot) {
            console.warn(`[Webhook] Webhook recebido para botId não encontrado: ${botId}`);
            return;
        }
        
        const { seller_id: sellerId, bot_token: botToken } = bot;
        
        const text = message.text;
        const isStartCommand = text.startsWith('/start ');
        const clickIdValue = isStartCommand ? text : null;

        await sql`
            INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, click_id, message_text, sender_type)
            VALUES (${sellerId}, ${botId}, ${chatId}, ${message.message_id}, ${message.from.id}, ${message.from.first_name}, ${message.from.last_name || null}, ${message.from.username || null}, ${clickIdValue}, ${text}, 'user')
            ON CONFLICT (chat_id, message_id) DO NOTHING;
        `;
        
        let initialVars = {};
        if (isStartCommand) {
            initialVars.click_id = clickIdValue;
        }
        
        await processFlow(chatId, botId, botToken, sellerId, null, initialVars);

    } catch (error) {
        console.error("Erro CRÍTICO ao processar webhook do Telegram:", error);
    }
});


app.get('/api/dispatches', authenticateJwt, async (req, res) => {
    try {
        const dispatches = await sql`SELECT * FROM mass_sends WHERE seller_id = ${req.user.id} ORDER BY sent_at DESC;`;
        res.status(200).json(dispatches);
    } catch (error) {
        console.error("Erro ao buscar histórico de disparos:", error);
        res.status(500).json({ message: 'Erro ao buscar histórico.' });
    }
});
app.get('/api/dispatches/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        const details = await sql`
            SELECT d.*, u.first_name, u.username
            FROM mass_send_details d
            LEFT JOIN telegram_chats u ON d.chat_id = u.chat_id
            WHERE d.mass_send_id = ${id}
            ORDER BY d.sent_at;
        `;
        res.status(200).json(details);
    } catch (error) {
        console.error("Erro ao buscar detalhes do disparo:", error);
        res.status(500).json({ message: 'Erro ao buscar detalhes.' });
    }
});
app.post('/api/bots/mass-send', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { botIds, flowType, initialText, ctaButtonText, pixValue, externalLink, imageUrl } = req.body;

    if (!botIds || botIds.length === 0 || !initialText || !ctaButtonText) {
        return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    }

    try {
        const bots = await sql`SELECT id, bot_token FROM telegram_bots WHERE id = ANY(${botIds}) AND seller_id = ${sellerId}`;
        if (bots.length === 0) return res.status(404).json({ message: 'Nenhum bot válido selecionado.' });
        
        const users = await sql`SELECT DISTINCT ON (chat_id) chat_id, bot_id FROM telegram_chats WHERE bot_id = ANY(${botIds}) AND seller_id = ${sellerId}`;
        if (users.length === 0) return res.status(404).json({ message: 'Nenhum usuário encontrado para os bots selecionados.' });

        const [log] = await sql`INSERT INTO mass_sends (seller_id, message_content, button_text, button_url, image_url) VALUES (${sellerId}, ${initialText}, ${ctaButtonText}, ${externalLink || null}, ${imageUrl || null}) RETURNING id;`;
        const logId = log.id;
        
        res.status(202).json({ message: `Disparo agendado para ${users.length} usuários.`, logId });
        
        (async () => {
            let successCount = 0, failureCount = 0;
            const botTokenMap = new Map(bots.map(b => [b.id, b.bot_token]));

            for (const user of users) {
                const botToken = botTokenMap.get(user.bot_id);
                if (!botToken) continue;

                const endpoint = imageUrl ? 'sendPhoto' : 'sendMessage';
                const apiUrl = `https://api.telegram.org/bot${botToken}/${endpoint}`;
                let payload;

                if (flowType === 'pix_flow') {
                    const valueInCents = Math.round(parseFloat(pixValue) * 100);
                    const callback_data = `generate_pix|${valueInCents}`;
                    payload = { chat_id: user.chat_id, caption: initialText, text: initialText, photo: imageUrl, parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: ctaButtonText, callback_data }]] } };
                } else {
                    payload = { chat_id: user.chat_id, caption: initialText, text: initialText, photo: imageUrl, parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: ctaButtonText, url: externalLink }]] } };
                }
                
                if (!imageUrl) { delete payload.photo; delete payload.caption; } else { delete payload.text; }

                try {
                    await axios.post(apiUrl, payload, { timeout: 10000 });
                    successCount++;
                    await sql`INSERT INTO mass_send_details (mass_send_id, chat_id, status) VALUES (${logId}, ${user.chat_id}, 'success')`;
                } catch (error) {
                    failureCount++;
                    const errorMessage = error.response?.data?.description || error.message;
                    console.error(`Falha ao enviar para ${user.chat_id}: ${errorMessage}`);
                    await sql`INSERT INTO mass_send_details (mass_send_id, chat_id, status, details) VALUES (${logId}, ${user.chat_id}, 'failure', ${errorMessage})`;
                }
                await new Promise(resolve => setTimeout(resolve, 300));
            }

            await sql`UPDATE mass_sends SET success_count = ${successCount}, failure_count = ${failureCount} WHERE id = ${logId};`;
            console.log(`Disparo ${logId} concluído. Sucessos: ${successCount}, Falhas: ${failureCount}`);
        })();

    } catch (error) {
        console.error("Erro no disparo em massa:", error);
        if (!res.headersSent) res.status(500).json({ message: 'Erro ao iniciar o disparo.' });
    }
});
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
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0299)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0299))) },
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
async function checkPendingTransactions() {
    try {
        const pendingTransactions = await sql`
            SELECT id, provider, provider_transaction_id, click_id_internal, status
            FROM pix_transactions WHERE status = 'pending' AND created_at > NOW() - INTERVAL '30 minutes'`;

        if (pendingTransactions.length === 0) return;
        
        for (const tx of pendingTransactions) {
            if (tx.provider === 'oasyfy' || tx.provider === 'cnpay' || tx.provider === 'brpix') {
                continue;
            }

            try {
                const [seller] = await sql`
                    SELECT *
                    FROM sellers s JOIN clicks c ON c.seller_id = s.id
                    WHERE c.id = ${tx.click_id_internal}`;
                if (!seller) continue;

                let providerStatus, customerData = {};
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
                
                if ((providerStatus === 'paid' || providerStatus === 'COMPLETED') && tx.status !== 'paid') {
                     await handleSuccessfulPayment(tx.id, customerData);
                }
            } catch (error) {
                if (!error.response || error.response.status !== 404) {
                    console.error(`Erro ao verificar transação ${tx.id} (${tx.provider}):`, error.response?.data || error.message);
                }
            }
            await new Promise(resolve => setTimeout(resolve, 200)); 
        }
    } catch (error) {
        console.error("Erro na rotina de verificação geral:", error.message);
    }
}

app.post('/api/webhook/syncpay', async (req, res) => {
    try {
        const notification = req.body;
        console.log('[Webhook SyncPay] Notificação recebida:', JSON.stringify(notification, null, 2));

        if (!notification.data) {
            console.log('[Webhook SyncPay] Webhook ignorado: formato inesperado, objeto "data" não encontrado.');
            return res.sendStatus(200);
        }

        const transactionData = notification.data;
        const transactionId = transactionData.id;
        const status = transactionData.status;
        const customer = transactionData.client;

        if (!transactionId || !status) {
            console.log('[Webhook SyncPay] Ignorado: "id" ou "status" não encontrados dentro do objeto "data".');
            return res.sendStatus(200);
        }

        if (String(status).toLowerCase() === 'completed') {
            
            console.log(`[Webhook SyncPay] Processando pagamento para transação: ${transactionId}`);
            
            const [tx] = await sql`
                SELECT * FROM pix_transactions 
                WHERE provider_transaction_id = ${transactionId} AND provider = 'syncpay'
            `;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook SyncPay] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
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
    const { event, data } = req.body;
    console.log('[Webhook BRPix] Notificação recebida:', JSON.stringify(req.body, null, 2));

    if (event === 'transaction.updated' && data?.status === 'paid') {
        const transactionId = data.id;
        const customer = data.customer;

        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'brpix'`;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook BRPix] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document?.number });
            } else if (tx) {
                console.log(`[Webhook BRPix] Transação ${tx.id} já estava como 'paga'.`);
            } else {
                 console.warn(`[Webhook BRPix] AVISO: Transação com ID ${transactionId} não foi encontrada no banco de dados.`);
            }
        } catch (error) {
            console.error("Erro CRÍTICO no webhook da BRPix:", error);
        }
    }

    res.sendStatus(200);
});

module.exports = app;
