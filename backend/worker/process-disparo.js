// /backend/worker/process-disparo.js
// Este worker não tem a função processFlow, a correção não é aplicável aqui. disparo em massa.

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const { neon } = require('@neondatabase/serverless');
const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');

// ==========================================================
//                   INICIALIZAÇÃO
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
//          FUNÇÕES AUXILIARES (Copiadas do backend.js)
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
            const chatId = data instanceof FormData ? data.getBoundary && data.get('chat_id') : data.chat_id;
            if (error.response && error.response.status === 403) {
                console.warn(`[WORKER-DISPARO] Bot bloqueado. ChatID: ${chatId}`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento específico para TOPIC_CLOSED
            if (error.response && error.response.status === 400 && 
                error.response.data?.description?.includes('TOPIC_CLOSED')) {
                console.warn(`[WORKER-DISPARO] Chat de grupo fechado. ChatID: ${chatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }
            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');
            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }
            console.error(`[WORKER-DISPARO - Telegram API ERROR] Method: ${method}, ChatID: ${chatId}:`, error.response?.data || error.message);
            throw error;
        }
    }
}

async function saveMessageToDb(sellerId, botId, message, senderType, variables = {}) {
    const { message_id, chat, from, text, photo, video, voice } = message;
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

    // CORREÇÃO FINAL: Salva NULL para os dados do usuário quando o remetente é o bot.
    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (chat_id, message_id) DO NOTHING;
    `, [
        sellerId, botId, chat.id, message_id, fromUser.id, 
        senderType === 'user' ? fromUser.first_name : null, 
        senderType === 'user' ? fromUser.last_name : null, 
        senderType === 'user' ? fromUser.username : null, 
        messageText, senderType, mediaType, mediaFileId, 
        variables.click_id || null
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
    const commission_rate = seller.commission_rate || 0.0500;
    const clientPayload = { document: { number: "21376710773", type: "CPF" }, name: "Cliente Padrão", email: "gabriel@email.com", phone: "27995310379" };
    
    if (provider === 'brpix') {
        if (!seller.brpix_secret_key || !seller.brpix_company_id) { throw new Error('Credenciais da BR PIX não configuradas.'); }
        const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');
        const payload = { customer: clientPayload, items: [{ title: "Produto Digital", unitPrice: parseInt(value_cents, 10), quantity: 1 }], paymentMethod: "PIX", amount: parseInt(value_cents, 10), pix: { expiresInDays: 1 }, ip: ip_address };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && BRPIX_SPLIT_RECIPIENT_ID) { payload.split = [{ recipientId: BRPIX_SPLIT_RECIPIENT_ID, amount: commission_cents }]; }
        const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, { headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/json' } });
        pixData = response.data;
        acquirer = "BRPix";

        const brpixTransactionId = pixData?.transaction_id || pixData?.id;
        const qrCodeText = pixData?.pix?.qrcode || pixData?.pix?.qrcodeText || pixData?.pix?.qr_code || pixData?.pix?.qrCode;
        const qrCodeBase64 = pixData?.pix?.qr_code_base64 || pixData?.pix?.qrcode_base64 || pixData?.pix?.qrcode || pixData?.pix?.qr_code;

        return { qr_code_text: qrCodeText, qr_code_base64: qrCodeBase64, transaction_id: brpixTransactionId, acquirer, provider };
    } else if (provider === 'syncpay') {
        const token = await getSyncPayAuthToken(seller);
        const payload = { amount: value_cents / 100, payer: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" }, callbackUrl: `https://${host}/api/webhook/syncpay` };
        const commission_percentage = commission_rate * 100;
        if (apiKey !== ADMIN_API_KEY && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) { payload.split = [{ percentage: Math.round(commission_percentage), user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID }]; }
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/cash-in`, payload, { headers: { 'Authorization': `Bearer ${token}` } });
        pixData = response.data;
        acquirer = "SyncPay";
        return { qr_code_text: pixData.pix_code, qr_code_base64: null, transaction_id: pixData.identifier, acquirer, provider };
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
        acquirer = isCnpay ? "CNPay" : "Oasy.fy";
        return { qr_code_text: pixData.pix.code, qr_code_base64: pixData.pix.base64, transaction_id: pixData.transactionId, acquirer, provider };
    } else { // Padrão é PushinPay
        if (!seller.pushinpay_token) throw new Error(`Token da PushinPay não configurado.`);
        const payload = { value: value_cents, webhook_url: `https://${host}/api/webhook/pushinpay` };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && PUSHINPAY_SPLIT_ACCOUNT_ID) { payload.split_rules = [{ value: commission_cents, account_id: PUSHINPAY_SPLIT_ACCOUNT_ID }]; }
        const pushinpayResponse = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
        pixData = pushinpayResponse.data;
        acquirer = "Woovi";
        return { qr_code_text: pixData.qr_code, qr_code_base64: pixData.qr_code_base64, transaction_id: pixData.id, acquirer, provider: 'pushinpay' };
    }
}

async function generatePixWithFallback(seller, value_cents, host, apiKey, ip_address, click_id_internal) {
    const providerOrder = [seller.pix_provider_primary, seller.pix_provider_secondary, seller.pix_provider_tertiary].filter(Boolean);
    if (providerOrder.length === 0) { throw new Error('Nenhum provedor de PIX configurado.'); }
    let lastError = null;
    for (const provider of providerOrder) {
        try {
            // Log removido por segurança
            const pixResult = await generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address);
            
            const [transaction] = await sql`
                INSERT INTO pix_transactions (click_id_internal, pix_value, qr_code_text, qr_code_base64, provider, provider_transaction_id, pix_id)
                VALUES (${click_id_internal}, ${value_cents / 100}, ${pixResult.qr_code_text}, ${pixResult.qr_code_base64}, ${pixResult.provider}, ${pixResult.transaction_id}, ${pixResult.transaction_id})
                RETURNING id`;
            // Log removido por segurança
            pixResult.internal_transaction_id = transaction.id;
            return pixResult;
        } catch (error) {
            console.error(`[WORKER-DISPARO - PIX Fallback] FALHA com ${provider.toUpperCase()}:`, error.response?.data?.message || error.message);
            lastError = error;
        }
    }
    const specificMessage = lastError.response?.data?.message || lastError.message || 'Todos os provedores de PIX falharam.';
    throw new Error(`Não foi possível gerar o PIX: ${specificMessage}`);
}


// ==========================================================
//           LÓGICA DO WORKER
// ==========================================================

async function handler(req, res) {
      const { history_id, chat_id, bot_id, step_json, variables_json } = req.body;
        // Log removido por segurança
    
      const step = JSON.parse(step_json);
      const userVariables = JSON.parse(variables_json);
       
      let logStatus = 'SENT';
        let logDetails = 'Enviado com sucesso.';
      let lastTransactionId = null;
    
      try {
        const [bot] = await sqlWithRetry(sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`);
        if (!bot || !bot.bot_token) {
          throw new Error(`[WORKER-DISPARO] Bot com ID ${bot_id} não encontrado ou sem token.`);
        }
         
        const [seller] = await sqlWithRetry(sql`SELECT * FROM sellers WHERE id = ${bot.seller_id}`);
            if (!seller) {
                throw new Error(`[WORKER-DISPARO] Vendedor com ID ${bot.seller_id} não encontrado.`);
            }

            // Busca o click_id mais recente para garantir que está atualizado
            const [chat] = await sqlWithRetry(sql`
                SELECT click_id FROM telegram_chats 
                WHERE chat_id = ${chat_id} AND bot_id = ${bot_id} AND click_id IS NOT NULL 
                ORDER BY created_at DESC LIMIT 1
            `);
            if (chat?.click_id) {
                userVariables.click_id = chat.click_id;
            }
    
          let response;
            const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
    
        try {
            if (step.type === 'message') {
                // (Lógica para enviar 'message' ... igual a antes)
          const textToSend = await replaceVariables(step.text, userVariables);
          let payload = { chat_id: chat_id, text: textToSend, parse_mode: 'HTML' };
          if (step.buttonText && step.buttonUrl) {
            payload.reply_markup = { inline_keyboard: [[{ text: step.buttonText, url: step.buttonUrl }]] };
          }
          response = await sendTelegramRequest(bot.bot_token, 'sendMessage', payload);
        } else if (['image', 'video', 'audio'].includes(step.type)) {
          const urlMap = { image: 'fileUrl', video: 'fileUrl', audio: 'fileUrl' };
          let fileIdentifier = step[urlMap[step.type]];
          const caption = await replaceVariables(step.caption, userVariables);
          
          // Se o step tem mediaLibraryId, busca o file_id da biblioteca
          if (step.mediaLibraryId) {
            try {
              const [media] = await sqlWithRetry(
                'SELECT file_id FROM media_library WHERE id = $1 LIMIT 1',
                [step.mediaLibraryId]
              );
              if (media && media.file_id) {
                fileIdentifier = media.file_id;
                // Log removido por segurança
              } else {
                console.error(`[WORKER-DISPARO] Arquivo da biblioteca não encontrado: mediaLibraryId ${step.mediaLibraryId}`);
                throw new Error(`Arquivo da biblioteca não encontrado: ${step.mediaLibraryId}`);
              }
            } catch (error) {
              console.error(`[WORKER-DISPARO] Erro ao buscar arquivo da biblioteca:`, error);
              throw error;
            }
          }
          
          if (!fileIdentifier) {
            throw new Error(`Nenhum file_id ou mediaLibraryId fornecido para o step ${step.type}`);
          }
          
          const isLibraryFile = fileIdentifier && (fileIdentifier.startsWith('BAAC') || fileIdentifier.startsWith('AgAC') || fileIdentifier.startsWith('AwAC'));
          if (isLibraryFile) {
            response = await sendMediaAsProxy(bot.bot_token, chat_id, fileIdentifier, step.type, caption);
          } else {
            const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[step.type];
            const field = { image: 'photo', video: 'video', audio: 'voice' }[step.type];
            const payload = { chat_id: chat_id, [field]: fileIdentifier, caption: caption, parse_mode: 'HTML' };
            response = await sendTelegramRequest(bot.bot_token, method, payload);
          }
            } else if (step.type === 'pix') {
                // Delegar ao endpoint central para garantir eventos (InitiateCheckout e waiting_payment)
                if (!userVariables.click_id) {
                    throw new Error(`Ignorando passo PIX para chat ${chat_id} por falta de click_id nas variáveis.`);
                }
                const baseApiUrl = process.env.HOTTRACK_API_URL;
                if (!baseApiUrl) {
                    throw new Error('HOTTRACK_API_URL não configurada no worker.');
                }
                const cleanedClickId = userVariables.click_id.startsWith('/start ')
                    ? userVariables.click_id.replace('/start ', '')
                    : userVariables.click_id;
                const apiResp = await axios.post(`${baseApiUrl}/api/pix/generate`, {
                    click_id: cleanedClickId,
                    value_cents: step.valueInCents
                }, {
                    headers: { 'x-api-key': seller.api_key }
                });
                const { transaction_id, qr_code_text } = apiResp.data;
                lastTransactionId = transaction_id;
                const messageText = await replaceVariables(step.pixMessage || "", userVariables);
                const buttonText = await replaceVariables(step.pixButtonText || "📋 Copiar", userVariables);
                const textToSend = `<pre>${qr_code_text}</pre>\n\n${messageText}`;
                response = await sendTelegramRequest(bot.bot_token, 'sendMessage', {
                    chat_id: chat_id, text: textToSend, parse_mode: 'HTML',
                    reply_markup: { inline_keyboard: [[{ text: buttonText, copy_text: { text: qr_code_text } }]] }
                });
        } else if (step.type === 'check_pix' || step.type === 'delay') {
                // Ignora ativamente esses passos, eles não enviam nada
                logStatus = 'SKIPPED';
                logDetails = `Passo ${step.type} ignorado pelo worker.`;
                response = { ok: true, result: { message_id: `skip_${Date.now()}`, chat: { id: chat_id }, from: { id: 'worker' } }};
            }
         
          if (response && response.ok) {
                    if (step.type !== 'delay' && step.type !== 'check_pix') {
              await saveMessageToDb(bot.seller_id, bot_id, response.result, 'bot', userVariables);
                    }
          } else if(response && !response.ok) {
            throw new Error(response.description || 'Falha no Telegram');
          }
        } catch(e) {
          logStatus = 'FAILED';
                logDetails = e.message.substring(0, 255); 
          console.error(`[WORKER-DISPARO] Falha ao processar job para chat ${chat_id}: ${e.message}`);
        }
    
        // --- LÓGICA DE CONCLUSÃO ---
        try {
          // 1. Loga o resultado deste job
          await sqlWithRetry(
            sql`INSERT INTO disparo_log (history_id, chat_id, bot_id, status, details, transaction_id) 
                       VALUES (${history_id}, ${chat_id}, ${bot_id}, ${logStatus}, ${logDetails}, ${lastTransactionId})`
          );
    
          // 2. Atualiza a contagem de falhas (se houver) e de processados
            let query;
            if (logStatus === 'FAILED') {
                query = sql`UPDATE disparo_history
                            SET processed_jobs = processed_jobs + 1,
                                failure_count = failure_count + 1
                            WHERE id = ${history_id}
                            RETURNING processed_jobs, total_jobs, status`;
            } else {
                query = sql`UPDATE disparo_history
                            SET processed_jobs = processed_jobs + 1
                            WHERE id = ${history_id}
                            RETURNING processed_jobs, total_jobs, status`;
            }
          const [history] = await sqlWithRetry(query);
    
          // 3. Verifica se a campanha terminou
          if (history && history.status === 'RUNNING' && history.processed_jobs >= history.total_jobs) {
            console.log(`[WORKER-DISPARO] Campanha ${history_id} concluída! Marcando como COMPLETED.`);
            await sqlWithRetry(
              sql`UPDATE disparo_history SET status = 'COMPLETED' WHERE id = ${history_id}`
            );
          }
        } catch (dbError) {
            console.error(`[WORKER-DISPARO] FALHA CRÍTICA ao logar no DB (History ${history_id}):`, dbError);
        }
            // --- FIM DA LÓGICA DE CONCLUSÃO ---
    
            res.status(200).send('Worker de disparo finalizado.');
    } catch (error) {
        console.error('[WORKER-DISPARO] Erro crítico ao processar job:', error);
        // Tenta logar a falha mesmo se o processamento principal quebrar
        try {
             await sqlWithRetry(
                sql`INSERT INTO disparo_log (history_id, chat_id, bot_id, status, details) 
                   VALUES (${history_id || 0}, ${chat_id || 0}, ${bot_id || 0}, 'FAILED', ${error.message.substring(0, 255)})`
            );
        } catch(logFailError) {
            console.error('[WORKER-DISPARO] Falha ao logar a falha crítica:', logFailError);
        }
        res.status(500).send('Erro interno no worker de disparo.');
    }
}

module.exports = handler;