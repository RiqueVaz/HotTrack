// /backend/worker/process-disparo.js
// Este worker não tem a função processFlow, a correção não é aplicável aqui. disparo em massa.

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../../.env' });
}

const axios = require('axios');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
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
        const [bot] = await sqlWithRetry(sqlTx`SELECT seller_id, bot_token, telegram_supergroup_id FROM telegram_bots WHERE id = ${bot_id}`);
        if (!bot || !bot.bot_token) {
          throw new Error(`[WORKER-DISPARO] Bot com ID ${bot_id} não encontrado ou sem token.`);
        }
         
        const [seller] = await sqlWithRetry(sqlTx`SELECT * FROM sellers WHERE id = ${bot.seller_id}`);
            if (!seller) {
                throw new Error(`[WORKER-DISPARO] Vendedor com ID ${bot.seller_id} não encontrado.`);
            }

            // Busca o click_id mais recente para garantir que está atualizado
            const [chat] = await sqlWithRetry(sqlTx`
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
                logger.error(`[WORKER-DISPARO] Arquivo da biblioteca não encontrado: mediaLibraryId ${step.mediaLibraryId}`);
                throw new Error(`Arquivo da biblioteca não encontrado: ${step.mediaLibraryId}`);
              }
            } catch (error) {
              logger.error(`[WORKER-DISPARO] Erro ao buscar arquivo da biblioteca:`, error);
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
        } else if (step.type === 'action_create_invite_link') {
            if (!bot.telegram_supergroup_id) {
                throw new Error('Supergrupo não configurado para este bot.');
            }

            const normalizedChatId = normalizeChatIdentifier(bot.telegram_supergroup_id);
            if (!normalizedChatId) {
                throw new Error('ID do supergrupo inválido para criação de convite.');
            }

            const userToUnban = step.userId || chat_id;
            const normalizedUserId = normalizeChatIdentifier(userToUnban);

            try {
                const unbanResponse = await sendTelegramRequest(
                    bot.bot_token,
                    'unbanChatMember',
                    {
                        chat_id: normalizedChatId,
                        user_id: normalizedUserId,
                        only_if_banned: true
                    }
                );
                if (unbanResponse?.ok) {
                    logger.debug(`[WORKER-DISPARO] Usuário ${userToUnban} desbanido antes da criação do convite.`);
                } else if (unbanResponse && !unbanResponse.ok) {
                    const desc = (unbanResponse.description || '').toLowerCase();
                    if (desc.includes("can't remove chat owner")) {
                        logger.debug(`[WORKER-DISPARO] Tentativa de desbanir o proprietário do grupo ignorada.`);
                    } else {
                        logger.warn(`[WORKER-DISPARO] Não foi possível desbanir usuário ${userToUnban}: ${unbanResponse.description}`);
                    }
                }
            } catch (unbanError) {
                const message = (unbanError?.message || '').toLowerCase();
                if (message.includes("can't remove chat owner")) {
                    logger.debug(`[WORKER-DISPARO] Tentativa de desbanir o proprietário do grupo ignorada.`);
                } else {
                    logger.warn(`[WORKER-DISPARO] Erro ao tentar desbanir usuário ${userToUnban}:`, unbanError.message);
                }
            }

            const inviteNameRaw = (step.linkName || `Convite_${chat_id}_${Date.now()}`).toString().trim();
            const inviteName = inviteNameRaw ? inviteNameRaw.slice(0, 32) : `Convite_${Date.now()}`;

            const invitePayload = {
                chat_id: normalizedChatId,
                name: inviteName,
                member_limit: 1,
                creates_join_request: false
            };

            if (step.expireMinutes) {
                invitePayload.expire_date = Math.floor(Date.now() / 1000) + (parseInt(step.expireMinutes, 10) * 60);
            }

            const inviteResponse = await sendTelegramRequest(bot.bot_token, 'createChatInviteLink', invitePayload);
            if (!inviteResponse.ok) {
                throw new Error(inviteResponse.description || 'Falha ao criar link de convite.');
            }

            userVariables.invite_link = inviteResponse.result.invite_link;
            userVariables.invite_link_name = inviteResponse.result.name;
            userVariables.invite_link_single_use = true;
            userVariables.user_was_banned = false;
            userVariables.banned_user_id = undefined;

            const buttonText = (step.buttonText || DEFAULT_INVITE_BUTTON_TEXT).trim() || DEFAULT_INVITE_BUTTON_TEXT;
            const template = (step.messageText || step.text || DEFAULT_INVITE_MESSAGE).trim() || DEFAULT_INVITE_MESSAGE;
            const messageText = await replaceVariables(template, userVariables);
            const messagePayload = {
                chat_id: chat_id,
                text: messageText,
                parse_mode: 'HTML',
                reply_markup: {
                    inline_keyboard: [[{ text: buttonText, url: inviteResponse.result.invite_link }]]
                }
            };

            const messageResponse = await sendTelegramRequest(bot.bot_token, 'sendMessage', messagePayload);
            if (messageResponse.ok) {
                await saveMessageToDb(bot.seller_id, bot_id, messageResponse.result, 'bot', userVariables);
                response = messageResponse;
            } else {
                throw new Error(messageResponse.description || 'Falha ao enviar mensagem do convite.');
            }

            logger.debug(`[WORKER-DISPARO] Link de convite criado: ${userVariables.invite_link}`);

        } else if (step.type === 'action_remove_user_from_group') {
            if (!bot.telegram_supergroup_id) {
                throw new Error('Supergrupo não configurado para este bot.');
            }

            const normalizedChatId = normalizeChatIdentifier(bot.telegram_supergroup_id);
            if (!normalizedChatId) {
                throw new Error('ID do supergrupo inválido para banimento.');
            }

            const handleOwnerBanRestriction = () => {
                logger.debug(`[WORKER-DISPARO] Tentativa de banir o proprietário do grupo ignorada.`);
                userVariables.user_was_banned = false;
                userVariables.banned_user_id = undefined;
            };

            const userToRemove = step.userId || chat_id;
            const normalizedUserId = normalizeChatIdentifier(userToRemove);

            let banResponse;
            try {
                banResponse = await sendTelegramRequest(
                    bot.bot_token,
                    'banChatMember',
                    {
                        chat_id: normalizedChatId,
                        user_id: normalizedUserId,
                        revoke_messages: step.deleteMessages || false
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
                    response = null;
                } else {
                    throw banError;
                }
            }

            if (banResponse?.ok) {
                logger.debug(`[WORKER-DISPARO] Usuário ${userToRemove} removido e banido do grupo.`);
                userVariables.user_was_banned = true;
                userVariables.banned_user_id = userToRemove;
                userVariables.last_ban_at = new Date().toISOString();

                const linkToRevoke = step.inviteLink || userVariables.invite_link;
                if (linkToRevoke) {
                    try {
                        const revokeResponse = await sendTelegramRequest(
                            bot.bot_token,
                            'revokeChatInviteLink',
                            {
                                chat_id: normalizedChatId,
                                invite_link: linkToRevoke
                            }
                        );
                        if (revokeResponse.ok) {
                            logger.debug(`[WORKER-DISPARO] Link de convite revogado: ${linkToRevoke}`);
                            userVariables.invite_link_revoked = true;
                            delete userVariables.invite_link;
                            delete userVariables.invite_link_name;
                        } else {
                            logger.warn(`[WORKER-DISPARO] Falha ao revogar link ${linkToRevoke}: ${revokeResponse.description}`);
                        }
                    } catch (revokeError) {
                        logger.warn(`[WORKER-DISPARO] Erro ao revogar link ${linkToRevoke}:`, revokeError.message);
                    }
                }

                if (step.sendMessage) {
                    const messageText = await replaceVariables(
                        step.messageText || 'Você foi removido do grupo.',
                        userVariables
                    );
                    const messageResponse = await sendTelegramRequest(bot.bot_token, 'sendMessage', {
                        chat_id: chat_id,
                        text: messageText,
                        parse_mode: 'HTML'
                    });
                    if (messageResponse.ok) {
                        await saveMessageToDb(bot.seller_id, bot_id, messageResponse.result, 'bot', userVariables);
                        response = messageResponse;
                    } else {
                        throw new Error(messageResponse.description || 'Falha ao enviar mensagem pós-banimento.');
                    }
                } else {
                    response = null;
                }
            } else if (banResponse) {
                const desc =
                    (banResponse.description || '').toLowerCase();
                if (desc.includes("can't remove chat owner")) {
                    handleOwnerBanRestriction();
                    response = null;
                } else {
                    throw new Error(banResponse.description || 'Falha ao remover usuário.');
                }
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
          logger.error(`[WORKER-DISPARO] Falha ao processar job para chat ${chat_id}: ${e.message}`);
        }
    
        // --- LÓGICA DE CONCLUSÃO ---
        try {
          // 1. Loga o resultado deste job
          await sqlWithRetry(
            sqlTx`INSERT INTO disparo_log (history_id, chat_id, bot_id, status, details, transaction_id) 
                       VALUES (${history_id}, ${chat_id}, ${bot_id}, ${logStatus}, ${logDetails}, ${lastTransactionId})`
          );
    
          // 2. Atualiza a contagem de falhas (se houver) e de processados
            let query;
            if (logStatus === 'FAILED') {
                query = sqlTx`UPDATE disparo_history
                            SET processed_jobs = processed_jobs + 1,
                                failure_count = failure_count + 1
                            WHERE id = ${history_id}
                            RETURNING processed_jobs, total_jobs, status`;
            } else {
                query = sqlTx`UPDATE disparo_history
                            SET processed_jobs = processed_jobs + 1
                            WHERE id = ${history_id}
                            RETURNING processed_jobs, total_jobs, status`;
            }
          const [history] = await sqlWithRetry(query);
    
          // 3. Verifica se a campanha terminou
          if (history && history.status === 'RUNNING' && history.processed_jobs >= history.total_jobs) {
            logger.info(`[WORKER-DISPARO] Campanha ${history_id} concluída! Marcando como COMPLETED.`);
            await sqlWithRetry(
              sqlTx`UPDATE disparo_history SET status = 'COMPLETED' WHERE id = ${history_id}`
            );
          }
        } catch (dbError) {
            logger.error(`[WORKER-DISPARO] FALHA CRÍTICA ao logar no DB (History ${history_id}):`, dbError);
        }
            // --- FIM DA LÓGICA DE CONCLUSÃO ---

            res.status(200).send('Worker de disparo finalizado.');
    } catch (error) {

        logger.error('[WORKER-DISPARO] Erro crítico ao processar job:', error);
        // Tenta logar a falha mesmo se o processamento principal quebrar
        try {
             await sqlWithRetry(
                sqlTx`INSERT INTO disparo_log (history_id, chat_id, bot_id, status, details) 
                   VALUES (${history_id || 0}, ${chat_id || 0}, ${bot_id || 0}, 'FAILED', ${error.message.substring(0, 255)})`
            );
        } catch(logFailError) {
            logger.error('[WORKER-DISPARO] Falha ao logar a falha crítica:', logFailError);
        }
        res.status(500).send('Erro interno no worker de disparo.');
    
    }
}

module.exports = handler;