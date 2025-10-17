// /api/worker/process-timeout.js

// Carrega as variáveis de ambiente. O caminho sobe dois níveis para chegar na raiz do projeto.
if (process.env.NODE_ENV !== 'production') {
    // ATENÇÃO: O caminho para o .env precisa subir 3 níveis a partir daqui
    require('dotenv').config({ path: '../../.env' });
  }

const { neon } = require('@neondatabase/serverless');
const { verifySignature } = require("@upstash/qstash/nextjs");
const axios = require('axios');
const FormData = require('form-data'); // Necessário para sendMediaAsProxy

// ==========================================================
//                  INICIALIZAÇÃO
// ==========================================================
const sql = neon(process.env.DATABASE_URL);

// ====================================================================================
// ATENÇÃO: As funções abaixo foram copiadas do seu arquivo principal.
// Se você fizer alterações no motor de fluxo (processFlow, sendMessage, etc.)
// no seu arquivo principal, lembre-se de atualizar as cópias aqui neste arquivo também.
// ====================================================================================

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

async function sendTelegramRequest(botToken, method, data, options = {}, retries = 3) {
    const { headers = {} } = options;
    const apiUrl = `https://api.telegram.org/bot${botToken}/${method}`;
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, { headers });
            return response.data;
        } catch (error) {
            if (i < retries - 1) {
                await new Promise(res => setTimeout(res, 1500 * (i + 1)));
                continue;
            }
            console.error(`[WORKER - Telegram API ERROR] Method: ${method}:`, error.response?.data || error.message);
            throw error;
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

    if (!method) throw new Error('Tipo de arquivo não suportado.');

    formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

    return await sendTelegramRequest(destinationBotToken, method, formData, { headers: formData.getHeaders() });
}

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
        console.warn(`[WORKER - Flow Engine] Falha ao enviar ação 'typing' para ${chatId}:`, error.response?.data || error.message);
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping) {
    if (!text || text.trim() === '') return;
    const apiUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    try {
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            let typingDuration = Math.max(500, Math.min(2000, text.length * 50));
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }

        const response = await axios.post(apiUrl, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        
        if (response.data.ok) {
            // Salvar a mensagem enviada pelo bot (lógica simplificada para o worker)
            const sentMessage = response.data.result;
            await sql`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, message_text, sender_type)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, 'Bot', '(Fluxo)', ${text}, 'bot')
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        console.error(`[WORKER - Flow Engine] Erro ao enviar/salvar mensagem para ${chatId}:`, error.response?.data || error.message);
    }
}

// COPIE AQUI A FUNÇÃO `processFlow` COMPLETA DO SEU ARQUIVO PRINCIPAL
// Exemplo (simplificado, cole a sua versão completa):
async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    console.log(`[WORKER - Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId}`);
    // ... cole sua lógica completa da função processFlow aqui ...
    // É crucial que esta função esteja completa e funcional.
}


// ==========================================================
//                      LÓGICA DO WORKER
// ==========================================================

async function handler(req, res) {
    try {
        // 1. Extrai os dados do corpo da requisição enviada pelo QStash
        const { chat_id, bot_id, target_node_id, variables } = req.body;
        console.log(`[WORKER] Recebido job de timeout para chat: ${chat_id}, bot: ${bot_id}`);

        // 2. Verifica se o usuário ainda está aguardando uma resposta.
        // Isso previne que o fluxo de "não respondeu" seja executado se o usuário respondeu no último segundo.
        const [userState] = await sql`SELECT waiting_for_input FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

        // 3. Se o estado for "waiting_for_input", significa que o usuário não respondeu a tempo.
        if (userState && userState.waiting_for_input) {
            console.log(`[WORKER] Timeout confirmado! Processando fluxo para ${chat_id} a partir do nó ${target_node_id}`);
            
            // Busca as credenciais do bot para poder continuar o fluxo
            const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;

            if (bot) {
                // Chama o motor de fluxo, mas forçando o início a partir do nó de "não respondeu"
                await processFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, target_node_id, variables);
            } else {
                 console.error(`[WORKER] Bot com ID ${bot_id} não encontrado no banco.`);
            }
        } else {
            // Se o estado não é mais "waiting_for_input", significa que o usuário já respondeu
            // e o webhook principal já cancelou esta tarefa (ou está prestes a).
            console.log(`[WORKER] Tarefa para ${chat_id} ignorada, pois o usuário já respondeu.`);
        }

        // 4. Responde com sucesso para o QStash saber que a tarefa foi processada.
        res.status(200).send('Worker finished successfully.');

    } catch (error) {
        console.error('[WORKER] Erro crítico ao processar timeout:', error);
        res.status(500).send('Erro interno no servidor durante o processamento do worker.');
    }
}


// ==========================================================
//                        EXPORTAÇÃO
// ==========================================================

// Exporta o handler, mas o envolve com a verificação de segurança do QStash.
// Isso garante que apenas requisições legítimas do Upstash possam executar este código.
module.exports = verifySignature(handler, {
    currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
    nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
    // Opcional, mas recomendado: adicione a URL para uma verificação extra
    // url: `https://${process.env.VERCEL_URL}/api/worker/process-timeout` 
});