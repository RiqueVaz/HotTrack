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
                    
                    const noReplyNodeId = findNextNode(currentNode.id, 'b', edges);
                    
                    if (noReplyNodeId) {
                        const timeoutMinutes = currentNode.data.replyTimeout || 5;
                        
                        console.log(`[Flow Engine] Agendando worker via QStash em ${timeoutMinutes} min para o nó ${noReplyNodeId}`);
            
                        // A MÁGICA ACONTECE AQUI
                        // Em vez de INSERT no SQL, você publica uma mensagem no QStash
                        const response = await qstashClient.publishJSON({
                            // A URL do seu worker que será chamado
                            url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`, 
                            // O corpo da requisição que seu worker vai receber
                            body: {
                                chat_id: chatId,
                                bot_id: botId,
                                target_node_id: noReplyNodeId,
                                variables: variables
                            },
                            // Atraso para a entrega da mensagem
                            delay: `${timeoutMinutes}m`,
                            // ID único para poder cancelar depois
                            contentBasedDeduplication: true, // Garante que a mesma tarefa não seja duplicada
                            "Upstash-Method": "POST" // Especifica que o método deve ser POST
                        });
                        
                        // Salva o ID da mensagem para poder cancelar depois
                        await sql`UPDATE user_flow_states SET scheduled_message_id = '${response.messageId}' WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
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


// ==========================================================
//                      LÓGICA DO WORKER
// ==========================================================

async function handler(req, res) {
    try {
        const { chat_id, bot_id, target_node_id, variables } = req.body;
        console.log(`[WORKER] Recebido job de timeout para chat: ${chat_id}, bot: ${bot_id}`);

        const [userState] = await sql`SELECT waiting_for_input FROM user_flow_states WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

        if (userState && userState.waiting_for_input) {
            console.log(`[WORKER] Timeout confirmado! Processando fluxo para ${chat_id} a partir do nó ${target_node_id}`);
            
            const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${bot_id}`;

            if (bot) {
                // CORREÇÃO DE ROBUSTEZ: Limpa o estado de espera ANTES de continuar o fluxo.
                await sql`UPDATE user_flow_states SET waiting_for_input = false, scheduled_message_id = NULL WHERE chat_id = ${chat_id} AND bot_id = ${bot_id}`;

                await processFlow(chat_id, bot_id, bot.bot_token, bot.seller_id, target_node_id, variables);
            } else {
                console.error(`[WORKER] Bot com ID ${bot_id} não encontrado no banco.`);
            }
        } else {
            console.log(`[WORKER] Tarefa para ${chat_id} ignorada, pois o usuário já respondeu.`);
        }

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