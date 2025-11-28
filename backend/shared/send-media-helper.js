// Helper para envio de mídias com suporte a R2 e migração sob demanda
const { sqlWithRetry } = require('../db');
const { migrateMediaOnDemand } = require('./migrate-media-on-demand');

/**
 * Cria uma função sendMediaFromLibrary configurada com as dependências
 * @param {Function} sendTelegramRequest - Função para enviar requisições ao Telegram
 * @param {Function} sendMediaAsProxy - Função de fallback (método antigo)
 * @param {Object} logger - Objeto logger (opcional)
 * @returns {Function} Função sendMediaFromLibrary configurada
 */
function createSendMediaFromLibrary(sendTelegramRequest, sendMediaAsProxy, logger = console) {
    return async function sendMediaFromLibrary(destinationBotToken, chatId, fileId, fileType, caption, sellerId = null) {
    // Normalizar caption para evitar UNDEFINED_VALUE (Telegram não aceita undefined)
    caption = caption || "";
    
    // 1. Tentar buscar mídia no banco pelo file_id
    if (sellerId) {
        const [media] = await sqlWithRetry(`
            SELECT id, file_id, storage_url, storage_type, migration_status
            FROM media_library 
            WHERE file_id = $1 AND seller_id = $2
            LIMIT 1
        `, [fileId, sellerId]);

        if (media) {
            // 2. Se já está no R2, usar URL direta
            if (media.storage_type === 'r2' && media.storage_url) {
                try {
                    const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
                    const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
                    const method = methodMap[fileType];
                    const field = fieldMap[fileType];
                    
                    if (!method) throw new Error('Tipo de arquivo não suportado.');
                    
                    return await sendTelegramRequest(destinationBotToken, method, {
                        chat_id: chatId,
                        [field]: media.storage_url, // Telegram aceita URL direta!
                        caption: caption || "",
                        parse_mode: 'HTML'
                    }, { timeout: fileType === 'video' ? 120000 : 60000 });
                } catch (urlError) {
                    logger.warn(`[Media] Erro ao enviar via R2 URL, tentando fallback:`, urlError.message);
                    // Continuar para fallback
                }
            }

            // 3. Se não está migrado, tentar migrar sob demanda
            if (media.storage_type === 'telegram' && media.migration_status !== 'migrated') {
                try {
                    logger.info(`[Media] Migrando mídia ${media.id} sob demanda...`);
                    await migrateMediaOnDemand(media.id);
                    
                    // Buscar novamente após migração
                    const [migratedMedia] = await sqlWithRetry(`
                        SELECT storage_url FROM media_library WHERE id = $1
                    `, [media.id]);

                    if (migratedMedia?.storage_url) {
                        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
                        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
                        const method = methodMap[fileType];
                        const field = fieldMap[fileType];
                        
                        return await sendTelegramRequest(destinationBotToken, method, {
                            chat_id: chatId,
                            [field]: migratedMedia.storage_url,
                            caption: caption || "",
                            parse_mode: 'HTML'
                        }, { timeout: fileType === 'video' ? 120000 : 60000 });
                    }
                } catch (migrationError) {
                    logger.error(`[Media] Erro na migração sob demanda:`, migrationError.message);
                    // Continuar com método antigo
                }
            }
        }
    }

        // 4. Fallback: método antigo (download/re-upload)
        return await sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption);
    };
}

module.exports = { createSendMediaFromLibrary };
