// Helper para envio de mídias com suporte a R2 e migração sob demanda
const { sqlWithRetry } = require('../db');
const { migrateMediaOnDemand } = require('./migrate-media-on-demand');

/**
 * Normaliza payload do Telegram removendo undefined e garantindo tipos corretos
 */
function normalizeTelegramPayload(payload) {
    const normalized = {};
    for (const [key, value] of Object.entries(payload)) {
        if (value !== undefined) {
            // Converter null para string vazia em campos opcionais de texto
            if (value === null && (key === 'caption' || key === 'text')) {
                normalized[key] = '';
            } else {
                normalized[key] = value;
            }
        }
    }
    
    // Remover parse_mode se caption estiver vazio ou ausente
    // Telegram não aceita parse_mode sem caption
    if ((!normalized.caption || normalized.caption === '') && normalized.parse_mode) {
        delete normalized.parse_mode;
    }
    
    return normalized;
}

/**
 * Cria uma função sendMediaFromLibrary configurada com as dependências
 * @param {Function} sendTelegramRequest - Função para enviar requisições ao Telegram
 * @param {Function} sendMediaAsProxy - Função de fallback (método antigo)
 * @param {Object} logger - Objeto logger (opcional)
 * @returns {Function} Função sendMediaFromLibrary configurada
 */
function createSendMediaFromLibrary(sendTelegramRequest, sendMediaAsProxy, logger = console) {
    // Cache de mídias para evitar queries repetidas
    const mediaCache = new Map();
    const MEDIA_CACHE_TTL = 5 * 60 * 1000; // 5 minutos
    
    return async function sendMediaFromLibrary(destinationBotToken, chatId, fileId, fileType, caption, sellerId = null) {
        // Normalizar caption
        caption = caption || "";
        
        // Validar file_id antes de usar
        if (!fileId || typeof fileId !== 'string' || fileId.trim() === '') {
            throw new Error('File ID inválido ou vazio');
        }
        
        // Verificar se file_id tem formato válido (prefixos conhecidos ou R2)
        const isValidFileId = fileId.startsWith('BAAC') || fileId.startsWith('AgAC') || 
                             fileId.startsWith('AwAC') || fileId.startsWith('R2_') ||
                             fileId.startsWith('http://') || fileId.startsWith('https://');
        
        // 1. Tentar buscar mídia no banco pelo file_id (com cache)
        let media = null;
        if (sellerId) {
            const cacheKey = `media_${sellerId}_${fileId}`;
            const cached = mediaCache.get(cacheKey);
            
            if (cached && (Date.now() - cached.timestamp) < MEDIA_CACHE_TTL) {
                media = cached.media;
            } else {
                const [mediaResult] = await sqlWithRetry(`
                    SELECT id, file_id, storage_url, storage_type, migration_status
                    FROM media_library 
                    WHERE file_id = $1 AND seller_id = $2
                    LIMIT 1
                `, [fileId, sellerId]);

                if (mediaResult) {
                    media = mediaResult;
                    mediaCache.set(cacheKey, { media, timestamp: Date.now() });
                }
            }

            if (media) {
                // 2. PRIORIDADE 1: Se já está no R2, usar URL direta (não tentar file_id)
                if (media.storage_type === 'r2' && media.storage_url) {
                    try {
                        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
                        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
                        const method = methodMap[fileType];
                        const field = fieldMap[fileType];
                        
                        if (!method) throw new Error('Tipo de arquivo não suportado.');
                        
                        const payload = normalizeTelegramPayload({
                            chat_id: chatId,
                            [field]: media.storage_url,
                            caption: caption || "",
                            parse_mode: 'HTML'
                        });
                        
                        return await sendTelegramRequest(destinationBotToken, method, payload, { 
                            timeout: fileType === 'video' ? 120000 : 60000 
                        });
                    } catch (urlError) {
                        logger.warn(`[Media] Erro ao enviar via R2 URL:`, urlError.message);
                        // Não tentar fallback com file_id se R2 falhou
                        throw urlError;
                    }
                }

                // 3. PRIORIDADE 2: Se não está migrado, tentar migrar sob demanda
                if (media.storage_type === 'telegram' && media.migration_status !== 'migrated') {
                    try {
                        logger.info(`[Media] Migrando mídia ${media.id} sob demanda...`);
                        const migrationResult = await migrateMediaOnDemand(media.id);
                        
                        if (migrationResult?.success && migrationResult?.storageUrl) {
                            // Invalidar cache
                            mediaCache.delete(cacheKey);
                            
                            const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
                            const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
                            const method = methodMap[fileType];
                            const field = fieldMap[fileType];
                            
                            const payload = normalizeTelegramPayload({
                                chat_id: chatId,
                                [field]: migrationResult.storageUrl,
                                caption: caption || "",
                                parse_mode: 'HTML'
                            });
                            
                            return await sendTelegramRequest(destinationBotToken, method, payload, { 
                                timeout: fileType === 'video' ? 120000 : 60000 
                            });
                        }
                    } catch (migrationError) {
                        logger.error(`[Media] Erro na migração sob demanda:`, migrationError.message);
                        // Continuar com método antigo apenas se migração falhou
                    }
                }
            }
        }

        // 4. Fallback: método antigo (download/re-upload) - apenas se não encontrou no banco ou migração falhou
        return await sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption);
    };
}

module.exports = { createSendMediaFromLibrary };
