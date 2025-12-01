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
    
    async function sendMediaFromR2(botToken, chatId, storageUrl, fileType, caption) {
        const axios = require('axios');
        const FormData = require('form-data');
        
        // Baixar arquivo do R2
        const fileResponse = await axios.get(storageUrl, {
            responseType: 'arraybuffer',
            timeout: 120000
        });
        
        const fileBuffer = Buffer.from(fileResponse.data);
        const contentType = fileResponse.headers['content-type'] || 
            (fileType === 'image' ? 'image/jpeg' : 
             fileType === 'video' ? 'video/mp4' : 
             'audio/ogg');
        
        // Criar FormData para upload
        const formData = new FormData();
        formData.append('chat_id', chatId);
        
        const method = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' }[fileType];
        const field = { image: 'photo', video: 'video', audio: 'voice' }[fileType];
        const fileName = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' }[fileType];
        
        formData.append(field, fileBuffer, {
            filename: fileName,
            contentType: contentType
        });
        
        if (caption) {
            formData.append('caption', caption);
            formData.append('parse_mode', 'HTML');
        }
        
        const timeout = fileType === 'video' ? 120000 : 60000;
        return await sendTelegramRequest(botToken, method, formData, {
            headers: formData.getHeaders(),
            timeout
        });
    }
    
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
                // 2. PRIORIDADE 1: Se já está no R2, baixar e fazer upload
                if (media.storage_type === 'r2' && media.storage_url) {
                    try {
                        return await sendMediaFromR2(destinationBotToken, chatId, media.storage_url, fileType, caption);
                    } catch (urlError) {
                        logger.warn(`[Media] Erro ao enviar via R2 (download + upload):`, urlError.message);
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
                            
                            return await sendMediaFromR2(destinationBotToken, chatId, migrationResult.storageUrl, fileType, caption);
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
