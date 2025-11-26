// Migração sob demanda de mídias para R2
const axios = require('axios');
const { sqlWithRetry } = require('../db');
const r2Storage = require('./r2-storage');

// Cache de tentativas para evitar loops
const migrationAttempts = new Map();

async function migrateMediaOnDemand(mediaId, fileId = null) {
    if (!r2Storage.enabled) {
        throw new Error('R2 Storage não está habilitado.');
    }

    // Verificar se já está migrado
    const [media] = await sqlWithRetry(
        'SELECT id, seller_id, file_name, file_id, file_type, thumbnail_file_id, storage_url, migration_status FROM media_library WHERE id = $1',
        [mediaId]
    );

    if (!media) {
        throw new Error(`Mídia com ID ${mediaId} não encontrada.`);
    }

    // Se já está migrada, retornar
    if (media.storage_type === 'r2' && media.storage_url) {
        return { success: true, alreadyMigrated: true, storageUrl: media.storage_url };
    }

    // Verificar se já tentamos migrar recentemente (evitar loops)
    const attemptKey = `${media.id}_${Date.now()}`;
    const lastAttempt = migrationAttempts.get(media.id);
    if (lastAttempt && Date.now() - lastAttempt < 60000) { // 1 minuto
        throw new Error('Migração já tentada recentemente. Aguarde um momento.');
    }

    migrationAttempts.set(media.id, Date.now());

    // Limpar cache antigo (manter apenas últimas 100 entradas)
    if (migrationAttempts.size > 100) {
        const oldestKey = Array.from(migrationAttempts.entries())
            .sort((a, b) => a[1] - b[1])[0][0];
        migrationAttempts.delete(oldestKey);
    }

    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    if (!storageBotToken) {
        throw new Error('TELEGRAM_STORAGE_BOT_TOKEN não configurado');
    }

    try {
        // 1. Buscar informações do arquivo no Telegram
        const sendTelegramRequest = async (token, method, data) => {
            const response = await axios.post(`https://api.telegram.org/bot${token}/${method}`, data, {
                timeout: 120000
            });
            return response.data;
        };

        const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { 
            file_id: media.file_id 
        });

        if (!fileInfo.ok || !fileInfo.result?.file_path) {
            throw new Error(`Arquivo não encontrado no Telegram: ${fileInfo.description || 'Erro desconhecido'}`);
        }

        // 2. Baixar arquivo do Telegram
        const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;
        const response = await axios.get(fileUrl, { 
            responseType: 'arraybuffer',
            timeout: 120000 // 2 minutos para arquivos grandes
        });

        const buffer = Buffer.from(response.data);

        // 3. Upload para R2
        const { storageKey, publicUrl } = await r2Storage.uploadFile(
            buffer,
            media.file_name,
            media.file_type,
            media.seller_id
        );

        // 4. Migrar thumbnail se existir
        let thumbnailStorageUrl = null;
        if (media.thumbnail_file_id) {
            try {
                const thumbInfo = await sendTelegramRequest(storageBotToken, 'getFile', { 
                    file_id: media.thumbnail_file_id 
                });

                if (thumbInfo.ok && thumbInfo.result?.file_path) {
                    const thumbUrl = `https://api.telegram.org/file/bot${storageBotToken}/${thumbInfo.result.file_path}`;
                    const thumbResponse = await axios.get(thumbUrl, { 
                        responseType: 'arraybuffer',
                        timeout: 60000
                    });
                    const thumbBuffer = Buffer.from(thumbResponse.data);

                    const thumbFileName = `thumb_${media.file_name}`;
                    const { publicUrl: thumbPublicUrl } = await r2Storage.uploadThumbnail(
                        thumbBuffer,
                        thumbFileName,
                        media.seller_id
                    );
                    thumbnailStorageUrl = thumbPublicUrl;
                }
            } catch (thumbError) {
                console.warn(`[Migrate On Demand] Erro ao migrar thumbnail de ${media.file_name}:`, thumbError.message);
                // Não falha a migração principal por causa do thumbnail
            }
        }

        // 5. Atualizar banco de dados
        await sqlWithRetry(`
            UPDATE media_library 
            SET storage_url = $1,
                storage_key = $2,
                storage_type = 'r2',
                migration_status = 'migrated',
                thumbnail_storage_url = $3
            WHERE id = $4
        `, [publicUrl, storageKey, thumbnailStorageUrl, media.id]);

        // Limpar do cache de tentativas
        migrationAttempts.delete(media.id);

        return { 
            success: true, 
            storageUrl: publicUrl,
            thumbnailStorageUrl 
        };
    } catch (error) {
        // Marcar como falha no banco
        await sqlWithRetry(`
            UPDATE media_library 
            SET migration_status = 'failed'
            WHERE id = $1
        `, [media.id]);

        migrationAttempts.delete(media.id);
        throw error;
    }
}

module.exports = { migrateMediaOnDemand };

