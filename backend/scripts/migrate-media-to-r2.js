// Script de migração batch de mídias para Cloudflare R2
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

const axios = require('axios');
const { sqlWithRetry } = require('../db');
const r2Storage = require('../shared/r2-storage');

const BATCH_SIZE = 10; // Processar 10 mídias por vez
const DELAY_BETWEEN_BATCHES = 2000; // 2 segundos entre batches

async function sendTelegramRequest(botToken, method, data) {
    const response = await axios.post(`https://api.telegram.org/bot${botToken}/${method}`, data, {
        timeout: 120000
    });
    return response.data;
}

async function migrateSingleMedia(media) {
    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    
    if (!storageBotToken) {
        throw new Error('TELEGRAM_STORAGE_BOT_TOKEN não configurado');
    }

    try {
        // 1. Buscar informações do arquivo no Telegram
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
                console.warn(`[Migração] Erro ao migrar thumbnail de ${media.file_name}:`, thumbError.message);
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

        return { success: true, mediaId: media.id };
    } catch (error) {
        // Marcar como falha no banco
        await sqlWithRetry(`
            UPDATE media_library 
            SET migration_status = 'failed'
            WHERE id = $1
        `, [media.id]);

        throw error;
    }
}

async function migrateMediaToR2() {
    if (!r2Storage.enabled) {
        console.error('[Migração] R2 Storage não está habilitado. Configure as variáveis de ambiente.');
        return;
    }

    console.log('[Migração] Iniciando migração de mídias para R2...');

    // Buscar todas as mídias que ainda não foram migradas
    const allMedia = await sqlWithRetry(`
        SELECT id, seller_id, file_name, file_id, file_type, thumbnail_file_id, storage_url
        FROM media_library 
        WHERE storage_type = 'telegram' 
        AND (storage_url IS NULL OR migration_status = 'pending' OR migration_status IS NULL)
        ORDER BY created_at ASC
    `);

    console.log(`[Migração] Encontradas ${allMedia.length} mídias para migrar.`);

    if (allMedia.length === 0) {
        console.log('[Migração] Nenhuma mídia para migrar.');
        return;
    }

    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    // Processar em batches
    for (let i = 0; i < allMedia.length; i += BATCH_SIZE) {
        const batch = allMedia.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(allMedia.length / BATCH_SIZE);
        console.log(`[Migração] Processando batch ${batchNumber}/${totalBatches} (${batch.length} mídias)...`);

        const batchPromises = batch.map(media => migrateSingleMedia(media));
        const results = await Promise.allSettled(batchPromises);

        results.forEach((result, index) => {
            if (result.status === 'fulfilled') {
                successCount++;
                console.log(`[Migração] ✓ Migrada: ${batch[index].file_name} (ID: ${batch[index].id})`);
            } else {
                errorCount++;
                const error = result.reason;
                errors.push({ media: batch[index], error: error.message });
                console.error(`[Migração] ✗ Erro ao migrar ${batch[index].file_name}:`, error.message);
            }
        });

        // Delay entre batches para não sobrecarregar
        if (i + BATCH_SIZE < allMedia.length) {
            await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
        }
    }

    console.log('\n[Migração] Resumo:');
    console.log(`  ✓ Sucesso: ${successCount}`);
    console.log(`  ✗ Erros: ${errorCount}`);
    
    if (errors.length > 0) {
        console.log('\n[Migração] Erros detalhados:');
        errors.forEach(({ media, error }) => {
            console.log(`  - ${media.file_name} (ID: ${media.id}): ${error}`);
        });
    }

    console.log('\n[Migração] Concluída!');
}

// Executar se chamado diretamente
if (require.main === module) {
    migrateMediaToR2()
        .then(() => {
            console.log('[Migração] Processo finalizado com sucesso!');
            process.exit(0);
        })
        .catch(error => {
            console.error('[Migração] Erro fatal:', error);
            process.exit(1);
        });
}

module.exports = { migrateMediaToR2, migrateSingleMedia };

