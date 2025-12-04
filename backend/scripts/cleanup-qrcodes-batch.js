/**
 * Script de Limpeza de QR Codes em Batches
 * 
 * Este script limpa qr_code_text e qr_code_base64 de transações > 2 dias
 * processando em batches pequenos para evitar:
 * - Bloqueio prolongado da tabela
 * - Consumo excessivo de memória
 * - Geração excessiva de WAL
 * - Checkpoints frequentes
 * 
 * Após a limpeza, executa VACUUM FULL ANALYZE para recuperar espaço físico.
 * ATENÇÃO: VACUUM FULL pode demorar várias horas e bloquear a tabela.
 * 
 * Executar via:
 * node backend/scripts/cleanup-qrcodes-batch.js
 */

// Carrega variáveis de ambiente (para execução local)
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: require('path').join(__dirname, '../../.env') });
}

const { sqlTx } = require('../db');

async function cleanupQRCodesInBatches() {
    const BATCH_SIZE = 10000; // Processar 10k registros por vez
    const DELAY_BETWEEN_BATCHES = 2000; // 2 segundos de pausa entre batches
    let totalAffected = 0;
    let batchCount = 0;
    const startTime = Date.now();
    
    try {
        console.log('[Cleanup QR] Iniciando limpeza de QR codes em batches...');
        console.log(`[Cleanup QR] Configuração: ${BATCH_SIZE} registros por batch, ${DELAY_BETWEEN_BATCHES}ms de pausa`);
        
        // Verificar quantas linhas precisam ser processadas
        const countResult = await sqlTx`
            SELECT COUNT(*) as total
            FROM pix_transactions
            WHERE created_at < NOW() - INTERVAL '2 days'
              AND (qr_code_text IS NOT NULL OR qr_code_base64 IS NOT NULL)
        `;
        
        const totalToProcess = parseInt(countResult[0].total) || 0;
        console.log(`[Cleanup QR] Total de registros a processar: ${totalToProcess.toLocaleString()}`);
        
        if (totalToProcess === 0) {
            console.log('[Cleanup QR] ✓ Nenhum registro para processar. Concluído!');
            return { success: true, totalAffected: 0, batchCount: 0 };
        }
        
        // Processar em batches
        while (true) {
            // Processar um batch usando CTID (mais eficiente que LIMIT/OFFSET)
            const result = await sqlTx`
                WITH batch AS (
                    SELECT ctid
                    FROM pix_transactions
                    WHERE created_at < NOW() - INTERVAL '2 days'
                      AND (qr_code_text IS NOT NULL OR qr_code_base64 IS NOT NULL)
                    LIMIT ${BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE pix_transactions pt
                SET qr_code_text = NULL, qr_code_base64 = NULL
                FROM batch b
                WHERE pt.ctid = b.ctid
                RETURNING pt.id
            `;
            
            const affected = result.length;
            totalAffected += affected;
            batchCount++;
            
            // Calcular progresso
            const progress = totalToProcess > 0 
                ? ((totalAffected / totalToProcess) * 100).toFixed(2)
                : 0;
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            const avgSpeed = totalAffected > 0 ? (totalAffected / (elapsed / 60)).toFixed(0) : 0;
            
            console.log(
                `[Cleanup QR] Batch ${batchCount}: ${affected.toLocaleString()} linhas | ` +
                `Total: ${totalAffected.toLocaleString()}/${totalToProcess.toLocaleString()} ` +
                `(${progress}%) | Tempo: ${elapsed}s | Velocidade: ~${avgSpeed} linhas/min`
            );
            
            // Se não há mais linhas, terminar
            if (affected === 0) {
                break;
            }
            
            // Pausa entre batches para não sobrecarregar o banco
            await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
        }
        
        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`[Cleanup QR] ✓ Concluído!`);
        console.log(`[Cleanup QR] - Total processado: ${totalAffected.toLocaleString()} linhas`);
        console.log(`[Cleanup QR] - Batches executados: ${batchCount}`);
        console.log(`[Cleanup QR] - Tempo total: ${totalTime}s`);
        console.log(`[Cleanup QR] - Velocidade média: ~${(totalAffected / (totalTime / 60)).toFixed(0)} linhas/min`);
        
        // Verificar tamanho atual da tabela
        const sizeResult = await sqlTx`
            SELECT 
                pg_size_pretty(pg_total_relation_size('pix_transactions')) AS total_size,
                pg_size_pretty(pg_relation_size('pix_transactions')) AS table_size,
                pg_size_pretty(pg_indexes_size('pix_transactions')) AS indexes_size,
                pg_size_pretty(pg_total_relation_size('pix_transactions') - 
                               pg_relation_size('pix_transactions') - 
                               pg_indexes_size('pix_transactions')) AS toast_and_bloat
        `;
        
        console.log(`[Cleanup QR] Tamanho atual da tabela (antes do VACUUM FULL):`);
        console.log(`[Cleanup QR] - Total: ${sizeResult[0].total_size}`);
        console.log(`[Cleanup QR] - Tabela: ${sizeResult[0].table_size}`);
        console.log(`[Cleanup QR] - Índices: ${sizeResult[0].indexes_size}`);
        console.log(`[Cleanup QR] - TOAST/Bloat: ${sizeResult[0].toast_and_bloat}`);
        
        // Executar VACUUM FULL ANALYZE se houver registros processados
        let sizeAfterVacuum = null;
        if (totalAffected > 0) {
            console.log(`[Cleanup QR] ⚠️  ATENÇÃO: Executando VACUUM FULL ANALYZE...`);
            console.log(`[Cleanup QR] ⚠️  Isso pode demorar várias horas e bloquear a tabela temporariamente.`);
            console.log(`[Cleanup QR] ⚠️  A tabela ficará indisponível durante a execução.`);
            const vacuumStartTime = Date.now();
            try {
                await sqlTx`VACUUM FULL ANALYZE pix_transactions;`;
                const vacuumTime = ((Date.now() - vacuumStartTime) / 1000).toFixed(1);
                const vacuumMinutes = (vacuumTime / 60).toFixed(1);
                console.log(`[Cleanup QR] ✓ VACUUM FULL ANALYZE concluído em ${vacuumMinutes} minutos (${vacuumTime}s)`);
                
                // Verificar tamanho após VACUUM
                const sizeResultAfter = await sqlTx`
                    SELECT 
                        pg_size_pretty(pg_total_relation_size('pix_transactions')) AS total_size,
                        pg_size_pretty(pg_relation_size('pix_transactions')) AS table_size,
                        pg_size_pretty(pg_indexes_size('pix_transactions')) AS indexes_size,
                        pg_size_pretty(pg_total_relation_size('pix_transactions') - 
                                       pg_relation_size('pix_transactions') - 
                                       pg_indexes_size('pix_transactions')) AS toast_and_bloat
                `;
                
                sizeAfterVacuum = sizeResultAfter[0];
                
                console.log(`[Cleanup QR] Tamanho após VACUUM FULL:`);
                console.log(`[Cleanup QR] - Total: ${sizeAfterVacuum.total_size}`);
                console.log(`[Cleanup QR] - Tabela: ${sizeAfterVacuum.table_size}`);
                console.log(`[Cleanup QR] - Índices: ${sizeAfterVacuum.indexes_size}`);
                console.log(`[Cleanup QR] - TOAST/Bloat: ${sizeAfterVacuum.toast_and_bloat}`);
            } catch (vacuumError) {
                console.error('[Cleanup QR] ⚠️  Erro ao executar VACUUM FULL ANALYZE:', vacuumError.message);
                console.error('[Cleanup QR] ⚠️  Você pode executar manualmente: VACUUM FULL ANALYZE pix_transactions;');
            }
        } else {
            console.log(`[Cleanup QR] Nenhum registro processado, pulando VACUUM FULL`);
        }
        
        return { 
            success: true, 
            totalAffected, 
            batchCount,
            totalTime: parseFloat(totalTime),
            size: sizeResult[0],
            sizeAfterVacuum: sizeAfterVacuum
        };
    } catch (error) {
        console.error('[Cleanup QR] ✗ Erro durante processamento:', error);
        console.error(`[Cleanup QR] - Processados até agora: ${totalAffected.toLocaleString()} linhas em ${batchCount} batches`);
        throw error;
    }
}

// Executar se chamado diretamente
if (require.main === module) {
    cleanupQRCodesInBatches()
        .then((result) => {
            console.log('[Cleanup QR] Resultado final:', result);
            process.exit(0);
        })
        .catch((error) => {
            console.error('[Cleanup QR] Erro fatal:', error);
            process.exit(1);
        });
}

module.exports = { cleanupQRCodesInBatches };

