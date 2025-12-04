/**
 * Script de Arquivamento Automático de Transações PIX Antigas
 * 
 * Este script arquiva transações PIX antigas (> 12 meses) mantendo:
 * - Dados completos dos últimos 12 meses
 * - Agregações mensais para dados mais antigos
 * - Campos essenciais para webhooks (provider_transaction_id, pix_id, status)
 * 
 * Executar via cron ou scheduler:
 * - Exemplo cron: 0 2 * * * (diariamente às 2h)
 * - Ou agendar via Railway Cron Jobs
 */

const { sqlTx } = require('../db');

async function archiveOldTransactions() {
    try {
        console.log('[Archive] Iniciando arquivamento de transações antigas...');
        
        // 1. Agregar dados antigos por mês (apenas se ainda não existir)
        console.log('[Archive] Agregando dados antigos por mês...');
        const aggregateResult = await sqlTx`
            INSERT INTO pix_transactions_monthly_summary 
                (seller_id, year, month, total_transactions, total_paid, total_pending, total_canceled, total_revenue)
            SELECT 
                c.seller_id,
                EXTRACT(YEAR FROM pt.created_at)::INTEGER as year,
                EXTRACT(MONTH FROM pt.created_at)::INTEGER as month,
                COUNT(*) as total_transactions,
                COUNT(*) FILTER (WHERE pt.status = 'paid') as total_paid,
                COUNT(*) FILTER (WHERE pt.status = 'pending') as total_pending,
                COUNT(*) FILTER (WHERE pt.status IN ('canceled', 'expired', 'failed')) as total_canceled,
                COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) as total_revenue
            FROM pix_transactions pt
            JOIN clicks c ON pt.click_id_internal = c.id
            WHERE pt.created_at < NOW() - INTERVAL '12 months'
              AND NOT EXISTS (
                  SELECT 1 FROM pix_transactions_monthly_summary pms
                  WHERE pms.seller_id = c.seller_id
                    AND pms.year = EXTRACT(YEAR FROM pt.created_at)::INTEGER
                    AND pms.month = EXTRACT(MONTH FROM pt.created_at)::INTEGER
              )
            GROUP BY c.seller_id, EXTRACT(YEAR FROM pt.created_at), EXTRACT(MONTH FROM pt.created_at)
            ON CONFLICT (seller_id, year, month) DO NOTHING
            RETURNING COUNT(*)
        `;
        
        const aggregatedCount = aggregateResult.length > 0 ? aggregateResult[0].count || 0 : 0;
        console.log(`[Archive] ✓ Agregados ${aggregatedCount} meses de dados`);
        
        // 2. Limpar campos desnecessários de transações antigas
        console.log('[Archive] Limpando campos desnecessários (qr_code_text, qr_code_base64)...');
        const cleanupResult = await sqlTx`
            UPDATE pix_transactions
            SET qr_code_text = NULL, qr_code_base64 = NULL
            WHERE created_at < NOW() - INTERVAL '12 months'
              AND (qr_code_text IS NOT NULL OR qr_code_base64 IS NOT NULL)
            RETURNING COUNT(*)
        `;
        
        const cleanedCount = cleanupResult.length > 0 ? cleanupResult[0].count || 0 : 0;
        console.log(`[Archive] ✓ Limpos ${cleanedCount} registros`);
        
        // 3. Verificar tamanho atual da tabela
        const sizeResult = await sqlTx`
            SELECT pg_size_pretty(pg_total_relation_size('pix_transactions')) AS total_size,
                   pg_size_pretty(pg_relation_size('pix_transactions')) AS table_size,
                   pg_size_pretty(pg_total_relation_size('pix_transactions') - pg_relation_size('pix_transactions')) AS indexes_size
        `;
        
        console.log(`[Archive] ✓ Tamanho atual: ${sizeResult[0].total_size} (tabela: ${sizeResult[0].table_size}, índices: ${sizeResult[0].indexes_size})`);
        
        console.log(`[Archive] ✓ Arquivamento concluído com sucesso!`);
        
        return {
            success: true,
            aggregated: aggregatedCount,
            cleaned: cleanedCount,
            size: sizeResult[0]
        };
    } catch (error) {
        console.error('[Archive] Erro ao arquivar transações:', error);
        throw error;
    }
}

// Executar se chamado diretamente
if (require.main === module) {
    archiveOldTransactions()
        .then((result) => {
            console.log('[Archive] Resultado:', result);
            process.exit(0);
        })
        .catch((error) => {
            console.error('[Archive] Erro fatal:', error);
            process.exit(1);
        });
}

module.exports = { archiveOldTransactions };

