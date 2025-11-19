/**
 * Payment Handler - Função unificada para processar pagamentos bem-sucedidos
 * 
 * Esta função é idempotente e thread-safe, garantindo que transações sejam
 * processadas apenas uma vez mesmo com webhooks duplicados ou chamadas simultâneas.
 */

const logger = require('../logger');

async function handleSuccessfulPayment({
    transaction_id,
    customerData,
    sqlTx,
    adminSubscription,
    webpush,
    sendEventToUtmify,
    sendMetaEvent
}) {
    try {
        // Atualização atômica: marcar como pago E definir meta_event_id em uma única operação
        // Isso previne race conditions mesmo com webhooks duplicados simultâneos
        // A condição WHERE garante que apenas a primeira chamada consegue atualizar
        const [updateResult] = await sqlTx`
            UPDATE pix_transactions 
            SET 
                status = 'paid',
                paid_at = COALESCE(paid_at, NOW()),
                meta_event_id = COALESCE(meta_event_id, ${`Purchase.${transaction_id}.processing`})
            WHERE id = ${transaction_id} 
                AND (status != 'paid' OR meta_event_id IS NULL)
            RETURNING *
        `;
        
        if (!updateResult) {
            // Transação já foi processada completamente (paid + meta_event_id definido)
            // Isso pode acontecer se:
            // - Webhook foi chamado múltiplas vezes
            // - Outro processo já processou a transação
            logger.info(`[handleSuccessfulPayment] Transação ${transaction_id} já processada completamente. Ignorando.`);
            return;
        }
        
        const transaction = updateResult;
        logger.info(`[handleSuccessfulPayment] ===== PROCESSANDO PAGAMENTO TRANSACTION ${transaction_id} =====`);
        logger.info(`[handleSuccessfulPayment] Valor: R$ ${parseFloat(transaction.pix_value).toFixed(2)}`);
        logger.info(`[handleSuccessfulPayment] Status atualizado para: ${transaction.status}`);
        logger.info(`[handleSuccessfulPayment] meta_event_id definido como: ${transaction.meta_event_id}`);

        // Notificação push (se configurada)
        if (adminSubscription && webpush) {
            const payload = JSON.stringify({
                title: 'Nova Venda Paga!',
                body: `Venda de R$ ${parseFloat(transaction.pix_value).toFixed(2)} foi confirmada.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(error => {
                if (error.statusCode === 410) {
                    logger.info("Inscrição de notificação expirada. Removendo.");
                    adminSubscription = null;
                } else {
                    logger.warn("Falha ao enviar notificação push (não-crítico):", error.message);
                }
            });
        }
        
        // Buscar dados do clique e vendedor
        logger.info(`[handleSuccessfulPayment] Buscando dados do clique (click_id_internal: ${transaction.click_id_internal})...`);
        const [click] = await sqlTx`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        if (!click) {
            logger.error(`[handleSuccessfulPayment] ✗ ERRO CRÍTICO: Clique não encontrado para transação ${transaction_id}`);
            logger.error(`[handleSuccessfulPayment] click_id_internal: ${transaction.click_id_internal}`);
            return;
        }
        logger.info(`[handleSuccessfulPayment] ✓ Clique encontrado - ID: ${click.id}, pressel_id: ${click.pressel_id}, checkout_id: ${click.checkout_id}`);
        
        logger.info(`[handleSuccessfulPayment] Buscando dados do vendedor (seller_id: ${click.seller_id})...`);
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${click.seller_id}`;
        if (!seller) {
            logger.error(`[handleSuccessfulPayment] ✗ ERRO CRÍTICO: Vendedor não encontrado para transação ${transaction_id}`);
            logger.error(`[handleSuccessfulPayment] seller_id: ${click.seller_id}`);
            return;
        }
        logger.info(`[handleSuccessfulPayment] ✓ Vendedor encontrado - ID: ${seller.id}`);

        const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
        const productData = { id: "prod_final", name: "Produto Vendido" };
        logger.debug(`[handleSuccessfulPayment] Customer data:`, JSON.stringify(finalCustomerData, null, 2));

        // Enviar eventos (Utmify e Meta)
        // Se algum evento falhar, não abortar o processo (já marcamos como pago)
        logger.info(`[handleSuccessfulPayment] Enviando evento para Utmify...`);
        try {
            await sendEventToUtmify({
                status: 'paid',
                clickData: click,
                pixData: transaction,
                sellerData: seller,
                customerData: finalCustomerData,
                productData: productData,
                sqlTx: sqlTx
            });
            logger.info(`[handleSuccessfulPayment] ✓ Evento Utmify enviado com sucesso`);
        } catch (error) {
            logger.error(`[handleSuccessfulPayment] ✗ Erro ao enviar evento Utmify:`, error.message);
            logger.error(`[handleSuccessfulPayment] Stack Utmify:`, error.stack);
        }
        
        logger.info(`[handleSuccessfulPayment] Enviando evento Purchase para Meta...`);
        try {
            await sendMetaEvent({
                eventName: 'Purchase',
                clickData: click,
                transactionData: transaction,
                customerData: finalCustomerData
            });
            logger.info(`[handleSuccessfulPayment] ✓ Chamada sendMetaEvent concluída`);
        } catch (error) {
            logger.error(`[handleSuccessfulPayment] ✗ Erro ao enviar evento Meta:`, error.message);
            logger.error(`[handleSuccessfulPayment] Stack Meta:`, error.stack);
        }
        
        // Verificar resultado final
        const [finalCheck] = await sqlTx`SELECT meta_event_id, status FROM pix_transactions WHERE id = ${transaction_id}`;
        logger.info(`[handleSuccessfulPayment] Estado final - status: ${finalCheck?.status}, meta_event_id: ${finalCheck?.meta_event_id}`);
        
        if (finalCheck?.meta_event_id) {
            const eventIdPattern = new RegExp(`^Purchase\\.${transaction_id}\\.[0-9]+$`);
            if (eventIdPattern.test(finalCheck.meta_event_id)) {
                logger.info(`[handleSuccessfulPayment] ✓✓✓ Purchase enviado com SUCESSO para Meta! meta_event_id: ${finalCheck.meta_event_id}`);
            } else if (finalCheck.meta_event_id.includes('.processing')) {
                logger.error(`[handleSuccessfulPayment] ⚠⚠⚠ PROBLEMA: Purchase NÃO foi enviado! meta_event_id ainda está como: ${finalCheck.meta_event_id}`);
            } else {
                logger.warn(`[handleSuccessfulPayment] ⚠ meta_event_id em estado desconhecido: ${finalCheck.meta_event_id}`);
            }
        }
        
        logger.info(`[handleSuccessfulPayment] ===== FIM PROCESSAMENTO TRANSACTION ${transaction_id} =====`);
        
    } catch(error) {
        logger.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
        // Não re-lançar o erro para não quebrar o fluxo do webhook
    }
}

module.exports = { handleSuccessfulPayment };


