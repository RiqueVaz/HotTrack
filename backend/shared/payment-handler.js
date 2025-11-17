/**
 * Payment Handler - Função unificada para processar pagamentos bem-sucedidos
 * 
 * Esta função é idempotente e thread-safe, garantindo que transações sejam
 * processadas apenas uma vez mesmo com webhooks duplicados ou chamadas simultâneas.
 */

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
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já processada completamente. Ignorando.`);
            return;
        }
        
        const transaction = updateResult;
        console.log(`[handleSuccessfulPayment] Processando pagamento para transação ${transaction_id}.`);

        // Notificação push (se configurada)
        if (adminSubscription && webpush) {
            const payload = JSON.stringify({
                title: 'Nova Venda Paga!',
                body: `Venda de R$ ${parseFloat(transaction.pix_value).toFixed(2)} foi confirmada.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(error => {
                if (error.statusCode === 410) {
                    console.log("Inscrição de notificação expirada. Removendo.");
                    adminSubscription = null;
                } else {
                    console.warn("Falha ao enviar notificação push (não-crítico):", error.message);
                }
            });
        }
        
        // Buscar dados do clique e vendedor
        const [click] = await sqlTx`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        if (!click) {
            console.error(`[handleSuccessfulPayment] ERRO: Clique não encontrado para transação ${transaction_id}`);
            return;
        }
        
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${click.seller_id}`;
        if (!seller) {
            console.error(`[handleSuccessfulPayment] ERRO: Vendedor não encontrado para transação ${transaction_id}`);
            return;
        }

        const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
        const productData = { id: "prod_final", name: "Produto Vendido" };

        // Enviar eventos (Utmify e Meta)
        // Se algum evento falhar, não abortar o processo (já marcamos como pago)
        try {
            await sendEventToUtmify('paid', click, transaction, seller, finalCustomerData, productData);
        } catch (error) {
            console.error(`[handleSuccessfulPayment] Erro ao enviar evento Utmify (não-crítico):`, error.message);
        }
        
        try {
            await sendMetaEvent('Purchase', click, transaction, finalCustomerData);
        } catch (error) {
            console.error(`[handleSuccessfulPayment] Erro ao enviar evento Meta (não-crítico):`, error.message);
        }
        
    } catch(error) {
        console.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
        // Não re-lançar o erro para não quebrar o fluxo do webhook
    }
}

module.exports = { handleSuccessfulPayment };

