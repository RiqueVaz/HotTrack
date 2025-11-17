/**
 * Event Sender - Funções unificadas para enviar eventos para Meta e Utmify
 */

const crypto = require('crypto');
const axios = require('axios');

/**
 * Envia evento para Utmify
 */
async function sendEventToUtmify({
    status,
    clickData,
    pixData,
    sellerData,
    customerData,
    productData,
    sqlTx
}) {
    console.log(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            console.log(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sqlTx`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            console.log(`[Utmify] Clique originado do Checkout ID: ${clickData.checkout_id}. Lógica de associação não implementada para checkouts.`);
        }

        if (!integrationId) {
            console.log(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        const [integration] = await sqlTx`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            console.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id, platform: "HotTrack", paymentMethod: 'pix',
            status: status, createdAt: createdAt, approvedDate: approvedDate, refundedAt: null,
            customer: { name: customerData?.name || "Não informado", email: customerData?.email || "naoinformado@email.com", phone: customerData?.phone || null, document: customerData?.document || null, },
            products: [{ id: productData?.id || "default_product", name: productData?.name || "Produto Digital", planId: null, planName: null, quantity: 1, priceInCents: Math.round(pixData.pix_value * 100) }],
            trackingParameters: { src: null, sck: null, utm_source: clickData.utm_source, utm_campaign: clickData.utm_campaign, utm_medium: clickData.utm_medium, utm_content: clickData.utm_content, utm_term: clickData.utm_term },
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0500)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0500))) },
            isTest: false
        };

        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        console.log(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        console.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}

/**
 * Envia evento para Meta (Facebook Pixel)
 */
async function sendMetaEvent({
    eventName,
    clickData,
    transactionData,
    customerData,
    sqlTx
}) {
    try {
        // Para Purchase, verificar se meta_event_id foi definido em handleSuccessfulPayment
        if (eventName === 'Purchase' && transactionData.id) {
            const [check] = await sqlTx`SELECT meta_event_id FROM pix_transactions WHERE id = ${transactionData.id}`;
            if (!check?.meta_event_id) {
                console.log(`[Meta Pixel] ERRO: Tentando enviar Purchase mas meta_event_id não foi definido para transação ${transactionData.id}. Abortando.`);
                return;
            }
        }
        
        let presselPixels = [];
        if (clickData.pressel_id) {
            presselPixels = await sqlTx`SELECT pixel_config_id FROM pressel_pixels WHERE pressel_id = ${clickData.pressel_id}`;
        } else if (clickData.checkout_id) {
            // Verificar se é um checkout antigo (integer) ou hosted_checkout (UUID)
            const checkoutId = clickData.checkout_id;
            
            // Se começa com 'cko_', é um hosted_checkout (UUID)
            if (typeof checkoutId === 'string' && checkoutId.startsWith('cko_')) {
                // Buscar pixel_id do config do hosted_checkout
                const [hostedCheckout] = await sqlTx`
                    SELECT config->'tracking'->>'pixel_id' as pixel_id 
                    FROM hosted_checkouts 
                    WHERE id = ${checkoutId}
                `;
                
                if (hostedCheckout?.pixel_id) {
                    // Converter pixel_id para o formato esperado
                    presselPixels = [{ pixel_config_id: parseInt(hostedCheckout.pixel_id) }];
                }
            } else {
                // É um checkout antigo (integer), usar a tabela checkout_pixels
                presselPixels = await sqlTx`SELECT pixel_config_id FROM checkout_pixels WHERE checkout_id = ${checkoutId}`;
            }
        }

        if (presselPixels.length === 0) {
            console.log(`Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}.`);
            return;
        }

        const userData = {
            fbp: clickData.fbp || undefined,
            fbc: clickData.fbc || undefined,
            external_id: clickData.click_id ? clickData.click_id.replace('/start ', '') : undefined
        };

        if (clickData.ip_address && clickData.ip_address !== '::1' && !clickData.ip_address.startsWith('127.0.0.1')) {
            userData.client_ip_address = clickData.ip_address;
        }
        if (clickData.user_agent && clickData.user_agent.length > 10) { 
            userData.client_user_agent = clickData.user_agent;
        }

        if (customerData?.name) {
            const nameParts = customerData.name.trim().split(' ');
            const firstName = nameParts[0].toLowerCase();
            const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1].toLowerCase() : undefined;
            userData.fn = crypto.createHash('sha256').update(firstName).digest('hex');
            if (lastName) {
                userData.ln = crypto.createHash('sha256').update(lastName).digest('hex');
            }
        }

        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        let lastEventId = null;
        for (const { pixel_config_id } of presselPixels) {
            const [pixelConfig] = await sqlTx`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE id = ${pixel_config_id}`;
            if (pixelConfig) {
                const { pixel_id, meta_api_token } = pixelConfig;
                const event_id = `${eventName}.${transactionData.id || clickData.id}.${pixel_id}`;
                
                const payload = {
                    data: [{
                        event_name: eventName,
                        event_time: Math.floor(Date.now() / 1000),
                        event_id,
                        user_data: userData,
                        custom_data: {
                            currency: 'BRL',
                            value: transactionData.pix_value
                        },
                    }]
                };
                
                if (eventName !== 'Purchase') {
                    delete payload.data[0].custom_data.value;
                }

                console.log(`[Meta Pixel] Enviando payload para o pixel ${pixel_id}:`, JSON.stringify(payload, null, 2));
                await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                console.log(`Evento '${eventName}' enviado para o Pixel ID ${pixel_id}.`);
                
                // Guardar o último event_id para atualizar no final (se Purchase)
                if (eventName === 'Purchase') {
                    lastEventId = event_id;
                }
            }
        }
        
        // Para Purchase, atualizar meta_event_id apenas uma vez no final com o último event_id
        if (eventName === 'Purchase' && lastEventId && transactionData.id) {
            await sqlTx`UPDATE pix_transactions SET meta_event_id = ${lastEventId} WHERE id = ${transactionData.id}`;
        }
    } catch (error) {
        console.error(`Erro ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}

module.exports = { sendEventToUtmify, sendMetaEvent };

