/**
 * Event Sender - Funções unificadas para enviar eventos para Meta e Utmify
 */

const crypto = require('crypto');
const axios = require('axios');
const logger = require('../logger');

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
    logger.debug(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            logger.debug(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sqlTx`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            // Verificar se é um checkout antigo (integer) ou hosted_checkout (UUID)
            const checkoutId = clickData.checkout_id;
            
            // Se começa com 'cko_', é um hosted_checkout (UUID)
            if (typeof checkoutId === 'string' && checkoutId.startsWith('cko_')) {
                logger.debug(`[Utmify] Clique originado do Checkout Hospedado ID: ${checkoutId}`);
                // Buscar utmify_integration_id do config do hosted_checkout
                const [hostedCheckout] = await sqlTx`
                    SELECT config->'tracking'->>'utmify_integration_id' as utmify_integration_id 
                    FROM hosted_checkouts 
                    WHERE id = ${checkoutId}
                `;
                
                if (hostedCheckout?.utmify_integration_id) {
                    const parsedId = parseInt(hostedCheckout.utmify_integration_id);
                    if (!isNaN(parsedId)) {
                        integrationId = parsedId;
                        logger.debug(`[Utmify] Integração Utmify encontrada no checkout hospedado: ${integrationId}`);
                    } else {
                        logger.warn(`[Utmify] Valor inválido para utmify_integration_id no checkout hospedado: ${hostedCheckout.utmify_integration_id}`);
                    }
                } else {
                    logger.debug(`[Utmify] Clique originado do Checkout Hospedado ID: ${checkoutId}, mas nenhuma integração Utmify configurada.`);
                }
            } else {
                logger.debug(`[Utmify] Clique originado do Checkout ID: ${checkoutId}. Lógica de associação não implementada para checkouts antigos.`);
            }
        } else {
            logger.debug(`[Utmify] Clique ID ${clickData.id} não originado de Pressel ou Checkout Hospedado conhecido.`);
        }

        if (!integrationId) {
            logger.debug(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        const [integration] = await sqlTx`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            logger.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        logger.debug(`[Utmify] Token encontrado. Montando payload...`);
        
        // Log dos dados do click recebidos para debug
        logger.debug(`[Utmify] Dados do click recebidos:`, {
            id: clickData.id,
            pressel_id: clickData.pressel_id,
            checkout_id: clickData.checkout_id,
            utm_source: clickData.utm_source,
            utm_campaign: clickData.utm_campaign,
            utm_medium: clickData.utm_medium,
            utm_content: clickData.utm_content,
            utm_term: clickData.utm_term,
            click_id: clickData.click_id
        });
        
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id || `ht_${pixData.id}`, // Use provider ID or internal ID as fallback
            platform: "HotTrack", 
            paymentMethod: 'pix',
            status: status, 
            createdAt: createdAt, 
            approvedDate: approvedDate, 
            refundedAt: null,
            customer: { 
                name: customerData?.name || "Não informado", 
                email: customerData?.email || "naoinformado@email.com", 
                phone: customerData?.phone || null, 
                document: customerData?.document || null
            },
            products: [{ 
                id: productData?.id || "default_product", 
                name: productData?.name || "Produto Digital", 
                planId: null, 
                planName: null, 
                quantity: 1, 
                priceInCents: Math.round(pixData.pix_value * 100) 
            }],
            trackingParameters: { 
                src: null, 
                sck: null, 
                utm_source: clickData.utm_source, 
                utm_campaign: clickData.utm_campaign, 
                utm_medium: clickData.utm_medium, 
                utm_content: clickData.utm_content, 
                utm_term: clickData.utm_term,
                click_id: clickData.click_id?.replace('/start ', '') || null // Adiciona click_id aqui
            },
            commission: { 
                totalPriceInCents: Math.round(pixData.pix_value * 100), 
                gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0500)), 
                userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0500))) 
            },
            isTest: false
        };

        // Log do payload completo antes de enviar (apenas em debug)
        logger.debug(`[Utmify] Payload completo que será enviado:`, JSON.stringify(payload, null, 2));
        logger.debug(`[Utmify] TrackingParameters no payload:`, JSON.stringify(payload.trackingParameters, null, 2));

        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        logger.info(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        logger.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
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
    logger.info(`[Meta Pixel] ===== INÍCIO ENVIO EVENTO ${eventName} =====`);
    logger.info(`[Meta Pixel] Transaction ID: ${transactionData?.id}, Click ID: ${clickData?.id}`);
    
    try {
        // Para Purchase, verificar status da transação (sem abortar se já foi enviado)
        if (eventName === 'Purchase' && transactionData.id) {
            logger.info(`[Meta Pixel] [Purchase] Verificando transação ${transactionData.id}...`);
            const [check] = await sqlTx`SELECT meta_event_id, status FROM pix_transactions WHERE id = ${transactionData.id}`;
            logger.info(`[Meta Pixel] [Purchase] Status da transação: ${check?.status}, meta_event_id atual: ${check?.meta_event_id}`);
            
            // Não abortar se meta_event_id já existe - permitir reenvio se necessário
            // O código antigo não verificava isso e sempre tentava enviar
        }
        
        logger.info(`[Meta Pixel] Buscando pixels configurados...`);
        logger.info(`[Meta Pixel] Click data - pressel_id: ${clickData.pressel_id}, checkout_id: ${clickData.checkout_id}`);
        
        let presselPixels = [];
        if (clickData.pressel_id) {
            logger.info(`[Meta Pixel] Buscando pixels do pressel ${clickData.pressel_id}...`);
            presselPixels = await sqlTx`
                SELECT pc.id as pixel_config_id, pc.pixel_id, pc.meta_api_token
                FROM pixel_configurations pc
                JOIN pressel_pixels pp ON pc.id = pp.pixel_config_id
                WHERE pp.pressel_id = ${clickData.pressel_id} 
                    AND pc.seller_id = ${clickData.seller_id}
            `;
            logger.info(`[Meta Pixel] Encontrados ${presselPixels.length} pixel(s) no pressel.`);
        } else if (clickData.checkout_id) {
            // Verificar se é um checkout antigo (integer) ou hosted_checkout (UUID)
            const checkoutId = clickData.checkout_id;
            logger.info(`[Meta Pixel] Buscando pixels do checkout ${checkoutId}...`);
            
            // Se começa com 'cko_', é um hosted_checkout (UUID)
            if (typeof checkoutId === 'string' && checkoutId.startsWith('cko_')) {
                logger.info(`[Meta Pixel] É um hosted_checkout (UUID)`);
                // Buscar pixel_id do config do hosted_checkout
                const [hostedCheckout] = await sqlTx`
                    SELECT config->'tracking'->>'pixel_id' as pixel_id 
                    FROM hosted_checkouts 
                    WHERE id = ${checkoutId}
                `;
                logger.info(`[Meta Pixel] Pixel ID do hosted_checkout: ${hostedCheckout?.pixel_id}`);
                
                if (hostedCheckout?.pixel_id) {
                    // Buscar configuração completa com JOIN direto (como código antigo)
                    const pixelId = parseInt(hostedCheckout.pixel_id);
                    const [pixelConfig] = await sqlTx`
                        SELECT id as pixel_config_id, pixel_id, meta_api_token 
                        FROM pixel_configurations 
                        WHERE pixel_id = ${pixelId} 
                            AND seller_id = ${clickData.seller_id}
                    `;
                    if (pixelConfig) {
                        presselPixels = [pixelConfig];
                        logger.info(`[Meta Pixel] Pixel encontrado para hosted_checkout - pixel_id: ${pixelConfig.pixel_id}`);
                    }
                }
            } else {
                logger.info(`[Meta Pixel] É um checkout antigo (integer)`);
                // É um checkout antigo (integer), usar JOIN direto como código antigo
                presselPixels = await sqlTx`
                    SELECT pc.id as pixel_config_id, pc.pixel_id, pc.meta_api_token
                    FROM pixel_configurations pc
                    JOIN checkout_pixels cp ON pc.id = cp.pixel_config_id
                    WHERE cp.checkout_id = ${checkoutId}
                        AND pc.seller_id = ${clickData.seller_id}
                `;
                logger.info(`[Meta Pixel] Encontrados ${presselPixels.length} pixel(s) no checkout antigo.`);
            }
        } else {
            logger.info(`[Meta Pixel] Clique não tem pressel_id nem checkout_id.`);
        }

        if (presselPixels.length === 0) {
            logger.error(`[Meta Pixel] ⚠⚠⚠ PROBLEMA ENCONTRADO: Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}.`);
            logger.error(`[Meta Pixel] pressel_id: ${clickData.pressel_id}, checkout_id: ${clickData.checkout_id}`);
            if (eventName === 'Purchase' && transactionData.id) {
                logger.error(`[Meta Pixel] [Purchase] Transação ${transactionData.id} NÃO TERÁ evento Purchase enviado para Meta!`);
            }
            return;
        }

        logger.info(`[Meta Pixel] Preparando userData...`);
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
        logger.debug(`[Meta Pixel] userData preparado:`, JSON.stringify(userData, null, 2));
        
        // Validar dados mínimos antes de enviar
        const hasMinimalData = userData.client_ip_address || userData.client_user_agent || userData.external_id;
        if (!hasMinimalData && eventName === 'Purchase') {
            logger.warn(`[Meta Pixel] [Purchase] ⚠ Dados mínimos ausentes (IP/UserAgent/ExternalID). Meta pode rejeitar.`);
            // Continuar mesmo assim, mas logar aviso
        }
        
        let pixelsProcessed = 0;
        let pixelsSuccess = 0;
        let pixelsFailed = 0;
        
        logger.info(`[Meta Pixel] Processando ${presselPixels.length} pixel(s)...`);
        for (const pixelConfig of presselPixels) {
            pixelsProcessed++;
            const { pixel_id, meta_api_token, pixel_config_id } = pixelConfig;
            logger.info(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Processando pixel_config_id: ${pixel_config_id}`);
            
            try {
                if (!pixel_id || !meta_api_token) {
                    logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] ✗ ERRO: Pixel ID ou token ausente. pixel_id: ${pixel_id}, token: ${!!meta_api_token}`);
                    pixelsFailed++;
                    continue;
                }
                
                logger.info(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Pixel encontrado - pixel_id: ${pixel_id}, token presente: ${!!meta_api_token}`);
                
                const event_id = `${eventName}.${transactionData.id || clickData.id}.${pixel_id}`;
                logger.info(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Event ID gerado: ${event_id}`);
                
                const payload = {
                    data: [{
                        event_name: eventName,
                        event_time: Math.floor(Date.now() / 1000),
                        event_id,
                        action_source: 'other', // Indicate server-side event
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

                logger.info(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Enviando para Meta API...`);
                logger.debug(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Payload:`, JSON.stringify(payload, null, 2));
                
                try {
                    const response = await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                    logger.info(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] ✓ SUCESSO! Resposta da Meta:`, JSON.stringify(response.data, null, 2));
                    pixelsSuccess++;
                    
                    // Store the Meta event ID in the transaction record for reference (como código antigo)
                    if (eventName === 'Purchase') {
                        await sqlTx`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                        logger.info(`[Meta Pixel] [Purchase] meta_event_id atualizado: ${event_id}`);
                    }
                } catch (apiError) {
                    pixelsFailed++;
                    logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] ✗ FALHA ao enviar para Meta API:`);
                    logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Status:`, apiError.response?.status);
                    logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Erro:`, JSON.stringify(apiError.response?.data || apiError.message, null, 2));
                    logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Stack:`, apiError.stack);
                }
            } catch (error) {
                pixelsFailed++;
                logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] ERRO inesperado ao processar pixel:`, error.message);
                logger.error(`[Meta Pixel] [${pixelsProcessed}/${presselPixels.length}] Stack:`, error.stack);
            }
        }
        
        logger.info(`[Meta Pixel] Resumo: ${pixelsSuccess} sucesso, ${pixelsFailed} falhas de ${presselPixels.length} pixel(s) processados.`);
        
        logger.info(`[Meta Pixel] ===== FIM ENVIO EVENTO ${eventName} =====`);
    } catch (error) {
        logger.error(`[Meta Pixel] ===== ERRO CRÍTICO NO ENVIO DE ${eventName} =====`);
        logger.error(`[Meta Pixel] Erro:`, error.response?.data || error.message);
        logger.error(`[Meta Pixel] Stack trace completo:`, error.stack);
        logger.error(`[Meta Pixel] Transaction ID: ${transactionData?.id}`);
        logger.error(`[Meta Pixel] Click ID: ${clickData?.id}`);
        logger.error(`[Meta Pixel] ================================================`);
    }
}

module.exports = { sendEventToUtmify, sendMetaEvent };





