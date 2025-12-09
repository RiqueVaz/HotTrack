const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });
const { sqlTx, sqlWithRetry } = require('../db');



// Função para validar se uma URL de botão é permitida (pressel, obrigado ou checkout)
const validateButtonUrl = async (sellerId, buttonUrl) => {
    if (!buttonUrl || typeof buttonUrl !== 'string' || !buttonUrl.trim()) {
        return { valid: true }; // URL vazia é válida (botão sem URL)
    }

    try {
        // Normalizar URL: remover protocolo, query params e fragmentos
        let normalizedUrl = buttonUrl.trim();
        
        // Remover protocolo (http://, https://)
        normalizedUrl = normalizedUrl.replace(/^https?:\/\//i, '');
        
        // Remover query parameters e fragmentos
        normalizedUrl = normalizedUrl.split('?')[0].split('#')[0];
        
        // Remover trailing slash
        normalizedUrl = normalizedUrl.replace(/\/$/, '');

        // Buscar domínios de pressel do seller
        const presselDomains = await sqlTx`
            SELECT DISTINCT pad.domain
            FROM pressel_allowed_domains pad
            JOIN pressels p ON pad.pressel_id = p.id
            WHERE p.seller_id = ${sellerId}
        `;

        // Verificar se é um domínio de pressel
        for (const row of presselDomains) {
            const domain = row.domain.toLowerCase().replace(/^https?:\/\//i, '').replace(/\/$/, '');
            if (normalizedUrl.toLowerCase() === domain || normalizedUrl.toLowerCase().startsWith(domain + '/')) {
                return { valid: true };
            }
        }

        // Buscar IDs de páginas de obrigado do seller
        const thankYouPages = await sqlTx`
            SELECT id::text as id
            FROM thank_you_pages
            WHERE seller_id = ${sellerId}
        `;

        // Verificar se é uma página de obrigado (/obrigado/{id})
        for (const page of thankYouPages) {
            const obrigadoPath = `/obrigado/${page.id}`;
            if (normalizedUrl === obrigadoPath || normalizedUrl.endsWith(obrigadoPath)) {
                return { valid: true };
            }
        }

        // Buscar IDs de checkouts hospedados do seller
        const hostedCheckouts = await sqlTx`
            SELECT id::text as id
            FROM hosted_checkouts
            WHERE seller_id = ${sellerId}
        `;

        // Verificar se é um checkout (/oferta/{id})
        for (const checkout of hostedCheckouts) {
            const ofertaPath = `/oferta/${checkout.id}`;
            if (normalizedUrl === ofertaPath || normalizedUrl.endsWith(ofertaPath)) {
                return { valid: true };
            }
        }

        // Se não corresponde a nenhuma opção permitida
        return {
            valid: false,
            message: `URL de botão inválida: "${buttonUrl}". Apenas links de pressel, páginas de obrigado (/obrigado/{id}) ou checkouts (/oferta/{id}) são permitidos.`
        };
    } catch (error) {
        console.error('[validateButtonUrl] Erro ao validar URL:', error);
        return {
            valid: false,
            message: 'Erro ao validar URL de botão. Tente novamente.'
        };
    }
};

// Função para extrair URLs de botão de um fluxo
const extractButtonUrls = (nodes) => {
    const buttonUrls = [];
    
    if (!nodes || typeof nodes !== 'object') return buttonUrls;
    
    const flowData = typeof nodes === 'string' ? JSON.parse(nodes) : nodes;
    const flowNodes = flowData.nodes || [];
    
    for (const node of flowNodes) {
        if (!node.data || !node.data.actions || !Array.isArray(node.data.actions)) {
            continue;
        }
        
        for (const action of node.data.actions) {
            if (action.type === 'message' && action.data?.buttonUrl) {
                buttonUrls.push({
                    nodeId: node.id,
                    nodeType: node.type,
                    buttonUrl: action.data.buttonUrl,
                    buttonText: action.data.buttonText || '(sem texto)'
                });
            }
        }
    }
    
    return buttonUrls;
};

async function main() {
    try {
        console.log('🔍 Buscando todos os fluxos com URLs de botão...\n');

        // Busca todos os fluxos normais
        const flowsResult = await sqlWithRetry(
            `SELECT id, seller_id, name, nodes 
             FROM flows 
             WHERE nodes::text ILIKE '%buttonUrl%'`
        );

        // Busca todos os fluxos de disparo
        const disparoFlowsResult = await sqlWithRetry(
            `SELECT id, seller_id, name, nodes 
             FROM disparo_flows 
             WHERE nodes::text ILIKE '%buttonUrl%'`
        );

        console.log(`📊 Encontrados ${flowsResult.length} fluxos normais com URLs de botão`);
        console.log(`📊 Encontrados ${disparoFlowsResult.length} fluxos de disparo com URLs de botão\n`);

        const invalidFlows = [];
        let totalChecked = 0;
        let totalInvalid = 0;

        // Processa fluxos normais
        for (const flow of flowsResult) {
            totalChecked++;
            const buttonUrls = extractButtonUrls(flow.nodes);
            
            if (buttonUrls.length === 0) continue;
            
            for (const buttonUrlInfo of buttonUrls) {
                const validation = await validateButtonUrl(flow.seller_id, buttonUrlInfo.buttonUrl);
                
                if (!validation.valid) {
                    totalInvalid++;
                    invalidFlows.push({
                        flowId: flow.id,
                        flowName: flow.name,
                        sellerId: flow.seller_id,
                        flowType: 'normal',
                        nodeId: buttonUrlInfo.nodeId,
                        nodeType: buttonUrlInfo.nodeType,
                        buttonText: buttonUrlInfo.buttonText,
                        invalidUrl: buttonUrlInfo.buttonUrl,
                        errorMessage: validation.message
                    });
                }
            }
        }

        // Processa fluxos de disparo
        for (const flow of disparoFlowsResult) {
            totalChecked++;
            const buttonUrls = extractButtonUrls(flow.nodes);
            
            if (buttonUrls.length === 0) continue;
            
            for (const buttonUrlInfo of buttonUrls) {
                const validation = await validateButtonUrl(flow.seller_id, buttonUrlInfo.buttonUrl);
                
                if (!validation.valid) {
                    totalInvalid++;
                    invalidFlows.push({
                        flowId: flow.id,
                        flowName: flow.name,
                        sellerId: flow.seller_id,
                        flowType: 'disparo',
                        nodeId: buttonUrlInfo.nodeId,
                        nodeType: buttonUrlInfo.nodeType,
                        buttonText: buttonUrlInfo.buttonText,
                        invalidUrl: buttonUrlInfo.buttonUrl,
                        errorMessage: validation.message
                    });
                }
            }
        }

        // Busca informações dos sellers para exibir nomes
        const sellerIds = [...new Set(invalidFlows.map(f => f.sellerId))];
        const sellers = await sqlTx`
            SELECT id, name, email
            FROM sellers
            WHERE id = ANY(${sellerIds})
        `;
        const sellerMap = {};
        for (const seller of sellers) {
            sellerMap[seller.id] = seller;
        }

        // Exibe resultados
        console.log('\n' + '='.repeat(80));
        console.log('📋 RESULTADO DA VERIFICAÇÃO');
        console.log('='.repeat(80));
        console.log(`\n✅ Fluxos verificados: ${totalChecked}`);
        console.log(`❌ URLs inválidas encontradas: ${totalInvalid}`);
        console.log(`📦 Fluxos com problemas: ${invalidFlows.length}\n`);

        if (invalidFlows.length === 0) {
            console.log('🎉 Nenhuma URL inválida encontrada! Todos os fluxos estão corretos.\n');
        } else {
            console.log('⚠️  FLUXOS COM URLs INVÁLIDAS:\n');
            console.log('-'.repeat(80));
            
            // Agrupa por seller
            const bySeller = {};
            for (const flow of invalidFlows) {
                if (!bySeller[flow.sellerId]) {
                    bySeller[flow.sellerId] = [];
                }
                bySeller[flow.sellerId].push(flow);
            }

            for (const [sellerId, flows] of Object.entries(bySeller)) {
                const seller = sellerMap[sellerId] || { name: 'Desconhecido', email: 'N/A' };
                console.log(`\n👤 SELLER ID: ${sellerId}`);
                console.log(`   Nome: ${seller.name || 'N/A'}`);
                console.log(`   Email: ${seller.email || 'N/A'}`);
                console.log(`   Fluxos com problemas: ${flows.length}`);
                console.log('');
                
                for (const flow of flows) {
                    console.log(`   📄 Fluxo #${flow.flowId} - "${flow.flowName}" (${flow.flowType})`);
                    console.log(`      Nó: ${flow.nodeId} (${flow.nodeType})`);
                    console.log(`      Texto do botão: "${flow.buttonText}"`);
                    console.log(`      ❌ URL inválida: ${flow.invalidUrl}`);
                    console.log(`      💬 Erro: ${flow.errorMessage}`);
                    console.log('');
                }
            }
        }

        console.log('='.repeat(80));
        console.log('\n✅ Verificação concluída!\n');

    } catch (error) {
        console.error('❌ Erro:', error);
        process.exit(1);
    } finally {
        await sqlTx.end();
        process.exit(0);
    }
}

main();
