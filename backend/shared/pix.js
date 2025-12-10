const axiosDefault = require('axios');
const { v4: uuidv4Default } = require('uuid');
const apiRateLimiter = require('./api-rate-limiter-bullmq');
const redisCache = require('./redis-cache');

function createPixService({
  sql,
  sqlWithRetry,
  axios = axiosDefault,
  uuidv4 = uuidv4Default,
  syncPayTokenCache = null, // Deprecated: usar redisCache agora
  pixupTokenCache = null, // Deprecated: usar redisCache agora
  adminApiKey,
  synPayBaseUrl = 'https://api.syncpayments.com.br',
  pushinpaySplitAccountId = null,
  cnpaySplitProducerId = null,
  oasyfySplitProducerId = null,
  brpixSplitRecipientId = null,
  wiinpaySplitUserId = null,
  pixupSplitUsername = null,
  paradiseSplitRecipientId = null,
  hottrackApiUrl = process.env.HOTTRACK_API_URL,
}) {
  if (!sql) {
    throw new Error('createPixService: sql instance é obrigatório.');
  }

  const runQuery = sqlWithRetry || sql;

  const resolvePreferredHost = (fallbackHost) => {
    if (!hottrackApiUrl) {
      return fallbackHost;
    }
    try {
      return new URL(hottrackApiUrl).host;
    } catch {
      return fallbackHost;
    }
  };

  async function getSyncPayAuthToken(seller) {
    const cacheKey = `syncpay-token:${seller.id}`;
    
    // Verificar cache no Redis
    const cached = await redisCache.get(cacheKey);
    if (cached) {
      const now = Date.now();
      if (cached.expiresAt && now < cached.expiresAt && cached.expiresAt > now + 60000) {
        // Token válido e ainda tem mais de 1 minuto
        return cached.accessToken;
      }
      // Token expirado, remover do cache
      if (cached.expiresAt && now >= cached.expiresAt) {
        await redisCache.delete(cacheKey);
      }
    }

    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) {
      throw new Error('Credenciais da SyncPay não configuradas para este vendedor.');
    }

    const response = await axios.post(`${synPayBaseUrl}/api/partner/v1/auth-token`, {
      client_id: seller.syncpay_client_id,
      client_secret: seller.syncpay_client_secret,
    }, {
      timeout: 15000 // 15 segundos para autenticação
    });

    const { access_token, expires_in } = response.data;
    const expiresAt = Date.now() + (expires_in * 1000);
    
    // Armazenar no Redis com TTL
    await redisCache.set(cacheKey, { accessToken: access_token, expiresAt }, expires_in * 1000);
    
    return access_token;
  }

  async function getPixupAuthToken(seller) {
    if (!seller.pixup_client_id || !seller.pixup_client_secret) {
      throw new Error('Credenciais da Pixup não configuradas para este vendedor. Configure pixup_client_id e pixup_client_secret.');
    }

    const cacheKey = `pixup-token:${seller.id}`;
    
    // Verificar cache no Redis
    const cached = await redisCache.get(cacheKey);
    if (cached) {
      const now = Date.now();
      if (cached.expiresAt && now < cached.expiresAt && cached.expiresAt > now + 60000) {
        // Token válido e ainda tem mais de 1 minuto
        return cached.accessToken;
      }
      // Token expirado, remover do cache
      if (cached.expiresAt && now >= cached.expiresAt) {
        await redisCache.delete(cacheKey);
      }
    }

    // OAuth2 com Basic Auth conforme documentação Pixup
    // Concatena client_id:client_secret e codifica em base64
    // Remove espaços em branco que podem causar erro 401
    const clientId = (seller.pixup_client_id || '').trim();
    const clientSecret = (seller.pixup_client_secret || '').trim();
    
    if (!clientId || !clientSecret) {
      throw new Error('Credenciais da Pixup não configuradas corretamente. Client ID e Client Secret são obrigatórios.');
    }
    
    const credentials = `${clientId}:${clientSecret}`;
    const base64Credentials = Buffer.from(credentials).toString('base64');

    try {
      // Log para debug (não logar as credenciais completas por segurança)
      if (process.env.NODE_ENV !== 'production' || process.env.ENABLE_VERBOSE_LOGS === 'true') {
        console.log(`[PIXUP AUTH] Seller ID: ${seller.id} - Tentando obter token OAuth2`);
      }
      
      const response = await axios.post('https://api.pixupbr.com/v2/oauth/token', {}, {
        headers: {
          'Authorization': `Basic ${base64Credentials}`,
          'Content-Type': 'application/json',
        },
        timeout: 15000 // 15 segundos para autenticação
      });

      const { access_token, expires_in } = response.data;
      if (!access_token) {
        throw new Error('Token de acesso não retornado pela API Pixup');
      }

      const expiresAt = Date.now() + (expires_in * 1000);
      
      // Armazenar no Redis com TTL
      await redisCache.set(cacheKey, { accessToken: access_token, expiresAt }, expires_in * 1000);
      
      if (process.env.NODE_ENV !== 'production' || process.env.ENABLE_VERBOSE_LOGS === 'true') {
        console.log(`[PIXUP AUTH] Seller ID: ${seller.id} - Token obtido com sucesso, expira em ${expires_in}s`);
      }
      return access_token;
    } catch (error) {
      // Log detalhado do erro para debug
      const status = error.response?.status;
      const statusText = error.response?.statusText;
      const errorData = error.response?.data;
      const errorMessage = errorData?.message || errorData?.error || error.message;
      
      console.error(`[PIXUP AUTH ERROR] Seller ID: ${seller.id} - Erro ao obter token:`, {
        status,
        statusText,
        message: errorMessage,
        data: errorData,
        clientIdPrefix: seller.pixup_client_id ? seller.pixup_client_id.substring(0, 10) + '...' : 'não configurado',
      });
      
      if (status === 401) {
        throw new Error('Credenciais inválidas da Pixup. Verifique se o Client ID e Client Secret estão corretos.');
      }
      
      throw new Error(`Erro ao obter token da Pixup: ${errorMessage || 'Erro desconhecido'}`);
    }
  }

  async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address, click_id_internal = null) {
    let pixData;
    let acquirer = 'Não identificado';
    const commission_rate = seller.commission_rate || 0.0500;
    const preferredHost = resolvePreferredHost(host);

    const clientPayload = {
      document: { number: '21376710773', type: 'CPF' },
      name: 'Cliente Padrão',
      email: 'gabriel@email.com',
      phone: '27995310379',
    };

    if (provider === 'brpix') {
      if (!seller.brpix_secret_key || !seller.brpix_company_id) {
        throw new Error('Credenciais da BR PIX não configuradas para este vendedor.');
      }
      const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');

      const payload = {
        customer: clientPayload,
        items: [{ title: 'Produto Digital', unitPrice: parseInt(value_cents, 10), quantity: 1 }],
        paymentMethod: 'PIX',
        amount: parseInt(value_cents, 10),
        pix: { expiresInDays: 1 },
        ip: ip_address,
      };

      const commission_cents = Math.floor(value_cents * commission_rate);
      if (apiKey !== adminApiKey && commission_cents > 0 && brpixSplitRecipientId) {
        payload.split = [{ recipientId: brpixSplitRecipientId, amount: commission_cents }];
      }

      const response = await apiRateLimiter.createTransaction({
        provider: 'brpix',
        sellerId: seller.id,
        method: 'post',
        url: 'https://api.brpixdigital.com/functions/v1/transactions',
        headers: { Authorization: `Basic ${credentials}`, 'Content-Type': 'application/json' },
        data: payload
      });
      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = 'BRPix';

      const brpixTransactionId = pixData?.transaction_id || pixData?.id;
      const qrCodeText =
        pixData?.pix?.qrcode ||
        pixData?.pix?.qrcodeText ||
        pixData?.pix?.qr_code ||
        pixData?.pix?.qrCode;
      const qrCodeBase64 =
        pixData?.pix?.qr_code_base64 ||
        pixData?.pix?.qrcode_base64 ||
        pixData?.pix?.qrcode ||
        pixData?.pix?.qr_code;

      return {
        qr_code_text: qrCodeText,
        qr_code_base64: qrCodeBase64,
        transaction_id: brpixTransactionId,
        acquirer,
        provider,
      };
    }

    if (provider === 'syncpay') {
      const token = await getSyncPayAuthToken(seller);
      const payload = {
        amount: value_cents / 100,
        payer: {
          name: 'Cliente Padrão',
          email: 'gabriel@gmail.com',
          document: '21376710773',
          phone: '27995310379',
        },
        webhook_url: `https://${preferredHost}/api/webhook/syncpay`,
      };
      const commission_percentage = commission_rate * 100;
      if (apiKey !== adminApiKey && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) {
        payload.split = [{ percentage: Math.round(commission_percentage), user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID }];
      }
      const response = await apiRateLimiter.createTransaction({
        provider: 'syncpay',
        sellerId: seller.id,
        method: 'post',
        url: `${synPayBaseUrl}/api/partner/v1/cash-in`,
        headers: { Authorization: `Bearer ${token}` },
        data: payload
      });
      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = 'SyncPay';
      return {
        qr_code_text: pixData.pix_code,
        qr_code_base64: null,
        transaction_id: pixData.identifier,
        acquirer,
        provider,
      };
    }

    if (provider === 'wiinpay') {
      const wiinpayApiKey = seller.wiinpay_api_key || seller.wiinpay_api_token || seller.wiinpay_key;
      if (!wiinpayApiKey) {
        throw new Error('Credenciais da WiinPay não configuradas para este vendedor.');
      }
      const amount = parseFloat((value_cents / 100).toFixed(2));
      let commissionValue = parseFloat((amount * commission_rate).toFixed(2));

      const payload = {
        api_key: wiinpayApiKey,
        value: amount,
        name: clientPayload.name,
        email: clientPayload.email,
        description: `PIX HotTrack #${uuidv4()}`,
        webhook_url: `https://${preferredHost}/api/webhook/wiinpay`,
        metadata: { origin: 'HotTrack', seller_id: seller.id },
      };

      if (apiKey !== adminApiKey && commissionValue > 0 && wiinpaySplitUserId) {
        // WiinPay requer que o split seja maior que R$ 0,10
        // Se o valor calculado for exatamente 0.10 ou menor, ajustamos para 0.11
        if (commissionValue <= 0.10) {
          commissionValue = 0.11;
        }
        
        payload.split = {
          value: commissionValue,
          percentage: parseFloat((commission_rate * 100).toFixed(4)), // percentage como inteiro (1 = 1%)
          user_id: wiinpaySplitUserId,
        };
      }

      const response = await apiRateLimiter.createTransaction({
        provider: 'wiinpay',
        sellerId: seller.id,
        method: 'post',
        url: 'https://api-v2.wiinpay.com.br/payment/create',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        data: payload
      });

      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = 'WiinPay';

      // Verifica diferentes formatos de resposta (similar ao parseWiinpayPayment)
      const paymentData =
        pixData?.payment ||
        pixData?.data ||
        pixData?.payload ||
        pixData?.transaction ||
        pixData;

      const transactionId =
        paymentData?.id ||
        paymentData?.payment_id ||
        paymentData?.paymentId ||
        paymentData?.transaction_id ||
        paymentData?.transactionId ||
        pixData?.id ||
        pixData?.payment_id ||
        pixData?.paymentId ||
        pixData?.transaction_id ||
        pixData?.transactionId;
      
      const qrCodeText =
        paymentData?.pix?.copy_paste ||
        paymentData?.pix?.copyAndPaste ||
        paymentData?.pix?.code ||
        paymentData?.pix?.qrcode ||
        paymentData?.pix_code ||
        paymentData?.qr_code ||
        paymentData?.qrCode ||
        paymentData?.copy_paste ||
        pixData?.pix?.copy_paste ||
        pixData?.pix?.copyAndPaste ||
        pixData?.pix?.code ||
        pixData?.pix?.qrcode ||
        pixData?.pix_code ||
        pixData?.qr_code ||
        pixData?.qrCode ||
        pixData?.copy_paste;
      
      const qrCodeBase64 =
        paymentData?.pix?.base64 ||
        paymentData?.pix?.qrcode_base64 ||
        paymentData?.pix?.qr_code_base64 ||
        paymentData?.qr_code_base64 ||
        paymentData?.qrcode_base64 ||
        pixData?.pix?.base64 ||
        pixData?.pix?.qrcode_base64 ||
        pixData?.pix?.qr_code_base64 ||
        pixData?.qr_code_base64 ||
        pixData?.qrcode_base64;

      if (!transactionId || !qrCodeText) {
        console.error(`[PIX TEST ERROR] Seller ID: ${seller.id}, Provider: wiinpay - Resposta inesperada:`, {
          responseData: JSON.stringify(pixData),
          transactionIdFound: !!transactionId,
          qrCodeTextFound: !!qrCodeText,
        });
        const errorMessage = pixData?.message || pixData?.error || 'Resposta não contém transactionId ou qrCodeText';
        throw new Error(`Resposta inesperada da WiinPay ao gerar PIX: ${errorMessage}. Resposta recebida: ${JSON.stringify(pixData)}`);
      }

      return {
        qr_code_text: qrCodeText,
        qr_code_base64: qrCodeBase64 || null,
        transaction_id: transactionId,
        acquirer,
        provider,
      };
    }

    if (provider === 'cnpay' || provider === 'oasyfy') {
      const isCnpay = provider === 'cnpay';
      const publicKey = isCnpay ? seller.cnpay_public_key : seller.oasyfy_public_key;
      const secretKey = isCnpay ? seller.cnpay_secret_key : seller.oasyfy_secret_key;
      if (!publicKey || !secretKey) {
        throw new Error(`Credenciais para ${provider.toUpperCase()} não configuradas.`);
      }

      const apiUrl = isCnpay
        ? 'https://painel.appcnpay.com/api/v1/gateway/pix/receive'
        : 'https://app.oasyfy.com/api/v1/gateway/pix/receive';
      const splitId = isCnpay ? cnpaySplitProducerId : oasyfySplitProducerId;

      const payload = {
        identifier: uuidv4(),
        amount: value_cents / 100,
        client: {
          name: clientPayload.name,
          email: 'gabriel@gmail.com',
          document: '21376710773',
          phone: '27995310379',
        },
        callbackUrl: `https://${preferredHost}/api/webhook/${provider}`,
      };

      const commission = parseFloat(((value_cents / 100) * commission_rate).toFixed(2));
      if (apiKey !== adminApiKey && commission > 0 && splitId) {
        payload.splits = [{ producerId: splitId, amount: commission }];
      }

      const response = await apiRateLimiter.createTransaction({
        provider: isCnpay ? 'cnpay' : 'oasyfy',
        sellerId: seller.id,
        method: 'post',
        url: apiUrl,
        headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey },
        data: payload
      });
      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = isCnpay ? 'CNPay' : 'Oasy.fy';

      const transactionId =
        pixData?.transaction?.id ||
        pixData?.transaction?.identifier ||
        pixData?.transaction_id ||
        pixData?.transactionId ||
        pixData?.id ||
        payload.identifier;

      const qrCodeText =
        pixData?.pix?.code ||
        pixData?.pix?.qrcode ||
        pixData?.pix?.copy_and_paste ||
        pixData?.pix?.copyAndPaste ||
        pixData?.pix?.copy_paste ||
        pixData?.pix?.copyPaste ||
        pixData?.pix_code ||
        pixData?.pixCode ||
        pixData?.qr_code ||
        pixData?.qrCode ||
        pixData?.qr_code_text ||
        pixData?.qrCodeText;

      const qrCodeBase64 =
        pixData?.pix?.base64 ||
        pixData?.pix?.qr_code_base64 ||
        pixData?.pix?.qrcode_base64 ||
        pixData?.qr_code_base64 ||
        pixData?.qrCodeBase64 ||
        pixData?.pix?.qr_code ||
        pixData?.pix?.qrcode;

      if (!transactionId || !qrCodeText) {
        throw new Error(`Resposta inesperada da ${isCnpay ? 'CNPay' : 'Oasy.fy'} ao gerar PIX.`);
      }

      return {
        qr_code_text: qrCodeText,
        qr_code_base64: qrCodeBase64 || null,
        transaction_id: transactionId,
        acquirer,
        provider,
      };
    }

    if (provider === 'pushinpay') {
      if (!seller.pushinpay_token) {
        throw new Error('Token da PushinPay não configurado.');
      }
      const payload = {
        value: value_cents,
        webhook_url: `https://${preferredHost}/api/webhook/pushinpay`,
      };
      const commission_cents = Math.floor(value_cents * commission_rate);
      if (apiKey !== adminApiKey && commission_cents > 0 && pushinpaySplitAccountId) {
        payload.split_rules = [{ value: commission_cents, account_id: pushinpaySplitAccountId }];
      }
      const pushinpayResponse = await apiRateLimiter.createTransaction({
        provider: 'pushinpay',
        sellerId: seller.id,
        method: 'post',
        url: 'https://api.pushinpay.com.br/api/pix/cashIn',
        headers: { Authorization: `Bearer ${seller.pushinpay_token}` },
        data: payload
      });
      pixData = pushinpayResponse;
      acquirer = 'Woovi';
      return {
        qr_code_text: pixData.qr_code,
        qr_code_base64: pixData.qr_code_base64,
        transaction_id: pushinpayResponse.id || pushinpayResponse.transaction_id || pushinpayResponse.identifier,
        acquirer,
        provider,
      };
    }

    if (provider === 'pixup') {
      // Obtém o token OAuth2 ou usa o token direto como fallback
      const pixupToken = await getPixupAuthToken(seller);
      
      const amount = parseFloat((value_cents / 100).toFixed(2));
      const externalId = uuidv4();

      const payload = {
        amount: amount,
        external_id: externalId,
        postbackUrl: `https://${preferredHost}/api/webhook/pixup`,
        payerQuestion: `PIX HotTrack #${externalId}`,
        payer: {
          name: clientPayload.name,
          document: clientPayload.document?.number || '',
          email: clientPayload.email,
        },
      };

      // Implementar split se houver comissão e username configurado
      // Documentação Pixup: mínimo 0.1%, máximo 95%, percentageSplit deve ser string
      if (apiKey !== adminApiKey && commission_rate > 0 && pixupSplitUsername) {
        const commissionPercentage = parseFloat((commission_rate * 100).toFixed(1));
        // Validar: mínimo 0.1%, máximo 95%
        if (commissionPercentage >= 0.1 && commissionPercentage <= 95) {
          payload.split = [
            {
              username: pixupSplitUsername,
              percentageSplit: commissionPercentage.toFixed(1), // String conforme documentação
            },
          ];
        }
      }

      const response = await apiRateLimiter.createTransaction({
        provider: 'pixup',
        sellerId: seller.id,
        method: 'post',
        url: 'https://api.pixupbr.com/v2/pix/qrcode',
        headers: {
          Authorization: `Bearer ${pixupToken}`,
          'Content-Type': 'application/json',
        },
        data: payload
      });
      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = 'Pixup';

      const transactionId = pixData?.transactionId || pixData?.transaction_id || externalId;
      const qrCodeText = pixData?.qrcode || pixData?.qr_code || pixData?.qrcode_text;

      if (!transactionId || !qrCodeText) {
        throw new Error('Resposta inesperada da Pixup ao gerar PIX.');
      }

      return {
        qr_code_text: qrCodeText,
        qr_code_base64: null, // Pixup não retorna base64 na resposta padrão
        transaction_id: transactionId,
        acquirer,
        provider,
      };
    }

    if (provider === 'paradise') {
      const paradiseSecretKey = seller.paradise_secret_key;
      if (!paradiseSecretKey) {
        throw new Error('Chave secreta da Paradise não configurada para este vendedor.');
      }

      const paradiseProductHash = seller.paradise_product_hash;
      if (!paradiseProductHash) {
        throw new Error('Product Hash da Paradise não configurado para este vendedor. Configure o código do produto no painel Paradise.');
      }

      const amount = parseInt(value_cents, 10); // Paradise usa centavos como integer
      const reference = uuidv4();
      const customerPhone = clientPayload.phone?.replace(/\D/g, '') || '11999999999'; // Remove caracteres não numéricos
      const customerDocument = clientPayload.document?.number?.replace(/\D/g, '') || '21376710773'; // Remove caracteres não numéricos

      // Buscar dados do click para tracking se click_id_internal for fornecido
      let clickData = null;
      if (click_id_internal) {
        try {
          const [click] = await sql`SELECT utm_source, utm_medium, utm_campaign, utm_content, utm_term FROM clicks WHERE id = ${click_id_internal}`;
          if (click) {
            clickData = click;
          }
        } catch (error) {
          console.warn(`[Paradise] Erro ao buscar dados do click ${click_id_internal} para tracking:`, error.message);
        }
      }

      const payload = {
        amount: amount,
        description: `PIX HotTrack #${reference}`,
        reference: reference,
        productHash: paradiseProductHash,
        customer: {
          name: clientPayload.name,
          email: clientPayload.email,
          document: customerDocument,
          phone: customerPhone,
        },
        postback_url: `https://${preferredHost}/api/webhook/paradise`,
      };

      // Adicionar objeto tracking se houver dados do click
      if (clickData && (clickData.utm_source || clickData.utm_medium || clickData.utm_campaign || clickData.utm_content || clickData.utm_term)) {
        payload.tracking = {
          utm_source: clickData.utm_source || null,
          utm_medium: clickData.utm_medium || null,
          utm_campaign: clickData.utm_campaign || null,
          utm_content: clickData.utm_content || null,
          utm_term: clickData.utm_term || null,
          src: null, // Campo não existe na tabela clicks
          sck: null, // Campo não existe na tabela clicks
        };
      }

      // Implementar split seguindo o padrão dos outros gateways
      // Apenas a comissão do SaaS vai para PARADISE_SPLIT_RECIPIENT_ID (do .env)
      // O restante fica na conta principal do seller (que está fazendo a transação)
      if (apiKey !== adminApiKey && commission_rate > 0 && paradiseSplitRecipientId) {
        const commissionAmount = Math.floor(value_cents * commission_rate);
        if (commissionAmount > 0) {
          payload.splits = [
            {
              recipientId: paradiseSplitRecipientId,
              amount: commissionAmount,
            },
          ];
        }
      }

      const response = await apiRateLimiter.createTransaction({
        provider: 'paradise',
        sellerId: seller.id,
        method: 'post',
        url: 'https://multi.paradisepags.com/api/v1/transaction.php',
        headers: {
          'X-API-Key': paradiseSecretKey,
          'Content-Type': 'application/json',
        },
        data: payload
      });
      pixData = response; // apiRateLimiter retorna response.data diretamente
      acquirer = pixData?.acquirer || 'ParadiseBank';

      // Paradise retorna transaction_id (numérico) e id (que é o reference)
      const transactionId = pixData?.transaction_id || pixData?.id || reference;
      const qrCodeText = pixData?.qr_code || pixData?.qrcode;
      const qrCodeBase64 = pixData?.qr_code_base64 || null;

      if (!transactionId || !qrCodeText) {
        console.error(`[PIX TEST ERROR] Seller ID: ${seller.id}, Provider: paradise - Resposta inesperada:`, {
          responseData: JSON.stringify(pixData),
          transactionIdFound: !!transactionId,
          qrCodeTextFound: !!qrCodeText,
        });
        throw new Error('Resposta inesperada da Paradise ao gerar PIX.');
      }

      return {
        qr_code_text: qrCodeText,
        qr_code_base64: qrCodeBase64,
        transaction_id: String(transactionId), // Converter para string para consistência
        acquirer,
        provider,
      };
    }

    throw new Error(`Provedor de PIX desconhecido: ${provider}`);
  }

  async function generatePixWithFallback(seller, value_cents, host, apiKey, ip_address, click_id_internal) {
    const providerOrder = [
      seller.pix_provider_primary,
      seller.pix_provider_secondary,
      seller.pix_provider_tertiary,
    ].filter(Boolean);

    if (providerOrder.length === 0) {
      throw new Error('Nenhum provedor de PIX configurado para este vendedor.');
    }

    // Tentar todos os provedores em paralelo
    const promises = providerOrder.map(provider => 
      generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address, click_id_internal)
        .then(result => ({ success: true, provider, result }))
        .catch(error => ({ success: false, provider, error }))
    );

    const results = await Promise.allSettled(promises);
    
    // Priorizar primeiro provedor bem-sucedido na ordem de preferência
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === 'fulfilled' && result.value.success) {
        const pixResult = result.value.result;
        
        const [transaction] = await runQuery`
          INSERT INTO pix_transactions (
              click_id_internal, pix_value, qr_code_text, qr_code_base64,
              provider, provider_transaction_id, pix_id
          ) VALUES (
              ${click_id_internal}, ${value_cents / 100}, ${pixResult.qr_code_text},
              ${pixResult.qr_code_base64}, ${pixResult.provider},
              ${pixResult.transaction_id}, ${pixResult.transaction_id}
          ) RETURNING id`;

        pixResult.internal_transaction_id = transaction.id;
        return pixResult;
      }
    }

    // Se nenhum funcionou, usar o primeiro erro
    const firstError = results.find(r => r.status === 'fulfilled' && !r.value.success)?.value?.error ||
                      results.find(r => r.status === 'rejected')?.reason;
    const specificMessage = firstError?.response?.data?.message || firstError?.message || 'Todos os provedores de PIX falharam.';
    throw new Error(`Não foi possível gerar o PIX: ${specificMessage}`);
  }

  return {
    getSyncPayAuthToken,
    generatePixForProvider,
    generatePixWithFallback,
  };
}

module.exports = { createPixService };

