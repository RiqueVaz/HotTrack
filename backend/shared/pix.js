const axiosDefault = require('axios');
const { v4: uuidv4Default } = require('uuid');

function createPixService({
  sql,
  sqlWithRetry,
  axios = axiosDefault,
  uuidv4 = uuidv4Default,
  syncPayTokenCache = new Map(),
  adminApiKey,
  synPayBaseUrl = 'https://api.syncpayments.com.br',
  pushinpaySplitAccountId = null,
  cnpaySplitProducerId = null,
  oasyfySplitProducerId = null,
  brpixSplitRecipientId = null,
  wiinpaySplitUserId = null,
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
    if (!syncPayTokenCache) {
      throw new Error('createPixService: syncPayTokenCache não foi fornecido.');
    }

    const cachedToken = syncPayTokenCache.get(seller.id);
    if (cachedToken && cachedToken.expiresAt > Date.now() + 60000) {
      return cachedToken.accessToken;
    }

    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) {
      throw new Error('Credenciais da SyncPay não configuradas para este vendedor.');
    }

    const response = await axios.post(`${synPayBaseUrl}/api/partner/v1/auth-token`, {
      client_id: seller.syncpay_client_id,
      client_secret: seller.syncpay_client_secret,
    });

    const { access_token, expires_in } = response.data;
    const expiresAt = Date.now() + (expires_in * 1000);
    syncPayTokenCache.set(seller.id, { accessToken: access_token, expiresAt });
    return access_token;
  }

  async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address) {
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

      const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, {
        headers: { Authorization: `Basic ${credentials}`, 'Content-Type': 'application/json' },
      });
      pixData = response.data;
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
      const response = await axios.post(`${synPayBaseUrl}/api/partner/v1/cash-in`, payload, {
        headers: { Authorization: `Bearer ${token}` },
      });
      pixData = response.data;
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

      let response;
      try {
        response = await axios.post('https://api-v2.wiinpay.com.br/payment/create', payload, {
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        });
      } catch (axiosError) {
        // Extrai a mensagem de erro corretamente, lidando com objetos aninhados
        let errorMessage = axiosError.message;
        const errorData = axiosError.response?.data;
        
        if (errorData) {
          // Tenta diferentes formatos de resposta de erro
          if (typeof errorData === 'string') {
            try {
              const parsed = JSON.parse(errorData);
              errorMessage = parsed?.error?.message || parsed?.message || errorMessage;
            } catch {
              errorMessage = errorData;
            }
          } else if (errorData.error?.message) {
            errorMessage = errorData.error.message;
          } else if (errorData.message) {
            errorMessage = typeof errorData.message === 'string' ? errorData.message : errorData.message.message || JSON.stringify(errorData.message);
          } else if (errorData.error) {
            errorMessage = typeof errorData.error === 'string' ? errorData.error : JSON.stringify(errorData.error);
          }
        }
        
        const errorDetails = errorData ? JSON.stringify(errorData) : 'Sem detalhes';
        console.error(`[PIX TEST ERROR] Seller ID: ${seller.id}, Provider: wiinpay - Erro HTTP:`, {
          status: axiosError.response?.status,
          statusText: axiosError.response?.statusText,
          message: errorMessage,
          data: errorDetails,
        });
        throw new Error(`Erro ao comunicar com WiinPay: ${errorMessage || 'Erro desconhecido'}`);
      }

      if (response.status < 200 || response.status >= 300) {
        // Extrai a mensagem de erro corretamente
        let errorMessage = `Status HTTP ${response.status}`;
        const errorData = response.data;
        
        if (errorData) {
          if (typeof errorData === 'string') {
            try {
              const parsed = JSON.parse(errorData);
              errorMessage = parsed?.error?.message || parsed?.message || errorMessage;
            } catch {
              errorMessage = errorData;
            }
          } else if (errorData.error?.message) {
            errorMessage = errorData.error.message;
          } else if (errorData.message) {
            errorMessage = typeof errorData.message === 'string' ? errorData.message : errorData.message.message || JSON.stringify(errorData.message);
          } else if (errorData.error) {
            errorMessage = typeof errorData.error === 'string' ? errorData.error : JSON.stringify(errorData.error);
          }
        }
        
        console.error(`[PIX TEST ERROR] Seller ID: ${seller.id}, Provider: wiinpay - Status HTTP inválido:`, {
          status: response.status,
          data: JSON.stringify(response.data),
          message: errorMessage,
        });
        throw new Error(`WiinPay retornou status inválido: ${errorMessage}`);
      }

      pixData = response.data;
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
          status: response.status,
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

      const response = await axios.post(apiUrl, payload, {
        headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey },
      });
      pixData = response.data;
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
      const pushinpayResponse = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, {
        headers: { Authorization: `Bearer ${seller.pushinpay_token}` },
      });
      pixData = pushinpayResponse.data;
      acquirer = 'Woovi';
      return {
        qr_code_text: pixData.qr_code,
        qr_code_base64: pixData.qr_code_base64,
        transaction_id: pushinpayResponse.data.id || pushinpayResponse.data.transaction_id || pushinpayResponse.data.identifier,
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

    let lastError = null;

    for (const provider of providerOrder) {
      try {
        const pixResult = await generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address);
        console.log(
          '[PIX][generatePixWithFallback] provider=%s transaction_id=%s qr_len=%s',
          pixResult.provider,
          pixResult.transaction_id,
          pixResult.qr_code_text ? pixResult.qr_code_text.length : 0
        );

        const [transaction] = await runQuery`
          INSERT INTO pix_transactions (
              click_id_internal, pix_value, qr_code_text, qr_code_base64,
              provider, provider_transaction_id, pix_id
          ) VALUES (
              ${click_id_internal}, ${value_cents / 100}, ${pixResult.qr_code_text},
              ${pixResult.qr_code_base64}, ${pixResult.provider},
              ${pixResult.transaction_id}, ${pixResult.transaction_id}
          ) RETURNING id`;
        console.log(
          '[PIX][generatePixWithFallback] insert ok id=%s provider_tx=%s',
          transaction.id,
          pixResult.transaction_id
        );

        pixResult.internal_transaction_id = transaction.id;
        return pixResult;
      } catch (error) {
        lastError = error;
      }
    }

    const specificMessage =
      lastError?.response?.data?.message || lastError?.message || 'Todos os provedores de PIX falharam.';
    throw new Error(`Não foi possível gerar o PIX: ${specificMessage}`);
  }

  return {
    getSyncPayAuthToken,
    generatePixForProvider,
    generatePixWithFallback,
  };
}

module.exports = { createPixService };

