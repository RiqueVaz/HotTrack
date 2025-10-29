// VERSÃO FINAL E COMPLETA - PRONTA PARA PRODUÇÃO
// VERSÃO FINAL E COMPLETA - PRONTA PARA PRODUÇÃO

// Carrega as variáveis de ambiente APENAS se não estivermos em produção (Render/Vercel)
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../.env' });
}
const express = require('express');
const cors = require('cors');
const { neon } = require('@neondatabase/serverless');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const axios = require('axios');
const FormData = require('form-data');
const http = require('http');
const https = require('https');
const path = require('path');
const crypto = require('crypto');
const webpush = require('web-push');
const { OAuth2Client } = require('google-auth-library');
const { Client } = require("@upstash/qstash");
const { Receiver } = require("@upstash/qstash");
const { MailerSend, EmailParams, Sender, Recipient } = require("mailersend");

// Configuração do Google OAuth
const googleClient = new OAuth2Client(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/google-callback.html'
);

// Configuração do MailerSend
const mailerSend = new MailerSend({
    apiKey: process.env.MAILERSEND_API_KEY,
});

const qstashClient = new Client({
  token: process.env.QSTASH_TOKEN,
});

const processDisparoWorker = require('./worker/process-disparo');
const processTimeoutWorker = require('./worker/process-timeout');

const receiver = new Receiver({
    currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
    nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
  });

const app = express();

// Configuração do servidor
const PORT = process.env.PORT || 3001;


app.post(
  '/api/worker/process-timeout',
  express.raw({ type: 'application/json' }), // 1. Ainda é OBRIGATÓRIO para pegar o corpo original
  async (req, res) => {
    try {
      // 2. Extraia as informações necessárias manualmente
      const signature = req.headers["upstash-signature"];
      const bodyString = req.body.toString(); // O Receiver espera uma string, não um Buffer

      // 3. Verifique a assinatura
      const isValid = await receiver.verify({
        signature,
        body: bodyString,
      });

      // 4. Se a assinatura for inválida, rejeite a requisição
      if (!isValid) {
        console.error("QStash Signature Verification Failed (Manual Receiver)");
        return res.status(401).send("Invalid signature");
      }

      // 5. Se for válida, prossiga: transforme a string de volta em JSON para o worker
      console.log("[WORKER] Assinatura válida. Processando a tarefa.");
      req.body = JSON.parse(bodyString);
      await processTimeoutWorker(req, res);

    } catch (error) {
      console.error("Erro crítico no handler do worker:", error);
      res.status(500).send("Internal Server Error");
    }
  }
);

app.post(
      '/api/worker/process-disparo',
      express.raw({ type: 'application/json' }), // Obrigatório para verificação do QStash
      async (req, res) => {
        try {
          // 1. Verificar a assinatura do QStash
          const signature = req.headers["upstash-signature"];
          const bodyString = req.body.toString();
    
          const isValid = await receiver.verify({
            signature,
            body: bodyString,
          });
    
          if (!isValid) {
            console.error("[WORKER-DISPARO] Verificação de assinatura do QStash falhou.");
            return res.status(401).send("Invalid signature");
          }
    
          // 2. Se for válida, processar a tarefa
          console.log("[WORKER-DISPARO] Assinatura válida. Processando disparo.");
          req.body = JSON.parse(bodyString); // Converte de volta para JSON para o worker
          await processDisparoWorker(req, res); // Chama o handler do worker
    
        } catch (error) {
          console.error("Erro crítico no handler do worker de disparo:", error);
          res.status(500).send("Internal Server Error");
        }
      }
    );
// ==========================================================
// FIM DA ROTA DO QSTASH
// ==========================================================


app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ==========================================================
//          CONFIGURAÇÃO CORS SEGURA E SELETIVA
// ==========================================================

// Origens permitidas para rotas administrativas (restritivas)
const adminAllowedOrigins = [
  process.env.FRONTEND_URL,
  process.env.ADMIN_FRONTEND_URL,
  'http://localhost:3000',
  'http://localhost:3001',  
  'http://localhost:3000/admin',
  'http://localhost:3001/admin'
].filter(Boolean);

// Cache para domínios permitidos por pressel (performance)
const allowedDomainsCache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutos

// Rate limiting simples para registerClick
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 15 * 60 * 1000; // 15 minutos
const RATE_LIMIT_MAX_REQUESTS = 100; // 100 requests por 15 minutos por IP

// Middleware de rate limiting
function rateLimitMiddleware(req, res, next) {
    const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    const now = Date.now();
    
    if (!rateLimitMap.has(ip)) {
        rateLimitMap.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
        return next();
    }
    
    const rateLimitData = rateLimitMap.get(ip);
    
    // Reset se a janela expirou
    if (now > rateLimitData.resetTime) {
        rateLimitMap.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
        return next();
    }
    
    // Verificar se excedeu o limite
    if (rateLimitData.count >= RATE_LIMIT_MAX_REQUESTS) {
        return res.status(429).json({ 
            message: 'Muitas tentativas. Tente novamente em alguns minutos.',
            retryAfter: Math.ceil((rateLimitData.resetTime - now) / 1000)
        });
    }
    
    // Incrementar contador
    rateLimitData.count++;
    next();
}

// Função para verificar se um domínio é permitido para uma pressel
async function isDomainAllowedForPressel(presselId, origin) {
  try {
    // Verificar cache primeiro
    const cacheKey = `${presselId}-${origin}`;
    const cached = allowedDomainsCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
      return cached.allowed;
    }

    // Buscar no banco de dados
    const result = await sql`
      SELECT COUNT(*) as count 
      FROM pressel_allowed_domains 
      WHERE pressel_id = ${presselId} 
      AND (domain = ${origin} OR domain = ${origin.replace(/^https?:\/\//, '')})
    `;

    const isAllowed = result[0]?.count > 0;
    
    // Atualizar cache
    allowedDomainsCache.set(cacheKey, {
      allowed: isAllowed,
      timestamp: Date.now()
    });

    return isAllowed;
  } catch (error) {
    console.error('Erro ao verificar domínio permitido:', error);
    return false;
  }
}

// Middleware CORS seletivo baseado na rota
const corsOptions = async (req, callback) => {
  const origin = req.header('Origin');
  const isPresselRoute = req.path === '/api/registerClick';
  const isVerifyEmailRoute = req.path === '/api/sellers/verify-email';
  
  if (isVerifyEmailRoute) {
    // Para verificação de email, permitir qualquer origem
    callback(null, { 
      origin: true,
      methods: ['GET', 'POST', 'OPTIONS'],
      allowedHeaders: ['Content-Type'],
      credentials: false
    });
  } else if (isPresselRoute) {
    // Para rotas de pressel, verificar domínio no banco
    const presselId = req.body?.presselId;
    
    if (presselId && origin) {
      const isAllowed = await isDomainAllowedForPressel(presselId, origin);
      
      callback(null, { 
        origin: isAllowed ? origin : false,
        methods: ['GET', 'POST', 'OPTIONS'],
        allowedHeaders: ['Content-Type'],
        credentials: false
      });
    } else {
      // Se não tem presselId ou origin, permitir (fallback para compatibilidade)
      callback(null, { 
        origin: origin,
        methods: ['GET', 'POST', 'OPTIONS'],
        allowedHeaders: ['Content-Type'],
        credentials: false
      });
    }
  } else {
    // Para outras rotas (admin), usar lista restritiva
    const isAllowed = adminAllowedOrigins.some(allowedOrigin => {
      if (typeof allowedOrigin === 'string') {
        return allowedOrigin === origin;
      } else if (allowedOrigin instanceof RegExp) {
        return allowedOrigin.test(origin);
      }
      return false;
    });
    
    callback(null, { 
      origin: isAllowed ? origin : false,
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-api-key'],
      credentials: true
    });
  }
};

app.use(cors(corsOptions));


// Agentes HTTP/HTTPS para reutilização de conexão (melhora a performance)
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

// --- OTIMIZAÇÃO CRÍTICA: A conexão com o banco é inicializada UMA VEZ e reutilizada ---
const sql = neon(process.env.DATABASE_URL);

// ==========================================================
//          VARIÁVEIS DE AMBIENTE E CONFIGURAÇÕES
// ==========================================================
const HOTTRACK_API_URL = process.env.HOTTRACK_API_URL || 'https://hottrack.vercel.app/api';
const FRONTEND_URL = process.env.FRONTEND_URL || 'https://hottrackerbot.netlify.app';
const DOCUMENTATION_URL = process.env.DOCUMENTATION_URL || 'https://documentacaohot.netlify.app';

// ==========================================================
//          LÓGICA DE RETRY PARA O BANCO DE DADOS
// ==========================================================
async function sqlWithRetry(query, params = [], retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            if (typeof query === 'string') { return await sql(query, params); }
            return await query;
        } catch (error) {
            const isRetryable = error.message.includes('fetch failed') || (error.sourceError && error.sourceError.code === 'UND_ERR_SOCKET');
            if (isRetryable && i < retries - 1) { await new Promise(res => setTimeout(res, delay)); } else { throw error; }
        }
    }
}



// --- CONFIGURAÇÃO DAS NOTIFICAÇÕES ---
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    webpush.setVapidDetails(
        process.env.VAPID_SUBJECT,
        process.env.VAPID_PUBLIC_KEY,
        process.env.VAPID_PRIVATE_KEY
    );
}
let adminSubscription = null;

// --- CONFIGURAÇÃO ---
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();

// ==========================================================
//          FUNÇÕES DO HOTBOT INTEGRADAS
// ==========================================================

// ==========================================================
//          FUNÇÕES DE INTEGRAÇÃO NETLIFY
// ==========================================================


async function createNetlifySite(accessToken, siteName) {
    try {
        const response = await axios.post('https://api.netlify.com/api/v1/sites', {
            name: siteName,
            custom_domain: null
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });
        
        console.log('[Netlify] Resposta da API ao criar site:', JSON.stringify(response.data, null, 2));
        
        const siteUrl = `https://${response.data.subdomain || response.data.name}.netlify.app`;
        console.log('[Netlify] URL gerada:', siteUrl);
        
        return {
            success: true,
            site: response.data,
            url: siteUrl
        };
    } catch (error) {
        console.error('[Netlify] Erro ao criar site:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}

async function deployToNetlify(accessToken, siteId, htmlContent, fileName = 'index.html') {
    try {
        // 1. Calcular hash SHA1 do conteúdo
        const contentHash = crypto.createHash('sha1').update(htmlContent).digest('hex');
        
        // 2. Criar deploy
        const deployResponse = await axios.post(`https://api.netlify.com/api/v1/sites/${siteId}/deploys`, {
            files: {
                [fileName]: contentHash
            }
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });
        
        const deployId = deployResponse.data.id;
        
        // 3. Upload do arquivo
        await axios.put(`https://api.netlify.com/api/v1/deploys/${deployId}/files/${fileName}`, htmlContent, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'text/html'
            }
        });
        
        return {
            success: true,
            deployId: deployId,
            url: `https://${deployResponse.data.subdomain || deployResponse.data.name}.netlify.app`
        };
    } catch (error) {
        const status = error.response?.status;
        const netlifyMsg = error.response?.data?.message || error.response?.data?.error || error.message;
        let userMessage = 'Falha ao publicar no Netlify. Tente novamente em instantes.';
        if (status === 401) {
            userMessage = 'Token Netlify inválido ou expirado. Refaça a conexão com o Netlify nas configurações.';
        } else if (status === 404) {
            userMessage = 'Site do Netlify não encontrado. Verifique o site selecionado ou crie um novo.';
        } else if (status === 422) {
            userMessage = 'Dados inválidos para deploy no Netlify (422). Revise o conteúdo/arquivo e tente novamente.';
        }
        console.error('[Netlify] Erro ao fazer deploy:', error.response?.data || error.message);
        return {
            success: false,
            statusCode: status,
            error: netlifyMsg,
            userMessage
        };
    }
}

app.post('/api/netlify/validate-token', authenticateJwt, async (req, res) => {
    const { access_token } = req.body;
    const userId = req.user?.id; // Pega o ID do usuário autenticado

    if (!access_token) {
        return res.status(400).json({ message: 'Token de acesso é obrigatório.' });
    }
    if (!userId) {
         console.error("[Netlify Validate Backend] Erro: userId não encontrado em req.user após authenticateJwt.");
         return res.status(401).json({ message: 'Usuário não autenticado ou ID não encontrado.' });
    }

    try {
        // --- INÍCIO DA LÓGICA DE VALIDAÇÃO (Substitui a chamada inexistente) ---
        let validationResult = { success: false, error: null, user: null };
        try {
            // Faz a chamada para a API do Netlify para obter informações do usuário atual
            // Uma chamada bem-sucedida indica que o token é válido.
            const netlifyApiResponse = await axios.get('https://api.netlify.com/api/v1/user', {
                headers: {
                    'Authorization': `Bearer ${access_token}`
                },
                httpsAgent: httpsAgent // Reutiliza o agente HTTPS
            });

            // Se a chamada foi bem-sucedida (status 2xx)
            validationResult = {
                success: true,
                error: null,
                user: netlifyApiResponse.data // Guarda os dados do usuário do Netlify
            };
            console.log(`[Netlify Validate] Token validado com sucesso para usuário: ${netlifyApiResponse.data.email}`);

        } catch (netlifyError) {
            // Se a chamada falhar (ex: 401 Unauthorized), o token é inválido
            console.error('[Netlify Validate] Erro ao chamar API Netlify:', netlifyError.response?.data || netlifyError.message);
            validationResult = {
                success: false,
                error: netlifyError.response?.data?.message || netlifyError.message || 'Falha na comunicação com a API Netlify.',
                user: null
            };
        }
        // --- FIM DA LÓGICA DE VALIDAÇÃO ---

        if (validationResult.success) {
            // Salva o token no banco de dados se for válido
            await sql`UPDATE sellers SET netlify_access_token = ${access_token} WHERE id = ${userId}`;

            res.json({
                success: true,
                message: 'Token válido! Configuração salva.',
                user: validationResult.user // Retorna os dados do usuário Netlify para o frontend
            });
        } else {
            res.status(400).json({
                success: false,
                message: 'Token inválido: ' + validationResult.error
            });
        }
    } catch (error) { // Este catch pega erros do SQL ou outros erros inesperados
        console.error("Erro GERAL ao validar/salvar token Netlify (backend):", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

async function getNetlifySites(accessToken) {
    try {
        const response = await axios.get('https://api.netlify.com/api/v1/sites', {
            headers: {
                'Authorization': `Bearer ${accessToken}`
            }
        });
        
        return {
            success: true,
            sites: response.data
        };
    } catch (error) {
        console.error('[Netlify] Erro ao listar sites:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}

async function deleteNetlifySite(accessToken, siteId) {
    try {
        await axios.delete(`https://api.netlify.com/api/v1/sites/${siteId}`, {
            headers: {
                'Authorization': `Bearer ${accessToken}`
            }
        });

        return {
            success: true,
            message: 'Site excluído com sucesso'
        };
    } catch (error) {
        // Se o site não existe (404), considerar como sucesso
        if (error.response?.status === 404) {
            console.log(`[Netlify] Site ${siteId} não encontrado (já excluído ou não existe)`);
            return {
                success: true,
                message: 'Site não encontrado (já excluído)'
            };
        }
        
        console.error('[Netlify] Erro ao excluir site:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}

/**
 * Gera o código HTML completo para uma página de pressel, espelhando
 * a lógica da função generatePresselCode do frontend.
 * @param {object} pressel - O objeto da pressel do banco de dados.
 * @param {Array<number>} pixelIds - Array de IDs numéricos das configurações de pixel associadas.
 * @returns {Promise<string>} O código HTML completo como string.
 */
async function generatePresselHTML(pressel, pixelIds) {
    try {
        // 1. Buscar dados dos pixels associados
        if (!Array.isArray(pixelIds) || pixelIds.length === 0) {
            throw new Error('Pelo menos um ID de pixel é necessário.');
        }
        const pixels = await sql`
            SELECT pc.pixel_id, pc.account_name
            FROM pixel_configurations pc
            WHERE pc.id = ANY(${pixelIds}) AND pc.seller_id = ${pressel.seller_id}
        `;
        if (pixels.length === 0) {
            console.warn(`[generatePresselHTML] Nenhum pixel encontrado para os IDs fornecidos: ${pixelIds} para seller ${pressel.seller_id}`);
            // Considerar lançar um erro ou retornar um HTML de erro, dependendo da regra de negócio
            // throw new Error('Configurações de pixel não encontradas.');
        }

        // 2. Buscar nome do bot
        const [bot] = await sql`
            SELECT bot_name FROM telegram_bots
            WHERE id = ${pressel.bot_id} AND seller_id = ${pressel.seller_id}
        `;
        if (!bot?.bot_name) {
            throw new Error(`Bot com ID ${pressel.bot_id} não encontrado ou sem nome para a pressel ${pressel.id}.`);
        }
        const botUsername = bot.bot_name.replace('@', '');

        // 3. Definir a URL base da API (do ambiente do backend)
        // Exemplo de ajuste para desenvolvimento local (assumindo HTTP)
        const isProduction = process.env.NODE_ENV === 'production';
        const apiBaseUrl = isProduction
            ? (process.env.HOTTRACK_API_URL)
            : 'http://localhost:3001'; // Ajuste aqui se usar HTTPS localmente

        // 4. Lógica de detecção de dispositivo
        const trafficType = pressel.traffic_type || 'both';
        let deviceDetectionCode = '';
        if (trafficType === 'mobile') {
            deviceDetectionCode = `
            // Verificar se é mobile
            const isMobile = /android|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(navigator.userAgent);
            if (!isMobile) {
                console.log('Dispositivo não móvel detectado. Redirecionando para a página branca.');
                window.location.href = CONFIG.WHITE_PAGE_URL;
                return; // Interrompe a execução
            }`;
        } else if (trafficType === 'desktop') {
            deviceDetectionCode = `
            // Verificar se é desktop
            const isMobile = /android|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(navigator.userAgent);
            if (isMobile) {
                console.log('Dispositivo móvel detectado. Redirecionando para a página branca.');
                window.location.href = CONFIG.WHITE_PAGE_URL;
                return; // Interrompe a execução
            }`;
        }

        // 5. Preparar scripts do Pixel Meta
        const pixelInitScripts = pixels.map(p => `fbq('init', '${p.pixel_id}');`).join('\n        ');
        const noscriptPixelTag = pixels.length > 0
            ? `<noscript><img height="1" width="1" style="display:none"
               src="https://www.facebook.com/tr?id=${pixels[0].pixel_id}&ev=PageView&noscript=1"
             /></noscript>`
            : '';

        // 6. Montar o HTML final usando template literals
        const htmlContent = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${pressel.name || 'Carregando...'}</title>
    <style>
        body, html {
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #1a1a1a; /* Fundo escuro */
            color: #ffffff;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        }
        .loader-container {
            text-align: center;
        }
        .spinner {
            border: 4px solid rgba(255, 255, 255, 0.2);
            border-left-color: #ffffff;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px auto;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        p {
            font-size: 1.2em;
            opacity: 0;
            animation: fadeIn 1s forwards;
            animation-delay: 0.5s;
        }
        @keyframes fadeIn {
            to { opacity: 1; }
        }
    </style>
    <script>
        !function(f,b,e,v,n,t,s)
        {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
        n.callMethod.apply(n,arguments):n.queue.push(arguments)};
        if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
        n.queue=[];t=b.createElement(e);t.async=!0;
        t.src=v;s=b.getElementsByTagName(e)[0];
        s.parentNode.insertBefore(t,s)}(window, document,'script',
        'https://connect.facebook.net/en_US/fbevents.js');
        ${pixelInitScripts}
        fbq('track', 'PageView');
    </script>
    ${noscriptPixelTag}
    <script>
        (async function() {
            const CONFIG = {
                PRESSEL_ID: ${pressel.id},
                WHITE_PAGE_URL: "${pressel.white_page_url}",
                BOT_USERNAME: "${botUsername}",
                API_BASE_URL: "${apiBaseUrl}" // <<< USA A URL DO BACKEND
            };
            function getQueryParam(param) {
                const urlParams = new URLSearchParams(window.location.search);
                return urlParams.get(param);
            }
            function getFacebookCookies() {
                const cookies = document.cookie.split('; ');
                let fbp = null, fbc = null;
                for (const cookie of cookies) {
                    const parts = cookie.split('=');
                    if (parts[0] === '_fbp') fbp = parts[1];
                    if (parts[0] === '_fbc') fbc = parts[1];
                }
                return { fbp, fbc };
            }
            try {
                // Bloqueia bots conhecidos do Facebook
                const userAgent = navigator.userAgent;
                if (/facebookexternalhit|Facebot/i.test(userAgent)) {
                    console.log('Bot do Facebook detectado. Redirecionando...');
                    window.location.href = CONFIG.WHITE_PAGE_URL;
                    return; // Interrompe a execução
                }

                ${deviceDetectionCode} // <<< INJETA O CÓDIGO DE DETECÇÃO

                const { fbp, fbc } = getFacebookCookies();
                // Construir payload para /api/registerClick
                const clickPayload = {
                    presselId: CONFIG.PRESSEL_ID,
                    referer: document.referrer || null,
                    fbclid: getQueryParam('fbclid'),
                    fbp: fbp,
                    fbc: fbc,
                    user_agent: navigator.userAgent,
                    utm_source: getQueryParam('utm_source'),
                    utm_campaign: getQueryParam('utm_campaign'),
                    utm_medium: getQueryParam('utm_medium'),
                    utm_content: getQueryParam('utm_content'),
                    utm_term: getQueryParam('utm_term')
                };

                const response = await fetch(CONFIG.API_BASE_URL + '/api/registerClick', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(clickPayload)
                });
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.message || 'Falha ao registrar clique.');
                }
                const { click_id } = await response.json();

                // Dispara ViewContent no Pixel da Meta com o eventID
                if (typeof fbq !== 'undefined') {
                    fbq('track', 'ViewContent', {}, { eventID: click_id });
                    console.log('Evento ViewContent disparado com eventID:', click_id);
                } else {
                    console.warn('fbq não definido. Não foi possível disparar ViewContent.');
                }

                const botUrl = \`https://t.me/\${CONFIG.BOT_USERNAME}?start=\${click_id}\`;
                window.location.href = botUrl;
            } catch (error) {
                console.error("Erro na lógica da pressel:", error);
                window.location.href = CONFIG.WHITE_PAGE_URL; // Redireciona para página branca em caso de erro
            }
        })();
    </script>
</head>
<body>
    <div class="loader-container">
        <div class="spinner"></div>
        <p>Aguarde, estamos redirecionando...</p>
    </div>
</body>
</html>`;

        return htmlContent;

    } catch (error) {
        console.error(`[generatePresselHTML] Erro ao gerar HTML para pressel ${pressel?.id}:`, error);
        // Retornar um HTML de erro ou relançar a exceção
        throw new Error(`Falha ao gerar o código HTML da pressel: ${error.message}`);
    }
}

// ==========================================================
//          FUNÇÕES DO HOTBOT INTEGRADAS
// ==========================================================
async function sendTelegramRequest(botToken, method, data, options = {}, retries = 3, delay = 1500) {
    const { headers = {}, responseType = 'json', timeout = 30000 } = options;
    const apiUrl = `https://api.telegram.org/bot${botToken}/${method}`;

    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, {
                headers,
                responseType,
                httpAgent,
                httpsAgent,
                timeout
            });
            return response.data;
        } catch (error) {
            const chatId = data instanceof FormData ? data.getBoundary && data.get('chat_id') : data.chat_id;

            if (error.response && error.response.status === 403) {
                console.warn(`[TELEGRAM API WARN] O bot foi bloqueado pelo usuário. ChatID: ${chatId}`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento específico para TOPIC_CLOSED
            if (error.response && error.response.status === 400 && 
                error.response.data?.description?.includes('TOPIC_CLOSED')) {
                console.warn(`[TELEGRAM API WARN] Chat de grupo fechado. ChatID: ${chatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }

            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');

            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }
            
            const errorData = error.response?.data;
            const errorMessage = (errorData instanceof ArrayBuffer)
                ? JSON.parse(Buffer.from(errorData).toString('utf8'))
                : errorData;

            console.error(`[TELEGRAM API ERROR] Method: ${method}, ChatID: ${chatId}:`, errorMessage || error.message);
            throw error;
        }
    }
}

async function saveMessageToDb(sellerId, botId, message, senderType) {
    const { message_id, chat, from, text, photo, video, voice } = message;
    let mediaType = null;
    let mediaFileId = null;
    let messageText = text;
    let newClickId = null;

    if (text && text.startsWith('/start ')) {
        newClickId = text.substring(7);
    }

    let finalClickId = newClickId;
    if (!finalClickId) {
        const result = await sqlWithRetry(
            'SELECT click_id FROM telegram_chats WHERE chat_id = $1 AND bot_id = $2 AND click_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
            [chat.id, botId]
        );
        if (result.length > 0) {
            finalClickId = result[0].click_id;
        }
    }

    if (photo) {
        mediaType = 'photo';
        mediaFileId = photo[photo.length - 1].file_id;
        messageText = message.caption || '[Foto]';
    } else if (video) {
        mediaType = 'video';
        mediaFileId = video.file_id;
        messageText = message.caption || '[Vídeo]';
    } else if (voice) {
        mediaType = 'voice';
        mediaFileId = voice.file_id;
        messageText = '[Mensagem de Voz]';
    }
    const botInfo = senderType === 'bot' ? { first_name: 'Bot', last_name: '(Automação)' } : {};
    const fromUser = from || chat;

    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (chat_id, message_id) DO NOTHING;
    `, [sellerId, botId, chat.id, message_id, fromUser.id, fromUser.first_name || botInfo.first_name, fromUser.last_name || botInfo.last_name, fromUser.username || null, messageText, senderType, mediaType, mediaFileId, finalClickId]);

    if (newClickId) {
        await sqlWithRetry(
            'UPDATE telegram_chats SET click_id = $1 WHERE chat_id = $2 AND bot_id = $3',
            [newClickId, chat.id, botId]
        );
    }
}

async function replaceVariables(text, variables) {
    if (!text) return '';
    let processedText = text;
    for (const key in variables) {
        const regex = new RegExp(`{{${key}}}`, 'g');
        processedText = processedText.replace(regex, variables[key]);
    }
    return processedText;
}

async function sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption) {
    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    if (!storageBotToken) throw new Error('Token do bot de armazenamento não configurado.');

    const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { file_id: fileId });
    if (!fileInfo.ok) throw new Error('Não foi possível obter informações do arquivo da biblioteca.');

    const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;

    const { data: fileBuffer, headers: fileHeaders } = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    
    const formData = new FormData();
    formData.append('chat_id', chatId);
    if (caption) {
        formData.append('caption', caption);
    }

    const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
    const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
    const fileNameMap = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' };

    const method = methodMap[fileType];
    const field = fieldMap[fileType];
    const fileName = fileNameMap[fileType];
    const timeout = fileType === 'video' ? 60000 : 30000;

    if (!method) throw new Error('Tipo de arquivo não suportado.');

    formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

    return await sendTelegramRequest(destinationBotToken, method, formData, { headers: formData.getHeaders(), timeout });
}

async function handleMediaNode(node, botToken, chatId, caption) {
    const type = node.type;
    const nodeData = node.data || {};
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    const fileIdentifier = nodeData[urlMap[type]];

    if (!fileIdentifier) {
        console.warn(`[Flow Media] Nenhum file_id ou URL fornecido para o nó de ${type} ${node.id}`);
        return null;
    }

    const isLibraryFile = fileIdentifier.startsWith('BAAC') || fileIdentifier.startsWith('AgAC') || fileIdentifier.startsWith('AwAC');
    let response;
    const timeout = type === 'video' ? 60000 : 30000;

    if (isLibraryFile) {
        if (type === 'audio') {
            const duration = parseInt(nodeData.durationInSeconds, 10) || 0;
            if (duration > 0) {
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'record_voice' });
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            }
        }
        response = await sendMediaAsProxy(botToken, chatId, fileIdentifier, type, caption);
    } else {
        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
        
        const method = methodMap[type];
        const field = fieldMap[type];
        
        const payload = { chat_id: chatId, [field]: fileIdentifier, caption };
        response = await sendTelegramRequest(botToken, method, payload, { timeout });
    }
    
    return response;
}

// --- MIDDLEWARE DE AUTENTICAÇÃO ---
async function authenticateJwt(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ message: 'Token não fornecido.' });
    
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ message: 'Token inválido ou expirado.' });
        req.user = user;
        next();
    });
}

// --- MIDDLEWARE DE LOG DE REQUISIÇÕES ---
async function logApiRequest(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) return next();
    try {
        const sellerResult = await sql`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length > 0) {
            sql`INSERT INTO api_requests (seller_id, endpoint) VALUES (${sellerResult[0].id}, ${req.path})`.catch(err => console.error("Falha ao logar requisição:", err));
        }
    } catch (error) {
        console.error("Erro no middleware de log:", error);
    }
    next();
}

// --- FUNÇÕES DE LÓGICA DE NEGÓCIO ---
async function getSyncPayAuthToken(seller) {
    const cachedToken = syncPayTokenCache.get(seller.id);
    if (cachedToken && cachedToken.expiresAt > Date.now() + 60000) {
        return cachedToken.accessToken;
    }
    if (!seller.syncpay_client_id || !seller.syncpay_client_secret) {
        throw new Error('Credenciais da SyncPay não configuradas para este vendedor.');
    }
    console.log(`[SyncPay] Solicitando novo token para o vendedor ID: ${seller.id}`);
    const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/auth-token`, {
        client_id: seller.syncpay_client_id,
        client_secret: seller.syncpay_client_secret,
    });
    const { access_token, expires_in } = response.data;
    const expiresAt = Date.now() + (expires_in * 1000);
    syncPayTokenCache.set(seller.id, { accessToken: access_token, expiresAt });
    return access_token;
}

// NOVA FUNÇÃO REUTILIZÁVEL PARA GERAR PIX COM FALLBACK
async function generatePixWithFallback(seller, value_cents, host, apiKey, ip_address, click_id_internal) {
    const providerOrder = [
        seller.pix_provider_primary,
        seller.pix_provider_secondary,
        seller.pix_provider_tertiary
    ].filter(Boolean); // Remove nulos ou vazios

    if (providerOrder.length === 0) {
        throw new Error('Nenhum provedor de PIX configurado para este vendedor.');
    }

    let lastError = null;

    for (const provider of providerOrder) {
        try {
            console.log(`[PIX Fallback] Tentando gerar PIX com ${provider.toUpperCase()} para ${value_cents} centavos.`);
            const pixResult = await generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address);
            console.log(`[PIX Fallback] SUCESSO com ${provider.toUpperCase()}. Transaction ID: ${pixResult.transaction_id}`);

            // Salvar a transação no banco AQUI DENTRO da função de fallback
            // Isso garante que a transação só é salva se a geração for bem-sucedida
            const [transaction] = await sql`
                INSERT INTO pix_transactions (
                    click_id_internal, pix_value, qr_code_text, qr_code_base64,
                    provider, provider_transaction_id, pix_id
                ) VALUES (
                    ${click_id_internal}, ${value_cents / 100}, ${pixResult.qr_code_text},
                    ${pixResult.qr_code_base64}, ${pixResult.provider},
                    ${pixResult.transaction_id}, ${pixResult.transaction_id}
                ) RETURNING id`;

             // Adiciona o ID interno da transação salva ao resultado para uso posterior
            pixResult.internal_transaction_id = transaction.id;

            return pixResult; // Retorna o resultado SUCESSO

        } catch (error) {
            console.error(`[PIX Fallback] FALHA ao gerar PIX com ${provider.toUpperCase()}:`, error.response?.data?.message || error.message);
            lastError = error; // Guarda o erro para o caso de todos falharem
        }
    }

    // Se o loop terminar sem sucesso, lança o último erro ocorrido
    console.error(`[PIX Fallback FINAL ERROR] Seller ID: ${seller?.id} - Todas as tentativas de geração PIX falharam.`);
    // Tenta repassar a mensagem de erro mais específica do provedor, se disponível
    const specificMessage = lastError.response?.data?.message || lastError.message || 'Todos os provedores de PIX falharam.';
    throw new Error(`Não foi possível gerar o PIX: ${specificMessage}`);
}

async function generatePixForProvider(provider, seller, value_cents, host, apiKey, ip_address) {
    let pixData;
    let acquirer = 'Não identificado';
    const commission_rate = seller.commission_rate || 0.0299;
    
    const clientPayload = {
        document: { number: "21376710773", type: "CPF" },
        name: "Cliente Padrão",
        email: "gabriel@email.com",
        phone: "27995310379"
    };
    
    if (provider === 'brpix') {
        if (!seller.brpix_secret_key || !seller.brpix_company_id) {
            throw new Error('Credenciais da BR PIX não configuradas para este vendedor.');
        }
        const credentials = Buffer.from(`${seller.brpix_secret_key}:${seller.brpix_company_id}`).toString('base64');
        
        const payload = {
            customer: clientPayload,
            items: [{ title: "Produto Digital", unitPrice: parseInt(value_cents, 10), quantity: 1 }],
            paymentMethod: "PIX",
            amount: parseInt(value_cents, 10),
            pix: { expiresInDays: 1 },
            ip: ip_address
        };

        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && BRPIX_SPLIT_RECIPIENT_ID) {
            payload.split = [{ recipientId: BRPIX_SPLIT_RECIPIENT_ID, amount: commission_cents }];
        }
        const response = await axios.post('https://api.brpixdigital.com/functions/v1/transactions', payload, {
            headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/json' }
        });
        pixData = response.data;
        acquirer = "BRPix";
        return {
            qr_code_text: pixData.pix.qrcode, // Alterado de pixData.pix.qrcodeText
            qr_code_base64: pixData.pix.qrcode, // Mantido para consistência com a resposta atual
            transaction_id: pixData.id,
            acquirer,
            provider
        };
    } else if (provider === 'syncpay') {
        const token = await getSyncPayAuthToken(seller);
        const payload = { 
            amount: value_cents / 100, 
            payer: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" },
            callbackUrl: `https://${host}/api/webhook/syncpay`
        };
        const commission_percentage = commission_rate * 100;
        if (apiKey !== ADMIN_API_KEY && process.env.SYNCPAY_SPLIT_ACCOUNT_ID) {
            payload.split = [{
                percentage: Math.round(commission_percentage), 
                user_id: process.env.SYNCPAY_SPLIT_ACCOUNT_ID 
            }];
        }
        const response = await axios.post(`${SYNCPAY_API_BASE_URL}/api/partner/v1/cash-in`, payload, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        pixData = response.data;
        acquirer = "SyncPay";
        return { 
            qr_code_text: pixData.pix_code, 
            qr_code_base64: null, 
            transaction_id: pixData.identifier, 
            acquirer, 
            provider 
        };
    } else if (provider === 'cnpay' || provider === 'oasyfy') {
        const isCnpay = provider === 'cnpay';
        const publicKey = isCnpay ? seller.cnpay_public_key : seller.oasyfy_public_key;
        const secretKey = isCnpay ? seller.cnpay_secret_key : seller.oasyfy_secret_key;
        if (!publicKey || !secretKey) throw new Error(`Credenciais para ${provider.toUpperCase()} não configuradas.`);
        const apiUrl = isCnpay ? 'https://painel.appcnpay.com/api/v1/gateway/pix/receive' : 'https://app.oasyfy.com/api/v1/gateway/pix/receive';
        const splitId = isCnpay ? CNPAY_SPLIT_PRODUCER_ID : OASYFY_SPLIT_PRODUCER_ID;
        const payload = {
            identifier: uuidv4(),
            amount: value_cents / 100,
            client: { name: "Cliente Padrão", email: "gabriel@gmail.com", document: "21376710773", phone: "27995310379" },
            callbackUrl: `https://${host}/api/webhook/${provider}`
        };
        const commission = parseFloat(((value_cents / 100) * commission_rate).toFixed(2));
        if (apiKey !== ADMIN_API_KEY && commission > 0 && splitId) {
            payload.splits = [{ producerId: splitId, amount: commission }];
        }
        const response = await axios.post(apiUrl, payload, { headers: { 'x-public-key': publicKey, 'x-secret-key': secretKey } });
        pixData = response.data;
        acquirer = isCnpay ? "CNPay" : "Oasy.fy";
        return { qr_code_text: pixData.pix.code, qr_code_base64: pixData.pix.base64, transaction_id: pixData.transactionId, acquirer, provider };
    } else { // Padrão é PushinPay
        if (!seller.pushinpay_token) throw new Error(`Token da PushinPay não configurado.`);
        const payload = {
            value: value_cents,
            webhook_url: `https://${host}/api/webhook/pushinpay`,
        };
        const commission_cents = Math.floor(value_cents * commission_rate);
        if (apiKey !== ADMIN_API_KEY && commission_cents > 0 && PUSHINPAY_SPLIT_ACCOUNT_ID) {
            payload.split_rules = [{ value: commission_cents, account_id: PUSHINPAY_SPLIT_ACCOUNT_ID }];
        }
        const pushinpayResponse = await axios.post('https://api.pushinpay.com.br/api/pix/cashIn', payload, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
        pixData = pushinpayResponse.data;
        acquirer = "Woovi";
        return { qr_code_text: pixData.qr_code, qr_code_base64: pixData.qr_code_base64, transaction_id: pixData.id, acquirer, provider: 'pushinpay' };
    }
}

async function handleSuccessfulPayment(transaction_id, customerData) {
    try {
        const [transaction] = await sql`UPDATE pix_transactions SET status = 'paid', paid_at = NOW() WHERE id = ${transaction_id} AND status != 'paid' RETURNING *`;
        if (!transaction) { 
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já processada ou não encontrada.`);
            return; 
        }

        console.log(`[handleSuccessfulPayment] Processando pagamento para transação ${transaction_id}.`);

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
        
        const [click] = await sql`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${click.seller_id}`;

        if (click && seller) {
            const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
            const productData = { id: "prod_final", name: "Produto Vendido" };

            await sendEventToUtmify('paid', click, transaction, seller, finalCustomerData, productData);
            await sendMetaEvent('Purchase', click, transaction, finalCustomerData);
        } else {
            console.error(`[handleSuccessfulPayment] ERRO: Não foi possível encontrar dados do clique ou vendedor para a transação ${transaction_id}`);
        }
    } catch(error) {
        console.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
    }
}

// --- MIDDLEWARE DE AUTENTICAÇÃO POR API KEY ---
async function authenticateApiKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) {
        return res.status(401).json({ message: 'Chave de API não fornecida.' });
    }
    try {
        const sellerResult = await sql`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            return res.status(401).json({ message: 'Chave de API inválida.' });
        }
        req.sellerId = sellerResult[0].id;
        next();
    } catch (error) {
        console.error("Erro na autenticação por API Key:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
}

// --- ROTAS DO PAINEL ADMINISTRATIVO ---
function authenticateAdmin(req, res, next) {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ message: 'Acesso negado. Chave de administrador inválida.' });
    }
    next();
}

// Endpoint para validar chave de admin
// Endpoint para fornecer configurações ao frontend
app.get('/api/config', (req, res) => {
    // Usa variável de ambiente se definida, senão detecta automaticamente
    const apiBaseUrl = process.env.API_BASE_URL || (() => {
        const isProduction = process.env.NODE_ENV === 'production';
        if (isProduction) {
            const protocol = req.headers['x-forwarded-proto'] || 'https';
            const host = req.headers['host'];
            return `${protocol}://${host}`;
        }
        return `http://localhost:${PORT}`;
    })();

    res.json({
        apiBaseUrl: apiBaseUrl,
        environment: process.env.NODE_ENV || 'development'
    });
});

app.get('/api/pix-status/:transaction_id', async (req, res) => {
    const { transaction_id } = req.params;

    if (!transaction_id) {
        return res.status(400).json({ error: 'ID da transação não fornecido.' });
    }

    try {
        const result = await sql`
            SELECT status, pix_value FROM pix_transactions WHERE provider_transaction_id = ${transaction_id}
        `;

        if (result.length === 0) {
            return res.status(404).json({ error: 'Transação não encontrada.' });
        }

        const transaction = result[0];
        res.status(200).json({ 
            status: transaction.status, 
            pix_value: transaction.pix_value 
        });

    } catch (error) {
        console.error('Erro ao consultar status do PIX:', error);
        res.status(500).json({ error: 'Erro interno ao verificar o status do PIX.' });
    }
});

app.post('/api/admin/validate-key', (req, res) => {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ 
            message: 'Chave de administrador inválida.',
            valid: false 
        });
    }
    res.status(200).json({ 
        message: 'Chave válida.',
        valid: true,
        timestamp: new Date().toISOString()
    });
});

async function sendHistoricalMetaEvent(eventName, clickData, transactionData, targetPixel) {
    let payload_sent = null;
    try {
        const userData = {};
        if (clickData.ip_address) userData.client_ip_address = clickData.ip_address;
        if (clickData.user_agent) userData.client_user_agent = clickData.user_agent;
        if (clickData.fbp) userData.fbp = clickData.fbp;
        if (clickData.fbc) userData.fbc = clickData.fbc;
        if (clickData.firstName) userData.fn = crypto.createHash('sha256').update(clickData.firstName.toLowerCase()).digest('hex');
        if (clickData.lastName) userData.ln = crypto.createHash('sha256').update(clickData.lastName.toLowerCase()).digest('hex');
        
        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        const { pixelId, accessToken } = targetPixel;
        const event_time = Math.floor(new Date(transactionData.paid_at).getTime() / 1000);
        const event_id = `${eventName}.${transactionData.id}.${pixelId}`;

        const payload = {
            data: [{
                event_name: eventName,
                event_time: event_time,
                event_id,
                action_source: 'other',
                user_data: userData,
                custom_data: {
                    currency: 'BRL',
                    value: parseFloat(transactionData.pix_value)
                },
            }]
        };
        payload_sent = payload;

        if (Object.keys(userData).length === 0) {
            throw new Error('Dados de usuário insuficientes para envio (IP/UserAgent faltando).');
        }

        await axios.post(`https://graph.facebook.com/v19.0/${pixelId}/events`, payload, { params: { access_token: accessToken } });
        
        return { success: true, payload: payload_sent };

    } catch (error) {
        const metaError = error.response?.data?.error || { message: error.message };
        console.error(`Erro ao reenviar evento (Transação ID: ${transactionData.id}):`, metaError.message);
        return { success: false, error: metaError, payload: payload_sent };
    }
}

app.post('/api/admin/resend-events', authenticateAdmin, async (req, res) => {
    const { 
        target_pixel_id, target_meta_api_token, seller_id, 
        start_date, end_date, page = 1, limit = 50
    } = req.body;

    if (!target_pixel_id || !target_meta_api_token || !start_date || !end_date) {
        return res.status(400).json({ message: 'Todos os campos obrigatórios devem ser preenchidos.' });
    }

    try {
        const query = seller_id
            ? sql`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND c.seller_id = ${seller_id} AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`
            : sql`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`;
        
        const allPaidTransactions = await query;
        
        if (allPaidTransactions.length === 0) {
            return res.status(200).json({ 
                total_events: 0, 
                total_pages: 0, 
                message: 'Nenhuma transação paga encontrada para os filtros fornecidos.' 
            });
        }

        const clickIds = allPaidTransactions.map(t => t.click_id).filter(Boolean);
        let userDataMap = new Map();
        if (clickIds.length > 0) {
            const telegramUsers = await sql`
                SELECT click_id, first_name, last_name 
                FROM telegram_chats 
                WHERE click_id = ANY(${clickIds})
            `;
            telegramUsers.forEach(user => {
                const cleanClickId = user.click_id.startsWith('/start ') ? user.click_id : `/start ${user.click_id}`;
                userDataMap.set(cleanClickId, { firstName: user.first_name, lastName: user.last_name });
            });
        }

        const totalEvents = allPaidTransactions.length;
        const totalPages = Math.ceil(totalEvents / limit);
        const offset = (page - 1) * limit;
        const batch = allPaidTransactions.slice(offset, offset + limit);
        
        const detailedResults = [];
        const targetPixel = { pixelId: target_pixel_id, accessToken: target_meta_api_token };

        console.log(`[ADMIN] Processando página ${page}/${totalPages}. Lote com ${batch.length} eventos.`);

        for (const transaction of batch) {
            const extraUserData = userDataMap.get(transaction.click_id);
            const enrichedTransactionData = { ...transaction, ...extraUserData };
            const result = await sendHistoricalMetaEvent('Purchase', enrichedTransactionData, transaction, targetPixel);
            
            detailedResults.push({
                transaction_id: transaction.id,
                status: result.success ? 'success' : 'failure',
                payload_sent: result.payload,
                meta_response: result.error || 'Enviado com sucesso.'
            });
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        res.status(200).json({
            total_events: totalEvents,
            total_pages: totalPages,
            current_page: page,
            limit: limit,
            results: detailedResults
        });

    } catch (error) {
        console.error("Erro geral na rota de reenviar eventos:", error);
        res.status(500).json({ message: 'Erro interno do servidor ao processar o reenvio.' });
    }
});

app.get('/api/admin/vapidPublicKey', authenticateAdmin, (req, res) => {
    if (!process.env.VAPID_PUBLIC_KEY) {
        return res.status(500).send('VAPID Public Key não configurada no servidor.');
    }
    res.type('text/plain').send(process.env.VAPID_PUBLIC_KEY);
});

app.post('/api/admin/save-subscription', authenticateAdmin, (req, res) => {
    adminSubscription = req.body;
    console.log("Inscrição de admin para notificações recebida e guardada.");
    res.status(201).json({});
});

app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
    try {
        const totalSellers = await sql`SELECT COUNT(*) FROM sellers;`;
        const paidTransactions = await sql`SELECT COUNT(*) as count, SUM(pix_value) as total_revenue FROM pix_transactions WHERE status = 'paid';`;
        const total_sellers = parseInt(totalSellers[0].count);
        const total_paid_transactions = parseInt(paidTransactions[0].count);
        const total_revenue = parseFloat(paidTransactions[0].total_revenue || 0);
        const saas_profit = total_revenue * 0.0299;
        res.json({
            total_sellers, total_paid_transactions,
            total_revenue: total_revenue.toFixed(2),
            saas_profit: saas_profit.toFixed(2)
        });
    } catch (error) {
        console.error("Erro no dashboard admin:", error);
        res.status(500).json({ message: 'Erro ao buscar dados do dashboard.' });
    }
});
app.get('/api/admin/ranking', authenticateAdmin, async (req, res) => {
    try {
        const ranking = await sql`
            SELECT s.id, s.name, s.email, COUNT(pt.id) AS total_sales, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name, s.email ORDER BY total_revenue DESC LIMIT 20;`;
        res.json(ranking);
    } catch (error) {
        console.error("Erro no ranking de sellers:", error);
        res.status(500).json({ message: 'Erro ao buscar ranking.' });
    }
});
// No seu backend.js
app.get('/api/admin/sellers', authenticateAdmin, async (req, res) => {
      try {
        const sellers = await sql`SELECT id, name, email, created_at, is_active, commission_rate FROM sellers ORDER BY created_at DESC;`;
        res.json(sellers);
      } catch (error) {
        res.status(500).json({ message: 'Erro ao listar vendedores.' });
      }
    });
app.post('/api/admin/sellers/:id/toggle-active', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { isActive } = req.body;
    try {
        await sql`UPDATE sellers SET is_active = ${isActive} WHERE id = ${id};`;
        res.status(200).json({ message: `Usuário ${isActive ? 'ativado' : 'bloqueado'} com sucesso.` });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar status do usuário.' });
    }
});
app.put('/api/admin/sellers/:id/password', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { newPassword } = req.body;
    if (!newPassword || newPassword.length < 8) return res.status(400).json({ message: 'A nova senha deve ter pelo menos 8 caracteres.' });
    try {
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        await sql`UPDATE sellers SET password_hash = ${hashedPassword} WHERE id = ${id};`;
        res.status(200).json({ message: 'Senha alterada com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar senha.' });
    }
});
app.put('/api/admin/sellers/:id/credentials', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { pushinpay_token, cnpay_public_key, cnpay_secret_key } = req.body;
    try {
        await sql`
            UPDATE sellers 
            SET pushinpay_token = ${pushinpay_token}, cnpay_public_key = ${cnpay_public_key}, cnpay_secret_key = ${cnpay_secret_key}
            WHERE id = ${id};`;
        res.status(200).json({ message: 'Credenciais alteradas com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar credenciais.' });
    }
});
app.get('/api/admin/transactions', authenticateAdmin, async (req, res) => {
    try {
        const page = parseInt(req.query.page || 1);
        const limit = parseInt(req.query.limit || 20);
        const offset = (page - 1) * limit;
        const transactions = await sql`
            SELECT pt.id, pt.status, pt.pix_value, pt.provider, pt.created_at, s.name as seller_name, s.email as seller_email
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            JOIN sellers s ON c.seller_id = s.id ORDER BY pt.created_at DESC
            LIMIT ${limit} OFFSET ${offset};`;
         const totalTransactionsResult = await sql`SELECT COUNT(*) FROM pix_transactions;`;
         const total = parseInt(totalTransactionsResult[0].count);
        res.json({ transactions, total, page, pages: Math.ceil(total / limit), limit });
    } catch (error) {
        console.error("Erro ao buscar transações admin:", error);
        res.status(500).json({ message: 'Erro ao buscar transações.' });
    }
});
app.get('/api/admin/usage-analysis', authenticateAdmin, async (req, res) => {
    try {
        const usageData = await sql`
            SELECT
                s.id, s.name, s.email,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '1 hour') AS requests_last_hour,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '24 hours') AS requests_last_24_hours
            FROM sellers s
            LEFT JOIN api_requests ar ON s.id = ar.seller_id
            GROUP BY s.id, s.name, s.email
            ORDER BY requests_last_24_hours DESC, requests_last_hour DESC;
        `;
        res.json(usageData);
    } catch (error) {
        console.error("Erro na análise de uso:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de uso.' });
    }
});

app.put('/api/admin/sellers/:id/commission', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { commission_rate } = req.body;

    if (typeof commission_rate !== 'number' || commission_rate < 0 || commission_rate > 1) {
        return res.status(400).json({ message: 'A taxa de comissão deve ser um número entre 0 e 1 (ex: 0.0299 para 2.99%).' });
    }

    try {
        await sql`UPDATE sellers SET commission_rate = ${commission_rate} WHERE id = ${id};`;
        res.status(200).json({ message: 'Comissão do usuário atualizada com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar comissão:", error);
        res.status(500).json({ message: 'Erro ao atualizar a comissão.' });
    }
});

// ==========================================================
//          ROTAS ÚNICAS DO HOTBOT INTEGRADAS
// ==========================================================
app.get('/api/health', async (req, res) => {
    try {
        await sqlWithRetry('SELECT 1 as status;');
        res.status(200).json({ status: 'ok' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: 'Erro de conexão ao BD.' });
    }
});

// --- ROTAS DE FLOWS ---
app.get('/api/flows', authenticateJwt, async (req, res) => {
    try {
        const flows = await sqlWithRetry('SELECT * FROM flows WHERE seller_id = $1 ORDER BY created_at DESC', [req.user.id]);
        res.status(200).json(flows.map(f => ({ ...f, nodes: f.nodes || { nodes: [], edges: [] } })));
    } catch (error) { res.status(500).json({ message: 'Erro ao buscar os fluxos.' }); }
});

app.post('/api/flows', authenticateJwt, async (req, res) => {
    const { name, botId } = req.body;
    if (!name || !botId) return res.status(400).json({ message: 'Nome e ID do bot são obrigatórios.' });
    try {
        const initialFlow = { nodes: [{ id: 'start', type: 'trigger', position: { x: 250, y: 50 }, data: {} }], edges: [] };
        const [newFlow] = await sqlWithRetry(`
            INSERT INTO flows (seller_id, bot_id, name, nodes) VALUES ($1, $2, $3, $4) RETURNING *;`, [req.user.id, botId, name, JSON.stringify(initialFlow)]);
        res.status(201).json(newFlow);
    } catch (error) { res.status(500).json({ message: 'Erro ao criar o fluxo.' }); }
});

app.put('/api/flows/:id', authenticateJwt, async (req, res) => {
    const { name, nodes } = req.body;
    if (!name || !nodes) return res.status(400).json({ message: 'Nome e estrutura de nós são obrigatórios.' });
    try {
        const [updated] = await sqlWithRetry('UPDATE flows SET name = $1, nodes = $2, updated_at = NOW() WHERE id = $3 AND seller_id = $4 RETURNING *;', [name, nodes, req.params.id, req.user.id]);
        if (updated) res.status(200).json(updated);
        else res.status(404).json({ message: 'Fluxo não encontrado.' });
    } catch (error) { res.status(500).json({ message: 'Erro ao salvar o fluxo.' }); }
});

app.delete('/api/flows/:id', authenticateJwt, async (req, res) => {
    try {

        
        // Primeiro verificar se o flow existe e pertence ao usuário
        const [existingFlow] = await sqlWithRetry('SELECT id FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);

        
        if (!existingFlow) {
            console.log('Flow não encontrado ou não pertence ao usuário');
            return res.status(404).json({ message: 'Fluxo não encontrado.' });
        }
        
        // Se existe, deletar
        const result = await sqlWithRetry('DELETE FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);

        res.status(204).send();
        
    } catch (error) { 
        console.error('Erro ao deletar flow:', error);
        res.status(500).json({ message: 'Erro ao deletar o fluxo.' }); 
    }
});

// --- ROTAS DE CHATS ---
app.get('/api/chats/:botId', authenticateJwt, async (req, res) => {
    try {
        const users = await sqlWithRetry(`
            SELECT 
                t.chat_id,
                (array_agg(t.first_name ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as first_name,
                (array_agg(t.last_name ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as last_name,
                (array_agg(t.username ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as username,
                (array_agg(t.click_id ORDER BY t.created_at DESC) FILTER (WHERE t.click_id IS NOT NULL))[1] as click_id,
                MAX(t.created_at) as last_message_at
            FROM telegram_chats t
            WHERE t.bot_id = $1 AND t.seller_id = $2
            GROUP BY t.chat_id
            ORDER BY last_message_at DESC;
        `, [req.params.botId, req.user.id]);
        res.status(200).json(users);
    } catch (error) { 
        res.status(500).json({ message: 'Erro ao buscar usuários do chat.' }); 
    }
});

app.get('/api/chats/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        const messages = await sqlWithRetry(`
            SELECT * FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND seller_id = $3 ORDER BY created_at ASC;`, [req.params.botId, req.params.chatId, req.user.id]);
        res.status(200).json(messages);
    } catch (error) { res.status(500).json({ message: 'Erro ao buscar mensagens.' }); }
});

app.post('/api/chats/:botId/send-message', authenticateJwt, async (req, res) => {
    const { chatId, text } = req.body;
    if (!chatId || !text) return res.status(400).json({ message: 'Chat ID e texto são obrigatórios.' });
    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [req.params.botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });
        const response = await sendTelegramRequest(bot.bot_token, 'sendMessage', { chat_id: chatId, text });
        if (response.ok) {
            await saveMessageToDb(req.user.id, req.params.botId, response.result, 'operator');
        }
        res.status(200).json({ message: 'Mensagem enviada!' });
    } catch (error) { res.status(500).json({ message: 'Não foi possível enviar a mensagem.' }); }
});

app.post('/api/chats/:botId/send-library-media', authenticateJwt, async (req, res) => {
    const { chatId, fileId, fileType } = req.body;
    const { botId } = req.params;

    if (!chatId || !fileId || !fileType) {
        return res.status(400).json({ message: 'Dados incompletos.' });
    }

    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });

        const response = await sendMediaAsProxy(bot.bot_token, chatId, fileId, fileType, null);

        if (response.ok) {
            await saveMessageToDb(req.user.id, botId, response.result, 'operator');
            res.status(200).json({ message: 'Mídia enviada!' });
        } else {
            throw new Error('Falha ao enviar mídia para o usuário final.');
        }
    } catch (error) {
        res.status(500).json({ message: 'Não foi possível enviar a mídia: ' + error.message });
    }
});

// --- ROTAS GERAIS DE USUÁRIO ---
app.post('/api/sellers/register', async (req, res) => {
    const { name, email, password } = req.body;

    if (!name || !email || !password || password.length < 8) {
        return res.status(400).json({ message: 'Dados inválidos. Nome, email e senha (mínimo 8 caracteres) são obrigatórios.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const existingSeller = await sql`SELECT id FROM sellers WHERE LOWER(email) = ${normalizedEmail}`;
        if (existingSeller.length > 0) {
            return res.status(409).json({ message: 'Este email já está em uso.' });
        }

        // Adicionar campos de verificação se não existirem
        try {
            await sql`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE`;
            await sql`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS verification_code TEXT`;
            await sql`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS verification_expires TIMESTAMP`;
        } catch (error) {
            console.log('Campos de verificação já existem ou erro:', error.message);
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        const apiKey = uuidv4();
        const verificationCode = crypto.randomBytes(32).toString('hex');
        const verificationExpires = new Date(Date.now() + 24 * 60 * 60 * 1000); // 24 horas
        
        // Criar usuário como não verificado
        await sql`INSERT INTO sellers (name, email, password_hash, api_key, is_active, email_verified, verification_code, verification_expires) VALUES (${name}, ${normalizedEmail}, ${hashedPassword}, ${apiKey}, FALSE, FALSE, ${verificationCode}, ${verificationExpires})`;
        
        // Enviar email de verificação
        try {
            if (!process.env.MAILERSEND_FROM_EMAIL) {
                throw new Error('MAILERSEND_FROM_EMAIL não configurado');
            }
            
            const verificationUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/verify-email?code=${verificationCode}&email=${encodeURIComponent(normalizedEmail)}`;
            
            const sentFrom = new Sender(process.env.MAILERSEND_FROM_EMAIL, 'HotTrack');
            const recipients = [new Recipient(normalizedEmail, name)];
            
            const emailParams = new EmailParams()
                .setFrom(sentFrom)
                .setTo(recipients)
                .setReplyTo(sentFrom)
                .setSubject('Verifique seu email - HotTrack')
                .setHtml(`
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #0ea5e9;">Bem-vindo ao HotTrack!</h2>
                        <p>Olá ${name},</p>
                        <p>Obrigado por se cadastrar no HotTrack. Para ativar sua conta, clique no botão abaixo:</p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="${verificationUrl}" style="background-color: #0ea5e9; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Verificar Email</a>
                        </div>
                        <p>Ou copie e cole este link no seu navegador:</p>
                        <p style="word-break: break-all; color: #666;">${verificationUrl}</p>
                        <p>Este link expira em 24 horas.</p>
                        <p>Se você não criou uma conta no HotTrack, ignore este email.</p>
                        <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
                        <p style="color: #666; font-size: 12px;">Este é um email automático, não responda.</p>
                    </div>
                `)
                .setText(`
                    Bem-vindo ao HotTrack!
                    
                    Olá ${name},
                    
                    Obrigado por se cadastrar no HotTrack. Para ativar sua conta, acesse o link abaixo:
                    ${verificationUrl}
                    
                    Este link expira em 24 horas.
                    
                    Se você não criou uma conta no HotTrack, ignore este email.
                `);

            await mailerSend.email.send(emailParams);
            
            res.status(201).json({ 
                message: 'Cadastro realizado! Verifique seu email para ativar a conta.',
                requiresVerification: true
            });
        } catch (emailError) {
            console.error('Erro ao enviar email de verificação:', emailError);
            // Mesmo com erro no email, o usuário foi criado
            res.status(201).json({ 
                message: 'Cadastro realizado! Verifique seu email para ativar a conta.',
                requiresVerification: true,
                warning: 'Erro ao enviar email. Entre em contato com o suporte.'
            });
        }
        
    } catch (error) {
        console.error("Erro no registro:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para verificar email
app.post('/api/sellers/verify-email', async (req, res) => {
    const { code, email } = req.body;
    
    console.log('=== VERIFICAÇÃO DE EMAIL ===');
    console.log('Headers:', req.headers);
    console.log('Body:', req.body);
    console.log('Code:', code);
    console.log('Email:', email);
    
    if (!code || !email) {
        console.log('Erro: Código ou email não fornecidos');
        return res.status(400).json({ message: 'Código e email são obrigatórios.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        
        // Buscar usuário com código válido
        const sellerResult = await sql`
            SELECT id, verification_code, verification_expires, email_verified 
            FROM sellers 
            WHERE email = ${normalizedEmail} AND verification_code = ${code}
        `;
        
        if (sellerResult.length === 0) {
            return res.status(400).json({ message: 'Código de verificação inválido.' });
        }
        
        const seller = sellerResult[0];
        
        // Verificar se já foi verificado
        if (seller.email_verified) {
            return res.status(400).json({ message: 'Email já foi verificado.' });
        }
        
        // Verificar se o código não expirou
        if (new Date() > new Date(seller.verification_expires)) {
            return res.status(400).json({ message: 'Código de verificação expirado.' });
        }
        
        // Ativar conta
        await sql`
            UPDATE sellers 
            SET email_verified = TRUE, is_active = TRUE, verification_code = NULL, verification_expires = NULL 
            WHERE id = ${seller.id}
        `;
        
        res.json({ message: 'Email verificado com sucesso! Sua conta foi ativada.' });
        
    } catch (error) {
        console.error('Erro na verificação de email:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para reenviar email de verificação
app.post('/api/sellers/resend-verification', async (req, res) => {
    const { email } = req.body;
    
    if (!email) {
        return res.status(400).json({ message: 'Email é obrigatório.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        
        // Buscar usuário não verificado
        const sellerResult = await sql`
            SELECT id, name, email_verified 
            FROM sellers 
            WHERE email = ${normalizedEmail}
        `;
        
        if (sellerResult.length === 0) {
            return res.status(404).json({ message: 'Usuário não encontrado.' });
        }
        
        const seller = sellerResult[0];
        
        if (seller.email_verified) {
            return res.status(400).json({ message: 'Email já foi verificado.' });
        }
        
        // Gerar novo código
        const verificationCode = crypto.randomBytes(32).toString('hex');
        const verificationExpires = new Date(Date.now() + 24 * 60 * 60 * 1000);
        
        // Atualizar código no banco
        await sql`
            UPDATE sellers 
            SET verification_code = ${verificationCode}, verification_expires = ${verificationExpires}
            WHERE id = ${seller.id}
        `;
        
        // Enviar novo email
        if (!process.env.MAILERSEND_FROM_EMAIL) {
            return res.status(500).json({ message: 'MAILERSEND_FROM_EMAIL não configurado' });
        }
        
        const verificationUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/verify-email?code=${verificationCode}&email=${encodeURIComponent(normalizedEmail)}`;
        
        const sentFrom = new Sender(process.env.MAILERSEND_FROM_EMAIL, 'HotTrack');
        const recipients = [new Recipient(normalizedEmail, seller.name)];
        
        const emailParams = new EmailParams()
            .setFrom(sentFrom)
            .setTo(recipients)
            .setReplyTo(sentFrom)
            .setSubject('Verifique seu email - HotTrack')
            .setHtml(`
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #0ea5e9;">Verificação de Email - HotTrack</h2>
                    <p>Olá ${seller.name},</p>
                    <p>Você solicitou um novo código de verificação. Clique no botão abaixo para ativar sua conta:</p>
                    <div style="text-align: center; margin: 30px 0;">
                        <a href="${verificationUrl}" style="background-color: #0ea5e9; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Verificar Email</a>
                    </div>
                    <p>Ou copie e cole este link no seu navegador:</p>
                    <p style="word-break: break-all; color: #666;">${verificationUrl}</p>
                    <p>Este link expira em 24 horas.</p>
                </div>
            `)
            .setText(`
                Verificação de Email - HotTrack
                
                Olá ${seller.name},
                
                Você solicitou um novo código de verificação. Para ativar sua conta, acesse o link abaixo:
                ${verificationUrl}
                
                Este link expira em 24 horas.
            `);

        await mailerSend.email.send(emailParams);
        
        res.json({ message: 'Email de verificação reenviado com sucesso!' });
        
    } catch (error) {
        console.error('Erro ao reenviar verificação:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.post('/api/sellers/login', async (req, res) => {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email e senha são obrigatórios.' });
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const sellerResult = await sql`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        if (sellerResult.length === 0) {
             console.warn(`[LOGIN FAILURE] Usuário não encontrado no banco de dados para o email: "${normalizedEmail}"`);
            return res.status(404).json({ message: 'Usuário não encontrado.' });
        }
        
        const seller = sellerResult[0];
        
        // Verificar se email foi verificado
        if (!seller.email_verified) {
            return res.status(403).json({ 
                message: 'Email não verificado. Verifique sua caixa de entrada.',
                requiresVerification: true,
                email: seller.email
            });
        }
        
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }
        
        const isPasswordCorrect = await bcrypt.compare(password, seller.password_hash);
        if (!isPasswordCorrect) return res.status(401).json({ message: 'Senha incorreta.' });
        
        const tokenPayload = { id: seller.id, email: seller.email };
        const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '1d' });
        
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({ message: 'Login bem-sucedido!', token, seller: sellerData });

    } catch (error) {
        console.error("ERRO DETALHADO NO LOGIN:", error); 
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para obter URL de autorização do Google
app.get('/api/auth/google/url', (req, res) => {
    try {
        const authUrl = googleClient.generateAuthUrl({
            access_type: 'offline',
            scope: ['profile', 'email']
        });
        
        res.json({ authUrl });
    } catch (error) {
        console.error('Erro ao gerar URL do Google:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para callback do Google OAuth
app.post('/api/auth/google/callback', async (req, res) => {
    try {
        const { code } = req.body;
        
        if (!code) {
            return res.status(400).json({ message: 'Código de autorização é obrigatório.' });
        }

        // Trocar código por token
        const { tokens } = await googleClient.getToken(code);
        googleClient.setCredentials(tokens);

        // Obter informações do usuário
        const ticket = await googleClient.verifyIdToken({
            idToken: tokens.id_token,
            audience: process.env.GOOGLE_CLIENT_ID
        });

        const payload = ticket.getPayload();
        const { sub: googleId, email, name, picture } = payload;

        if (!email) {
            return res.status(400).json({ message: 'Email não fornecido pelo Google.' });
        }

        const normalizedEmail = email.toLowerCase();

        // Verificar se usuário já existe
        let sellerResult = await sql`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        
        if (sellerResult.length === 0) {
            // Adicionar campos OAuth se não existirem
            
            // Criar novo usuário
            const apiKey = uuidv4();
            
            await sql`INSERT INTO sellers (
                name, email, api_key, is_active, 
                google_id, google_email, google_name, google_picture
            ) VALUES (
                ${name}, ${normalizedEmail}, ${apiKey}, TRUE,
                ${googleId}, ${email}, ${name}, ${picture}
            )`;
            
            sellerResult = await sql`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        } else {
            // Atualizar dados do Google se necessário
            await sql`UPDATE sellers SET 
                google_id = ${googleId},
                google_email = ${email},
                google_name = ${name},
                google_picture = ${picture}
                WHERE email = ${normalizedEmail}`;
        }

        const seller = sellerResult[0];
        
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }

        // Gerar JWT
        const tokenPayload = { id: seller.id, email: seller.email };
        const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '1d' });
        
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({ 
            message: 'Login com Google bem-sucedido!', 
            token, 
            seller: sellerData 
        });

    } catch (error) {
        console.error('Erro no callback do Google:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.get('/api/dashboard/data', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const settingsPromise = sql`SELECT api_key, pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key, syncpay_client_id, syncpay_client_secret, brpix_secret_key, brpix_company_id, pix_provider_primary, pix_provider_secondary, pix_provider_tertiary, commission_rate, netlify_access_token, netlify_site_id FROM sellers WHERE id = ${sellerId}`;
        const pixelsPromise = sql`SELECT * FROM pixel_configurations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const presselsPromise = sql`
            SELECT p.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids, b.bot_name
            FROM pressels p
            LEFT JOIN ( SELECT pressel_id, array_agg(pixel_config_id) as pixel_ids FROM pressel_pixels GROUP BY pressel_id ) px ON p.id = px.pressel_id
            JOIN telegram_bots b ON p.bot_id = b.id
            WHERE p.seller_id = ${sellerId} ORDER BY p.created_at DESC`;
        const botsPromise = sql`SELECT * FROM telegram_bots WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const checkoutsPromise = sql`
            SELECT c.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids
            FROM checkouts c
            LEFT JOIN ( SELECT checkout_id, array_agg(pixel_config_id) as pixel_ids FROM checkout_pixels GROUP BY checkout_id ) px ON c.id = px.checkout_id
            WHERE c.seller_id = ${sellerId} ORDER BY c.created_at DESC`;
        const utmifyIntegrationsPromise = sql`SELECT id, account_name FROM utmify_integrations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;

        const [settingsResult, pixels, pressels, bots, checkouts, utmifyIntegrations] = await Promise.all([
            settingsPromise, pixelsPromise, presselsPromise, botsPromise, checkoutsPromise, utmifyIntegrationsPromise
        ]);
        
        const settings = settingsResult[0] || {};
        res.json({ settings, pixels, pressels, bots, checkouts, utmifyIntegrations });
    } catch (error) {
        console.error("Erro ao buscar dados do dashboard:", error);
        res.status(500).json({ message: 'Erro ao buscar dados.' });
    }
});
app.get('/api/dashboard/achievements-and-ranking', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        
        const userAchievements = await sql`
            SELECT a.title, a.description, ua.is_completed, a.sales_goal
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id
            WHERE ua.seller_id = ${sellerId}
            ORDER BY a.sales_goal ASC;
        `;

        const topSellersRanking = await sql`
            SELECT s.name, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        `;
        
        const [userRevenue] = await sql`
            SELECT COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            WHERE s.id = ${sellerId}
            GROUP BY s.id;
        `;

        const userRankResult = await sql`
            SELECT COUNT(T1.id) + 1 AS rank
            FROM (
                SELECT s.id
                FROM sellers s
                LEFT JOIN clicks c ON s.id = c.seller_id
                LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
                GROUP BY s.id
                HAVING COALESCE(SUM(pt.pix_value), 0) > ${userRevenue.total_revenue}
            ) AS T1;
        `;
        
        const userRank = userRankResult[0].rank;

        res.json({
            userAchievements,
            topSellersRanking,
            currentUserRank: userRank
        });
    } catch (error) {
        console.error("Erro ao buscar conquistas e ranking:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de ranking.' });
    }
});
app.post('/api/pixels', authenticateJwt, async (req, res) => {
    const { account_name, pixel_id, meta_api_token } = req.body;
    if (!account_name || !pixel_id || !meta_api_token) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    try {
        const newPixel = await sql`INSERT INTO pixel_configurations (seller_id, account_name, pixel_id, meta_api_token) VALUES (${req.user.id}, ${account_name}, ${pixel_id}, ${meta_api_token}) RETURNING *;`;
        res.status(201).json(newPixel[0]);
    } catch (error) {
        if (error.code === '23505') { return res.status(409).json({ message: 'Este ID de Pixel já foi cadastrado.' }); }
        console.error("Erro ao salvar pixel:", error);
        res.status(500).json({ message: 'Erro ao salvar o pixel.' });
    }
});
app.delete('/api/pixels/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM pixel_configurations WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pixel:", error);
        res.status(500).json({ message: 'Erro ao excluir o pixel.' });
    }
});

app.post('/api/bots', authenticateJwt, async (req, res) => {
    const { bot_name } = req.body;
    if (!bot_name) {
        return res.status(400).json({ message: 'O nome do bot é obrigatório.' });
    }
    try {
        const placeholderToken = uuidv4();

        const [newBot] = await sql`
            INSERT INTO telegram_bots (seller_id, bot_name, bot_token) 
            VALUES (${req.user.id}, ${bot_name}, ${placeholderToken}) 
            RETURNING *;
        `;
        res.status(201).json(newBot);
    } catch (error) {
        if (error.code === '23505' && error.constraint_name === 'telegram_bots_bot_name_key') {
            return res.status(409).json({ message: 'Um bot com este nome de usuário já existe.' });
        }
        console.error("Erro ao salvar bot:", error);
        res.status(500).json({ message: 'Erro ao salvar o bot.' });
    }
});

app.delete('/api/bots/:id', authenticateJwt, async (req, res) => {
    try {
        await sql`DELETE FROM telegram_bots WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir bot:", error);
        res.status(500).json({ message: 'Erro ao excluir o bot.' });
    }
});

app.put('/api/bots/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    let { bot_token } = req.body;
    if (!bot_token) {
        return res.status(400).json({ message: 'O token do bot é obrigatório.' });
    }
    bot_token = bot_token.trim();
    try {
        await sql`
            UPDATE telegram_bots 
            SET bot_token = ${bot_token} 
            WHERE id = ${id} AND seller_id = ${req.user.id}`;
        res.status(200).json({ message: 'Token do bot atualizado com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar token do bot:", error);
        res.status(500).json({ message: 'Erro ao atualizar o token do bot.' });
    }
});

app.post('/api/bots/:id/set-webhook', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    try {
        const [bot] = await sql`
            SELECT bot_token FROM telegram_bots 
            WHERE id = ${id} AND seller_id = ${sellerId}`;

        if (!bot || !bot.bot_token || bot.bot_token.trim() === '') {
            return res.status(400).json({ message: 'O token do bot não está configurado. Salve um token válido primeiro.' });
        }
        const token = bot.bot_token.trim();
        const webhookUrl = `${HOTTRACK_API_URL}/api/webhook/telegram/${id}`;
        const telegramApiUrl = `https://api.telegram.org/bot${token}/setWebhook?url=${webhookUrl}`;
        
        const response = await axios.get(telegramApiUrl);

        if (response.data.ok) {
            res.status(200).json({ message: 'Webhook configurado com sucesso!' });
        } else {
            throw new Error(response.data.description);
        }
    } catch (error) {
        console.error("Erro ao configurar webhook:", error);
        if (error.isAxiosError && error.response) {
            const status = error.response.status;
            const telegramMessage = error.response.data?.description || 'Resposta inválida do Telegram.';
            if (status === 401 || status === 404) {
                return res.status(400).json({ message: `O Telegram rejeitou seu token: "${telegramMessage}". Verifique se o token está correto.` });
            }
            return res.status(500).json({ message: `Erro de comunicação com o Telegram: ${telegramMessage}` });
        }
        res.status(500).json({ message: `Erro interno no servidor: ${error.message}` });
    }
});

app.post('/api/bots/test-connection', authenticateJwt, async (req, res) => {
    const { bot_id } = req.body;
    if (!bot_id) return res.status(400).json({ message: 'ID do bot é obrigatório.' });

    try {
        const [bot] = await sql`SELECT bot_token, bot_name FROM telegram_bots WHERE id = ${bot_id} AND seller_id = ${req.user.id}`;
        if (!bot) {
            return res.status(404).json({ message: 'Bot não encontrado ou não pertence a este usuário.' });
        }
        if (!bot.bot_token) {
            return res.status(400).json({ message: 'Token do bot não configurado. Impossível testar.'})
        }

        const response = await axios.get(`https://api.telegram.org/bot${bot.bot_token}/getMe`);
        
        if (response.data.ok) {
            res.status(200).json({ 
                message: `Conexão com o bot @${response.data.result.username} bem-sucedida!`,
                bot_info: response.data.result
            });
        } else {
            throw new Error('A API do Telegram retornou um erro.');
        }

    } catch (error) {
        console.error(`[BOT TEST ERROR] Bot ID: ${bot_id} - Erro:`, error.response?.data || error.message);
        let errorMessage = 'Falha ao conectar com o bot. Verifique o token e tente novamente.';
        if (error.response?.status === 401) {
            errorMessage = 'Token inválido. Verifique se o token do bot foi copiado corretamente do BotFather.';
        } else if (error.response?.status === 404) {
            errorMessage = 'Bot não encontrado. O token pode estar incorreto ou o bot foi deletado.';
        }
        res.status(500).json({ message: errorMessage });
    }
});

app.get('/api/bots/users', authenticateJwt, async (req, res) => {
    const { botIds } = req.query; 

    if (!botIds) {
        return res.status(400).json({ message: 'IDs dos bots são obrigatórios.' });
    }
    const botIdArray = botIds.split(',').map(id => parseInt(id.trim(), 10));

    try {
        const users = await sql`
            SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username 
            FROM telegram_chats 
            WHERE bot_id = ANY(${botIdArray}) AND seller_id = ${req.user.id};
        `;
        res.status(200).json({ total_users: users.length });
    } catch (error) {
        console.error("Erro ao buscar contagem de usuários do bot:", error);
        res.status(500).json({ message: 'Erro interno ao buscar usuários.' });
    }
});
app.post('/api/pressels', authenticateJwt, async (req, res) => {
    const { name, bot_id, white_page_url, pixel_ids, utmify_integration_id, traffic_type, deploy_to_netlify, netlify_site_name } = req.body;
    console.log('Dados recebidos na criação de pressel:', { name, bot_id, white_page_url, pixel_ids, utmify_integration_id, traffic_type, deploy_to_netlify, netlify_site_name }); // Debug
    if (!name || !bot_id || !white_page_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    
    // Validação do nome do site Netlify
    if (deploy_to_netlify && netlify_site_name) {
        const siteName = netlify_site_name.trim();
        
        // Verificar se tem caracteres válidos (apenas alfanuméricos e hífens)
        const validNameRegex = /^[a-zA-Z0-9-]+$/;
        if (!validNameRegex.test(siteName)) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify deve conter apenas letras, números e hífens.' 
            });
        }
        
        // Verificar limite de 37 caracteres
        if (siteName.length > 37) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify deve ter no máximo 37 caracteres.' 
            });
        }
        
        // Verificar se não começa ou termina com hífen
        if (siteName.startsWith('-') || siteName.endsWith('-')) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify não pode começar ou terminar com hífen.' 
            });
        }
    }
    
    try {
        const numeric_bot_id = parseInt(bot_id, 10);
        const numeric_pixel_ids = pixel_ids.map(id => parseInt(id, 10));

        const botResult = await sql`SELECT bot_name FROM telegram_bots WHERE id = ${numeric_bot_id} AND seller_id = ${req.user.id}`;
        if (botResult.length === 0) {
            return res.status(404).json({ message: 'Bot não encontrado.' });
        }
        const bot_name = botResult[0].bot_name;

        await sql`BEGIN`;
        try {
            
            const [newPressel] = await sql`
                INSERT INTO pressels (seller_id, name, bot_id, bot_name, white_page_url, utmify_integration_id, traffic_type, netlify_url) 
                VALUES (${req.user.id}, ${name}, ${numeric_bot_id}, ${bot_name}, ${white_page_url}, ${utmify_integration_id || null}, ${traffic_type || 'both'}, NULL) 
                RETURNING *;
            `;
            
            for (const pixelId of numeric_pixel_ids) {
                await sql`INSERT INTO pressel_pixels (pressel_id, pixel_config_id) VALUES (${newPressel.id}, ${pixelId})`;
            }
            
            let netlifyUrl = null;
            let netlifyDeploy = null;
            
            // Deploy opcional para Netlify
            if (deploy_to_netlify) {
                try {
                    // Buscar configurações do Netlify
                    const [seller] = await sql`SELECT netlify_access_token, netlify_site_id FROM sellers WHERE id = ${req.user.id}`;
                    
                    if (seller?.netlify_access_token) {
                        // Gerar HTML da pressel
                        const htmlContent = await generatePresselHTML(newPressel, numeric_pixel_ids);
                        
                        if (seller.netlify_site_id) {
                            // Usar site existente
                           const fileName = `pressel-${newPressel.id}.html`; // Nome do arquivo
                            const deployResult = await deployToNetlify(seller.netlify_access_token, seller.netlify_site_id, htmlContent, fileName); // Passa o nome

                            if (deployResult.success) {
                               // ### CORREÇÃO AQUI ###
                               // Constrói a URL completa incluindo o nome do arquivo
                               if (deployResult.url) { // Verifica se a URL base foi retornada
                                  netlifyUrl = `${deployResult.url}/${fileName}`; // Adiciona o nome do arquivo
                               } else {
                                  console.error(`[Netlify] Deploy bem-sucedido para site existente ${seller.netlify_site_id}, mas URL base não retornada.`);
                                  // Tratar erro ou definir netlifyUrl como null
                                  netlifyUrl = null;
                                  netlifyDeploy = { success: false, message: 'Deploy bem-sucedido, mas URL não obtida.' }; // Atualiza status
                               }
                               // ### FIM DA CORREÇÃO ###

                               if (netlifyUrl) { // Só atualiza se a URL foi construída
                                  netlifyDeploy = { success: true, url: netlifyUrl };

                                  // Atualizar campo netlify_url na tabela pressels
                                  await sql`UPDATE pressels SET netlify_url = ${netlifyUrl} WHERE id = ${newPressel.id}`;
                                  
                                  // Adicionar domínio automaticamente
                                  const domain = new URL(deployResult.url).hostname; // Pega só o hostname
                                  await sql`INSERT INTO pressel_allowed_domains (pressel_id, domain) VALUES (${newPressel.id}, ${domain}) ON CONFLICT DO NOTHING`; // Evita duplicados
                                  
                                  console.log(`[Netlify] Pressel ${newPressel.id} deployada com sucesso no site existente: ${netlifyUrl}`);
                               }
                            } else {
                                console.warn(`[Netlify] Site existente não encontrado, criando novo site. Erro:`, deployResult.error);
                                netlifyDeploy = { success: false, message: deployResult.userMessage || 'Falha ao publicar no Netlify.' };
                                
                                // Limpar site_id inválido e criar novo site
                                await sql`UPDATE sellers SET netlify_site_id = NULL WHERE id = ${req.user.id}`;
                                
                                // Criar novo site
                                const siteName = netlify_site_name && netlify_site_name.trim() 
                                    ? netlify_site_name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-')
                                    : `pressel-${newPressel.id}-${Date.now()}`;
                                const siteResult = await createNetlifySite(seller.netlify_access_token, siteName);
                                
                                if (siteResult.success) {
                                    // Salvar novo site_id
                                    await sql`UPDATE sellers SET netlify_site_id = ${siteResult.site.id} WHERE id = ${req.user.id}`;
                                    
                                    // Fazer deploy
                                    const deployResult2 = await deployToNetlify(seller.netlify_access_token, siteResult.site.id, htmlContent, 'index.html');
                                    
                                    if (deployResult2.success) {
                                        netlifyUrl = deployResult2.url;
                                        netlifyDeploy = { success: true, url: netlifyUrl };
                                        
                                        // Atualizar campo netlify_url na tabela pressels
                                        await sql`UPDATE pressels SET netlify_url = ${netlifyUrl} WHERE id = ${newPressel.id}`;
                                        
                                        // Adicionar domínio automaticamente
                                        const domain = deployResult2.url.replace('https://', '');
                                        await sql`INSERT INTO pressel_allowed_domains (pressel_id, domain) VALUES (${newPressel.id}, ${domain})`;
                                        
                                        console.log(`[Netlify] Novo site criado e pressel ${newPressel.id} deployada: ${netlifyUrl}`);
                                    } else {
                                        netlifyDeploy = { success: false, message: deployResult2.userMessage || 'Falha ao publicar no Netlify.' };
                                    }
                                } else {
                                    console.error(`[Netlify] Erro ao criar novo site para pressel ${newPressel.id}:`, siteResult.error);
                                    netlifyDeploy = { success: false, message: siteResult.error || 'Falha ao criar site no Netlify.' };
                                }
                            }
                        } else {
                            // Criar novo site
                            const siteName = netlify_site_name && netlify_site_name.trim() 
                                ? netlify_site_name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-')
                                : `pressel-${newPressel.id}-${Date.now()}`;
                            const siteResult = await createNetlifySite(seller.netlify_access_token, siteName);
                            
                            if (siteResult.success) {
                                // Salvar site_id
                                await sql`UPDATE sellers SET netlify_site_id = ${siteResult.site.id} WHERE id = ${req.user.id}`;
                                
                                // Fazer deploy
                                const deployResult = await deployToNetlify(seller.netlify_access_token, siteResult.site.id, htmlContent, 'index.html');
                                
                                if (deployResult.success) {
                                    netlifyUrl = deployResult.url;
                                    netlifyDeploy = { success: true, url: netlifyUrl };
                                    
                                    // Atualizar campo netlify_url na tabela pressels
                                    await sql`UPDATE pressels SET netlify_url = ${netlifyUrl} WHERE id = ${newPressel.id}`;
                                    
                                    // Adicionar domínio automaticamente
                                    const domain = deployResult.url.replace('https://', '');
                                    await sql`INSERT INTO pressel_allowed_domains (pressel_id, domain) VALUES (${newPressel.id}, ${domain})`;
                                    
                                    console.log(`[Netlify] Site criado e pressel ${newPressel.id} deployada: ${netlifyUrl}`);
                                } else {
                                    netlifyDeploy = { success: false, message: deployResult.userMessage || 'Falha ao publicar no Netlify.' };
                                }
                            } else {
                                console.error(`[Netlify] Erro ao criar site para pressel ${newPressel.id}:`, siteResult.error);
                                netlifyDeploy = { success: false, message: siteResult.error || 'Falha ao criar site no Netlify.' };
                            }
                        }
                    } else {
                        console.warn(`[Netlify] Token Netlify não configurado para vendedor ${req.user.id}`);
                        netlifyDeploy = { success: false, message: 'Token Netlify não configurado. Configure em Integrações > Netlify.' };
                    }
                } catch (netlifyError) {
                    console.error(`[Netlify] Erro no deploy da pressel ${newPressel.id}:`, netlifyError);
                    // Não falha a criação da pressel se o deploy falhar
                    if (!netlifyDeploy) {
                        netlifyDeploy = { success: false, message: 'Falha inesperada no deploy Netlify. Tente novamente.' };
                    }
                }
            }
            
            await sql`COMMIT`;
            
            res.status(201).json({ 
                ...newPressel, 
                pixel_ids: numeric_pixel_ids, 
                bot_name,
                netlify_url: netlifyUrl,
                netlify_deploy: netlifyDeploy
            });
        } catch (transactionError) {
            await sql`ROLLBACK`;
            throw transactionError;
        }
    } catch (error) {
        console.error("Erro ao salvar pressel:", error);
        res.status(500).json({ message: 'Erro ao salvar a pressel.' });
    }
});
app.delete('/api/pressels/:id', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        
        // Buscar informações da pressel antes de excluir
        const [pressel] = await sql`
            SELECT id, seller_id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Buscar configurações do Netlify do seller
        const [seller] = await sql`
            SELECT netlify_access_token, netlify_site_id 
            FROM sellers 
            WHERE id = ${req.user.id}
        `;
        
        // Excluir site do Netlify se existir
        if (seller?.netlify_access_token && seller?.netlify_site_id) {
            try {
                const deleteResult = await deleteNetlifySite(seller.netlify_access_token, seller.netlify_site_id);
                if (deleteResult.success) {
                    console.log(`[Netlify] Site ${seller.netlify_site_id} excluído com sucesso`);
                } else {
                    console.warn(`[Netlify] Site não encontrado ou já excluído:`, deleteResult.error);
                }
                
                // Sempre limpar netlify_site_id do seller (mesmo se o site não existir)
                await sql`UPDATE sellers SET netlify_site_id = NULL WHERE id = ${req.user.id}`;
            } catch (netlifyError) {
                console.warn(`[Netlify] Erro ao excluir site da pressel ${presselId}:`, netlifyError);
                // Limpar netlify_site_id mesmo se houver erro
                await sql`UPDATE sellers SET netlify_site_id = NULL WHERE id = ${req.user.id}`;
            }
        }
        
        // Excluir a pressel
        await sql`DELETE FROM pressels WHERE id = ${presselId} AND seller_id = ${req.user.id}`;
        
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pressel:", error);
        res.status(500).json({ message: 'Erro ao excluir a pressel.' });
    }
});

// ==========================================================
//          ROTAS PARA GERENCIAR DOMÍNIOS PERMITIDOS
// ==========================================================

// Buscar domínios permitidos para uma pressel
app.get('/api/pressels/:id/domains', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sql`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        const domains = await sql`
            SELECT id, domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        res.json(domains);
    } catch (error) {
        console.error("Erro ao buscar domínios permitidos:", error);
        res.status(500).json({ message: 'Erro ao buscar domínios.' });
    }
});

// Adicionar domínio permitido para uma pressel
app.post('/api/pressels/:id/domains', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        const { domain } = req.body;
        
        if (!domain || typeof domain !== 'string' || domain.trim().length === 0) {
            return res.status(400).json({ message: 'Domínio é obrigatório.' });
        }
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sql`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Normalizar domínio (remover protocolo se presente)
        const normalizedDomain = domain.trim().replace(/^https?:\/\//, '');
        
        // Verificar se já existe
        const [existing] = await sql`
            SELECT id FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} AND domain = ${normalizedDomain}
        `;
        
        if (existing) {
            return res.status(400).json({ message: 'Domínio já está cadastrado para esta pressel.' });
        }
        
        // Inserir domínio
        const [newDomain] = await sql`
            INSERT INTO pressel_allowed_domains (pressel_id, domain) 
            VALUES (${presselId}, ${normalizedDomain}) 
            RETURNING id, domain, created_at
        `;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(201).json(newDomain);
    } catch (error) {
        console.error("Erro ao adicionar domínio:", error);
        res.status(500).json({ message: 'Erro ao adicionar domínio.' });
    }
});

// Remover domínio permitido
app.delete('/api/pressels/:presselId/domains/:domainId', authenticateJwt, async (req, res) => {
    try {
        const { presselId, domainId } = req.params;
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sql`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Verificar se o domínio pertence à pressel
        const [domain] = await sql`
            SELECT id FROM pressel_allowed_domains 
            WHERE id = ${domainId} AND pressel_id = ${presselId}
        `;
        
        if (!domain) {
            return res.status(404).json({ message: 'Domínio não encontrado.' });
        }
        
        // Remover domínio
        await sql`DELETE FROM pressel_allowed_domains WHERE id = ${domainId}`;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao remover domínio:", error);
        res.status(500).json({ message: 'Erro ao remover domínio.' });
    }
});

// Micro painel público para publishers registrarem domínios
app.get('/api/pressel-domains/:presselId', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        
        // Verificar se a pressel existe
        const [pressel] = await sql`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        const domains = await sql`
            SELECT domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        res.json({
            pressel: { id: pressel.id, name: pressel.name },
            domains
        });
    } catch (error) {
        console.error("Erro ao buscar domínios da pressel:", error);
        res.status(500).json({ message: 'Erro ao buscar domínios.' });
    }
});

// Registrar domínio via micro painel (sem autenticação)
app.post('/api/pressel-domains/:presselId/register', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        const { domain, verification_code } = req.body;
        
        if (!domain || typeof domain !== 'string' || domain.trim().length === 0) {
            return res.status(400).json({ message: 'Domínio é obrigatório.' });
        }
        
        // Verificar se a pressel existe
        const [pressel] = await sql`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Normalizar domínio
        const normalizedDomain = domain.trim().replace(/^https?:\/\//, '');
        
        // Verificar se já existe
        const [existing] = await sql`
            SELECT id FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} AND domain = ${normalizedDomain}
        `;
        
        if (existing) {
            return res.status(400).json({ message: 'Domínio já está cadastrado para esta pressel.' });
        }
        
        // TODO: Implementar verificação de domínio (DNS, arquivo de verificação, etc.)
        // Por enquanto, aceitar automaticamente
        
        // Inserir domínio
        const [newDomain] = await sql`
            INSERT INTO pressel_allowed_domains (pressel_id, domain) 
            VALUES (${presselId}, ${normalizedDomain}) 
            RETURNING id, domain, created_at
        `;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(201).json({
            message: 'Domínio registrado com sucesso!',
            domain: newDomain
        });
    } catch (error) {
        console.error("Erro ao registrar domínio:", error);
        res.status(500).json({ message: 'Erro ao registrar domínio.' });
    }
});

// Micro painel público para publishers registrarem domínios
app.get('/pressel-domains/:presselId', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        
        // Verificar se a pressel existe
        const [pressel] = await sql`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).send(`
                <!DOCTYPE html>
                <html lang="pt-BR">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Pressel não encontrada</title>
                    <style>
                        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f0f2f5; }
                        .container { max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                        h1 { color: #e74c3c; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>❌ Pressel não encontrada</h1>
                        <p>Esta pressel não existe ou foi removida.</p>
                    </div>
                </body>
                </html>
            `);
        }
        
        const domains = await sql`
            SELECT domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        const html = `
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gerenciador de Domínios - ${pressel.name}</title>
    <style>
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Open Sans", "Helvetica Neue", sans-serif; 
            background-color: #f0f2f5; 
            color: #1c1e21; 
            padding: 20px;
        }
        .container { 
            max-width: 800px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 8px; 
            padding: 30px; 
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid #e1e5e9;
        }
        .header h1 {
            color: #1877f2;
            margin: 0 0 10px 0;
        }
        .header p {
            color: #606770;
            margin: 0;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #1c1e21;
        }
        .form-group input {
            width: 100%;
            padding: 12px;
            border: 1px solid #dadde1;
            border-radius: 6px;
            font-size: 16px;
            box-sizing: border-box;
        }
        .form-group input:focus {
            outline: none;
            border-color: #1877f2;
            box-shadow: 0 0 0 2px rgba(24, 119, 242, 0.2);
        }
        .btn {
            background: #1877f2;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn:hover {
            background: #166fe5;
        }
        .btn:disabled {
            background: #dadde1;
            cursor: not-allowed;
        }
        .domains-list {
            margin-top: 30px;
        }
        .domains-list h3 {
            margin-bottom: 15px;
            color: #1c1e21;
        }
        .domain-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px;
            background: #f8f9fa;
            border-radius: 6px;
            margin-bottom: 8px;
        }
        .domain-name {
            font-family: monospace;
            color: #1877f2;
            font-weight: 600;
        }
        .domain-date {
            color: #606770;
            font-size: 14px;
        }
        .alert {
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
        }
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .loading {
            display: none;
            text-align: center;
            padding: 20px;
        }
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #1877f2;
            border-radius: 50%;
            width: 20px;
            height: 20px;
            animation: spin 1s linear infinite;
            margin: 0 auto 10px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌐 Gerenciador de Domínios</h1>
            <p>Registre os domínios que podem usar a pressel: <strong>${pressel.name}</strong></p>
        </div>

        <div id="alert-container"></div>

        <form id="domain-form">
            <div class="form-group">
                <label for="domain">Domínio:</label>
                <input 
                    type="text" 
                    id="domain" 
                    name="domain" 
                    placeholder="exemplo.com ou https://exemplo.com"
                    required
                />
            </div>
            <button type="submit" class="btn" id="submit-btn">
                Adicionar Domínio
            </button>
        </form>

        <div class="loading" id="loading">
            <div class="spinner"></div>
            <p>Carregando domínios...</p>
        </div>

        <div class="domains-list" id="domains-list" style="display: none;">
            <h3>Domínios Registrados:</h3>
            <div id="domains-container"></div>
        </div>
    </div>

    <script>
        const API_BASE_URL = '${process.env.API_BASE_URL}';
        const PRESSEL_ID = ${presselId};

        // Função para mostrar alertas
        function showAlert(message, type = 'success') {
            const alertContainer = document.getElementById('alert-container');
            alertContainer.innerHTML = \`
                <div class="alert alert-\${type}">
                    \${message}
                </div>
            \`;
            
            // Remover alerta após 5 segundos
            setTimeout(() => {
                alertContainer.innerHTML = '';
            }, 5000);
        }

        // Função para carregar domínios
        async function loadDomains() {
            try {
                document.getElementById('loading').style.display = 'block';
                document.getElementById('domains-list').style.display = 'none';
                
                const response = await fetch(\`\${API_BASE_URL}/api/pressel-domains/\${PRESSEL_ID}\`);
                const data = await response.json();
                
                if (response.ok) {
                    displayDomains(data.domains);
                    document.getElementById('domains-list').style.display = 'block';
                } else {
                    showAlert('Erro ao carregar domínios: ' + data.message, 'error');
                }
            } catch (error) {
                console.error('Erro ao carregar domínios:', error);
                showAlert('Erro ao carregar domínios. Tente novamente.', 'error');
            } finally {
                document.getElementById('loading').style.display = 'none';
            }
        }

        // Função para exibir domínios
        function displayDomains(domains) {
            const container = document.getElementById('domains-container');
            
            if (domains.length === 0) {
                container.innerHTML = '<p style="color: #606770; text-align: center; padding: 20px;">Nenhum domínio registrado ainda.</p>';
                return;
            }
            
            container.innerHTML = domains.map(domain => \`
                <div class="domain-item">
                    <div>
                        <div class="domain-name">\${domain.domain}</div>
                        <div class="domain-date">Registrado em: \${new Date(domain.created_at).toLocaleString('pt-BR')}</div>
                    </div>
                </div>
            \`).join('');
        }

        // Função para adicionar domínio
        async function addDomain(domain) {
            try {
                const response = await fetch(\`\${API_BASE_URL}/api/pressel-domains/\${PRESSEL_ID}/register\`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ domain })
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    showAlert('Domínio registrado com sucesso!', 'success');
                    document.getElementById('domain').value = '';
                    loadDomains(); // Recarregar lista
                } else {
                    showAlert('Erro: ' + data.message, 'error');
                }
            } catch (error) {
                console.error('Erro ao adicionar domínio:', error);
                showAlert('Erro ao adicionar domínio. Tente novamente.', 'error');
            }
        }

        // Event listeners
        document.getElementById('domain-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const domainInput = document.getElementById('domain');
            const submitBtn = document.getElementById('submit-btn');
            const domain = domainInput.value.trim();
            
            if (!domain) {
                showAlert('Por favor, insira um domínio válido.', 'error');
                return;
            }
            
            submitBtn.disabled = true;
            submitBtn.textContent = 'Adicionando...';
            
            try {
                await addDomain(domain);
            } finally {
                submitBtn.disabled = false;
                submitBtn.textContent = 'Adicionar Domínio';
            }
        });

        // Carregar domínios ao inicializar
        loadDomains();
    </script>
</body>
</html>
        `;
        
        res.send(html);
    } catch (error) {
        console.error("Erro ao servir micro painel:", error);
        res.status(500).send(`
            <!DOCTYPE html>
            <html lang="pt-BR">
            <head>
                <meta charset="UTF-8">
                <title>Erro</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f0f2f5; }
                    .container { max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                    h1 { color: #e74c3c; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>❌ Erro interno</h1>
                    <p>Ocorreu um erro ao carregar o painel. Tente novamente mais tarde.</p>
                </div>
            </body>
            </html>
        `);
    }
});
app.post('/api/checkouts', authenticateJwt, async (req, res) => {
    const { name, product_name, redirect_url, value_type, fixed_value_cents, pixel_ids } = req.body;

    if (!name || !product_name || !redirect_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) {
        return res.status(400).json({ message: 'Nome, Nome do Produto, URL de Redirecionamento e ao menos um Pixel são obrigatórios.' });
    }
    if (value_type === 'fixed' && (!fixed_value_cents || fixed_value_cents <= 0)) {
        return res.status(400).json({ message: 'Para valor fixo, o valor em centavos deve ser maior que zero.' });
    }

    try {
        await sql`BEGIN`;

        const [newCheckout] = await sql`
            INSERT INTO checkouts (seller_id, name, product_name, redirect_url, value_type, fixed_value_cents)
            VALUES (${req.user.id}, ${name}, ${product_name}, ${redirect_url}, ${value_type}, ${value_type === 'fixed' ? fixed_value_cents : null})
            RETURNING *;
        `;

        for (const pixelId of pixel_ids) {
            await sql`INSERT INTO checkout_pixels (checkout_id, pixel_config_id) VALUES (${newCheckout.id}, ${pixelId})`;
        }
        
        await sql`COMMIT`;

        res.status(201).json({ ...newCheckout, pixel_ids: pixel_ids.map(id => parseInt(id)) });
    } catch (error) {
        await sql`ROLLBACK`;
        console.error("Erro ao salvar checkout:", error);
        res.status(500).json({ message: 'Erro interno ao salvar o checkout.' });
    }
});
// EXCLUIR CHECKOUT
app.delete('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;

    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }

    try {
        // IMPORTANT: First, delete associated clicks to avoid foreign key constraint errors
        await sql `DELETE FROM clicks WHERE checkout_id = ${checkoutId} AND seller_id = ${sellerId}`;

        // Now, delete the checkout itself
        const result = await sql`
            DELETE FROM hosted_checkouts
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Return ID to confirm deletion
        `;

        if (result.length === 0) {
            console.warn(`Tentativa de excluir checkout não encontrado ou não pertencente ao seller: ${checkoutId}, Seller: ${sellerId}`);
            // Still return success as the end state (checkout doesn't exist) is achieved
        }

        res.status(200).json({ message: 'Checkout excluído com sucesso!' }); // Use 200 with message

    } catch (error) {
        console.error(`Erro ao excluir checkout ${checkoutId}:`, error);
        // Specifically handle foreign key violations if pix_transactions block deletion
        if (error.code === '23503') { // PostgreSQL foreign key violation error code
             console.error(`Erro de chave estrangeira ao excluir checkout ${checkoutId}. Pode haver transações PIX associadas.`);
             return res.status(409).json({ message: 'Não é possível excluir este checkout pois existem transações PIX associadas a ele através de cliques. Contacte o suporte se necessário.' });
        }
        res.status(500).json({ message: 'Erro interno ao excluir o checkout.' });
    }
});
app.post('/api/settings/pix', authenticateJwt, async (req, res) => {
    const { 
        pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key,
        syncpay_client_id, syncpay_client_secret,
        brpix_secret_key, brpix_company_id,
        pix_provider_primary, pix_provider_secondary, pix_provider_tertiary
    } = req.body;
    try {
        await sql`UPDATE sellers SET 
            pushinpay_token = ${pushinpay_token || null}, 
            cnpay_public_key = ${cnpay_public_key || null}, 
            cnpay_secret_key = ${cnpay_secret_key || null}, 
            oasyfy_public_key = ${oasyfy_public_key || null}, 
            oasyfy_secret_key = ${oasyfy_secret_key || null},
            syncpay_client_id = ${syncpay_client_id || null},
            syncpay_client_secret = ${syncpay_client_secret || null},
            brpix_secret_key = ${brpix_secret_key || null},
            brpix_company_id = ${brpix_company_id || null},
            pix_provider_primary = ${pix_provider_primary || 'pushinpay'},
            pix_provider_secondary = ${pix_provider_secondary || null},
            pix_provider_tertiary = ${pix_provider_tertiary || null}
            WHERE id = ${req.user.id}`;
        res.status(200).json({ message: 'Configurações de PIX salvas com sucesso.' });
    } catch (error) {
        console.error("Erro ao salvar configurações de PIX:", error);
        res.status(500).json({ message: 'Erro ao salvar as configurações.' });
    }
});

// ==========================================================
//          ROTAS DE INTEGRAÇÃO NETLIFY
// ==========================================================

app.post('/api/netlify/validate-token', authenticateJwt, async (req, res) => {
    const { access_token } = req.body;
    
    if (!access_token) {
        return res.status(400).json({ message: 'Token de acesso é obrigatório.' });
    }
    
    try {
        const result = await validateNetlifyToken(access_token);
        
        if (result.success) {
            // Salva o token se for válido
            await sql`UPDATE sellers SET netlify_access_token = ${access_token} WHERE id = ${req.user.id}`;
            res.json({ 
                success: true, 
                message: 'Token válido! Configuração salva.',
                user: result.user
            });
        } else {
            res.status(400).json({ 
                success: false, 
                message: 'Token inválido: ' + result.error 
            });
        }
    } catch (error) {
        console.error("Erro ao validar token Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.get('/api/netlify/sites', authenticateJwt, async (req, res) => {
    try {
        const [seller] = await sql`SELECT netlify_access_token FROM sellers WHERE id = ${req.user.id}`;
        
        if (!seller?.netlify_access_token) {
            return res.status(400).json({ message: 'Token Netlify não configurado.' });
        }
        
        const result = await getNetlifySites(seller.netlify_access_token);
        
        if (result.success) {
            res.json({ sites: result.sites });
        } else {
            res.status(400).json({ message: 'Erro ao buscar sites: ' + result.error });
        }
    } catch (error) {
        console.error("Erro ao buscar sites Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.post('/api/netlify/create-site', authenticateJwt, async (req, res) => {
    const { site_name } = req.body;
    
    if (!site_name) {
        return res.status(400).json({ message: 'Nome do site é obrigatório.' });
    }
    
    try {
        const [seller] = await sql`SELECT netlify_access_token FROM sellers WHERE id = ${req.user.id}`;
        
        if (!seller?.netlify_access_token) {
            return res.status(400).json({ message: 'Token Netlify não configurado.' });
        }
        
        const result = await createNetlifySite(seller.netlify_access_token, site_name);
        
        if (result.success) {
            // Salva o site_id no banco
            await sql`UPDATE sellers SET netlify_site_id = ${result.site.id} WHERE id = ${req.user.id}`;
            
            res.json({ 
                success: true, 
                site: result.site,
                url: result.url,
                message: 'Site criado com sucesso!'
            });
        } else {
            res.status(400).json({ 
                success: false, 
                message: 'Erro ao criar site: ' + result.error 
            });
        }
    } catch (error) {
        console.error("Erro ao criar site Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
app.get('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    try {
        const integrations = await sql`
            SELECT id, account_name, created_at 
            FROM utmify_integrations 
            WHERE seller_id = ${req.user.id} 
            ORDER BY created_at DESC
        `;
        res.status(200).json(integrations);
    } catch (error) {
        console.error("Erro ao buscar integrações Utmify:", error);
        res.status(500).json({ message: 'Erro ao buscar integrações.' });
    }
});
app.post('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    const { account_name, api_token } = req.body;
    if (!account_name || !api_token) {
        return res.status(400).json({ message: 'Nome da conta e token da API são obrigatórios.' });
    }
    try {
        const [newIntegration] = await sql`
            INSERT INTO utmify_integrations (seller_id, account_name, api_token) 
            VALUES (${req.user.id}, ${account_name}, ${api_token}) 
            RETURNING id, account_name, created_at
        `;
        res.status(201).json(newIntegration);
    } catch (error) {
        console.error("Erro ao adicionar integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao salvar integração.' });
    }
});
app.delete('/api/integrations/utmify/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        await sql`
            DELETE FROM utmify_integrations 
            WHERE id = ${id} AND seller_id = ${req.user.id}
        `;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao excluir integração.' });
    }
});
// ROTA /api/registerClick CORRIGIDA PARA BUSCAR SELLER_ID PELO PRESSSEL_ID OU CHECKOUT_ID

app.post('/api/registerClick', rateLimitMiddleware, logApiRequest, async (req, res) => {
    // REMOVIDO: sellerApiKey
    const { presselId, checkoutId, referer, fbclid, fbp, fbc, user_agent, utm_source, utm_campaign, utm_medium, utm_content, utm_term } = req.body;

    // MODIFICADO: Validação inicial
    if (!presselId && !checkoutId) {
        // Agora verifica apenas se pelo menos um ID está presente
        return res.status(400).json({ message: 'É necessário fornecer presselId ou checkoutId.' });
    }

    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    // Validação de domínio para pressels (mantida)
    if (presselId) {
        const origin = req.headers.origin;
        if (origin) {
            const isDomainAllowed = await isDomainAllowedForPressel(presselId, origin);
            if (!isDomainAllowed) {
                console.log(`[SECURITY] Tentativa de acesso de domínio não autorizado: ${origin} para pressel ${presselId}`);
                return res.status(403).json({
                    message: 'Domínio não autorizado para esta pressel.',
                    hint: `Verifique o painel de domínios ou registre seu domínio.` // Mensagem genérica
                });
            }
        } else {
             console.warn(`[SECURITY] Não foi possível verificar a origem (cabeçalho Origin ausente) para pressel ${presselId}. Permitindo acesso.`);
             // Considerar se deve bloquear ou permitir requisições sem Origin. Permitir pode ser necessário para alguns cenários, mas menos seguro.
        }
    }

    try {
        let sellerId = null;

        // --- INÍCIO DA LÓGICA PARA ENCONTRAR seller_id ---
        if (presselId) {
            const [pressel] = await sql`SELECT seller_id FROM pressels WHERE id = ${presselId}`;
            if (!pressel) {
                return res.status(404).json({ message: 'Pressel não encontrada.' });
            }
            sellerId = pressel.seller_id;
        } else if (checkoutId) {
            const [checkout] = await sql`SELECT seller_id FROM hosted_checkouts WHERE id = ${checkoutId}`;
            if (!checkout) {
                return res.status(404).json({ message: 'Checkout não encontrado.' });
            }
            sellerId = checkout.seller_id;
        }

        // Se, por algum motivo, não encontramos o sellerId
        if (!sellerId) {
             console.error(`[registerClick] ERRO CRÍTICO: Não foi possível determinar o seller_id para presselId=${presselId} ou checkoutId=${checkoutId}`);
             return res.status(500).json({ message: 'Erro ao identificar o vendedor associado.' });
        }
        // --- FIM DA LÓGICA PARA ENCONTRAR seller_id ---

        // MODIFICADO: Query INSERT usa o sellerId encontrado
        const result = await sql`INSERT INTO clicks (
            seller_id, pressel_id, checkout_id, ip_address, user_agent, referer, fbclid, fbp, fbc,
            utm_source, utm_campaign, utm_medium, utm_content, utm_term
        ) VALUES (
            ${sellerId}, ${presselId || null}, ${checkoutId || null}, ${ip_address}, ${user_agent}, ${referer}, ${fbclid}, ${fbp}, ${fbc},
            ${utm_source || null}, ${utm_campaign || null}, ${utm_medium || null}, ${utm_content || null}, ${utm_term || null}
        ) RETURNING *;`; // Não precisamos mais do JOIN com sellers aqui

        // O resto da lógica permanece o mesmo...
        const newClick = result[0];
        const click_record_id = newClick.id;
        // Gera um click_id único e amigável, mesmo que já exista um no banco (para consistência)
        const clean_click_id = `lead${click_record_id.toString().padStart(6, '0')}`;
        const db_click_id = `/start ${clean_click_id}`;

        await sql`UPDATE clicks SET click_id = ${db_click_id} WHERE id = ${click_record_id}`;

        // Retorna o click_id limpo para o frontend/JS da pressel
        res.status(200).json({ status: 'success', click_id: clean_click_id });

        // Tarefas assíncronas (Geolocalização e Evento Meta)
        (async () => {
            try {
                let city = 'Desconhecida', state = 'Desconhecido';
                // Adiciona verificação para IPs locais comuns
                const isLocalIp = ip_address === '::1' || ip_address === '127.0.0.1' || ip_address.startsWith('192.168.') || ip_address.startsWith('10.');
                if (ip_address && !isLocalIp) {
                    try {
                        const geo = await axios.get(`http://ip-api.com/json/${ip_address}?fields=status,city,regionName`);
                        if (geo.data.status === 'success') {
                            city = geo.data.city || city;
                            state = geo.data.regionName || state;
                        } else {
                             console.warn(`[GEO] Falha ao obter geolocalização para IP ${ip_address}: ${geo.data.message || 'Status não foi success'}`);
                        }
                    } catch (geoError) {
                         console.error(`[GEO] Erro na API de geolocalização para IP ${ip_address}:`, geoError.message);
                    }
                } else if (isLocalIp) {
                     console.log(`[GEO] IP ${ip_address} é local. Pulando geolocalização.`);
                     city = 'Local'; // Ou mantenha Desconhecida
                     state = 'Local';
                }
                await sql`UPDATE clicks SET city = ${city}, state = ${state} WHERE id = ${click_record_id}`;
                console.log(`[BACKGROUND] Geolocalização atualizada para o clique ${click_record_id} -> Cidade: ${city}, Estado: ${state}.`);

                // Envia InitiateCheckout apenas se originado de um checkout hosted
                if (checkoutId) {
                     const [checkoutDetails] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${checkoutId}`;
                     // Tenta pegar um valor representativo, pode ser o primeiro pacote ou um valor fixo
                     const representativeValueCents = checkoutDetails?.config?.pricing?.packages?.[0]?.value_cents || checkoutDetails?.config?.pricing?.fixed_value_cents || 0;
                     const eventValue = representativeValueCents > 0 ? (representativeValueCents / 100) : 0.01; // Envia 0.01 se não encontrar valor

                     // Passa o click_id LIMPO para o evento Meta
                     await sendMetaEvent('InitiateCheckout', { ...newClick, click_id: clean_click_id }, { pix_value: eventValue, id: click_record_id });
                     console.log(`[BACKGROUND] Evento InitiateCheckout enviado para o clique ${click_record_id}.`);
                }
            } catch (backgroundError) {
                console.error("Erro em tarefa de segundo plano (registerClick):", backgroundError.message);
            }
        })();

    } catch (error) {
        console.error("Erro ao registrar clique:", error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Erro interno do servidor.' });
        }
    }
});
app.post('/api/click/info', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id } = req.body;
    if (!apiKey || !click_id) return res.status(400).json({ message: 'API Key e click_id são obrigatórios.' });
    
    try {
        const sellerResult = await sql`SELECT id, email FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            console.warn(`[CLICK INFO] Tentativa de consulta com API Key inválida: ${apiKey}`);
            return res.status(401).json({ message: 'API Key inválida.' });
        }
        
        const seller_id = sellerResult[0].id;
        const seller_email = sellerResult[0].email;
        
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        
        const clickResult = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller_id}`;
        
        if (clickResult.length === 0) {
            console.warn(`[CLICK INFO NOT FOUND] Vendedor (ID: ${seller_id}, Email: ${seller_email}) tentou consultar o click_id "${click_id}", mas não foi encontrado.`);
            return res.status(404).json({ message: 'Click ID não encontrado para este vendedor.' });
        }
        
        const clickInfo = clickResult[0];
        res.status(200).json({ status: 'success', city: clickInfo.city, state: clickInfo.state });

    } catch (error) {
        console.error("Erro ao consultar informações do clique:", error);
        res.status(500).json({ message: 'Erro interno ao consultar informações do clique.' });
    }
});
app.get('/api/dashboard/metrics', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        let { startDate, endDate } = req.query;
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        const totalClicksQuery = hasDateFilter
            ? sql`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId} AND created_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId}`;

        const pixGeneratedQuery = hasDateFilter
            ? sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.created_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId}`;

        const pixPaidQuery = hasDateFilter
            ? sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate}`
            : sql`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid'`;

        const botsPerformanceQuery = hasDateFilter
            ? sql`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON (c.pressel_id = p.id OR c.bot_id = tb.id) AND c.seller_id = ${sellerId} AND c.created_at BETWEEN ${startDate} AND ${endDate} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`
            : sql`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON (c.pressel_id = p.id OR c.bot_id = tb.id) AND c.seller_id = ${sellerId} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`;

        const clicksByStateQuery = hasDateFilter
             ? sql`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' AND c.created_at BETWEEN ${startDate} AND ${endDate} GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`
             : sql`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`;

        const userTimezone = 'America/Sao_Paulo'; 
        const dailyRevenueQuery = hasDateFilter
             ? sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate} GROUP BY 1 ORDER BY 1 ASC`
             : sql`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' GROUP BY 1 ORDER BY 1 ASC`;
        
        const [
               totalClicksResult, pixGeneratedResult, pixPaidResult, botsPerformance,
               clicksByState, dailyRevenue
        ] = await Promise.all([
              totalClicksQuery, pixGeneratedQuery, pixPaidQuery, botsPerformanceQuery,
              clicksByStateQuery, dailyRevenueQuery
        ]);

        const totalClicks = totalClicksResult[0].count;
        const totalPixGenerated = pixGeneratedResult[0].total;
        const totalRevenue = pixGeneratedResult[0].revenue;
        const totalPixPaid = pixPaidResult[0].total;
        const paidRevenue = pixPaidResult[0].revenue;
        
        res.status(200).json({
            total_clicks: parseInt(totalClicks),
            total_pix_generated: parseInt(totalPixGenerated),
            total_pix_paid: parseInt(totalPixPaid),
            total_revenue: parseFloat(totalRevenue),
            paid_revenue: parseFloat(paidRevenue),
            bots_performance: botsPerformance.map(b => ({ ...b, total_clicks: parseInt(b.total_clicks), total_pix_paid: parseInt(b.total_pix_paid), paid_revenue: parseFloat(b.paid_revenue) })),
            clicks_by_state: clicksByState.map(s => ({ ...s, total_clicks: parseInt(s.total_clicks) })),
            daily_revenue: dailyRevenue.filter(d => d.date).map(d => ({ date: d.date.toISOString().split('T')[0], revenue: parseFloat(d.revenue) }))
        });
    } catch (error) {
        console.error("Erro ao buscar métricas do dashboard:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
// ########## ROTA DE TRANSAÇÕES CORRIGIDA ##########
app.get('/api/transactions', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const { startDate, endDate } = req.query; // Pega as datas da query string
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        // 1. Iniciar a string da query e o array de parâmetros
        let queryString = `
            SELECT
                pt.status,
                pt.pix_value,
                COALESCE(tb_pressel.bot_name, tb_organic.bot_name, hc.config->'content'->>'main_title', 'Origem Desconhecida') as source_name,
                pt.provider,
                pt.created_at
            FROM pix_transactions pt
            JOIN clicks c ON pt.click_id_internal = c.id
            -- Join para bots via Pressel
            LEFT JOIN pressels p ON c.pressel_id = p.id
            LEFT JOIN telegram_bots tb_pressel ON p.bot_id = tb_pressel.id
            -- Join para bots via Tráfego Orgânico (direto do clique)
            LEFT JOIN telegram_bots tb_organic ON c.bot_id = tb_organic.id
            -- Join para Checkouts Hospedados
            LEFT JOIN hosted_checkouts hc ON c.checkout_id = hc.id
            WHERE c.seller_id = $1
        `;
        const queryParams = [sellerId]; // $1 é sellerId

        if (hasDateFilter) {
            // 2. Adicionar o filtro de data à string e os parâmetros ao array
            queryString += ` AND pt.created_at BETWEEN $2 AND $3`;
            queryParams.push(startDate); // $2 é startDate
            queryParams.push(endDate);   // $3 é endDate
        }

        // 3. Adicionar a ordenação
        queryString += ` ORDER BY pt.created_at DESC;`;

        // 4. Executar a consulta usando a sintaxe de função sql(query, params)
        //    Isto é diferente do "sql`...`" (template tag) e é o que resolve o erro.
        const transactions = await sql(queryString, queryParams);

        res.status(200).json(transactions);
    } catch (error) {
        console.error("Erro ao buscar transações:", error); // Loga o erro completo no console
        res.status(500).json({ message: 'Erro ao buscar dados das transações.' });
    }
});

app.post('/api/pix/generate', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id, value_cents, customer, product } = req.body;

    if (!apiKey || !click_id || !value_cents) return res.status(400).json({ message: 'API Key, click_id e value_cents são obrigatórios.' });

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) return res.status(401).json({ message: 'API Key inválida.' });

        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller.id}`;
        if (!click) return res.status(404).json({ message: 'Click ID não encontrado.' });

        const ip_address = click.ip_address;

        // *** SUBSTITUIÇÃO DA LÓGICA DO LOOP PELA NOVA FUNÇÃO ***
        const pixResult = await generatePixWithFallback(seller, value_cents, req.headers.host, apiKey, ip_address, click.id); // Passa click.id

        // O INSERT já foi feito dentro de generatePixWithFallback
        // Apenas continue com os eventos pós-geração

        if (click.pressel_id || click.checkout_id) { // Verifica se veio de pressel ou checkout
             await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: value_cents / 100 }, null);
        }

        const customerDataForUtmify = customer || { name: "Cliente Interessado", email: "cliente@email.com" };
        const productDataForUtmify = product || { id: "prod_1", name: "Produto Ofertado" };
        await sendEventToUtmify('waiting_payment', click, { provider_transaction_id: pixResult.transaction_id, pix_value: value_cents / 100, created_at: new Date() }, seller, customerDataForUtmify, productDataForUtmify);

        // Remove o internal_transaction_id antes de retornar para a API externa
        const { internal_transaction_id, ...apiResponse } = pixResult;
        return res.status(200).json(apiResponse);

    } catch (error) { // O catch agora pega o erro lançado por generatePixWithFallback se todos falharem
        console.error(`[PIX GENERATE ERROR] Erro na rota /api/pix/generate:`, error.message);
        // Retorna a mensagem de erro específica lançada pela função de fallback
        res.status(500).json({ message: error.message || 'Erro interno ao processar a geração de PIX.' });
    }
});

app.get('/api/pix/status/:transaction_id', async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { transaction_id } = req.params; // Pode ser ID do provedor ou nosso pix_id

    if (!apiKey) return res.status(401).json({ message: 'API Key não fornecida.' });
    if (!transaction_id) return res.status(400).json({ message: 'ID da transação é obrigatório.' });

    try {
        // Valida API Key e obtém o seller
        const [seller] = await sql`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) {
            return res.status(401).json({ message: 'API Key inválida.' });
        }

        // Busca a transação e dados do clique associado (inclui checkout_id e click_id originais)
        const [transaction] = await sql`
            SELECT pt.*, c.checkout_id, c.click_id
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            WHERE (pt.provider_transaction_id = ${transaction_id} OR pt.pix_id = ${transaction_id})
              AND c.seller_id = ${seller.id}`;

        if (!transaction) {
            return res.status(404).json({ status: 'not_found', message: 'Transação não encontrada.' });
        }

        let currentStatus = transaction.status;
        let customerData = {};

        // Se ainda pendente e passível de consulta, tenta confirmar no provedor
        if (currentStatus === 'pending' && transaction.provider !== 'oasyfy' && transaction.provider !== 'cnpay' && transaction.provider !== 'brpix') {
            try {
                let providerStatus;
                if (transaction.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                        headers: { 'Authorization': `Bearer ${syncPayToken}` }
                    });
                    providerStatus = response.data.status;
                    customerData = response.data.payer;
                } else if (transaction.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    customerData = { name: response.data.payer_name, document: response.data.payer_document };
                }

                if (providerStatus === 'paid' || providerStatus === 'COMPLETED') {
                    await handleSuccessfulPayment(transaction.id, customerData);
                    currentStatus = 'paid';
                }
            } catch (providerError) {
                if (!providerError.response || providerError.response.status !== 404) {
                    console.error(`Falha ao consultar o provedor (${transaction.provider}) para a transação ${transaction.id}:`, providerError.message);
                }
                // Continua retornando pending se não conseguiu confirmar
            }
        }

        if (currentStatus === 'paid') {
            let redirectUrl = null;
            // Se veio de checkout hospedado, tenta buscar URL de sucesso no config
            if (transaction.checkout_id && String(transaction.checkout_id).startsWith('cko_')) {
                const [checkoutConfig] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${transaction.checkout_id}`;
                // Ajuste o caminho conforme seu schema de config
                redirectUrl = checkoutConfig?.config?.redirects?.success_url || null;
                if (!redirectUrl) {
                    console.warn(`[PIX Status] Checkout ${transaction.checkout_id} sem redirects.success_url configurado.`);
                }
            }

            return res.status(200).json({
                status: 'paid',
                redirectUrl: redirectUrl,
                click_id: transaction.click_id
            });
        }

        return res.status(200).json({ status: currentStatus });

    } catch (error) {
        console.error("Erro ao consultar status da transação:", error);
        res.status(500).json({ message: 'Erro interno ao consultar o status.' });
    }
});
app.post('/api/pix/test-provider', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { provider } = req.body;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    if (!provider) {
        return res.status(400).json({ message: 'O nome do provedor é obrigatório.' });
    }

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        const value_cents = 3333;
        
        const startTime = Date.now();
        const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
        const endTime = Date.now();
        const responseTime = ((endTime - startTime) / 1000).toFixed(2);

        res.status(200).json({
            provider: provider.toUpperCase(),
            acquirer: pixResult.acquirer,
            responseTime: responseTime,
            qr_code_text: pixResult.qr_code_text
        });

    } catch (error) {
        console.error(`[PIX TEST ERROR] Seller ID: ${sellerId}, Provider: ${provider} - Erro:`, error.response?.data || error.message);
        res.status(500).json({ 
            message: `Falha ao gerar PIX de teste com ${provider.toUpperCase()}. Verifique as credenciais.`, 
            details: error.response?.data ? JSON.stringify(error.response.data) : error.message 
        });
    }
});
app.post('/api/pix/test-priority-route', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    let testLog = [];

    try {
        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        const providerOrder = [
            { name: seller.pix_provider_primary, position: 'Primário' },
            { name: seller.pix_provider_secondary, position: 'Secundário' },
            { name: seller.pix_provider_tertiary, position: 'Terciário' }
        ].filter(p => p.name); 

        if (providerOrder.length === 0) {
            return res.status(400).json({ message: 'Nenhuma ordem de prioridade de provedores foi configurada.' });
        }

        const value_cents = 3333;

        for (const providerInfo of providerOrder) {
            const provider = providerInfo.name;
            const position = providerInfo.position;
            
            try {
                const startTime = Date.now();
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
                const endTime = Date.now();
                const responseTime = ((endTime - startTime) / 1000).toFixed(2);

                testLog.push(`SUCESSO com Provedor ${position} (${provider.toUpperCase()}).`);
                return res.status(200).json({
                    success: true, position: position, provider: provider.toUpperCase(),
                    acquirer: pixResult.acquirer, responseTime: responseTime,
                    qr_code_text: pixResult.qr_code_text, log: testLog
                });

            } catch (error) {
                let errorMessage = error.message;
                if (error.response && error.response.data) {
                    errorMessage = JSON.stringify(error.response.data);
                }
                console.error(`Falha no provedor ${position} (${provider}):`, error.response?.data || error.message);
                testLog.push(`FALHA com Provedor ${position} (${provider.toUpperCase()}): ${errorMessage}`);
            }
        }

        console.error("Todos os provedores na rota de prioridade falharam.");
        return res.status(500).json({
            success: false, message: 'Todos os provedores configurados na sua rota de prioridade falharam.',
            log: testLog
        });

    } catch (error) {
        console.error(`[PIX PRIORITY TEST ERROR] Erro geral:`, error.message);
        res.status(500).json({ 
            success: false, message: 'Ocorreu um erro inesperado ao testar a rota de prioridade.',
            log: testLog
        });
    }
});

// ==========================================================
//          MOTOR DE FLUXO E WEBHOOK DO TELEGRAM (VERSÃO FINAL)
// ==========================================================
function findNextNode(currentNodeId, handleId, edges) {
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null;
}

async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, {
            chat_id: chatId,
            action: 'typing',
        });
    } catch (error) {
        console.warn(`[Flow Engine] Falha ao enviar ação 'typing' para ${chatId}:`, error.response?.data || error.message);
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping, variables = {}) {
    if (!text || text.trim() === '') return;
    try {
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            let typingDuration = Math.max(500, Math.min(2000, text.length * 50));
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }
        const response = await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        if (response.data.ok) {
            const sentMessage = response.data.result;
            // CORREÇÃO FINAL: Salva NULL para os dados do usuário quando o remetente é o bot.
            await sql`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, click_id)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, NULL, NULL, NULL, ${text}, 'bot', ${variables.click_id || null})
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        console.error(`[Flow Engine] Erro ao enviar/salvar mensagem:`, error.response?.data || error.message);
    }
}

async function processActions(actions, chatId, botId, botToken, sellerId, variables, edges, logPrefix = '[Actions]') {
    console.log(`${logPrefix} Iniciando processamento de ${actions.length} ações aninhadas para chat ${chatId}`);
    for (const action of actions) {
        // CORREÇÃO: Usar 'action.data' e não 'currentNode.data'
        const actionData = action.data || {}; // Garante que actionData exista

        switch (action.type) {
            case 'message':
                if (actionData.typingDelay && actionData.typingDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, actionData.typingDelay * 1000));
                }

                const textToSend = await replaceVariables(actionData.text, variables);
                await sendMessage(chatId, textToSend, botToken, sellerId, botId, actionData.showTyping, variables);
                
                // Processamento recursivo (se esta ação tiver ações)
                if (actionData.actions && actionData.actions.length > 0) {
                    await processActions(actionData.actions, chatId, botId, botToken, sellerId, variables, edges, `${logPrefix}-Nested`);
                }

                // REMOVIDO: Lógica de waitForReply e findNextNode não pertencem aqui.
                // A função 'processFlow' é quem controla a navegação entre os NÓS.
                // Esta função 'processActions' apenas executa a lista de AÇÕES.
                break;

            case 'image':
            case 'video':
            case 'audio': {
                try {
                    const caption = await replaceVariables(actionData.caption, variables);
                    // Passa o objeto 'action' (que tem 'type' e 'data')
                    const response = await handleMediaNode(action, botToken, chatId, caption);

                    if (response && response.ok) {
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    console.error(`${logPrefix} [Flow Media] Erro ao enviar mídia (ação ${action.type}) para o chat ${chatId}: ${e.message}`);
                }
                // REMOVIDO: findNextNode
                break;
            }

            case 'delay':
                const delaySeconds = actionData.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                // REMOVIDO: findNextNode
                break;
            
            case 'action_pix':
                try {
                    console.log(`${logPrefix} Executando action_pix para chat ${chatId}`);
                    const valueInCents = actionData.valueInCents;
                    if (!valueInCents) throw new Error("Valor do PIX não definido na ação do fluxo.");
    
                    const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    if (!seller) throw new Error(`${logPrefix} Vendedor ${sellerId} não encontrado.`);
    
                    let click_id_from_vars = variables.click_id;
                    if (!click_id_from_vars) {
                        const [recentClick] = await sql`
                            SELECT click_id FROM telegram_chats 
                            WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL 
                            ORDER BY created_at DESC LIMIT 1`;
                        if (recentClick?.click_id) {
                            click_id_from_vars = recentClick.click_id;
                        }
                    }
    
                    if (!click_id_from_vars) {
                        throw new Error(`${logPrefix} Click ID não encontrado para gerar PIX.`);
                    }
    
                    const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                    const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error(`${logPrefix} Click ID não encontrado para este vendedor.`);
    
                    const ip_address = click.ip_address;
                    const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
                    const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);
    
                    variables.last_transaction_id = pixResult.transaction_id;
                    // A função 'processFlow' que chamou 'processActions' deve persistir as 'variables' atualizadas.
    
                    const messageText = await replaceVariables(actionData.pixMessage || "✅ PIX Gerado! Copie:", variables);
                    const buttonText = await replaceVariables(actionData.pixButtonText || "📋 Copiar", variables);
                    const pixToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                        chat_id: chatId, text: pixToSend, parse_mode: 'HTML',
                        reply_markup: { inline_keyboard: [[{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]] }
                    });
    
                    if (sentMessage.ok) {
                        await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot');
                        console.log(`${logPrefix} PIX gerado com sucesso para chat ${chatId}, transação ${pixResult.transaction_id}`);
                    }
    
                } catch (error) {
                    console.error(`${logPrefix} Erro no nó action_pix para chat ${chatId}:`, error);
                }
                // REMOVIDO: findNextNode
                break;

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX nas variáveis.");
                    
                    const [transaction] = await sql`SELECT status FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        console.log(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true, variables);
                    } else {
                        console.log(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true, variables);
                    }
                } catch (error) {
                    console.error(`${logPrefix} Erro ao consultar PIX:`, error);
                }
                // REMOVIDO: findNextNode
                break;

            default:
                console.warn(`${logPrefix} Tipo de ação aninhada desconhecida: ${action.type}. Ignorando.`);
                // REMOVIDO: findNextNode e currentNodeId = null
                break;
        }
    }
}

async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    
    console.log(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);

    // ==========================================================
    // PASSO 1: CARREGAR AS VARIÁVEIS NO INÍCIO
    // ==========================================================
    let variables = { ...initialVariables };

    // Pega os dados do usuário da última mensagem ENVIADA PELO USUÁRIO
    const [user] = await sql`
        SELECT first_name, last_name 
        FROM telegram_chats 
        WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
        ORDER BY created_at DESC LIMIT 1`;

    if (user) {
        variables.primeiro_nome = user.first_name || '';
        variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }

    // Se tiver um click_id, busca os dados de geolocalização (cidade)
    if (variables.click_id) {
        const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
        const [click] = await sql`SELECT city, state FROM clicks WHERE click_id = ${db_click_id}`;
        if (click) {
            variables.cidade = click.city || 'Desconhecida';
            variables.estado = click.state || 'Desconhecido';
        }
    }
    // ==========================================================
    // FIM DO PASSO 1
    // ==========================================================
    
    const [flow] = await sql`SELECT * FROM flows WHERE bot_id = ${botId} ORDER BY updated_at DESC LIMIT 1`;
    if (!flow || !flow.nodes) {
        console.log(`${logPrefix} [Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
        return;
    }

    const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
    const nodes = flowData.nodes || [];
    const edges = flowData.edges || [];

    let currentNodeId = startNodeId;
    const isStartCommand = initialVariables.click_id && initialVariables.click_id.startsWith('/start');

    if (!currentNodeId) {
        if (isStartCommand) {
            console.log(`${logPrefix} [Flow Engine] Comando /start detectado. Reiniciando fluxo.`);

            // CORREÇÃO: Cancela a tarefa de timeout pendente ANTES de reiniciar o fluxo.
            const [stateToCancel] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (stateToCancel && stateToCancel.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                    console.log(`[Flow Engine] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada com sucesso antes de reiniciar.`);
                } catch (e) {
                    const errorMessage = e.response?.data?.error || e.message || '';
                    if (errorMessage.includes('invalid message id')) {
                        console.warn(`[Flow Engine] QStash retornou 'invalid message id' para ${stateToCancel.scheduled_message_id} durante o reinício. Ignorando.`);
                    } else {
                        console.error(`[Flow Engine] Erro CRÍTICO ao tentar cancelar a tarefa de timeout ${stateToCancel.scheduled_message_id}:`, e.response?.data || e.message || e);
                    }
                }
            }

            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            const startNode = nodes.find(node => node.type === 'trigger');
            if (startNode) {
                currentNodeId = findNextNode(startNode.id, null, edges);
            }
        } else {
            const [userState] = await sql`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (userState && userState.waiting_for_input) {
                console.log(`${logPrefix} [Flow Engine] Usuário respondeu. Continuando.`);
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges);
                
                let parsedVariables = {};
                if (userState.variables) {
                    try {
                        parsedVariables = JSON.parse(userState.variables);
                    } catch (e) {
                        parsedVariables = userState.variables;
                    }
                }
                // Une as variáveis já carregadas com as salvas no estado
                variables = { ...variables, ...parsedVariables };

            } else {
                console.log(`${logPrefix} [Flow Engine] Nova conversa sem /start. Iniciando do gatilho.`);
                const startNode = nodes.find(node => node.type === 'trigger');
                if (startNode) {
                    currentNodeId = findNextNode(startNode.id, null, edges);
                }
            }
        }
    }

    if (!currentNodeId) {
        console.log(`${logPrefix} [Flow Engine] Nenhum nó para processar. Fim do fluxo.`);
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        return;
    }

    let safetyLock = 0;
    while (currentNodeId && safetyLock < 20) {
        const currentNode = nodes.find(node => node.id === currentNodeId);
        if (!currentNode) {
            console.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        await sql`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET current_node_id = EXCLUDED.current_node_id, variables = EXCLUDED.variables, waiting_for_input = false, scheduled_message_id = NULL;
        `;

        switch (currentNode.type) {
            case 'message':
                if (currentNode.data.typingDelay && currentNode.data.typingDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, currentNode.data.typingDelay * 1000));
                }

                // ==========================================================
                // PASSO 2: USAR A VARIÁVEL CORRETA AO ENVIAR A MENSAGEM
                // ==========================================================
                const textToSend = await replaceVariables(currentNode.data.text, variables);
                await sendMessage(chatId, textToSend, botToken, sellerId, botId, currentNode.data.showTyping, variables);
                // ==========================================================
                // FIM DO PASSO 2
                // ==========================================================
                console.log(currentNode.data)
                console.log(currentNode.data.actions)
                // Execute nested actions if any
                if (currentNode.data.actions && currentNode.data.actions.length > 0) {
                    console.log(`${logPrefix} [Flow Engine] Executando ${currentNode.data.actions.length} ações aninhadas no nó message`);
                    await processActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, edges);
                    // Persist updated variables after processing actions
                    await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                }

            if (currentNode.data.waitForReply) {
                await sql`UPDATE user_flow_states SET waiting_for_input = true WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                const timeoutMinutes = currentNode.data.replyTimeout || 5;
                console.log(`${logPrefix} [Flow Engine] Agendando worker em ${timeoutMinutes} min para encerrar o fluxo`);
                try {
                    // cancel old
                    const [existingState] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    if (existingState && existingState.scheduled_message_id) {
                        try {
                            await qstashClient.messages.delete(existingState.scheduled_message_id);
                            console.log(`[Flow Engine] Tarefa de timeout antiga ${existingState.scheduled_message_id} cancelada.`);
                        } catch (e) {
                            console.warn(`[Flow Engine] Não foi possível cancelar a tarefa antiga ${existingState.scheduled_message_id}:`, e.message);
                        }
                    }
                    const response = await qstashClient.publishJSON({
                        url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                        body: { chat_id: chatId, bot_id: botId, target_node_id: null, variables: variables },
                        delay: `${timeoutMinutes}m`,
                        contentBasedDeduplication: true,
                        method: "POST"
                    });
                    await sql`UPDATE user_flow_states SET scheduled_message_id = ${response.messageId} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                } catch (error) {
                    console.error("Erro ao agendar timeout:", error);
                }
                currentNodeId = null;
            } else {
                currentNodeId = findNextNode(currentNodeId, 'a', edges);
            }
                break;

            // ===== IMPLEMENTAÇÃO DOS NÓS DE MÍDIA =====
            case 'image':
            case 'video':
            case 'audio': {
                try {
                    const caption = await replaceVariables(currentNode.data.caption, variables);
                    const response = await handleMediaNode(currentNode, botToken, chatId, caption);

                    if (response && response.ok) {
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    console.error(`[Flow Media] Erro ao enviar mídia no nó ${currentNode.id} para o chat ${chatId}: ${e.message}`);
                }
                currentNodeId = findNextNode(currentNodeId, 'a', edges);
                break;
            }
            // ===========================================

            case 'delay':
                const delaySeconds = currentNode.data.delayInSeconds || 1;
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                currentNodeId = findNextNode(currentNodeId, null, edges);
                break;
            
                case 'action_pix':
                    try {
                        const valueInCents = currentNode.data.valueInCents;
                        if (!valueInCents) throw new Error("Valor do PIX não definido no nó do fluxo.");
    
                        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
                         // Adiciona verificação do seller
                        if (!seller) throw new Error(`Vendedor ${sellerId} não encontrado no processFlow.`);
    
                        // Busca o click_id das variáveis do fluxo ou do banco de dados
                        let click_id_from_vars = variables.click_id;
                        
                        // Se não encontrou nas variáveis, tenta buscar do banco de dados
                        if (!click_id_from_vars) {
                            const [recentClick] = await sql`
                                SELECT click_id FROM telegram_chats 
                                WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL 
                                ORDER BY created_at DESC LIMIT 1
                            `;
                            if (recentClick?.click_id) {
                                click_id_from_vars = recentClick.click_id;
                                console.log(`[Flow Engine] Click ID recuperado do banco: ${click_id_from_vars}`);
                            }
                        }
                        
                        if (!click_id_from_vars) {
                            console.error(`[Flow Engine] Click ID não encontrado para chat ${chatId}, bot ${botId}. Variáveis:`, variables);
                            throw new Error("Click ID não encontrado nas variáveis do fluxo nem no histórico do chat.");
                        }
    
                        const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                        const [click] = await sql`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                        if (!click) throw new Error("Dados do clique não encontrados ou não pertencem ao vendedor para gerar o PIX no fluxo.");
    
                        const ip_address = click.ip_address; // IP do clique original
    
                        // *** SUBSTITUIÇÃO DA CHAMADA DIRETA PELA NOVA FUNÇÃO ***
                        // Usar 'localhost' ou um placeholder se req.headers.host não estiver disponível aqui
                        const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
                        const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id); // Passa click.id

                        // Dispara eventos de InitiateCheckout (Meta) e waiting_payment (Utmify)
                        try {
                            await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: valueInCents / 100 }, null);
                        } catch (e) { console.warn(`[Flow Engine] Falha ao enviar InitiateCheckout: ${e.message}`); }
                        try {
                            const customerDataForUtmify = { name: "Cliente Interessado", email: "cliente@email.com" };
                            const productDataForUtmify = { id: "prod_1", name: "Produto Ofertado" };
                            await sendEventToUtmify('waiting_payment', click, { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() }, seller, customerDataForUtmify, productDataForUtmify);
                        } catch (e) { console.warn(`[Flow Engine] Falha ao enviar waiting_payment para Utmify: ${e.message}`); }
    
                        // O INSERT já foi feito dentro de generatePixWithFallback
    
                        variables.last_transaction_id = pixResult.transaction_id;
                        // Salva as variáveis atualizadas (com o last_transaction_id)
                        await sql`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
    
                        // Envia o PIX para o usuário
                        const messageText = await replaceVariables(currentNode.data.pixMessage || "✅ PIX Gerado! Copie o código abaixo:", variables);
                        const buttonText = await replaceVariables(currentNode.data.pixButtonText || "📋 Copiar Código PIX", variables);
                        const textToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                        const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                            chat_id: chatId,
                            text: textToSend,
                            parse_mode: 'HTML',
                            reply_markup: {
                                inline_keyboard: [
                                    [{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]
                                ]
                            }
                        });
    
                         if (sentMessage.ok) {
                             await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot'); // Salva como 'bot'
                         }
    
                    } catch (error) {
                        console.error(`[Flow Engine] Erro no nó action_pix para chat ${chatId}:`, error);
                        // Informa o usuário sobre o erro
                        console.log(chatId, "Desculpe, não consegui gerar o PIX neste momento. Tente novamente mais tarde.", botToken, sellerId, botId, true);
                        // Decide se o fluxo deve parar ou seguir por um caminho de erro (se houver)
                        // Por enquanto, vamos parar aqui para evitar loops
                        currentNodeId = null; // Para o fluxo neste ponto em caso de erro no PIX
                        break; // Sai do switch
                    }
                    // Se chegou aqui, o PIX foi gerado e enviado com sucesso
                    currentNodeId = findNextNode(currentNodeId, 'a', edges); // Assume que a saída 'a' é o caminho de sucesso
                    break; // Sai do switch

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX encontrado para consultar.");
                    
                    const [transaction] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    if (transaction.status === 'paid') {
                        console.log(chatId, "Pagamento confirmado! ✅", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'a', edges); // Caminho 'Pago'
                    } else {
                        console.log(chatId, "Ainda estamos aguardando o pagamento.", botToken, sellerId, botId, true);
                        currentNodeId = findNextNode(currentNodeId, 'b', edges); // Caminho 'Pendente'
                    }
                } catch (error) {
                    console.error("[Flow Engine] Erro ao consultar PIX:", error);
                    currentNodeId = findNextNode(currentNodeId, 'b', edges);
                }
                break;

                default:
                    console.warn(`${logPrefix} [Flow Engine] Tipo de nó desconhecido: ${currentNode.type}. Parando fluxo.`);
                    currentNodeId = null;
                    break;
            }

            if (!currentNodeId) {
                const [state] = await sql`SELECT 1 FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId} AND waiting_for_input = true`;
                if(!state){
                    console.log(`${logPrefix} [Flow Engine] Fim do fluxo para ${chatId}. Limpando estado.`);
                    await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                }
            }
            safetyLock++;
    }
}

app.post('/api/webhook/telegram/:botId', async (req, res) => {
    const { botId } = req.params;
    // CORREÇÃO 1: Extrai o objeto 'message' do corpo da requisição logo no início.
    const { message } = req.body; 
    
    // Responde imediatamente ao Telegram para evitar timeouts.
    res.sendStatus(200);

    try {
        // Validação mais robusta da estrutura da mensagem
        if (!message) {
            console.warn('[Webhook] Requisição ignorada: objeto message ausente.');
            return;
        }
        
        if (!message.chat) {
            console.warn('[Webhook] Requisição ignorada: objeto chat ausente na mensagem.');
            return;
        }
        
        if (!message.chat.id) {
            console.warn('[Webhook] Requisição ignorada: chat.id ausente na mensagem.');
            return;
        }
        
        // Verifica se é uma mensagem válida (não callback_query, etc.)
        if (message.message_id === undefined) {
            console.warn('[Webhook] Requisição ignorada: message_id ausente (pode ser callback_query ou outro tipo).');
            return;
        }
        const chatId = message.chat.id;
        const text = message.text || '';
        const isStartCommand = text.startsWith('/start');

        const [bot] = await sql`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${botId}`;
        if (!bot) {
            console.warn(`[Webhook] Bot ID ${botId} não encontrado.`);
            return;
        }
        const { seller_id: sellerId, bot_token: botToken } = bot;

        // Salva a mensagem do usuário (seja /start ou resposta)
        await saveMessageToDb(sellerId, botId, message, 'user');

        // PRIORIDADE 1: Comando /start reinicia tudo.
        if (isStartCommand) {
            console.log(`[Webhook] Comando /start recebido. Reiniciando fluxo para o chat ${chatId}.`);
            
            // Cancela qualquer tarefa pendente e deleta o estado antigo.
            const [existingState] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (existingState && existingState.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(existingState.scheduled_message_id);
                    console.log(`[Webhook] Tarefa de timeout antiga cancelada devido ao /start.`);
                } catch (e) { /* Ignora erros se a tarefa já foi executada */ }
            }
            await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;

            // Lógica para criar click_id (orgânico ou de campanha)
            let clickIdValue = null;
            const parts = text.split(' ');
            if (parts.length > 1 && parts[1].trim() !== '') {
                clickIdValue = text;
                console.log(`[Webhook] Click ID de campanha detectado: ${clickIdValue}`);
            } else {
                console.log(`[Webhook] Tráfego orgânico do bot detectado. Gerando novo click_id.`);
                const [newClick] = await sql`INSERT INTO clicks (seller_id, bot_id, is_organic) VALUES (${sellerId}, ${botId}, TRUE) RETURNING id`;
                clickIdValue = `/start bot_org_${newClick.id.toString().padStart(7, '0')}`;
                await sql`UPDATE clicks SET click_id = ${clickIdValue} WHERE id = ${newClick.id}`;
                console.log(`[Webhook] Novo click_id orgânico gerado: ${clickIdValue}`);
            }
            
            // Inicia o fluxo do zero.
            await processFlow(chatId, botId, botToken, sellerId, null, { click_id: clickIdValue });
            return; // Finaliza a execução.
        }

        // PRIORIDADE 2: Se não for /start, trata como uma resposta normal.
        const [userState] = await sql`SELECT current_node_id, variables, scheduled_message_id, waiting_for_input FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        
        if (userState && userState.waiting_for_input) {
            // Cancela o timeout, pois o usuário respondeu.
            if (userState.scheduled_message_id) {
                 try {
                    await qstashClient.messages.delete(userState.scheduled_message_id);
                    console.log(`[Webhook] Tarefa de timeout cancelada pela resposta do usuário.`);
                } catch (e) { /* Ignora erros */ }
            }

            // Continua o fluxo a partir do próximo nó.
            const [flow] = await sql`SELECT nodes FROM flows WHERE bot_id = ${botId}`;
            if (flow && flow.nodes) {
                const nodes = flow.nodes.nodes || [];
                const edges = flow.nodes.edges || [];
                const nextNodeId = findNextNode(userState.current_node_id, 'a', edges);

                if (nextNodeId) {
                    await processFlow(chatId, botId, botToken, sellerId, nextNodeId, userState.variables);
                } else {
                    console.log(`[Webhook] Fim do fluxo após resposta do usuário.`);
                    await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                }
            } else {
                 console.error(`[Webhook] Fluxo para o bot ${botId} não encontrado ao tentar continuar.`);
            }
        } else {
            console.log(`[Webhook] Mensagem de ${chatId} ignorada (não é /start e não há fluxo esperando resposta).`);
        }
    
    } catch (error) {
        // Este catch agora só pegará erros realmente inesperados no fluxo principal.
        console.error("Erro GERAL e INESPERADO ao processar webhook do Telegram:", error);
// ... (restante do código permanece igual a partir daqui)
    }
});


app.get('/api/dispatches', authenticateJwt, async (req, res) => {
    try {
        const dispatches = await sql`SELECT * FROM mass_sends WHERE seller_id = ${req.user.id} ORDER BY sent_at DESC;`;
        res.status(200).json(dispatches);
    } catch (error) {
        console.error("Erro ao buscar histórico de disparos:", error);
        res.status(500).json({ message: 'Erro ao buscar histórico.' });
    }
});
app.get('/api/dispatches/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        const details = await sql`
            SELECT d.*, u.first_name, u.username
            FROM mass_send_details d
            LEFT JOIN telegram_chats u ON d.chat_id = u.chat_id
            WHERE d.mass_send_id = ${id}
            ORDER BY d.sent_at;
        `;
        res.status(200).json(details);
    } catch (error) {
        console.error("Erro ao buscar detalhes do disparo:", error);
        res.status(500).json({ message: 'Erro ao buscar detalhes.' });
    }
});

app.post('/api/bots/mass-send', authenticateJwt, async (req, res) => {
        const sellerId = req.user.id;
        const { botIds, flowSteps, campaignName } = req.body;
    
        if (!botIds || !Array.isArray(botIds) || botIds.length === 0 || !Array.isArray(flowSteps) || flowSteps.length === 0 || !campaignName) {
            return res.status(400).json({ message: 'Nome da campanha, IDs dos Bots e pelo menos um passo no fluxo são obrigatórios.' });
        }
    
        let historyId; 
    
        try {
            // 1. Buscar todos os contatos únicos (semelhante a antes)
            const allContacts = new Map();
            for (const botId of botIds) {
                const [botCheck] = await sqlWithRetry(
                    'SELECT id FROM telegram_bots WHERE id = $1 AND seller_id = $2',
                    [botId, sellerId]
                );
                if (!botCheck) continue; 
    
            const contacts = await sqlWithRetry(
                'SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username, click_id FROM telegram_chats WHERE bot_id = $1 AND seller_id = $2 ORDER BY chat_id, created_at DESC',
                [botId, sellerId]
            );
                contacts.forEach(c => {
                    if (!allContacts.has(c.chat_id)) {
                        allContacts.set(c.chat_id, { ...c, bot_id_source: botId });
                    }
                });
            }
            const uniqueContacts = Array.from(allContacts.values());
    
            if (uniqueContacts.length === 0) {
                return res.status(404).json({ message: 'Nenhum contato encontrado para os bots selecionados.' });
            }
    
            // --- MUDANÇA PRINCIPAL AQUI ---
            // 2. Calcular o total de trabalhos (Contatos * Passos)
            const total_jobs_to_queue = uniqueContacts.length * flowSteps.length;
            if (total_jobs_to_queue === 0) {
                return res.status(400).json({ message: 'Nenhum trabalho a ser agendado (0 contatos ou 0 passos).' });
            }
    
            // 3. Criar o registro mestre da campanha com os totais corretos
            const [history] = await sqlWithRetry(
                sql`INSERT INTO disparo_history (
                    seller_id, campaign_name, bot_ids, flow_steps, 
                    status, total_sent, failure_count, 
                    total_jobs, processed_jobs
                   ) 
                   VALUES (
                    ${sellerId}, ${campaignName}, ${JSON.stringify(botIds)}, ${JSON.stringify(flowSteps)}, 
                    'PENDING', ${uniqueContacts.length}, 0, 
                    ${total_jobs_to_queue}, 0
                   ) 
                   RETURNING id`
            );
            historyId = history.id;
            // --- FIM DA MUDANÇA ---
    
            // 4. Publicar CADA TAREFA (step) para o QStash com um atraso
            let messageCounter = 0;
            const delayBetweenMessages = 1; // 1 segundo de atraso entre cada contato
            const qstashPromises = [];
    
            for (const contact of uniqueContacts) {
                const userVariables = {
                    primeiro_nome: contact.first_name || '',
                    nome_completo: `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
                    click_id: contact.click_id ? contact.click_id.replace('/start ', '') : null
                };
                
                let currentStepDelay = 0; 
    
                for (const step of flowSteps) {
                    const payload = {
                        history_id: historyId,
                        chat_id: contact.chat_id,
                        bot_id: contact.bot_id_source,
                        step_json: JSON.stringify(step),
                        variables_json: JSON.stringify(userVariables)
                    };
    
                    const totalDelaySeconds = (messageCounter * delayBetweenMessages) + currentStepDelay;
    
                    qstashPromises.push(
                        qstashClient.publishJSON({
                            url: `${process.env.HOTTRACK_API_URL}/api/worker/process-disparo`, 
                            body: payload,
                            delay: `${totalDelaySeconds}s`, 
                            retries: 2 
                        })
                    );
                    
                    const delayData = step.data || step; 
                    if (step.type === 'delay') {
                        currentStepDelay += (delayData.delayInSeconds || 1);
                    } else {
                        currentStepDelay += 1; 
                    }
                }
                messageCounter++; 
            }
    
            await Promise.all(qstashPromises);
    
            // 5. Atualizar o status da campanha para "RUNNING"
            await sqlWithRetry(
                sql`UPDATE disparo_history SET status = 'RUNNING' WHERE id = ${historyId}`
            );
    
            res.status(202).json({ message: `Disparo "${campaignName}" para ${uniqueContacts.length} contatos agendado com sucesso! O processo ocorrerá em segundo plano.` });
    
        } catch (error) {
            console.error("Erro crítico no agendamento do disparo:", error);
            if(historyId) {
                await sqlWithRetry('DELETE FROM disparo_history WHERE id = $1', [historyId]).catch(e => console.error("Falha ao limpar histórico órfão:", e));
            }
            if (!res.headersSent) {
                res.status(500).json({ message: 'Erro interno ao agendar o disparo.' });
            }
        }
    });


app.post('/api/webhook/pushinpay', async (req, res) => {
    const { id, status, payer_name, payer_document } = req.body;
    if (status === 'paid') {
        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${id} AND provider = 'pushinpay'`;
            if (tx && tx.status !== 'paid') {
                await handleSuccessfulPayment(tx.id, { name: payer_name, document: payer_document });
            }
        } catch (error) { console.error("Erro no webhook da PushinPay:", error); }
    }
    res.sendStatus(200);
});
app.post('/api/webhook/cnpay', async (req, res) => {
    const { transactionId, status, customer } = req.body;
    if (status === 'COMPLETED') {
        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'cnpay'`;
            if (tx && tx.status !== 'paid') {
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.taxID?.taxID });
            }
        } catch (error) { console.error("Erro no webhook da CNPay:", error); }
    }
    res.sendStatus(200);
});
app.post('/api/webhook/oasyfy', async (req, res) => {
    console.log('[Webhook Oasy.fy] Corpo completo do webhook recebido:', JSON.stringify(req.body, null, 2));
    const transactionData = req.body.transaction;
    const customer = req.body.client;
    if (!transactionData || !transactionData.status) {
        console.log("[Webhook Oasy.fy] Webhook ignorado: objeto 'transaction' ou 'status' ausente.");
        return res.sendStatus(200);
    }
    const { id: transactionId, status } = transactionData;
    if (status === 'COMPLETED') {
        try {
            console.log(`[Webhook Oasy.fy] Processando pagamento para transactionId: ${transactionId}`);
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'oasyfy'`;
            if (tx) {
                console.log(`[Webhook Oasy.fy] Transação encontrada no banco. ID interno: ${tx.id}, Status atual: ${tx.status}`);
                if (tx.status !== 'paid') {
                    console.log(`[Webhook Oasy.fy] Status não é 'paid'. Chamando handleSuccessfulPayment...`);
                    await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
                    console.log(`[Webhook Oasy.fy] handleSuccessfulPayment concluído para transação ID ${tx.id}.`);
                } else {
                    console.log(`[Webhook Oasy.fy] Transação ${transactionId} já está marcada como 'paid'. Nenhuma ação necessária.`);
                }
            } else {
                console.error(`[Webhook Oasy.fy] ERRO CRÍTICO: Transação com provider_transaction_id = '${transactionId}' NÃO FOI ENCONTRADA no banco de dados.`);
            }
        } catch (error) { 
            console.error("[Webhook Oasy.fy] ERRO DURANTE O PROCESSAMENTO:", error); 
        }
    } else {
        console.log(`[Webhook Oasy.fy] Recebido webhook com status '${status}', que não é 'COMPLETED'. Ignorando.`);
    }
    res.sendStatus(200);
});

async function sendEventToUtmify(status, clickData, pixData, sellerData, customerData, productData) {
    console.log(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            console.log(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sql`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            console.log(`[Utmify] Clique originado do Checkout ID: ${clickData.checkout_id}. Tentando resolver integração a partir do checkout.`);
            try {
                const [checkout] = await sql`SELECT config FROM hosted_checkouts WHERE id = ${clickData.checkout_id}`;
                const cfg = checkout?.config || {};
                const utmifyId = cfg.utmify_integration_id || cfg.integration_id || null;
                if (utmifyId) {
                    integrationId = utmifyId;
                }
            } catch (e) {
                console.warn(`[Utmify] Não foi possível obter integração do checkout ${clickData.checkout_id}: ${e.message}`);
            }
        }

        if (!integrationId) {
            console.log(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        console.log(`[Utmify] Integração vinculada ID: ${integrationId}. Buscando token...`);
        const [integration] = await sql`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            console.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        console.log(`[Utmify] Token encontrado. Montando payload...`);
        
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id, platform: "HotTrack", paymentMethod: 'pix',
            status: status, createdAt: createdAt, approvedDate: approvedDate, refundedAt: null,
            customer: { name: customerData?.name || "Não informado", email: customerData?.email || "naoinformado@email.com", phone: customerData?.phone || null, document: customerData?.document || null, },
            products: [{ id: productData?.id || "default_product", name: productData?.name || "Produto Digital", planId: null, planName: null, quantity: 1, priceInCents: Math.round(pixData.pix_value * 100) }],
            trackingParameters: { src: null, sck: null, utm_source: clickData.utm_source, utm_campaign: clickData.utm_campaign, utm_medium: clickData.utm_medium, utm_content: clickData.utm_content, utm_term: clickData.utm_term },
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0299)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0299))) },
            isTest: false
        };

        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        console.log(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        console.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}
async function sendMetaEvent(eventName, clickData, transactionData, customerData = null) {
    try {
        let presselPixels = [];
        if (clickData.pressel_id) {
            presselPixels = await sql`SELECT pixel_config_id FROM pressel_pixels WHERE pressel_id = ${clickData.pressel_id}`;
        } else if (clickData.checkout_id) {
            presselPixels = await sql`SELECT pixel_config_id FROM checkout_pixels WHERE checkout_id = ${clickData.checkout_id}`;
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
        
        for (const { pixel_config_id } of presselPixels) {
            const [pixelConfig] = await sql`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE id = ${pixel_config_id}`;
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

                if (eventName === 'Purchase') {
                     await sql`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                }
            }
        }
    } catch (error) {
        console.error(`Erro ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}
async function checkPendingTransactions() {
    try {
        const pendingTransactions = await sql`
            SELECT id, provider, provider_transaction_id, click_id_internal, status
            FROM pix_transactions WHERE status = 'pending' AND created_at > NOW() - INTERVAL '30 minutes'`;

        if (pendingTransactions.length === 0) return;
        
        for (const tx of pendingTransactions) {
            if (tx.provider === 'oasyfy' || tx.provider === 'cnpay' || tx.provider === 'brpix') {
                continue;
            }

            try {
                const [seller] = await sql`
                    SELECT *
                    FROM sellers s JOIN clicks c ON c.seller_id = s.id
                    WHERE c.id = ${tx.click_id_internal}`;
                if (!seller) continue;

                let providerStatus, customerData = {};
                if (tx.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${tx.provider_transaction_id}`, { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                    providerStatus = response.data.status;
                    customerData = response.data.payer;
                } else if (tx.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${tx.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    customerData = { name: response.data.payer_name, document: response.data.payer_document };
                }
                
                if ((providerStatus === 'paid' || providerStatus === 'COMPLETED') && tx.status !== 'paid') {
                     await handleSuccessfulPayment(tx.id, customerData);
                }
            } catch (error) {
                if (!error.response || error.response.status !== 404) {
                    console.error(`Erro ao verificar transação ${tx.id} (${tx.provider}):`, error.response?.data || error.message);
                }
            }
            await new Promise(resolve => setTimeout(resolve, 200)); 
        }
    } catch (error) {
        console.error("Erro na rotina de verificação geral:", error.message);
    }
}

app.post('/api/webhook/syncpay', async (req, res) => {
    try {
        const notification = req.body;
        console.log('[Webhook SyncPay] Notificação recebida:', JSON.stringify(notification, null, 2));

        if (!notification.data) {
            console.log('[Webhook SyncPay] Webhook ignorado: formato inesperado, objeto "data" não encontrado.');
            return res.sendStatus(200);
        }

        const transactionData = notification.data;
        const transactionId = transactionData.id;
        const status = transactionData.status;
        const customer = transactionData.client;

        if (!transactionId || !status) {
            console.log('[Webhook SyncPay] Ignorado: "id" ou "status" não encontrados dentro do objeto "data".');
            return res.sendStatus(200);
        }

        if (String(status).toLowerCase() === 'completed') {
            
            console.log(`[Webhook SyncPay] Processando pagamento para transação: ${transactionId}`);
            
            const [tx] = await sql`
                SELECT * FROM pix_transactions 
                WHERE provider_transaction_id = ${transactionId} AND provider = 'syncpay'
            `;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook SyncPay] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document });
            } else if (tx) {
                console.log(`[Webhook SyncPay] Transação ${tx.id} já estava como 'paga'.`);
            } else {
                console.warn(`[Webhook SyncPay] AVISO: Transação com ID ${transactionId} não foi encontrada no banco de dados.`);
            }
        }
        
        res.sendStatus(200);
    
    } catch (error) {
        console.error("Erro CRÍTICO no webhook da SyncPay:", error);
        res.sendStatus(500);
    }
});

app.post('/api/webhook/brpix', async (req, res) => {
    const { event, data } = req.body;
    console.log('[Webhook BRPix] Notificação recebida:', JSON.stringify(req.body, null, 2));

    if (event === 'transaction.updated' && data?.status === 'paid') {
        const transactionId = data.id;
        const customer = data.customer;

        try {
            const [tx] = await sql`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'brpix'`;

            if (tx && tx.status !== 'paid') {
                console.log(`[Webhook BRPix] Transação ${tx.id} encontrada. Atualizando para PAGO.`);
                await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document?.number });
            } else if (tx) {
                console.log(`[Webhook BRPix] Transação ${tx.id} já estava como 'paga'.`);
            } else {
                 console.warn(`[Webhook BRPix] AVISO: Transação com ID ${transactionId} não foi encontrada no banco de dados.`);
            }
        } catch (error) {
            console.error("Erro CRÍTICO no webhook da BRPix:", error);
        }
    }

    res.sendStatus(200);
});

// ==========================================================
//          ENDPOINTS ADICIONAIS DO ARQUIVO 1
// ==========================================================

// Endpoint 1: Configurações HotTrack
app.put('/api/settings/hottrack-key', authenticateJwt, async (req, res) => {
    const { apiKey } = req.body;
    if (typeof apiKey === 'undefined') return res.status(400).json({ message: 'O campo apiKey é obrigatório.' });
    try {
        await sqlWithRetry('UPDATE sellers SET api_key = $1 WHERE id = $2', [apiKey, req.user.id]);
        res.status(200).json({ message: 'Chave de API do HotTrack salva com sucesso!' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao salvar a chave.' });
    }
});

// Endpoint 2: Contagem de contatos
app.post('/api/bots/contacts-count', authenticateJwt, async (req, res) => {
    const { botIds } = req.body;
    const sellerId = req.user.id;

    if (!botIds || !Array.isArray(botIds) || botIds.length === 0) {
        return res.status(200).json({ count: 0 });
    }

    try {
        const result = await sqlWithRetry(
            `SELECT COUNT(DISTINCT chat_id) FROM telegram_chats WHERE seller_id = $1 AND bot_id = ANY($2::int[])`,
            [sellerId, botIds]
        );
        res.status(200).json({ count: parseInt(result[0].count, 10) });
    } catch (error) {
        console.error("Erro ao contar contatos:", error);
        res.status(500).json({ message: 'Erro interno ao contar contatos.' });
    }
});

// Endpoint 3: Validação de contatos
app.post('/api/bots/validate-contacts', authenticateJwt, async (req, res) => {
    const { botId } = req.body;
    const sellerId = req.user.id;

    if (!botId) {
        return res.status(400).json({ message: 'ID do bot é obrigatório.' });
    }

    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [botId, sellerId]);
        if (!bot || !bot.bot_token) {
            return res.status(404).json({ message: 'Bot não encontrado ou sem token configurado.' });
        }

        const contacts = await sqlWithRetry('SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username FROM telegram_chats WHERE bot_id = $1', [botId]);
        if (contacts.length === 0) {
            return res.status(200).json({ inactive_contacts: [], message: 'Nenhum contato para validar.' });
        }

        const inactiveContacts = [];
        const BATCH_SIZE = 30; 

        for (let i = 0; i < contacts.length; i += BATCH_SIZE) {
            const batch = contacts.slice(i, i + BATCH_SIZE);
            const promises = batch.map(contact => 
                sendTelegramRequest(bot.bot_token, 'sendChatAction', { chat_id: contact.chat_id, action: 'typing' })
                    .catch(error => {
                        if (error.response && (error.response.status === 403 || error.response.status === 400)) {
                            inactiveContacts.push(contact);
                        }
                    })
            );

            await Promise.all(promises);

            if (i + BATCH_SIZE < contacts.length) {
                await new Promise(resolve => setTimeout(resolve, 1100));
            }
        }

        res.status(200).json({ inactive_contacts: inactiveContacts });

    } catch (error) {
        console.error("Erro ao validar contatos:", error);
        res.status(500).json({ message: 'Erro interno ao validar contatos.' });
    }
});

// Endpoint 4: Remoção de contatos
app.post('/api/bots/remove-contacts', authenticateJwt, async (req, res) => {
    const { botId, chatIds } = req.body;
    const sellerId = req.user.id;

    if (!botId || !Array.isArray(chatIds) || chatIds.length === 0) {
        return res.status(400).json({ message: 'ID do bot e uma lista de chat_ids são obrigatórios.' });
    }

    try {
        const result = await sqlWithRetry('DELETE FROM telegram_chats WHERE bot_id = $1 AND seller_id = $2 AND chat_id = ANY($3::bigint[])', [botId, sellerId, chatIds]);
        res.status(200).json({ message: `${result.count} contatos inativos foram removidos com sucesso.` });
    } catch (error) {
        console.error("Erro ao remover contatos:", error);
        res.status(500).json({ message: 'Erro interno ao remover contatos.' });
    }
});

// Endpoint 5: Envio de mídia (base64)
app.post('/api/chats/:botId/send-media', authenticateJwt, async (req, res) => {
    const { chatId, fileData, fileType, fileName } = req.body;
    if (!chatId || !fileData || !fileType || !fileName) {
        return res.status(400).json({ message: 'Dados incompletos.' });
    }
    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [req.params.botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });
        const buffer = Buffer.from(fileData, 'base64');
        const formData = new FormData();
        formData.append('chat_id', chatId);
        let method, field;
        if (fileType.startsWith('image/')) {
            method = 'sendPhoto';
            field = 'photo';
        } else if (fileType.startsWith('video/')) {
            method = 'sendVideo';
            field = 'video';
        } else if (fileType.startsWith('audio/')) {
            method = 'sendVoice';
            field = 'voice';
        } else {
            return res.status(400).json({ message: 'Tipo de arquivo não suportado.' });
        }
        formData.append(field, buffer, { filename: fileName });
        const response = await sendTelegramRequest(bot.bot_token, method, formData, { headers: formData.getHeaders() });
        if (response.ok) {
            await saveMessageToDb(req.user.id, req.params.botId, response.result, 'operator');
        }
        res.status(200).json({ message: 'Mídia enviada!' });
    } catch (error) {
        res.status(500).json({ message: 'Não foi possível enviar a mídia.' });
    }
});

// Endpoint 6: Deletar conversa
app.delete('/api/chats/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        await sqlWithRetry('DELETE FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND seller_id = $3', [req.params.botId, req.params.chatId, req.user.id]);
        await sqlWithRetry('DELETE FROM user_flow_states WHERE bot_id = $1 AND chat_id = $2', [req.params.botId, req.params.chatId]);
        res.status(204).send();
    } catch (error) { res.status(500).json({ message: 'Erro ao deletar a conversa.' }); }
});

app.post('/api/chats/generate-pix', authenticateJwt, async (req, res) => {
    const { botId, chatId, click_id, valueInCents, pixMessage, pixButtonText } = req.body;
    try {
        if (!click_id) return res.status(400).json({ message: "Usuário não tem um Click ID para gerar PIX." });
        
        const [seller] = await sqlWithRetry('SELECT api_key FROM sellers WHERE id = $1', [req.user.id]);
        if (!seller || !seller.api_key) return res.status(400).json({ message: "API Key não configurada." });
        
        const baseApiUrl = process.env.HOTTRACK_API_URL;
        const pixResponse = await axios.post(`${baseApiUrl}/api/pix/generate`, { click_id, value_cents: valueInCents }, { headers: { 'x-api-key': seller.api_key } });
        const { transaction_id, qr_code_text } = pixResponse.data;

        await sqlWithRetry(`UPDATE telegram_chats SET last_transaction_id = $1 WHERE bot_id = $2 AND chat_id = $3`, [transaction_id, botId, chatId]);

        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1', [botId]);
        
        const messageText = pixMessage || '✅ PIX Gerado! Copie o código abaixo para pagar:';
        const buttonText = pixButtonText || '📋 Copiar Código PIX';
        const textToSend = `<pre>${qr_code_text}</pre>\n\n${messageText}`;

        const sentMessage = await sendTelegramRequest(bot.bot_token, 'sendMessage', {
            chat_id: chatId,
            text: textToSend,
            parse_mode: 'HTML',
            reply_markup: {
                inline_keyboard: [
                    [{ text: buttonText, copy_text: { text: qr_code_text } }]
                ]
            }
        });

        if (sentMessage.ok) {
            await saveMessageToDb(req.user.id, botId, sentMessage.result, 'operator');
        }
        res.status(200).json({ message: 'PIX enviado ao usuário.' });
    } catch (error) {
        res.status(500).json({ message: error.response?.data?.message || 'Erro ao gerar PIX.' });
    }
});

// Endpoint 8: Verificar status do PIX (modificado)
app.get('/api/chats/check-pix/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        const { botId, chatId } = req.params;
        // 1) Descobrir a última transação do chat
        const [chat] = await sqlWithRetry(
            'SELECT last_transaction_id FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND last_transaction_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
            [botId, chatId]
        );
        if (!chat || !chat.last_transaction_id) {
            return res.status(404).json({ message: 'Nenhuma transação PIX recente encontrada para este usuário.' });
        }

        // 2) Obter a API Key do seller logado
        const [seller] = await sqlWithRetry('SELECT api_key FROM sellers WHERE id = $1', [req.user.id]);
        if (!seller || !seller.api_key) {
            return res.status(400).json({ message: 'API Key não configurada.' });
        }

        // 3) Delegar para o endpoint central de status
        const baseApiUrl = process.env.HOTTRACK_API_URL;
        if (!baseApiUrl) {
            return res.status(500).json({ message: 'HOTTRACK_API_URL não configurada no servidor.' });
        }

        const response = await axios.get(`${baseApiUrl}/api/pix/status/${encodeURIComponent(chat.last_transaction_id)}`, {
            headers: { 'x-api-key': seller.api_key }
        });

        return res.status(200).json(response.data);
    } catch (error) {
        const status = error.response?.status || 500;
        const payload = error.response?.data || { message: 'Erro ao consultar PIX.' };
        return res.status(status).json(payload);
    }
});

// Endpoint 9: Iniciar fluxo manualmente
app.post('/api/chats/start-flow', authenticateJwt, async (req, res) => {
    const { botId, chatId, flowId } = req.body;
    const sellerId = req.user.id;

    try {
        const [bot] = await sql`SELECT bot_token FROM telegram_bots WHERE id = ${botId} AND seller_id = ${sellerId}`;
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado ou não pertence a você.' });

        const [flow] = await sql`SELECT nodes FROM flows WHERE id = ${flowId} AND bot_id = ${botId}`;
        if (!flow || !flow.nodes) return res.status(404).json({ message: 'Fluxo não encontrado ou não pertence a este bot.' });

        // --- LÓGICA DE LIMPEZA ---
        console.log(`[Manual Flow Start] Iniciando limpeza para o chat ${chatId} antes de iniciar o fluxo ${flowId}.`);
        const [existingState] = await sql`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        if (existingState && existingState.scheduled_message_id) {
            try {
                await qstashClient.messages.delete(existingState.scheduled_message_id);
                console.log(`[Manual Flow Start] Tarefa de timeout antiga cancelada.`);
            } catch (e) { /* Ignora erro se a tarefa já foi executada */ }
        }
        await sql`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        console.log(`[Manual Flow Start] Estado de fluxo antigo deletado.`);
        // --- FIM DA LÓGICA DE LIMPEZA ---

        // Encontra o ponto de partida do fluxo
        const startNode = flow.nodes.nodes?.find(node => node.type === 'trigger');
        const firstNodeId = findNextNode(startNode.id, null, flow.nodes.edges);

        if (!firstNodeId) {
            return res.status(400).json({ message: 'O fluxo selecionado não tem um nó inicial configurado após o gatilho.' });
        }

        // Busca o click_id mais recente para preservar o contexto
        const [chatContext] = await sql`SELECT click_id FROM telegram_chats WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL ORDER BY created_at DESC LIMIT 1`;
        let initialVars = {};
        if (chatContext?.click_id) {
            initialVars.click_id = chatContext.click_id;
        }

        // Inicia o fluxo para o usuário
        await processFlow(chatId, botId, bot.bot_token, sellerId, firstNodeId, initialVars);

        res.status(200).json({ message: 'Fluxo iniciado para o usuário com sucesso!' });
    } catch (error) {
        console.error("Erro ao iniciar fluxo manualmente:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint 10: Preview de mídia
app.get('/api/media/preview/:bot_id/:file_id', async (req, res) => {
    try {
        const { bot_id, file_id } = req.params;
        let token;
        if (bot_id === 'storage') {
            token = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
        } else {
            const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1', [bot_id]);
            token = bot?.bot_token;
        }
        if (!token) return res.status(404).send('Bot não encontrado.');
        const fileInfoResponse = await sendTelegramRequest(token, 'getFile', { file_id });
        if (!fileInfoResponse.ok || !fileInfoResponse.result?.file_path) {
            return res.status(404).send('Arquivo não encontrado no Telegram.');
        }
        const fileUrl = `https://api.telegram.org/file/bot${token}/${fileInfoResponse.result.file_path}`;
        const response = await axios.get(fileUrl, { responseType: 'stream' });
        res.setHeader('Content-Type', response.headers['content-type']);
        response.data.pipe(res);
    } catch (error) {
        console.error("Erro no preview:", error.message);
        res.status(500).send('Erro ao buscar o arquivo.');
    }
});

// Endpoint 11: Listar biblioteca de mídia
app.get('/api/media', authenticateJwt, async (req, res) => {
    try {
        const mediaFiles = await sqlWithRetry('SELECT id, file_name, file_id, file_type, thumbnail_file_id FROM media_library WHERE seller_id = $1 ORDER BY created_at DESC', [req.user.id]);
        res.status(200).json(mediaFiles);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar a biblioteca de mídia.' });
    }
});

// Endpoint 12: Upload para biblioteca de mídia
app.post('/api/media/upload', authenticateJwt, async (req, res) => {
    const { fileName, fileData, fileType } = req.body;
    if (!fileName || !fileData || !fileType) return res.status(400).json({ message: 'Dados do ficheiro incompletos.' });
    try {
        const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
        const storageChannelId = process.env.TELEGRAM_STORAGE_CHANNEL_ID;
        if (!storageBotToken || !storageChannelId) throw new Error('Credenciais do bot de armazenamento não configuradas.');
        const buffer = Buffer.from(fileData, 'base64');
        const formData = new FormData();
        formData.append('chat_id', storageChannelId);
        let telegramMethod = '', fieldName = '';
        if (fileType === 'image') {
            telegramMethod = 'sendPhoto';
            fieldName = 'photo';
        } else if (fileType === 'video') {
            telegramMethod = 'sendVideo';
            fieldName = 'video';
        } else if (fileType === 'audio') {
            telegramMethod = 'sendVoice';
            fieldName = 'voice';
        } else {
            return res.status(400).json({ message: 'Tipo de ficheiro não suportado.' });
        }
        formData.append(fieldName, buffer, { filename: fileName });
        const response = await sendTelegramRequest(storageBotToken, telegramMethod, formData, { headers: formData.getHeaders() });
        const result = response.result;
        let fileId, thumbnailFileId = null;
        if (fileType === 'image') {
            fileId = result.photo[result.photo.length - 1].file_id;
            thumbnailFileId = result.photo[0].file_id;
        } else if (fileType === 'video') {
            fileId = result.video.file_id;
            thumbnailFileId = result.video.thumbnail?.file_id || null;
        } else {
            fileId = result.voice.file_id;
        }
        if (!fileId) throw new Error('Não foi possível obter o file_id do Telegram.');
        const [newMedia] = await sqlWithRetry(`
            INSERT INTO media_library (seller_id, file_name, file_id, file_type, thumbnail_file_id)
            VALUES ($1, $2, $3, $4, $5) RETURNING id, file_name, file_id, file_type, thumbnail_file_id;
        `, [req.user.id, fileName, fileId, fileType, thumbnailFileId]);
        res.status(201).json(newMedia);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao processar o upload do ficheiro: ' + error.message });
    }
});

// Endpoint 13: Deletar mídia da biblioteca
app.delete('/api/media/:id', authenticateJwt, async (req, res) => {
    try {
        const result = await sqlWithRetry('DELETE FROM media_library WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        if (result.count > 0) res.status(204).send();
        else res.status(404).json({ message: 'Mídia não encontrada.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao excluir a mídia.' });
    }
});

// Endpoint 14: Compartilhar fluxo
app.post('/api/flows/:id/share', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const { name, description } = req.body;
    const sellerId = req.user.id;
    try {
        const [flow] = await sqlWithRetry('SELECT * FROM flows WHERE id = $1 AND seller_id = $2', [id, sellerId]);
        if (!flow) return res.status(404).json({ message: 'Fluxo não encontrado.' });

        const [seller] = await sqlWithRetry('SELECT name FROM sellers WHERE id = $1', [sellerId]);
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        await sqlWithRetry(`
            INSERT INTO shared_flows (name, description, original_flow_id, seller_id, seller_name, nodes)
            VALUES ($1, $2, $3, $4, $5, $6)`,
            [name, description, id, sellerId, seller.name, flow.nodes]
        );
        await sqlWithRetry('UPDATE flows SET is_shared = TRUE WHERE id = $1', [id]);
        res.status(201).json({ message: 'Fluxo compartilhado com sucesso!' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao compartilhar fluxo: ' + error.message });
    }
});

// Endpoint 15: Listar fluxos compartilhados
app.get('/api/shared-flows', authenticateJwt, async (req, res) => {
    try {
        const sharedFlows = await sqlWithRetry('SELECT id, name, description, seller_name, import_count, created_at FROM shared_flows ORDER BY import_count DESC, created_at DESC');
        res.status(200).json(sharedFlows);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar fluxos da comunidade.' });
    }
});

// Endpoint 16: Importar fluxo compartilhado
app.post('/api/shared-flows/:id/import', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const { botId } = req.body;
    const sellerId = req.user.id;
    try {
        if (!botId) return res.status(400).json({ message: 'É necessário selecionar um bot para importar.' });
        
        const [sharedFlow] = await sqlWithRetry('SELECT * FROM shared_flows WHERE id = $1', [id]);
        if (!sharedFlow) return res.status(404).json({ message: 'Fluxo compartilhado não encontrado.' });

        const newFlowName = `${sharedFlow.name} (Importado)`;
        const [newFlow] = await sqlWithRetry(
            `INSERT INTO flows (seller_id, bot_id, name, nodes) VALUES ($1, $2, $3, $4) RETURNING *`,
            [sellerId, botId, newFlowName, sharedFlow.nodes]
        );
        
        await sqlWithRetry('UPDATE shared_flows SET import_count = import_count + 1 WHERE id = $1', [id]);
        
        res.status(201).json(newFlow);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao importar fluxo: ' + error.message });
    }
});

// Endpoint 17: Gerar link de compartilhamento
app.post('/api/flows/:id/generate-share-link', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    const { priceInCents, allowReshare, bundleLinkedFlows, bundleMedia } = req.body;

    try {
        const shareId = uuidv4();
        const [updatedFlow] = await sqlWithRetry(
            `UPDATE flows SET 
                shareable_link_id = $1,
                share_price_cents = $2,
                share_allow_reshare = $3,
                share_bundle_linked_flows = $4,
                share_bundle_media = $5
             WHERE id = $6 AND seller_id = $7 RETURNING shareable_link_id`,
            [shareId, priceInCents || 0, !!allowReshare, !!bundleLinkedFlows, !!bundleMedia, id, sellerId]
        );
        if (!updatedFlow) return res.status(404).json({ message: 'Fluxo não encontrado.' });
        res.status(200).json({ shareable_link_id: updatedFlow.shareable_link_id });
    } catch (error) {
        console.error("Erro ao gerar link de compartilhamento:", error);
        res.status(500).json({ message: 'Erro ao gerar link de compartilhamento.' });
    }
});

// Endpoint 18: Detalhes do link compartilhado
app.get('/api/share/details/:shareId', async (req, res) => {
    try {
        const { shareId } = req.params;
        const [flow] = await sqlWithRetry(`
            SELECT name, share_price_cents, share_allow_reshare, share_bundle_linked_flows, share_bundle_media
            FROM flows WHERE shareable_link_id = $1
        `, [shareId]);

        if (!flow) {
            return res.status(404).json({ message: 'Link de compartilhamento inválido ou não encontrado.' });
        }

        res.status(200).json(flow);
    } catch (error) {
        console.error("Erro ao buscar detalhes do compartilhamento:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint 19: Importar fluxo por link
app.post('/api/flows/import-from-link', authenticateJwt, async (req, res) => {
    const { shareableLinkId, botId } = req.body;
    const sellerId = req.user.id;
    try {
        if (!botId || !shareableLinkId) return res.status(400).json({ message: 'ID do link e ID do bot são obrigatórios.' });
        
        const [originalFlow] = await sqlWithRetry('SELECT name, nodes, share_price_cents FROM flows WHERE shareable_link_id = $1', [shareableLinkId]);
        if (!originalFlow) return res.status(404).json({ message: 'Link de compartilhamento inválido ou expirado.' });

        if (originalFlow.share_price_cents > 0) {
            return res.status(400).json({ message: 'Este fluxo é pago e não pode ser importado por esta via.' });
        }

        const newFlowName = `${originalFlow.name} (Importado)`;
        const [newFlow] = await sqlWithRetry(
            `INSERT INTO flows (seller_id, bot_id, name, nodes) VALUES ($1, $2, $3, $4) RETURNING *`,
            [sellerId, botId, newFlowName, originalFlow.nodes]
        );
        res.status(201).json(newFlow);
    } catch (error) {
        console.error("Erro ao importar fluxo por link:", error);
        res.status(500).json({ message: 'Erro ao importar fluxo por link: ' + error.message });
    }
});

// Endpoint 22: Histórico de disparos
app.get('/api/disparos/history', authenticateJwt, async (req, res) => {
    try {
        const history = await sqlWithRetry(`
            SELECT 
                h.*,
                (SELECT COUNT(*) FROM disparo_log WHERE history_id = h.id AND status = 'CONVERTED') as conversions
            FROM 
                disparo_history h
            WHERE 
                h.seller_id = $1
            ORDER BY 
                h.created_at DESC;
        `, [req.user.id]);
        res.status(200).json(history);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar histórico de disparos.' });
    }
});

// Endpoint 23: Verificar conversões de disparos (modificado)
app.post('/api/disparos/check-conversions/:historyId', authenticateJwt, async (req, res) => {
    const { historyId } = req.params;
    const sellerId = req.user.id;

    try {
        const [seller] = await sqlWithRetry('SELECT * FROM sellers WHERE id = $1', [sellerId]);
        if (!seller) {
            return res.status(400).json({ message: "Vendedor não encontrado." });
        }

        const logs = await sqlWithRetry(
            `SELECT id, transaction_id FROM disparo_log WHERE history_id = $1 AND status != 'CONVERTED' AND transaction_id IS NOT NULL`,
            [historyId]
        );
        
        let updatedCount = 0;
        for (const log of logs) {
            try {
                // Buscar a transação no banco
                const [transaction] = await sqlWithRetry('SELECT * FROM pix_transactions WHERE provider_transaction_id = $1 OR pix_id = $1', [log.transaction_id]);
                if (!transaction) {
                    continue;
                }

                if (transaction.status === 'paid') {
                    await sqlWithRetry(`UPDATE disparo_log SET status = 'CONVERTED' WHERE id = $1`, [log.id]);
                    updatedCount++;
                    continue;
                }

                // Se não está paga, verificar no provedor
                let providerStatus, customerData = {};
                try {
                    if (transaction.provider === 'syncpay') {
                        const syncPayToken = await getSyncPayAuthToken(seller);
                        const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                            headers: { 'Authorization': `Bearer ${syncPayToken}` }
                        });
                        providerStatus = response.data.status;
                        customerData = response.data.payer;
                    } else if (transaction.provider === 'pushinpay') {
                        const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                        providerStatus = response.data.status;
                        customerData = { name: response.data.payer_name, document: response.data.payer_document };
                    } else if (transaction.provider === 'oasyfy' || transaction.provider === 'cnpay' || transaction.provider === 'brpix') {
                        // Não consultamos, depende do webhook
                        continue;
                    }
                } catch (providerError) {
                    console.error(`Falha ao consultar o provedor para a transação ${transaction.id}:`, providerError.message);
                    continue;
                }

                if (providerStatus === 'paid' || providerStatus === 'COMPLETED') {
                    await handleSuccessfulPayment(transaction.id, customerData);
                    await sqlWithRetry(`UPDATE disparo_log SET status = 'CONVERTED' WHERE id = $1`, [log.id]);
                    updatedCount++;
                }
            } catch(e) {
                // Ignora erros de PIX não encontrado, etc.
            }
            await new Promise(resolve => setTimeout(resolve, 200)); // Rate limiting
        }

        res.status(200).json({ message: `Verificação concluída. ${updatedCount} novas conversões encontradas.` });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao verificar conversões.' });
    }
});

// Endpoint 24: CRON para processar fila de disparos (modificado)
// [REMOVIDO] Rota de CRON de disparos (não utilizada)

// Health check endpoint para Render
app.get('/api/health', (req, res) => {
    res.status(200).json({ 
        status: 'OK', 
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        port: PORT
    });
});


// ==========================================================
// ROTAS CHECKOUTS HOSPEDADOS E PÁGINAS DE OBRIGADO
// ==========================================================

// LISTAR CHECKOUTS
app.get('/api/checkouts', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        // Select the ID, extract the main title from the config JSON, and the creation date
        const checkouts = await sql`
            SELECT
                id,
                config->'content'->>'main_title' as name, -- Extracts 'main_title' from the 'content' object within 'config'
                created_at
            FROM hosted_checkouts
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(checkouts);
    } catch (error) {
        console.error("Erro ao listar checkouts hospedados:", error);
        res.status(500).json({ message: 'Erro ao buscar seus checkouts.' });
    }
});

// EDITAR (ATUALIZAR) CHECKOUT
app.put('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;
    const newConfig = req.body; // Expects the full updated config object from the frontend

    // Basic validation
    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }

    try {
        // Update the config JSON and updated_at timestamp for the specific checkout ID and seller ID
        const result = await sql`
            UPDATE hosted_checkouts
            SET config = ${JSON.stringify(newConfig)}, updated_at = NOW()
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Return the ID to confirm update occurred
        `;

        // Check if any row was updated
        if (result.length === 0) {
            return res.status(404).json({ message: 'Checkout não encontrado ou você não tem permissão para editá-lo.' });
        }

        res.status(200).json({ message: 'Checkout atualizado com sucesso!', checkoutId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar checkout ${checkoutId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar o checkout.' });
    }
});


// ROTA CRIAÇÃO CHECKOUT HOSPEDADO
app.post('/api/checkouts/create-hosted', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const config = req.body; // Expects the full config object from the frontend

    // Generate a unique ID for the new checkout, prefixed for easy identification
    const checkoutId = `cko_${uuidv4()}`;

    try {
        // Insert into the hosted_checkouts table
        await sql`
            INSERT INTO hosted_checkouts (id, seller_id, config)
            VALUES (${checkoutId}, ${sellerId}, ${JSON.stringify(config)});
        `;

        // Return the generated ID to the frontend
        res.status(201).json({
            message: 'Checkout hospedado criado com sucesso!',
            checkoutId: checkoutId
        });

    } catch (error) {
        console.error("Erro ao criar checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno ao criar o checkout.' });
    }
});

// ROTA PÁGINA DE OFERTA
app.get('/api/oferta/:checkoutId', async (req, res) => {
    const { checkoutId } = req.params;
    const { click_id, cid } = req.query; // Captura IDs de clique da URL

    try {
        // 1. Busca a configuração do checkout e o ID do vendedor
        const [checkout] = await sql`
            SELECT seller_id, config FROM hosted_checkouts WHERE id = ${checkoutId}
        `;

        if (!checkout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        let finalClickId = click_id || cid || null;

        // 2. Lógica para tráfego orgânico (nenhum ID de clique na URL)
        if (!finalClickId) {
            console.log(`[Organic Traffic] Nenhum click_id encontrado para checkout ${checkoutId}. Gerando um novo.`);
            
            const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
            const user_agent = req.headers['user-agent'];

            // Insere um novo clique para a visita orgânica
            const [newClick] = await sql`
                INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent, is_organic)
                VALUES (${checkout.seller_id}, ${checkoutId}, ${ip_address}, ${user_agent}, TRUE)
                RETURNING id;
            `;

            // Gera um click_id único e amigável
            finalClickId = `org_${newClick.id.toString().padStart(7, '0')}`;

            // Atualiza o registro com o novo click_id gerado
            await sql`
                UPDATE clicks SET click_id = ${finalClickId} WHERE id = ${newClick.id}
            `;
             console.log(`[Organic Traffic] Novo click_id gerado e associado: ${finalClickId}`);
        }

        // 3. Retorna a configuração e o click_id final para o frontend
        res.status(200).json({
            config: checkout.config,
            click_id: finalClickId // Envia o ID existente ou o novo ID orgânico
        });

    } catch (error) {
        console.error("Erro ao buscar dados do checkout ou processar tráfego orgânico:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});

app.post('/api/oferta/generate-pix', async (req, res) => {
    const { checkoutId, value_cents, click_id } = req.body;

    if (!checkoutId || !value_cents) {
        return res.status(400).json({ message: 'Dados insuficientes para gerar o PIX.' });
    }

    try {
        // 1) Validar checkout e obter seller
        const [hostedCheckout] = await sql`SELECT seller_id FROM hosted_checkouts WHERE id = ${checkoutId}`;
        if (!hostedCheckout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        const sellerId = hostedCheckout.seller_id;
        const [seller] = await sql`SELECT api_key FROM sellers WHERE id = ${sellerId}`;
        if (!seller || !seller.api_key) {
            return res.status(400).json({ message: 'API Key não configurada para o vendedor.' });
        }

        // 2) Garantir que há um click_id associado (reutilizando lógica atual)
        const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
        const user_agent = req.headers['user-agent'];
        let finalClickId = click_id;

        if (!finalClickId) {
            // Criar clique orgânico e gerar click_id no padrão existente
            const [newClick] = await sql`
                INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent)
                VALUES (${sellerId}, ${checkoutId}, ${ip_address}, ${user_agent})
                RETURNING id;
            `;
            finalClickId = `lead${newClick.id.toString().padStart(6, '0')}`;
            await sql`UPDATE clicks SET click_id = ${`/start ${finalClickId}`} WHERE id = ${newClick.id}`;
        }

        // 3) Delegar geração para o endpoint central usando HOTTRACK_API_URL
        const baseApiUrl = process.env.HOTTRACK_API_URL;
        if (!baseApiUrl) {
            return res.status(500).json({ message: 'HOTTRACK_API_URL não configurada no servidor.' });
        }

        const response = await axios.post(`${baseApiUrl}/api/pix/generate`, {
            click_id: finalClickId,
            value_cents: value_cents
        }, {
            headers: { 'x-api-key': seller.api_key }
        });

        // 4) Retornar a resposta da API central
        return res.status(200).json(response.data);
    } catch (error) {
        const status = error.response?.status || 500;
        const message = error.response?.data?.message || error.message || 'Não foi possível gerar o PIX no momento.';
        return res.status(status).json({ message });
    }
});

// ==========================================================
// ROTAS PÁGINAS DE OBRIGADO
// ==========================================================
// ROTA CRIAÇÃO CHECKOUT HOSPEDADO (create-hosted)
app.post('/api/checkouts/create-hosted', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects the full config object from the frontend

    // Generate a unique ID for the new checkout, prefixed for easy identification
    const checkoutId = `cko_${uuidv4()}`;

    try {
        // Insert into the hosted_checkouts table
        await sql`
            INSERT INTO hosted_checkouts (id, seller_id, config)
            VALUES (${checkoutId}, ${sellerId}, ${JSON.stringify(config)});
        `;

        // Return the generated ID to the frontend
        res.status(201).json({
            message: 'Checkout hospedado criado com sucesso!',
            checkoutId: checkoutId
        });

    } catch (error) {
        console.error("Erro ao criar checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno ao criar o checkout.' });
    }
});

// ATUALIZAR UMA PÁGINA DE OBRIGADO
app.put('/api/thank-you-pages/:pageId', authenticateApiKey, async (req, res) => { // Usando ApiKey para consistência com create
    const { pageId } = req.params;
    const sellerId = req.sellerId;
    const newConfig = req.body; // Espera o objeto config atualizado

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }
     // Valida campos essenciais no newConfig
    if (!newConfig.page_name || !newConfig.purchase_value || !newConfig.pixel_id || !newConfig.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para atualizar a página.' });
    }

    try {
        const result = await sql`
            UPDATE thank_you_pages
            SET config = ${JSON.stringify(newConfig)}, updated_at = NOW()
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
            return res.status(404).json({ message: 'Página não encontrada ou você não tem permissão para editá-la.' });
        }
        res.status(200).json({ message: 'Página de obrigado atualizada com sucesso!', pageId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar a página.' });
    }
});

app.post('/api/thank-you-pages/create', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects config object { page_name, purchase_value, pixel_id, redirect_url, utmify_integration_id? }

    // Validate essential fields
    if (!config.page_name || !config.purchase_value || !config.pixel_id || !config.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para criar a página.' });
    }

    // Generate unique ID for the page
    const pageId = `ty_${uuidv4()}`;

    try {
        // Insert the configuration into the database
        await sql`
            INSERT INTO thank_you_pages (id, seller_id, config)
            VALUES (${pageId}, ${sellerId}, ${JSON.stringify(config)});
        `;

        res.status(201).json({
            message: 'Página de obrigado criada com sucesso!',
            pageId: pageId // Return the generated ID
        });

    } catch (error) {
        console.error("Erro ao criar página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno ao criar a página.' });
    }
});

// Fetch configuration for a specific Thank You Page
app.get('/api/obrigado/:pageId', async (req, res) => {
    const { pageId } = req.params;

    try {
        const [page] = await sql`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada.' });
        }

        res.status(200).json({
            config: page.config,
        });

    } catch (error) {
        console.error("Erro ao buscar dados da página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});

// Trigger Utmify event from the Thank You Page frontend
app.post('/api/thank-you-pages/fire-utmify', async (req, res) => {
    const { pageId, trackingParameters, customerData } = req.body;

    try {
        const [page] = await sql`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        if (!page || !page.config.utmify_integration_id) {
            return res.status(404).json({ message: 'Página ou integração Utmify não configurada.' });
        }

        const sellerId = page.seller_id;
        const utmifyIntegrationId = page.config.utmify_integration_id;

        const [seller] = await sql`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) {
            return res.status(404).json({ message: 'Vendedor não encontrado.' });
        }

        const utmifyData = {
            integration_id: utmifyIntegrationId,
            event_name: 'Purchase',
            customer_data: {
                email: customerData.email || '',
                phone: customerData.phone || '',
                name: customerData.name || '',
                ...trackingParameters
            },
            purchase_data: {
                value: page.config.purchase_value,
                currency: 'BRL'
            }
        };

        const utmifyResponse = await fetch('https://api.utmify.com.br/v1/events', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${seller.utmify_token}`
            },
            body: JSON.stringify(utmifyData)
        });

        if (!utmifyResponse.ok) {
            throw new Error(`Utmify API error: ${utmifyResponse.status}`);
        }

        res.status(200).json({ message: 'Evento enviado para Utmify com sucesso!' });

    } catch (error) {
        console.error(`[Utmify TY Page Error]`, error.response?.data || error.message);
        res.status(500).json({ message: 'Erro ao enviar evento para Utmify.' });
    }
});

// LISTAR PÁGINAS DE OBRIGADO DO VENDEDOR
app.get('/api/thank-you-pages', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const pages = await sql`
            SELECT
                id,
                config->>'page_name' as name,
                created_at
            FROM thank_you_pages
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(pages);
    } catch (error) {
        console.error("Erro ao listar páginas de obrigado:", error);
        res.status(500).json({ message: 'Erro ao buscar suas páginas de obrigado.' });
    }
});

// BUSCAR UMA PÁGINA DE OBRIGADO ESPECÍFICA
app.get('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;
    try {
        const [page] = await sql`
            SELECT id, config FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId};
        `;
        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada ou não pertence a você.' });
        }
        res.status(200).json(page);
    } catch (error) {
        console.error(`Erro ao buscar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao buscar a página.' });
    }
});

// ATUALIZAR UMA PÁGINA DE OBRIGADO
app.put('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;
    const newConfig = req.body;

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }

    if (!newConfig.page_name || !newConfig.purchase_value || !newConfig.pixel_id || !newConfig.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para atualizar a página.' });
    }

    try {
        const result = await sql`
            UPDATE thank_you_pages
            SET config = ${JSON.stringify(newConfig)}, updated_at = NOW()
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
            return res.status(404).json({ message: 'Página não encontrada ou você não tem permissão para editá-la.' });
        }
        res.status(200).json({ message: 'Página de obrigado atualizada com sucesso!', pageId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar a página.' });
    }
});

// DELETAR UMA PÁGINA DE OBRIGADO
app.delete('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }

    try {
        const result = await sql`
            DELETE FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
             console.warn(`Tentativa de excluir página TY não encontrada ou não pertencente ao seller: ${pageId}, Seller: ${sellerId}`);
        }
        res.status(200).json({ message: 'Página de obrigado excluída com sucesso!' });
    } catch (error) {
        console.error(`Erro ao excluir página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao excluir a página.' });
    }
});

// ==========================================================
//          SERVIÇO DE ARQUIVOS ESTÁTICOS (FRONTEND & ADMIN)
// ==========================================================


const isProduction = process.env.NODE_ENV === 'production';

// Define os caminhos baseados no ambiente
const frontendPath = isProduction ? 'frontend' : '../frontend';
const adminFrontendPath = isProduction ? 'admin-frontend' : '../admin-frontend';

// Rota específica para verificação de email - DEVE VIR ANTES DO EXPRESS.STATIC
app.get('/verify-email', (req, res) => {
    console.log('Servindo página de verificação de email');
    res.sendFile(path.join(__dirname, frontendPath, 'verify-email.html'));
});

// Servir frontend estático
app.use(express.static(path.join(__dirname, frontendPath)));

// Servir admin estático em /admin
app.use('/admin', express.static(path.join(__dirname, adminFrontendPath)));

// Rota catch-all para SPA - DEVE SER A ÚLTIMA ROTA
app.get('*', (req, res) => {
  if (req.path.startsWith('/api')) {
    // API routes - não servir arquivos estáticos
    return res.status(404).json({ error: 'API route not found' });
  } else if (req.path.startsWith('/admin')) {
    // Admin routes
    return res.sendFile(path.join(__dirname, adminFrontendPath, 'index.html'));
  } else {
    // Frontend routes
    return res.sendFile(path.join(__dirname, frontendPath, 'index.html'));
  }
});

// Inicialização do servidor
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Servidor HotTrack rodando na porta ${PORT}`);
    console.log(`📱 API disponível em: http://localhost:${PORT}/api`);
    console.log(`🏥 Health check: http://localhost:${PORT}/api/health`);
    console.log(`🌍 Ambiente: ${process.env.NODE_ENV || 'development'}`);
});

module.exports = app;
