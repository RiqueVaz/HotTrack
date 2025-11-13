-- Adicionar campo pixup_api_token na tabela sellers
-- Este campo armazena o token Bearer da API Pixup para autenticação

ALTER TABLE public.sellers 
ADD COLUMN IF NOT EXISTS pixup_api_token text;

-- Comentário explicativo
COMMENT ON COLUMN public.sellers.pixup_api_token IS 'Token Bearer da API Pixup para geração de PIX e recebimento de webhooks';


