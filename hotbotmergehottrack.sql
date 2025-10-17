-- =====================================================================
-- SCRIPT DE ATUALIZAÇÃO MESTRE
-- Pode ser executado com segurança, mesmo em um banco de dados
-- parcialmente atualizado.
-- =====================================================================

-- =====================================================================
-- PARTE 1: ADICIONANDO TABELAS FALTANTES (COM VERIFICAÇÃO "IF NOT EXISTS")
-- =====================================================================

-- Tabela para Histórico de Disparos
CREATE TABLE IF NOT EXISTS public.disparo_history (
    id integer NOT NULL,
    seller_id integer,
    campaign_name character varying(255) NOT NULL,
    bot_ids jsonb,
    flow_steps jsonb,
    status character varying(50) DEFAULT 'PENDING'::character varying,
    total_sent integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para Log de Disparos
CREATE TABLE IF NOT EXISTS public.disparo_log (
    id integer NOT NULL,
    history_id integer,
    chat_id bigint NOT NULL,
    bot_id integer NOT NULL,
    status character varying(50) DEFAULT 'SENT'::character varying,
    transaction_id character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para Fila de Disparos
CREATE TABLE IF NOT EXISTS public.disparo_queue (
    id integer NOT NULL,
    history_id integer NOT NULL,
    chat_id bigint NOT NULL,
    bot_id integer NOT NULL,
    step_json text NOT NULL,
    variables_json text,
    created_at timestamp with time zone DEFAULT now()
);

-- Tabela para Biblioteca de Mídia
CREATE TABLE IF NOT EXISTS public.media_library (
    id integer NOT NULL,
    seller_id integer,
    file_name text NOT NULL,
    file_id text NOT NULL,
    file_type character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    thumbnail_file_id text
);

-- Tabela para Fluxos Compartilhados
CREATE TABLE IF NOT EXISTS public.shared_flows (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    original_flow_id integer NOT NULL,
    seller_id integer NOT NULL,
    seller_name character varying(255),
    nodes jsonb NOT NULL,
    import_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);

-- Bloco para criar sequências, chaves primárias e estrangeiras somente se não existirem
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'disparo_history_pkey') THEN
        CREATE SEQUENCE public.disparo_history_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
        ALTER SEQUENCE public.disparo_history_id_seq OWNED BY public.disparo_history.id;
        ALTER TABLE ONLY public.disparo_history ALTER COLUMN id SET DEFAULT nextval('public.disparo_history_id_seq'::regclass);
        ALTER TABLE ONLY public.disparo_history ADD CONSTRAINT disparo_history_pkey PRIMARY KEY (id);
        ALTER TABLE ONLY public.disparo_history ADD CONSTRAINT disparo_history_seller_id_fkey FOREIGN KEY (seller_id) REFERENCES public.sellers(id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'disparo_log_pkey') THEN
        CREATE SEQUENCE public.disparo_log_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
        ALTER SEQUENCE public.disparo_log_id_seq OWNED BY public.disparo_log.id;
        ALTER TABLE ONLY public.disparo_log ALTER COLUMN id SET DEFAULT nextval('public.disparo_log_id_seq'::regclass);
        ALTER TABLE ONLY public.disparo_log ADD CONSTRAINT disparo_log_pkey PRIMARY KEY (id);
        ALTER TABLE ONLY public.disparo_log ADD CONSTRAINT disparo_log_history_id_fkey FOREIGN KEY (history_id) REFERENCES public.disparo_history(id) ON DELETE CASCADE;
        CREATE INDEX IF NOT EXISTS idx_disparo_log_history_id ON public.disparo_log USING btree (history_id);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'disparo_queue_pkey') THEN
        CREATE SEQUENCE public.disparo_queue_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
        ALTER SEQUENCE public.disparo_queue_id_seq OWNED BY public.disparo_queue.id;
        ALTER TABLE ONLY public.disparo_queue ALTER COLUMN id SET DEFAULT nextval('public.disparo_queue_id_seq'::regclass);
        ALTER TABLE ONLY public.disparo_queue ADD CONSTRAINT disparo_queue_pkey PRIMARY KEY (id);
        ALTER TABLE ONLY public.disparo_queue ADD CONSTRAINT disparo_queue_history_id_fkey FOREIGN KEY (history_id) REFERENCES public.disparo_history(id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'media_library_pkey') THEN
        CREATE SEQUENCE public.media_library_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
        ALTER SEQUENCE public.media_library_id_seq OWNED BY public.media_library.id;
        ALTER TABLE ONLY public.media_library ALTER COLUMN id SET DEFAULT nextval('public.media_library_id_seq'::regclass);
        ALTER TABLE ONLY public.media_library ADD CONSTRAINT media_library_pkey PRIMARY KEY (id);
        ALTER TABLE ONLY public.media_library ADD CONSTRAINT media_library_seller_id_fkey FOREIGN KEY (seller_id) REFERENCES public.sellers(id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shared_flows_pkey') THEN
        CREATE SEQUENCE public.shared_flows_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
        ALTER SEQUENCE public.shared_flows_id_seq OWNED BY public.shared_flows.id;
        ALTER TABLE ONLY public.shared_flows ALTER COLUMN id SET DEFAULT nextval('public.shared_flows_id_seq'::regclass);
        ALTER TABLE ONLY public.shared_flows ADD CONSTRAINT shared_flows_pkey PRIMARY KEY (id);
        ALTER TABLE ONLY public.shared_flows ADD CONSTRAINT shared_flows_original_flow_id_fkey FOREIGN KEY (original_flow_id) REFERENCES public.flows(id) ON DELETE CASCADE;
        ALTER TABLE ONLY public.shared_flows ADD CONSTRAINT shared_flows_seller_id_fkey FOREIGN KEY (seller_id) REFERENCES public.sellers(id);
    END IF;
END $$;


-- =====================================================================
-- PARTE 2: ADICIONANDO COLUNAS FALTANTES (COM VERIFICAÇÃO "IF NOT EXISTS")
-- =====================================================================

ALTER TABLE public.flows
    ADD COLUMN IF NOT EXISTS is_shared boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS shareable_link_id text,
    ADD COLUMN IF NOT EXISTS share_price_cents integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS share_allow_reshare boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS share_bundle_linked_flows boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS share_bundle_media boolean DEFAULT false;

ALTER TABLE public.sellers
    ADD COLUMN IF NOT EXISTS hottrack_api_key text;

ALTER TABLE public.telegram_chats
    ADD COLUMN IF NOT EXISTS media_type character varying(20),
    ADD COLUMN IF NOT EXISTS media_file_id text,
    ADD COLUMN IF NOT EXISTS last_transaction_id text;


-- =====================================================================
-- PARTE 3: APLICANDO MODIFICAÇÕES CRÍTICAS (DE FORMA SEGURA)
-- =====================================================================

-- Ajustando a coluna `waiting_for_input` para o tipo booleano
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='user_flow_states' AND column_name='waiting_for_input' AND data_type='character varying') THEN
        ALTER TABLE public.user_flow_states
            ALTER COLUMN waiting_for_input TYPE boolean
            USING (waiting_for_input IS NOT NULL AND waiting_for_input != 'false');
        ALTER TABLE public.user_flow_states
            ALTER COLUMN waiting_for_input SET DEFAULT false;
    END IF;
END $$;

-- **BLOCO UNIFICADO E SEGURO PARA CORRIGIR A CHAVE PRIMÁRIA DE 'telegram_chats'**
BEGIN;
    -- Passo 1: Remove a dependência da tabela 'telegram_messages'
    ALTER TABLE public.telegram_messages 
    DROP CONSTRAINT IF EXISTS telegram_messages_chat_record_id_fkey;
    
    -- Passo 2: Remove a chave primária e a constraint de unicidade antigas
    ALTER TABLE public.telegram_chats DROP CONSTRAINT IF EXISTS telegram_chats_pkey;
    ALTER TABLE public.telegram_chats DROP CONSTRAINT IF EXISTS telegram_chats_chat_id_message_id_key;

    -- Passo 3: Adiciona a nova chave primária composta
    ALTER TABLE public.telegram_chats ADD PRIMARY KEY (chat_id, message_id);
COMMIT;


-- =====================================================================
-- FIM DO SCRIPT DE ATUALIZAÇÃO
-- =====================================================================