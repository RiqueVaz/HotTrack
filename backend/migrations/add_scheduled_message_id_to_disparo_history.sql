-- Migration: Adicionar campo scheduled_message_id em disparo_history
-- Data: 2025-11-23
-- Descrição: Campo para armazenar o ID do job BullMQ para disparos agendados

ALTER TABLE disparo_history 
ADD COLUMN IF NOT EXISTS scheduled_message_id TEXT;

-- Criar índice para melhorar performance de buscas por scheduled_message_id
CREATE INDEX IF NOT EXISTS idx_disparo_history_scheduled_message_id 
ON disparo_history(scheduled_message_id) 
WHERE scheduled_message_id IS NOT NULL;

