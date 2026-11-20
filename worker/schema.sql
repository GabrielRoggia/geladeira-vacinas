-- Banco D1: historico da geladeira de vacinas
-- wrangler d1 execute geladeira-db --remote --file=schema.sql

CREATE TABLE IF NOT EXISTS leituras (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  disp         TEXT    NOT NULL,             -- ex.: gel01
  seq          INTEGER NOT NULL,             -- contador do ESP32 (buraco = leitura perdida)
  ts           INTEGER NOT NULL,             -- epoch UTC da MEDICAO (nao do recebimento)
  temp_c       REAL,                         -- NULL = sensor em falha
  umid_pct     REAL,
  porta        TEXT    NOT NULL,             -- fechada | aberta | desconhecida
  alarme       TEXT    NOT NULL,             -- normal | temp_alta | temp_baixa | sensor_falha
  mudo         INTEGER NOT NULL DEFAULT 0,
  origem       INTEGER NOT NULL DEFAULT 0,   -- 0 tempo real | 1 veio da fila offline | 2 hora estimada (sem NTP)
  recebido_em  INTEGER NOT NULL,
  UNIQUE (disp, seq, ts)                     -- reenvio da fila nao duplica
);
CREATE INDEX IF NOT EXISTS idx_leituras_disp_ts ON leituras (disp, ts);

CREATE TABLE IF NOT EXISTS alertas (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  disp        TEXT    NOT NULL,
  ts          INTEGER NOT NULL,
  tipo        TEXT    NOT NULL,              -- temp_alta, temp_baixa, sensor_falha, porta_aberta, offline...
  valor       REAL,
  msg         TEXT,
  notificado  INTEGER NOT NULL DEFAULT 0,    -- 1 = saiu no Telegram; 0 = segurado pelo cooldown
  recebido_em INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alertas_disp_ts ON alertas (disp, ts);
