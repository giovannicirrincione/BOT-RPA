-- Tabla de historial de archivos procesados
CREATE TABLE IF NOT EXISTS upload_history (
    id          BIGSERIAL PRIMARY KEY,
    filename    TEXT        NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_rows  INTEGER     NOT NULL,
    success_rows INTEGER    NOT NULL DEFAULT 0,
    error_rows  INTEGER     NOT NULL DEFAULT 0,
    status      TEXT        NOT NULL CHECK (status IN ('Procesando', 'Éxito', 'Error')),
    error_detail TEXT
);

-- Tabla de detalle fila por fila (opcional, para trazabilidad)
CREATE TABLE IF NOT EXISTS upload_rows (
    id          BIGSERIAL PRIMARY KEY,
    history_id  BIGINT      NOT NULL REFERENCES upload_history(id) ON DELETE CASCADE,
    row_index   INTEGER     NOT NULL,
    row_data    JSONB,
    status      TEXT        NOT NULL CHECK (status IN ('Éxito', 'Error')),
    error_msg   TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
