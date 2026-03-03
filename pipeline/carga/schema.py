def init_all_tables(conn):
    ddl = """
    -- PRODUCTIVA
    CREATE TABLE IF NOT EXISTS consolidado (
      id_registro       TEXT PRIMARY KEY,
      fuente            TEXT NOT NULL,
      tipo_sujeto       TEXT,
      nombres           TEXT,
      apellidos         TEXT,
      aliases           TEXT,      -- JSON array en TEXT
      fecha_nacimiento  TEXT,
      nacionalidad      TEXT,      -- JSON array en TEXT
      numero_documento  TEXT,
      tipo_sancion      TEXT,
      fecha_sancion     TEXT,
      fecha_vencimiento TEXT,
      activo            INTEGER NOT NULL,
      fecha_ingesta     TEXT NOT NULL,
      origen_id         TEXT,
      hash_contenido    TEXT NOT NULL,

      -- control de cambios
      hash_anterior     TEXT,
      first_seen_at     TEXT NOT NULL DEFAULT (datetime('now')),
      last_seen_at      TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_consolidado_fuente ON consolidado(fuente);
    CREATE INDEX IF NOT EXISTS idx_consolidado_hash   ON consolidado(hash_contenido);

    -- STAGING (por run)
    CREATE TABLE IF NOT EXISTS staging_consolidado (
      run_id            TEXT NOT NULL,
      id_registro       TEXT NOT NULL,
      fuente            TEXT NOT NULL,
      tipo_sujeto       TEXT,
      nombres           TEXT,
      apellidos         TEXT,
      aliases           TEXT,
      fecha_nacimiento  TEXT,
      nacionalidad      TEXT,
      numero_documento  TEXT,
      tipo_sancion      TEXT,
      fecha_sancion     TEXT,
      fecha_vencimiento TEXT,
      activo            INTEGER NOT NULL,
      fecha_ingesta     TEXT NOT NULL,
      origen_id         TEXT,
      hash_contenido    TEXT NOT NULL,

      PRIMARY KEY(run_id, id_registro)
    );

    CREATE INDEX IF NOT EXISTS idx_staging_run ON staging_consolidado(run_id);
    CREATE INDEX IF NOT EXISTS idx_staging_hash ON staging_consolidado(hash_contenido);

    -- RUNS (control plane)
    CREATE TABLE IF NOT EXISTS ingestion_runs (
      run_id TEXT PRIMARY KEY,
      env TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      status TEXT NOT NULL,
      notes TEXT
    );

    -- METRICS por run+fuente
    CREATE TABLE IF NOT EXISTS ingestion_run_metrics (
      run_id TEXT NOT NULL,
      source_key TEXT NOT NULL,

      records_out INTEGER,
      staging_records INTEGER,
      quality_issue_count INTEGER,
      quality_error_count INTEGER,
      quality_warn_count INTEGER,

      extract_status TEXT,
      transform_status TEXT,

      -- matching (opcional para futuro)
      avg_match_score REAL,
      match_rate REAL,
      matches_count INTEGER,

      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (run_id, source_key)
    );

    CREATE INDEX IF NOT EXISTS idx_run_metrics_source ON ingestion_run_metrics(source_key);
    CREATE INDEX IF NOT EXISTS idx_run_metrics_run ON ingestion_run_metrics(run_id);

    -- ALERTAS
    CREATE TABLE IF NOT EXISTS alerts (
      alert_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      source_key TEXT,
      alert_type TEXT NOT NULL,
      severity TEXT NOT NULL,
      channel TEXT NOT NULL,
      threshold TEXT,
      value TEXT,
      message TEXT NOT NULL,
      context_json TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_alerts_run ON alerts(run_id);
    CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type);
    """
    conn.executescript(ddl)
    conn.commit()