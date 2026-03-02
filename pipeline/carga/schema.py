def init_staging_and_consolidado(conn):
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
    """
    conn.executescript(ddl)
    conn.commit()