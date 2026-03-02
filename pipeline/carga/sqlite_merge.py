import sqlite3


def merge_staging_to_consolidado(conn: sqlite3.Connection, run_id: str) -> None:
    """
    Merge final optimizado (INSERT + 2 UPDATEs) y con pocos bindings.

    Flujo:
    1) Si staging está vacío → salir
    2) INSERT nuevos
    3) UPDATE last_seen_at + fecha_ingesta (todos los presentes)
    4) UPDATE SOLO registros cuyo hash cambió (copia columnas desde staging)
    """

    # 0) Validar si hay datos en staging para este run
    cur = conn.execute(
        "SELECT 1 FROM staging_consolidado WHERE run_id = ? LIMIT 1;",
        (run_id,),
    )
    if cur.fetchone() is None:
        return

    # 1) INSERT nuevos registros
    conn.execute(
        """
        INSERT INTO consolidado (
          id_registro, fuente, tipo_sujeto, nombres, apellidos, aliases,
          fecha_nacimiento, nacionalidad, numero_documento,
          tipo_sancion, fecha_sancion, fecha_vencimiento,
          activo, fecha_ingesta, origen_id, hash_contenido,
          hash_anterior, first_seen_at, last_seen_at, updated_at
        )
        SELECT
          s.id_registro, s.fuente, s.tipo_sujeto, s.nombres, s.apellidos, s.aliases,
          s.fecha_nacimiento, s.nacionalidad, s.numero_documento,
          s.tipo_sancion, s.fecha_sancion, s.fecha_vencimiento,
          s.activo, s.fecha_ingesta, s.origen_id, s.hash_contenido,
          NULL, datetime('now'), datetime('now'), datetime('now')
        FROM staging_consolidado s
        LEFT JOIN consolidado c ON c.id_registro = s.id_registro
        WHERE s.run_id = ? AND c.id_registro IS NULL;
        """,
        (run_id,),
    )

    # 2) UPDATE básico: siempre actualizar last_seen_at + fecha_ingesta
    conn.execute(
        """
        WITH s AS (
          SELECT id_registro, fecha_ingesta
          FROM staging_consolidado
          WHERE run_id = ?
        )
        UPDATE consolidado
        SET
          last_seen_at = datetime('now'),
          fecha_ingesta = (SELECT s.fecha_ingesta FROM s WHERE s.id_registro = consolidado.id_registro)
        WHERE id_registro IN (SELECT id_registro FROM s);
        """,
        (run_id,),
    )

    # 3) UPDATE SOLO cuando hash cambió (copiar columnas desde staging)
    #    Ventaja: solo 1 binding (run_id) y el resto lo resuelve el CTE.
    conn.execute(
        """
        WITH s AS (
          SELECT
            id_registro, fuente, tipo_sujeto, nombres, apellidos, aliases,
            fecha_nacimiento, nacionalidad, numero_documento,
            tipo_sancion, fecha_sancion, fecha_vencimiento,
            activo, origen_id, hash_contenido
          FROM staging_consolidado
          WHERE run_id = ?
        )
        UPDATE consolidado
        SET
          hash_anterior = consolidado.hash_contenido,

          fuente = (SELECT s.fuente FROM s WHERE s.id_registro = consolidado.id_registro),
          tipo_sujeto = (SELECT s.tipo_sujeto FROM s WHERE s.id_registro = consolidado.id_registro),
          nombres = (SELECT s.nombres FROM s WHERE s.id_registro = consolidado.id_registro),
          apellidos = (SELECT s.apellidos FROM s WHERE s.id_registro = consolidado.id_registro),
          aliases = (SELECT s.aliases FROM s WHERE s.id_registro = consolidado.id_registro),
          fecha_nacimiento = (SELECT s.fecha_nacimiento FROM s WHERE s.id_registro = consolidado.id_registro),
          nacionalidad = (SELECT s.nacionalidad FROM s WHERE s.id_registro = consolidado.id_registro),
          numero_documento = (SELECT s.numero_documento FROM s WHERE s.id_registro = consolidado.id_registro),
          tipo_sancion = (SELECT s.tipo_sancion FROM s WHERE s.id_registro = consolidado.id_registro),
          fecha_sancion = (SELECT s.fecha_sancion FROM s WHERE s.id_registro = consolidado.id_registro),
          fecha_vencimiento = (SELECT s.fecha_vencimiento FROM s WHERE s.id_registro = consolidado.id_registro),
          activo = (SELECT s.activo FROM s WHERE s.id_registro = consolidado.id_registro),
          origen_id = (SELECT s.origen_id FROM s WHERE s.id_registro = consolidado.id_registro),
          hash_contenido = (SELECT s.hash_contenido FROM s WHERE s.id_registro = consolidado.id_registro),

          updated_at = datetime('now')
        WHERE id_registro IN (
          SELECT c.id_registro
          FROM consolidado c
          JOIN s ON c.id_registro = s.id_registro
          WHERE c.hash_contenido <> s.hash_contenido
        );
        """,
        (run_id,),
    )

    conn.commit()