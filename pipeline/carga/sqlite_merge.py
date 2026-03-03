"""pipeline/carga/sqlite_merge.py

Merge final: staging_consolidado -> consolidado (productiva).

Idea:
- staging_consolidado tiene data canónica de un run específico.
- consolidado mantiene historial ligero:
    - hash_anterior: hash previo cuando cambia un registro
    - first_seen_at / last_seen_at / updated_at: auditoría temporal

Estrategia (SQLite-friendly):
1) Insertar nuevos id_registro que no existan en consolidado.
2) Para id_registro existentes:
   - si hash_contenido cambió => actualizar columnas y mover hash a hash_anterior
   - siempre actualizar last_seen_at (visto en este run)

Esto te da:
- Upsert determinístico (por id_registro)
- Detección de cambios (por hash_contenido)
"""

import sqlite3
from typing import Dict, Any


def merge_staging_to_consolidado(conn: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    """
    Merge final: staging_consolidado -> consolidado.

    Retorna estadísticas para el reporte de ingesta:
    - seen: total de registros presentes en staging para el run
    - seen_by_source: conteo por fuente en staging
    - inserted: nuevos registros insertados en consolidado
    - updated: registros existentes cuyo hash cambió (actualizados)
    - missing_by_source: conteo de registros que existían en consolidado (activo=1)
      pero NO aparecieron en el snapshot del run (por fuente). Esto es el equivalente
      operativo de "eliminados" / "missing" para listas tipo snapshot.

    Nota: NO marcamos inactivos automáticamente; solo reportamos. Si quisieras,
    se puede extender con un flag para setear activo=0.
    """

    # 0) Validar si hay datos en staging para este run
    cur = conn.execute(
        "SELECT 1 FROM staging_consolidado WHERE run_id = ? LIMIT 1;",
        (run_id,),
    )
    if cur.fetchone() is None:
        return {
            "run_id": run_id,
            "seen": 0,
            "seen_by_source": {},
            "inserted": 0,
            "updated": 0,
            "missing_by_source": {},
        }

    # 0.1) Conteos "seen" (staging snapshot del run)
    seen = conn.execute(
        "SELECT COUNT(*) FROM staging_consolidado WHERE run_id = ?;",
        (run_id,),
    ).fetchone()[0]

    seen_by_source_rows = conn.execute(
        """
        SELECT fuente, COUNT(*) 
        FROM staging_consolidado
        WHERE run_id = ?
        GROUP BY fuente;
        """,
        (run_id,),
    ).fetchall()
    seen_by_source = {fuente: int(n) for fuente, n in seen_by_source_rows}

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
    inserted = conn.execute("SELECT changes();").fetchone()[0]

    # 2) UPDATE básico: siempre actualizar last_seen_at + fecha_ingesta (todos los presentes)
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
    # (no tomamos changes() aquí porque incluye también updates “sin cambio”)

    # 3) UPDATE SOLO cuando hash cambió (copiar columnas desde staging)
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
    updated = conn.execute("SELECT changes();").fetchone()[0]

    # 4) Missing/"eliminados" por fuente (snapshot semantics)
    # Definición: registros activos en consolidado para una fuente que NO aparecen en staging (run actual).
    missing_by_source: Dict[str, int] = {}
    for fuente in seen_by_source.keys():
        missing = conn.execute(
            """
            SELECT COUNT(*)
            FROM consolidado c
            WHERE c.fuente = ?
              AND c.activo = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM staging_consolidado s
                  WHERE s.run_id = ?
                    AND s.fuente = ?
                    AND s.id_registro = c.id_registro
              );
            """,
            (fuente, run_id, fuente),
        ).fetchone()[0]
        missing_by_source[fuente] = int(missing)

    conn.commit()

    return {
        "run_id": run_id,
        "seen": int(seen),
        "seen_by_source": seen_by_source,
        "inserted": int(inserted),
        "updated": int(updated),
        "missing_by_source": missing_by_source,
    }