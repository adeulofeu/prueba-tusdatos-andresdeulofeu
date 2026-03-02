# Diseño del Esquema Canónico
## Suposiciones y Decisiones de Arquitectura

## 1. Objetivo

El objetivo del esquema canónico es unificar múltiples fuentes heterogéneas de datos de sanciones en un modelo único, consistente y extensible que permita:

- Integrar diferentes estructuras (XML, CSV, TXT, ZIP).
- Estandarizar atributos clave.
- Detectar cambios mediante hash de contenido.
- Facilitar futuras extensiones.
- Mantener simplicidad operativa.

---

## 2. Fuentes Integradas

Las fuentes consideradas en el diseño fueron:

- OFAC (SDN XML)
- Naciones Unidas (UN Consolidated XML)
- PACO SIRI (ZIP + TXT)
- PACO FGN (CSV)
- EU (XML – parcialmente disponible)

Cada fuente presenta diferencias en:

- Estructura
- Granularidad
- Identificadores
- Modelo de sanción
- Tipo de entidad

---

## 3. Estructura Final del Esquema Canónico

```json
{
  "fuente": str,
  "tipo_sujeto": str,
  "nombres": str,
  "apellidos": str,
  "aliases": list,
  "fecha_nacimiento": str,
  "nacionalidad": list,
  "numero_documento": str,
  "tipo_sancion": str,
  "fecha_sancion": str,
  "fecha_vencimiento": str,
  "activo": bool,
  "fecha_ingesta": str,
  "origen_id": str,
  "hash_contenido": str,
  "id_registro": str
}

## 4 Suposiciones y decisiones por campo

# 4.1 Fuente
- Cada registro pertence exclusivamente a una fuente primaria
- No se implementó deduplicción entre fuentes

# 4.2 Tipo sujeto
Valores definidos
- PERSONA_NATURAL
- PERSONA_JURIDICA

Decisiones tomadas
- OFAC: Individual (PERSONA_NATURAL), resto (PERSONA_JURIDICA)
- UN: Individual/ENTITY
- PACO_SIRI: Tratados como PERSONA_NATURAL
- PACO_FGN: Tratados como PERSONA_JURIDICA

Limitaciones:
No se implementaron categorías intermedias

# 4.3 Nombres y apellidos
Decisiones:
- Se separan nombres y apellidos solo cuando la fuente lo permite
- Para PERSONA_JURIDICA apellidos siempre seran NULL
- Si no es posible separar, el nombre completo se guarda en nombres

Estandarización aplicadas:
- Eliminación de espacios redundantes
- Trim
- Conversión de mayúsculas y eliminación de tildes

# 4.4 Aliases
- Siempre se representa como lista
- Nunca se alamacenan como NULL
- Permite múltiples alias cuando la fuente lo proporciona

Esto facilita comparaciones, serialización y hash deterministico

# 4.5 Fecha de nacimiento
- Si solo trae el año se convierte a formato iso YYYY-01-01
- Si trae un rango se almacena como NULL

# 4.6 Nacionalidad
- siempre representado como lista
- Actualmente no se realiza mapeo obligatorio a codigos ISO

# 4.7 Numero de documento
- Se utiliza solo el primer documento disponible
- No se modelan múltiples documentos por registro

# 4.8 Tipo de sanción
Como cada fuente modela sanciones de forma distinta se construye un campo explicativo tipo string compuesto:
List=SDN | Program=CUBA | Type=Block

Se prioriza legibilidad, trazabilidad simplicidad

# 4.9 Fecha de sanción
- Todas las fechas se convierten a formato ISO YYYY-MM-DD
- Si no es posible parsear se deja como NULL

# 4.10 Fecha de vencimiento
- Actualmente siempre como NULL
- Se mantiene en el modelo para futuras actualziaciones

# 4.11 Activo
- Se asume True para todos los registros
- No se ha implementado detección de sanciones removidas

# 4.12 Fecha de ingesta
- Formato ISO UTC con sufijo Z
- No participa en el hash de contenido

# 4.13 Origen Id
Se usa el identificador de origen dependiendo de la fuente
- OFAC: identityId
- UN: DATAID
- PACO_SIRI: id_fuente
- PACO_FGN: id

Permite trazabilidad hacia el sistema origen

# 4.14 Hash de contenido
Se calcula sobre el valor del diccionario con el modelo canonico excluyendo la fecha de ingesta con el fin de:

- Detectar cambios reales en el contenido
- Permitir merge incremental

# 4.15 Id de registro
Formato:
{fuente}:{hash_contenido}

Con el fin de garantizar unicidad y trazabilidad

## 5. Estrategia de actualización, matching incremental y notificaciones

# 5.1 ¿Con qué frecuencia se actualiza cada fuente y cómo se detecta que hay una versión nueva?

La frecuencia se define según criticidad y volatilidad histórica de la fuente:

- OFAC SDN: diario.
- UN Consolidated: diario.
- EU: diario.
- PACO (SIRI / FGN): semanal (ajustable según publicación real).
- WorldBank (scraper): semanal o mensual.

La ejecución diaria es el baseline recomendado para listas de sanciones por su impacto regulatorio.

Antes de ejecutar transformación y carga, se valida si la fuente cambió.

5.1.1. La detección sigue el siguiente orden de prioridad:
- Señales HTTP (si están disponibles)
- Comparar ETag
- Comparar Last-Modified

Si alguno de estos atributos esta disponible y cambia se asume que existe una versión nueva.

5.1.2 Hash del archivo raw
Si no hay metadata confiable:
- Descargar archivo raw.
- Calcular sha256.
- Comparar contra el último sha256 persistido para esa fuente.

Si el hash es igual se omite transformación.

5.2 ¿Cómo se re-ejecuta el matching solo sobre los registros que cambiaron sin reprocesar todo?
El sistema utiliza hash_contenido estable por registro en consolidado. Un registro se considera impactado cuando:
- Es un INSERT nuevo.
- Su hash_contenido cambió respecto al valor anterior, si el hash no cambiael registro no se vuelve a evaluar.

Para ejecutarlo solamente en los registros que cambiaron se utilizaria la consulta

SELECT id_registro
FROM cambios_consolidado
WHERE run_id = ?
AND change_type IN ('INSERT', 'UPDATE_HASH')

para extraer solamente los id_registros con cambios y así reducir los costos computacionales y tener una mejor escalabilidad cuando el volumne crezca

5.3 ¿Cómo se notifica a los consumidores cuando un tercero que antes estaba limpio ahora aparece en una lista?

- Se genera una tabla para guardar los cambios, donde se guarde el evento y el tipo de evento si fue sancionado o termino su sanción
- El pipeline detecta el cambio de y actualiza la información en la tabla consolidada
- Un proceso independiente consume eventos para enviar la notificación.

## 6. Preguntas tecnicas
6.2 Schema evolution: la fuente OFAC agrega un campo nuevo en su XML. ¿Cómo maneja ese cambio sin romper el pipeline ni perder datos históricos?

Falso negativo: Dejar pasar un sancionado.
- Impacto regulatorio y financiero severo.
- Riesgo reputacional.
- Posibles sanciones legales.

Falso positivo: Marcar incorrectamente a una persona como sancionada.
- Fricción en la experiencia.
- Costos operativos por revisión manual.
- Posible afectación reputacional individual.

Desde el punto de vista de compliance, los falsos negativos son más costosos, ya que implican riesgo regulatorio directo.

Estrategia de tres zonas
Se propone un modelo con tres niveles de decisión:

Auto-match
Registros con alta similitud (score >= T_high).
- Se consideran coincidencias válidas.
- No requieren intervención humana.

Zona gris
Registros con similitud intermedia (T_low <= score < T_high).
- Se genera alerta.
- Revisión manual por el equipo de operaciones.
- Reduce riesgo de falsos negativos.

No match
Registros con baja similitud (score < T_low).
- No se genera alerta.
- No requiere intervención.

Calibración de umbrales
Los valores T_low y T_high se calibran utilizando la base de datos sintética generada, evaluando:

- Precision
- Recall
- Tasa de falsos positivos
- Tasa de falsos negativos
- Volumen de casos en zona gris

Ajustes operativos:
- Si aumenta el riesgo de falsos negativos se reduce T_low.
- Si aumenta la carga manual se incrementa T_high.

Este enfoque permite balancear riesgo regulatorio y costo operativo.

6.3 Acceso a los datos: un analista externo solicita acceso completo a las listas normalizadas para un proyecto de investigación. Los datos son públicos en origen pero el pipeline agrega información adicional. ¿Cómo maneja el request?

Estrategia: Acceso por capas y roles

Aunque los datos en origen son públicos, el pipeline agrega:
- Normalización estructurada
- Identificadores internos
- Metadatos de ejecución
- Información operativa de matching

Por lo tanto, el acceso se organiza en capas:

Capa pública normalizada
Incluye:
- Datos canónicos de las listas.
- Información pública normalizada.

Excluye:

- Scores de matching.
- Reglas internas.
- Decisiones manuales.
- Información operativa.

Es la capa recomendada para proyectos externos de investigación.

Capa operativa restringida
Incluye:
- Tablas de matching.
- Alertas generadas.
- Scores.
- Decisiones humanas.

Acceso solo para usuarios internos autorizados.

Capa raw
Incluye:
- Archivos originales.
- Información de trazabilidad.

Se entrega únicamente si el proyecto requiere auditoría profunda.

Principios aplicados
- Separación entre datos públicos y lógica interna.
- Mínimo privilegio.
- Trazabilidad y control de acceso.
- Exportes específicos para el caso de uso.

6.4 Frecuencia vs. costo: OFAC puede actualizarse varias veces al día. ¿Cómo diseñaría el pipeline para balancear frescura de datos con costo operacional?

El pipeline implementa:
Detección de cambios previa al procesamiento
- Validación por ETag, Last-Modified o sha256.
- Solo se procesa si existe un cambio real en la fuente.

Procesamiento incremental
- Solo se transforman y mergean registros nuevos o modificados.
- El matching se ejecuta únicamente sobre registros impactados.

Ventanas de actualización
- Se pueden definir ventanas máximas de procesamiento.
- En caso de múltiples cambios en corto tiempo, se puede agrupar el procesamiento.

Resultado
- Se mantiene frescura de datos.
- Se evita reprocesar innecesariamente.
- Se optimiza el consumo computacional.
- El sistema escala adecuadamente si el volumen crece.

