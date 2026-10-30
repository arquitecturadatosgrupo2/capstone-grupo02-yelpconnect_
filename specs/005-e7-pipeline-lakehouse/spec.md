# Feature Specification: E7.1–E7.3 · Pipeline de ingesta y habilitación

**Feature Branch**: `feature/E7-pipeline-lakehouse`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E7.1–E7.3 (Pipeline bronze → silver → gold, M6 Big Data + M7 Integración).

**Trazabilidad**: implementa la sección 3 «Arquitectura de Referencia (ABB/SBB)» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Pipeline reproducible de extremo a extremo (Priority: P1)

El Ingeniero de datos necesita que, al clonar el repositorio y ejecutar los pasos en el orden documentado sobre la muestra de una ciudad, se obtengan las tres capas pobladas con el mismo resultado, sin pasos manuales no documentados.

**Why this priority**: es el corazón técnico (15 % de la nota) y la demo en vivo de M6; sin él no hay datos para E5, E6 ni E8.

**Independent Test**: ejecutar el pipeline completo en un workspace limpio y comparar conteos por tabla con una corrida de referencia.

**Acceptance Scenarios**:

1. **Given** la muestra cargada en el área de datos crudos, **When** se ejecuta el pipeline completo, **Then** termina sin errores y todas las tablas bronze, silver y gold tienen filas.
2. **Given** una corrida previa, **When** se re-ejecuta el pipeline, **Then** los conteos no se duplican.

---

### User Story 2 - Bronze fiel al origen con auditoría (Priority: P1)

La CDO necesita que bronze conserve los datos tal como llegaron (sin limpiar ni filtrar) más el archivo de origen y el momento de carga, para poder rastrear cualquier dato hasta su fuente.

**Independent Test**: comparar el conteo de registros de cada archivo crudo con su tabla bronze y verificar las columnas de auditoría.

**Acceptance Scenarios**:

1. **Given** un archivo crudo, **When** se ingiere, **Then** la tabla bronze tiene el mismo número de registros y cada fila indica su archivo de origen y timestamp de carga.
2. **Given** las fuentes internas simuladas de Trust & Safety, **When** se ingieren, **Then** quedan en bronze declaradas como simuladas.

---

### User Story 3 - Ingesta incremental idempotente (Priority: P1)

Para M7, el grupo necesita demostrar en vivo que, al subir un archivo nuevo de la entidad de mayor volumen (reseñas), solo se procesa lo nuevo, y que re-ejecutar sin archivos nuevos no agrega nada.

**Why this priority**: es el criterio único de evaluación de M7.

**Independent Test**: anotar el conteo de reseñas en bronze, subir un archivo con N reseñas nuevas, re-ejecutar la ingesta y verificar que el conteo aumenta exactamente en N; re-ejecutar de nuevo y verificar que no cambia.

**Acceptance Scenarios**:

1. **Given** la ingesta incremental al día, **When** se sube un archivo con N registros nuevos y se re-ejecuta, **Then** bronze crece exactamente en N.
2. **Given** ningún archivo nuevo, **When** se re-ejecuta, **Then** bronze no cambia.

---

### User Story 4 - Silver limpio que resuelve los dolores del caso (Priority: P2)

Marketing necesita conteos de reseñas sin duplicados y Operaciones Comerciales un conteo oficial y normalizado de check-ins. Silver debe aplicar limpieza y conformación siguiendo el modelo físico (E3), apartando —no borrando— los registros que violan integridad.

**Independent Test**: verificar 0 duplicados en reseñas de silver, que los duplicados lógicos están en cuarentena con su motivo, y que los check-ins están normalizados a un registro por negocio y fecha.

**Acceptance Scenarios**:

1. **Given** reseñas con duplicados exactos y lógicos (mismo usuario, negocio y texto), **When** se procesa silver, **Then** queda una sola versión y las copias quedan en cuarentena con el motivo.
2. **Given** reseñas cuyo autor no está en la muestra, **When** se procesa silver, **Then** se conservan marcadas y el impacto queda cuantificado como riesgo aceptado.

---

### User Story 5 - Productos de datos gold con consumidor identificado (Priority: P2)

Cada stakeholder necesita al menos un producto gold que responda una pregunta concreta suya; si no se puede nombrar al consumidor, la tabla no debe existir. Mínimo tres productos documentados, más el modelo estrella para Data Science.

**Independent Test**: listar las tablas gold y, para cada una, leer en su descripción el stakeholder y la pregunta que responde.

**Acceptance Scenarios**:

1. **Given** la capa gold, **When** se listan sus tablas, **Then** hay ≥ 3 productos de datos, cada uno con stakeholder y pregunta declarados.
2. **Given** cualquier producto gold, **When** se inspecciona su linaje, **Then** solo lee de silver (nunca de bronze ni de datos crudos).
3. **Given** las reglas de fraude parametrizadas, **When** Trust & Safety cambia un umbral en el catálogo, **Then** el producto de riesgo refleja el cambio sin modificar código.

### Edge Cases

- El área de datos crudos está vacía o falta un archivo: el pipeline falla temprano indicando qué falta.
- Registros con claves nulas o claves foráneas que no existen en maestros: van a cuarentena o generan miembros inferidos según lo decidido en E3, nunca se pierden en silencio.
- Esquema de un archivo nuevo con columnas adicionales: la ingesta incremental lo tolera sin romper la tabla.
- Agotamiento de cuota a mitad de corrida: la re-ejecución posterior completa sin duplicar.
- Notebooks de plantilla del esqueleto que ya no se usan: se eliminan o se marcan para que no confundan el orden de ejecución.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Bronze DEBE contener una tabla `brz_` por entidad fuente (Yelp y fuentes simuladas de Trust & Safety) con columnas de auditoría de origen y timestamp de carga.
- **FR-002**: La entidad de mayor volumen DEBE ingerirse de forma incremental e idempotente.
- **FR-003**: Silver DEBE escribir sobre las tablas definidas en E3, sin redefinir su esquema.
- **FR-004**: Silver DEBE deduplicar reseñas (exactas y lógicas) y apartar a cuarentena, con motivo, los registros que violan integridad.
- **FR-005**: Silver DEBE normalizar los check-ins a un registro por negocio y fecha.
- **FR-006**: Gold DEBE publicar ≥ 3 productos de datos documentados, cada uno con stakeholder y pregunta de negocio declarados.
- **FR-007**: Gold DEBE leer exclusivamente de silver.
- **FR-008**: Los parámetros de negocio de los productos de riesgo DEBEN gobernarse desde el catálogo parametrizado de reglas de fraude.
- **FR-009**: Todo el pipeline DEBE ser re-ejecutable sin duplicar datos.
- **FR-010**: Cada decisión batch vs. incremental por entidad y cada riesgo aceptado DEBEN documentarse con justificación e impacto cuantificado.
- **FR-011**: El README DEBE reflejar el orden real de ejecución de los notebooks del pipeline.

### Key Entities

- **Entidad fuente**: negocio, usuario, reseña, tip, check-in; fuentes simuladas: áreas de Trust & Safety y reglas de fraude.
- **Registro en cuarentena**: reseña apartada con su motivo.
- **Producto de datos**: tabla gold con consumidor, pregunta y Data Owner declarados.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: El pipeline completo corre sin errores sobre la muestra (< 300 MB en crudo) dentro de la cuota diaria.
- **SC-002**: Demo incremental: subir N registros nuevos incrementa bronze exactamente en N; re-ejecutar sin archivos nuevos produce 0 cambios.
- **SC-003**: 0 reseñas duplicadas en silver; 100 % de los registros apartados localizables en cuarentena.
- **SC-004**: ≥ 3 productos gold con consumidor declarado; 0 productos gold que lean bronze.
- **SC-005**: Dos ejecuciones consecutivas producen conteos idénticos por tabla.

## Assumptions

- Estado actual: la implementación vive en `E7_pipeline_lakehouse/pipelines/` y está en `main`; los notebooks `02_silver_limpieza` y `03_gold_productos_datos` siguen siendo plantilla y su relación con `pipelines/` debe aclararse.
- Falta documentar con capturas el experimento de idempotencia incremental.
- Las fuentes de Trust & Safety son simuladas y se versionan en `datos/semillas/`.
- La orquestación, monitoreo y FinOps se especifican aparte (006-e7-operacion-finops).
