# Feature Specification: E3 · Modelo físico del Lakehouse

**Feature Branch**: `feature/E3-modelo-fisico`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E3 (Modelo físico, M4 Modelado Avanzado).

**Trazabilidad**: implementa la sección 4 «Modelo de datos conceptual y lógico» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Materializar cada entidad del modelo lógico (Priority: P1)

La Modeladora de datos (rol de Andrew Collins, VP de Producto) necesita que cada entidad y atributo del modelo lógico del Documento Formal exista en la plataforma como tabla y columna, en la capa conformada (silver) y en la dimensional (gold), para que el docente pueda recorrer el modelo y el catálogo y comprobar que lo diseñado es lo construido.

**Why this priority**: es la base de E4, E5, E6 y E7; sin el modelo físico ningún otro entregable tiene dónde escribir ni qué documentar. Pesa en la dimensión «Plataforma: modelado y metadata» (15 %).

**Independent Test**: ejecutar el notebook de E3 sobre un catálogo vacío y comparar la lista de tablas/columnas resultante contra la lista de entidades/atributos del modelo lógico del documento.

**Acceptance Scenarios**:

1. **Given** un catálogo con los schemas creados pero sin tablas, **When** se ejecuta el DDL de E3 de inicio a fin, **Then** termina sin errores y existen todas las tablas `slv_*` y `gld_*` del modelo lógico.
2. **Given** el modelo lógico del documento, **When** se compara entidad por entidad contra el catálogo, **Then** cada entidad tiene su tabla y cada atributo su columna, sin entidades huérfanas en ninguno de los dos lados (o la diferencia está registrada como decisión).

---

### User Story 2 - Metadata completa que alimenta el diccionario (Priority: P1)

El Steward de gobierno necesita que toda tabla y columna lleve una descripción en español y que cada tabla declare su Data Owner, porque el diccionario de datos (E4) se autogenera desde esa metadata.

**Why this priority**: una columna sin comentario se arrastra como hueco en el diccionario y baja la cobertura de documentación evaluada.

**Independent Test**: consultar el catálogo de metadatos y contar columnas de silver y gold sin descripción.

**Acceptance Scenarios**:

1. **Given** el DDL ejecutado, **When** se consulta la metadata de silver y gold, **Then** el 0 % de tablas y columnas carece de descripción.
2. **Given** cualquier tabla de silver o gold, **When** se lee su descripción, **Then** nombra a su Data Owner según la asignación vigente (Producto, Trust & Safety o CDO).

---

### User Story 3 - Integridad declarada y exigida (Priority: P2)

La Modeladora necesita que las claves primarias y foráneas queden declaradas (documentan relaciones y cardinalidades) y que las reglas de nulidad y dominio queden exigidas por la propia tabla, para que la carga impida datos imposibles (calidad preventiva).

**Why this priority**: demuestra fidelidad al modelo lógico y habilita linaje; las restricciones exigidas complementan al motor de calidad de E5.

**Independent Test**: intentar insertar una fila que viole una restricción exigida (p. ej. calificación 7) y verificar el rechazo; listar las claves declaradas y contrastarlas con las relaciones del modelo lógico.

**Acceptance Scenarios**:

1. **Given** una tabla con restricción de dominio, **When** se intenta cargar un valor fuera de dominio, **Then** la carga se rechaza.
2. **Given** el modelo lógico, **When** se listan las claves primarias y foráneas declaradas, **Then** toda relación del modelo tiene su clave declarada.

---

### User Story 4 - Re-ejecución segura (Priority: P2)

El Ingeniero de datos necesita poder re-ejecutar el DDL sobre tablas ya pobladas sin perder datos, para que el job y cualquier integrante puedan correrlo en cualquier momento.

**Independent Test**: poblar las tablas con el pipeline, re-ejecutar E3 y verificar que los conteos de filas no cambian.

**Acceptance Scenarios**:

1. **Given** tablas silver y gold pobladas, **When** se re-ejecuta E3, **Then** termina sin errores y los conteos de filas son idénticos a los previos.

### Edge Cases

- Una entidad del modelo lógico se descarta o cambia durante la implementación: se actualiza el documento en el mismo ciclo y se registra la decisión en el markdown del notebook.
- Dimensiones con miembros inferidos (claves que llegan en hechos sin registro maestro por el recorte del muestreo): la restricción de FK no debe impedir su carga.
- Una restricción de dominio se modifica: se debe poder recrear sin borrar la tabla.
- Tablas obsoletas de iteraciones previas presentes en el catálogo: deben eliminarse o justificarse, nunca quedar sin documentar.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El entregable DEBE crear, en la capa conformada, una tabla por cada entidad maestra y transaccional del modelo lógico (negocio, usuario, reseña, tip, check-in, áreas de Trust & Safety, reglas de fraude y la cuarentena de reseñas).
- **FR-002**: El entregable DEBE crear, en la capa dimensional, las dimensiones y hechos del modelo estrella declarado en el documento, más los productos de datos de Trust & Safety.
- **FR-003**: Toda tabla y toda columna de silver y gold DEBE tener descripción en español.
- **FR-004**: La descripción de cada tabla DEBE nombrar a su Data Owner según la asignación vigente.
- **FR-005**: Toda relación del modelo lógico DEBE tener declarada su clave primaria y foránea.
- **FR-006**: Los atributos obligatorios DEBEN rechazar nulos, y los atributos con dominio acotado (p. ej. calificaciones 1–5) DEBEN rechazar valores fuera de dominio al cargarse.
- **FR-007**: El DDL DEBE ser re-ejecutable sin pérdida de datos ni errores por objetos existentes.
- **FR-008**: El DDL DEBE ser la única definición del esquema físico de silver y gold; los pipelines escriben sobre esas tablas sin redefinirlas.
- **FR-009**: Los nombres DEBEN seguir las convenciones `slv_`, `gld_`, `vw_` y los schemas oficiales.
- **FR-010**: El notebook DEBE abrir con el bloque de Trazabilidad y documentar en markdown toda decisión que difiera del modelo lógico.

### Key Entities

- **Entidad conformada (silver)**: versión limpia de cada entidad del caso; atributos de negocio + timestamp de auditoría de carga.
- **Dimensión (gold)**: fecha, negocio, usuario; admite miembros inferidos.
- **Hecho (gold)**: reseña y check-in diario, con claves hacia las dimensiones.
- **Producto de datos de riesgo (gold)**: perfil de fraude de usuario, ráfagas anómalas por negocio, gobierno de reglas de Trust & Safety.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: El 100 % de las entidades del modelo lógico tiene su tabla en el catálogo (0 entidades huérfanas no justificadas).
- **SC-002**: 0 columnas de silver y gold sin descripción.
- **SC-003**: El DDL corre de inicio a fin sin errores sobre un catálogo vacío y sobre uno poblado, sin variación en los conteos de filas tras la re-ejecución.
- **SC-004**: El 100 % de las relaciones del modelo lógico tiene clave declarada.
- **SC-005**: Un integrante cualquiera puede recorrer el modelo y el catálogo en la sustentación respondiendo de dónde sale cada tabla en menos de 1 minuto por entidad.

## Assumptions

- El modelo lógico vigente es el de `YELP.sql` y `mapeo_equipo_02_utec.xlsx` (Drive); la sección 4 del Documento Formal debe reflejarlo (hoy el documento está casi vacío: es una dependencia).
- Asignación de Data Owners del 2026-10-23: Producto → negocio, usuario, check-in y sus dims; Trust & Safety → reseña, tip, fraude, equipos y productos de riesgo; CDO → dimensión fecha y gobierno.
- Estado actual: el DDL existe en `main` y es idempotente; esta spec sirve de referencia para verificar cobertura contra el documento y limpiar tablas obsoletas.
- Las claves foráneas son informativas (no exigidas por la plataforma); la integridad referencial efectiva la verifica E5.
