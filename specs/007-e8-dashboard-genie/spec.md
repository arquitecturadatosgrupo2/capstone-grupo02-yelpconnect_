# Feature Specification: E8 · Dashboard BI y Genie

**Feature Branch**: `feature/E8-dashboard-genie`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E8 (Dashboard AI/BI + Genie, M9 BI y Visualización).

**Trazabilidad**: implementa la sección 7 «Requerimientos de información y KPIs» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Página de Gobierno para la CDO (Priority: P1)

La CDO (Laura Hernández) necesita una página que muestre la salud del gobierno: cumplimiento de calidad por dimensión y su evolución, cobertura de documentación del diccionario, cobertura de ownership por entidad, alertas abiertas de Trust & Safety y estado operativo del pipeline.

**Why this priority**: la guía indica que esta página «suele marcar la diferencia»; concentra la evidencia de E4, E5, E6 y E7.4.

**Independent Test**: abrir la página de Gobierno y verificar que cada indicador cuadra con una consulta directa a las tablas de gobierno.

**Acceptance Scenarios**:

1. **Given** el historial de calidad, **When** se abre la página, **Then** se ve el cumplimiento por dimensión de la última ejecución, las reglas en rojo y la tendencia entre ejecuciones.
2. **Given** el diccionario y la matriz RACI, **When** se abre la página, **Then** se ve el % de columnas documentadas y el % de entidades con Data Owner.
3. **Given** una alerta de gobierno abierta en Trust & Safety, **When** se abre la página, **Then** aparece con su estado (resuelta o aceptada).

---

### User Story 2 - Página de Negocio por stakeholder (Priority: P1)

Producto, Marketing, Operaciones Comerciales y Trust & Safety necesitan ver los KPIs que pidieron en el documento: evolución de reseñas y rating, check-ins diarios oficiales, negocios y usuarios con señales de fraude, y salud de reseñas (cuarentena/duplicados). Cada visual declara a qué stakeholder y requerimiento responde.

**Why this priority**: «un indicador que ningún stakeholder pidió no suma»; la trazabilidad visual ↔ requerimiento es el criterio de evaluación.

**Independent Test**: para cada visual, encontrar el requerimiento correspondiente en la sección 7 del documento.

**Acceptance Scenarios**:

1. **Given** la página de Negocio, **When** se revisa cada visual, **Then** su subtítulo nombra al stakeholder y existe el requerimiento en el documento.
2. **Given** los filtros de ciudad y mes, **When** se aplican, **Then** todos los visuales de la página responden.

---

### User Story 3 - Preguntas en lenguaje natural con el vocabulario del negocio (Priority: P1)

Un usuario de negocio necesita preguntar en lenguaje natural (p. ej. «¿qué negocios tuvieron ráfagas sospechosas de reseñas el último mes?») y obtener respuestas correctas, usando los sinónimos del glosario.

**Why this priority**: el docente hace preguntas a Genie en vivo en M9.

**Independent Test**: plantear 5 preguntas de negocio predefinidas y verificar cada respuesta contra una consulta de referencia.

**Acceptance Scenarios**:

1. **Given** el espacio de preguntas configurado sobre gold y gobierno, **When** se plantean 5 preguntas de negocio, **Then** las 5 respuestas coinciden con la consulta de referencia.
2. **Given** una pregunta que usa un sinónimo del glosario, **When** se plantea, **Then** se interpreta como el término oficial.
3. **Given** una pregunta que inicialmente falló, **When** se ajustan las instrucciones, **Then** se responde correctamente y la iteración queda documentada.

---

### User Story 4 - Consumo solo desde capas publicadas (Priority: P2)

La Arquitecta necesita que el dashboard y el espacio de preguntas consuman exclusivamente gold y gobierno —nunca silver ni bronze— y que ningún dato personal se exponga sin máscara, validando la arquitectura de capas y los controles de E6.

**Independent Test**: revisar las fuentes de cada visual y del espacio de preguntas, y abrir el dashboard con un rol no autorizado.

**Acceptance Scenarios**:

1. **Given** el dashboard publicado, **When** se listan sus fuentes, **Then** el 100 % pertenece a gold o gobierno.
2. **Given** un rol no autorizado, **When** abre el dashboard, **Then** no ve datos personales en claro.

### Edge Cases

- El historial de calidad tiene una sola ejecución: la tendencia muestra un punto y lo indica, sin error.
- El dashboard se consulta mientras el job está corriendo: muestra la última versión publicada de gold.
- Genie no logra responder una pregunta ambigua: debe pedir aclaración o declarar que no sabe, no inventar.
- Un visual referencia una tabla gold eliminada o renombrada: se detecta antes de publicar.
- La sección 7 del documento aún no lista un requerimiento que un visual necesita: primero se agrega al documento.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El dashboard DEBE tener dos páginas: Negocio y Gobierno.
- **FR-002**: Cada visual DEBE declarar el stakeholder al que responde y corresponder a un requerimiento de la sección 7 del documento.
- **FR-003**: La página de Gobierno DEBE mostrar salud de calidad (por dimensión y en el tiempo), cobertura de documentación, cobertura de ownership, alertas de gobierno de Trust & Safety y estado del pipeline.
- **FR-004**: La página de Negocio DEBE cubrir al menos a Producto, Operaciones Comerciales y Trust & Safety, con filtros de ciudad y mes.
- **FR-005**: El dashboard y el espacio de preguntas DEBEN consumir solo gold y gobierno.
- **FR-006**: El espacio de preguntas DEBE incorporar instrucciones y ≥ 5 sinónimos tomados del glosario.
- **FR-007**: El espacio de preguntas DEBE responder correctamente 5 preguntas de negocio predefinidas, documentadas con su consulta de referencia.
- **FR-008**: El entregable DEBE documentar al menos una iteración en que una pregunta falló y cómo se corrigió.
- **FR-009**: Ningún visual DEBE exponer datos `confidencial` o `restringido` sin máscara.
- **FR-010**: El dashboard DEBE publicarse con refresco alineado a la cadencia del job.
- **FR-011**: Las consultas base DEBEN apuntar a los productos gold que existen realmente (no a ejemplos de la plantilla).
- **FR-012** *(traído de E6, research R7)*: El dashboard DEBE publicarse **sin credenciales embebidas** (cada espectador ve según sus permisos y máscaras) o con un publicador no autorizado a ver datos sensibles; con credenciales embebidas de un rol autorizado, todos los espectadores verían en claro. Genie usa siempre los permisos del usuario. La página de Gobierno lee `yelp_gov`, que solo leen `steward`, `cdo` y `custodio` (E6 `gov_permiso_rol`).
- **FR-013** *(traído de E6)*: Tras publicar el dashboard, se completa la captura de linaje «gold → dashboard» de `gld_user_fraud_profile` en `E6_controles_gobierno/img/`.

### Key Entities

- **Requerimiento de información**: stakeholder, pregunta de negocio, KPI, visual que lo responde.
- **Visual**: página, fuente (gold/gobierno), stakeholder, requerimiento.
- **Pregunta de prueba de Genie**: texto, consulta de referencia, resultado esperado, resultado obtenido.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100 % de visuales trazados a un requerimiento de la sección 7 (0 visuales huérfanos).
- **SC-002**: Genie responde correctamente 5 de 5 preguntas de negocio en la demo en vivo.
- **SC-003**: 0 fuentes del dashboard fuera de gold/gobierno.
- **SC-004**: La CDO identifica en menos de 1 minuto qué regla de calidad está en rojo y qué área es responsable.
- **SC-005**: ≥ 6 visuales en total entre ambas páginas.

## Assumptions

- Estado actual: solo existen consultas base de plantilla; varias apuntan a tablas de ejemplo (`gld_kpi_negocio_mensual`) que no forman parte del gold vigente. Dashboard y Genie están pendientes.
- La sección 7 del Documento Formal está pendiente de redactar; es dependencia previa para trazar los visuales.
- Depende de E6 para la matriz RACI consultable y el enmascaramiento, y de E7.4 para la bitácora.
- Stakeholders del caso: Producto (Andrew Collins), Marketing (Jessica Martinez), Operaciones Comerciales (Carla Romero), Trust & Safety (Rafael Nakamura), Data Science (Mei), CDO (Laura Hernández).
