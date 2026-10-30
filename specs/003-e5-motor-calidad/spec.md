# Feature Specification: E5 · Motor de reglas de calidad y gate

**Feature Branch**: `feature/E5-reglas-calidad`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E5 (Motor de calidad, definición M2/M3 · ejecución M6).

**Trazabilidad**: implementa la sección 6 «Reglas de calidad en lenguaje de negocio» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reglas como datos, evaluadas por un motor genérico (Priority: P1)

El Steward de gobierno y calidad (rol de Rafael Nakamura, Trust & Safety) necesita que las reglas de calidad vivan como registros de un catálogo —no dispersas en el código— y que un motor genérico las lea, las evalúe sobre silver y guarde cada resultado con fecha.

**Why this priority**: es el núcleo del entregable y la condición NO NEGOCIABLE de la constitución (principio III).

**Independent Test**: añadir una regla nueva al catálogo y ejecutar el motor sin tocar el código; verificar que aparece su resultado en el historial.

**Acceptance Scenarios**:

1. **Given** el catálogo de reglas, **When** se ejecuta el motor, **Then** cada regla genera un resultado con fecha, porcentaje de cumplimiento, filas evaluadas y si pasó o no.
2. **Given** una regla nueva agregada como registro, **When** corre el motor, **Then** se evalúa sin cambios de código.
3. **Given** el catálogo, **When** se inspecciona, **Then** hay ≥ 12 reglas que cubren las 6 dimensiones (completitud, unicidad, validez, consistencia, exactitud, oportunidad).

---

### User Story 2 - Gate que impide publicar gold con datos inválidos (Priority: P1)

La CDO necesita que, si una regla de criticidad alta no alcanza su umbral, los productos gold no se publiquen y llegue una alerta; las de criticidad media se registran y reportan sin detener.

**Why this priority**: publicar un producto de Trust & Safety con datos inválidos es peor que no publicarlo.

**Independent Test**: forzar una regla alta imposible de cumplir y ejecutar el job; verificar que gold no se ejecuta, llega la alerta y la bitácora lo registra.

**Acceptance Scenarios**:

1. **Given** una regla alta bajo su umbral, **When** corre el gate, **Then** la tarea falla y las tareas gold no se ejecutan.
2. **Given** solo reglas medias bajo su umbral, **When** corre el gate, **Then** el pipeline continúa y los incumplimientos quedan registrados.
3. **Given** la prueba de falla controlada, **When** se vuelve a correr sin forzar, **Then** el gate pasa y gold se publica.

---

### User Story 3 - Umbrales defendibles y gobernados (Priority: P2)

El Steward necesita que cada regla declare dimensión, criticidad, umbral, justificación del umbral (qué pasa con el porcentaje restante), acción si falla y Data Owner, y que cambiar un umbral requiera aprobación del Data Owner.

**Independent Test**: revisar el catálogo y comprobar que ningún campo obligatorio está vacío; revisar el registro de aprobaciones de los umbrales pendientes.

**Acceptance Scenarios**:

1. **Given** cualquier regla, **When** se revisa, **Then** tiene los seis atributos de gobierno completos y coinciden con el texto de la sección 6 del documento.
2. **Given** un cambio de umbral, **When** se propone, **Then** queda registrada la aprobación del Data Owner antes de aplicarse.

---

### User Story 4 - Historial que demuestra evolución (Priority: P2)

La CDO necesita ver el cumplimiento a lo largo del tiempo (≥ 2 ejecuciones en fechas distintas) para demostrar si la calidad mejora, y ese historial alimenta la página de gobierno de E8.

**Independent Test**: consultar el historial y contar fechas de ejecución distintas.

**Acceptance Scenarios**:

1. **Given** el historial, **When** se agrupa por fecha de ejecución, **Then** hay ≥ 2 fechas distintas con resultados de todas las reglas.

### Edge Cases

- La tabla objetivo de una regla no existe o está vacía: el resultado se registra como fallo explícito, no se omite.
- Una regla tiene una expresión inválida: el motor registra el error de esa regla y sigue evaluando el resto, y el gate la trata según su criticidad.
- Riesgo de calidad aceptado (p. ej. reseñas de autores fuera de la muestra): se documenta con impacto cuantificado y firma del Data Owner; la regla nunca falla en silencio.
- Reglas de prueba (p. ej. la de falla controlada) no deben contaminar el scorecard de gobierno ni reutilizar códigos `DQ-nnn` productivos.
- Re-ejecución el mismo día: no debe duplicar el catálogo de reglas; el historial sí acumula corridas.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Las reglas DEBEN almacenarse como registros de un catálogo de gobierno, con código `DQ-nnn` correlativo y nunca reutilizado.
- **FR-002**: Cada regla DEBE declarar tabla y columna objetivo, dimensión, descripción en lenguaje de negocio, criticidad (alta/media), umbral, justificación del umbral, acción si falla y Data Owner.
- **FR-003**: El catálogo DEBE contener ≥ 12 reglas cubriendo las 6 dimensiones de calidad.
- **FR-004**: Un motor genérico DEBE evaluar todas las reglas del catálogo sobre silver y persistir un resultado por regla y ejecución.
- **FR-005**: El gate DEBE ejecutarse entre silver y gold y fallar si cualquier regla alta queda bajo su umbral.
- **FR-006**: Las reglas medias bajo umbral DEBEN registrarse y reportarse sin detener el pipeline.
- **FR-007**: El job DEBE permitir una falla controlada del gate a demanda, sin modificar el catálogo productivo.
- **FR-008**: El historial DEBE conservar todas las ejecuciones (≥ 2 en fechas distintas para la entrega).
- **FR-009**: Las reglas en el catálogo DEBEN coincidir en texto, umbral y criticidad con la sección 6 del Documento Formal.
- **FR-010**: Los umbrales pendientes de aprobación (reglas 6 y 9) DEBEN quedar aprobados o explícitamente aceptados por Trust & Safety antes de la entrega.

### Key Entities

- **Regla de calidad**: código, objetivo, dimensión, expresión de cumplimiento, criticidad, umbral, justificación, acción, Data Owner.
- **Resultado de calidad**: regla, timestamp de ejecución, filas evaluadas, filas que cumplen, % de cumplimiento, pasó/no pasó.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: ≥ 12 reglas activas, 6 de 6 dimensiones cubiertas.
- **SC-002**: ≥ 2 ejecuciones del motor en fechas distintas registradas.
- **SC-003**: En la falla controlada, 0 tablas gold se actualizan y la alerta llega en menos de 5 minutos.
- **SC-004**: 100 % de reglas con los seis atributos de gobierno completos y alineadas con el documento.
- **SC-005**: Agregar una regla nueva toma menos de 10 minutos y 0 cambios de código.

## Assumptions

- Estado actual: 19 reglas y el gate están en `main` (PR #8); faltan ≥ 2 ejecuciones históricas en fechas distintas, la aprobación de umbrales de las reglas 6 y 9 y la redacción de la sección 6 del documento.
- Las alertas se envían por correo a quien despliega el job.
- Las reglas evalúan silver; gold se protege por el gate, no por reglas propias.
