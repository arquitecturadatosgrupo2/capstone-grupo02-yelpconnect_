# Feature Specification: E7.4 · Operación, monitoreo y FinOps

**Feature Branch**: `feature/E7-operacion-finops`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E7.4 (Job orquestado y FinOps, M10 DataOps/MLOps/FinOps).

**Trazabilidad**: implementa la sección 8 «Modelo operativo y FinOps» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Pipeline completo sin intervención manual (Priority: P1)

El Ingeniero de datos necesita que el pipeline deje de ser «notebooks que alguien corre a mano» y se ejecute como un job versionado con dependencias explícitas entre tareas, de modo que una sola orden lo corra de principio a fin.

**Why this priority**: es el Definition of Done de M10 y la base de la sustentación final integradora.

**Independent Test**: lanzar el job una vez y verificar que todas las tareas terminan en el orden del grafo sin intervención.

**Acceptance Scenarios**:

1. **Given** el job desplegado desde el repositorio, **When** se lanza, **Then** todas las tareas terminan con éxito respetando las dependencias (bronze → silver → gate → gold → monitoreo).
2. **Given** un cambio en la configuración del job, **When** se propone, **Then** entra por Pull Request como cualquier notebook.

---

### User Story 2 - Fallas controladas, reintentos y alertas (Priority: P1)

La CDO necesita evidencia de que el job maneja fallas: cada tarea reintenta ante errores transitorios, tiene tiempo máximo, y quien opera recibe una alerta por correo; una falla de calidad bloquea gold sin reintentos inútiles.

**Independent Test**: ejecutar el job con la falla controlada del gate y verificar estado de tareas, alerta recibida y registro en bitácora; luego re-ejecutar sin forzar y verificar éxito.

**Acceptance Scenarios**:

1. **Given** la falla controlada activada, **When** corre el job, **Then** el gate falla, las tareas gold quedan sin ejecutar, llega la alerta y la bitácora registra la corrida.
2. **Given** un error transitorio en una tarea, **When** ocurre, **Then** la tarea se reintenta automáticamente una vez.
3. **Given** una tarea que excede su tiempo máximo, **When** se alcanza el límite, **Then** se detiene y alerta.

---

### User Story 3 - Bitácora de operación y consumo de cuota (Priority: P1)

El equipo necesita una bitácora que registre cada corrida (tarea, estado, filas, duración) desde el principio, porque es parte del entregable de FinOps y no se puede reconstruir al final.

**Independent Test**: consultar la bitácora y contar corridas, incluyendo al menos una fallida.

**Acceptance Scenarios**:

1. **Given** varias corridas del job, **When** se consulta la bitácora, **Then** hay ≥ 3 corridas registradas, al menos una exitosa y una con falla controlada, con tarea, estado, filas y duración.
2. **Given** una corrida en la que falla una tarea, **When** termina, **Then** la tarea de monitoreo igual se ejecuta y la registra.

---

### User Story 4 - Estimación de costo y cadencia justificada (Priority: P2)

La CDO necesita saber cuánto costaría operar el pipeline en un workspace pagado y por qué se eligió la cadencia de ejecución, para decidir su escalamiento.

**Independent Test**: revisar en el notebook y en la sección 8 del documento la estimación mensual con sus supuestos y la justificación de la cadencia.

**Acceptance Scenarios**:

1. **Given** las duraciones registradas en la bitácora, **When** se calcula el costo, **Then** hay una estimación mensual con supuestos explícitos (frecuencia, duración, tarifa de referencia).
2. **Given** la cadencia diaria, **When** se revisa, **Then** está justificada frente a la alternativa por llegada de archivos, y en desarrollo permanece pausada.

### Edge Cases

- La cuota diaria se agota durante una corrida: la bitácora refleja la falla y la siguiente corrida completa sin duplicar.
- El CLI usado para desplegar no lee el estado del bundle: el procedimiento de despliegue documentado indica la versión compatible.
- La tarea de monitoreo falla: no debe enmascarar el estado real de las tareas de negocio.
- El schedule queda activo por error en desarrollo: debe detectarse antes de consumir cuota.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El job DEBE definirse como código en el repositorio y desplegarse desde él, sin configuración manual en la interfaz.
- **FR-002**: El job DEBE componerse de tareas con dependencias explícitas, incluyendo el gate de calidad entre silver y gold.
- **FR-003**: Cada tarea DEBE tener reintento ante falla (excepto el gate) y tiempo máximo; la corrida completa DEBE tener tiempo máximo global.
- **FR-004**: Toda falla DEBE notificar por correo a quien opera el job.
- **FR-005**: Cada corrida DEBE registrarse en la bitácora de operación con tarea, estado, filas y duración, aun si fallan tareas previas.
- **FR-006**: El job DEBE permitir una falla controlada a demanda.
- **FR-007**: La cadencia programada DEBE estar justificada y pausada en desarrollo.
- **FR-008**: El entregable DEBE incluir una estimación de costo mensual en workspace pagado con supuestos explícitos.
- **FR-009**: El notebook DEBE incluir evidencia (capturas) del grafo, de una corrida exitosa y de la falla controlada con su reintento.
- **FR-010**: Toda nueva tarea DEBE estimar su impacto en la cuota diaria antes de mergearse.

### Key Entities

- **Tarea del job**: nombre, dependencias, reintentos, tiempo máximo, notificación.
- **Registro de bitácora**: corrida, tarea, estado, filas procesadas, duración, timestamp.
- **Estimación FinOps**: frecuencia, duración media, tarifa de referencia, costo mensual.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: El pipeline completo corre con una sola orden, 0 pasos manuales.
- **SC-002**: ≥ 3 corridas en bitácora, con ≥ 1 exitosa y ≥ 1 falla controlada seguida de recuperación.
- **SC-003**: Alerta de falla recibida en menos de 5 minutos desde la falla.
- **SC-004**: La corrida completa termina dentro de su tiempo máximo global (2 h) y dentro de la cuota diaria.
- **SC-005**: Estimación de costo mensual presentada con todos sus supuestos y trazada a la sección 8 del documento.

## Assumptions

- Estado actual: job de 14 tareas con reintentos, timeouts, alertas, schedule pausado y tarea de bitácora ya en `main`; faltan ≥ 3 corridas registradas, capturas y la estimación de costo.
- Tarifa de referencia: precio de lista público de cómputo serverless vigente a la fecha de la estimación.
- Despliegue con el CLI de Databricks incluido en la extensión de VS Code (compatible con el estado del bundle); ninguna credencial se versiona.
