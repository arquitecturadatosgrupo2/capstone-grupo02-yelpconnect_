# Feature Specification: E6 · Controles técnicos de gobierno

**Feature Branch**: `feature/E6-controles-gobierno`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E6 (Controles técnicos, M2 diseño · M5 implementación).

**Trazabilidad**: implementa la sección 5 «Estrategia de gobierno y matriz RACI» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Acceso por rol con mínimo privilegio (Priority: P1)

La CDO necesita que cada rol del caso (simulado por un integrante del grupo) acceda solo a las capas que le corresponden según la matriz RACI: el pipeline escribe bronze, analistas y stewards leen silver, consumidores de negocio leen solo gold. Un rol sin permisos otorgados es gobierno declarativo, no efectivo.

**Why this priority**: es el control que se demuestra en vivo en M5 («demostración de permisos actuando los roles») y pesa en «Plataforma: gobierno y calidad» (15 %).

**Independent Test**: cada integrante inicia sesión con su rol e intenta leer bronze, silver y gold; se registra qué puede y qué no.

**Acceptance Scenarios**:

1. **Given** el usuario que actúa Marketing, **When** intenta leer una tabla bronze, **Then** el acceso se deniega.
2. **Given** el usuario que actúa Marketing, **When** consulta un producto gold, **Then** obtiene resultados.
3. **Given** la matriz RACI del documento, **When** se compara con los permisos otorgados en el catálogo, **Then** coinciden rol por rol y capa por capa.

---

### User Story 2 - Datos personales enmascarados (Priority: P1)

El Steward necesita que las columnas clasificadas como `confidencial` o `restringido` (nombres de usuarios y otros datos personales) aparezcan enmascaradas para todo rol no autorizado, y en claro solo para el rol autorizado, tanto en gold como en el dashboard.

**Why this priority**: «ningún acceso en claro fuera del rol autorizado» es el Definition of Done de M5 y una restricción de la constitución.

**Independent Test**: ejecutar la misma consulta sobre datos personales con dos usuarios de roles distintos y comparar los resultados.

**Acceptance Scenarios**:

1. **Given** un usuario sin rol autorizado, **When** consulta una columna personal, **Then** ve el valor enmascarado.
2. **Given** el usuario con rol autorizado (steward), **When** ejecuta la misma consulta, **Then** ve el valor en claro.
3. **Given** cualquier objeto consumido por el dashboard, **When** se inspeccionan sus columnas, **Then** ninguna columna personal se expone sin máscara.

---

### User Story 3 - Clasificación que gobierna los controles (Priority: P2)

La CDO necesita que las etiquetas de clasificación provenientes del diccionario (E4) sean la base de qué se enmascara y quién accede, de modo que reclasificar una columna cambie su control sin reescribir políticas a mano.

**Independent Test**: listar las columnas etiquetadas como `confidencial`/`restringido` y verificar que todas tienen máscara; las demás no.

**Acceptance Scenarios**:

1. **Given** las etiquetas aplicadas, **When** se listan las columnas `confidencial` o `restringido`, **Then** el 100 % tiene control de enmascaramiento.
2. **Given** los schemas, **When** se inspeccionan, **Then** tienen etiquetas de capa y dominio.

---

### User Story 4 - Linaje de extremo a extremo (Priority: P2)

Trust & Safety necesita ver el linaje de al menos un producto de datos desde el dato crudo hasta el dashboard (raw → bronze → silver → gold → visual), respondiendo a su dolor «reglas de fraude sin linaje claro».

**Independent Test**: abrir el linaje del producto de fraude en el catálogo y verificar que se ven todos los saltos.

**Acceptance Scenarios**:

1. **Given** un producto gold de Trust & Safety, **When** se abre su linaje, **Then** se ven sus orígenes silver y bronze y su consumo en el dashboard.
2. **Given** la evidencia, **When** se revisa el notebook, **Then** incluye capturas del linaje y del intento de acceso denegado.

### Edge Cases

- La plataforma gratuita no permite crear grupos de cuenta: los roles se modelan con usuarios individuales y la limitación queda documentada como decisión.
- La función nativa de máscara de columna no está disponible: se usan vistas seguras y se revoca el acceso directo a la tabla base para los roles no autorizados.
- Un integrante cambia de rol: se revocan sus permisos previos antes de otorgar los nuevos.
- Una columna nueva con datos personales se añade en E3: queda sin máscara hasta que el diccionario la clasifique; el validador de cobertura debe detectarlo.
- El usuario que despliega el job necesita permisos de escritura en todas las capas sin que eso se extienda a los roles de consumo.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Los permisos DEBEN otorgarse por rol simulado siguiendo la matriz RACI del Documento Formal y el principio de mínimo privilegio.
- **FR-002**: Ningún rol de consumo de negocio DEBE poder leer bronze.
- **FR-003**: Toda columna clasificada `confidencial` o `restringido` DEBE mostrarse enmascarada para los roles no autorizados y en claro solo para el autorizado.
- **FR-004**: Los controles de enmascaramiento DEBEN derivarse de la etiqueta `clasificacion` aplicada desde el diccionario.
- **FR-005**: Los schemas DEBEN llevar etiquetas de capa y dominio.
- **FR-006**: El entregable DEBE producir evidencia verificable: listado de permisos, comparación de una misma consulta con dos roles, intento de acceso denegado y linaje de extremo a extremo de al menos un producto.
- **FR-007**: Los controles DEBEN ser re-ejecutables sin error (otorgar/revocar idempotente).
- **FR-008**: La matriz RACI DEBE existir también como tabla consultable de gobierno para alimentar el indicador de ownership de E8.
- **FR-009**: Toda limitación de la plataforma que impida el diseño ideal DEBE documentarse como decisión en el notebook y en la sección 5 del documento.

### Key Entities

- **Rol del caso**: CDO, Producto, Trust & Safety, Marketing, Operaciones Comerciales, Data Science; mapeado a un integrante.
- **Permiso**: rol × capa/objeto × privilegio.
- **Política de enmascaramiento**: clasificación → tratamiento por rol.
- **Matriz RACI**: entidad × rol × responsabilidad (R, A, C, I).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 0 accesos en claro a columnas `confidencial`/`restringido` fuera del rol autorizado, verificados con los 5 integrantes.
- **SC-002**: 100 % de coincidencia entre permisos otorgados y matriz RACI.
- **SC-003**: 100 % de columnas `confidencial`/`restringido` con enmascaramiento.
- **SC-004**: Linaje visible de extremo a extremo para ≥ 1 producto de datos.
- **SC-005**: La demo de permisos en vivo se completa en menos de 10 minutos sin pasos improvisados.

## Assumptions

- Estado actual: tags `clasificacion` aplicados desde E4; GRANT, enmascaramiento, matriz RACI como tabla y evidencia de linaje están pendientes (el notebook sigue siendo plantilla). Es el siguiente entregable crítico.
- Los 5 integrantes están invitados como usuarios del workspace del grupo; el Steward de gobierno actúa el rol autorizado para ver datos personales.
- La sección 5 del Documento Formal (Libro de Gobierno / RACI) debe actualizarse con la asignación de Data Owners del 2026-10-23.
