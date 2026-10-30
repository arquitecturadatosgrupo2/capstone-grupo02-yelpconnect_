# Feature Specification: E4 · Glosario y Diccionario operativos

**Feature Branch**: `feature/E4-glosario-diccionario`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable E4 (Glosario y Diccionario, M3 MDM y Metadata).

**Trazabilidad**: implementa los Anexos «Glosario de términos de negocio» y «Diccionario de datos» del Documento Formal.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Un término, una definición, un dueño (Priority: P1)

Las áreas del caso (Marketing, Operaciones Comerciales, Trust & Safety, Producto, Data Science, CDO) llaman igual a cosas distintas («check-in», «reseña válida», «negocio activo»). El Steward de gobierno necesita un glosario con una única definición acordada por término, escrita para que la entienda alguien de Marketing, con un propietario que sea un área real del caso, y consultable desde la plataforma.

**Why this priority**: resuelve el dolor central del caso y alimenta los sinónimos de Genie (E8).

**Independent Test**: cargar el Excel institucional del glosario y ejecutar los validadores; verificar conteo, duplicados, dominios y propietarios.

**Acceptance Scenarios**:

1. **Given** el Excel del glosario completado en la plantilla institucional, **When** se carga a la plataforma, **Then** la tabla consultable tiene exactamente las 12 columnas de la plantilla y una fila por término.
2. **Given** el glosario cargado, **When** corren los validadores, **Then** hay ≥ 20 términos, 0 duplicados, ≥ 4 dominios y 0 términos sin propietario válido.
3. **Given** un término cualquiera, **When** se lee su definición de negocio, **Then** no menciona nombres de tablas ni columnas (eso va en la definición técnica).

---

### User Story 2 - Diccionario con cobertura total (Priority: P1)

El Steward necesita que el 100 % de las columnas de silver y gold figure en el diccionario con responsable, clasificación de sensibilidad y política de retención, partiendo de un borrador técnico heredado del catálogo y completado con metadata de negocio acordada por las áreas.

**Why this priority**: es el criterio de «cobertura de documentación» evaluado y la fuente de las clasificaciones que E6 convierte en controles.

**Independent Test**: cruzar las columnas del catálogo (silver y gold) contra las filas del diccionario cargado.

**Acceptance Scenarios**:

1. **Given** el diccionario cargado, **When** se cruza con las columnas existentes de silver y gold, **Then** el 100 % aparece y no hay filas que apunten a columnas inexistentes.
2. **Given** cualquier fila del diccionario, **When** se revisa, **Then** tiene responsable, clasificación (`publico | interno | confidencial | restringido`) y política de retención.
3. **Given** una regla citada como `DQ-nnn` en el diccionario, **When** se busca en el catálogo de reglas, **Then** existe.

---

### User Story 3 - Clasificación con efecto técnico (Priority: P2)

La CDO necesita que la clasificación declarada en el diccionario se aplique como etiqueta real sobre cada columna del catálogo, de modo que gobierne permisos y enmascaramiento (E6).

**Independent Test**: comparar la clasificación de cada columna en el diccionario con la etiqueta aplicada en el catálogo.

**Acceptance Scenarios**:

1. **Given** el diccionario cargado, **When** se aplican las etiquetas, **Then** el 100 % de las columnas tiene la etiqueta `clasificacion` igual a la del diccionario.

---

### User Story 4 - Golden record de negocios (Priority: P2)

La Modeladora (Producto) necesita demostrar un registro maestro único y confiable por negocio (golden record): una sola versión vigente por negocio con reglas de supervivencia explícitas cuando hay datos duplicados o en conflicto, porque la rúbrica evalúa «golden record demostrado».

**Why this priority**: es parte explícita del Definition of Done de M3 y hoy no tiene evidencia en el repositorio.

**Independent Test**: tomar un negocio con registros duplicados o en conflicto en origen y mostrar que el golden record conserva una sola versión, indicando qué regla decidió cada atributo.

**Acceptance Scenarios**:

1. **Given** negocios con registros repetidos o atributos en conflicto, **When** se construye el golden record, **Then** existe exactamente un registro por negocio.
2. **Given** un atributo en conflicto, **When** se consulta el golden record, **Then** se puede explicar qué regla de supervivencia lo resolvió y de qué fuente provino.

### Edge Cases

- Un término con sinónimos que colisionan con otro término: se resuelve en el glosario y se documenta en `relaciones`.
- El Excel se sube con nombre distinto al esperado: la carga falla con mensaje claro indicando la ruta y nombre esperados.
- Se agrega una columna nueva en E3 sin actualizar el diccionario: el validador de cobertura falla y la nombra.
- Cambio de Data Owner: se refleja a la vez en E3, E5, glosario, diccionario y RACI.
- Clasificaciones con tilde u otra grafía: se normalizan a los cuatro valores sin tilde.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El glosario y el diccionario DEBEN completarse en las plantillas institucionales y cargarse sin traducir ni renombrar columnas (12 y 25 columnas).
- **FR-002**: El Excel anexado al Documento Formal y el cargado en la plataforma DEBEN ser el mismo archivo, versionado en el repositorio.
- **FR-003**: El glosario DEBE tener ≥ 20 términos únicos que cubran ≥ 4 dominios de datos.
- **FR-004**: Todo término DEBE tener un propietario correspondiente a un área real del caso.
- **FR-005**: Todo término de negocio usado en un producto de datos gold DEBE existir en el glosario.
- **FR-006**: El diccionario DEBE cubrir el 100 % de las columnas de silver y gold, cada una con responsable, clasificación y retención.
- **FR-007**: La parte técnica del diccionario (nombre físico, tipo, nulidad, clave) DEBE heredarse del catálogo, no redactarse a mano.
- **FR-008**: Toda regla `DQ-nnn` citada en glosario o diccionario DEBE existir en el catálogo de reglas de E5.
- **FR-009**: La clasificación de cada columna DEBE aplicarse como etiqueta `clasificacion` en el catálogo.
- **FR-010**: El entregable DEBE demostrar un golden record de negocios con reglas de supervivencia documentadas.
- **FR-011**: Validadores automáticos DEBEN verificar FR-003, FR-004, FR-006, FR-008 y FR-009 y fallar con mensaje explícito.

### Key Entities

- **Término de negocio**: término, sinónimos, definición de negocio y técnica, dominio, categoría, propietario, fuente normativa, fecha de actualización.
- **Elemento del diccionario**: columna física con nombre de negocio, descripción, dominio, sistema fuente, tipo, obligatoriedad, sensibilidad, responsable, retención, reglas DQ asociadas.
- **Golden record de negocio**: registro maestro único por negocio con trazabilidad de la fuente y regla que fijó cada atributo.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: ≥ 20 términos, ≥ 4 dominios, 0 duplicados, 0 términos sin propietario.
- **SC-002**: 100 % de columnas de silver y gold documentadas con responsable, clasificación y retención.
- **SC-003**: 0 reglas `DQ-nnn` citadas que no existan en el catálogo de reglas.
- **SC-004**: 100 % de coincidencia entre clasificación del diccionario y etiqueta aplicada.
- **SC-005**: Exactamente 1 golden record por negocio, con su regla de supervivencia explicable en la demo en vivo.
- **SC-006**: Un usuario de Marketing entiende la definición de negocio de 9 de cada 10 términos sin ayuda técnica (revisión cruzada).

## Assumptions

- Estado actual: glosario v2 (31 términos) y diccionario (152 columnas) cargados y etiquetados en `main`; el golden record de negocios no tiene evidencia y es la brecha principal.
- El golden record se construye sobre la entidad negocio, cuyo Data Owner es Producto.
- Las definiciones se negocian con las áreas del caso representadas por los integrantes; el acuerdo se refleja en el Excel antes de cargarlo.
