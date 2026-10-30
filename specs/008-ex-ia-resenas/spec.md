# Feature Specification: EX · Bonus de IA sobre reseñas

**Feature Branch**: `feature/EX-ia-resenas`

**Created**: 2026-10-24

**Status**: Draft

**Input**: User description: "Para cada entregable de este proyecto, genera las specs necesarias" — entregable EX (Bonus IA, M8 IA y Datos No Estructurados, +5 %).

**Trazabilidad**: implementa la sección «Casos de uso analíticos avanzados» del Documento Formal (a incorporar en la sección 3 o como anexo).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reseñas enriquecidas como dato gobernado (Priority: P1)

Trust & Safety y Producto necesitan una muestra acotada de reseñas enriquecida con sentimiento y tema (comida, servicio, precio, ambiente, otro), persistida como tabla catalogada con descripción, Data Owner, clasificación y linaje, para tratar la salida del modelo como cualquier otro dato gobernado.

**Why this priority**: lo evaluado no es que el modelo acierte sino que su salida se gobierne; es el mínimo para obtener el bonus.

**Independent Test**: ejecutar el notebook y verificar la tabla resultante: tamaño de muestra, descripción, etiquetas y linaje hacia silver.

**Acceptance Scenarios**:

1. **Given** las reseñas de silver, **When** se ejecuta el enriquecimiento, **Then** existe una tabla gold con ≤ 500 reseñas, cada una con sentimiento y tema.
2. **Given** la tabla enriquecida, **When** se inspecciona en el catálogo, **Then** tiene descripción, Data Owner, clasificación, figura en el diccionario y muestra linaje hasta silver.

---

### User Story 2 - Señal de fraude por inconsistencia rating ↔ sentimiento (Priority: P2)

Trust & Safety necesita una lista de reseñas cuya calificación contradice su sentimiento (5 estrellas con sentimiento negativo, o 1 estrella con positivo) como candidatas a revisión, con conclusiones y un visual en el dashboard.

**Independent Test**: consultar las reseñas inconsistentes y revisar manualmente una muestra.

**Acceptance Scenarios**:

1. **Given** la tabla enriquecida, **When** se aplica el criterio de inconsistencia, **Then** se obtiene la lista de candidatas con su proporción sobre la muestra.
2. **Given** la lista, **When** se presenta, **Then** incluye conclusiones accionables para Trust & Safety y un visual en E8.

---

### User Story 3 - Lectura crítica de los errores del modelo (Priority: P2)

La CDO necesita una evaluación honesta de la calidad del modelo: precisión estimada contra una validación manual, ejemplos de errores típicos y una reflexión de gobierno de IA (riesgos de persistir salidas de un modelo, sesgos, cuándo no confiar en él).

**Independent Test**: etiquetar manualmente una submuestra y comparar con la salida del modelo.

**Acceptance Scenarios**:

1. **Given** una submuestra etiquetada manualmente, **When** se compara con el modelo, **Then** se reporta el % de acierto por sentimiento y tema.
2. **Given** los errores encontrados, **When** se documentan, **Then** se clasifican en tipos (sarcasmo, reseñas mixtas, idioma, etc.) con ejemplos.

### Edge Cases

- Reseñas vacías, muy largas o en otro idioma: se registran con resultado nulo o «otro», no rompen la ejecución.
- Las funciones de IA no están disponibles o se agota la cuota: el entregable se marca como no ejecutado sin afectar a los entregables base ni al job.
- Re-ejecución: la muestra debe ser reproducible (misma selección) para que la evaluación sea comparable.
- Texto de reseñas con datos personales: la tabla enriquecida no debe ampliar la exposición más allá de la clasificación vigente.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El entregable DEBE enriquecer una muestra reproducible de ≤ 500 reseñas con sentimiento y tema.
- **FR-002**: El resultado DEBE persistirse como tabla gold catalogada con descripción, Data Owner, clasificación y presencia en el diccionario.
- **FR-003**: El linaje de la tabla enriquecida hasta silver DEBE ser visible en el catálogo.
- **FR-004**: El entregable DEBE identificar reseñas con inconsistencia rating ↔ sentimiento y presentar conclusiones para Trust & Safety.
- **FR-005**: El entregable DEBE reportar una evaluación del modelo contra una validación manual y una reflexión escrita de gobierno de IA.
- **FR-006**: El enriquecimiento NO DEBE formar parte del job obligatorio ni bloquear entregables base.
- **FR-007**: El consumo de cuota del enriquecimiento DEBE estimarse antes de ejecutarse.

### Key Entities

- **Reseña enriquecida**: reseña, negocio, calificación, sentimiento, tema, fecha de enriquecimiento.
- **Evaluación del modelo**: submuestra, etiqueta manual, etiqueta del modelo, acierto, tipo de error.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Tabla enriquecida con ≤ 500 reseñas, 100 % con descripción y clasificación, linaje visible.
- **SC-002**: Evaluación sobre una submuestra manual de ≥ 50 reseñas con % de acierto reportado.
- **SC-003**: Lista de candidatas a revisión con conclusiones y 1 visual en el dashboard.
- **SC-004**: 0 impacto en el job base (el pipeline obligatorio corre igual si EX no se ejecuta).

## Assumptions

- Estado actual: existe la consulta de enriquecimiento de plantilla; faltan el análisis de inconsistencia, la evaluación crítica, la documentación en E4 y el visual en E8.
- Es opcional (+5 %) y se aborda después de E6 y E8.
- Las funciones de IA de la edición gratuita pueden cambiar; la muestra aleatoria se fija con una semilla para ser reproducible.
