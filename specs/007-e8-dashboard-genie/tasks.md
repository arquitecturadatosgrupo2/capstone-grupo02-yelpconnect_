---

description: "Lista de tareas de E8 · Dashboard BI y Genie"
---

# Tasks: E8 · Dashboard BI y Genie

**Input**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [contracts/visuales.md](contracts/visuales.md), [seccion7-requerimientos.md](seccion7-requerimientos.md)

**Tests**: los validadores de cada dataset y las pruebas de Genie son parte de la implementación (FR-007, SC-002).

## Phase 1: Setup

- [X] T001 Crear `resources/capstone_yelp.dashboard.yml` con el recurso `dashboards.yelpconnect` (`display_name: YelpConnect · Negocio y Gobierno`, `file_path: ../E8_dashboard_bi/dashboard_yelpconnect.lvdash.json`, `warehouse_id: 656cbc31df3fbcc2`, `embed_credentials: false`)

## Phase 2: Foundational

- [X] T002 Reescribir `E8_dashboard_bi/01_consultas_base_dashboard.py`: cabecera de Trazabilidad (sección 7), una celda por dataset con la consulta exacta que usa el dashboard (`ds_rafagas`, `ds_riesgo`, `ds_resenas`, `ds_resenas_categoria`, `ds_checkins`, `ds_calidad_ultima`, `ds_calidad_historia`, `ds_cobertura`, `ds_alertas`, `ds_pipeline`) y validadores que cruzan cada total con una consulta directa a la tabla de origen; corregir `descripcion_columna` → `descripcion_negocio` (FR-011)

## Phase 3: User Story 1 - Página de Gobierno (P1) 🎯 MVP

- [X] T003 [US1] Página «Gobierno» en `E8_dashboard_bi/dashboard_yelpconnect.lvdash.json` con los visuales G1–G7 de [contracts/visuales.md](contracts/visuales.md); cada descripción nombra REQ y stakeholder; `DQ-900` excluida (research R5)

## Phase 4: User Story 2 - Página de Negocio (P1)

- [X] T004 [US2] Página «Negocio» en `E8_dashboard_bi/dashboard_yelpconnect.lvdash.json` con N1–N10 y filtros de mes, ciudad y categoría conectados a todos los datasets de la página (FR-004)
- [X] T005 [US2] Desplegar con `bundle deploy -t dev`, verificar con `lakeview get` que el dashboard publicado tiene 2 páginas y 17 visuales y que `embed_credentials` es falso; corregir los widgets que no rendericen — *Desplegado y publicado (2 páginas, `embed_credentials: false`); la revisión visual de cada widget queda para la UI.*

## Phase 5: User Story 3 - Genie (P1)

- [X] T006 [US3] Crear `E8_dashboard_bi/genie_space.json` (serialized_space v2): tablas gold y de gobierno, instrucciones con ≥ 5 sinónimos del glosario, SQL de ejemplo y benchmarks G1–G5 con su SQL de referencia
- [X] T007 [US3] Crear `E8_dashboard_bi/02_genie_space.py`: crea o actualiza el espacio (idempotente por título) desde el JSON con la API y muestra su `space_id`
- [X] T008 [US3] Crear el espacio en el workspace y plantear G1–G5 con `genie ask`; comparar cada respuesta con el SQL de referencia; ajustar instrucciones si alguna falla y registrar la iteración (FR-008) y el resultado 5/5 en `02_genie_space.py`

## Phase 6: User Story 4 - Consumo solo desde capas publicadas (P2)

- [X] T009 [US4] Validador en `01_consultas_base_dashboard.py` que lee el JSON del dashboard y el del espacio Genie y comprueba que toda tabla referenciada está en `yelp_gold` o `yelp_gov` (SC-003)

## Phase 7: Polish

- [X] T010 Programación de refresco diaria 07:00 America/Lima, en pausa en dev (FR-010), documentada en el notebook — *Decisión: el dashboard lee en vivo; la programación solo calienta la caché, documentada y no creada.*
- [X] T011 Definition of Done y decisiones en `01_consultas_base_dashboard.py`; marcar en los notebooks de gold los checklists «visual asociado en E8»
- [X] T012 Commit, push y PR de `feature/E8-dashboard-genie`

## Dependencies

T001–T002 → US1/US2 (mismo JSON: secuencial) → T005. US3 (T006–T008) es independiente del dashboard. T009 después de T005 y T008.
