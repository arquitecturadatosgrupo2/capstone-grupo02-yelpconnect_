---

description: "Lista de tareas de E6 · Controles técnicos de gobierno"
---

# Tasks: E6 · Controles técnicos de gobierno

**Input**: documentos de diseño en `specs/004-e6-controles-gobierno/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: la spec no pide TDD. Los validadores con `assert` dentro del notebook son **parte de la
implementación** (FR-006, patrón de E4/E5) y van en cada historia.

**Organization**: tareas agrupadas por historia de usuario para implementarlas y probarlas por separado.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: puede ir en paralelo (otro archivo, sin dependencias pendientes)
- **[Story]**: historia a la que pertenece (US1–US4)

## Convenciones de este repo

- Notebooks fuente `.py` de Databricks (`# Databricks notebook source`, `# COMMAND ----------`,
  `# MAGIC %sql`, `# DBTITLE 1,…`), comentarios y COMMENT en español, como en
  `E5_calidad_datos/01_reglas_calidad.py`.
- Notebook de controles: `E6_controles_gobierno/01_controles_unity_catalog.py` (lo ejecuta el
  custodio `mateomirandah@gmail.com`). Notebook de demo: `E6_controles_gobierno/02_demo_roles.py`
  (lo ejecuta cualquier integrante).
- Objetos totalmente calificados: `workspace.yelp_gov.*`. Nada de `display()` sin `LIMIT` (principio VI).
- Toda creación es idempotente: `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, carga con
  overwrite, reconciliación diferencial (FR-007).
- Validación desde el CLI (opcional): `api post /api/2.0/sql/statements` con `MSYS_NO_PATHCONV=1` en Git
  Bash, warehouse `656cbc31df3fbcc2`, perfil `FREE`.

---

## Phase 1: Setup

**Purpose**: estructura de archivos y entrada de roles fuera de Git.

- [ ] T001 [P] Crear `E6_controles_gobierno/artefactos/roles_usuarios_PLANTILLA.csv` con cabecera `usuario,rol,integrante,vigente_desde` y filas de ejemplo con correos ficticios (`usuario-marketing@ejemplo.com`), una por cada rol de `gov_rol` (`cdo`, `steward`, `producto`, `trust_safety`, `marketing`, `operaciones_comerciales`, `data_science`, `custodio`)
- [ ] T002 [P] Agregar `E6_controles_gobierno/artefactos/roles_usuarios.csv` a `.gitignore` (la plantilla sí se versiona) y crear `E6_controles_gobierno/img/.gitkeep`
- [ ] T003 Reescribir la cabecera de `E6_controles_gobierno/01_controles_unity_catalog.py`: título, «Cierra con: M2 + M5», bloque **Trazabilidad** → sección 5 «Estrategia de gobierno y matriz RACI» del Documento Formal, y un índice de las secciones del notebook (Inventario · Roles · Permisos · RACI · Máscaras · Etiquetas · Validadores · Linaje · Decisiones · DoD); eliminar las celdas de plantilla actuales (Control 1–4 con `is_account_group_member`)

---

## Phase 2: Foundational (bloquea todas las historias)

**Purpose**: inventario «antes» y tablas de roles que usan permisos (US1) y máscaras (US2).

**⚠️ CRITICAL**: ninguna historia empieza sin esta fase.

- [ ] T004 Celda «Inventario inicial» en `E6_controles_gobierno/01_controles_unity_catalog.py`: guardar en `workspace.yelp_gov.gov_inventario_permisos` (con `CREATE TABLE IF NOT EXISTS`; columnas `capturado_ts TIMESTAMP, momento STRING ('antes'|'despues'), grantee STRING, objeto STRING, privilegio STRING`) el resultado de `workspace.information_schema.schema_privileges` para `schema_name LIKE 'yelp%'` más `SHOW GRANTS ON CATALOG workspace`, con `momento = 'antes'`; mostrar un resumen `GROUP BY grantee, objeto` (evidencia: `account users` tiene SELECT en `yelp_silver` y USE SCHEMA en `yelp_bronze`, research R11)
- [ ] T005 Celda «Roles» en `E6_controles_gobierno/01_controles_unity_catalog.py`: crear `workspace.yelp_gov.gov_rol` (`rol STRING NOT NULL`, `nombre_rol STRING NOT NULL`, `descripcion STRING`, `tipo STRING NOT NULL` con valores `gobierno · dueno_datos · consumo · tecnico`) con COMMENT de tabla y columnas, y cargarla con overwrite con los 8 roles de [data-model.md](data-model.md); `tipo`: `cdo`/`steward` → gobierno, `producto`/`trust_safety` → dueno_datos, `marketing`/`operaciones_comerciales`/`data_science` → consumo, `custodio` → tecnico
- [ ] T006 Celda «Asignación de roles» en `E6_controles_gobierno/01_controles_unity_catalog.py`: crear `workspace.yelp_gov.gov_rol_usuario` (`usuario STRING NOT NULL`, `rol STRING NOT NULL`, `integrante STRING`, `vigente_desde DATE NOT NULL`; PK lógica (usuario, rol), «un usuario puede tener varios roles») y cargarla con overwrite desde `/Volumes/workspace/yelp_gov/artefactos/roles_usuarios.csv`; asserts: todo `rol` existe en `gov_rol`; «el rol `custodio` tiene exactamente 1 usuario» y es igual a `current_user()` del notebook (el custodio es quien lo ejecuta)
- [ ] T007 Crear el `roles_usuarios.csv` real (fuera de Git) con las filas confirmadas al 2026-10-29: `mateomirandah@gmail.com,steward,Mateo Miranda,2026-10-29` y `mateomirandah@gmail.com,custodio,Mateo Miranda,2026-10-29`, y subirlo a `/Volumes/workspace/yelp_gov/artefactos/roles_usuarios.csv` con el CLI (`fs cp`); el resto de integrantes queda pendiente (decisión diferida D3)
- [ ] T008 Celda «Política de enmascaramiento» en `E6_controles_gobierno/01_controles_unity_catalog.py`: crear `workspace.yelp_gov.gov_politica_mascara` (`clasificacion STRING NOT NULL` con valores `confidencial · restringido`, `rol STRING NOT NULL`, `justificacion STRING NOT NULL`) y cargar con overwrite 6 filas: `confidencial` y `restringido` × `steward`, `trust_safety`, `custodio` (decisión provisional D4; `custodio` es obligatorio, research R5)
- [ ] T009 Celda «Función de autorización» en `E6_controles_gobierno/01_controles_unity_catalog.py`: `CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_autorizado(clasificacion STRING) RETURNS BOOLEAN` con `EXISTS (SELECT 1 FROM gov_rol_usuario u JOIN gov_politica_mascara p ON u.rol = p.rol WHERE u.usuario = current_user() AND p.clasificacion = clasificacion)`; clasificación NULL o desconocida → FALSE («falla cerrada», [contracts/funciones_mascara.md](contracts/funciones_mascara.md)); COMMENT en español; assert `fn_autorizado('confidencial')` = TRUE para el custodio

**Checkpoint**: tablas `gov_rol`, `gov_rol_usuario`, `gov_politica_mascara`, `gov_inventario_permisos` y `fn_autorizado` existen; las historias pueden empezar.

---

## Phase 3: User Story 1 - Acceso por rol con mínimo privilegio (Priority: P1) 🎯 MVP

**Goal**: los GRANT de los 4 schemas coinciden con la matriz rol × capa y con la RACI.

**Independent Test**: el validador de permisos da 0 diferencias en ambos sentidos; un usuario no admin con rol `marketing` recibe PERMISSION_DENIED en `yelp_bronze` y lee `yelp_gold` (con los 6 admins, solo se demuestra con la cuenta no admin, decisión D1).

- [ ] T010 [US1] Celda «Matriz de permisos» en `E6_controles_gobierno/01_controles_unity_catalog.py`: crear `workspace.yelp_gov.gov_permiso_rol` (`rol STRING NOT NULL`, `objeto STRING NOT NULL`, `privilegio STRING NOT NULL` en `USE SCHEMA · SELECT · MODIFY · EXECUTE · ALL PRIVILEGES`, `justificacion STRING NOT NULL`; PK lógica (rol, objeto, privilegio)) y cargarla con overwrite exactamente con la tabla de [contracts/matriz_permisos.md](contracts/matriz_permisos.md) (ningún rol distinto de `custodio` en `workspace.yelp_bronze`)
- [ ] T011 [US1] Celda «Reconciliación de GRANT» en `E6_controles_gobierno/01_controles_unity_catalog.py`: calcular `esperados = gov_rol_usuario ⋈ gov_permiso_rol` y `otorgados = information_schema.schema_privileges` (schemas `yelp_%`, `inherited_from = 'NONE'`, excluyendo al owner del schema); ejecutar **primero** `REVOKE` de `otorgados − esperados` (incluye a `account users` y a los grants ad hoc de integrantes de R11) y **después** `GRANT` de `esperados − otorgados`; `ALL PRIVILEGES` se compara como privilegio propio; dejar `USE CATALOG` en el catálogo para `account users` intacto; imprimir cada sentencia ejecutada; re-ejecución sin cambios = 0 sentencias
- [ ] T012 [US1] Celda «Matriz RACI» en `E6_controles_gobierno/01_controles_unity_catalog.py`: cargar con overwrite la tabla existente `workspace.yelp_gov.matriz_raci` con las 10 entidades de [data-model.md](data-model.md) (`data_owner` = A, `data_steward` = `steward`, `data_custodian` = `custodio`, `consumidores` = roles con SELECT sobre las tablas de la entidad según `gov_permiso_rol`, `clasificacion` = máxima de sus columnas en `information_schema.column_tags` con orden `publico < interno < confidencial < restringido`); aplicar `ALTER TABLE … ALTER COLUMN … COMMENT` para corregir el COMMENT de `clasificacion` a `publico|interno|confidencial|restringido` y documentar `dominio`, `data_steward` y `data_custodian`
- [ ] T013 [P] [US1] Corregir el DDL de `matriz_raci` en `00_setup/00_configurar_lakehouse.py` con los mismos COMMENT de T012 (valores de clasificación de E4) para que un setup desde cero quede igual
- [ ] T014 [US1] Celda «Validador de permisos (SC-002)» en `E6_controles_gobierno/01_controles_unity_catalog.py`: guardar el inventario `momento = 'despues'` en `gov_inventario_permisos`; assert `esperados − otorgados = ∅` y `otorgados − esperados = ∅`; assert ningún grantee distinto del custodio u owner con privilegios en `yelp_bronze` (FR-002); assert `matriz_raci` con 10 filas y sin `data_owner` NULL; mostrar la comparación antes/después agregada por grantee
- [ ] T015 [P] [US1] Crear `E6_controles_gobierno/02_demo_roles.py` con cabecera de Trazabilidad, celda que muestra `current_user()` y sus roles desde `gov_rol_usuario`, y celdas de acceso de [quickstart.md](quickstart.md) §3: `SELECT * FROM workspace.yelp_bronze.brz_review LIMIT 5` (esperado PERMISSION_DENIED salvo admins), `SELECT * FROM workspace.yelp_gold.gld_fact_resena LIMIT 5` (esperado OK); cada celda con un `try/except` que imprime «✔ acceso concedido» o «⛔ acceso denegado: <mensaje>» para que la demo no se corte
- [ ] T016 [US1] Ejecutar `E6_controles_gobierno/01_controles_unity_catalog.py` hasta T014 en el workspace con el custodio, ejecutarlo una segunda vez para comprobar 0 sentencias en la reconciliación, y anotar el resultado en la celda de decisiones

**Checkpoint**: US1 funcional; los permisos otorgados coinciden con la matriz.

---

## Phase 4: User Story 2 - Datos personales enmascarados (Priority: P1)

**Goal**: las 17 columnas `confidencial`/`restringido` se ven enmascaradas para roles no autorizados y en claro para `steward`, `trust_safety` y `custodio`.

**Independent Test**: la misma consulta sobre `gld_user_fraud_profile` devuelve `'****'`/NULL para un usuario sin rol autorizado y valores reales para el Steward; el job sigue produciendo gold sin degradarse.

- [ ] T017 [US2] Celda «Funciones de máscara» en `E6_controles_gobierno/01_controles_unity_catalog.py`: `CREATE OR REPLACE FUNCTION` de `workspace.yelp_gov.fn_mascara_texto(valor STRING, clasificacion STRING) RETURNS STRING` (`'****'`), `fn_mascara_decimal(valor DOUBLE, …) RETURNS DOUBLE` (NULL), `fn_mascara_entero(valor INT, …) RETURNS INT` (NULL) y `fn_mascara_arreglo(valor ARRAY<BIGINT>, …) RETURNS ARRAY<BIGINT>` (NULL), todas con `CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor ELSE … END` y COMMENT; más `fn_mascara_nombre(valor STRING, clasificacion STRING) RETURNS STRING` = inicial + `'****'` solo para `slv_user.nombre` (excepción de [contracts/funciones_mascara.md](contracts/funciones_mascara.md))
- [ ] T018 [US2] Celda «Reconciliación de máscaras» en `E6_controles_gobierno/01_controles_unity_catalog.py`: leer `information_schema.column_tags` (`tag_name = 'clasificacion'`, `tag_value IN ('confidencial','restringido')`, schemas `yelp_silver`/`yelp_gold`) unido a `information_schema.columns.full_data_type`; mapear `string→texto` (salvo `slv_user.nombre → nombre`), `double→decimal`, `int→entero`, `array<bigint>→arreglo`; tipo sin función → `raise` con el nombre de la columna («no se deja una columna sensible sin máscara en silencio»); comparar con `information_schema.column_masks` y ejecutar `ALTER TABLE … ALTER COLUMN … SET MASK workspace.yelp_gov.fn_mascara_<tipo> USING COLUMNS ('<clasificacion>')` solo si falta o difiere, y `DROP MASK` en columnas enmascaradas que ya no tienen la etiqueta sensible; imprimir las sentencias
- [ ] T019 [US2] Celda «Validador de comportamiento (SC-001)» en `E6_controles_gobierno/01_controles_unity_catalog.py`: para `current_user()`, assert que `fn_autorizado('confidencial')` y `fn_autorizado('restringido')` coinciden con lo esperado según `gov_rol_usuario`; assert que el custodio está autorizado en ambas clasificaciones (research R5); mostrar `SELECT user_id, clasificacion_riesgo, score_riesgo_usuario FROM workspace.yelp_gold.gld_user_fraud_profile LIMIT 5`
- [ ] T020 [P] [US2] Agregar a `E6_controles_gobierno/02_demo_roles.py` las celdas de máscara de [quickstart.md](quickstart.md) §3: `SELECT nombre FROM workspace.yelp_silver.slv_user LIMIT 5` y `SELECT clasificacion_riesgo, score_riesgo_usuario, reglas_activadas FROM workspace.yelp_gold.gld_user_fraud_profile LIMIT 5`, con una celda markdown que explica qué debe ver cada rol
- [ ] T021 [US2] Ejecutar T017–T019 en el workspace y luego correr el job `Capstone_YELP` (target dev) con el CLI de la extensión; verificar en `workspace.yelp_gov.pipeline_bitacora` que `DQ_gate` y las tareas `GOLD_*` terminaron en SUCCESS, y que `gld_user_fraud_profile` tiene la misma cantidad de usuarios con `clasificacion_riesgo` no nula que en la corrida previa (si gold se degrada, el custodio no está autorizado: revisar T006–T008)
- [ ] T022 [US2] Verificar con un usuario **no admin** (candidato `freddy.chicomar@softtek.com`, sujeto a D1/D3) si consultar una columna enmascarada exige `SELECT` sobre `gov_rol_usuario`/`gov_politica_mascara` o `USE SCHEMA` sobre `yelp_gov` (riesgo pendiente del plan); si lo exige, agregar a `gov_permiso_rol` la fila mínima necesaria para los roles de lectura y documentarlo; si no hay usuario no admin disponible, dejar la verificación anotada como pendiente en la celda de decisiones

**Checkpoint**: US1 y US2 funcionan por separado; las máscaras están aplicadas y el pipeline sigue sano.

---

## Phase 5: User Story 3 - Clasificación que gobierna los controles (Priority: P2)

**Goal**: las etiquetas determinan las máscaras, y schemas y tablas llevan capa, dominio y owner.

**Independent Test**: columnas `confidencial`/`restringido` = columnas con máscara (100 %, en ambos sentidos); reclasificar una columna y re-ejecutar cambia su control.

- [ ] T023 [US3] Celda «Etiquetas de schema» en `E6_controles_gobierno/01_controles_unity_catalog.py`: `ALTER SCHEMA workspace.yelp_<capa> SET TAGS ('capa' = 'bronze'|'silver'|'gold'|'gobierno', 'dominio' = 'yelpconnect')` para los 4 schemas (FR-005, valores sin tildes)
- [ ] T024 [US3] Celda «Etiquetas de tabla» en `E6_controles_gobierno/01_controles_unity_catalog.py`: para cada tabla de `yelp_silver` y `yelp_gold`, `SET TAGS ('dominio' = <tema de datos>, 'data_owner' = 'producto'|'trust_safety'|'cdo')` derivados de `workspace.yelp_gov.diccionario_datos` (columnas `dominio` y `responsable` agregadas por tabla a partir de `nombre_fisico`), normalizados sin tildes; assert que `data_owner` de cada tabla coincide con el Data Owner de la constitución IV y con `matriz_raci.data_owner` de su entidad
- [ ] T025 [US3] Celda «Validador de cobertura (SC-003)» en `E6_controles_gobierno/01_controles_unity_catalog.py`: assert `etiquetadas_sensibles − enmascaradas = ∅` y `enmascaradas − etiquetadas_sensibles = ∅` usando `information_schema.column_tags` e `information_schema.column_masks`; assert 17 columnas (10 confidencial + 7 restringido al 2026-10-29); assert los 4 schemas con tags `capa` y `dominio` en `information_schema.schema_tags`
- [ ] T026 [US3] Prueba de reclasificación (evidencia FR-004) en una celda de `E6_controles_gobierno/01_controles_unity_catalog.py` desactivada por defecto (widget `probar_reclasificacion = 'no'`, como `forzar_falla_gate` de E5): etiquetar `workspace.yelp_gold.gld_dim_negocio.nombre` como `confidencial`, re-ejecutar la reconciliación (T018), comprobar que aparece su máscara, restaurar la etiqueta original (`publico`) y volver a reconciliar; ejecutarla una vez y guardar la salida como evidencia

**Checkpoint**: la clasificación gobierna las máscaras y la cobertura es del 100 %.

---

## Phase 6: User Story 4 - Linaje de extremo a extremo (Priority: P2)

**Goal**: evidencia del linaje raw → bronze → silver → gold de `gld_user_fraud_profile` (el salto al dashboard llega con E8).

**Independent Test**: en Catalog Explorer, el linaje de `gld_user_fraud_profile` muestra sus orígenes silver, bronze y el volumen `raw`.

- [ ] T027 [US4] Celda «Linaje» en `E6_controles_gobierno/01_controles_unity_catalog.py`: consultar `system.access.table_lineage` filtrando `target_table_full_name` en la cadena de `workspace.yelp_gold.gld_user_fraud_profile` (gold ← `slv_user`, `slv_review`, `slv_fraud_rule` ← `brz_*`) con `LIMIT 50`; si la tabla de sistema no está habilitada en Free Edition, envolver en `try/except` y documentar que la evidencia es solo la captura
- [ ] T028 [US4] Tomar en Catalog Explorer las capturas del linaje de `gld_user_fraud_profile` (vista de tablas y de notebooks/jobs) y guardarlas como `E6_controles_gobierno/img/linaje_gld_user_fraud_profile.png`; insertarlas en la celda markdown «Linaje» del notebook con su ruta relativa
- [ ] T029 [P] [US4] Agregar a `specs/007-e8-dashboard-genie/spec.md` (Requirements) el requisito traído de E6: el dashboard se publica **sin credenciales embebidas** o con un publicador no autorizado (research R7); Genie respeta los permisos del usuario; tras publicarlo se completa la captura de linaje «gold → dashboard» en `E6_controles_gobierno/img/`

**Checkpoint**: las 4 historias se pueden verificar por separado.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [ ] T030 Celda «Bitácora» al final de `E6_controles_gobierno/01_controles_unity_catalog.py`: insertar en `workspace.yelp_gov.pipeline_bitacora` una fila con `tarea = 'E6_controles'`, `capa = 'gobierno'`, `tabla = 'gov_*, matriz_raci, máscaras, grants'`, `estado = 'SUCCESS'`, `intentos = 1`, `filas` = número de sentencias GRANT/REVOKE/MASK ejecutadas, `duracion_seg` medida en el notebook y `job_run_id = 'manual'` (principio VII)
- [ ] T031 Celda markdown «Decisiones de implementación» en `E6_controles_gobierno/01_controles_unity_catalog.py`: roles como datos sin grupos de cuenta (R4), máscara nativa con tabla de mapeo (R1, R11), custodio autorizado (R5), alcance del diccionario sin `gov_*` (nota 2 del plan), limitaciones de Free Edition (FR-009), y la tabla de **decisiones provisionales D1–D4** con «revisar al cierre del proyecto» copiada de [plan.md](plan.md)
- [ ] T032 Celda markdown «Definition of Done (E6)» en `E6_controles_gobierno/01_controles_unity_catalog.py` con checks de SC-001 a SC-005 y de la constitución (Trazabilidad, COMMENT, idempotencia, PR revisado); marcar los cumplidos
- [ ] T033 [P] Actualizar `README.md`: paso manual de E6 (copiar `roles_usuarios_PLANTILLA.csv` → `roles_usuarios.csv`, completarlo y subirlo a `/Volumes/workspace/yelp_gov/artefactos/`), que E6 se ejecuta con el custodio después de E4 y E5 y cada vez que cambia la clasificación, y el nuevo `02_demo_roles` en el orden de ejecución
- [ ] T034 Ejecutar `E6_controles_gobierno/01_controles_unity_catalog.py` completo dos veces seguidas en el workspace (la segunda sin sentencias GRANT/REVOKE/MASK, idempotencia FR-007) y recorrer [quickstart.md](quickstart.md) §1–§4; guardar en `img/` las capturas de «misma consulta, dos roles» y del acceso denegado si hay usuario no admin
- [ ] T035 Actualizar la sección 5 del Documento Formal en Drive (carpeta «G02 - BANSEG - UTEC»): RACI igual a `matriz_raci`, Data Owners del 2026-10-23, matriz de permisos, política de enmascaramiento y limitaciones de Free Edition, con D1–D4 como provisionales (FR-009, principio I)
- [ ] T036 Commit, push y PR de `feature/E6-controles-gobierno` contra `main` con la descripción del entregable, los validadores ejecutados en el workspace y su resultado; recordar en el PR que la constitución pide revisión de otro integrante

---

## Phase 8: Decisiones diferidas (al cierre del proyecto, NO ejecutar ahora)

**Purpose**: deuda registrada en [plan.md](plan.md) («Decisiones diferidas»). Se ejecuta cuando E3–E8 y EX estén en `main`, antes de la sustentación.

- [ ] T037 (D1) Quitar el rol admin del workspace a los integrantes que no despliegan; repetir la demo de `E6_controles_gobierno/02_demo_roles.py` para capturar el acceso denegado efectivo en `img/`
- [ ] T038 (D2) Cambiar el custodio a `arquitecturadatos.grupo2@gmail.com`: `run_as` en `resources/capstone_yelp.job.yml`, `ALTER TABLE … OWNER TO` en tablas `brz_*`/`slv_*`/`gld_*`, actualizar `roles_usuarios.csv` y re-ejecutar `E6_controles_gobierno/01_controles_unity_catalog.py`
- [ ] T039 (D3) Completar `roles_usuarios.csv` con todos los integrantes (incluido Yovani Pahuara una vez invitado) y resolver la cuenta duplicada de Freddy; re-ejecutar E6 y el validador de permisos
- [ ] T040 (D4) Ratificar con los Data Owners qué roles ven en claro; ajustar `gov_politica_mascara` en `E6_controles_gobierno/01_controles_unity_catalog.py` si cambia y re-ejecutar

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Fase 1)**: sin dependencias.
- **Foundational (Fase 2)**: depende de Setup; bloquea todas las historias.
- **US1 (Fase 3)** y **US2 (Fase 4)**: dependen solo de la Fase 2; son independientes entre sí.
- **US3 (Fase 5)**: T025–T026 dependen de T018 (US2); T023–T024 solo de la Fase 2. T024 usa `matriz_raci` de T012 (US1).
- **US4 (Fase 6)**: depende solo de que el job haya corrido (linaje); T029 es independiente.
- **Polish (Fase 7)**: después de las historias elegidas.
- **Diferidas (Fase 8)**: al cierre del proyecto.

### Within Each User Story

- Tablas de gobierno → reconciliación → validador → ejecución en el workspace.
- Las celdas de `01_controles_unity_catalog.py` son secuenciales (mismo archivo, orden de ejecución del notebook).

### Parallel Opportunities

- T001 y T002 (archivos distintos).
- T013 (`00_setup`), T015 y T020 (`02_demo_roles.py`), T029 (`specs/007…`) y T033 (`README.md`) se pueden hacer en paralelo con las celdas del notebook principal.

## Parallel Example: User Story 1

```text
Mientras se escriben T010–T012 en 01_controles_unity_catalog.py:
  T013 [P] [US1] COMMENT de matriz_raci en 00_setup/00_configurar_lakehouse.py
  T015 [P] [US1] Celdas de acceso en 02_demo_roles.py
```

## Parallel Example: User Story 2

```text
Mientras se escriben T017–T019 en 01_controles_unity_catalog.py:
  T020 [P] [US2] Celdas de máscara en 02_demo_roles.py
```

## Implementation Strategy

### MVP (US1 + US2, ambas P1)

1. Fases 1 y 2.
2. US1 → validar permisos (SC-002).
3. US2 → validar máscaras y que el job no se degradó (SC-001).
4. **Stop y validar**: es la demo de M5 («permisos y máscara actuando los roles»).

### Incremental Delivery

1. MVP → PR parcial posible.
2. US3 → cobertura del 100 % y gobierno por tags (SC-003).
3. US4 → linaje (SC-004); el salto al dashboard llega con E8.
4. Polish → Documento Formal, README y PR final.
5. Fase 8 al cierre del proyecto.
