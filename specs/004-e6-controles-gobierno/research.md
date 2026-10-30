# Research · E6 Controles técnicos de gobierno

**Fecha**: 2026-10-29 · **Plan**: [plan.md](plan.md)

Insumos revisados: spec, constitución v1.0.0, diccionario E4
(`E4_diccionario_glosario/artefactos/Diccionario_de_Datos_YelpConnect.xlsx`), DDL de E3, notebooks
de pipeline (`E7_pipeline_lakehouse/pipelines/`) y job del bundle.

> Los puntos marcados **[verificar en workspace]** se verificaron el 2026-10-29 (ver R11).

---

## R1 · Mecanismo de enmascaramiento

- **Decision**: máscara de columna nativa de Unity Catalog (`ALTER TABLE … ALTER COLUMN … SET MASK
  fn USING COLUMNS ('<clasificacion>')`) con una función SQL por tipo de dato, cuya autorización se
  resuelve contra tablas de gobierno (`gov_rol_usuario` × `gov_politica_mascara`) con
  `current_user()`.
- **Rationale**: la máscara viaja con la tabla (protege silver, gold, consultas ad hoc, dashboard y
  Genie a la vez) y no exige reescribir consumidores. El patrón «tabla de mapeo + `EXISTS`» es el
  recomendado por Databricks cuando no hay grupos de cuenta.
- **Alternatives considered**:
  - `is_account_group_member()` (plantilla actual): requiere grupos de cuenta, que Free Edition no
    permite crear → descartado.
  - Políticas ABAC por tag (`CREATE POLICY … MATCH COLUMNS has_tag_value(...)`): sería el ideal
    de FR-004, pero está en preview y no se asume disponible en Free Edition **[verificar en
    workspace]**. Si existe, se documenta como mejora; el diseño elegido lo emula (R3).
  - Vistas seguras `vw_*` + REVOKE sobre la tabla base: queda como **plan B** si `SET MASK` falla
    en el workspace. Su costo: duplicar objetos y redirigir el dashboard a las vistas.

## R2 · Tipos de dato a cubrir

El diccionario marca 17 columnas sensibles (10 `confidencial`, 7 `restringido`):

| Tipo | Columnas | Valor enmascarado |
|---|---|---|
| STRING | 10 (nombre, amigos, comentario ×2, texto, responsable ×2, fraud_description ×2, clasificacion_riesgo) | `'****'` (`nombre`: inicial + `'****'`) |
| DOUBLE | 3 (umbral ×2, score_riesgo_usuario) | `NULL` |
| INT | 3 (min_resenas, puntos ×2) | `NULL` |
| ARRAY<BIGINT> | 1 (reglas_activadas) | `NULL` |

- **Decision**: 4 funciones `yelp_gov.fn_mascara_{texto,decimal,entero,arreglo}`; la función de
  máscara debe devolver el mismo tipo que la columna. Nulo para numéricos y arreglos porque cualquier
  número sustituto (p. ej. 0) se confundiría con un dato real en agregados.
- **Alternatives**: una sola función STRING con CAST → rompe tipos y consumidores; descartado.

## R3 · Clasificación que gobierna (FR-004)

- **Decision**: un paso de **reconciliación** lee `information_schema.column_tags`
  (`clasificacion IN ('confidencial','restringido')`), elige la función por `data_type` y aplica
  `SET MASK`; y retira la máscara (`DROP MASK`) de columnas que perdieron esa etiqueta. Reclasificar en
  el diccionario → re-ejecutar E4 (tags) + E6 (reconciliación) cambia el control sin editar SQL.
- **Rationale**: cumple «derivar de la etiqueta» sin depender de ABAC; es idempotente (FR-007).
- **Fuente de verdad de máscaras aplicadas**: `information_schema.column_masks` **[verificar en
  workspace]**; si no existe en Free Edition, `DESCRIBE TABLE EXTENDED` por tabla.

## R4 · Identidades y roles sin grupos

- **Decision**: los roles se modelan como **datos** en `yelp_gov.gov_rol_usuario` (rol ↔ correo).
  Los GRANT se otorgan a usuarios individuales, derivados de esa tabla × `gov_permiso_rol`.
- Correos de los integrantes **no se versionan**: se cargan desde
  `/Volumes/workspace/yelp_gov/artefactos/roles_usuarios.csv`; en el repo va la plantilla
  `E6_controles_gobierno/artefactos/roles_usuarios_PLANTILLA.csv`.
- **Propuesta de asignación** (a confirmar por el grupo; el grupo tiene 6 integrantes, la spec
  dice 5):

| Rol del caso | Integrante propuesto |
|---|---|
| Steward de gobierno (autorizado a ver datos sensibles) | Mateo Miranda |
| CDO | Freddy Chicoma |
| Trust & Safety | Yovani Pahuara |
| Producto | Guillermo Mendoza |
| Operaciones Comerciales | Cesar Loli |
| Marketing | Rey Ventocilla |
| Data Science | sin integrante (permisos definidos, no otorgados) |
| Custodio (pipeline) | `mateomirandah` (ver R12) |

- **Alternatives**: grupos locales del workspace — no son utilizables en GRANT de Unity Catalog
  (solo grupos de cuenta) → descartado.

## R5 · La identidad del pipeline debe estar autorizada

- **Hallazgo**: las máscaras se aplican a **todos**, incluido el owner. Los notebooks gold leen
  columnas sensibles de silver (`slv_fraud_rule.umbral/puntos/min_resenas`,
  `slv_trust_safety_area.responsable`, etc.). Si la cuenta del job no está autorizada, gold se
  calcularía con `NULL`/`'****'` silenciosamente.
- **Decision**: rol `Custodio (pipeline)` en `gov_politica_mascara` para ambas clasificaciones;
  validador post-E6 que corre una vez el job (o `DQ_gate`) y compara conteos de `gld_user_fraud_profile`
  con la corrida previa en `pipeline_bitacora`.
- Escritura: el pipeline usa `insertInto(overwrite=True)` sobre tablas creadas por E3 con
  `CREATE TABLE IF NOT EXISTS` → máscaras y tags **persisten** entre corridas. El único
  `CREATE OR REPLACE` (`03_gold_productos_datos.py`, `gld_kpi_negocio_mensual`) no está en el job ni
  tiene columnas sensibles.

## R6 · Privilegios heredados del catálogo `workspace`

- **Riesgo**: en workspaces con catálogo por defecto, `workspace` suele traer privilegios amplios
  para `account users` (p. ej. `USE CATALOG`, `USE SCHEMA`, a veces `SELECT`/`CREATE`) que anularían
  el mínimo privilegio **[verificar en workspace]**.
- **Decision**: T0 inventaria `SHOW GRANTS` en catálogo y en los 4 schemas; se mantiene solo
  `USE CATALOG` para `account users` y se revocan los demás privilegios heredados sobre
  `yelp_bronze/silver/gold/gov`. Los integrantes de la demo **no** deben ser admins del workspace
  (un admin puede otorgarse permisos): verificar y documentar.

## R7 · Dashboard y Genie (contrato con E8)

- **Hallazgo**: un dashboard AI/BI publicado con **credenciales embebidas** ejecuta las consultas
  como el publicador; si lo publica un rol autorizado, todos los espectadores verían datos sensibles.
- **Decision**: contrato para E8 → publicar el dashboard **sin credenciales embebidas** (cada
  espectador ve según su rol) **o** que el publicador sea un rol no autorizado. Genie usa siempre los
  permisos del usuario. Se valida en E8 (US2-AS3); E6 deja el validador de columnas expuestas.

## R8 · Linaje

- **Decision**: linaje nativo de UC (lo captura el job al leer/escribir tablas y volúmenes). Producto
  elegido: `gld_user_fraud_profile` (dolor de T&S). Evidencia: capturas en
  `E6_controles_gobierno/img/` y consulta a `system.access.table_lineage` si está habilitado
  **[verificar en workspace]**. El salto «gold → dashboard» depende de E8: la captura final se toma
  cuando exista el dashboard.

## R9 · Etiquetas de schema y tabla (FR-005)

- **Decision**: schemas con `capa` (`bronze|silver|gold|gobierno`) y `dominio` = `yelpconnect`; tablas
  silver/gold con `dominio` (tema de datos del diccionario) y `data_owner` (Producto | Trust & Safety |
  CDO, según constitución IV). Valores sin tildes, como `clasificacion`.

## R11 · Inventario del workspace (2026-10-29)

Prueba con objetos temporales `tmp_e6_*` en `yelp_gov`, que se borraron al terminar:

- ✅ `SET MASK … USING COLUMNS ('confidencial')` funciona; la función con `EXISTS` sobre una tabla de
  mapeo y `current_user()` enmascara y desenmascara según la fila del mapa. Plan B de vistas descartado.
- ✅ `information_schema.column_masks` existe (columnas `mask_name`, `using_columns`).
- ✅ La máscara persiste tras `INSERT OVERWRITE`.
- ⚠️ **Todos los integrantes son admins del workspace** y el catálogo `workspace` es propiedad del
  grupo `_workspace_admins_…` → como dueños del catálogo, los admins leen todo y **el acceso denegado
  no se puede demostrar** con ellos. Las máscaras sí aplican a admins.
- ⚠️ Privilegios ad hoc que contradicen la matriz: `account users` tiene `SELECT` en `yelp_silver` y
  `USE SCHEMA` en `yelp_bronze`; varios integrantes tienen `SELECT` en bronze o `CREATE_TABLE`/`MODIFY`
  en silver y gold; `mateomirandah` tiene privilegios a nivel de catálogo.
- ⚠️ El job `Capstone_YELP` (dev y prod) corre como `mateomirandah@gmail.com` (creador, sin `run_as`),
  que además es dueño de 26 tablas; la cuenta del grupo `arquitecturadatos.grupo2@gmail.com` es
  dueña de los 4 schemas. El custodio efectivo hoy es una cuenta personal.
- ⚠️ Usuarios del workspace: freddychicoma y freddy.chicomar@softtek.com (dos cuentas), cesarlg03,
  ingventocilla, mateomirandah, gmendozatello, arquitecturadatos.grupo2. **Yovani Pahuara no está
  invitado.**

## R12 · Decisiones del Steward (2026-10-29)

- **Admins**: no se modifican. Los admins conservan acceso total vía propiedad del catálogo; se
  documenta como limitación (FR-009). Los GRANT se reconcilian igual contra la matriz (SC-002 mide lo
  otorgado, no lo efectivo para admins). La única cuenta no admin (`freddy.chicomar@softtek.com`) puede
  actuar el rol de consumo en la demo del acceso denegado, si el grupo lo acepta.
- **Custodio**: `mateomirandah@gmail.com` (status quo: corre el job y es dueño de las tablas). Mateo
  tiene **dos roles** (`steward` + `custodio`) → `gov_rol_usuario` admite varios roles por usuario.
- **Ver en claro**: `steward`, `trust_safety` y `custodio` para `confidencial` y `restringido`.
- **Asignación de integrantes**: pendiente (Yovani no invitado; Freddy con dos cuentas). La
  implementación no depende de ella: el CSV de roles se carga con las filas confirmadas
  (`mateomirandah` → `steward`, `custodio`) y se completa después re-ejecutando E6.

## R10 · Costo (principio VI)

- Todo E6 es DDL/DCL de metadatos más pocas consultas sobre `information_schema`; sin escaneo de
  tablas grandes salvo la consulta de evidencia (con `LIMIT`). Impacto en la cuota: despreciable. E6 **no**
  se agrega como tarea del job: se ejecuta bajo demanda y tras cambios de clasificación.
