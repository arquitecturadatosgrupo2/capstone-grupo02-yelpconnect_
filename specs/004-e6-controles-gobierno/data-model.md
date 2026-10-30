# Data Model · E6 Controles técnicos de gobierno

Todas las tablas viven en `workspace.yelp_gov` (el schema de gobierno
sigue el estilo de `dq_reglas`, `pipeline_bitacora`, `diccionario_datos`). Creación con
`CREATE TABLE IF NOT EXISTS` + `COMMENT` en tabla y columnas; carga con overwrite controlado
(idempotente, FR-007). Data Owner de todas: CDO (Laura Hernández).

## gov_rol

Catálogo de roles del caso.

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| rol | STRING | NO (PK) | `cdo`, `steward`, `producto`, `trust_safety`, `marketing`, `operaciones_comerciales`, `data_science`, `custodio` |
| nombre_rol | STRING | NO | Nombre de negocio (p. ej. «Trust & Safety») |
| descripcion | STRING | SÍ | Responsabilidad en el Libro de Gobierno |
| tipo | STRING | NO | `gobierno` · `dueno_datos` · `consumo` · `tecnico` |

## gov_rol_usuario

Asignación rol ↔ identidad (simula grupos). Origen: `roles_usuarios.csv` en el volumen de artefactos.

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| usuario | STRING | NO | Correo del usuario en el workspace |
| rol | STRING | NO (FK → gov_rol) | Rol que actúa (edge case «cambio de rol»: se reemplaza la fila y la reconciliación revoca lo anterior) |
| integrante | STRING | SÍ | Nombre del integrante que actúa el rol |
| vigente_desde | DATE | NO | Fecha de asignación |

PK lógica: (usuario, rol). Un usuario puede tener varios roles (Mateo: `steward` + `custodio`, research R12).

**Validación**: todo `rol` existe en `gov_rol`; el rol `custodio` tiene exactamente 1 usuario y es la identidad que ejecuta el job.

## gov_permiso_rol

Matriz rol × schema × privilegio esperada (fuente de los GRANT y del validador SC-002).
Ver la matriz en [contracts/matriz_permisos.md](contracts/matriz_permisos.md).

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| rol | STRING | NO (FK) | |
| objeto | STRING | NO | `workspace.yelp_bronze` · `…yelp_silver` · `…yelp_gold` · `…yelp_gov` |
| privilegio | STRING | NO | `USE SCHEMA`, `SELECT`, `MODIFY`, `EXECUTE`, `ALL PRIVILEGES` |
| justificacion | STRING | NO | Cita de la RACI / mínimo privilegio |

PK lógica: (rol, objeto, privilegio).

## gov_politica_mascara

Clasificación → roles que ven en claro.

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| clasificacion | STRING | NO | `confidencial` · `restringido` (las demás no se enmascaran) |
| rol | STRING | NO (FK) | Rol autorizado a ver en claro |
| justificacion | STRING | NO | |

Contenido inicial: `confidencial` → `steward`, `trust_safety`, `custodio`; `restringido` →
`steward`, `trust_safety`, `custodio`. (`custodio` es obligatorio: ver research R5).

## gov_inventario_permisos

Evidencia «antes/después» de los permisos (FR-006). Solo se agregan filas (append).

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| capturado_ts | TIMESTAMP | NO | Momento de la captura |
| momento | STRING | NO | `antes` · `despues` |
| grantee | STRING | NO | Usuario o grupo |
| objeto | STRING | NO | Catálogo o schema |
| privilegio | STRING | NO | Privilegio de UC |

## matriz_raci (tabla existente, la llena E6)

RACI consultable (FR-008). **Ya existe**: la crea `00_setup/00_configurar_lakehouse.py` (vacía al
2026-10-29) y E8 la consulta (`entidades_con_owner`). E6 no crea una tabla nueva: la carga con
overwrite controlado. Formato ancho, una fila por entidad:

| Columna | Tipo | RACI | Contenido |
|---|---|---|---|
| entidad | STRING | — | `negocio`, `usuario`, `resena`, `tip`, `checkin`, `regla_fraude`, `equipo_trust_safety`, `producto_riesgo`, `fecha`, `gobierno_calidad` (10 filas) |
| dominio | STRING | — | Dominio / tema de datos del diccionario |
| data_owner | STRING | **A** | Rol (`gov_rol.rol`): Producto → negocio/usuario/checkin; Trust & Safety → resena/tip/regla_fraude/equipo_trust_safety/producto_riesgo; CDO → fecha/gobierno_calidad |
| data_steward | STRING | **R** (gobierno) | `steward` |
| data_custodian | STRING | **R** (técnico) | `custodio` |
| consumidores | STRING | **I** | Roles separados por coma, coherentes con [matriz_permisos.md](contracts/matriz_permisos.md) |
| clasificacion | STRING | — | Máxima clasificación de las columnas de la entidad: `publico\|interno\|confidencial\|restringido` |

Ajuste en 00_setup: el `COMMENT` de `clasificacion` dice `publica|interna|confidencial|pii`; se
corrige a los valores de E4 (`publico|interno|confidencial|restringido`) y se agregan COMMENT a
`dominio`, `data_steward` y `data_custodian`, y a la tabla en vivo con `ALTER … COMMENT`.

**Validación**: 10 entidades, ninguna con `data_owner` NULL; `data_owner` coincide con el tag
`data_owner` de las tablas de la entidad; `clasificacion` = la máxima de sus columnas según
`column_tags`.

## Objetos no tabulares

| Objeto | Tipo | Descripción |
|---|---|---|
| `yelp_gov.fn_autorizado(clasificacion STRING)` | función SQL → BOOLEAN | `EXISTS` sobre `gov_rol_usuario` ⋈ `gov_politica_mascara` para `current_user()` |
| `yelp_gov.fn_mascara_texto / _decimal / _entero / _arreglo` | funciones de máscara | Ver [contracts/funciones_mascara.md](contracts/funciones_mascara.md) |
| Tags de schema | `capa`, `dominio` | FR-005 |
| Tags de tabla | `dominio`, `data_owner` | Derivados del diccionario |
| Tags de columna | `clasificacion` | Ya aplicados por E4 (insumo, no se modifican aquí) |

## Relaciones

```text
gov_rol 1─* gov_rol_usuario
gov_rol 1─* gov_permiso_rol ──► GRANT por usuario (gov_rol_usuario × gov_permiso_rol)
gov_rol 1─* gov_politica_mascara ──► fn_autorizado ──► fn_mascara_* ──► SET MASK
gov_rol 1─* matriz_raci (data_owner, data_steward, data_custodian)
column_tags(clasificacion) ──► reconciliación de máscaras
```

## Transiciones (cambio de rol)

`usuario` con rol A → rol B: se actualiza `gov_rol_usuario`; la reconciliación de GRANT calcula
`otorgados − esperados` y **revoca primero**, luego otorga `esperados − otorgados`. La máscara cambia
sola (se evalúa en cada consulta contra la tabla).
