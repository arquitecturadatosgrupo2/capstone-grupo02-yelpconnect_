# Data Model · E6 Controles técnicos de gobierno

Todas las tablas viven en `workspace.yelp_gov`, sin prefijo de capa (el schema de gobierno
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
| usuario | STRING | NO (PK) | Correo del usuario en el workspace |
| rol | STRING | NO (FK → gov_rol) | Un rol por usuario (edge case «cambio de rol»: se reemplaza la fila y la reconciliación revoca lo anterior) |
| integrante | STRING | SÍ | Nombre del integrante que actúa el rol |
| vigente_desde | DATE | NO | Fecha de asignación |

**Validación**: un usuario = un rol; todo `rol` existe en `gov_rol`; el rol `custodio` tiene exactamente 1 usuario.

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

## gov_matriz_raci

RACI consultable (FR-008), alimenta el indicador de ownership de E8.

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| entidad | STRING | NO | Negocio, Usuario, Reseña, Tip, Check-in, Reglas de fraude, Equipos T&S, Productos de riesgo, Fecha, Gobierno/Calidad |
| rol | STRING | NO (FK) | |
| responsabilidad | STRING | NO | `R` · `A` · `C` · `I` |
| tablas | ARRAY<STRING> | SÍ | Tablas silver/gold de la entidad |

**Validación**: exactamente una `A` por entidad, y el rol `A` coincide con el Data Owner de la
constitución IV (Producto → Negocio/Usuario/Check-in; Trust & Safety → Reseña/Tip/fraude/equipos/
riesgo; CDO → Fecha y gobierno) y con el tag `data_owner` de sus tablas.

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
gov_rol 1─* gov_matriz_raci
column_tags(clasificacion) ──► reconciliación de máscaras
```

## Transiciones (cambio de rol)

`usuario` con rol A → rol B: se actualiza `gov_rol_usuario`; la reconciliación de GRANT calcula
`otorgados − esperados` y **revoca primero**, luego otorga `esperados − otorgados`. La máscara cambia
sola (se evalúa en cada consulta contra la tabla).
