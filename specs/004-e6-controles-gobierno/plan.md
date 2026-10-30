# Implementation Plan: E6 · Controles técnicos de gobierno

**Branch**: `feature/E6-controles-gobierno` | **Date**: 2026-10-29 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/004-e6-controles-gobierno/spec.md`

## Summary

Convertir la estrategia de gobierno (sección 5 del Documento Formal) en controles efectivos de
Unity Catalog sobre las capas ya construidas: permisos de mínimo privilegio por rol simulado,
máscaras de columna derivadas de la etiqueta `clasificacion` de E4, etiquetas de capa/dominio/owner,
RACI como tabla y evidencia de linaje. Sin grupos de cuenta en Free Edition, los roles se modelan
como **datos** (`yelp_gov.gov_rol_usuario`) y GRANT y máscaras se **reconcilian** desde esas tablas,
de forma idempotente. Detalle de decisiones en [research.md](research.md).

## Technical Context

**Language/Version**: Databricks SQL + PySpark en notebooks fuente `.py` (serverless, Free Edition)

**Primary Dependencies**: Unity Catalog (GRANT/REVOKE, `SET MASK`, `SET TAGS`, `information_schema`, linaje); CLI de Databricks de la extensión VS Code 2.22.0 (v1.22.1), perfil `FREE`

**Storage**: Delta en `workspace.yelp_gov` (tablas `gov_*`); CSV de roles en `/Volumes/workspace/yelp_gov/artefactos/`

**Testing**: celdas validadoras con `assert` dentro del notebook (patrón de E4/E5) + demo con dos usuarios reales

**Target Platform**: Databricks Free Edition, catálogo `workspace`

**Project Type**: notebooks de plataforma de datos (entregable del Capstone)

**Performance Goals**: re-ejecución completa del notebook E6 < 2 min; demo en vivo < 10 min (SC-005)

**Constraints**: sin grupos de cuenta; sin clusters clásicos; sin `display()` de tablas completas; correos de integrantes fuera de Git; ABAC por tags no asumido (research R1)

**Scale/Scope**: 4 schemas, ~20 tablas silver/gold, 17 columnas sensibles (10 confidencial, 7 restringido), 8 roles, 6 integrantes + cuenta del grupo

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principio | Evaluación | Estado |
|---|---|---|
| I. Documento define, plataforma demuestra | Notebook abre con Trazabilidad → sección 5. Limitaciones de Free Edition (sin grupos, sin ABAC) registradas como decisiones en el notebook **y** en la sección 5 (FR-009) | ✅ |
| II. Convenciones del Lakehouse | Solo objetos en `yelp_gov` (tablas `gov_*`, funciones `fn_*`); plan B de vistas usa `vw_`. Sin schemas nuevos. Gold no lee bronze. COMMENT en español | ✅ (ver nota 1) |
| III. Calidad como datos y gate | E6 no toca `dq_reglas`. Riesgo R5: máscaras podrían degradar silenciosamente el input de gold y del `DQ_gate` → mitigado autorizando `custodio` y con validador post-corrida | ✅ con mitigación |
| IV. Gobierno por diseño | Tags `clasificacion` de E4 **gobiernan** las máscaras (reconciliación); `data_owner` de tablas y `gov_matriz_raci` alineados con los Data Owners de la constitución; tablas `gov_*` entran al diccionario | ✅ (ver nota 2) |
| V. Reproducibilidad e idempotencia | `CREATE … IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, reconciliación diferencial de GRANT/máscaras; paso manual del CSV de roles documentado en quickstart y README | ✅ |
| VI. Frugalidad | Solo metadatos; consultas de evidencia con `LIMIT`; E6 no se suma al job | ✅ |
| VII. Operación observable | E6 no agrega tareas al job; sí registra su ejecución en `pipeline_bitacora` (tarea `E6_controles`) para dejar rastro de cambios de permisos | ✅ |
| Flujo y revisión | Rama `feature/E6-controles-gobierno`, PR con validadores ejecutados | ✅ |

**Notas**

1. Las tablas de gobierno existentes no llevan prefijo (`dq_reglas`, `pipeline_bitacora`); se usa
   `gov_` para agrupar las de E6. La constitución no fija prefijo para `yelp_gov`, así que no hay violación.
2. Agregar 5 tablas `gov_*` a `yelp_gov` obliga a agregarlas al diccionario de E4 (plantilla Excel +
   recarga) en este mismo ciclo, o declarar `yelp_gov` fuera del alcance del diccionario como ya
   ocurre hoy (el diccionario cubre silver y gold). **Decisión**: mantener el alcance actual (silver +
   gold) y documentarlo; las tablas `gov_*` llevan COMMENT completo.

**Re-check post-diseño (Phase 1)**: sin violaciones nuevas. El contrato con E8 (dashboard sin
credenciales embebidas, research R7) se traslada como requisito a `specs/007-e8-dashboard-genie`.

## Project Structure

### Documentation (this feature)

```text
specs/004-e6-controles-gobierno/
├── plan.md                      # este archivo
├── research.md                  # Phase 0
├── data-model.md                # Phase 1
├── quickstart.md                # Phase 1
├── contracts/
│   ├── matriz_permisos.md       # rol × schema × privilegio
│   └── funciones_mascara.md     # firmas, aplicación y validadores
└── tasks.md                     # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
E6_controles_gobierno/
├── 01_controles_unity_catalog.py     # reescritura de la plantilla: T0 inventario → tablas gov_* →
│                                     # tags → GRANT reconciliado → máscaras reconciliadas →
│                                     # validadores → decisiones + DoD
├── 02_demo_roles.py                  # celdas de demo que ejecuta cada integrante (acceso, máscara)
├── artefactos/
│   └── roles_usuarios_PLANTILLA.csv  # rol,usuario,integrante (sin correos reales)
└── img/                              # capturas: acceso denegado, misma consulta 2 roles, linaje

README.md                             # orden de ejecución: paso manual del CSV de roles
specs/007-e8-dashboard-genie/spec.md  # + requisito de publicación sin credenciales embebidas
.gitignore                            # roles_usuarios.csv (no la plantilla)
```

**Structure Decision**: se mantiene la estructura por entregable del repo; E6 queda en su carpeta con un
notebook de controles (custodio) y uno de demo (cualquier integrante). No se modifica el job del bundle.

## Riesgos y verificación temprana (T0)

| Riesgo | Si se confirma |
|---|---|
| `SET MASK` no disponible en Free Edition | Plan B: vistas `vw_*` en gold/silver + REVOKE en tablas base (research R1) |
| `information_schema.column_masks` ausente | Validador de cobertura con `DESCRIBE TABLE EXTENDED` |
| Grants amplios heredados para `account users` | Revocarlos en la reconciliación; documentar «antes/después» |
| Integrantes con admin del workspace | Quitar admin para la demo o documentarlo como limitación |
| Máscara con subconsulta exige SELECT en `gov_*` al consultante | Otorgar `SELECT` en `gov_rol_usuario`/`gov_politica_mascara` a todos los roles con acceso, o volver la tabla de mapeo una función de valores fijos |

## Complexity Tracking

Sin violaciones de la constitución que justificar.
