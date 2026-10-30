# Quickstart · Validar E6 de extremo a extremo

## Prerrequisitos

- Perfil CLI `FREE` autenticado (`databricks auth login --profile FREE`) con el CLI de la extensión
  de VS Code.
- E3, E4 (tags `clasificacion` aplicados) y al menos una corrida completa del job en `main`.
- Los integrantes invitados al workspace. Los 6 son admins (decisión: no se modifica, research R12):
  la demo de acceso denegado requiere un usuario no admin (p. ej. `freddy.chicomar@softtek.com`).
- `roles_usuarios.csv` (copia completada de
  `E6_controles_gobierno/artefactos/roles_usuarios_PLANTILLA.csv`) subido a
  `/Volumes/workspace/yelp_gov/artefactos/`.

## 1 · Aplicar controles (cuenta del grupo = custodio)

Ejecutar `E6_controles_gobierno/01_controles_unity_catalog` completo. Esperado:

- Inventario inicial de grants impreso (evidencia «antes»).
- Tablas `gov_*` creadas y cargadas ([data-model.md](data-model.md)).
- Tags `capa`/`dominio` en 4 schemas y `dominio`/`data_owner` en tablas silver/gold.
- 17 columnas con máscara; validador de cobertura ✔ ([contracts/funciones_mascara.md](contracts/funciones_mascara.md)).
- Validador de permisos: 0 diferencias contra [contracts/matriz_permisos.md](contracts/matriz_permisos.md).
- Re-ejecutarlo: termina sin errores y sin cambios (idempotencia).

## 2 · Verificar que el pipeline no se degradó

Correr el job (o solo `DQ_gate` + tareas `GOLD_*`). Esperado: `DQ_gate` en verde y
`gld_user_fraud_profile` con el mismo número de usuarios clasificados en riesgo que la corrida previa
en `pipeline_bitacora`; `score_riesgo_usuario` sin `NULL` para la cuenta custodio.

## 3 · Demo de permisos en vivo (< 10 min, SC-005)

Celda de demo del notebook (`02_demo_roles`), ejecutada por cada integrante:

| Paso | Marketing | Steward |
|---|---|---|
| `SELECT … FROM yelp_bronze.brz_review LIMIT 5` | ❌ PERMISSION_DENIED | ❌ PERMISSION_DENIED |
| `SELECT nombre FROM yelp_silver.slv_user LIMIT 5` | ❌ (sin acceso a silver) | ✔ en claro |
| `SELECT clasificacion_riesgo, score_riesgo_usuario FROM yelp_gold.gld_user_fraud_profile LIMIT 5` | ✔ `'****'` / NULL | ✔ en claro |
| `SELECT … FROM yelp_gold.gld_fact_resena LIMIT 5` | ✔ | ✔ |

Capturar cada resultado en `E6_controles_gobierno/img/`.

## 4 · Linaje

Catalog Explorer → `yelp_gold.gld_user_fraud_profile` → Lineage: se ven `slv_review`, `slv_user`,
`slv_fraud_rule`, sus `brz_*` y el volumen `raw`. Captura en `img/`. Repetir cuando E8 publique el
dashboard para mostrar el salto a la visualización.

## 5 · Cierre

- Marcar el Definition of Done del notebook.
- Actualizar la sección 5 del Documento Formal: RACI (= `gov_matriz_raci`), Data Owners
  (2026-10-23) y limitaciones de Free Edition (FR-009).
