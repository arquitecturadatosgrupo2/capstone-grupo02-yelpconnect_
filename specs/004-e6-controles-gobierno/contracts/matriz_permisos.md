# Contrato · Matriz de permisos por rol (contenido de `gov_permiso_rol`)

Principio: mínimo privilegio por capa (FR-001, FR-002). `USE CATALOG` sobre `workspace` se
mantiene para `account users`; todo lo demás se otorga por usuario según esta matriz.

| Rol | yelp_bronze | yelp_silver | yelp_gold | yelp_gov |
|---|---|---|---|---|
| custodio (pipeline) | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES |
| steward | — | USE SCHEMA, SELECT | USE SCHEMA, SELECT | USE SCHEMA, SELECT, MODIFY |
| cdo | — | USE SCHEMA, SELECT | USE SCHEMA, SELECT | USE SCHEMA, SELECT |
| trust_safety | — | USE SCHEMA, SELECT | USE SCHEMA, SELECT | — |
| data_science | — | USE SCHEMA, SELECT | USE SCHEMA, SELECT | — |
| producto | — | — | USE SCHEMA, SELECT | — |
| marketing | — | — | USE SCHEMA, SELECT | — |
| operaciones_comerciales | — | — | USE SCHEMA, SELECT | — |

Reglas del contrato:

1. Ningún rol distinto de `custodio` tiene privilegios en `yelp_bronze` (FR-002, SC-002).
2. Los privilegios se otorgan a nivel **schema** (heredan a tablas actuales y futuras); no hay GRANT
   por tabla, salvo el plan B de vistas seguras (research R1).
3. La reconciliación es diferencial: revoca `otorgados − esperados` antes de otorgar
   `esperados − otorgados` (FR-007, cambio de rol).
4. El validador compara `information_schema.schema_privileges` (o `SHOW GRANTS`) contra esta matriz
   × `gov_rol_usuario` y exige 0 diferencias en ambos sentidos.
5. El scorecard de gobierno de E8 lee `yelp_gov`: si se publica sin credenciales embebidas, solo lo
   ven `steward`, `cdo` y `custodio`. Si otros roles lo necesitan, E8 expone una vista agregada
   `vw_*` en gold, sin datos sensibles.
