# Contrato · Funciones de autorización y máscara

Todas en `workspace.yelp_gov`, creadas con `CREATE OR REPLACE FUNCTION` (idempotente) y con
`COMMENT`.

## fn_autorizado

```text
fn_autorizado(clasificacion STRING) RETURNS BOOLEAN
```

TRUE si `current_user()` tiene en `gov_rol_usuario` un rol presente en `gov_politica_mascara` para
esa clasificación. Clasificación desconocida o NULL → FALSE (falla cerrada).

## Funciones de máscara (una por tipo; el retorno = tipo de la columna)

| Función | Firma | En claro | Enmascarado |
|---|---|---|---|
| fn_mascara_texto | `(valor STRING, clasificacion STRING) RETURNS STRING` | `valor` | `'****'` |
| fn_mascara_decimal | `(valor DOUBLE, clasificacion STRING) RETURNS DOUBLE` | `valor` | `NULL` |
| fn_mascara_entero | `(valor INT, clasificacion STRING) RETURNS INT` | `valor` | `NULL` |
| fn_mascara_arreglo | `(valor ARRAY<BIGINT>, clasificacion STRING) RETURNS ARRAY<BIGINT>` | `valor` | `NULL` |

Excepción documentada: `slv_user.nombre` usa la inicial + `'****'` para que la demo muestre que la
fila existe; no aporta reidentificación en una muestra de una ciudad.

## Aplicación (reconciliación)

```text
ALTER TABLE <tabla> ALTER COLUMN <columna>
  SET MASK workspace.yelp_gov.fn_mascara_<tipo> USING COLUMNS ('<clasificacion>')
```

- Entrada: `information_schema.column_tags` con `tag_name = 'clasificacion'` y
  `tag_value IN ('confidencial','restringido')`, schemas `yelp_silver` y `yelp_gold`.
- Mapeo de tipo: `STRING→texto`, `DOUBLE→decimal`, `INT→entero`, `ARRAY<BIGINT>→arreglo`. Un tipo sin
  función **falla la ejecución** con el nombre de la columna (no se deja una columna sensible sin
  máscara en silencio, edge case «columna nueva»).
- Columnas enmascaradas sin la etiqueta sensible → `DROP MASK`.
- Tabla con restricciones CHECK (no admite máscara, research R13) → la columna queda **protegida
  por acceso**: si algún rol de `gov_permiso_rol` con SELECT sobre su schema no está en
  `gov_politica_mascara` para su clasificación, la ejecución falla.

## Validadores (salida esperada)

| Validador | Pasa si |
|---|---|
| Cobertura (SC-003) | columnas sensibles etiquetadas − (con máscara ∪ protegidas por acceso) = ∅; máscaras sin etiqueta sensible = ∅; ninguna columna de gold protegida solo por acceso |
| Comportamiento (SC-001) | para el usuario actual, `fn_autorizado(...)` coincide con su rol en `gov_rol_usuario` y una muestra `LIMIT 5` de cada columna enmascarada es `'****'`/NULL si no está autorizado |
| Pipeline (R5) | `fn_autorizado` es TRUE para el usuario `custodio` en ambas clasificaciones |
