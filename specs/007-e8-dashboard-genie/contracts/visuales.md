# Contrato · Visuales del dashboard y su trazabilidad

Cada visual lleva en su descripción el REQ y el stakeholder (FR-002). Fuentes solo gold y gobierno (FR-005).

## Página «Negocio»

Filtros: mes (rango), ciudad, categoría principal.

| # | Visual | Tipo | Dataset | REQ · stakeholder |
|---|---|---|---|---|
| N1 | Ráfagas sospechosas en el periodo | Contador | `ds_rafagas` | REQ-01 · Trust & Safety |
| N2 | Ráfagas sospechosas por mes | Línea | `ds_rafagas` | REQ-01 · Trust & Safety |
| N3 | Negocios con más ráfagas | Barras horizontales | `ds_rafagas` | REQ-01 · Trust & Safety |
| N4 | Usuarios por nivel de riesgo | Barras | `ds_riesgo_nivel` | REQ-02 · Trust & Safety |
| N5 | Usuarios marcados por regla de fraude | Barras | `ds_riesgo_regla` | REQ-02 · Trust & Safety |
| N6 | Reseñas por mes | Línea | `ds_resenas` | REQ-03 · Producto |
| N6b | Calificación promedio por mes | Línea | `ds_resenas` | REQ-03 · Producto |
| N7 | % de calificaciones extremas por categoría | Barras | `ds_resenas_categoria` | REQ-04 · Marketing |
| N8 | Votos útiles por reseña por categoría | Barras | `ds_resenas_categoria` | REQ-04 · Marketing |
| N9 | Check-ins válidos promedio por día de la semana | Barras | `ds_checkins` | REQ-05 · Operaciones Comerciales |
| N10 | Check-ins válidos por mes | Línea | `ds_checkins` | REQ-05 · Operaciones Comerciales |

## Página «Gobierno»

| # | Visual | Tipo | Dataset | REQ · stakeholder |
|---|---|---|---|---|
| G1 | Reglas de calidad en rojo (última ejecución) | Contador | `ds_calidad_ultima` | REQ-06 · CDO |
| G2 | Reglas de la última ejecución con cumplimiento, umbral y Data Owner | Tabla | `ds_calidad_ultima` | REQ-06 · CDO |
| G3 | Cumplimiento promedio por dimensión y ejecución | Línea | `ds_calidad_historia` | REQ-07 · CDO |
| G4 | % de columnas documentadas | Contador | `ds_cobertura` | REQ-08 · CDO |
| G5 | % de entidades con Data Owner | Contador | `ds_cobertura` | REQ-08 · CDO |
| G6 | Reglas de fraude: casos marcados y alertas de gobierno | Tabla | `ds_alertas` | REQ-09 · CDO y Trust & Safety |
| G7 | Corridas del pipeline: estado, duración, fallas y reintentos | Tabla | `ds_pipeline` | REQ-10 · CDO |

Total: 18 visuales (SC-005: ≥ 6). Datasets: 11 (el de riesgo se divide en nivel y regla para no duplicar usuarios al expandir las reglas).
