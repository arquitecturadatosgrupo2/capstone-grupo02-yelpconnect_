# Implementation Plan: E8 · Dashboard BI y Genie

**Branch**: `feature/E8-dashboard-genie` | **Date**: 2026-10-30 | **Spec**: [spec.md](spec.md)

**Input**: spec `specs/007-e8-dashboard-genie/spec.md` y requerimientos aprobados en [seccion7-requerimientos.md](seccion7-requerimientos.md)

## Summary

Dashboard AI/BI de dos páginas (Negocio y Gobierno) con 17 visuales trazados a REQ-01..REQ-10, y un
espacio Genie sobre gold y gobierno con los sinónimos del glosario y 5 preguntas de prueba (REQ-11).
Dashboard y espacio se definen como código en el repo y se despliegan con el bundle o con la API.
Se publican sin credenciales embebidas, para que las máscaras de E6 se apliquen a cada espectador.
Decisiones en [research.md](research.md).

## Technical Context

**Language/Version**: Databricks SQL (datasets), JSON (dashboard `.lvdash.json`, Genie `serialized_space` v2), notebooks fuente `.py`

**Primary Dependencies**: AI/BI Dashboards (Lakeview), AI/BI Genie, Databricks Asset Bundles, CLI de la extensión 2.22.0

**Storage**: N/A (consume `yelp_gold` y `yelp_gov`)

**Testing**: validadores en `01_consultas_base_dashboard` (consulta del dataset contra consulta directa) y benchmarks de Genie probados con `genie ask`

**Target Platform**: Databricks Free Edition, warehouse serverless `656cbc31df3fbcc2`

**Project Type**: entregable de BI del Capstone

**Performance Goals**: cada dataset responde en segundos en el warehouse 2X-Small (pre-agregados)

**Constraints**: solo gold y gobierno (FR-005); sin credenciales embebidas (FR-012); refresco en pausa en dev

**Scale/Scope**: 2 páginas, 9 datasets, 17 visuales, 5 preguntas Genie

## Constitution Check

| Principio | Evaluación | Estado |
|---|---|---|
| I. Documento define, plataforma demuestra | Cada visual se traza a un REQ de la sección 7. La sección se aprueba antes de construir y se transcribe al Documento Formal. Los notebooks abren con Trazabilidad | ✅ |
| II. Convenciones | Solo lee `yelp_gold` y `yelp_gov`; no crea tablas. Metadata en español | ✅ |
| III. Calidad como datos | La página de Gobierno muestra `dq_resultados`/`dq_reglas`, que son el scorecard de E5 | ✅ |
| IV. Gobierno por diseño | KPIs apoyados en términos del glosario. Brecha registrada: falta «Corrida del Pipeline». Máscaras de E6 respetadas sin credenciales embebidas | ✅ |
| V. Reproducibilidad | Dashboard en el bundle; Genie desde un JSON versionado con un notebook idempotente (crea o actualiza) | ✅ |
| VI. Frugalidad | Datasets pre-agregados; refresco programado en pausa en dev | ✅ |
| VII. Operación observable | La página de Gobierno muestra `pipeline_bitacora` | ✅ |

**Re-check post-diseño**: sin violaciones.

## Project Structure

```text
specs/007-e8-dashboard-genie/
├── plan.md · research.md · quickstart.md · seccion7-requerimientos.md
├── contracts/visuales.md
└── tasks.md

E8_dashboard_bi/
├── 01_consultas_base_dashboard.py     # reescrito: consulta de cada dataset + validadores (FR-011)
├── dashboard_yelpconnect.lvdash.json  # dashboard como código
├── 02_genie_space.py                  # crea/actualiza Genie desde el JSON y registra las pruebas
└── genie_space.json                   # tablas, instrucciones, sinónimos, SQL de ejemplo, benchmarks

resources/capstone_yelp.dashboard.yml  # recurso del bundle (embed_credentials: false)
```

**Structure Decision**: todo el entregable vive en `E8_dashboard_bi/` y su recurso del bundle va en `resources/`.

## Complexity Tracking

Sin violaciones.
