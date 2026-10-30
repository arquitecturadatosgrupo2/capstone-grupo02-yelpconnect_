# Research · E8 Dashboard BI y Genie

**Fecha**: 2026-10-30 · **Plan**: [plan.md](plan.md) · **Requerimientos**: [seccion7-requerimientos.md](seccion7-requerimientos.md)

## R1 · Dashboard como código

- **Decision**: dashboard AI/BI (Lakeview) definido en `E8_dashboard_bi/dashboard_yelpconnect.lvdash.json`
  y desplegado como recurso `dashboards` del bundle (`resources/capstone_yelp.dashboard.yml`), igual
  que el job.
- **Rationale**: la configuración pasa por Pull Request (principio VII) y se despliega de forma
  reproducible (principio V). Se verificó el 2026-10-30 que la API de Lakeview responde en Free Edition
  (`lakeview list`).
- **Alternatives**: armarlo a mano en la UI. Se descartó porque no queda versionado y cualquier cambio
  hecho en la UI se pierde en el siguiente despliegue.

## R2 · Credenciales de publicación (FR-012, traído de E6)

- **Decision**: `embed_credentials: false`. Cada espectador consulta con sus permisos y sus máscaras:
  Trust & Safety ve en claro la clasificación de riesgo y las reglas; Marketing ve `****`/NULL en esas
  columnas.
- **Consecuencia**: la página de Gobierno lee `yelp_gov`, que solo leen `steward`, `cdo` y `custodio`
  (matriz de E6). Es coherente: es la página de la CDO.

## R3 · Datasets pre-agregados

- **Decision**: cada dataset agrega en SQL al grano que necesita el visual: mes × categoría para
  reseñas, mes × día de la semana × categoría para check-ins, solo ráfagas sospechosas (585 de
  1,65 M ventanas). Las medidas se guardan como sumas (`suma_calificacion`, `resenas`) y el promedio
  se calcula en el widget, para que el filtro por categoría no promedie promedios.
- **Rationale**: frugalidad (principio VI) y respuesta rápida en el warehouse 2X-Small.

## R4 · Filtros

- **Decision**: filtro de **mes** (rango de fechas) y de **categoría principal** en la página de
  Negocio; filtro de **ciudad** también presente aunque la muestra tenga una sola ciudad
  (Philadelphia, 14.575 negocios en PA y 2 en NJ), para cumplir FR-004 y porque el modelo admite más
  ciudades.
- 727 categorías principales: el filtro es de selección múltiple con búsqueda. Los rankings por
  categoría se limitan en el dataset a categorías con volumen suficiente (≥ 500 reseñas) para no
  mostrar porcentajes sobre muestras pequeñas.

## R5 · Historial de calidad

- **Decision**: la página de Gobierno excluye la regla `DQ-900`, que es la regla de prueba que agrega
  `forzar_falla_gate` en E5 (falla controlada), y muestra la última ejecución de cada regla y la
  tendencia por dimensión. Al 2026-10-30 hay 7 ejecuciones (23/10 y 30/10).

## R6 · Genie como código y pruebas automatizadas

- **Decision**: el espacio se define en `E8_dashboard_bi/genie_space.json` (formato `serialized_space`
  versión 2: tablas, instrucciones, SQL de ejemplo y benchmarks) y se crea o actualiza con la API
  (`genie create-space` / `update-space`). Las 5 preguntas de prueba G1–G5 van como **benchmarks** con su
  SQL de referencia. Se prueban con `genie ask` desde el CLI y la evidencia (pregunta, SQL generado,
  resultado, coincide sí/no) queda en `E8_dashboard_bi/02_genie_space.py`.
- **Rationale**: SC-002 se verifica de forma reproducible, no solo en la demo en vivo. Se verificó que la
  API de Genie está disponible en Free Edition (existe el espacio de ejemplo «Bakehouse»).
- Sinónimos: se cargan como instrucciones de texto a partir del glosario (≥ 5, FR-006).

## R7 · Refresco (FR-010)

- **Decision**: programación del dashboard diaria a las 07:00 America/Lima, una hora después del job
  (06:00). En el target `dev` queda en pausa, como el schedule del job (principio VI).

## R8 · Corrección de la plantilla (FR-011)

- `E8_dashboard_bi/01_consultas_base_dashboard.py` usa `diccionario_datos.descripcion_columna`, que no
  existe: la columna es `descripcion_negocio`. El notebook se reescribe con las consultas reales de
  cada REQ y sus validadores.
