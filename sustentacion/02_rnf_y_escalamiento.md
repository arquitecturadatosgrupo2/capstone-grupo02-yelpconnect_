# Requisitos no funcionales y comportamiento a escala ×10

Insumo para la sustentación del 10/11 (rúbrica C1 y C2) y para el Documento Formal (secciones 2 y 8).
Las cifras salen del workspace al 08/11/2026: `yelp_gov.pipeline_bitacora`, el historial de Jobs, el Volume
`raw/` y la sección 8.2 del Documento Formal v0.13. Lo estimado está marcado como estimación.

## 1. Requisitos no funcionales derivados del caso

Cada requisito nace de una decisión de un stakeholder (sección 7, REQ-xx) y fija una decisión de arquitectura. Si
un requisito no mueve ninguna decisión, no está en la tabla.

| ID | Requisito | Cifra | De dónde sale (caso) | Qué decisión sostiene | Cómo se verifica |
|---|---|---|---|---|---|
| RNF-01 | Frescura | Datos del día anterior disponibles cada día antes de las 08:00 (Lima) | Trust & Safety revisa cuentas y ráfagas una vez al día y el fraude se mide por negocio y día (REQ-01, REQ-02, REQ-10) | Batch diario a las 06:00, no streaming (ADR-16); el grano de ráfagas es negocio-día | Job programado a las 06:00 con un tope de 2 h; DQ-007 marca silver con más de 24 h |
| RNF-02 | Duración de la corrida | ≤ 2 h por corrida y ≤ 30 min por tarea; hoy 4–5 min de reloj y 6–8 min de cómputo | Ventana entre las 06:00 y el inicio de la jornada de revisión | Timeouts por tarea y por corrida; serverless sin clúster encendido | Historial de Jobs y bitácora (REQ-10) |
| RNF-03 | Volumen | 351 MB comprimidos en crudo; 400.000 reseñas, 1,84 M check-ins, 150.000 usuarios, 14.577 negocios; unos 2,6 M filas en silver | Muestra de una ciudad (Philadelphia) bajo el tope del curso de 300 MB | Recálculo completo de silver y gold; Auto Loader solo en reseñas, la entidad que crece (ADR-12) | Conteos en la bitácora |
| RNF-04 | Incremental sin duplicar | 0 filas nuevas al re-ejecutar sin archivos nuevos; mismo conteo en corridas consecutivas | Las reseñas llegan por lotes nuevos y Marketing cuenta duplicados (MOT-01, CAP-07) | Auto Loader con checkpoint; `insertInto(overwrite)`; llaves hash (ADR-04, 11) | Notebook de demo, sección 1, y bitácora (9 corridas con los mismos conteos) |
| RNF-05 | Calidad antes de publicar | 11 reglas de criticidad alta al 99,5–100 % o gold no se publica | Una alerta de fraude debe poder defenderse ante el negocio que reclama (MOT-04) | `DQ_gate` entre silver y gold (ADR-13); cuarentena (ADR-10) | `yelp_gov.dq_resultados`; falla controlada del 23/10 |
| RNF-06 | Retención | Usuario: 36 meses tras la baja; texto de reseña: 7 años y luego anonimizado; cuarentena: 90 días; regla de fraude: 5 años tras su vigencia | Auditoría de decisiones de moderación y Ley 29733, RGPD art. 5 (sección 5.4) | Retención por columna en el diccionario; supresión de extremo a extremo desde raw | Diccionario (153 columnas). Brecha: la política es declarativa; no hay VACUUM ni propiedades de Delta configuradas |
| RNF-07 | Acceso | 7 columnas de dato personal y los parámetros antifraude, en claro solo para trust_safety, steward y custodio | MOT-05; las reglas no deben filtrarse a quien podría evadirlas | Máscaras nativas desde el tag `clasificacion`; silver como zona restringida (ADR-15) | CE-06; `E6_controles_gobierno/02_demo_roles` |
| RNF-08 | Concurrencia | Supuesto: hasta 10 lectores simultáneos del dashboard y Genie (6 integrantes y 5 stakeholders) | Consumo interno de 5 áreas | SQL warehouse serverless 2X-Small con autoapagado | No medido. Se verificaría con 10 sesiones simultáneas del dashboard y midiendo la cola del warehouse |
| RNF-09 | Recuperación | RPO = 1 día (se regenera desde raw); RTO ≈ 5 min (re-ejecutar el job) | Los indicadores son diarios; perder un día no detiene la operación | Raw inmutable en el Volume y capas regenerables; idempotencia | Re-ejecución en la demo |
| RNF-10 | Residencia | Datos en la región del workspace de Databricks (AWS). **Verificar la región en la consola antes de la sesión** | Ley 29733: el flujo transfronterizo exige informar al titular | Un solo workspace y un solo catálogo; nada sale a otro servicio salvo Power BI (importa desde yelp_gold) | Configuración del workspace |

**Requisito descartado: latencia por debajo del minuto (streaming).** Se evaluó porque el caso habla de detectar
fraude, pero ninguna decisión de los stakeholders se toma en minutos: Trust & Safety revisa una vez al día, el
comité de integridad es semanal y el conjunto de datos termina el 19/01/2022. Un streaming continuo consumiría la
cuota de Free Edition sin cambiar ninguna decisión. Si Yelp necesitara bloquear una ráfaga mientras ocurre, el
cambio sería de producto (moderación en línea), no de este pipeline analítico.

**También descartados:** alta disponibilidad multirregión (RPO de 1 día es suficiente para indicadores diarios) y
retención infinita en bronze (choca con la supresión que exigen la Ley 29733 y el RGPD).

## 2. Qué se rompe primero si el volumen crece diez veces

**Escenario:** 4 M de reseñas, 18 M de check-ins, 1,5 M de usuarios y unos 3,5 GB comprimidos. Es más o menos
el tamaño del Yelp Open Dataset completo (7 M de reseñas), así que el escenario es realista.

**No lo medimos.** Las cifras son estimaciones lineales desde la corrida limpia. Para verificarlas, se cargaría
el dataset completo de Yelp en un esquema de prueba y se correría el job una vez, comparando la duración de cada
tarea contra la bitácora.

| Orden | Qué se rompe | Hoy | Con ×10 (estimado) | Por qué | Qué decisión lo mueve |
|---|---|---|---|---|---|
| 1 | **El modelo importado de Power BI** | El pbix pesa 117 MB e importa 8 tablas y 6 vistas de gold | ≈ 1,2 GB, por encima del límite de 1 GB por modelo semántico en Power BI Pro | El modo importación copia el hecho de reseñas completo | Importar solo vistas agregadas (`vw_*`) o pasar a DirectQuery contra el SQL warehouse. Es el único quiebre duro: los demás vuelven lento el pipeline, pero este impide publicar |
| 2 | **La ingesta bronze** | `BRONZE_yelp` 82 s, la tarea más larga | 12–15 min en una sola tarea | Los archivos son `.json.gz`: gzip no se puede dividir, así que cada archivo lo lee un solo núcleo y añadir cómputo no ayuda. `user.json.gz` pasaría de 197 MB a ≈ 2 GB | Recibir el origen en varios archivos (por fecha) o en un formato que sí se divida (JSON sin comprimir, Parquet). Auto Loader ya está preparado para muchos archivos |
| 3 | **El recálculo completo de silver de reseñas** | `SILVER_review` 24 s | Varios minutos y un shuffle de unos 3 GB | Deduplicar exige particionar por usuario, negocio **y texto completo**, cada día y sobre toda la historia, aunque solo llegue un 1 % nuevo | Pasar a `MERGE` incremental por `_fecha_ingesta` y particionar por un hash del texto en lugar del texto. Es la deuda técnica que aceptamos (abajo) |
| 4 | **El `DQ_gate`** | 46 s | 5–8 min | 19 reglas con una o dos lecturas completas cada una, y DQ-013 cuenta reseñas por usuario | Evaluar todas las reglas de una tabla en una sola pasada |
| 5 | **La cuota de Free Edition** | 6–8 min de cómputo por corrida | 60–80 min de cómputo por corrida | El cómputo crece casi linealmente | En un workspace pagado deja de ser un límite y pasa a ser costo |

**Lo que no se rompe:** las dimensiones (pasan de 14.577 negocios a unos 146.000), los productos de riesgo, que ya
están agregados por usuario o por negocio-día, y el dashboard AI/BI, que lee gold agregado en vivo. Puede necesitar
un warehouse Small en lugar de 2X-Small para las consultas sobre el hecho de reseñas.

**Costo con ×10 (estimado, lineal):** de alrededor de 1 DBU a unos 10 DBU por corrida; con 30 corridas al mes son
300 DBU, entre USD 105 (Premium, USD 0,35 por DBU) y USD 135 (Enterprise, USD 0,45 por DBU), a precios de lista que
deben confirmarse. Con `MERGE` incremental, el costo diario dependería del volumen nuevo y no del histórico.

**Lo que domina la factura hoy no es el pipeline:** Genie y el asistente de IA suman el 80 % de los DBU del
workspace (693 de 866), y el job, menos del 3 %. La decisión que más mueve el costo es cómo se usa Genie (una
consulta en lenguaje natural genera varias consultas SQL), no el diseño del pipeline.

## 3. Deuda técnica deliberada y punto de falla más probable

**Deuda técnica deliberada: el recálculo completo de silver y gold.** Se eligió frente a `MERGE` incremental
porque la fuente no entrega borrados, marca de actualización ni secuencia (no hay CDC posible), y con 400.000
reseñas recalcular cuesta 24 s y garantiza la idempotencia por construcción. **Costo:** el cómputo diario crece con
el histórico y no con lo nuevo. **Cuándo se paga:** cuando una corrida pase de 30 minutos, o cuando la fuente
empiece a entregar archivos diarios con marca de tiempo. En ese momento se pasa silver de reseñas a `MERGE` por
`review_id` usando `_fecha_ingesta` como ventana: inicio en la última ingesta procesada y fin en la ingesta actual.

**Punto de falla más probable: la carga manual de las semillas y del origen.** El catálogo de reglas de fraude y
los equipos llegan como CSV que el custodio sube a mano al Volume. Un Pull Request aprobado no cambia nada hasta que
alguien sube el archivo, y si se sube uno con formato equivocado, `SILVER_fraud_rule` falla o, peor, carga una
regla sin equipo. **Lo que lo contiene hoy:** DQ-018 (toda regla activa tiene equipo), DQ-019 y la alerta de
`gld_trust_governance` para reglas con equipo inactivo. **Lo que falta:** que el CSV llegue al Volume desde el
repositorio, en el despliegue del bundle, y no a mano.

**Segundo punto de falla: el checkpoint de Auto Loader.** Si alguien lo borra, el mismo archivo se vuelve a leer y
`brz_review` duplica. Silver no se contamina, porque el `review_id` repetido va a cuarentena (motivo
`review_id_duplicado`). La reconciliación bronze = silver + cuarentena sigue cuadrando, así que no basta para
detectarlo: lo delata que las filas de `brz_review` superen a sus `review_id` distintos (sección 1 del notebook
de demo) y que la cuarentena por `review_id_duplicado` pase de 0. Ese motivo hoy no tiene regla de calidad propia.
