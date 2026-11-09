# Databricks notebook source
# MAGIC %md
# MAGIC # Sustentación · Verificaciones en vivo
# MAGIC Notebook de la demo del 10/11. El jurado pide **una** de estas tres verificaciones; cada sección se ejecuta sola,
# MAGIC sin depender de las otras.
# MAGIC
# MAGIC | # | Verificación que pide la rúbrica | Sección | Tiempo aprox. |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Volver a correr una carga y comparar el conteo | Idempotencia | 2–3 min (bronze + silver) o ~40 s (solo silver) |
# MAGIC | 2 | Trazar una fila hasta su origen | Linaje de una reseña | ~1 min (la lectura del JSON crudo es lo más lento) |
# MAGIC | 3 | Cuadrar una cifra de consumo contra su origen | Reconciliación | ~30 s |
# MAGIC
# MAGIC **Antes de la sesión:** correr la celda de parámetros con el cómputo serverless ya encendido. **Quién ejecuta:** un
# MAGIC usuario con rol `steward`, `trust_safety` o `custodio` en `yelp_gov.gov_rol_usuario`; con otro rol, el texto de la
# MAGIC reseña y los parámetros de las reglas salen enmascarados (es el control de E6 funcionando, no un error), y silver no
# MAGIC se puede leer porque es zona restringida.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

dbutils.widgets.dropdown("recarga", "bronze_y_silver", ["bronze_y_silver", "solo_silver"], "1 · Qué se vuelve a correr")
dbutils.widgets.text("review_id", "SD6vciCeq5rOryr5WIn1GA", "2 · review_id a trazar")
dbutils.widgets.text("mes", "2015-05", "3 · Mes a cuadrar (AAAA-MM)")

BRZ, SLV, GLD, GOV = "workspace.yelp_bronze", "workspace.yelp_silver", "workspace.yelp_gold", "workspace.yelp_gov"
REVIEW_ID = dbutils.widgets.get("review_id").strip()
MES = dbutils.widgets.get("mes").strip() + "-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Idempotencia: dos corridas, mismo conteo
# MAGIC **Qué se afirma:** re-ejecutar la carga no duplica ni pierde filas.
# MAGIC - Bronze de reseñas usa **Auto Loader**: el checkpoint guarda qué archivos ya se leyeron, así que una
# MAGIC   re-ejecución sin archivos nuevos lee 0 filas y `brz_review` no crece.
# MAGIC - Silver y gold se **recalculan completos** desde bronze y se escriben con `insertInto(overwrite)` (ADR-11): la
# MAGIC   misma entrada produce la misma salida. Las llaves de tip y check-in son hash determinísticos (ADR-04).
# MAGIC
# MAGIC Primero, el historial: la bitácora del job ya registra varias corridas completas con los mismos conteos.

# COMMAND ----------

# DBTITLE 1,1a · Historial: conteos por corrida en la bitácora del job
display(spark.sql(f"""
SELECT job_run_id,
       date_format(min(ejecucion_ts), 'yyyy-MM-dd HH:mm') AS inicio_utc,
       max(CASE WHEN tarea = 'BRONZE_yelp'               THEN filas END) AS filas_bronze_yelp,
       max(CASE WHEN tarea = 'SILVER_review'             THEN filas END) AS filas_silver_review,
       max(CASE WHEN tarea = 'SILVER_checkin'            THEN filas END) AS filas_silver_checkin,
       max(CASE WHEN tarea = 'GOLD_user_fraud_profile'   THEN filas END) AS filas_perfil_riesgo,
       count_if(estado <> 'SUCCESS')                                     AS tareas_con_falla
FROM {GOV}.pipeline_bitacora
WHERE job_run_id RLIKE '^[0-9]+$'
GROUP BY job_run_id
ORDER BY inicio_utc
"""))
# El perfil de riesgo cambia de 150.000 a 160.993 el 01/11 por un cambio de diseño (PR #13: entran los autores
# fuera de la muestra), no por la re-ejecución. Entre corridas con el mismo código, los conteos son idénticos.

# COMMAND ----------

# DBTITLE 1,1b · Foto ANTES de re-ejecutar
TABLAS = [f"{BRZ}.brz_review", f"{SLV}.slv_review", f"{SLV}.slv_review_cuarentena",
          f"{BRZ}.brz_checkin", f"{SLV}.slv_checkin"]

def foto():
    filas = [(t.split(".")[-1], spark.table(t).count()) for t in TABLAS]
    filas.append(("brz_review (review_id distintos)",
                  spark.table(f"{BRZ}.brz_review").select("review_id").distinct().count()))
    return dict(filas)

antes = foto()
for t, n in antes.items():
    print(f"{t:<36}{n:>12,}")

# COMMAND ----------

# DBTITLE 1,1c · Re-ejecutar la carga con los mismos notebooks del job
import time

pasos = ["../E7_pipeline_lakehouse/pipelines/nb_review_bronze_to_silver",
         "../E7_pipeline_lakehouse/pipelines/nb_checkin_bronze_to_silver"]
if dbutils.widgets.get("recarga") == "bronze_y_silver":
    pasos.insert(0, "../E7_pipeline_lakehouse/01_bronze_ingesta")   # batch de 4 entidades + Auto Loader de reseñas

for p in pasos:
    t0 = time.time()
    dbutils.notebook.run(p, 1800)
    print(f"✔ {p.split('/')[-1]}  ({time.time() - t0:,.0f} s)")

# COMMAND ----------

# DBTITLE 1,1d · Foto DESPUÉS y comparación
despues = foto()
comparacion = spark.createDataFrame(
    [(t, antes[t], despues[t], despues[t] - antes[t]) for t in antes],
    "tabla STRING, filas_antes BIGINT, filas_despues BIGINT, diferencia BIGINT")
display(comparacion)
assert all(antes[t] == despues[t] for t in antes), "❌ La re-ejecución cambió un conteo"
print("✔ Misma entrada, mismo conteo: la carga es idempotente")

# COMMAND ----------

# DBTITLE 1,1e · Prueba de que sí se escribió: nueva versión Delta con el mismo número de filas
display(spark.sql(f"""
SELECT version, timestamp, operation, operationMetrics.numOutputRows AS filas_escritas
FROM (DESCRIBE HISTORY {SLV}.slv_review) LIMIT 3
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Si el conteo cambiara**, las causas posibles serían: (1) llegó un archivo nuevo a `raw/` (Auto Loader lo procesa
# MAGIC y el aumento es legítimo: se ve en `_archivo_origen`); (2) alguien reinició el checkpoint de Auto Loader, y
# MAGIC entonces el mismo archivo entra dos veces. En ese caso silver no se duplica, porque el `review_id` repetido va a
# MAGIC cuarentena con el motivo `review_id_duplicado`, y la regla DQ-002 lo vigila.
# MAGIC
# MAGIC **Qué no entrega la fuente:** el JSON de Yelp no informa borrados, no trae marca de actualización (solo la fecha
# MAGIC de publicación) ni secuencia. Por eso no hay CDC posible: bronze guarda la foto completa con `_archivo_origen` y
# MAGIC `_fecha_ingesta`, y silver se recalcula entero desde bronze. Una reseña borrada en el origen desaparece solo si
# MAGIC el archivo nuevo la omite y se recarga el lote completo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Trazar una fila hasta su origen
# MAGIC Recorrido de la reseña del widget `review_id`, de gold hacia atrás: gold → silver → bronze → archivo crudo en el
# MAGIC Volume. El valor por defecto es una reseña cuyo autor tiene riesgo **Alto** y que además tiene un duplicado
# MAGIC lógico en cuarentena.

# COMMAND ----------

# DBTITLE 1,2a · Gold: la reseña en el hecho, con sus dimensiones
display(spark.sql(f"""
SELECT r.review_id, r.calificacion, r.es_extrema, r.n_caracteres, r.util,
       d.fecha, n.nombre AS negocio, n.ciudad, n.categoria_principal,
       r.user_id, u.es_miembro_inferido AS autor_inferido
FROM {GLD}.gld_fact_resena r
JOIN {GLD}.gld_dim_fecha   d USING (fecha_id)
JOIN {GLD}.gld_dim_negocio n USING (business_id)
JOIN {GLD}.gld_dim_usuario u USING (user_id)
WHERE r.review_id = '{REVIEW_ID}'
"""))

# COMMAND ----------

# DBTITLE 1,2b · Gold: por qué su autor está marcado, regla por regla
display(spark.sql(f"""
SELECT p.user_id, p.clasificacion_riesgo, p.score_riesgo_usuario, p.total_resenas,
       p.max_resenas_en_un_dia, p.porcentaje_calificaciones_extremas,
       p.regla_activada, fr.fraud_description, fr.umbral, fr.puntos, fr.team_id, fr.rule_status
FROM (SELECT *, explode(reglas_activadas) AS regla_activada FROM {GLD}.gld_user_fraud_profile) p
JOIN {GLD}.gld_fact_resena r ON r.user_id = p.user_id AND r.review_id = '{REVIEW_ID}'
LEFT JOIN {SLV}.slv_fraud_rule fr ON fr.fraud_rule_id = p.regla_activada
"""))
# reglas_activadas es el linaje regla → caso: cada punto del puntaje sale de una fila del catálogo slv_fraud_rule,
# que a su vez viene del CSV semilla versionado en el repo (datos/semillas/fraud_rule.csv).

# COMMAND ----------

# DBTITLE 1,2c · Silver: la reseña conformada y lo que se apartó a cuarentena por ella
display(spark.sql(f"""
SELECT 'slv_review' AS tabla, review_id, user_id, business_id, calificacion, fecha_publicacion,
       left(comentario, 80) AS comentario_inicio, autor_en_muestra, CAST(NULL AS STRING) AS motivo, fecha_carga
FROM {SLV}.slv_review WHERE review_id = '{REVIEW_ID}'
UNION ALL
SELECT 'slv_review_cuarentena', review_id, user_id, business_id, calificacion, fecha_publicacion,
       left(comentario, 80), NULL, motivo, fecha_carga
FROM {SLV}.slv_review_cuarentena
WHERE review_id = '{REVIEW_ID}' OR review_id_conservado = '{REVIEW_ID}'
"""))
# Un duplicado lógico (mismo usuario, negocio y texto) no se borra: queda en cuarentena apuntando a la reseña que
# sobrevivió (review_id_conservado). Así el conteo de Marketing no se infla y nada se pierde en silencio.

# COMMAND ----------

# DBTITLE 1,2d · Bronze: la fila tal como llegó, con su auditoría
brz = spark.sql(f"""
SELECT review_id, user_id, business_id, stars, date, useful, _archivo_origen, _fecha_ingesta, _rescued_data
FROM {BRZ}.brz_review
WHERE review_id IN (SELECT '{REVIEW_ID}'
                    UNION SELECT review_id FROM {SLV}.slv_review_cuarentena WHERE review_id_conservado = '{REVIEW_ID}')
""")
display(brz)
# Tipos: en bronze `date` sigue siendo texto y `stars` es DOUBLE (inferencia del JSON). El tipado de negocio
# (calificacion SMALLINT, fecha_publicacion TIMESTAMP) se hace en silver, no en bronze.

# COMMAND ----------

# DBTITLE 1,2e · Origen: la línea del archivo crudo en el Volume
archivo = brz.filter(F.col("review_id") == REVIEW_ID).first()._archivo_origen
print(f"Archivo de origen: {archivo}")
crudo = (spark.read.json(archivo)            # lectura directa del .json.gz que ingirió Auto Loader
         .filter(F.col("review_id") == REVIEW_ID)
         .select("review_id", "user_id", "business_id", "stars", "date", "useful",
                 F.substring("text", 1, 80).alias("text_inicio")))
display(crudo)

# COMMAND ----------

# DBTITLE 1,2f · Mismo dato en las cuatro capas
fila = spark.sql(f"""
SELECT c.stars AS origen_stars, b.stars AS bronze_stars, s.calificacion AS silver_calificacion, g.calificacion AS gold_calificacion,
       b.date  AS bronze_date,  s.fecha_publicacion AS silver_fecha, g.fecha_id AS gold_fecha_id,
       length(s.comentario) AS silver_largo_texto, g.n_caracteres AS gold_n_caracteres
FROM {BRZ}.brz_review b
JOIN {SLV}.slv_review s USING (review_id)
JOIN {GLD}.gld_fact_resena g USING (review_id)
CROSS JOIN (SELECT {crudo.first().stars} AS stars) c
WHERE b.review_id = '{REVIEW_ID}'
""")
display(fila)
# Gold no publica el texto (ADR-06): solo su largo. El texto vive en silver, zona restringida.

# COMMAND ----------

# MAGIC %md
# MAGIC **Linaje consultable (no dibujado):** Catalog Explorer → `workspace.yelp_gold.gld_user_fraud_profile` → pestaña
# MAGIC *Lineage* → *See lineage graph*. Unity Catalog lo captura solo, a nivel de tabla y de columna, de cada ejecución
# MAGIC del job. (La tabla de sistema `system.access.table_lineage` requiere que un admin de cuenta otorgue `USE SCHEMA`
# MAGIC sobre `system.access`; en Free Edition no lo tenemos, por eso se muestra en la interfaz.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Cuadrar una cifra de consumo contra su origen
# MAGIC Tres cuadres: el total de reseñas por capa, una cifra del dashboard recalculada desde silver y bronze, y la
# MAGIC trazabilidad de cada caso de fraude a una regla del catálogo (CE-01).

# COMMAND ----------

# DBTITLE 1,3a · Reconciliación de reseñas: bronze = silver + cuarentena
rec = spark.sql(f"""
SELECT (SELECT count(*) FROM {BRZ}.brz_review)             AS bronze,
       (SELECT count(*) FROM {SLV}.slv_review)             AS silver,
       (SELECT count(*) FROM {SLV}.slv_review_cuarentena)  AS cuarentena,
       (SELECT count(*) FROM {GLD}.gld_fact_resena)        AS gold_fact_resena
""").first()
print(f"bronze {rec.bronze:,} = silver {rec.silver:,} + cuarentena {rec.cuarentena:,}  →  "
      f"{'CUADRA' if rec.bronze == rec.silver + rec.cuarentena else 'NO CUADRA'}")
print(f"silver {rec.silver:,} = gold_fact_resena {rec.gold_fact_resena:,}  →  "
      f"{'CUADRA' if rec.silver == rec.gold_fact_resena else 'NO CUADRA'}")
display(spark.sql(f"SELECT motivo, count(*) AS resenas FROM {SLV}.slv_review_cuarentena GROUP BY 1 ORDER BY 2 DESC"))
assert rec.bronze == rec.silver + rec.cuarentena and rec.silver == rec.gold_fact_resena

# COMMAND ----------

# MAGIC %md
# MAGIC **Si no cuadrara:** (1) se detiene la publicación: gold sigue mostrando la última versión válida; (2) se busca la
# MAGIC diferencia con un `EXCEPT` entre `brz_review` y la unión de silver y cuarentena por `review_id`, lo que da las
# MAGIC filas exactas que se perdieron; (3) con `DESCRIBE HISTORY` y la bitácora se identifica la corrida que lo causó, y
# MAGIC se re-ejecuta desde bronze, que es idempotente. Hoy esta reconciliación se corre aquí y no como regla del
# MAGIC `DQ_gate`: es la mejora siguiente (DQ-020, criticidad alta, umbral 100 %).

# COMMAND ----------

# DBTITLE 1,3b · Cifra del dashboard (REQ-03, reseñas del mes) contra silver y bronze
display(spark.sql(f"""
WITH dashboard AS (   -- misma consulta del dataset «Reseñas por mes y categoría» del dashboard, sumada en el mes
  SELECT sum(resenas) AS n FROM (
    SELECT date_trunc('MONTH', d.fecha) AS mes, n.categoria_principal, count(*) AS resenas
    FROM {GLD}.gld_fact_resena r
    JOIN {GLD}.gld_dim_fecha d USING (fecha_id)
    JOIN {GLD}.gld_dim_negocio n USING (business_id)
    GROUP BY ALL) WHERE mes = DATE'{MES}'
)
SELECT '{MES[:7]}' AS mes,
       (SELECT n FROM dashboard) AS dashboard_gold,
       (SELECT count(*) FROM {SLV}.slv_review WHERE date_trunc('MONTH', fecha_publicacion) = DATE'{MES}') AS silver,
       (SELECT count(*) FROM {SLV}.slv_review_cuarentena WHERE date_trunc('MONTH', fecha_publicacion) = DATE'{MES}') AS cuarentena,
       (SELECT count(*) FROM {BRZ}.brz_review WHERE date_trunc('MONTH', CAST(date AS TIMESTAMP)) = DATE'{MES}') AS bronze,
       bronze = silver + cuarentena AND dashboard_gold = silver AS cuadra
"""))
# Power BI lee las mismas tablas de yelp_gold, así que la cifra del reporte es la misma consulta.

# COMMAND ----------

# DBTITLE 1,3c · Check-ins del mes (REQ-05): dashboard contra silver y contra el texto crudo de bronze
display(spark.sql(f"""
WITH brz AS (
  SELECT business_id, to_timestamp(trim(f)) AS t
  FROM {BRZ}.brz_checkin LATERAL VIEW explode(split(date, ',')) x AS f
)
SELECT '{MES[:7]}' AS mes,
       (SELECT sum(c.n_checkins) FROM {GLD}.gld_fact_checkin_diario c
          JOIN {GLD}.gld_dim_fecha d USING (fecha_id) JOIN {GLD}.gld_dim_negocio n USING (business_id)
          WHERE date_trunc('MONTH', d.fecha) = DATE'{MES}')                                        AS dashboard_gold,
       (SELECT count(*) FROM {SLV}.slv_checkin WHERE date_trunc('MONTH', fecha_checkin) = DATE'{MES}') AS silver,
       (SELECT count(DISTINCT business_id, t) FROM brz WHERE date_trunc('MONTH', t) = DATE'{MES}')    AS bronze_eventos_distintos,
       (SELECT count(*) FROM brz WHERE date_trunc('MONTH', t) = DATE'{MES}')                          AS bronze_marcas_brutas
"""))
# El origen trae un texto con fechas concatenadas por negocio. Silver lo explota a una fila por evento y descarta las
# marcas repetidas (mismo negocio y mismo segundo, ADR-01); la diferencia entre brutas y distintas son esos repetidos.

# COMMAND ----------

# DBTITLE 1,3d · CE-01: todo caso marcado apunta a una regla existente del catálogo
display(spark.sql(f"""
SELECT 'Usuarios con puntaje > 0 sin regla'           AS control, count(*) AS casos
FROM {GLD}.gld_user_fraud_profile WHERE score_riesgo_usuario > 0 AND (reglas_activadas IS NULL OR size(reglas_activadas) = 0)
UNION ALL
SELECT 'Ráfagas marcadas sin regla', count(*)
FROM {GLD}.gld_business_anomaly_bursts WHERE es_rafaga_sospechosa AND (reglas_activadas IS NULL OR size(reglas_activadas) = 0)
UNION ALL
SELECT 'Reglas citadas que no existen en el catálogo', count(*)
FROM (SELECT explode(reglas_activadas) AS id FROM {GLD}.gld_user_fraud_profile
      UNION ALL SELECT explode(reglas_activadas) FROM {GLD}.gld_business_anomaly_bursts) x
LEFT ANTI JOIN {SLV}.slv_fraud_rule r ON r.fraud_rule_id = x.id
"""))
display(spark.sql(f"""
SELECT clasificacion_riesgo, count(*) AS usuarios   -- misma cifra del visual «Usuarios por nivel de riesgo» (REQ-02)
FROM {GLD}.gld_user_fraud_profile GROUP BY 1 ORDER BY 2
"""))
