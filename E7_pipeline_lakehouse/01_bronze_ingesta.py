# Databricks notebook source
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming) · **Tarea del job:** `BRONZE_yelp`
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (archivo origen, timestamp de ingesta).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (mínimo obligatorio): lectura completa del archivo en el Volume.
# MAGIC - **Incremental con Auto Loader** (obligatorio para al menos UNA entidad, se ve en M7):
# MAGIC   demuestra ingesta idempotente — si suben un archivo nuevo al Volume, solo procesa lo nuevo.
# MAGIC
# MAGIC Las fuentes internas simuladas de Trust & Safety se ingieren en `pipelines/nb_simulados_raw_to_bronze`.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este pipeline es el SBB que implementa los bloques de ingesta y almacenamiento definidos en el documento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisión batch vs streaming por entidad
# MAGIC | Entidad | Tabla | Modo | Justificación |
# MAGIC |---|---|---|---|
# MAGIC | business | `brz_business` | Batch full | Catálogo de ~15 mil negocios; se reemplaza completo en cada corrida a costo mínimo. |
# MAGIC | user | `brz_user` | Batch full | Perfiles que cambian (fans, votos, elite): la foto completa es más simple que reconciliar cambios. |
# MAGIC | tip | `brz_tip` | Batch full | Volumen bajo (~120 mil filas); no justifica estado de streaming. |
# MAGIC | checkin | `brz_checkin` | Batch full | Un registro por negocio con todas sus fechas: el archivo se reescribe entero, no llega por eventos. |
# MAGIC | review | `brz_review` | **Auto Loader** | Entidad de mayor volumen y que crece por llegada de archivos nuevos: procesar solo lo nuevo ahorra cuota. |

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

RAW = "/Volumes/workspace/yelp_bronze/raw"
BRZ = "workspace.yelp_bronze"
ENTIDADES = ["business", "user", "tip", "checkin"]  # review va con Auto Loader abajo

# COMMAND ----------

# DBTITLE 1,Modo batch — 4 entidades
for e in ENTIDADES:
    (spark.read.json(f"{RAW}/{e}.json.gz")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))   # input_file_name() no está soportado en Unity Catalog
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")                             # bronze sigue al origen si cambia su esquema
        .saveAsTable(f"{BRZ}.brz_{e}"))
    print(f"✔ brz_{e}: {spark.table(f'{BRZ}.brz_{e}').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Modo incremental — Auto Loader para review (la entidad de mayor volumen)
# Checkpoint propio de brz_review: el de _checkpoints/review pertenece a una carga anterior y ya marcó
# review.json.gz como procesado, por lo que reutilizarlo dejaría brz_review vacía.
CHECKPOINT = f"{RAW}/_checkpoints/brz_review"

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")   # mismos tipos que la lectura batch (stars DOUBLE, votos BIGINT)
    .load(f"{RAW}/review*.json.gz")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_fecha_ingesta", F.current_timestamp())
 .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # micro-batch y termina: amigable con la cuota serverless
    .toTable(f"{BRZ}.brz_review"))
query.awaitTermination()

nuevas = sum(p.get("numInputRows") or 0 for p in query.recentProgress)   # sin archivos nuevos llega nulo
print(f"✔ brz_review: {nuevas:,} filas nuevas en esta corrida · {spark.table(f'{BRZ}.brz_review').count():,} filas en total")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por qué Auto Loader es idempotente
# MAGIC - **Checkpoint:** registra qué archivos ya se procesaron. Una re-ejecución sin archivos nuevos lee 0 filas.
# MAGIC - **Schema tracking** (`schemaLocation`): guarda el esquema inferido; columnas inesperadas van a
# MAGIC   `_rescued_data` en lugar de romper la carga.
# MAGIC - **Exactly-once:** el checkpoint y la escritura Delta se confirman juntos; si una corrida falla a mitad,
# MAGIC   la siguiente retoma sin duplicar filas.
# MAGIC - `trigger(availableNow=True)` procesa lo pendiente y termina: es un batch incremental, no un cluster encendido.
# MAGIC
# MAGIC ### Experimento obligatorio (evidencia para el informe)
# MAGIC 1. Ejecutar este notebook completo → anotar conteos.
# MAGIC 2. Volver a ejecutar SOLO la celda de Auto Loader **sin subir archivos nuevos** → debe procesar 0 registros.
# MAGIC 3. Subir un archivo `review_2.json.gz` pequeño al Volume y re-ejecutar → solo procesa el nuevo.
# MAGIC > ✏️ TODO — Documentar el experimento con capturas: es la prueba de ingesta incremental.
# MAGIC
# MAGIC ### Definition of Done (E7.1)
# MAGIC - [x] 5 tablas bronze pobladas con columnas de auditoría.
# MAGIC - [ ] Experimento de idempotencia documentado.
# MAGIC - [x] Decisión batch vs streaming justificada por entidad (tabla en markdown).

# COMMAND ----------

# DBTITLE 1,Validación automática
for e in ENTIDADES + ["review"]:
    r = spark.table(f"{BRZ}.brz_{e}").agg(
        F.count("*").alias("filas"),
        F.count_if(F.col("_archivo_origen").isNull() | F.col("_fecha_ingesta").isNull()).alias("sin_auditoria"),
    ).first()
    assert r.filas > 0, f"❌ brz_{e} quedó vacía"
    assert r.sin_auditoria == 0, f"❌ brz_{e} tiene {r.sin_auditoria} filas sin metadata de auditoría"
print("✔ 5 tablas bronze pobladas con metadata de auditoría")
