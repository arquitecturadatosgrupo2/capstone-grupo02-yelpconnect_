# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_review`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_review`
# MAGIC
# MAGIC Conforma la entidad **Reseña** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC deduplicación por `review_id`, calificación a `SMALLINT`, fecha a `TIMESTAMP` y estado de moderación inicial.
# MAGIC Ataca el dolor de Marketing: reseñas contadas sin validar duplicados.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_review` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_review"
DESTINO = "workspace.yelp_silver.slv_review"

# COMMAND ----------

# DBTITLE 1: Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        F.col("review_id"),
        F.col("user_id"),
        F.col("business_id"),
        F.col("stars").cast("smallint").alias("calificacion"),
        F.col("text").alias("comentario"),                     # PII indirecta: tag y control en E6
        F.col("useful").cast("bigint").alias("util"),
        F.col("funny").cast("bigint").alias("divertido"),
        F.col("cool").cast("bigint").alias("cool"),
        F.col("date").cast("timestamp").alias("fecha_publicacion"),
        F.lit("aprobada").alias("estado_resena"),              # estado inicial; la moderación la define Trust & Safety
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("review_id").isNotNull())
    .dropDuplicates(["review_id"])
)

# ✏️ TODO (grupo): duplicados "lógicos" (mismo user + business + texto) y cuarentena de reseñas
#                  con FK inválida en slv_review_cuarentena, según exige la plantilla E7.2.

# COMMAND ----------

# DBTITLE 1: Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

slv.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1: Validación automática
r = spark.table(DESTINO).agg(
    F.count("*").alias("filas"),
    F.countDistinct("review_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ review_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - `estado_resena` no existe en el origen: se inicializa en `'aprobada'` (mapeo). Cuando Trust & Safety defina el
# MAGIC   flujo de moderación, este valor pasa a calcularse.
# MAGIC - `chk_calificacion` (1 a 5) es enforzada: una calificación imposible detiene la carga (ver DQ-001).
# MAGIC
# MAGIC ### Definition of Done (slv_review)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] Criterio de duplicados lógicos documentado + tabla de cuarentena operativa.
# MAGIC - [x] Origen `brz_review` alimentada con Auto Loader (E7.1 / M7).
