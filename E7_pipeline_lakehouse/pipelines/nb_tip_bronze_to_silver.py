# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_tip`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_tip`
# MAGIC
# MAGIC Conforma la entidad **Tip** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC llave sustituta `tip_id`, renombrado a español y fecha a `TIMESTAMP`.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_tip` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_tip"
DESTINO = "workspace.yelp_silver.slv_tip"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        # Llave sustituta determinística: el mismo tip recibe el mismo id en cada corrida
        F.xxhash64("user_id", "business_id", "date", "text").alias("tip_id"),
        F.col("business_id"),
        F.col("user_id"),
        F.col("text").alias("texto"),                              # PII indirecta: tag y control en E6
        F.col("compliment_count").cast("bigint").alias("cumplidos"),
        F.col("date").cast("timestamp").alias("fecha_tip"),
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("user_id").isNotNull() & F.col("business_id").isNotNull())
    .dropDuplicates(["tip_id"])
)

# COMMAND ----------

# DBTITLE 1,Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

slv.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
r = spark.table(DESTINO).agg(
    F.count("*").alias("filas"),
    F.countDistinct("tip_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ tip_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - `tip_id` se genera con `xxhash64(user_id, business_id, date, text)` en lugar de `row_number()`: es
# MAGIC   determinístico entre corridas, así que la llave no cambia al recargar. Tips idénticos en esos 4 campos se
# MAGIC   consideran duplicados y se conservan una sola vez.
# MAGIC
# MAGIC ### Definition of Done (slv_tip)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] Decisión de llave sustituta reflejada en el modelo lógico del Documento Formal.
# MAGIC - [x] Origen `brz_tip` con metadata de auditoría (E7.1).
