# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_business`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_business`
# MAGIC
# MAGIC Conforma la entidad **Negocio** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC normalización de ciudad y estado, `is_open` a booleano y `categories` parseado a `ARRAY<STRING>`.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_business` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_business"
DESTINO = "workspace.yelp_silver.slv_business"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        F.col("business_id"),
        F.col("name").alias("nombre"),
        F.initcap(F.trim(F.col("city"))).alias("ciudad"),           # Title Case sin espacios extra
        F.upper(F.trim(F.col("state"))).alias("estado"),            # código de 2 letras
        F.col("latitude").cast("double").alias("latitud"),
        F.col("longitude").cast("double").alias("longitud"),
        F.col("stars").cast("double").alias("estrellas"),
        F.col("review_count").cast("int").alias("n_resenas"),
        (F.col("is_open") == 1).alias("esta_abierto"),              # BIGINT 0/1 → BOOLEAN
        F.split(F.col("categories"), ",\\s*").alias("categorias"),  # decisión E3: array
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("business_id").isNotNull())
    .dropDuplicates(["business_id"])
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
    F.countDistinct("business_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ business_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Escritura con `insertInto(overwrite=True)` en lugar de `saveAsTable(..., overwriteSchema)`: la tabla la define
# MAGIC   el DDL de E3 y el pipeline no puede borrar sus COMMENT, PK ni CHECK.
# MAGIC - `chk_estrellas` (1.0 a 5.0) es enforzada: si llega un rating fuera de rango la carga falla en vez de propagarlo.
# MAGIC
# MAGIC ### Definition of Done (slv_business)
# MAGIC - [x] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [x] Columnas y tipos idénticos al DDL de E3.
# MAGIC - [x] Origen `brz_business` con metadata de auditoría (E7.1).
