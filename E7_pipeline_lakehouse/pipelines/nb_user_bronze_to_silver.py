# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_user`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_user`
# MAGIC
# MAGIC Conforma la entidad **Usuario** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC renombrado a español, fecha de registro a `TIMESTAMP` y estado de cuenta inicial.
# MAGIC `nombre` es PII: se clasifica y enmascara en E6.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_user` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_user"
DESTINO = "workspace.yelp_silver.slv_user"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        F.col("user_id"),
        F.col("name").alias("nombre"),                                  # PII
        F.col("average_stars").cast("double").alias("promedio_estrellas"),
        F.col("review_count").cast("bigint").alias("n_resenas"),
        F.col("fans").cast("bigint").alias("fans"),
        F.col("elite"),
        F.col("friends").alias("amigos"),
        F.col("yelping_since").cast("timestamp").alias("miembro_desde"),
        F.col("useful").cast("bigint").alias("util"),
        F.col("funny").cast("bigint").alias("divertido"),
        F.col("cool").cast("bigint").alias("cool"),
        F.lit("activa").alias("estado_cuenta"),                         # estado inicial (mapeo)
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("user_id").isNotNull())
    .dropDuplicates(["user_id"])
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
    F.countDistinct("user_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ user_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - `estado_cuenta` no existe en el origen: se inicializa en `'activa'` (mapeo).
# MAGIC - `chk_promedio` exige `promedio_estrellas` entre 1.0 y 5.0. Si la muestra trae usuarios con promedio 0,
# MAGIC   la carga falla: decidir con el Data Owner si se ajusta el CHECK o se envía a cuarentena, y documentarlo.
# MAGIC
# MAGIC ### Definition of Done (slv_user)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] `nombre` clasificado como PII en el diccionario y con tag aplicado (E4/E6).
# MAGIC - [x] Origen `brz_user` con metadata de auditoría (E7.1).
