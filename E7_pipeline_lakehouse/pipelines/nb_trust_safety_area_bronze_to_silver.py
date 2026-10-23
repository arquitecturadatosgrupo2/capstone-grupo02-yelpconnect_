# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_trust_safety_area`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_trust_safety_area`
# MAGIC
# MAGIC Conforma los **equipos de Trust & Safety** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC id a `BIGINT` y estandarización del estado del equipo.
# MAGIC Fuente **simulada** del caso (no forma parte del Yelp Open Dataset): su origen se declara en el diccionario.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_trust_safety_area` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_trust_safety_area"
DESTINO = "workspace.yelp_silver.slv_trust_safety_area"

# COMMAND ----------

# DBTITLE 1: Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        F.col("team_id").cast("bigint").alias("team_id"),
        F.trim(F.col("team_name")).alias("team_name"),
        F.trim(F.col("responsible")).alias("responsable"),
        F.lower(F.trim(F.col("team_status"))).alias("team_status"),   # activo | inactivo
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("team_id").isNotNull())
    .dropDuplicates(["team_id"])
)

# COMMAND ----------

# DBTITLE 1: Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

slv.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1: Validación automática
r = spark.table(DESTINO).agg(
    F.count("*").alias("filas"),
    F.countDistinct("team_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ team_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - `responsable` contiene nombres de personas: evaluar su clasificación (Confidencial) en el diccionario y E6.
# MAGIC - Fuente simulada: documentar en el Documento Formal y en el diccionario quién la genera y con qué supuestos.
# MAGIC
# MAGIC ### Definition of Done (slv_trust_safety_area)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] Origen de la fuente simulada documentado (sistema fuente en el diccionario).
# MAGIC - [x] Origen `brz_trust_safety_area` con metadata de auditoría (E7.1).
