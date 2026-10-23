# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_fraud_rule`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_fraud_rule`
# MAGIC
# MAGIC Conforma el catálogo de **reglas de fraude** de Trust & Safety según el mapeo bronze → silver del grupo
# MAGIC (`mapeo_equipo_02_utec.xlsx`): id a `BIGINT` y estandarización de nivel de riesgo y estado.
# MAGIC Fuente **simulada** del caso (no forma parte del Yelp Open Dataset): su origen se declara en el diccionario.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_fraud_rule` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.fraud_rule"   # ✏️ TODO: renombrar a brz_fraud_rule (convención) al re-ejecutar la ingesta
DESTINO = "workspace.yelp_silver.slv_fraud_rule"

# COMMAND ----------

# DBTITLE 1: Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

slv = (brz
    .select(
        F.col("fraud_rule_id").cast("bigint").alias("fraud_rule_id"),
        F.trim(F.col("fraud_description")).alias("fraud_description"),
        F.lower(F.trim(F.col("risk_level"))).alias("risk_level"),     # bajo | medio | alto (CHECK)
        F.lower(F.trim(F.col("rule_status"))).alias("rule_status"),   # activa | inactiva
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("fraud_rule_id").isNotNull())
    .dropDuplicates(["fraud_rule_id"])
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
    F.countDistinct("fraud_rule_id").alias("pk_distintas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ fraud_rule_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} filas · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - `risk_level` se lleva a minúsculas antes de insertar para cumplir `chk_risk_level` (`bajo`, `medio`, `alto`).
# MAGIC - Fuente simulada: documentar en el Documento Formal y en el diccionario quién la genera y con qué supuestos.
# MAGIC
# MAGIC ### Definition of Done (slv_fraud_rule)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] Origen de la fuente simulada documentado (sistema fuente en el diccionario).
# MAGIC - [ ] Origen renombrado a `brz_fraud_rule` junto con la ingesta (E7.1).
