# Databricks notebook source
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze: fuentes simuladas de Trust & Safety
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `BRONZE_simulados`
# MAGIC
# MAGIC `fraud_rule` y `trust_safety_area` **no forman parte del Yelp Open Dataset**: representan sistemas internos
# MAGIC de Trust & Safety que el caso supone. Sus datos semilla viven versionados en el repo
# MAGIC (`datos/semillas/*.csv`) y se suben al Volume en `raw/semillas/`.
# MAGIC
# MAGIC Principio de bronze: **fidelidad al origen** — todo se guarda como STRING, sin limpiar, con metadata de
# MAGIC auditoría (`ingestion_date`, `source_file`). La estandarización ocurre en silver.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**: es el SBB del bloque de ingesta para las fuentes internas simuladas.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

SEMILLAS = "/Volumes/workspace/yelp_bronze/raw/semillas"
FUENTES = {
    # archivo semilla → (tabla bronze, comentario de la tabla)
    "fraud_rule":        ("workspace.yelp_bronze.brz_fraud_rule",
                          "Catálogo de reglas de fraude de Trust & Safety — fuente simulada, fidelidad al origen."),
    "trust_safety_area": ("workspace.yelp_bronze.brz_trust_safety_area",
                          "Equipos de Trust & Safety — fuente simulada, fidelidad al origen."),
}

# COMMAND ----------

# DBTITLE 1,Carga batch full de las semillas
for archivo, (tabla, comentario) in FUENTES.items():
    (spark.read
        .option("header", True)
        .option("inferSchema", False)          # bronze guarda todo como STRING
        .csv(f"{SEMILLAS}/{archivo}.csv")
        .withColumn("ingestion_date", F.current_timestamp().cast("string"))
        .withColumn("source_file", F.col("_metadata.file_path"))
     .write.mode("overwrite")
        .option("overwriteSchema", "true")     # bronze sigue al origen: si la semilla suma columnas, bronze también
        .saveAsTable(tabla))
    spark.sql(f"COMMENT ON TABLE {tabla} IS '{comentario}'")
    print(f"✔ {tabla} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
for archivo, (tabla, _) in FUENTES.items():
    r = spark.table(tabla).agg(
        F.count("*").alias("filas"),
        F.count_if(F.col("source_file").isNull()).alias("sin_auditoria"),
    ).first()
    assert r.filas > 0, f"❌ {tabla} quedó vacía: ¿se subió {archivo}.csv a {SEMILLAS}?"
    assert r.sin_auditoria == 0, f"❌ {tabla} tiene filas sin source_file"
    print(f"✔ {tabla}: {r.filas} filas con metadata de auditoría")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Batch full (no Auto Loader): son catálogos pequeños que cambian por decisión de Trust & Safety, no por
# MAGIC   volumen. El Auto Loader queda para `review`, la entidad de mayor volumen (M7).
# MAGIC - El esquema de bronze sigue al CSV (`overwriteSchema`): agregar un parámetro a la semilla no exige tocar este notebook.
# MAGIC - Las semillas traen mayúsculas y minúsculas mezcladas a propósito (`Alto`, `ALTO`, `activa`): silver las
# MAGIC   estandariza y así se evidencia la regla de transformación del mapeo.
# MAGIC
# MAGIC ### Definition of Done (bronze simulados)
# MAGIC - [ ] Semillas subidas a `raw/semillas/` y validación en verde.
# MAGIC - [ ] Origen simulado declarado como «sistema fuente» en el diccionario de datos (E4).
# MAGIC - [x] Tablas con nombres `brz_fraud_rule` y `brz_trust_safety_area` (convención).
