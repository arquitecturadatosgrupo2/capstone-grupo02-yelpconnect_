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

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

SEMILLAS = "/Volumes/workspace/yelp_bronze/raw/semillas"
FUENTES = {
    # archivo semilla → tabla bronze
    "fraud_rule":        "workspace.yelp_bronze.brz_fraud_rule",
    "trust_safety_area": "workspace.yelp_bronze.brz_trust_safety_area",
}

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tablas bronze según YELP.sql (todo STRING + auditoría). IF NOT EXISTS: no altera las existentes.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_bronze.brz_fraud_rule (
# MAGIC   fraud_rule_id     STRING COMMENT 'Identificador de la regla, tal como llega del origen',
# MAGIC   fraud_description STRING COMMENT 'Descripción de la regla, tal como llega del origen',
# MAGIC   risk_level        STRING COMMENT 'Nivel de riesgo sin estandarizar',
# MAGIC   rule_status       STRING COMMENT 'Estado de la regla sin estandarizar',
# MAGIC   ingestion_date    STRING COMMENT 'Timestamp de ingesta (auditoría)',
# MAGIC   source_file       STRING COMMENT 'Archivo de origen (auditoría)'
# MAGIC ) COMMENT 'Reglas de fraude de Trust & Safety — fuente simulada, fidelidad al origen.';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_bronze.brz_trust_safety_area (
# MAGIC   team_id        STRING COMMENT 'Identificador del equipo, tal como llega del origen',
# MAGIC   team_name      STRING COMMENT 'Nombre del equipo',
# MAGIC   responsible    STRING COMMENT 'Responsable del equipo',
# MAGIC   team_status    STRING COMMENT 'Estado del equipo sin estandarizar',
# MAGIC   ingestion_date STRING COMMENT 'Timestamp de ingesta (auditoría)',
# MAGIC   source_file    STRING COMMENT 'Archivo de origen (auditoría)'
# MAGIC ) COMMENT 'Equipos de Trust & Safety — fuente simulada, fidelidad al origen.';

# COMMAND ----------

# DBTITLE 1: Carga batch full de las semillas
for archivo, tabla in FUENTES.items():
    df = (spark.read
        .option("header", True)
        .option("inferSchema", False)          # bronze guarda todo como STRING
        .csv(f"{SEMILLAS}/{archivo}.csv")
        .withColumn("ingestion_date", F.current_timestamp().cast("string"))
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    df.select(*spark.table(tabla).columns).write.insertInto(tabla, overwrite=True)
    print(f"✔ {tabla} cargada")

# COMMAND ----------

# DBTITLE 1: Validación automática
for archivo, tabla in FUENTES.items():
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
# MAGIC - Las semillas traen mayúsculas y minúsculas mezcladas a propósito (`Alto`, `ALTO`, `activa`): silver las
# MAGIC   estandariza y así se evidencia la regla de transformación del mapeo.
# MAGIC - `fraud_rule` no tiene una llave hacia el equipo responsable: queda pendiente en el modelo lógico
# MAGIC   (ver `gld_trust_governance` en E3).
# MAGIC
# MAGIC ### Definition of Done (bronze simulados)
# MAGIC - [ ] Semillas subidas a `raw/semillas/` y validación en verde.
# MAGIC - [ ] Origen simulado declarado como «sistema fuente» en el diccionario de datos (E4).
# MAGIC - [x] Tablas con nombres `brz_fraud_rule` y `brz_trust_safety_area` (convención).
