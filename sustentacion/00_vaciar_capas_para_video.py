# Databricks notebook source
# MAGIC %md
# MAGIC # Preparar el video · simular la primera corrida
# MAGIC Vacía los **datos** de bronze, silver y gold y aparta el checkpoint de Auto Loader. Conserva la estructura
# MAGIC (DDL, PK/FK/CHECK, COMMENT, tags, máscaras, GRANT) y `yelp_gov` (bitácora, DQ, gobierno).
# MAGIC Equivale al estado justo después de `00_setup` + `E3 01_modelo_fisico_ddl`, antes de la primera carga.
# MAGIC
# MAGIC No forma parte del job: se corre a mano antes de grabar el video de respaldo.
# MAGIC
# MAGIC **Reversible:** la celda 1 guarda la versión Delta de cada tabla; la celda 5 restaura todo.

# COMMAND ----------

# DBTITLE 1,Parámetros
from datetime import datetime

BRZ, SLV, GLD = "workspace.yelp_bronze", "workspace.yelp_silver", "workspace.yelp_gold"
TABLAS = (
    [f"{BRZ}.brz_{t}" for t in ["business", "user", "review", "tip", "checkin", "fraud_rule", "trust_safety_area"]]
  + [f"{SLV}.slv_{t}" for t in ["business", "user", "review", "review_cuarentena", "checkin", "tip",
                                "trust_safety_area", "fraud_rule"]]
  + [f"{GLD}.gld_{t}" for t in ["user_fraud_profile", "business_anomaly_bursts", "trust_governance", "dim_fecha",
                                "dim_negocio", "dim_usuario", "fact_resena", "fact_checkin_diario"]]
)
CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/brz_review"
RESPALDO_CHECKPOINT = CHECKPOINT + "_respaldo_" + datetime.now().strftime("%Y%m%d_%H%M")

# COMMAND ----------

# DBTITLE 1,1 · Respaldo: versión Delta actual de cada tabla (anotar o capturar esta salida)
versiones = {t: spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first().version for t in TABLAS}
conteos = {t: spark.table(t).count() for t in TABLAS}
display(spark.createDataFrame([(t, versiones[t], conteos[t]) for t in TABLAS], "tabla string, version long, filas long"))

# COMMAND ----------

# DBTITLE 1,2 · Vaciar datos y apartar el checkpoint (los dos juntos, o brz_review se duplica o queda vacía)
for t in TABLAS:
    spark.sql(f"TRUNCATE TABLE {t}")      # si una tabla con máscara lo rechaza: DELETE FROM {t}
dbutils.fs.mv(CHECKPOINT, RESPALDO_CHECKPOINT, recurse=True)
print(f"✔ {len(TABLAS)} tablas vaciadas · checkpoint movido a {RESPALDO_CHECKPOINT}")

# COMMAND ----------

# DBTITLE 1,3 · Toma del video: punto de partida, todas las capas en cero y sin checkpoint
display(spark.createDataFrame([(t, spark.table(t).count()) for t in TABLAS], "tabla string, filas long"))
display(dbutils.fs.ls("/Volumes/workspace/yelp_bronze/raw/"))   # fuentes presentes, _checkpoints sin brz_review

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Correr el job Capstone_YELP (grabar) y volver a la celda 3
# MAGIC Los conteos deben coincidir con los de la celda 1 y con las corridas previas de `yelp_gov.pipeline_bitacora`.

# COMMAND ----------

# DBTITLE 1,5 · SOLO si algo falla: restaurar datos y checkpoint al estado de la celda 1
# for t in TABLAS:
#     spark.sql(f"RESTORE TABLE {t} TO VERSION AS OF {versiones[t]}")
# dbutils.fs.rm(CHECKPOINT, recurse=True)
# dbutils.fs.mv(RESPALDO_CHECKPOINT, CHECKPOINT, recurse=True)
