# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Verificación de datos crudos
# MAGIC Valida que los 5 archivos del Yelp Open Dataset (muestreados) estén en el Volume
# MAGIC y hace una primera exploración de esquemas. **No transforma nada** — eso es trabajo
# MAGIC del pipeline (E7).

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
ESPERADOS = ["business", "review", "user", "tip", "checkin"]

archivos = [f.name for f in dbutils.fs.ls(RAW)]
print("Archivos en el Volume:", archivos)

faltantes = [e for e in ESPERADOS if not any(e in a for a in archivos)]
assert not faltantes, f"❌ Faltan archivos: {faltantes}. Suban la muestra al Volume."
print("✔ Los 5 datasets están presentes")

# COMMAND ----------

# DBTITLE 1,Exploración rápida de esquemas y volúmenes
for e in ESPERADOS:
    df = spark.read.json(f"{RAW}/{e}.json.gz")
    print(f"\n=== {e} · {df.count():,} filas ===")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC > ✏️ **TODO (grupo):** anotar aquí en markdown 3 observaciones sobre los datos crudos
# MAGIC > (campos anidados, tipos sospechosos, posibles problemas de calidad). Estas
# MAGIC > observaciones alimentan E3 (modelado) y E5 (reglas de calidad).
