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
# MAGIC ### Observaciones sobre los datos crudos
# MAGIC Alimentan E3 (modelado) y E5 (reglas de calidad). Cifras de la muestra cargada en bronze.
# MAGIC
# MAGIC 1. **Campos anidados y listas en texto.** En `business`, `attributes` es un struct de 39 atributos (todos
# MAGIC    string, con valores como `"u'free'"` o `"True"`) y `hours` un struct por día; `categories` es un string
# MAGIC    separado por comas. En `user`, `friends` y `elite` también son listas dentro de un string. → E3 tipa y
# MAGIC    separa (`categorias` en silver y como arreglo en `gld_dim_negocio`) y deja `attributes` y `hours` fuera del modelo.
# MAGIC 2. **Fechas como string y check-ins agrupados.** `review.date` y `user.yelping_since` llegan como texto, y
# MAGIC    `checkin.date` concentra todas las visitas de un negocio en un solo string (142,5 en promedio por fila,
# MAGIC    hasta 52.144). → silver castea fechas y explota `slv_checkin` a una fila por visita (grano evento,
# MAGIC    término «Check-in Válido» del glosario).
# MAGIC 3. **Calidad por el muestreo y el origen.** `review_id` es único en el crudo (400.000), pero hay duplicados
# MAGIC    lógicos (mismo autor, negocio y texto: 547 a cuarentena) y reseñas cuyo autor quedó fuera de la
# MAGIC    muestra de usuarios (26.698, 6,7 %). 1.170 de 14.577 negocios no traen `attributes` y 9 no traen
# MAGIC    `categories`. → regla DQ-004 de E5, `slv_review_cuarentena` y la marca `autor_en_muestra`.
