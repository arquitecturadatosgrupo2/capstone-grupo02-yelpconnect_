# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: limpieza y conformación
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo físico de E3**: mismos nombres, mismos tipos,
# MAGIC mismas constraints. Las transformaciones deben atacar los dolores del caso:
# MAGIC deduplicación de reseñas (Marketing), normalización de check-ins (Ops Comerciales).
# MAGIC
# MAGIC Ejemplo resuelto: `slv_business`. El grupo completa las demás entidades.

# COMMAND ----------

# DBTITLE 1,Ejemplo resuelto — slv_business
from pyspark.sql import functions as F

brz = spark.table("workspace.yelp_bronze.brz_business")

slv_business = (brz
    .select(
        F.col("business_id"),
        F.col("name").alias("nombre"),
        F.initcap(F.trim(F.col("city"))).alias("ciudad"),      # normalización
        F.upper(F.col("state")).alias("estado"),
        F.col("latitude").alias("latitud"),
        F.col("longitude").alias("longitud"),
        F.col("stars").cast("double").alias("estrellas"),
        F.col("review_count").cast("int").alias("n_resenas"),
        (F.col("is_open") == 1).alias("esta_abierto"),
        F.split(F.col("categories"), ",\\s*").alias("categorias"),  # decisión E3: array
        F.current_timestamp().alias("fecha_carga"),
    )
    .dropDuplicates(["business_id"])
    .filter(F.col("business_id").isNotNull())
)
slv_business.write.mode("overwrite").saveAsTable("workspace.yelp_silver.slv_business")
print(f"✔ slv_business: {slv_business.count():,} filas")

# COMMAND ----------

# DBTITLE 1,✏️ TODO — slv_review (deduplicación explícita + FK válidas)
# Requisitos mínimos:
#  - dropDuplicates por review_id Y detección de duplicados "lógicos"
#    (mismo user + business + texto): decidir y documentar el criterio.
#  - Cuarentena: las filas que violan integridad referencial NO se botan,
#    van a workspace.yelp_silver.slv_review_cuarentena con el motivo.
#  - Casteo de 'date' a timestamp.

# COMMAND ----------

# DBTITLE 1,✏️ TODO — slv_user, slv_tip, slv_checkin
# slv_checkin: el campo 'date' llega como STRING con fechas separadas por coma.
#  → explotar a grano evento (1 fila = 1 check-in) usando split + explode.
#  Esto resuelve el dolor de Ops Comerciales: un único conteo oficial de check-ins.

# COMMAND ----------

# DBTITLE 1,Gate de calidad — el pipeline falla si la calidad crítica no pasa
# Ejecuta el motor de E5 y corta el pipeline si una regla de criticidad alta falla.
resultado = dbutils.notebook.run("../E5_calidad_datos/reglas_calidad", 600)

fallas_criticas = spark.sql("""
  SELECT count(*) c
  FROM workspace.yelp_gov.dq_resultados res
  JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
  WHERE r.criticidad = 'alta' AND NOT res.paso
    AND res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
""").first().c
assert fallas_criticas == 0, f"❌ {fallas_criticas} reglas críticas fallaron — pipeline detenido (así se ve la calidad como GATE, no como reporte)"
print("✔ Gate de calidad superado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [ ] Todas las entidades silver pobladas conforme al DDL de E3.
# MAGIC - [ ] Deduplicación de reseñas con criterio documentado + tabla de cuarentena operativa.
# MAGIC - [ ] Check-ins explotados a grano evento.
# MAGIC - [ ] Gate de calidad integrado y funcionando.
