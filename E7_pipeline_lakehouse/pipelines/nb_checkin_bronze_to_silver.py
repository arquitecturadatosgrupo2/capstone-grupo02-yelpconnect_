# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_checkin`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_checkin`
# MAGIC
# MAGIC Conforma la entidad **Check-in** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`).
# MAGIC El campo `date` llega como STRING con fechas separadas por coma: se **explota a grano evento**
# MAGIC (1 fila = 1 check-in). Resuelve el dolor de Operaciones Comerciales: un único conteo oficial de check-ins.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_checkin` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_checkin"
DESTINO = "workspace.yelp_silver.slv_checkin"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver (explode a grano evento)
brz = spark.table(ORIGEN)

slv = (brz
    .filter(F.col("business_id").isNotNull())
    .select(
        F.col("business_id"),
        F.explode(F.split(F.col("date"), ",")).alias("fecha_txt"),
    )
    .select(
        F.col("business_id"),
        F.to_timestamp(F.trim(F.col("fecha_txt"))).alias("fecha_checkin"),
    )
    .filter(F.col("fecha_checkin").isNotNull())
    # Llave sustituta determinística: el mismo evento recibe el mismo id en cada corrida
    .withColumn("checkin_id", F.xxhash64("business_id", "fecha_checkin"))
    .withColumn("fecha_carga", F.current_timestamp())
    .dropDuplicates(["checkin_id"])
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
    F.countDistinct("checkin_id").alias("pk_distintas"),
    F.countDistinct("business_id").alias("negocios"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ checkin_id duplicado: {r.filas - r.pk_distintas} filas"
assert r.filas > r.negocios, "❌ El explode no se aplicó: hay un solo check-in por negocio"
print(f"✔ {DESTINO}: {r.filas:,} check-ins en {r.negocios:,} negocios · PK única")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Grano evento (no agregado diario): conserva el máximo detalle y gold agrega según cada producto
# MAGIC   (p. ej. `checkins_en_ventana` en el análisis de ráfagas).
# MAGIC - `checkin_id = xxhash64(business_id, fecha_checkin)`: determinístico entre corridas. Dos check-ins del mismo
# MAGIC   negocio en el mismo segundo cuentan como uno — supuesto aceptado y a documentar en el glosario
# MAGIC   («check-in válido»).
# MAGIC - Fechas que no parsean se descartan; su volumen debe medirse con una regla DQ de validez.
# MAGIC
# MAGIC ### Definition of Done (slv_checkin)
# MAGIC - [x] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [x] Definición de «check-in válido» del glosario coherente con este grano.
# MAGIC - [x] Origen `brz_checkin` con metadata de auditoría (E7.1).
