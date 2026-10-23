# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: `gld_business_anomaly_bursts`
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo) · **Tarea del job:** `GOLD_business_anomaly_bursts`
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_business_anomaly_bursts` | Trust & Safety (Rafael Nakamura) · Ops Comerciales (Carla Romero) | ¿Qué negocios reciben ráfagas de reseñas que no se explican por sus visitas reales (check-ins)? |
# MAGIC
# MAGIC Un registro por negocio y día con reseñas y check-ins de la ventana, su ratio y una bandera de ráfaga
# MAGIC sospechosa (reglas 5 y 6 de `slv_fraud_rule`). Lógica según el mapeo silver → gold del grupo
# MAGIC (`mapeo_equipo_02_utec.xlsx`).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs» del
# MAGIC > **Documento Formal de Arquitectura de Datos** para Trust & Safety, y materializa `gld_business_anomaly_bursts`
# MAGIC > del modelo físico (E3). Escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SLV = "workspace.yelp_silver"
DESTINO = "workspace.yelp_gold.gld_business_anomaly_bursts"

FACTOR_RAFAGA = 3      # regla 5: más del triple del promedio diario del negocio
MIN_RESENAS_RAFAGA = 3 # evita marcar como ráfaga a negocios con 1 reseña/día habitual y un día de 2

# COMMAND ----------

# DBTITLE 1: Actividad diaria por negocio (reseñas + check-ins)
resenas_dia = (spark.table(f"{SLV}.slv_review")
    .groupBy("business_id", F.to_date("fecha_publicacion").alias("fecha_ventana"))
    .agg(
        F.count("*").alias("resenas_en_ventana"),
        F.avg("calificacion").alias("promedio_estrellas_ventana"),
    )
)

checkins_dia = (spark.table(f"{SLV}.slv_checkin")
    .groupBy("business_id", F.to_date("fecha_checkin").alias("fecha_ventana"))
    .agg(F.count("*").alias("checkins_en_ventana"))
)

# Full outer: un día puede tener reseñas sin check-ins y viceversa
actividad = (resenas_dia
    .join(checkins_dia, ["business_id", "fecha_ventana"], "full_outer")
    .fillna(0, subset=["resenas_en_ventana", "checkins_en_ventana"])
)

# COMMAND ----------

# DBTITLE 1: Ratio y detección de ráfagas
# Promedio diario histórico del negocio = promedio de reseñas en los días en que recibió al menos una
w_negocio = Window.partitionBy("business_id")
promedio_historico = F.avg(F.when(F.col("resenas_en_ventana") > 0, F.col("resenas_en_ventana"))).over(w_negocio)

gld = (actividad
    .withColumn("promedio_historico", promedio_historico)
    .select(
        "business_id",
        "fecha_ventana",
        F.col("resenas_en_ventana").cast("int").alias("resenas_en_ventana"),
        F.round("promedio_estrellas_ventana", 2).alias("promedio_estrellas_ventana"),
        F.col("checkins_en_ventana").cast("int").alias("checkins_en_ventana"),
        F.round(F.col("resenas_en_ventana") / F.when(F.col("checkins_en_ventana") > 0, F.col("checkins_en_ventana")), 4)
         .alias("ratio_resenas_checkins"),
        ((F.col("resenas_en_ventana") >= MIN_RESENAS_RAFAGA)
         & (F.col("resenas_en_ventana") > FACTOR_RAFAGA * F.col("promedio_historico")))
         .alias("es_rafaga_sospechosa"),
        F.current_timestamp().alias("fecha_actualizacion"),
    )
    .filter(F.col("fecha_ventana").isNotNull())
)

# COMMAND ----------

# DBTITLE 1: Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

gld.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1: Validación automática
r = spark.table(DESTINO).agg(
    F.count("*").alias("filas"),
    F.countDistinct("business_id", "fecha_ventana").alias("pk_distintas"),
    F.count_if("es_rafaga_sospechosa").alias("rafagas"),
    F.countDistinct(F.when(F.col("es_rafaga_sospechosa"), F.col("business_id"))).alias("negocios_con_rafaga"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ (business_id, fecha_ventana) duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas:,} negocio-día · PK única · {r.rafagas:,} ráfagas en {r.negocios_con_rafaga:,} negocios")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Ventana diaria (`fecha_ventana` = día). Se incluyen días con reseñas, con check-ins o con ambos.
# MAGIC - `ratio_resenas_checkins` queda nulo si el día no tuvo check-ins (se evita la división por cero).
# MAGIC - Ráfaga sospechosa (regla 5): ≥ 3 reseñas en el día **y** más del triple del promedio diario del negocio,
# MAGIC   calculado sobre sus días con reseñas. Umbrales propuestos por el equipo: validarlos con Trust & Safety.
# MAGIC - La regla 6 (muchas más reseñas que check-ins) se expone vía `ratio_resenas_checkins` para que el
# MAGIC   dashboard aplique el corte; no se fija aquí un umbral sin acuerdo del Data Owner.
# MAGIC
# MAGIC ### Definition of Done (gld_business_anomaly_bursts)
# MAGIC - [ ] Corre de inicio a fin y la validación pasa.
# MAGIC - [ ] Umbrales de ráfaga aprobados por el Data Owner (Trust & Safety).
# MAGIC - [ ] Pregunta de negocio registrada en la sección 7 del Documento Formal y visual asociado en E8.
