# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: `gld_user_fraud_profile`
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo) · **Tarea del job:** `GOLD_user_fraud_profile`
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_user_fraud_profile` | Trust & Safety (Rafael Nakamura) | ¿Qué usuarios se comportan como bots o granjas de reseñas y con qué nivel de riesgo? |
# MAGIC
# MAGIC Un registro por usuario con señales de comportamiento calculadas sobre sus reseñas y un **score de riesgo**
# MAGIC que aplica las reglas activas del catálogo `slv_fraud_rule`. Lógica según el mapeo silver → gold del grupo
# MAGIC (`mapeo_equipo_02_utec.xlsx`).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs» del
# MAGIC > **Documento Formal de Arquitectura de Datos** para Trust & Safety, y materializa `gld_user_fraud_profile`
# MAGIC > del modelo físico (E3). Escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

SLV = "workspace.yelp_silver"
DESTINO = "workspace.yelp_gold.gld_user_fraud_profile"

# COMMAND ----------

# DBTITLE 1: Señales de comportamiento por usuario
resenas = spark.table(f"{SLV}.slv_review")
usuarios = spark.table(f"{SLV}.slv_user")

por_resena = (resenas
    .groupBy("user_id")
    .agg(
        F.count("*").alias("total_resenas"),
        F.stddev_samp("calificacion").alias("desviacion_estandar_estrellas"),
        (F.avg(F.col("calificacion").isin(1, 5).cast("int")) * 100).alias("porcentaje_calificaciones_extremas"),
    )
)

pico_diario = (resenas
    .groupBy("user_id", F.to_date("fecha_publicacion").alias("dia"))
    .count()
    .groupBy("user_id")
    .agg(F.max("count").alias("max_resenas_en_un_dia"))
)

senales = (usuarios
    .join(por_resena, "user_id")            # solo usuarios con reseñas en la muestra
    .join(pico_diario, "user_id")
    .select(
        "user_id",
        "total_resenas",
        F.col("promedio_estrellas").alias("promedio_estrellas_otorgadas"),
        F.round("desviacion_estandar_estrellas", 4).alias("desviacion_estandar_estrellas"),
        F.round("porcentaje_calificaciones_extremas", 2).alias("porcentaje_calificaciones_extremas"),
        "max_resenas_en_un_dia",
        # Yelp guarda "None" cuando el usuario no tiene amigos
        F.when(F.col("amigos").isNull() | (F.trim("amigos") == "None"), 0)
         .otherwise(F.size(F.split("amigos", ",")))
         .alias("amigos_count"),
        F.col("fans").alias("fans_count"),
        F.datediff(F.current_date(), F.col("miembro_desde")).alias("antiguedad_dias"),
    )
)

# COMMAND ----------

# DBTITLE 1: Score de riesgo — reglas del catálogo slv_fraud_rule
# Cada regla suma puntos; el máximo es 100. Ids = fraud_rule_id del catálogo de Trust & Safety.
REGLAS = [
    # (fraud_rule_id, puntos, condición)
    (1, 30, F.col("max_resenas_en_un_dia") >= 5),
    (2, 25, (F.col("desviacion_estandar_estrellas") == 0) & (F.col("total_resenas") >= 10)),
    (3, 20, (F.col("porcentaje_calificaciones_extremas") > 90) & (F.col("total_resenas") >= 5)),
    (7, 15, (F.col("amigos_count") == 0) & (F.col("fans_count") == 0) & (F.col("total_resenas") >= 10)),
    (4, 10, F.col("antiguedad_dias") < 30),
]
activas = {r.fraud_rule_id for r in spark.table(f"{SLV}.slv_fraud_rule").filter("rule_status = 'activa'").collect()}

score = F.lit(0)
for rule_id, puntos, cond in REGLAS:
    if rule_id in activas:          # una regla inactivada por Trust & Safety deja de puntuar sin tocar el código
        score = score + F.when(cond, puntos).otherwise(0)

gld = (senales
    .withColumn("score_riesgo_usuario", score.cast("double"))
    .withColumn("clasificacion_riesgo",
        F.when(F.col("score_riesgo_usuario") >= 50, "Alto")
         .when(F.col("score_riesgo_usuario") >= 25, "Moderado")
         .otherwise("Bajo"))
    .withColumn("fecha_actualizacion", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1: Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

gld.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1: Validación automática
df = spark.table(DESTINO)
r = df.agg(
    F.count("*").alias("filas"),
    F.countDistinct("user_id").alias("pk_distintas"),
    F.count_if(~F.col("score_riesgo_usuario").between(0, 100)).alias("score_fuera_rango"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ user_id duplicado: {r.filas - r.pk_distintas} filas"
assert r.score_fuera_rango == 0, f"❌ {r.score_fuera_rango} scores fuera de 0-100"
dist = {x.clasificacion_riesgo: x["count"] for x in df.groupBy("clasificacion_riesgo").count().collect()}
print(f"✔ {DESTINO}: {r.filas:,} usuarios · PK única · distribución de riesgo {dist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Solo se perfilan usuarios con al menos una reseña en la muestra: sin reseñas no hay comportamiento que evaluar.
# MAGIC - `total_resenas`, la desviación, el % de extremas y el pico diario se calculan sobre `slv_review` (la muestra);
# MAGIC   `promedio_estrellas_otorgadas` viene de `slv_user` (histórico de Yelp), como indica el mapeo.
# MAGIC - Desviación con `stddev_samp`: es nula con una sola reseña, para no marcar como bot a quien escribió una vez.
# MAGIC - El score aplica solo las reglas **activas** de `slv_fraud_rule`. Pesos y cortes (Alto ≥ 50, Moderado ≥ 25)
# MAGIC   son una propuesta del equipo y deben validarse con Trust & Safety y documentarse en el Documento Formal.
# MAGIC - `antiguedad_dias` se mide contra la fecha actual (mapeo); como el dataset es histórico, la regla 4
# MAGIC   (cuentas de menos de 30 días) prácticamente no se activa. Alternativa a evaluar: medirla a la fecha de la reseña.
# MAGIC
# MAGIC ### Definition of Done (gld_user_fraud_profile)
# MAGIC - [ ] Corre de inicio a fin y la validación pasa.
# MAGIC - [ ] Pesos del score y cortes de clasificación aprobados por el Data Owner (Trust & Safety).
# MAGIC - [ ] Pregunta de negocio registrada en la sección 7 del Documento Formal y visual asociado en E8.
