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
# MAGIC sospechosa que aplica las reglas de negocio activas del catálogo `slv_fraud_rule`, con sus parámetros.
# MAGIC Lógica según el mapeo silver → gold del grupo (`mapeo_equipo_02_utec.xlsx`).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs» del
# MAGIC > **Documento Formal de Arquitectura de Datos** para Trust & Safety, y materializa `gld_business_anomaly_bursts`
# MAGIC > del modelo físico (E3). Escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SLV = "workspace.yelp_silver"
DESTINO = "workspace.yelp_gold.gld_business_anomaly_bursts"

# COMMAND ----------

# DBTITLE 1,Actividad diaria por negocio (reseñas + check-ins)
resenas_dia = (spark.table(f"{SLV}.slv_review")
    .groupBy("business_id", F.to_date("fecha_publicacion").alias("fecha_ventana"))
    .agg(
        F.count("*").alias("resenas_en_ventana"),
        F.avg("calificacion").alias("promedio_estrellas_ventana"),
        F.count_if(F.col("calificacion") == 1).alias("resenas_una_estrella"),
    )
)

checkins_dia = (spark.table(f"{SLV}.slv_checkin")
    .groupBy("business_id", F.to_date("fecha_checkin").alias("fecha_ventana"))
    .agg(F.count("*").alias("checkins_en_ventana"))
)

# Full outer: un día puede tener reseñas sin check-ins y viceversa
actividad = (resenas_dia
    .join(checkins_dia, ["business_id", "fecha_ventana"], "full_outer")
    .fillna(0, subset=["resenas_en_ventana", "checkins_en_ventana", "resenas_una_estrella"])
)

# Promedio diario histórico del negocio = promedio de reseñas en los días en que recibió al menos una
w_negocio = Window.partitionBy("business_id")
actividad = (actividad
    .withColumn("promedio_historico",
                F.avg(F.when(F.col("resenas_en_ventana") > 0, F.col("resenas_en_ventana"))).over(w_negocio))
    .withColumn("ratio_resenas_checkins",
                F.round(F.col("resenas_en_ventana") / F.when(F.col("checkins_en_ventana") > 0, F.col("checkins_en_ventana")), 4))
)

# COMMAND ----------

# DBTITLE 1,Detección de ráfagas — reglas y parámetros del catálogo slv_fraud_rule
# El código define la FORMA de cada regla; umbral, mínimo de reseñas y estado vienen del catálogo.
CONDICIONES = {
    # fraud_rule_id: (parámetros obligatorios, condición en función de la fila del catálogo)
    5: ({"umbral"}, lambda r: F.col("resenas_en_ventana") > r.umbral * F.col("promedio_historico")),
    6: ({"umbral"}, lambda r: F.col("ratio_resenas_checkins") > r.umbral),
    9: ({"umbral"}, lambda r: F.col("resenas_una_estrella") >= r.umbral),
}

catalogo = spark.table(f"{SLV}.slv_fraud_rule").filter("entidad = 'negocio' AND rule_status = 'activa'").collect()

sin_logica = sorted(r.fraud_rule_id for r in catalogo if r.fraud_rule_id not in CONDICIONES)
if sin_logica:
    print(f"⚠️ Reglas de negocio activas sin implementación (no se aplican): {sin_logica}")

activadas = []
for r in (x for x in catalogo if x.fraud_rule_id in CONDICIONES):
    requeridos, condicion = CONDICIONES[r.fraud_rule_id]
    faltan = [p for p in requeridos if getattr(r, p) is None]
    assert not faltan, f"❌ La regla {r.fraud_rule_id} está activa pero le falta {faltan} en el catálogo"
    cond = condicion(r) & (F.col("resenas_en_ventana") >= (r.min_resenas or 0))
    activadas.append(F.when(cond, F.lit(r.fraud_rule_id)))

gld = (actividad
    .withColumn("reglas_activadas", F.array_compact(F.array(*activadas).cast("array<bigint>")))
    .select(
        "business_id",
        "fecha_ventana",
        F.col("resenas_en_ventana").cast("int").alias("resenas_en_ventana"),
        F.round("promedio_estrellas_ventana", 2).alias("promedio_estrellas_ventana"),
        F.col("checkins_en_ventana").cast("int").alias("checkins_en_ventana"),
        "ratio_resenas_checkins",
        "reglas_activadas",
        (F.size("reglas_activadas") > 0).alias("es_rafaga_sospechosa"),
        F.current_timestamp().alias("fecha_actualizacion"),
    )
    .filter(F.col("fecha_ventana").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

gld.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
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
# MAGIC - Un negocio-día es ráfaga sospechosa si se cumple al menos una regla de negocio **activa** del catálogo:
# MAGIC   regla 5 (reseñas del día > umbral × promedio diario del negocio), regla 6 (ratio reseñas / check-ins > umbral)
# MAGIC   o regla 9 (reseñas de 1 estrella en el día ≥ umbral). Umbral y mínimo de reseñas vienen de `slv_fraud_rule`.
# MAGIC - El promedio diario del negocio se calcula sobre sus días con al menos una reseña.
# MAGIC - Un día sin check-ins no activa la regla 6: su ratio es nulo.
# MAGIC - `reglas_activadas` guarda qué reglas se cumplieron en cada negocio-día (linaje regla → caso).
# MAGIC
# MAGIC ### Definition of Done (gld_business_anomaly_bursts)
# MAGIC - [x] Corre de inicio a fin y la validación pasa.
# MAGIC - [ ] Umbrales de ráfaga aprobados por el Data Owner (Trust & Safety).
# MAGIC - [ ] Pregunta de negocio registrada en la sección 7 del Documento Formal y visual asociado en E8.
