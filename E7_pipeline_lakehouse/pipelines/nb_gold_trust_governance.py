# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: `gld_trust_governance`
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo) · **Tarea del job:** `GOLD_trust_governance`
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_trust_governance` | CDO (Laura Hernández) · Trust & Safety (Rafael Nakamura) | ¿Qué equipo responde por cada regla de fraude, cuántos casos marcó en la última corrida y qué reglas tienen un problema de gobierno? |
# MAGIC
# MAGIC Una fila por regla del catálogo `slv_fraud_rule`, con sus parámetros vigentes, el equipo responsable, los
# MAGIC casos que marcó (desde `reglas_activadas` de los otros dos productos) y una alerta cuando la regla está activa
# MAGIC pero no puede operar bien.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Estrategia de gobierno y matriz RACI» del
# MAGIC > **Documento Formal de Arquitectura de Datos** (ownership de las reglas de fraude) y alimenta la página de
# MAGIC > gobierno del dashboard (E8). Escribe sobre la tabla creada por el DDL de E3.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

SLV = "workspace.yelp_silver"
GLD = "workspace.yelp_gold"
DESTINO = f"{GLD}.gld_trust_governance"

# Reglas con lógica en el pipeline. Debe coincidir con CONDICIONES de
# nb_gold_user_fraud_profile (usuario) y nb_gold_business_anomaly_bursts (negocio).
IMPLEMENTADAS = {1, 2, 3, 4, 7, 5, 6, 9}

# COMMAND ----------

# DBTITLE 1,Casos marcados por regla en la última corrida
casos = (
    spark.table(f"{GLD}.gld_user_fraud_profile").select(F.explode("reglas_activadas").alias("fraud_rule_id"))
    .unionByName(
    spark.table(f"{GLD}.gld_business_anomaly_bursts").select(F.explode("reglas_activadas").alias("fraud_rule_id")))
    .groupBy("fraud_rule_id")
    .agg(F.count("*").alias("n_casos_marcados"))
)

# COMMAND ----------

# DBTITLE 1,Regla + equipo + casos + alerta de gobierno
reglas = spark.table(f"{SLV}.slv_fraud_rule")
equipos = spark.table(f"{SLV}.slv_trust_safety_area").select("team_id", "team_name", "responsable", "team_status")

activa = F.col("rule_status") == "activa"
implementada = F.col("fraud_rule_id").isin(*IMPLEMENTADAS)

gld = (reglas
    .join(equipos, "team_id", "left")
    .join(casos, "fraud_rule_id", "left")
    .select(
        "fraud_rule_id", "fraud_description", "entidad", "umbral", "puntos", "risk_level", "rule_status",
        implementada.alias("esta_implementada"),
        F.coalesce("n_casos_marcados", F.lit(0)).cast("bigint").alias("n_casos_marcados"),
        "team_id", "team_name", "responsable", "team_status",
        F.when(activa & ~implementada, "Regla activa sin implementación en el pipeline")
         .when(activa & F.col("team_id").isNull(), "Regla activa sin equipo responsable")
         .when(activa & F.col("team_name").isNull(), "Regla activa asignada a un equipo que no existe")
         .when(activa & (F.col("team_status") != "activo"), "Regla activa asignada a un equipo inactivo")
         .alias("alerta_gobierno"),
        F.current_timestamp().alias("fecha_actualizacion"),
    )
)

# COMMAND ----------

# DBTITLE 1,Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

gld.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
df = spark.table(DESTINO)
r = df.agg(
    F.count("*").alias("filas"),
    F.countDistinct("fraud_rule_id").alias("pk_distintas"),
    F.count_if(F.col("alerta_gobierno").isNotNull()).alias("alertas"),
).first()
n_catalogo = spark.table(f"{SLV}.slv_fraud_rule").count()

assert r.filas == n_catalogo, f"❌ Se esperaba una fila por regla del catálogo ({n_catalogo}) y hay {r.filas}"
assert r.filas == r.pk_distintas, f"❌ fraud_rule_id duplicado: {r.filas - r.pk_distintas} filas"
print(f"✔ {DESTINO}: {r.filas} reglas · {r.alertas} con alerta de gobierno")
for x in df.filter("alerta_gobierno IS NOT NULL").orderBy("fraud_rule_id").collect():
    print(f"   ⚠️ Regla {x.fraud_rule_id}: {x.alerta_gobierno}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Grano: una fila por regla del catálogo, activa o no, para que la CDO vea el catálogo completo.
# MAGIC - `n_casos_marcados` cuenta usuarios (reglas de usuario) o negocio-día (reglas de negocio) en la última corrida;
# MAGIC   sale de `reglas_activadas`, así que hay linaje directo regla → caso.
# MAGIC - `alerta_gobierno` marca solo reglas **activas** que no pueden operar bien: sin lógica en el pipeline, sin
# MAGIC   equipo, con un equipo que no existe o con un equipo inactivo.
# MAGIC - `IMPLEMENTADAS` repite qué reglas tienen lógica en los otros dos notebooks gold: si se implementa una regla
# MAGIC   nueva, hay que agregarla en ambos lugares.
# MAGIC
# MAGIC ### Definition of Done (gld_trust_governance)
# MAGIC - [x] Corre de inicio a fin y la validación pasa.
# MAGIC - [ ] Alertas de gobierno revisadas con Trust & Safety y decisión registrada.
# MAGIC - [x] Visual asociado en la página de gobierno del dashboard (E8, REQ-09).
