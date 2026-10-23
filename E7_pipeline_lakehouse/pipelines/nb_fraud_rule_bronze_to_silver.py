# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_fraud_rule`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_fraud_rule`
# MAGIC
# MAGIC Conforma el **catálogo de reglas de fraude** de Trust & Safety. No es solo una tabla de referencia: los
# MAGIC notebooks gold leen de aquí qué reglas están activas, con qué umbral, con qué mínimo de reseñas y cuántos
# MAGIC puntos suman. Cambiar un parámetro en la semilla cambia el resultado del pipeline.
# MAGIC Fuente **simulada** del caso (no forma parte del Yelp Open Dataset): su origen se declara en el diccionario.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa la tabla `slv_fraud_rule` del modelo físico (E3).
# MAGIC > Silver no redefine el modelo: escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

ORIGEN  = "workspace.yelp_bronze.brz_fraud_rule"
DESTINO = "workspace.yelp_silver.slv_fraud_rule"
EQUIPOS = "workspace.yelp_silver.slv_trust_safety_area"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver
brz = spark.table(ORIGEN)

def texto_normalizado(c):
    return F.lower(F.trim(F.col(c)))

slv = (brz
    .select(
        F.col("fraud_rule_id").cast("bigint").alias("fraud_rule_id"),
        F.trim(F.col("fraud_description")).alias("fraud_description"),
        texto_normalizado("entidad").alias("entidad"),                 # usuario | negocio (CHECK)
        F.col("umbral").cast("double").alias("umbral"),
        F.col("min_resenas").cast("int").alias("min_resenas"),
        F.col("puntos").cast("int").alias("puntos"),                   # 0 a 100 (CHECK)
        texto_normalizado("risk_level").alias("risk_level"),           # bajo | medio | alto (CHECK)
        texto_normalizado("rule_status").alias("rule_status"),         # activa | inactiva (CHECK)
        F.col("team_id").cast("bigint").alias("team_id"),
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("fraud_rule_id").isNotNull())
    .dropDuplicates(["fraud_rule_id"])
)

# COMMAND ----------

# DBTITLE 1,Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

slv.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
df = spark.table(DESTINO)
r = df.agg(
    F.count("*").alias("filas"),
    F.countDistinct("fraud_rule_id").alias("pk_distintas"),
    F.count_if(F.col("rule_status") == "activa").alias("activas"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ fraud_rule_id duplicado: {r.filas - r.pk_distintas} filas"

# La FK a equipos es informativa en Unity Catalog: se verifica aquí
sin_equipo = (df.filter(F.col("team_id").isNotNull())
    .join(spark.table(EQUIPOS), "team_id", "left_anti")
    .select("fraud_rule_id").collect())
assert not sin_equipo, f"❌ Reglas con team_id que no existe en {EQUIPOS}: {[x.fraud_rule_id for x in sin_equipo]}"
print(f"✔ {DESTINO}: {r.filas} reglas ({r.activas} activas) · PK única · equipos válidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Textos de dominio en minúsculas antes de insertar: así un `Activa` o un `ALTO` de la semilla cumple los CHECK.
# MAGIC   Un valor fuera de dominio (por ejemplo `activo`) **hace fallar la carga**: una regla mal escrita no se
# MAGIC   apaga en silencio.
# MAGIC - `umbral` tiene un significado propio en cada regla (reseñas por día, porcentaje, días, multiplicador, ratio);
# MAGIC   la descripción de la regla lo explica en lenguaje de negocio.
# MAGIC - La FK `team_id → slv_trust_safety_area` se valida aquí porque en Unity Catalog es solo informativa.
# MAGIC   Este notebook depende de que `slv_trust_safety_area` esté cargada.
# MAGIC
# MAGIC ### Definition of Done (slv_fraud_rule)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [ ] Umbrales y puntos aprobados por Trust & Safety y registrados en el Documento Formal.
# MAGIC - [ ] Origen de la fuente simulada documentado (sistema fuente en el diccionario).
# MAGIC - [x] Origen `brz_fraud_rule` con metadata de auditoría (E7.1).
