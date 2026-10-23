# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: `slv_review` + `slv_review_cuarentena`
# MAGIC **Cierra con:** M6 Big Data · **Tarea del job:** `SILVER_review`
# MAGIC
# MAGIC Conforma la entidad **Reseña** según el mapeo bronze → silver del grupo (`mapeo_equipo_02_utec.xlsx`):
# MAGIC calificación a `SMALLINT`, fecha a `TIMESTAMP` y estado de moderación inicial. Ataca el dolor de Marketing
# MAGIC (reseñas contadas sin validar duplicados): cada reseña del origen termina **en silver o en cuarentena**, nunca
# MAGIC se descarta en silencio.
# MAGIC
# MAGIC | Motivo de cuarentena | Criterio | Qué se conserva en silver |
# MAGIC |---|---|---|
# MAGIC | `review_id_nulo` | La reseña llega sin identificador | Nada |
# MAGIC | `review_id_duplicado` | El mismo `review_id` llega más de una vez | La de ingesta más reciente |
# MAGIC | `duplicado_logico` | Mismo usuario, mismo negocio y mismo texto | La publicada primero |
# MAGIC | `negocio_inexistente` | El `business_id` no está en `slv_business` | Nada |
# MAGIC
# MAGIC Las reseñas cuyo autor no está en `slv_user` **se quedan** en silver con `autor_en_muestra = FALSE`: el
# MAGIC usuario existe en Yelp pero el muestreo lo recortó (riesgo aceptado, decisión del grupo).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos** y materializa las tablas `slv_review` y `slv_review_cuarentena`
# MAGIC > del modelo físico (E3). Silver no redefine el modelo: escribe sobre las tablas creadas por el DDL de E3.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ORIGEN     = "workspace.yelp_bronze.brz_review"
DESTINO    = "workspace.yelp_silver.slv_review"
CUARENTENA = "workspace.yelp_silver.slv_review_cuarentena"
NEGOCIOS   = "workspace.yelp_silver.slv_business"
USUARIOS   = "workspace.yelp_silver.slv_user"

# COMMAND ----------

# DBTITLE 1,Transformación según el mapeo bronze → silver
base = (spark.table(ORIGEN)
    .select(
        F.col("review_id"),
        F.col("user_id"),
        F.col("business_id"),
        F.col("stars").cast("smallint").alias("calificacion"),
        F.col("text").alias("comentario"),                     # PII indirecta: tag y control en E6
        F.col("useful").cast("bigint").alias("util"),
        F.col("funny").cast("bigint").alias("divertido"),
        F.col("cool").cast("bigint").alias("cool"),
        F.col("date").cast("timestamp").alias("fecha_publicacion"),
        F.col("_fecha_ingesta"),
    )
)

# COMMAND ----------

# DBTITLE 1,Reglas de integridad — cada fila sale con un motivo o sin él
# 1) review_id repetido: queda la ingesta más reciente (re-carga de un archivo por Auto Loader)
w_id = Window.partitionBy("review_id").orderBy(F.col("_fecha_ingesta").desc())
# 2) duplicado lógico: queda la publicada primero (desempate por review_id para que sea determinístico)
w_logico = Window.partitionBy("user_id", "business_id", "comentario").orderBy("fecha_publicacion", "review_id")

negocios = spark.table(NEGOCIOS).select("business_id", F.lit(True).alias("_negocio_existe"))

evaluadas = (base
    .withColumn("_rn_id", F.when(F.col("review_id").isNotNull(), F.row_number().over(w_id)))
    .join(negocios, "business_id", "left")
)
# El duplicado lógico se evalúa solo entre filas con id válido y único
unicas = evaluadas.filter(F.col("_rn_id") == 1)
unicas = (unicas
    .withColumn("_rn_logico", F.row_number().over(w_logico))
    .withColumn("_logico_ganador", F.first("review_id").over(w_logico))
)

motivo = (F.when(F.col("_negocio_existe").isNull(), "negocio_inexistente")
           .when(F.col("_rn_logico") > 1, "duplicado_logico"))

clasificadas = (unicas
    .withColumn("motivo", motivo)
    .withColumn("review_id_conservado", F.when(F.col("motivo") == "duplicado_logico", F.col("_logico_ganador")))
    .unionByName(evaluadas.filter(F.col("review_id").isNull())
        .withColumn("motivo", F.lit("review_id_nulo")).withColumn("review_id_conservado", F.lit(None).cast("string")),
        allowMissingColumns=True)
    .unionByName(evaluadas.filter(F.col("_rn_id") > 1)
        .withColumn("motivo", F.lit("review_id_duplicado")).withColumn("review_id_conservado", F.col("review_id")),
        allowMissingColumns=True)
    .withColumn("fecha_carga", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Marca de autor fuera de la muestra (no es motivo de cuarentena)
usuarios = spark.table(USUARIOS).select("user_id", F.lit(True).alias("_autor_existe"))

slv = (clasificadas.filter(F.col("motivo").isNull())
    .join(usuarios, "user_id", "left")
    .withColumn("autor_en_muestra", F.coalesce("_autor_existe", F.lit(False)))
    .withColumn("estado_resena", F.lit("aprobada"))           # estado inicial; la moderación la define Trust & Safety
)
cuarentena = clasificadas.filter(F.col("motivo").isNotNull())

# COMMAND ----------

# DBTITLE 1,Escritura sobre las tablas del DDL de E3 (conserva COMMENT y constraints)
for tabla in (DESTINO, CUARENTENA):
    assert spark.catalog.tableExists(tabla), f"❌ {tabla} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

slv.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
cuarentena.select(*spark.table(CUARENTENA).columns).write.insertInto(CUARENTENA, overwrite=True)
print(f"✔ {DESTINO} y {CUARENTENA} cargadas")

# COMMAND ----------

# DBTITLE 1,Validación automática
n_origen = spark.table(ORIGEN).count()
r = spark.table(DESTINO).agg(
    F.count("*").alias("filas"),
    F.countDistinct("review_id").alias("pk_distintas"),
    F.count_if(~F.col("autor_en_muestra")).alias("autor_fuera_muestra"),
).first()
q = spark.table(CUARENTENA)
n_cuarentena = q.count()
por_motivo = {x.motivo: x["count"] for x in q.groupBy("motivo").count().collect()}

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ review_id duplicado: {r.filas - r.pk_distintas} filas"
assert r.filas + n_cuarentena == n_origen, \
    f"❌ Se perdieron filas: origen {n_origen:,} ≠ silver {r.filas:,} + cuarentena {n_cuarentena:,}"
print(f"✔ {n_origen:,} reseñas de origen = {r.filas:,} en silver + {n_cuarentena:,} en cuarentena {por_motivo}")
print(f"  {r.autor_fuera_muestra:,} reseñas en silver con autor fuera de la muestra (autor_en_muestra = FALSE)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **Ninguna reseña se pierde:** la validación exige que silver + cuarentena sumen exactamente el origen.
# MAGIC - **Duplicado lógico** = mismo usuario, mismo negocio y mismo texto. En la muestra son republicaciones en días
# MAGIC   distintos (hasta 4 copias); se conserva la publicada primero y la cuarentena guarda cuál fue
# MAGIC   (`review_id_conservado`).
# MAGIC - **Autor fuera de la muestra:** `datos/preparar_muestra_yelp.py` recorta los usuarios a 150 mil al azar, así que
# MAGIC   ~6,7 % de las reseñas tienen un autor que no está en `slv_user`. El grupo decidió mantenerlas marcadas
# MAGIC   (`autor_en_muestra = FALSE`) porque la reseña es válida; gold las cubre con miembros inferidos. Debe
# MAGIC   registrarse como regla DQ de integridad con su umbral y la aceptación del Data Owner.
# MAGIC - `estado_resena` no existe en el origen: se inicializa en `'aprobada'` (mapeo).
# MAGIC - `chk_calificacion` (1 a 5) es enforzada: una calificación imposible detiene la carga (ver DQ-001).
# MAGIC - Depende de `slv_business` y `slv_user`, que deben cargarse antes en el job.
# MAGIC
# MAGIC ### Definition of Done (slv_review)
# MAGIC - [ ] Corre de inicio a fin sobre la muestra y la validación pasa.
# MAGIC - [x] Criterio de duplicados lógicos documentado + tabla de cuarentena operativa.
# MAGIC - [ ] Regla DQ de integridad de autor registrada en E5 con la aceptación del Data Owner.
# MAGIC - [x] Origen `brz_review` alimentada con Auto Loader (E7.1 / M7).
