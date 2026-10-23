# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: esquema estrella
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo) · **Tarea del job:** `GOLD_modelo_dimensional`
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_fact_resena` + dimensiones | Producto (Andrew Collins) · Marketing (Jessica Martinez) | ¿Cómo evolucionan el volumen y la calificación de las reseñas por negocio, ciudad, categoría y periodo? |
# MAGIC | `gld_fact_checkin_diario` + dimensiones | Operaciones Comerciales (Carla Romero) | ¿Cuál es el conteo **oficial** de check-ins por negocio y día? |
# MAGIC | Estrella completa | Data Science (Mei Lin) | Base estable y documentada para entrenar modelos sin rehacer joins sobre silver |
# MAGIC
# MAGIC Dimensiones: `gld_dim_fecha`, `gld_dim_negocio`, `gld_dim_usuario`. Hechos: `gld_fact_resena`, `gld_fact_checkin_diario`.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la «estrategia dimensional» de la sección «Modelo de datos
# MAGIC > conceptual y lógico» del **Documento Formal de Arquitectura de Datos**, y materializa las tablas `gld_dim_*` /
# MAGIC > `gld_fact_*` del modelo físico (E3). Escribe sobre las tablas creadas por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1: Parámetros
from pyspark.sql import functions as F

SLV = "workspace.yelp_silver"
GLD = "workspace.yelp_gold"

def cargar(df, tabla):
    """Escribe sobre la tabla del DDL de E3 (conserva COMMENT y constraints)."""
    destino = f"{GLD}.{tabla}"
    assert spark.catalog.tableExists(destino), f"❌ {destino} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"
    df.select(*spark.table(destino).columns).write.insertInto(destino, overwrite=True)
    print(f"✔ {destino} cargada")

def fecha_id(col):
    return F.date_format(col, "yyyyMMdd").cast("int")

resenas = spark.table(f"{SLV}.slv_review")
checkins = spark.table(f"{SLV}.slv_checkin")

# COMMAND ----------

# DBTITLE 1: gld_dim_fecha — generada programáticamente (sequence + explode)
rango = (resenas.select(F.to_date("fecha_publicacion").alias("f"))
    .union(checkins.select(F.to_date("fecha_checkin").alias("f")))
    .agg(F.min("f").alias("desde"), F.max("f").alias("hasta"))
    .first())

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]

dia_iso = ((F.dayofweek("fecha") + 5) % 7) + 1     # dayofweek: 1 = domingo → ISO: 1 = lunes

dim_fecha = (spark.range(1)
    .select(F.explode(F.sequence(F.lit(rango.desde), F.lit(rango.hasta))).alias("fecha"))
    .select(
        fecha_id(F.col("fecha")).alias("fecha_id"),
        "fecha",
        F.year("fecha").alias("anio"),
        F.quarter("fecha").alias("trimestre"),
        F.month("fecha").alias("mes"),
        F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("fecha")).alias("nombre_mes"),
        F.dayofmonth("fecha").alias("dia"),
        dia_iso.alias("dia_semana"),
        F.element_at(F.array(*[F.lit(d) for d in DIAS]), dia_iso).alias("nombre_dia"),
        (dia_iso >= 6).alias("es_fin_de_semana"),
    )
)
cargar(dim_fecha, "gld_dim_fecha")

# COMMAND ----------

# DBTITLE 1: gld_dim_negocio y gld_dim_usuario (SCD tipo 1)
dim_negocio = spark.table(f"{SLV}.slv_business").select(
    "business_id", "nombre", "ciudad", "estado", "categorias",
    F.element_at("categorias", 1).alias("categoria_principal"),
    "estrellas", "esta_abierto",
    F.current_timestamp().alias("fecha_actualizacion"),
)
cargar(dim_negocio, "gld_dim_negocio")

usuarios = spark.table(f"{SLV}.slv_user").select(
    "user_id",                                         # sin nombre: gold no expone PII directa
    "miembro_desde",
    F.year("miembro_desde").alias("anio_registro"),
    F.col("n_resenas").alias("n_resenas_historico"),
    "fans",
    (F.col("elite").isNotNull() & (F.trim("elite") != "") & (F.trim("elite") != "None")).alias("es_elite"),
    "estado_cuenta",
    F.lit(False).alias("es_miembro_inferido"),
)

# Miembros inferidos: autores de reseñas que no están en slv_user (el muestreo recorta usuarios a 150 mil).
# Se crean con atributos nulos para no perder reseñas y mantener la FK del hecho.
inferidos = (resenas.select("user_id").distinct()
    .join(usuarios.select("user_id"), "user_id", "left_anti")
    .select(
        "user_id",
        F.lit(None).cast("timestamp").alias("miembro_desde"),
        F.lit(None).cast("int").alias("anio_registro"),
        F.lit(None).cast("bigint").alias("n_resenas_historico"),
        F.lit(None).cast("bigint").alias("fans"),
        F.lit(None).cast("boolean").alias("es_elite"),
        F.lit(None).cast("string").alias("estado_cuenta"),
        F.lit(True).alias("es_miembro_inferido"),
    )
)

dim_usuario = usuarios.unionByName(inferidos).withColumn("fecha_actualizacion", F.current_timestamp())
cargar(dim_usuario, "gld_dim_usuario")

# COMMAND ----------

# DBTITLE 1: gld_fact_resena y gld_fact_checkin_diario
fact_resena = resenas.select(
    "review_id",
    fecha_id(F.col("fecha_publicacion")).alias("fecha_id"),
    "business_id",
    "user_id",
    "calificacion",
    F.col("calificacion").isin(1, 5).alias("es_extrema"),
    "util", "divertido", "cool",
    F.length("comentario").alias("n_caracteres"),     # el texto queda en silver (PII indirecta)
)
cargar(fact_resena, "gld_fact_resena")

fact_checkin = (checkins
    .groupBy("business_id", fecha_id(F.col("fecha_checkin")).alias("fecha_id"))
    .agg(F.count("*").cast("int").alias("n_checkins"))
)
cargar(fact_checkin, "gld_fact_checkin_diario")

# COMMAND ----------

# DBTITLE 1: Validación automática — PK únicas e integridad referencial hechos → dimensiones
def pk_unica(tabla, *pk):
    r = spark.table(f"{GLD}.{tabla}").agg(F.count("*").alias("n"), F.countDistinct(*pk).alias("d")).first()
    assert r.n > 0, f"❌ {tabla} quedó vacía"
    assert r.n == r.d, f"❌ {tabla}: {r.n - r.d} filas con PK duplicada"
    return r.n

def huerfanas(hecho, dim, clave):
    return (spark.table(f"{GLD}.{hecho}")
            .join(spark.table(f"{GLD}.{dim}"), clave, "left_anti").count())

conteos = {
    "gld_dim_fecha": pk_unica("gld_dim_fecha", "fecha_id"),
    "gld_dim_negocio": pk_unica("gld_dim_negocio", "business_id"),
    "gld_dim_usuario": pk_unica("gld_dim_usuario", "user_id"),
    "gld_fact_resena": pk_unica("gld_fact_resena", "review_id"),
    "gld_fact_checkin_diario": pk_unica("gld_fact_checkin_diario", "business_id", "fecha_id"),
}

fallas = {
    f"{h} → {d}": n for h, d, c in [
        ("gld_fact_resena", "gld_dim_fecha", "fecha_id"),
        ("gld_fact_resena", "gld_dim_negocio", "business_id"),
        ("gld_fact_resena", "gld_dim_usuario", "user_id"),
        ("gld_fact_checkin_diario", "gld_dim_fecha", "fecha_id"),
        ("gld_fact_checkin_diario", "gld_dim_negocio", "business_id"),
    ] if (n := huerfanas(h, d, c)) > 0
}
assert not fallas, f"❌ Filas de hechos sin dimensión: {fallas}"
n_inferidos = spark.table(f"{GLD}.gld_dim_usuario").filter("es_miembro_inferido").count()
print(f"✔ Esquema estrella válido · {conteos} · {n_inferidos:,} usuarios inferidos · fechas {rango.desde} a {rango.hasta}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **SCD tipo 1** en negocio y usuario: la muestra del Yelp Open Dataset es una sola foto, sin historia de cambios
# MAGIC   que versionar. Si el origen pasara a entregar cambios, `gld_dim_negocio` es la candidata a SCD tipo 2
# MAGIC   (cambios de categoría o de estado abierto/cerrado).
# MAGIC - **Claves naturales** (`business_id`, `user_id`) en lugar de surrogate keys: con SCD 1 la clave del origen ya es
# MAGIC   estable y única, y evita un lookup extra al cargar los hechos. `fecha_id` usa el formato `yyyymmdd`.
# MAGIC - **Minimización de PII en gold:** `gld_dim_usuario` no lleva el nombre y `gld_fact_resena` no lleva el texto
# MAGIC   (solo su longitud). Quien necesite esos datos los consulta en silver bajo los controles de E6.
# MAGIC - **Miembros inferidos en `gld_dim_usuario`:** ~6,7 % de las reseñas son de usuarios que no están en `slv_user`,
# MAGIC   porque `datos/preparar_muestra_yelp.py` recorta los usuarios a una muestra aleatoria de 150 mil. Se agregan
# MAGIC   a la dimensión con atributos nulos y `es_miembro_inferido = TRUE`: no se pierden reseñas y la FK se cumple.
# MAGIC   Los análisis de perfil de usuario deben filtrar `es_miembro_inferido = FALSE`.
# MAGIC - `gld_dim_fecha` cubre de la fecha más antigua a la más reciente entre reseñas y check-ins, sin huecos.
# MAGIC - `categoria_principal` = primera categoría declarada por el negocio; es una simplificación para agrupar en BI.
# MAGIC
# MAGIC ### Definition of Done (esquema estrella)
# MAGIC - [ ] Corre de inicio a fin y la validación (PK + integridad referencial) pasa.
# MAGIC - [ ] Estrategia dimensional (grano, SCD, claves) reflejada en la sección 4 del Documento Formal.
# MAGIC - [ ] Lineage silver → gold visible en Catalog Explorer (captura en `img/`).
