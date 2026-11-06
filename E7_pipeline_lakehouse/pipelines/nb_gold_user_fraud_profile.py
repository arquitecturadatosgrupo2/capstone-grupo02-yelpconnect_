# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: `gld_user_fraud_profile`
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo) · **Tarea del job:** `GOLD_user_fraud_profile`
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_user_fraud_profile` | Trust & Safety (Rafael Nakamura) | ¿Qué usuarios se comportan como bots o granjas de reseñas y con qué nivel de riesgo? |
# MAGIC
# MAGIC Un registro por usuario con señales de comportamiento calculadas sobre sus reseñas y un **score de riesgo**
# MAGIC que aplica las reglas activas del catálogo `slv_fraud_rule` con los parámetros que ese catálogo define. Lógica según el mapeo silver → gold del grupo
# MAGIC (`mapeo_equipo_02_utec.xlsx`).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs» del
# MAGIC > **Documento Formal de Arquitectura de Datos** para Trust & Safety, y materializa `gld_user_fraud_profile`
# MAGIC > del modelo físico (E3). Escribe sobre la tabla creada por el DDL de E3, con sus COMMENT y constraints.

# COMMAND ----------

# DBTITLE 1,Parámetros
from pyspark.sql import functions as F

SLV = "workspace.yelp_silver"
GOLD = "workspace.yelp_gold"
DESTINO = f"{GOLD}.gld_user_fraud_profile"
BURSTS = f"{GOLD}.gld_business_anomaly_bursts"  # regla 4: reseña del día de registro dentro de ráfaga marcada

# COMMAND ----------

# DBTITLE 1,Señales de comportamiento por usuario
resenas = spark.table(f"{SLV}.slv_review")
usuarios = spark.table(f"{SLV}.slv_user")

# Comportamiento calculado sobre las reseñas de la muestra (misma población que gld_fact_resena)
por_resena = (resenas
    .groupBy("user_id")
    .agg(
        F.count("*").alias("total_resenas"),
        F.stddev_samp("calificacion").alias("desviacion_estandar_estrellas"),
        (F.avg(F.col("calificacion").isin(1, 5).cast("int")) * 100).alias("porcentaje_calificaciones_extremas"),
        F.min("calificacion").alias("min_calificacion"),
        F.max("calificacion").alias("max_calificacion"),
    )
)

pico_diario = (resenas
    .groupBy("user_id", F.to_date("fecha_publicacion").alias("dia"))
    .count()
    .groupBy("user_id")
    .agg(F.max("count").alias("max_resenas_en_un_dia"))
)

# --- Regla 4 rediseñada (auditoría 2026-11-02) ---
# Reseñas extremas (1★ o 5★) de menos de 250 caracteres escritas el MISMO DÍA del registro del autor.
# La versión acotada a ventanas con ráfaga marcada es la regla; la amplia es solo un score input.
mismo_dia = (resenas
    .select("user_id", "business_id",
            F.to_date("fecha_publicacion").alias("dia_resena"),
            "calificacion",
            F.length("comentario").alias("caracteres"))
    .join(usuarios.select("user_id", F.to_date("miembro_desde").alias("dia_registro")), "user_id")
    .where((F.col("dia_resena") == F.col("dia_registro"))
           & F.col("calificacion").isin(1, 5)
           & (F.col("caracteres") < 250))
)

amplia = mismo_dia.groupBy("user_id").agg(F.count("*").alias("n_mismo_dia"))

rafagas = (spark.table(BURSTS)
    .where("es_rafaga_sospechosa")
    .select("business_id", F.col("fecha_ventana").alias("dia")))

en_rafaga = (mismo_dia
    .join(rafagas, (mismo_dia["business_id"] == rafagas["business_id"]) & (mismo_dia["dia_resena"] == rafagas["dia"]))
    .groupBy("user_id")
    .agg(F.count("*").alias("n_mismo_dia_rafaga")))

# P0 (auditoría 2026-11-02): cobrar a TODOS los autores de la muestra, incluidos los 10,993 miembros
# inferidos que no están en slv_user. Sus atributos de origen quedan NULL, nunca 0: 0 amigos/fans es
# una señal real de fraude y no se fabrica para atributos desconocidos.
senales = (por_resena
    .join(pico_diario, "user_id")   # por_resena y pico_diario derivan de slv_review → cobertura total de autores
    .join(usuarios.select("user_id", "promedio_estrellas", "amigos", "fans", "miembro_desde"), "user_id", "left")
    .join(amplia, "user_id", "left")
    .join(en_rafaga, "user_id", "left")
    .select(
        "user_id",
        "total_resenas",
        # Promedio HISTÓRICO del archivo fuente (no el de la muestra): renombrado por auditoría
        F.col("promedio_estrellas").alias("promedio_estrellas_historico"),
        F.round("desviacion_estandar_estrellas", 4).alias("desviacion_estandar_estrellas"),
        F.round("porcentaje_calificaciones_extremas", 2).alias("porcentaje_calificaciones_extremas"),
        "max_resenas_en_un_dia",
        "min_calificacion",
        "max_calificacion",
        # Yelp guarda "None" cuando el usuario no tiene amigos (0 real); NULL (miembro inferido) se conserva NULL
        F.when(F.trim(F.col("amigos")) == "None", F.lit(0))
         .when(F.col("amigos").isNotNull(), F.size(F.split("amigos", ",")))
         .alias("amigos_count"),
        F.col("fans").alias("fans_count"),
        F.datediff(F.current_date(), F.col("miembro_desde")).alias("antiguedad_dias"),
        # Score input de la regla 4 (versión amplia): NULL si la fecha de registro es desconocida
        F.when(F.col("miembro_desde").isNull(), F.lit(None).cast("int"))
         .otherwise(F.coalesce(F.col("n_mismo_dia"), F.lit(0)).cast("int"))
         .alias("n_resenas_dia_registro"),
        # Condición de la regla 4 (versión acotada a ráfaga marcada)
        F.when(F.col("miembro_desde").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.coalesce(F.col("n_mismo_dia_rafaga"), F.lit(0)) > 0)
         .alias("es_dia_registro_en_rafaga"),
    )
)

# COMMAND ----------

# DBTITLE 1,Score de riesgo — reglas y parámetros del catálogo slv_fraud_rule
# El código define la FORMA de cada regla; umbral, mínimo de reseñas, puntos y estado vienen del catálogo.
# Si Trust & Safety cambia un parámetro o desactiva una regla, el score cambia sin tocar este notebook.
#
# Rediseños verificados contra gold (auditoría 2026-11-02, volúmenes reproducidos):
#   R2 — "todas 1★ o todas 5★" se evalúa con MIN = MAX sobre las MISMAS reseñas de la muestra; el promedio
#        histórico del archivo fuente mezcla poblaciones (coincide con la muestra solo en el 21% de usuarios)
#        y ya no se usa en ninguna condición.
#   R4 — edad de cuenta medida a la fecha de la reseña: reseña extrema de <250 caracteres escrita el día de
#        registro del autor, dentro de una ventana con ráfaga marcada. La versión amplia es solo score input.
#   R7 — sin amigos ni fans, con mínimo de reseñas y porcentaje de calificaciones extremas (umbral del catálogo).
CONDICIONES = {
    # fraud_rule_id: (parámetros obligatorios, condición en función de la fila del catálogo)
    1: ({"umbral"}, lambda r: F.col("max_resenas_en_un_dia") >= r.umbral),
    2: ({"min_resenas"}, lambda r: (F.col("min_calificacion") == F.col("max_calificacion"))
                                   & F.col("min_calificacion").isin(1, 5)),
    3: ({"umbral"}, lambda r: F.col("porcentaje_calificaciones_extremas") > r.umbral),
    4: (set(),      lambda r: F.col("es_dia_registro_en_rafaga")),
    7: ({"umbral", "min_resenas"}, lambda r: (F.col("amigos_count") == 0) & (F.col("fans_count") == 0)
                                             & (F.col("porcentaje_calificaciones_extremas") >= r.umbral)),
}

catalogo = spark.table(f"{SLV}.slv_fraud_rule").filter("entidad = 'usuario' AND rule_status = 'activa'").collect()

sin_logica = sorted(r.fraud_rule_id for r in catalogo if r.fraud_rule_id not in CONDICIONES)
if sin_logica:
    print(f"⚠️ Reglas de usuario activas sin implementación (no puntúan): {sin_logica}")

score, activadas = F.lit(0), []
for r in (x for x in catalogo if x.fraud_rule_id in CONDICIONES):
    requeridos, condicion = CONDICIONES[r.fraud_rule_id]
    faltan = [p for p in requeridos if getattr(r, p) is None]
    assert not faltan, f"❌ La regla {r.fraud_rule_id} está activa pero le falta {faltan} en el catálogo"
    cond = condicion(r) & (F.col("total_resenas") >= (r.min_resenas or 0))
    score = score + F.when(cond, F.lit(r.puntos or 0)).otherwise(0)
    activadas.append(F.when(cond, F.lit(r.fraud_rule_id)))

gld = (senales
    .withColumn("reglas_activadas", F.array_compact(F.array(*activadas).cast("array<bigint>")))
    .withColumn("score_riesgo_usuario", F.least(score, F.lit(100)).cast("double"))
    .withColumn("clasificacion_riesgo",
        F.when(F.col("score_riesgo_usuario") >= 50, "Alto")
         .when(F.col("score_riesgo_usuario") >= 25, "Moderado")
         .otherwise("Bajo"))
    .withColumn("fecha_actualizacion", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Migración de esquema y vista consumidora (idempotente)
# ── Migración idempotente (auditoría 2026-11-02): esquema + vista consumidora ──
# Debe correr ANTES de la escritura: el promedio del archivo fuente se llamaba
# "promedio_estrellas_otorgadas", sugiriendo el promedio de la muestra (coincide solo en el 21% de
# usuarios). Se renombra para que ninguna regla ni reporte mezcle poblaciones.
#
# gld_vw_fraude_resena es el único consumidor directo de la columna renombrada. Se actualiza por cirugía
# sobre su DDL almacenado (SHOW CREATE TABLE). Ese DDL trae DOS lados: la lista de columnas con COMMENT y
# el SELECT; la cirugía toca AMBOS en la MISMA posición (nombre en la lista + expresión u.<columna>), o el
# alias quedaría desalineado. La nueva definición se construye y valida ANTES de tocar el esquema, y si
# el CREATE falla se revierte el renombrado para no dejar la vista rota para Power BI.
VISTA = f"{GOLD}.gld_vw_fraude_resena"
vista_existe = spark.catalog.tableExists(VISTA)
nueva_vista = None
if vista_existe:
    vdef = spark.sql(f"SHOW CREATE TABLE {VISTA}").first()[0]
    nuevo = vdef.replace("promedio_estrellas_otorgadas", "promedio_estrellas_historico")
    if "n_resenas_dia_registro" in nuevo:
        print("• gld_vw_fraude_resena ya estaba actualizada")
    else:
        # Anclas verificadas contra el DDL real de la vista (2026-11-02): la entrada de la lista de
        # columnas lleva COMMENT; la del SELECT va calificada con u. — ambas únicas en su lado.
        a_lista = "promedio_estrellas_historico COMMENT 'Promedio histórico de calificaciones otorgadas por el usuario',"
        a_select = "u.promedio_estrellas_historico,"
        assert a_lista in nuevo and nuevo.count(a_select) == 1, \
            "❌ gld_vw_fraude_resena no tiene la estructura esperada: revisar su DDL manualmente"
        nuevo = nuevo.replace(a_lista,
            "promedio_estrellas_historico COMMENT 'Promedio histórico de calificaciones del archivo fuente (no el de la muestra; ninguna regla lo usa)',\n"
            "  n_resenas_dia_registro COMMENT 'Score input de la regla 4 (versión amplia): reseñas extremas de <250 caracteres escritas el día de registro del autor',", 1)
        nuevo = nuevo.replace(a_select, "u.promedio_estrellas_historico,\n  u.n_resenas_dia_registro,", 1)
        assert nuevo.count("n_resenas_dia_registro") == 2 and "promedio_estrellas_otorgadas" not in nuevo, \
            "❌ la cirugía del DDL de la vista no quedó bien formada: revisar manualmente"
        # la coma tras la nueva entrada es obligatoria: el ancla la consume y una omisión produce
        # PARSE_SYNTAX_ERROR en la columna siguiente (desviacion_estandar_estrellas)
        assert "escritas el día de registro del autor',\n" in nuevo, \
            "❌ falta la coma tras la entrada nueva de la lista de columnas"
        nueva_vista = nuevo.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
else:
    print(f"⚠️ {VISTA} no existe en este entorno: la crea la tarea GOLD_vista_power_bi al final de gold")

cols = set(spark.table(DESTINO).columns)
primera_migracion = "promedio_estrellas_otorgadas" in cols
if primera_migracion:
    # El renombrado de columnas en Delta requiere column mapping (protocolo ≥ 2,5): se habilita una sola vez.
    spark.sql(f"ALTER TABLE {DESTINO} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    spark.sql(f"ALTER TABLE {DESTINO} RENAME COLUMN promedio_estrellas_otorgadas TO promedio_estrellas_historico")
    print("✔ Renombrada promedio_estrellas_otorgadas → promedio_estrellas_historico")
if "n_resenas_dia_registro" not in cols:
    spark.sql(f"ALTER TABLE {DESTINO} ADD COLUMN n_resenas_dia_registro INT")
    print("✔ Añadida n_resenas_dia_registro (score input de la regla 4)")

if nueva_vista:
    try:
        spark.sql(nueva_vista)
        print("✔ gld_vw_fraude_resena actualizada (renombrado + n_resenas_dia_registro, lista y SELECT alineados)")
    except Exception:
        # revertir el renombrado de esta corrida: la vista vieja vuelve a resolver y Power BI no se rompe
        if primera_migracion:
            spark.sql(f"ALTER TABLE {DESTINO} RENAME COLUMN promedio_estrellas_historico TO promedio_estrellas_otorgadas")
        raise
if vista_existe:
    assert "n_resenas_dia_registro" in spark.table(VISTA).columns, \
        "❌ la señal n_resenas_dia_registro no quedó expuesta en la vista: revisar su SELECT manualmente"

# COMMAND ----------

# DBTITLE 1,Escritura sobre la tabla del DDL de E3 (conserva COMMENT y constraints)
assert spark.catalog.tableExists(DESTINO), f"❌ {DESTINO} no existe: ejecutar primero E3_modelo_fisico/01_modelo_fisico_ddl"

gld.select(*spark.table(DESTINO).columns).write.insertInto(DESTINO, overwrite=True)
print(f"✔ {DESTINO} cargada")

# COMMAND ----------

# DBTITLE 1,Validación automática
# Validación sobre el DataFrame en sesión (los valores aún no pasan por las máscaras de la tabla gold)
r = gld.agg(
    F.count("*").alias("filas"),
    F.countDistinct("user_id").alias("pk_distintas"),
    F.count_if(~F.col("score_riesgo_usuario").between(0, 100)).alias("score_fuera_rango"),
).first()

assert r.filas > 0, f"❌ {DESTINO} quedó vacía"
assert r.filas == r.pk_distintas, f"❌ user_id duplicado: {r.filas - r.pk_distintas} filas"
assert r.score_fuera_rango == 0, f"❌ {r.score_fuera_rango} scores fuera de 0-100"

# P0 (auditoría 2026-11-02): el perfil debe cubrir a TODOS los autores de la muestra
autores = resenas.select("user_id").distinct().count()
assert r.filas == autores, f"❌ {autores - r.filas} autores de slv_review quedaron sin perfil"

# Volúmenes esperados por la auditoría: R2=479 · R4=43 · R7=477 · score input amplio=3,728
por_regla = {row["regla"]: row["count"] for row in
             gld.select(F.explode("reglas_activadas").alias("regla")).groupBy("regla").count().orderBy("regla").collect()}
amplios = gld.where(F.col("n_resenas_dia_registro") > 0).count()
dist = {x.clasificacion_riesgo: x["count"] for x in gld.groupBy("clasificacion_riesgo").count().collect()}

assert spark.table(DESTINO).count() == r.filas, "❌ la tabla física no refleja lo calculado"

print(f"✔ {DESTINO}: {r.filas:,} usuarios · PK única · cobertura total de {autores:,} autores · riesgo {dist}")
print(f"✔ Casos por regla (auditoría esperaba R2=479, R4=43, R7=477): {por_regla}")
print(f"✔ Score input regla 4, versión amplia (auditoría esperaba 3,728): {amplios:,} usuarios")

# COMMAND ----------

# DBTITLE 1,Verificación cruzada contra gold (post-carga)
# Verificación cruzada post-carga, solo con tablas GOLD (independiente de yelp_silver y del DataFrame
# en sesión): reproduce los conteos auditados del 2026-11-02 sobre gld_fact_resena + gld_dim_usuario
# + gld_business_anomaly_bursts + gld_user_fraud_profile (columnas sin máscara).
fact = spark.table(f"{GOLD}.gld_fact_resena")
dimf = spark.table(f"{GOLD}.gld_dim_fecha").select("fecha_id", "fecha")
dimu = spark.table(f"{GOLD}.gld_dim_usuario").select("user_id", F.to_date("miembro_desde").alias("dia_reg"))
perfil = spark.table(f"{GOLD}.gld_user_fraud_profile")

n_auth = fact.select("user_id").distinct().count()

agg = fact.groupBy("user_id").agg(F.count("*").alias("n"),
                                  F.min("calificacion").alias("mn"),
                                  F.max("calificacion").alias("mx"))
r2 = agg.where((F.col("n") >= 5) & (F.col("mn") == F.col("mx")) & F.col("mn").isin(1, 5)).count()

md = (fact.select("user_id", "business_id", "fecha_id", "es_extrema", "n_caracteres")
    .join(dimf, "fecha_id")
    .join(dimu, "user_id")
    .where((F.col("fecha") == F.col("dia_reg")) & F.col("es_extrema") & (F.col("n_caracteres") < 250)))

amplios = md.select("user_id").distinct().count()

rafagas = (spark.table(f"{GOLD}.gld_business_anomaly_bursts").where("es_rafaga_sospechosa")
    .select("business_id", F.col("fecha_ventana").alias("fecha")))
r4 = (md.join(rafagas, (md["business_id"] == rafagas["business_id"]) & (md["fecha"] == rafagas["fecha"]))
    .select("user_id").distinct().count())

r7 = perfil.where((F.col("amigos_count") == 0) & (F.col("fans_count") == 0)
                  & (F.col("total_resenas") >= 5)
                  & (F.col("porcentaje_calificaciones_extremas") >= 80)).count()

# Conteos auditados sobre esta muestra (dataset estático del capstone)
esperados = {"autores": 160_993, "R2": 479, "R4": 43, "R7": 477, "amplio": 3_728}
medidos = {"autores": n_auth, "R2": r2, "R4": r4, "R7": r7, "amplio": amplios}
for k, esperado in esperados.items():
    marca = "✔" if medidos[k] == esperado else "⚠️ DESVIACIÓN"
    print(f"{marca} {k} = {medidos[k]:,} (esperado {esperado:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **P0 — cobertura total (auditoría 2026-11-02):** se perfila a TODOS los autores de `slv_review` (160,993),
# MAGIC   incluidos los 10,993 miembros inferidos ausentes de `slv_user` (antes invisibles a toda regla de usuario).
# MAGIC   Sus atributos de origen quedan NULL, nunca 0: 0 amigos/fans es una señal real de fraude y no se fabrica.
# MAGIC - `total_resenas`, la desviación, el % de extremas, el pico diario y MIN/MAX de calificación se calculan sobre
# MAGIC   `slv_review` (la muestra). `promedio_estrellas_historico` (antes `promedio_estrellas_otorgadas`) es el promedio
# MAGIC   histórico del archivo fuente — coincide con el de la muestra solo en el 21% de usuarios — y NINGUNA regla lo usa.
# MAGIC - Desviación con `stddev_samp`: nula con una sola reseña, para no marcar como bot a quien escribió una vez.
# MAGIC - Reglas rediseñadas y aprobadas por Trust & Safety (volúmenes verificados contra gold el 2026-11-02):
# MAGIC   - **R2** (29 → 479): 5+ reseñas, todas 1★ o todas 5★, con `MIN = MAX` sobre las mismas reseñas.
# MAGIC   - **R4** (0 → 43): reseña extrema de <250 caracteres escrita el día de registro del autor, dentro de una
# MAGIC     ventana con ráfaga marcada (LEE `gld_business_anomaly_bursts`, declarada en el depends_on de la tarea).
# MAGIC     La versión amplia (3,728 usuarios) queda como score input (`n_resenas_dia_registro`) y no puntúa.
# MAGIC   - **R7** (182 → 477): 0 amigos, 0 fans, 5+ reseñas y ≥80% de calificaciones extremas, riesgo `medio`.
# MAGIC - `antiguedad_dias` se sigue midiendo contra la fecha actual (lo exige el mapeo); la regla 4 ya no la usa.
# MAGIC - El score aplica las reglas de usuario **activas** de `slv_fraud_rule`, con el umbral, el mínimo de reseñas y
# MAGIC   los puntos del catálogo; el código define solo la forma de cada regla. Tope de 100.
# MAGIC - `reglas_activadas` guarda el linaje regla → caso que consume `gld_trust_governance`.
# MAGIC - `gld_vw_fraude_resena` se define en `E3_modelo_fisico/02_vista_power_bi`. La celda de migración la ajusta por
# MAGIC   cirugía en entornos existentes, tocando lista de columnas y SELECT en la MISMA posición, y revierte el
# MAGIC   renombrado si el CREATE falla, para no romper el consumo de Power BI.
# MAGIC - El DDL de E3 quedó alineado con el esquema auditado (renombrado + `n_resenas_dia_registro`); la celda de
# MAGIC   migración ajusta en caliente las tablas ya creadas en entornos existentes.
# MAGIC - Una regla activa sin implementación se avisa y no puntúa; una regla activa a la que le falta un parámetro
# MAGIC   obligatorio detiene la carga.
# MAGIC - Los cortes de clasificación (Alto ≥ 50, Moderado ≥ 25) siguen en el código: recalibrarlos es P2, tras el
# MAGIC   conjunto de validación etiquetado de la auditoría.
# MAGIC
# MAGIC ### Definition of Done (gld_user_fraud_profile)
# MAGIC - [x] Corre de inicio a fin con el catálogo aprobado: corrida `233714860388458` del 2026-11-05 (14 tareas en
# MAGIC       SUCCESS). En gold: 160,993 perfiles, R2=479, R4=43, R7=477 y score input amplio=3,728.
# MAGIC - [x] Reglas R2/R4/R7 aprobadas por Trust & Safety y sincronizadas en `slv_fraud_rule`.
# MAGIC - [ ] Cortes de clasificación aprobados por el Data Owner (Trust & Safety).
