# Databricks notebook source
# MAGIC %md
# MAGIC # E5 · Reglas de Calidad de Datos + Motor de Evaluación
# MAGIC **Se define en:** M2/M3 (reglas en lenguaje de negocio) · **Se implementa tras:** M6 (cuando ya hay datos en silver)
# MAGIC · **Tarea del job:** `DQ_gate`
# MAGIC
# MAGIC Enfoque *rules-as-data*: las reglas viven en la tabla `yelp_gov.dq_reglas` (no
# MAGIC hardcodeadas), un motor genérico las ejecuta y persiste resultados en
# MAGIC `yelp_gov.dq_resultados`. Ese historial alimenta el dashboard de E8 y el job de E7.
# MAGIC
# MAGIC **Gate de calidad:** en el job, esta tarea corre después de silver y antes de gold. Si una regla de criticidad
# MAGIC **alta** no alcanza su umbral, la tarea falla, gold no se ejecuta y llega la alerta por correo. Las reglas de
# MAGIC criticidad **media** se registran y se reportan, pero no detienen el pipeline.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Reglas de calidad de datos (definición en lenguaje de negocio)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan y ejecutan esas reglas: el documento define QUÉ debe cumplirse y por qué; la plataforma demuestra que se cumple.

# COMMAND ----------

# DBTITLE 1,Catálogo de reglas — 19 reglas, 6 dimensiones, 7 entidades silver
from pyspark.sql import Row

S = "workspace.yelp_silver"
TS, PR = "Trust & Safety (Rafael Nakamura)", "Producto (Andrew Collins)"
DETENER = "Detener el pipeline: gold no se publica y se alerta al steward"
REPORTAR = "Reportar al steward en el scorecard; no detiene el pipeline"

def regla(regla_id, tabla, columna, dimension, descripcion, expresion, criticidad, umbral, justificacion, accion, owner):
    return Row(regla_id=regla_id, tabla_objetivo=f"{S}.{tabla}", columna=columna, dimension=dimension,
               descripcion=descripcion, expresion_sql=expresion, criticidad=criticidad, umbral_pct=float(umbral),
               justificacion_umbral=justificacion, accion_si_falla=accion, data_owner=owner)

# Las expresiones se evalúan sobre la tabla objetivo con alias `t` (permite subconsultas correlacionadas)
reglas = [
    # ---------- Reseña ----------
    regla("DQ-001", "slv_review", "calificacion", "validez",
          "Toda reseña tiene una calificación entre 1 y 5 estrellas",
          "t.calificacion BETWEEN 1 AND 5", "alta", 100,
          "Una calificación fuera de rango no es un dato imperfecto sino imposible: contaminaría el rating público del negocio. La carga ya lo impide con un CHECK (calidad preventiva); esta regla lo verifica.",
          DETENER, TS),
    regla("DQ-002", "slv_review", "review_id", "unicidad",
          "Cada reseña aparece una sola vez",
          "t.review_id IS NOT NULL", "alta", 100,
          "Un duplicado infla los conteos de Marketing. Silver ya aparta las copias en cuarentena, así que cualquier duplicado restante es un error del pipeline.",
          DETENER, TS),
    regla("DQ-003", "slv_review", "business_id", "integridad",
          "Toda reseña corresponde a un negocio que existe en el catálogo",
          f"t.business_id IN (SELECT business_id FROM {S}.slv_business)", "alta", 99.5,
          "El 0,5 % cubre negocios dados de baja entre la extracción de una entidad y la otra: desfase esperable del origen. Las huérfanas van a cuarentena.",
          DETENER, TS),
    regla("DQ-004", "slv_review", "user_id", "integridad",
          "El autor de cada reseña existe en el registro de usuarios",
          "t.autor_en_muestra", "media", 90,
          "El muestreo recorta los usuarios a 150 mil, así que se espera ~93 %. Riesgo aceptado: la reseña es válida aunque el perfil no esté en la muestra. Bajo 90 % indicaría que la muestra o la carga cambiaron.",
          REPORTAR, TS),
    regla("DQ-005", "slv_review", "fecha_publicacion", "consistencia",
          "Una reseña no puede ser anterior al registro de su autor en Yelp",
          f"NOT EXISTS (SELECT 1 FROM {S}.slv_user u WHERE u.user_id = t.user_id AND t.fecha_publicacion < u.miembro_desde)",
          "media", 100,
          "Es un caso imposible, pero viene así desde el origen y no se puede corregir aquí. Umbral 100 % para que cada caso quede visible; Trust & Safety los revisa como posible cuenta reutilizada o error de fecha.",
          REPORTAR, TS),
    regla("DQ-006", "slv_review", "fecha_publicacion", "actualidad",
          "Ninguna reseña tiene fecha de publicación futura",
          "t.fecha_publicacion <= current_timestamp()", "alta", 100,
          "Una fecha futura delata un error de carga o de zona horaria y rompería las series de tiempo del dashboard.",
          DETENER, TS),
    regla("DQ-007", "slv_review", "fecha_carga", "actualidad",
          "Las reseñas de silver se cargaron en las últimas 24 horas",
          "t.fecha_carga >= current_timestamp() - INTERVAL 24 HOURS", "media", 100,
          "El job corre a diario: si silver tiene más de un día, el pipeline no corrió y el dashboard muestra datos viejos.",
          REPORTAR, TS),
    regla("DQ-008", "slv_review", "comentario", "completitud",
          "Toda reseña tiene texto",
          "t.comentario IS NOT NULL AND trim(t.comentario) <> ''", "media", 99.9,
          "El texto es la evidencia que revisa Trust & Safety y la entrada del análisis con IA; un 0,1 % sin texto no altera los conteos.",
          REPORTAR, TS),
    # ---------- Negocio ----------
    regla("DQ-009", "slv_business", "business_id", "unicidad",
          "Cada negocio aparece una sola vez",
          "t.business_id IS NOT NULL", "alta", 100,
          "Es la entidad maestra del golden record: un duplicado duplicaría reseñas y check-ins en todos los reportes.",
          DETENER, PR),
    regla("DQ-010", "slv_business", "categorias", "completitud",
          "Todo negocio declara al menos una categoría",
          "t.categorias IS NOT NULL AND size(t.categorias) > 0", "media", 99,
          "Hoy 9 de 14.577 negocios llegan sin categoría desde Yelp; quedan fuera solo de los cortes por categoría. Bajo 99 % esos cortes dejarían de ser representativos.",
          REPORTAR, PR),
    # ---------- Usuario ----------
    regla("DQ-011", "slv_user", "promedio_estrellas", "validez",
          "El promedio de estrellas otorgadas por un usuario está entre 1 y 5",
          "t.promedio_estrellas BETWEEN 1 AND 5", "alta", 100,
          "Valor imposible fuera de rango; alimenta el perfil de riesgo de usuarios. La carga ya lo impide con un CHECK.",
          DETENER, PR),
    regla("DQ-012", "slv_user", "nombre", "completitud",
          "Todo usuario tiene un nombre visible",
          "t.nombre IS NOT NULL AND trim(t.nombre) <> ''", "media", 99.9,
          "Trust & Safety identifica al usuario por su nombre al revisar casos; un 0,1 % sin nombre se revisa por id.",
          REPORTAR, PR),
    regla("DQ-013", "slv_user", "n_resenas", "consistencia",
          "El total de reseñas de un perfil no es menor que las reseñas suyas presentes en la muestra",
          f"t.n_resenas >= (SELECT count(*) FROM {S}.slv_review r WHERE r.user_id = t.user_id)", "media", 99.5,
          "Perfiles y reseñas se extraen en momentos distintos, así que hay un desfase esperable (~0,2 %). Más de 0,5 % indicaría perfiles desactualizados.",
          REPORTAR, PR),
    # ---------- Tip ----------
    regla("DQ-014", "slv_tip", "business_id", "integridad",
          "Todo tip corresponde a un negocio que existe en el catálogo",
          f"t.business_id IN (SELECT business_id FROM {S}.slv_business)", "alta", 99.5,
          "Mismo criterio que las reseñas (DQ-003): el 0,5 % cubre el desfase entre extracciones.",
          DETENER, TS),
    regla("DQ-015", "slv_tip", "user_id", "integridad",
          "El autor de cada tip existe en el registro de usuarios",
          f"t.user_id IN (SELECT user_id FROM {S}.slv_user)", "media", 70,
          "El muestreo elige usuarios solo entre autores de reseñas, así que ~26 % de los tips tienen autor fuera de la muestra. Riesgo aceptado; bajo 70 % indicaría un cambio en la muestra.",
          REPORTAR, TS),
    # ---------- Check-in ----------
    regla("DQ-016", "slv_checkin", "checkin_id", "unicidad",
          "Cada check-in se cuenta una sola vez",
          "t.checkin_id IS NOT NULL", "alta", 100,
          "Es el conteo oficial de check-ins que pidió Operaciones Comerciales: un duplicado reabre la inconsistencia que el proyecto resuelve.",
          DETENER, PR),
    regla("DQ-017", "slv_checkin", "business_id", "integridad",
          "Todo check-in corresponde a un negocio que existe en el catálogo",
          f"t.business_id IN (SELECT business_id FROM {S}.slv_business)", "alta", 99.5,
          "Mismo criterio que DQ-003: el 0,5 % cubre el desfase entre extracciones.",
          DETENER, PR),
    # ---------- Trust & Safety ----------
    regla("DQ-018", "slv_fraud_rule", "team_id", "completitud",
          "Toda regla de fraude activa tiene un equipo responsable",
          "t.rule_status <> 'activa' OR t.team_id IS NOT NULL", "alta", 100,
          "Una regla activa sin responsable marca usuarios y negocios sin que nadie revise los casos: es el dolor de accountability del caso.",
          DETENER, TS),
    regla("DQ-019", "slv_trust_safety_area", "team_status", "validez",
          "El estado de cada equipo de Trust & Safety es activo o inactivo",
          "t.team_status IN ('activo', 'inactivo')", "alta", 100,
          "El estado del equipo decide las alertas de gobierno de las reglas; un valor desconocido las ocultaría.",
          DETENER, TS),
]

# COMMAND ----------

# DBTITLE 1,Prueba controlada del gate (solo si se pide)
# Con forzar_falla_gate = true se agrega una regla crítica imposible de cumplir para demostrar que el gate detiene
# el pipeline. Queda marcada como prueba y desaparece del catálogo en la siguiente corrida normal.
dbutils.widgets.text("forzar_falla_gate", "false")
if dbutils.widgets.get("forzar_falla_gate").strip().lower() == "true":
    reglas.append(regla("DQ-900", "slv_review", "calificacion", "validez",
          "PRUEBA CONTROLADA DEL GATE: ninguna reseña tiene 5 estrellas (falla a propósito)",
          "t.calificacion < 5", "alta", 100,
          "Regla de prueba para evidenciar que el gate detiene el pipeline (plantilla E7.4). No es una regla de negocio.",
          DETENER, "Equipo del Capstone (prueba)"))
    print("⚠️ Corrida de prueba: se agregó DQ-900 para forzar la falla del gate")

# COMMAND ----------

# DBTITLE 1,Carga del catálogo en yelp_gov.dq_reglas
CATALOGO = "workspace.yelp_gov.dq_reglas"

# El catálogo puede haberse creado con la versión anterior de 00_setup: agrega las columnas que falten
existentes = {c.name for c in spark.table(CATALOGO).schema}
nuevas = {
    "justificacion_umbral": "Por qué ese umbral y qué pasa con las filas que no cumplen",
    "accion_si_falla": "Decisión cuando la regla no se cumple (detener el pipeline, cuarentena o reportar)",
    "data_owner": "Área del caso que responde por la regla y acepta el riesgo",
}
for col, comentario in nuevas.items():
    if col not in existentes:
        spark.sql(f"ALTER TABLE {CATALOGO} ADD COLUMNS ({col} STRING COMMENT '{comentario}')")

ids = [r.regla_id for r in reglas]
assert len(ids) == len(set(ids)), "❌ Hay regla_id repetidos en el catálogo"
spark.createDataFrame(reglas).select(*spark.table(CATALOGO).columns).write.insertInto(CATALOGO, overwrite=True)
print(f"✔ {CATALOGO}: {len(reglas)} reglas")

# COMMAND ----------

# DBTITLE 1,Motor genérico de evaluación
from pyspark.sql import functions as F

def evaluar_reglas():
    resultados = []
    for r in spark.table(CATALOGO).collect():
        total = spark.table(r.tabla_objetivo).count()
        if r.dimension == "unicidad" and r.columna != "*":
            # La unicidad compara filas vs valores distintos
            ok = spark.sql(f"SELECT count(DISTINCT {r.columna}) c FROM {r.tabla_objetivo}").first().c
        else:
            ok = spark.sql(f"SELECT count(*) c FROM {r.tabla_objetivo} t WHERE {r.expresion_sql}").first().c
        # Se compara con el valor exacto: redondear antes haría pasar una regla de umbral 100 con pocas filas fallidas
        paso = total > 0 and 100.0 * ok >= r.umbral_pct * total
        pct = round(100.0 * ok / total, 4) if total else 0.0
        resultados.append((r.regla_id, r.tabla_objetivo, total, ok, pct, paso))
    df = spark.createDataFrame(resultados,
        "regla_id STRING, tabla_objetivo STRING, filas_totales BIGINT, filas_ok BIGINT, pct_cumplimiento DOUBLE, paso BOOLEAN"
    ).withColumn("ejecucion_ts", F.current_timestamp())
    df.select("ejecucion_ts","regla_id","tabla_objetivo","filas_totales","filas_ok","pct_cumplimiento","paso") \
      .write.mode("append").saveAsTable("workspace.yelp_gov.dq_resultados")
    return df

resultado = evaluar_reglas()

# COMMAND ----------

# DBTITLE 1,Scorecard de calidad (insumo directo del dashboard E8)
# MAGIC %sql
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                    AS reglas,
# MAGIC        sum(CASE WHEN res.paso THEN 1 ELSE 0 END)   AS reglas_ok,
# MAGIC        round(avg(res.pct_cumplimiento), 2)         AS cumplimiento_promedio
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
# MAGIC GROUP BY r.dimension ORDER BY cumplimiento_promedio;

# COMMAND ----------

# DBTITLE 1,Gate de calidad — detiene el pipeline si falla una regla crítica
ultima = spark.sql("""
  SELECT res.regla_id, r.descripcion, r.criticidad, r.umbral_pct, res.pct_cumplimiento, res.paso, r.accion_si_falla
  FROM workspace.yelp_gov.dq_resultados res
  JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
  WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
  ORDER BY res.regla_id
""").collect()

dimensiones = {r.dimension for r in spark.table(CATALOGO).collect()}
print(f"Reglas evaluadas: {len(ultima)} · dimensiones: {sorted(dimensiones)}")
for x in ultima:
    estado = "✔" if x.paso else ("❌" if x.criticidad == "alta" else "⚠️")
    print(f"  {estado} {x.regla_id} [{x.criticidad}] {x.pct_cumplimiento}% (umbral {x.umbral_pct}%) — {x.descripcion}")

criticas = [x.regla_id for x in ultima if not x.paso and x.criticidad == "alta"]
reportadas = [x.regla_id for x in ultima if not x.paso and x.criticidad != "alta"]
if reportadas:
    print(f"⚠️ Reglas no críticas por debajo del umbral (se reportan, no detienen): {reportadas}")
assert not criticas, \
    f"❌ Gate de calidad: {len(criticas)} reglas críticas fallaron {criticas} — gold no se publica (así se ve la calidad como GATE, no como reporte)"
print("✔ Gate de calidad superado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **Severidad:** solo las reglas de criticidad alta detienen el pipeline. Las de criticidad media dejan constancia
# MAGIC   en `dq_resultados` y aparecen en el scorecard; su decisión está en `accion_si_falla` y la firma en `data_owner`.
# MAGIC - **Riesgos aceptados por el muestreo:** DQ-004 (reseñas con autor fuera de la muestra) y DQ-015 (tips con autor
# MAGIC   fuera de la muestra) tienen umbrales bajos a propósito: el script de muestreo recorta usuarios y solo los toma
# MAGIC   de los autores de reseñas. El umbral detecta si eso cambia, no la existencia del efecto.
# MAGIC - **Hallazgo DQ-005:** 8 reseñas son anteriores al registro de su autor. Es imposible en la realidad, viene así del
# MAGIC   origen y queda reportado para revisión de Trust & Safety.
# MAGIC - **Calidad preventiva y detectiva:** DQ-001, DQ-011 y la validez de `risk_level` también se imponen con CHECK en
# MAGIC   el DDL de E3: la carga falla antes de que el dato entre, y el motor lo verifica después.
# MAGIC - El motor evalúa cada expresión con alias `t` sobre la tabla objetivo, lo que permite reglas que cruzan tablas
# MAGIC   (DQ-005, DQ-013).
# MAGIC - **Fix al motor de la plantilla:** redondeaba el porcentaje a 2 decimales antes de compararlo con el umbral, así
# MAGIC   que DQ-005 (8 fallas en 399.453, 99,998 %) figuraba como 100 % y pasaba. Ahora `paso` usa el valor exacto y
# MAGIC   `pct_cumplimiento` se guarda con 4 decimales.
# MAGIC
# MAGIC ### Definition of Done (E5)
# MAGIC - [x] ≥12 reglas en las 6 dimensiones, con criticidad y umbral justificados (19 reglas).
# MAGIC - [ ] Motor ejecutado con historial en `dq_resultados` (≥2 corridas en fechas distintas).
# MAGIC - [x] Para cada regla FALLIDA: decisión documentada (`accion_si_falla`, `data_owner`); falta la firma real del Data Owner.
# MAGIC - [x] Bonus: constraint `CHECK` en el pipeline para las reglas más críticas (calidad *preventiva* vs *detectiva*).
