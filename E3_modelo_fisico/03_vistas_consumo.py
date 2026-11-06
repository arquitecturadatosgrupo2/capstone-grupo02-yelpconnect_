# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Vistas de consumo sobre gold
# MAGIC **Tarea del job:** `GOLD_vistas_consumo`, después de `GOLD_vista_power_bi`, porque `vw_analisis_fraude_resenas`
# MAGIC lee `gld_vw_fraude_resena`.
# MAGIC
# MAGIC Vistas analíticas para reportes, sobre el modelo dimensional y los productos de riesgo. No llevan máscaras
# MAGIC propias: las columnas sensibles se enmascaran en sus tablas base.

# COMMAND ----------

# DBTITLE 1,vw_vista_principal_resenas
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_vista_principal_resenas (
# MAGIC   review_id COMMENT 'Identificador único de la reseña (grano del hecho)',
# MAGIC   fecha COMMENT 'Fecha calendario',
# MAGIC   anio COMMENT 'Año',
# MAGIC   trimestre COMMENT 'Trimestre del año (1 a 4)',
# MAGIC   mes COMMENT 'Mes del año (1 a 12)',
# MAGIC   nombre_mes COMMENT 'Nombre del mes en español',
# MAGIC   nombre_dia COMMENT 'Nombre del día de la semana en español',
# MAGIC   es_fin_de_semana COMMENT 'TRUE si es sábado o domingo',
# MAGIC   user_id COMMENT 'Identificador único del usuario (clave natural)',
# MAGIC   es_elite COMMENT 'TRUE si el usuario fue elite al menos un año',
# MAGIC   fans COMMENT 'Número de seguidores',
# MAGIC   estado_cuenta COMMENT 'Estado de la cuenta (activa, suspendida, cerrada)',
# MAGIC   business_id COMMENT 'Identificador único del negocio (clave natural)',
# MAGIC   negocio COMMENT 'Razón comercial del negocio',
# MAGIC   ciudad COMMENT 'Ciudad normalizada',
# MAGIC   estado COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   categoria_principal COMMENT 'Primera categoría declarada por el negocio',
# MAGIC   rating_negocio COMMENT 'Rating promedio publicado (1.0 a 5.0)',
# MAGIC   esta_abierto COMMENT 'TRUE si el negocio sigue operativo',
# MAGIC   calificacion COMMENT 'Estrellas otorgadas (1 a 5)',
# MAGIC   es_extrema COMMENT 'TRUE si la calificación es 1 o 5',
# MAGIC   util COMMENT 'Votos "útil" recibidos',
# MAGIC   divertido COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool COMMENT 'Votos "cool" recibidos',
# MAGIC   n_caracteres COMMENT 'Longitud del texto de la reseña (el texto no se publica en gold)',
# MAGIC   score_riesgo_usuario COMMENT 'Suma de puntos de las reglas activadas, tope 100',
# MAGIC   clasificacion_riesgo COMMENT 'Categoría de riesgo: Bajo, Moderado o Alto',
# MAGIC   total_resenas COMMENT 'Cantidad total de reseñas históricas del usuario',
# MAGIC   max_resenas_en_un_dia COMMENT 'Máximo de reseñas escritas por el usuario en un mismo día',
# MAGIC   porcentaje_calificaciones_extremas COMMENT 'Porcentaje de reseñas con calificación 1 o 5 sobre el total del usuario',
# MAGIC   fecha_carga COMMENT 'Momento en que se consulta la vista (CURRENT_TIMESTAMP), no la fecha de carga del dato')
# MAGIC COMMENT 'Vista principal: una fila por reseña, con fecha, usuario, negocio y riesgo del autor'
# MAGIC AS SELECT
# MAGIC     r.review_id,
# MAGIC
# MAGIC     f.fecha,
# MAGIC     f.anio,
# MAGIC     f.trimestre,
# MAGIC     f.mes,
# MAGIC     f.nombre_mes,
# MAGIC     f.nombre_dia,
# MAGIC     f.es_fin_de_semana,
# MAGIC
# MAGIC     u.user_id,
# MAGIC     u.es_elite,
# MAGIC     u.fans,
# MAGIC     u.estado_cuenta,
# MAGIC
# MAGIC     n.business_id,
# MAGIC     n.nombre AS negocio,
# MAGIC     n.ciudad,
# MAGIC     n.estado,
# MAGIC     n.categoria_principal,
# MAGIC     n.estrellas AS rating_negocio,
# MAGIC     n.esta_abierto,
# MAGIC
# MAGIC     r.calificacion,
# MAGIC     r.es_extrema,
# MAGIC     r.util,
# MAGIC     r.divertido,
# MAGIC     r.cool,
# MAGIC     r.n_caracteres,
# MAGIC
# MAGIC     fp.score_riesgo_usuario,
# MAGIC     fp.clasificacion_riesgo,
# MAGIC     fp.total_resenas,
# MAGIC     fp.max_resenas_en_un_dia,
# MAGIC     fp.porcentaje_calificaciones_extremas,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP() AS fecha_carga
# MAGIC FROM workspace.yelp_gold.gld_fact_resena r
# MAGIC INNER JOIN workspace.yelp_gold.gld_dim_fecha f
# MAGIC     ON r.fecha_id = f.fecha_id
# MAGIC LEFT JOIN workspace.yelp_gold.gld_dim_usuario u
# MAGIC     ON r.user_id = u.user_id
# MAGIC LEFT JOIN workspace.yelp_gold.gld_dim_negocio n
# MAGIC     ON r.business_id = n.business_id
# MAGIC LEFT JOIN workspace.yelp_gold.gld_user_fraud_profile fp
# MAGIC     ON r.user_id = fp.user_id

# COMMAND ----------

# DBTITLE 1,vw_analisis_fraude_resenas
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_analisis_fraude_resenas (
# MAGIC   review_id COMMENT 'Identificador único de la reseña (grano del hecho)',
# MAGIC   user_id COMMENT 'FK a gld_dim_usuario: autor de la reseña',
# MAGIC   business_id COMMENT 'FK a gld_dim_negocio: negocio reseñado',
# MAGIC   fecha COMMENT 'Fecha calendario',
# MAGIC   anio COMMENT 'Año',
# MAGIC   trimestre COMMENT 'Trimestre del año (1 a 4)',
# MAGIC   mes COMMENT 'Mes del año (1 a 12)',
# MAGIC   nombre_mes COMMENT 'Nombre del mes en español',
# MAGIC   anio_mes COMMENT 'Año y mes de la reseña en formato yyyy-MM',
# MAGIC   dia COMMENT 'Día del mes',
# MAGIC   dia_semana COMMENT 'Día de la semana ISO (1 = lunes, 7 = domingo)',
# MAGIC   nombre_dia COMMENT 'Nombre del día de la semana en español',
# MAGIC   es_fin_de_semana COMMENT 'TRUE si es sábado o domingo',
# MAGIC   calificacion COMMENT 'Estrellas otorgadas (1 a 5)',
# MAGIC   es_extrema COMMENT 'TRUE si la calificación es 1 o 5',
# MAGIC   util COMMENT 'Votos "útil" recibidos',
# MAGIC   divertido COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool COMMENT 'Votos "cool" recibidos',
# MAGIC   n_caracteres COMMENT 'Longitud del texto de la reseña (el texto no se publica en gold)',
# MAGIC   total_resenas COMMENT 'Cantidad total de reseñas históricas del usuario',
# MAGIC   promedio_estrellas_historico COMMENT 'Promedio histórico de calificaciones del archivo fuente (no el de la muestra; ninguna regla lo usa)',
# MAGIC   n_resenas_dia_registro COMMENT 'Score input de la regla 4 (versión amplia): reseñas extremas de <250 caracteres escritas el día de registro del autor',
# MAGIC   desviacion_estandar_estrellas COMMENT 'Desviación estándar de sus calificaciones (0 suele indicar comportamiento de bot)',
# MAGIC   porcentaje_calificaciones_extremas COMMENT 'Porcentaje de reseñas con calificación 1 o 5 sobre el total del usuario',
# MAGIC   max_resenas_en_un_dia COMMENT 'Máximo de reseñas escritas por el usuario en un mismo día',
# MAGIC   amigos_count COMMENT 'Cantidad de amigos en su red',
# MAGIC   fans_count COMMENT 'Cantidad de seguidores',
# MAGIC   antiguedad_dias COMMENT 'Días transcurridos desde el registro del usuario',
# MAGIC   score_riesgo_usuario COMMENT 'Suma de puntos de las reglas activadas, tope 100',
# MAGIC   clasificacion_riesgo COMMENT 'Categoría de riesgo: Bajo, Moderado o Alto',
# MAGIC   n_reglas_usuario COMMENT 'Cantidad de reglas de usuario activadas por el autor',
# MAGIC   reglas_fraude_usuario COMMENT 'Reglas de usuario activadas por el autor, con nivel de riesgo y descripción',
# MAGIC   dia_sospechoso_negocio COMMENT 'TRUE si el negocio tuvo una ráfaga sospechosa el día de la reseña',
# MAGIC   resenas_negocio_en_dia COMMENT 'Reseñas recibidas por el negocio el día de la reseña',
# MAGIC   checkins_negocio_en_dia COMMENT 'Check-ins recibidos por el negocio el día de la reseña',
# MAGIC   ratio_resenas_checkins COMMENT 'Reseñas / check-ins de la ventana (valores muy altos sugieren fraude)',
# MAGIC   promedio_estrellas_ventana COMMENT 'Calificación promedio obtenida en la ventana',
# MAGIC   n_reglas_negocio COMMENT 'Cantidad de reglas de negocio activadas ese día',
# MAGIC   reglas_fraude_negocio COMMENT 'Reglas de negocio activadas ese día, con nivel de riesgo y descripción')
# MAGIC COMMENT 'Vista de fraude: expone gld_vw_fraude_resena para análisis, una fila por reseña'
# MAGIC AS SELECT
# MAGIC     review_id,
# MAGIC     user_id,
# MAGIC     business_id,
# MAGIC     fecha,
# MAGIC     anio,
# MAGIC     trimestre,
# MAGIC     mes,
# MAGIC     nombre_mes,
# MAGIC     anio_mes,
# MAGIC     dia,
# MAGIC     dia_semana,
# MAGIC     nombre_dia,
# MAGIC     es_fin_de_semana,
# MAGIC     calificacion,
# MAGIC     es_extrema,
# MAGIC     util,
# MAGIC     divertido,
# MAGIC     cool,
# MAGIC     n_caracteres,
# MAGIC     total_resenas,
# MAGIC     promedio_estrellas_historico,
# MAGIC     n_resenas_dia_registro,
# MAGIC     desviacion_estandar_estrellas,
# MAGIC     porcentaje_calificaciones_extremas,
# MAGIC     max_resenas_en_un_dia,
# MAGIC     amigos_count,
# MAGIC     fans_count,
# MAGIC     antiguedad_dias,
# MAGIC     score_riesgo_usuario,
# MAGIC     clasificacion_riesgo,
# MAGIC     n_reglas_usuario,
# MAGIC     reglas_fraude_usuario,
# MAGIC     dia_sospechoso_negocio,
# MAGIC     resenas_negocio_en_dia,
# MAGIC     checkins_negocio_en_dia,
# MAGIC     ratio_resenas_checkins,
# MAGIC     promedio_estrellas_ventana,
# MAGIC     n_reglas_negocio,
# MAGIC     reglas_fraude_negocio
# MAGIC FROM workspace.yelp_gold.gld_vw_fraude_resena

# COMMAND ----------

# DBTITLE 1,vw_analisis_usuarios_riesgo
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_analisis_usuarios_riesgo (
# MAGIC   user_id COMMENT 'Identificador único del usuario (clave natural)',
# MAGIC   es_elite COMMENT 'TRUE si el usuario fue elite al menos un año',
# MAGIC   fans COMMENT 'Número de seguidores',
# MAGIC   estado_cuenta COMMENT 'Estado de la cuenta (activa, suspendida, cerrada)',
# MAGIC   total_resenas COMMENT 'Cantidad total de reseñas históricas del usuario',
# MAGIC   score_riesgo_usuario COMMENT 'Suma de puntos de las reglas activadas, tope 100',
# MAGIC   clasificacion_riesgo COMMENT 'Categoría de riesgo: Bajo, Moderado o Alto',
# MAGIC   max_resenas_en_un_dia COMMENT 'Máximo de reseñas escritas por el usuario en un mismo día',
# MAGIC   porcentaje_calificaciones_extremas COMMENT 'Porcentaje de reseñas con calificación 1 o 5 sobre el total del usuario',
# MAGIC   antiguedad_dias COMMENT 'Días transcurridos desde el registro del usuario')
# MAGIC COMMENT 'Usuarios con sus señales y clasificación de riesgo, una fila por usuario'
# MAGIC AS SELECT
# MAGIC     u.user_id,
# MAGIC     u.es_elite,
# MAGIC     u.fans,
# MAGIC     u.estado_cuenta,
# MAGIC     fp.total_resenas,
# MAGIC     fp.score_riesgo_usuario,
# MAGIC     fp.clasificacion_riesgo,
# MAGIC     fp.max_resenas_en_un_dia,
# MAGIC     fp.porcentaje_calificaciones_extremas,
# MAGIC     fp.antiguedad_dias
# MAGIC FROM workspace.yelp_gold.gld_user_fraud_profile fp
# MAGIC INNER JOIN workspace.yelp_gold.gld_dim_usuario u
# MAGIC     ON fp.user_id = u.user_id

# COMMAND ----------

# DBTITLE 1,vw_analisis_negocios_sospechosos
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_analisis_negocios_sospechosos (
# MAGIC   business_id COMMENT 'Identificador único del negocio (clave natural)',
# MAGIC   nombre COMMENT 'Razón comercial del negocio',
# MAGIC   ciudad COMMENT 'Ciudad normalizada',
# MAGIC   estado COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   categoria_principal COMMENT 'Primera categoría declarada por el negocio',
# MAGIC   fecha_ventana COMMENT 'Día analizado (ventana diaria)',
# MAGIC   resenas_en_ventana COMMENT 'Reseñas recibidas por el negocio en la ventana',
# MAGIC   checkins_en_ventana COMMENT 'Check-ins registrados en la misma ventana',
# MAGIC   ratio_resenas_checkins COMMENT 'Reseñas / check-ins de la ventana (valores muy altos sugieren fraude)',
# MAGIC   promedio_estrellas_ventana COMMENT 'Calificación promedio obtenida en la ventana',
# MAGIC   es_rafaga_sospechosa COMMENT 'TRUE si al menos una regla activa de negocio se cumple en el día')
# MAGIC COMMENT 'Negocio y día con sus métricas de ventana; incluye días sin ráfaga, filtrar por es_rafaga_sospechosa'
# MAGIC AS SELECT
# MAGIC     n.business_id,
# MAGIC     n.nombre,
# MAGIC     n.ciudad,
# MAGIC     n.estado,
# MAGIC     n.categoria_principal,
# MAGIC     b.fecha_ventana,
# MAGIC     b.resenas_en_ventana,
# MAGIC     b.checkins_en_ventana,
# MAGIC     b.ratio_resenas_checkins,
# MAGIC     b.promedio_estrellas_ventana,
# MAGIC     b.es_rafaga_sospechosa
# MAGIC FROM workspace.yelp_gold.gld_business_anomaly_bursts b
# MAGIC INNER JOIN workspace.yelp_gold.gld_dim_negocio n
# MAGIC     ON b.business_id = n.business_id

# COMMAND ----------

# DBTITLE 1,vw_metricas_resenas_mensuales
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_metricas_resenas_mensuales (
# MAGIC   anio COMMENT 'Año',
# MAGIC   mes COMMENT 'Mes del año (1 a 12)',
# MAGIC   nombre_mes COMMENT 'Nombre del mes en español',
# MAGIC   total_resenas COMMENT 'Reseñas publicadas en el mes',
# MAGIC   total_usuarios COMMENT 'Usuarios distintos que publicaron en el mes',
# MAGIC   total_negocios COMMENT 'Negocios distintos reseñados en el mes',
# MAGIC   promedio_calificacion COMMENT 'Calificación promedio del mes',
# MAGIC   resenas_extremas COMMENT 'Reseñas de 1 o 5 estrellas en el mes')
# MAGIC COMMENT 'Métricas de reseñas por año y mes'
# MAGIC AS SELECT
# MAGIC     f.anio,
# MAGIC     f.mes,
# MAGIC     f.nombre_mes,
# MAGIC     COUNT(DISTINCT r.review_id) AS total_resenas,
# MAGIC     COUNT(DISTINCT r.user_id) AS total_usuarios,
# MAGIC     COUNT(DISTINCT r.business_id) AS total_negocios,
# MAGIC     AVG(r.calificacion) AS promedio_calificacion,
# MAGIC     SUM(CASE WHEN r.es_extrema THEN 1 ELSE 0 END) AS resenas_extremas
# MAGIC FROM workspace.yelp_gold.gld_fact_resena r
# MAGIC INNER JOIN workspace.yelp_gold.gld_dim_fecha f
# MAGIC     ON r.fecha_id = f.fecha_id
# MAGIC GROUP BY
# MAGIC     f.anio,
# MAGIC     f.mes,
# MAGIC     f.nombre_mes

# COMMAND ----------

# DBTITLE 1,Validación automática
VISTAS = ['vw_vista_principal_resenas',
          'vw_analisis_fraude_resenas',
          'vw_analisis_usuarios_riesgo',
          'vw_analisis_negocios_sospechosos',
          'vw_metricas_resenas_mensuales']
G = "workspace.yelp_gold"

sin_comment = spark.sql(f"""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema = 'yelp_gold' AND table_name IN {tuple(VISTAS)}
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT: {[(r.table_name, r.column_name) for r in sin_comment]}"

# Las vistas por reseña deben conservar el grano del hecho
resenas = spark.table(f"{G}.gld_fact_resena").count()
for v in ("vw_vista_principal_resenas", "vw_analisis_fraude_resenas"):
    filas = spark.table(f"{G}.{v}").count()
    assert filas == resenas, f"❌ {v}: {filas:,} filas, se esperaban {resenas:,}"
print(f"✔ {len(VISTAS)} vistas con COMMENT · grano de reseña conservado ({resenas:,} filas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - Definiciones tomadas del notebook `VIistas`, con el que se crearon estas vistas en el workspace.
# MAGIC - Se quitó `DROP VIEW` antes de cada vista: `CREATE OR REPLACE` basta y corre en cada ejecución del job.
# MAGIC - Toda columna lleva `COMMENT`, como exige el validador de `01_modelo_fisico_ddl`; se heredan de la columna de origen.
# MAGIC - `vw_analisis_fraude_resenas` lista sus columnas en lugar de `SELECT *`, para que un cambio de esquema sea explícito.
# MAGIC - `fecha_carga` de `vw_vista_principal_resenas` devuelve la hora de la consulta, no la de la carga; se mantiene
# MAGIC   porque ya existe y su comentario lo aclara.
# MAGIC
# MAGIC ### Definition of Done (vistas de consumo)
# MAGIC - [x] DDL versionado con `COMMENT` en la vista y en sus columnas.
# MAGIC - [x] Creación de las vistas incluida en el job: corrida `1037841980114526` del 2026-11-06 (17 tareas en SUCCESS).
# MAGIC       Al reemplazarlas, el job queda como dueño de las vistas; los permisos heredados del schema no cambian.
