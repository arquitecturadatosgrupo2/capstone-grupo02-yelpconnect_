# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Vista analítica para Power BI: `vw_fraude_resena`
# MAGIC Una fila por reseña, con temporalidad, perfil de fraude del usuario, reglas con descripción y anomalías del
# MAGIC negocio en el día. Definición tomada del workspace (`SHOW CREATE TABLE`, 2026-11-05).
# MAGIC
# MAGIC La tarea `GOLD_vista_power_bi` del job la crea o actualiza después de gold. En entornos existentes, la celda de
# MAGIC migración de `nb_gold_user_fraud_profile` mantiene la vista alineada con el esquema de la tabla.
# MAGIC
# MAGIC Antes se llamaba `gld_vw_fraude_resena`, fuera de la convención `vw_<nombre>`. La vista antigua sigue en el workspace,
# MAGIC sin versionar, hasta que el reporte de Power BI lea el nombre nuevo; después se borra con `DROP VIEW`.
# MAGIC
# MAGIC > **Trazabilidad —** este notebook **implementa** la sección 4 «Modelo de datos conceptual y lógico» y el canal
# MAGIC > Power BI de la decisión ADR-09 (secciones 3.4 y 7) del **Documento Formal de Arquitectura de Datos**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_fraude_resena (
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
# MAGIC COMMENT 'Vista analítica Power BI: una fila por reseña, combinando temporalidad (gld_dim_fecha), perfil de fraude del usuario (gld_user_fraud_profile), reglas de usuario con descripción (gld_trust_governance) y anomalías/ráfagas del negocio en el día de la reseña (gld_business_anomaly_bursts). Grano: 1 fila = 1 reseña.'
# MAGIC WITH SCHEMA COMPENSATION
# MAGIC AS WITH reglas_usuario AS (
# MAGIC   SELECT
# MAGIC     ue.user_id,
# MAGIC     COUNT(tg.fraud_rule_id) AS n_reglas_usuario,
# MAGIC     ARRAY_JOIN(
# MAGIC       COLLECT_LIST(
# MAGIC         CONCAT('Regla ', CAST(ue.fraud_rule_id AS STRING), ' (', COALESCE(tg.risk_level,'?'), '): ', COALESCE(tg.fraud_description,'sin descripción'))
# MAGIC       ), ' | '
# MAGIC     ) AS reglas_fraude_usuario
# MAGIC   FROM (
# MAGIC     SELECT user_id, explode(reglas_activadas) AS fraud_rule_id
# MAGIC     FROM workspace.yelp_gold.gld_user_fraud_profile
# MAGIC   ) ue
# MAGIC   LEFT JOIN workspace.yelp_gold.gld_trust_governance tg
# MAGIC          ON tg.fraud_rule_id = ue.fraud_rule_id
# MAGIC   GROUP BY ue.user_id
# MAGIC ),
# MAGIC reglas_negocio AS (
# MAGIC   SELECT
# MAGIC     be.business_id,
# MAGIC     be.fecha_ventana,
# MAGIC     COUNT(tg.fraud_rule_id) AS n_reglas_negocio,
# MAGIC     ARRAY_JOIN(
# MAGIC       COLLECT_LIST(
# MAGIC         CONCAT('Regla ', CAST(be.fraud_rule_id AS STRING), ' (', COALESCE(tg.risk_level,'?'), '): ', COALESCE(tg.fraud_description,'sin descripción'))
# MAGIC       ), ' | '
# MAGIC     ) AS reglas_fraude_negocio
# MAGIC   FROM (
# MAGIC     SELECT business_id, fecha_ventana, explode(reglas_activadas) AS fraud_rule_id
# MAGIC     FROM workspace.yelp_gold.gld_business_anomaly_bursts
# MAGIC   ) be
# MAGIC   LEFT JOIN workspace.yelp_gold.gld_trust_governance tg
# MAGIC          ON tg.fraud_rule_id = be.fraud_rule_id
# MAGIC   GROUP BY be.business_id, be.fecha_ventana
# MAGIC )
# MAGIC SELECT
# MAGIC   f.review_id,
# MAGIC   f.user_id,
# MAGIC   f.business_id,
# MAGIC   d.fecha,
# MAGIC   d.anio,
# MAGIC   d.trimestre,
# MAGIC   d.mes,
# MAGIC   d.nombre_mes,
# MAGIC   date_format(d.fecha, 'yyyy-MM') AS anio_mes,
# MAGIC   d.dia,
# MAGIC   d.dia_semana,
# MAGIC   d.nombre_dia,
# MAGIC   d.es_fin_de_semana,
# MAGIC   f.calificacion,
# MAGIC   f.es_extrema,
# MAGIC   f.util,
# MAGIC   f.divertido,
# MAGIC   f.cool,
# MAGIC   f.n_caracteres,
# MAGIC   u.total_resenas,
# MAGIC   u.promedio_estrellas_historico,
# MAGIC   u.n_resenas_dia_registro,
# MAGIC   u.desviacion_estandar_estrellas,
# MAGIC   u.porcentaje_calificaciones_extremas,
# MAGIC   u.max_resenas_en_un_dia,
# MAGIC   u.amigos_count,
# MAGIC   u.fans_count,
# MAGIC   u.antiguedad_dias,
# MAGIC   u.score_riesgo_usuario,
# MAGIC   u.clasificacion_riesgo,
# MAGIC   COALESCE(ru.n_reglas_usuario, 0)                AS n_reglas_usuario,
# MAGIC   COALESCE(ru.reglas_fraude_usuario, 'Sin reglas activadas') AS reglas_fraude_usuario,
# MAGIC   COALESCE(ab.es_rafaga_sospechosa, FALSE)        AS dia_sospechoso_negocio,
# MAGIC   COALESCE(ab.resenas_en_ventana, 0)              AS resenas_negocio_en_dia,
# MAGIC   COALESCE(ab.checkins_en_ventana, 0)             AS checkins_negocio_en_dia,
# MAGIC   ab.ratio_resenas_checkins,
# MAGIC   ab.promedio_estrellas_ventana,
# MAGIC   COALESCE(rn.n_reglas_negocio, 0)                AS n_reglas_negocio,
# MAGIC   COALESCE(rn.reglas_fraude_negocio, 'Sin reglas activadas') AS reglas_fraude_negocio
# MAGIC FROM workspace.yelp_gold.gld_fact_resena f
# MAGIC JOIN workspace.yelp_gold.gld_dim_fecha d
# MAGIC   ON d.fecha_id = f.fecha_id
# MAGIC LEFT JOIN workspace.yelp_gold.gld_user_fraud_profile u
# MAGIC   ON u.user_id = f.user_id
# MAGIC LEFT JOIN reglas_usuario ru
# MAGIC   ON ru.user_id = f.user_id
# MAGIC LEFT JOIN workspace.yelp_gold.gld_business_anomaly_bursts ab
# MAGIC   ON ab.business_id = f.business_id
# MAGIC  AND ab.fecha_ventana = d.fecha
# MAGIC LEFT JOIN reglas_negocio rn
# MAGIC   ON rn.business_id = f.business_id
# MAGIC  AND rn.fecha_ventana = d.fecha
