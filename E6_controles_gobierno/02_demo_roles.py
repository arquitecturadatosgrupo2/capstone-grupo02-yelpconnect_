# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Demo de permisos y máscaras actuando los roles
# MAGIC **Lo ejecuta:** cada integrante con su propio usuario, en vivo (< 10 minutos). No modifica nada.
# MAGIC
# MAGIC Las mismas consultas dan resultados distintos según el rol de quien las corre: eso es el gobierno efectivo.
# MAGIC
# MAGIC > **Trazabilidad —** evidencia de la sección 5 «Estrategia de gobierno y matriz RACI» del
# MAGIC > **Documento Formal de Arquitectura de Datos** (controles aplicados por `01_controles_unity_catalog`).
# MAGIC
# MAGIC | Consulta | Marketing (consumo) | Steward / Trust & Safety |
# MAGIC |---|---|---|
# MAGIC | Bronze `brz_review` | ⛔ denegado | ⛔ denegado (solo el pipeline) |
# MAGIC | Gold `gld_fact_resena` | ✔ | ✔ |
# MAGIC | Silver `slv_user.nombre` | ⛔ sin acceso a silver | ✔ en claro |
# MAGIC | Gold `gld_user_fraud_profile` (riesgo) | ✔ con `****` / NULL | ✔ en claro |
# MAGIC
# MAGIC Mientras los integrantes sean admins del workspace (decisión provisional D1), el acceso denegado solo se ve
# MAGIC con un usuario no admin; las máscaras se ven con cualquiera.

# COMMAND ----------

# DBTITLE 1,¿Quién soy y qué roles tengo?
def intentar(titulo, sql):
    try:
        display(spark.sql(sql))
        print(f"✔ {titulo}: acceso concedido")
    except Exception as e:
        print(f"⛔ {titulo}: acceso denegado · {str(e).splitlines()[0][:200]}")

intentar("Mis roles", """
  SELECT current_user() AS usuario, rol,
         workspace.yelp_gov.fn_autorizado('confidencial') AS ve_confidencial,
         workspace.yelp_gov.fn_autorizado('restringido')  AS ve_restringido
  FROM workspace.yelp_gov.gov_rol_usuario WHERE usuario = current_user()
""")

# COMMAND ----------

# DBTITLE 1,1 · Bronze: dato crudo, solo para el pipeline
intentar("Bronze brz_review", "SELECT review_id, stars FROM workspace.yelp_bronze.brz_review LIMIT 5")

# COMMAND ----------

# DBTITLE 1,2 · Gold: capa de consumo
intentar("Gold gld_fact_resena", "SELECT * FROM workspace.yelp_gold.gld_fact_resena LIMIT 5")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Máscaras
# MAGIC Quien no tiene un rol autorizado ve `****` en textos (la inicial en nombres de persona) y `NULL` en números
# MAGIC y arreglos. Steward, Trust & Safety y el custodio ven el dato real.

# COMMAND ----------

# DBTITLE 1,3 · Silver: nombre de usuario (confidencial)
intentar("Silver slv_user.nombre", "SELECT user_id, nombre FROM workspace.yelp_silver.slv_user LIMIT 5")

# COMMAND ----------

# DBTITLE 1,4 · Gold: decisión de riesgo sobre personas (confidencial)
intentar("Gold gld_user_fraud_profile", """
  SELECT user_id, clasificacion_riesgo, score_riesgo_usuario, reglas_activadas
  FROM workspace.yelp_gold.gld_user_fraud_profile
  WHERE score_riesgo_usuario IS NULL OR score_riesgo_usuario > 0
  LIMIT 5
""")
