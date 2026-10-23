# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: productos de datos
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo)
# MAGIC
# MAGIC Gold materializa el modelo dimensional definido en el Documento Formal de Arquitectura de Datos y las tablas de KPIs que consumirá
# MAGIC el dashboard E8. **Cada tabla gold debe responder a UNA pregunta de UN stakeholder
# MAGIC del caso** — si no pueden nombrar al consumidor, la tabla no debería existir.
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta que responde |
# MAGIC |---|---|---|
# MAGIC | `gld_kpi_negocio_mensual` | Producto (Andrew) | ¿Cómo evoluciona rating y volumen de reseñas por negocio/mes? |
# MAGIC | `gld_checkins_diarios` | Ops Comerciales (Carla) | Conteo OFICIAL de check-ins por negocio/día |
# MAGIC | ✏️ TODO `gld_salud_resenas` | Trust & Safety (Rafael) | % reseñas en cuarentena, señales de duplicidad |
# MAGIC | ✏️ TODO dimensional completo | Data Science (Mei) | dim/fact para modelos con datos versionados |

# COMMAND ----------

# DBTITLE 1,Ejemplo resuelto — gld_kpi_negocio_mensual
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.yelp_gold.gld_kpi_negocio_mensual
# MAGIC COMMENT 'KPIs mensuales por negocio para Producto. Fuente: slv_review + slv_business.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   b.business_id,
# MAGIC   b.nombre,
# MAGIC   b.ciudad,
# MAGIC   date_trunc('month', r.fecha_resena)          AS mes,
# MAGIC   count(*)                                     AS n_resenas,
# MAGIC   round(avg(r.stars), 2)                       AS rating_promedio,
# MAGIC   count(DISTINCT r.user_id)                    AS usuarios_unicos
# MAGIC FROM workspace.yelp_silver.slv_review r
# MAGIC JOIN workspace.yelp_silver.slv_business b USING (business_id)
# MAGIC GROUP BY ALL;

# COMMAND ----------

# DBTITLE 1,✏️ TODO — gld_checkins_diarios, gld_salud_resenas y el esquema estrella
# Requisitos:
#  - dim_fecha generada programáticamente (sequence + explode).
#  - dim_negocio con SCD según lo decidido en el modelo lógico del Documento Formal de Arquitectura de Datos.
#  - fact_resena con FKs declaradas hacia las dimensiones.
#  - Todas las tablas con COMMENT (recuerden: alimentan el diccionario E4).

# COMMAND ----------

# DBTITLE 1,Validación — cada tabla gold tiene consumidor declarado
tablas_gold = [r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()]
assert len(tablas_gold) >= 5, f"Se esperan ≥5 objetos gold (hay {len(tablas_gold)})"
print("✔ Capa gold:", tablas_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.3)
# MAGIC - [ ] ≥2 tablas de KPIs + esquema estrella completo (≥3 dims, ≥1 fact).
# MAGIC - [ ] Tabla stakeholder→producto→pregunta completa en el markdown superior.
# MAGIC - [ ] Lineage silver→gold visible en Catalog Explorer (captura en `img/`).
