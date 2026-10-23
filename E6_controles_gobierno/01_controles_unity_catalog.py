# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Controles técnicos de gobierno en Unity Catalog
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma)
# MAGIC
# MAGIC La estrategia de gobierno definida en el Documento Formal de Arquitectura de Datos se **implementa** aquí con 4 controles nativos de UC:
# MAGIC tags de clasificación, permisos (GRANT), enmascaramiento de PII con vistas/funciones,
# MAGIC y evidencia de lineage. En Free Edition los 5 integrantes son usuarios del mismo
# MAGIC workspace → cada integrante "actúa" un rol del caso para probar los permisos.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Estrategia de gobierno: roles, matriz RACI y políticas de clasificación» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan los controles que hacen exigible esa estrategia. Un rol sin GRANT y una clasificación sin tag son gobierno declarativo, no efectivo.

# COMMAND ----------

# DBTITLE 1,Control 1 — Clasificación con tags
# MAGIC %sql
# MAGIC -- Etiquetar el schema y las columnas sensibles.
# MAGIC -- ✏️ TODO: completar según la matriz RACI aprobada en el Documento Formal de Arquitectura de Datos
# MAGIC -- ALTER SCHEMA workspace.yelp_silver SET TAGS ('capa' = 'silver', 'dominio' = 'yelpconnect');
# MAGIC -- ALTER TABLE workspace.yelp_silver.slv_user ALTER COLUMN nombre SET TAGS ('clasificacion' = 'pii');

# COMMAND ----------

# DBTITLE 1,Control 2 — Permisos por capa (principio de mínimo privilegio)
# MAGIC %sql
# MAGIC -- Modelo objetivo (documentarlo aunque Free Edition limite grupos):
# MAGIC --   bronze: solo el pipeline (custodian) · silver: analistas leen · gold: todos leen
# MAGIC -- ✏️ TODO: otorgar permisos a los usuarios reales del workspace simulando roles, ej.:
# MAGIC -- GRANT USAGE ON SCHEMA workspace.yelp_gold TO `usuario-marketing@grupo.com`;
# MAGIC -- GRANT SELECT ON SCHEMA workspace.yelp_gold TO `usuario-marketing@grupo.com`;
# MAGIC -- REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_bronze FROM `usuario-marketing@grupo.com`;
# MAGIC
# MAGIC -- Verificación de evidencia:
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gold;

# COMMAND ----------

# DBTITLE 1,Control 3 — Enmascaramiento de PII
# MAGIC %sql
# MAGIC -- Opción A (recomendada): función de máscara + column mask
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_nombre(nombre STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_stewards') THEN nombre   -- rol privilegiado
# MAGIC   ELSE concat(left(nombre, 1), '****')                        -- resto ve enmascarado
# MAGIC END;
# MAGIC
# MAGIC -- ✏️ TODO: aplicar la máscara a la(s) columna(s) PII de slv_user:
# MAGIC -- ALTER TABLE workspace.yelp_silver.slv_user
# MAGIC --   ALTER COLUMN nombre SET MASK workspace.yelp_gov.mascara_nombre;
# MAGIC
# MAGIC -- Opción B (si la column mask no está disponible en su workspace): vista segura
# MAGIC -- CREATE OR REPLACE VIEW workspace.yelp_gold.vw_usuarios_seguro AS
# MAGIC --   SELECT user_id, workspace.yelp_gov.mascara_nombre(nombre) AS nombre, ... FROM workspace.yelp_silver.slv_user;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Evidencia de lineage y auditoría
# MAGIC > ✏️ TODO — Insertar en `img/` capturas del lineage end-to-end de Catalog Explorer
# MAGIC > (raw → bronze → silver → gold → dashboard) para la entidad `review`, respondiendo
# MAGIC > directamente al dolor de Trust & Safety: "reglas de fraude sin linaje claro".
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [ ] Tags de clasificación aplicados a schemas y columnas PII.
# MAGIC - [ ] GRANTs ejecutados + captura de un usuario "Marketing" intentando (y fallando) leer bronze.
# MAGIC - [ ] Máscara PII funcionando: captura del mismo SELECT con dos usuarios distintos.
# MAGIC - [ ] Lineage de `review` documentado con capturas.
