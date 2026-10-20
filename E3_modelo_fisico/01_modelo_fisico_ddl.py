# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # E3 · Modelo Físico — DDL sobre Delta Lake / Unity Catalog
# MAGIC **Cierra con:** M4 Modelado Avanzado · Se implementa en `yelp_silver` (conformado) y `yelp_gold` (dimensional).
# MAGIC
# MAGIC Reglas del entregable:
# MAGIC 1. **Toda tabla y toda columna llevan `COMMENT`** — ese metadata alimenta E4 (diccionario) automáticamente.
# MAGIC 2. Declarar constraints informativas de UC: `PRIMARY KEY` / `FOREIGN KEY` (no se
# MAGIC    enforcan, pero documentan el modelo y habilitan optimizaciones y lineage).
# MAGIC 3. Constraints sí enforzadas donde aplique: `NOT NULL` y `CHECK`.
# MAGIC
# MAGIC Abajo va UNA tabla completa de ejemplo; el grupo completa el resto según su modelo lógico.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Modelos conceptual y lógico» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este notebook es la materialización física de ese modelo: cada entidad y atributo del modelo lógico debe existir aquí como tabla y columna con su COMMENT.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= EJEMPLO RESUELTO: slv_business =======================
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.yelp_silver;
# MAGIC
# MAGIC -- Drop old 02_DDL tables (sin COMMENT) para evitar conflictos en la validación
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.fraud_rule;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.trust_safety_area;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.tip;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.checkin;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.review;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.business;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_silver.user;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id   STRING NOT NULL COMMENT 'Identificador único del negocio en YelpConnect',
# MAGIC   business_name STRING NOT NULL COMMENT 'Razón comercial del negocio',
# MAGIC   city          STRING          COMMENT 'Ciudad normalizada (title case, sin espacios extra)',
# MAGIC   state         STRING          COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   latitude      DOUBLE          COMMENT 'Latitud geográfica',
# MAGIC   longitude     DOUBLE          COMMENT 'Longitud geográfica',
# MAGIC   stars         DOUBLE          COMMENT 'Rating promedio publicado (1.0 a 5.0)',
# MAGIC   review_count  INT             COMMENT 'Cantidad de reseñas acumuladas',
# MAGIC   is_open       BOOLEAN         COMMENT 'TRUE si el negocio sigue operativo',
# MAGIC   categories    ARRAY<STRING>   COMMENT 'Categorías del negocio ya parseadas del string original',
# MAGIC   load_date     TIMESTAMP       COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_business PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Negocios locales registrados en YelpConnect — capa conformada. Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= slv_user =======================
# MAGIC -- Debe crearse antes que slv_review y slv_tip (FKs hacia esta tabla)
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id        STRING   NOT NULL  COMMENT 'Identificador único del usuario en YelpConnect',
# MAGIC   user_name      STRING   NOT NULL  COMMENT 'Nombre visible del usuario (PII — tag en E6)',
# MAGIC   email          STRING            COMMENT 'Correo electrónico del usuario (PII — tag en E6)',
# MAGIC   average_stars  DOUBLE   NOT NULL  COMMENT 'Promedio de estrellas otorgadas (1.0 a 5.0)',
# MAGIC   review_count   BIGINT   NOT NULL  COMMENT 'Cantidad total de reseñas escritas',
# MAGIC   fans           BIGINT   NOT NULL  COMMENT 'Número de seguidores (fans)',
# MAGIC   elite          STRING            COMMENT 'Años en que el usuario fue elite (separados por coma)',
# MAGIC   friends        STRING            COMMENT 'IDs de amigos (separados por coma)',
# MAGIC   yelping_since  TIMESTAMP NOT NULL  COMMENT 'Fecha de registro del usuario en Yelp',
# MAGIC   useful         BIGINT   NOT NULL  COMMENT 'Votos "útil" acumulados por el usuario',
# MAGIC   funny          BIGINT   NOT NULL  COMMENT 'Votos "divertido" acumulados',
# MAGIC   cool           BIGINT   NOT NULL  COMMENT 'Votos "cool" acumulados',
# MAGIC   account_status STRING            COMMENT 'Estado de la cuenta (activa, suspendida, cerrada)',
# MAGIC   load_date      TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_user PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Usuarios registrados en YelpConnect — capa conformada. Data Owner: Marketing.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= slv_review (FK → slv_user, slv_business) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id        STRING    NOT NULL  COMMENT 'Identificador único de la reseña en YelpConnect',
# MAGIC   user_id          STRING    NOT NULL  COMMENT 'FK al usuario que escribió la reseña',
# MAGIC   business_id      STRING    NOT NULL  COMMENT 'FK al negocio reseñado',
# MAGIC   rating           SMALLINT  NOT NULL  COMMENT 'Estrellas otorgadas (1 a 5)',
# MAGIC   comment          STRING    NOT NULL  COMMENT 'Texto de la reseña (PII indirecta — revisar E6)',
# MAGIC   useful           BIGINT    NOT NULL  COMMENT 'Votos "útil" recibidos',
# MAGIC   funny            BIGINT    NOT NULL  COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool             BIGINT    NOT NULL  COMMENT 'Votos "cool" recibidos',
# MAGIC   published_date   TIMESTAMP NOT NULL  COMMENT 'Fecha y hora de publicación de la reseña',
# MAGIC   review_status    STRING             COMMENT 'Estado de moderación (pendiente, aprobada, rechazada)',
# MAGIC   load_date        TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_review PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_review_user FOREIGN KEY (user_id)
# MAGIC     REFERENCES workspace.yelp_silver.slv_user (user_id),
# MAGIC   CONSTRAINT fk_review_business FOREIGN KEY (business_id)
# MAGIC     REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Reseñas de usuarios sobre negocios — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= slv_checkin (FK → slv_business) =======================
# MAGIC -- El bronze trae checkins como string de fechas concatenadas; aquí se explota a grano evento
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_checkin (
# MAGIC   checkin_id    BIGINT    NOT NULL  COMMENT 'Identificador único del check-in (surrogate key)',
# MAGIC   business_id   STRING    NOT NULL  COMMENT 'FK al negocio donde se realizó el check-in',
# MAGIC   checkin_date  TIMESTAMP NOT NULL  COMMENT 'Fecha y hora del check-in individual',
# MAGIC   load_date     TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_checkin PRIMARY KEY (checkin_id),
# MAGIC   CONSTRAINT fk_checkin_business FOREIGN KEY (business_id)
# MAGIC     REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Check-ins de clientes en negocios — granularidad evento (explodido del string original). Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= slv_tip (FK → slv_business, slv_user) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_tip (
# MAGIC   tip_id            BIGINT    NOT NULL  COMMENT 'Identificador único del tip (surrogate key)',
# MAGIC   business_id       STRING    NOT NULL  COMMENT 'FK al negocio donde se dejó el tip',
# MAGIC   user_id           STRING    NOT NULL  COMMENT 'FK al usuario que escribió el tip',
# MAGIC   tip_text          STRING    NOT NULL  COMMENT 'Texto del tip (PII indirecta — revisar E6)',
# MAGIC   compliment_count  BIGINT    NOT NULL  COMMENT 'Número de cumplidos (compliments) recibidos',
# MAGIC   tip_date          TIMESTAMP NOT NULL  COMMENT 'Fecha del tip',
# MAGIC   load_date         TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_tip PRIMARY KEY (tip_id),
# MAGIC   CONSTRAINT fk_tip_business FOREIGN KEY (business_id)
# MAGIC     REFERENCES workspace.yelp_silver.slv_business (business_id),
# MAGIC   CONSTRAINT fk_tip_user FOREIGN KEY (user_id)
# MAGIC     REFERENCES workspace.yelp_silver.slv_user (user_id)
# MAGIC )
# MAGIC COMMENT 'Tips (recomendaciones cortas) sobre negocios — capa conformada. Data Owner: Marketing.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= slv_trust_safety_area =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_trust_safety_area (
# MAGIC   team_id      BIGINT    NOT NULL  COMMENT 'Identificador único del equipo de Trust & Safety',
# MAGIC   team_name    STRING    NOT NULL  COMMENT 'Nombre del equipo',
# MAGIC   responsible  STRING    NOT NULL  COMMENT 'Persona responsable del equipo',
# MAGIC   team_status  STRING    NOT NULL  COMMENT 'Estado del equipo (activo, inactivo)',
# MAGIC   load_date    TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_trust_safety PRIMARY KEY (team_id)
# MAGIC )
# MAGIC COMMENT 'Equipos de Trust & Safety — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= slv_fraud_rule =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_fraud_rule (
# MAGIC   fraud_rule_id      BIGINT    NOT NULL  COMMENT 'Identificador único de la regla de fraude',
# MAGIC   fraud_description  STRING    NOT NULL  COMMENT 'Descripción de la regla de detección de fraude',
# MAGIC   risk_level         STRING    NOT NULL  COMMENT 'Nivel de riesgo (bajo, medio, alto)',
# MAGIC   rule_status        STRING    NOT NULL  COMMENT 'Estado de la regla (activa, inactiva)',
# MAGIC   load_date          TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_fraud_rule PRIMARY KEY (fraud_rule_id)
# MAGIC )
# MAGIC COMMENT 'Reglas de detección de fraude — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- CAPA GOLD — esquema dimensional
# MAGIC -- ============================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.yelp_gold;
# MAGIC
# MAGIC -- Drop old 02_DDL gold tables (sin COMMENT)
# MAGIC DROP TABLE IF EXISTS workspace.yelp_gold.review_assessment_criteria;
# MAGIC DROP TABLE IF EXISTS workspace.yelp_gold.review_assessment;
# MAGIC
# MAGIC -- ======================= gld_review_assessment =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_review_assessment (
# MAGIC   review_id          BIGINT    NOT NULL  COMMENT 'Identificador de la reseña evaluada (surrogate)',
# MAGIC   user_id            BIGINT    NOT NULL  COMMENT 'FK al usuario autor de la reseña',
# MAGIC   business_id        BIGINT    NOT NULL  COMMENT 'FK al negocio reseñado',
# MAGIC   assessment_result  STRING    NOT NULL  COMMENT 'Resultado de la evaluación (aprobada, sospechosa, fraudulenta)',
# MAGIC   assessment_score   SMALLINT  NOT NULL  COMMENT 'Puntuación de riesgo (0 a 100)',
# MAGIC   assessment_date    TIMESTAMP NOT NULL  COMMENT 'Fecha de la evaluación',
# MAGIC   load_date          TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_review_assessment PRIMARY KEY (review_id)
# MAGIC )
# MAGIC COMMENT 'Evaluaciones de reseñas — capa dimensional (gold). Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= gld_review_assessment_criteria =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_review_assessment_criteria (
# MAGIC   criteria_id        BIGINT    NOT NULL  COMMENT 'Identificador único del criterio',
# MAGIC   review_id          BIGINT    NOT NULL  COMMENT 'FK a la evaluación de la reseña',
# MAGIC   result_flag        SMALLINT  NOT NULL  COMMENT 'Bandera de resultado (0 = no aplica, 1 = detectado)',
# MAGIC   assessment_date    TIMESTAMP NOT NULL  COMMENT 'Fecha de la evaluación del criterio',
# MAGIC   load_date          TIMESTAMP NOT NULL  COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_criteria PRIMARY KEY (criteria_id, review_id),
# MAGIC   CONSTRAINT fk_criteria_review FOREIGN KEY (review_id)
# MAGIC     REFERENCES workspace.yelp_gold.gld_review_assessment (review_id)
# MAGIC )
# MAGIC COMMENT 'Criterios individuales por evaluación de reseña — capa dimensional (gold). Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- CHECK constraints (ALTER TABLE — UC no soporta CHECK inline)
# MAGIC -- ============================================================
# MAGIC ALTER TABLE workspace.yelp_silver.slv_business ADD CONSTRAINT chk_stars CHECK (stars BETWEEN 1.0 AND 5.0);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user ADD CONSTRAINT chk_promedio CHECK (average_stars BETWEEN 1.0 AND 5.0);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ADD CONSTRAINT chk_calificacion CHECK (rating BETWEEN 1 AND 5);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule ADD CONSTRAINT chk_risk_level CHECK (risk_level IN ('bajo', 'medio', 'alto'));
# MAGIC ALTER TABLE workspace.yelp_gold.gld_review_assessment ADD CONSTRAINT chk_score CHECK (assessment_score BETWEEN 0 AND 100);
# MAGIC ALTER TABLE workspace.yelp_gold.gld_review_assessment ADD CONSTRAINT chk_result CHECK (assessment_result IN ('aprobada', 'sospechosa', 'fraudulenta'));
# MAGIC ALTER TABLE workspace.yelp_gold.gld_review_assessment_criteria ADD CONSTRAINT chk_flag CHECK (result_flag IN (0, 1));

# COMMAND ----------

# DBTITLE 1: Validación automática del entregable (correr antes del PR)
tablas = spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()
assert len(tablas) >= 5, "Deben existir al menos las 5 entidades en silver"

sin_comment = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT (rompe E4): {[(r.table_name, r.column_name) for r in sin_comment]}"
print("✔ Modelo físico validado: todas las columnas documentadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E3)
# MAGIC - [ ] DDL completo de silver (5 entidades mínimo) y gold (esquema estrella).
# MAGIC - [ ] 100% de columnas con COMMENT (la celda de validación pasa).
# MAGIC - [ ] PK/FK declaradas y CHECK donde el negocio lo exige.
# MAGIC - [ ] Captura del diagrama de lineage/ER de Catalog Explorer en `img/`.