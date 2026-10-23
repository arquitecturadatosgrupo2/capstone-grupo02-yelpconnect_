# Databricks notebook source
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
# MAGIC El DDL es **idempotente**: `CREATE TABLE IF NOT EXISTS` no toca tablas ya pobladas, y los `CHECK` se
# MAGIC recrean con `DROP CONSTRAINT IF EXISTS` + `ADD CONSTRAINT`. Se puede re-ejecutar sin perder datos.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Modelos conceptual y lógico» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este notebook es la materialización física de ese modelo: cada entidad y atributo del modelo lógico debe existir aquí como tabla y columna con su COMMENT.
# MAGIC > Fuente del diseño: `YELP.sql` y `mapeo_equipo_02_utec.xlsx` (carpeta del grupo).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= SILVER · entidades maestras =======================
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.yelp_silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id   STRING NOT NULL COMMENT 'Identificador único del negocio en YelpConnect',
# MAGIC   nombre        STRING NOT NULL COMMENT 'Razón comercial del negocio',
# MAGIC   ciudad        STRING          COMMENT 'Ciudad normalizada (title case, sin espacios extra)',
# MAGIC   estado        STRING          COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   latitud       DOUBLE          COMMENT 'Latitud geográfica',
# MAGIC   longitud      DOUBLE          COMMENT 'Longitud geográfica',
# MAGIC   estrellas     DOUBLE          COMMENT 'Rating promedio publicado (1.0 a 5.0)',
# MAGIC   n_resenas     INT             COMMENT 'Cantidad de reseñas acumuladas',
# MAGIC   esta_abierto  BOOLEAN         COMMENT 'TRUE si el negocio sigue operativo',
# MAGIC   categorias    ARRAY<STRING>   COMMENT 'Categorías del negocio ya parseadas del string original',
# MAGIC   fecha_carga   TIMESTAMP       COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_business PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Negocios locales registrados en YelpConnect — capa conformada. Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id            STRING    NOT NULL COMMENT 'Identificador único del usuario en YelpConnect',
# MAGIC   nombre             STRING    NOT NULL COMMENT 'Nombre visible del usuario (PII — tag en E6)',
# MAGIC   promedio_estrellas DOUBLE    NOT NULL COMMENT 'Promedio de estrellas otorgadas (1.0 a 5.0)',
# MAGIC   n_resenas          BIGINT    NOT NULL COMMENT 'Cantidad total de reseñas escritas',
# MAGIC   fans               BIGINT    NOT NULL COMMENT 'Número de seguidores (fans)',
# MAGIC   elite              STRING             COMMENT 'Años en que el usuario fue elite (separados por coma)',
# MAGIC   amigos             STRING             COMMENT 'IDs de amigos (separados por coma)',
# MAGIC   miembro_desde      TIMESTAMP NOT NULL COMMENT 'Fecha de registro del usuario en Yelp',
# MAGIC   util               BIGINT    NOT NULL COMMENT 'Votos "útil" acumulados por el usuario',
# MAGIC   divertido          BIGINT    NOT NULL COMMENT 'Votos "divertido" acumulados',
# MAGIC   cool               BIGINT    NOT NULL COMMENT 'Votos "cool" acumulados',
# MAGIC   estado_cuenta      STRING             COMMENT 'Estado de la cuenta (activa, suspendida, cerrada)',
# MAGIC   fecha_carga        TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_user PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Usuarios registrados en YelpConnect — capa conformada. Data Owner: Marketing.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= SILVER · contenido e interacción (FK → slv_business, slv_user) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id         STRING    NOT NULL COMMENT 'Identificador único de la reseña en YelpConnect',
# MAGIC   user_id           STRING    NOT NULL COMMENT 'FK al usuario que escribió la reseña',
# MAGIC   business_id       STRING    NOT NULL COMMENT 'FK al negocio reseñado',
# MAGIC   calificacion      SMALLINT  NOT NULL COMMENT 'Estrellas otorgadas (1 a 5)',
# MAGIC   comentario        STRING    NOT NULL COMMENT 'Texto de la reseña (PII indirecta — revisar E6)',
# MAGIC   util              BIGINT    NOT NULL COMMENT 'Votos "útil" recibidos',
# MAGIC   divertido         BIGINT    NOT NULL COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool              BIGINT    NOT NULL COMMENT 'Votos "cool" recibidos',
# MAGIC   fecha_publicacion TIMESTAMP NOT NULL COMMENT 'Fecha y hora de publicación de la reseña',
# MAGIC   estado_resena     STRING             COMMENT 'Estado de moderación (pendiente, aprobada, rechazada)',
# MAGIC   autor_en_muestra  BOOLEAN            COMMENT 'FALSE si el autor no está en slv_user por el recorte de usuarios del muestreo (riesgo aceptado)',
# MAGIC   fecha_carga       TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_review PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id),
# MAGIC   CONSTRAINT fk_review_user FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user (user_id)
# MAGIC )
# MAGIC COMMENT 'Reseñas de usuarios sobre negocios, sin duplicados lógicos — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Las reseñas que no pasan las reglas de integridad no se descartan: quedan aquí con su motivo
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review_cuarentena (
# MAGIC   review_id            STRING             COMMENT 'Identificador de la reseña tal como llegó (puede ser nulo)',
# MAGIC   user_id              STRING             COMMENT 'Usuario autor de la reseña',
# MAGIC   business_id          STRING             COMMENT 'Negocio reseñado',
# MAGIC   calificacion         SMALLINT           COMMENT 'Estrellas otorgadas',
# MAGIC   comentario           STRING             COMMENT 'Texto de la reseña (PII indirecta — revisar E6)',
# MAGIC   util                 BIGINT             COMMENT 'Votos "útil" recibidos',
# MAGIC   divertido            BIGINT             COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool                 BIGINT             COMMENT 'Votos "cool" recibidos',
# MAGIC   fecha_publicacion    TIMESTAMP          COMMENT 'Fecha y hora de publicación de la reseña',
# MAGIC   motivo               STRING    NOT NULL COMMENT 'Por qué quedó en cuarentena: review_id_nulo, review_id_duplicado, duplicado_logico, negocio_inexistente',
# MAGIC   review_id_conservado STRING             COMMENT 'Para duplicados: review_id de la reseña que sí quedó en slv_review',
# MAGIC   fecha_carga          TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)'
# MAGIC )
# MAGIC COMMENT 'Reseñas apartadas de slv_review por reglas de integridad, con su motivo. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- checkin llega como string de fechas concatenadas: decisión de modelado = grano evento (1 fila = 1 check-in)
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_checkin (
# MAGIC   checkin_id    BIGINT    NOT NULL COMMENT 'Identificador único del check-in (surrogate key)',
# MAGIC   business_id   STRING    NOT NULL COMMENT 'FK al negocio donde se realizó el check-in',
# MAGIC   fecha_checkin TIMESTAMP NOT NULL COMMENT 'Fecha y hora del check-in individual',
# MAGIC   fecha_carga   TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_checkin PRIMARY KEY (checkin_id),
# MAGIC   CONSTRAINT fk_checkin_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Check-ins de clientes en negocios — granularidad evento (explodido del string original). Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_tip (
# MAGIC   tip_id      BIGINT    NOT NULL COMMENT 'Identificador único del tip (surrogate key)',
# MAGIC   business_id STRING    NOT NULL COMMENT 'FK al negocio donde se dejó el tip',
# MAGIC   user_id     STRING    NOT NULL COMMENT 'FK al usuario que escribió el tip',
# MAGIC   texto       STRING    NOT NULL COMMENT 'Texto del tip (PII indirecta — revisar E6)',
# MAGIC   cumplidos   BIGINT    NOT NULL COMMENT 'Número de cumplidos (compliments) recibidos',
# MAGIC   fecha_tip   TIMESTAMP NOT NULL COMMENT 'Fecha del tip',
# MAGIC   fecha_carga TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_tip PRIMARY KEY (tip_id),
# MAGIC   CONSTRAINT fk_tip_user FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user (user_id),
# MAGIC   CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Tips (recomendaciones cortas) sobre negocios — capa conformada. Data Owner: Marketing.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= SILVER · Trust & Safety (fuentes simuladas del caso) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_trust_safety_area (
# MAGIC   team_id     BIGINT    NOT NULL COMMENT 'Identificador único del equipo de Trust & Safety',
# MAGIC   team_name   STRING    NOT NULL COMMENT 'Nombre del equipo',
# MAGIC   responsable STRING    NOT NULL COMMENT 'Persona responsable del equipo',
# MAGIC   team_status STRING    NOT NULL COMMENT 'Estado del equipo (activo, inactivo)',
# MAGIC   fecha_carga TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_trust_safety PRIMARY KEY (team_id)
# MAGIC )
# MAGIC COMMENT 'Equipos de Trust & Safety — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Catálogo de reglas que gobierna gold: cambiar estado, umbral o puntos aquí cambia el resultado del pipeline
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_fraud_rule (
# MAGIC   fraud_rule_id     BIGINT    NOT NULL COMMENT 'Identificador único de la regla de fraude',
# MAGIC   fraud_description STRING    NOT NULL COMMENT 'Descripción de la regla de detección de fraude',
# MAGIC   entidad           STRING    NOT NULL COMMENT 'Sobre qué se evalúa la regla (usuario, negocio)',
# MAGIC   umbral            DOUBLE             COMMENT 'Valor de corte de la regla; su significado depende de la regla (ver descripción)',
# MAGIC   min_resenas       INT                COMMENT 'Mínimo de reseñas para que la regla aplique (evita falsos positivos con poco volumen)',
# MAGIC   puntos            INT                COMMENT 'Puntos que suma al score de riesgo del usuario (solo reglas de usuario)',
# MAGIC   risk_level        STRING    NOT NULL COMMENT 'Nivel de riesgo (bajo, medio, alto)',
# MAGIC   rule_status       STRING    NOT NULL COMMENT 'Estado de la regla (activa, inactiva); solo las activas se aplican',
# MAGIC   team_id           BIGINT             COMMENT 'FK al equipo de Trust & Safety responsable de la regla',
# MAGIC   fecha_carga       TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_fraud_rule PRIMARY KEY (fraud_rule_id),
# MAGIC   CONSTRAINT fk_fraud_rule_team FOREIGN KEY (team_id) REFERENCES workspace.yelp_silver.slv_trust_safety_area (team_id)
# MAGIC )
# MAGIC COMMENT 'Catálogo de reglas de detección de fraude con sus parámetros y equipo responsable — capa conformada. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= SILVER · CHECK enforzados (idempotentes) =======================
# MAGIC ALTER TABLE workspace.yelp_silver.slv_business   DROP CONSTRAINT IF EXISTS chk_estrellas;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_business   ADD CONSTRAINT chk_estrellas    CHECK (estrellas BETWEEN 1.0 AND 5.0);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user       DROP CONSTRAINT IF EXISTS chk_promedio;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user       ADD CONSTRAINT chk_promedio     CHECK (promedio_estrellas BETWEEN 1.0 AND 5.0);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review     DROP CONSTRAINT IF EXISTS chk_calificacion;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review     ADD CONSTRAINT chk_calificacion CHECK (calificacion BETWEEN 1 AND 5);
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review_cuarentena DROP CONSTRAINT IF EXISTS chk_motivo;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review_cuarentena ADD CONSTRAINT chk_motivo
# MAGIC   CHECK (motivo IN ('review_id_nulo', 'review_id_duplicado', 'duplicado_logico', 'negocio_inexistente'));
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule DROP CONSTRAINT IF EXISTS chk_risk_level;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule ADD CONSTRAINT chk_risk_level   CHECK (risk_level IN ('bajo', 'medio', 'alto'));
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule DROP CONSTRAINT IF EXISTS chk_rule_status;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule ADD CONSTRAINT chk_rule_status  CHECK (rule_status IN ('activa', 'inactiva'));
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule DROP CONSTRAINT IF EXISTS chk_entidad;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule ADD CONSTRAINT chk_entidad      CHECK (entidad IN ('usuario', 'negocio'));
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule DROP CONSTRAINT IF EXISTS chk_puntos;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_fraud_rule ADD CONSTRAINT chk_puntos       CHECK (puntos IS NULL OR puntos BETWEEN 0 AND 100);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= GOLD · productos de datos de Trust & Safety =======================
# MAGIC -- Nombres con prefijo gld_ (convención obligatoria). En YELP.sql y el mapeo figuran como fact_/dim_.
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.yelp_gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_user_fraud_profile (
# MAGIC   user_id                            STRING NOT NULL COMMENT 'FK al usuario evaluado',
# MAGIC   total_resenas                      INT       COMMENT 'Cantidad total de reseñas históricas del usuario',
# MAGIC   promedio_estrellas_otorgadas       DOUBLE    COMMENT 'Promedio histórico de calificaciones otorgadas por el usuario',
# MAGIC   desviacion_estandar_estrellas      DOUBLE    COMMENT 'Desviación estándar de sus calificaciones (0 suele indicar comportamiento de bot)',
# MAGIC   porcentaje_calificaciones_extremas DOUBLE    COMMENT 'Porcentaje de reseñas con calificación 1 o 5 sobre el total del usuario',
# MAGIC   max_resenas_en_un_dia              INT       COMMENT 'Máximo de reseñas escritas por el usuario en un mismo día',
# MAGIC   amigos_count                       INT       COMMENT 'Cantidad de amigos en su red',
# MAGIC   fans_count                         BIGINT    COMMENT 'Cantidad de seguidores',
# MAGIC   antiguedad_dias                    INT       COMMENT 'Días transcurridos desde el registro del usuario',
# MAGIC   reglas_activadas                   ARRAY<BIGINT> COMMENT 'fraud_rule_id de las reglas activas de usuario que se cumplen (linaje regla → caso)',
# MAGIC   score_riesgo_usuario               DOUBLE    COMMENT 'Suma de puntos de las reglas activadas, tope 100',
# MAGIC   clasificacion_riesgo               STRING    COMMENT 'Categoría de riesgo: Bajo, Moderado o Alto',
# MAGIC   fecha_actualizacion                TIMESTAMP COMMENT 'Timestamp de cálculo de la métrica',
# MAGIC   CONSTRAINT pk_user_fraud PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Perfil analítico de riesgo por usuario para detección de cuentas falsas y bots. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_business_anomaly_bursts (
# MAGIC   business_id                STRING NOT NULL COMMENT 'FK al negocio evaluado',
# MAGIC   fecha_ventana              DATE   NOT NULL COMMENT 'Día analizado (ventana diaria)',
# MAGIC   resenas_en_ventana         INT       COMMENT 'Reseñas recibidas por el negocio en la ventana',
# MAGIC   promedio_estrellas_ventana DOUBLE    COMMENT 'Calificación promedio obtenida en la ventana',
# MAGIC   checkins_en_ventana        INT       COMMENT 'Check-ins registrados en la misma ventana',
# MAGIC   ratio_resenas_checkins     DOUBLE    COMMENT 'Reseñas / check-ins de la ventana (valores muy altos sugieren fraude)',
# MAGIC   reglas_activadas           ARRAY<BIGINT> COMMENT 'fraud_rule_id de las reglas activas de negocio que se cumplen en el día (linaje regla → caso)',
# MAGIC   es_rafaga_sospechosa       BOOLEAN   COMMENT 'TRUE si al menos una regla activa de negocio se cumple en el día',
# MAGIC   fecha_actualizacion        TIMESTAMP COMMENT 'Timestamp de procesamiento',
# MAGIC   CONSTRAINT pk_business_anomaly PRIMARY KEY (business_id, fecha_ventana)
# MAGIC )
# MAGIC COMMENT 'Anomalías y ráfagas de reseñas por negocio y día. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_trust_governance (
# MAGIC   fraud_rule_id       BIGINT NOT NULL COMMENT 'ID de la regla de fraude',
# MAGIC   fraud_description   STRING    COMMENT 'Descripción de la regla de detección',
# MAGIC   entidad             STRING    COMMENT 'Sobre qué se evalúa la regla (usuario, negocio)',
# MAGIC   umbral              DOUBLE    COMMENT 'Valor de corte vigente de la regla',
# MAGIC   puntos              INT       COMMENT 'Puntos que suma al score de riesgo (reglas de usuario)',
# MAGIC   risk_level          STRING    COMMENT 'Nivel de riesgo (bajo, medio, alto)',
# MAGIC   rule_status         STRING    COMMENT 'Estado de la regla (activa, inactiva)',
# MAGIC   esta_implementada   BOOLEAN   COMMENT 'TRUE si el pipeline tiene lógica para aplicar la regla',
# MAGIC   n_casos_marcados    BIGINT    COMMENT 'Usuarios o negocio-día marcados por la regla en la última corrida (0 si no aplica)',
# MAGIC   team_id             BIGINT    COMMENT 'ID del equipo de Trust & Safety responsable',
# MAGIC   team_name           STRING    COMMENT 'Nombre del equipo responsable',
# MAGIC   responsable         STRING    COMMENT 'Persona responsable del equipo',
# MAGIC   team_status         STRING    COMMENT 'Estado operativo del equipo',
# MAGIC   alerta_gobierno     STRING    COMMENT 'Problema de gobierno detectado sobre la regla (nulo si no hay)',
# MAGIC   fecha_actualizacion TIMESTAMP COMMENT 'Timestamp de consolidación',
# MAGIC   CONSTRAINT pk_trust_governance PRIMARY KEY (fraud_rule_id)
# MAGIC )
# MAGIC COMMENT 'Una fila por regla de fraude: parámetros vigentes, equipo responsable, casos marcados y alertas de gobierno. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= GOLD · esquema estrella (dimensiones) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_fecha (
# MAGIC   fecha_id         INT     NOT NULL COMMENT 'Clave de la fecha en formato yyyymmdd',
# MAGIC   fecha            DATE    NOT NULL COMMENT 'Fecha calendario',
# MAGIC   anio             INT     NOT NULL COMMENT 'Año',
# MAGIC   trimestre        INT     NOT NULL COMMENT 'Trimestre del año (1 a 4)',
# MAGIC   mes              INT     NOT NULL COMMENT 'Mes del año (1 a 12)',
# MAGIC   nombre_mes       STRING  NOT NULL COMMENT 'Nombre del mes en español',
# MAGIC   dia              INT     NOT NULL COMMENT 'Día del mes',
# MAGIC   dia_semana       INT     NOT NULL COMMENT 'Día de la semana ISO (1 = lunes, 7 = domingo)',
# MAGIC   nombre_dia       STRING  NOT NULL COMMENT 'Nombre del día de la semana en español',
# MAGIC   es_fin_de_semana BOOLEAN NOT NULL COMMENT 'TRUE si es sábado o domingo',
# MAGIC   CONSTRAINT pk_dim_fecha PRIMARY KEY (fecha_id)
# MAGIC )
# MAGIC COMMENT 'Dimensión calendario generada programáticamente para el rango de reseñas y check-ins. Data Owner: CDO (Data Platform).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_negocio (
# MAGIC   business_id         STRING        NOT NULL COMMENT 'Identificador único del negocio (clave natural)',
# MAGIC   nombre              STRING        NOT NULL COMMENT 'Razón comercial del negocio',
# MAGIC   ciudad              STRING                 COMMENT 'Ciudad normalizada',
# MAGIC   estado              STRING                 COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   categorias          ARRAY<STRING>          COMMENT 'Categorías del negocio',
# MAGIC   categoria_principal STRING                 COMMENT 'Primera categoría declarada por el negocio',
# MAGIC   estrellas           DOUBLE                 COMMENT 'Rating promedio publicado (1.0 a 5.0)',
# MAGIC   esta_abierto        BOOLEAN                COMMENT 'TRUE si el negocio sigue operativo',
# MAGIC   fecha_actualizacion TIMESTAMP              COMMENT 'Timestamp de carga de la dimensión',
# MAGIC   CONSTRAINT pk_dim_negocio PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Dimensión de negocios (SCD tipo 1: se sobrescribe con la última foto de silver). Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_usuario (
# MAGIC   user_id             STRING    NOT NULL COMMENT 'Identificador único del usuario (clave natural)',
# MAGIC   miembro_desde       TIMESTAMP          COMMENT 'Fecha de registro del usuario en Yelp',
# MAGIC   anio_registro       INT                COMMENT 'Año de registro (para cohortes)',
# MAGIC   n_resenas_historico BIGINT             COMMENT 'Reseñas escritas según el perfil de Yelp (histórico, no solo la muestra)',
# MAGIC   fans                BIGINT             COMMENT 'Número de seguidores',
# MAGIC   es_elite            BOOLEAN            COMMENT 'TRUE si el usuario fue elite al menos un año',
# MAGIC   estado_cuenta       STRING             COMMENT 'Estado de la cuenta (activa, suspendida, cerrada)',
# MAGIC   es_miembro_inferido BOOLEAN   NOT NULL COMMENT 'TRUE si el usuario aparece en reseñas pero no en el perfil de origen (atributos nulos)',
# MAGIC   fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de carga de la dimensión',
# MAGIC   CONSTRAINT pk_dim_usuario PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Dimensión de usuarios sin datos personales directos (SCD tipo 1). Data Owner: Marketing.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= GOLD · esquema estrella (hechos, FK → dimensiones) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_resena (
# MAGIC   review_id     STRING   NOT NULL COMMENT 'Identificador único de la reseña (grano del hecho)',
# MAGIC   fecha_id      INT      NOT NULL COMMENT 'FK a gld_dim_fecha: día de publicación',
# MAGIC   business_id   STRING   NOT NULL COMMENT 'FK a gld_dim_negocio: negocio reseñado',
# MAGIC   user_id       STRING   NOT NULL COMMENT 'FK a gld_dim_usuario: autor de la reseña',
# MAGIC   calificacion  SMALLINT NOT NULL COMMENT 'Estrellas otorgadas (1 a 5)',
# MAGIC   es_extrema    BOOLEAN  NOT NULL COMMENT 'TRUE si la calificación es 1 o 5',
# MAGIC   util          BIGINT            COMMENT 'Votos "útil" recibidos',
# MAGIC   divertido     BIGINT            COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool          BIGINT            COMMENT 'Votos "cool" recibidos',
# MAGIC   n_caracteres  INT               COMMENT 'Longitud del texto de la reseña (el texto no se publica en gold)',
# MAGIC   CONSTRAINT pk_fact_resena PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_fact_resena_fecha FOREIGN KEY (fecha_id) REFERENCES workspace.yelp_gold.gld_dim_fecha (fecha_id),
# MAGIC   CONSTRAINT fk_fact_resena_negocio FOREIGN KEY (business_id) REFERENCES workspace.yelp_gold.gld_dim_negocio (business_id),
# MAGIC   CONSTRAINT fk_fact_resena_usuario FOREIGN KEY (user_id) REFERENCES workspace.yelp_gold.gld_dim_usuario (user_id)
# MAGIC )
# MAGIC COMMENT 'Hecho de reseñas, grano 1 fila por reseña. Data Owner: Trust & Safety.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_checkin_diario (
# MAGIC   business_id STRING NOT NULL COMMENT 'FK a gld_dim_negocio',
# MAGIC   fecha_id    INT    NOT NULL COMMENT 'FK a gld_dim_fecha',
# MAGIC   n_checkins  INT    NOT NULL COMMENT 'Conteo oficial de check-ins del negocio en el día',
# MAGIC   CONSTRAINT pk_fact_checkin_diario PRIMARY KEY (business_id, fecha_id),
# MAGIC   CONSTRAINT fk_fact_checkin_negocio FOREIGN KEY (business_id) REFERENCES workspace.yelp_gold.gld_dim_negocio (business_id),
# MAGIC   CONSTRAINT fk_fact_checkin_fecha FOREIGN KEY (fecha_id) REFERENCES workspace.yelp_gold.gld_dim_fecha (fecha_id)
# MAGIC )
# MAGIC COMMENT 'Hecho de check-ins agregado por negocio y día: conteo oficial para Operaciones Comerciales. Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC ALTER TABLE workspace.yelp_gold.gld_fact_resena DROP CONSTRAINT IF EXISTS chk_fact_calificacion;
# MAGIC ALTER TABLE workspace.yelp_gold.gld_fact_resena ADD CONSTRAINT chk_fact_calificacion CHECK (calificacion BETWEEN 1 AND 5);

# COMMAND ----------

# DBTITLE 1,Validación automática del entregable (correr antes del PR)
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
# MAGIC ### Decisiones de implementación
# MAGIC - Silver en español y con los nombres del mapeo bronze → silver; gold renombrado a `gld_*` por convención
# MAGIC   (en `YELP.sql` y el mapeo figuran como `fact_user_fraud_profile`, `fact_business_anomaly_bursts` y
# MAGIC   `dim_trust_governance`: actualizar ambos documentos).
# MAGIC - `slv_checkin` a grano evento y llaves sustitutas determinísticas (`xxhash64`) en `slv_checkin` y `slv_tip`.
# MAGIC - Los CHECK se declaran con `ALTER TABLE` separado del `CREATE` para que el notebook sea re-ejecutable.
# MAGIC - `slv_fraud_rule` es un catálogo que gobierna gold: estado, umbral, mínimo de reseñas y puntos de cada regla
# MAGIC   se leen de la tabla, no del código, y `team_id` asigna un equipo responsable. `slv_trust_safety_area` se crea
# MAGIC   antes por la FK.
# MAGIC - Esquema estrella en gold con prefijo `gld_dim_` / `gld_fact_`; dimensiones SCD tipo 1 con clave natural
# MAGIC   (la muestra es una sola foto del origen: no hay historia que versionar). `gld_dim_usuario` excluye el nombre
# MAGIC   y `gld_fact_resena` excluye el texto: gold no expone PII.
# MAGIC
# MAGIC ### Definition of Done (E3)
# MAGIC - [x] DDL completo de silver (7 entidades) y gold (3 productos de datos + estrella de 3 dims y 2 facts).
# MAGIC - [ ] 100% de columnas con COMMENT (la celda de validación pasa).
# MAGIC - [x] PK/FK declaradas y CHECK donde el negocio lo exige.
# MAGIC - [ ] Captura del diagrama de lineage/ER de Catalog Explorer en `img/`.
