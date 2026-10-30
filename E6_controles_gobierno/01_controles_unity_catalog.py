# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Controles técnicos de gobierno en Unity Catalog
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma) · **Lo ejecuta:** el custodio del pipeline,
# MAGIC después de E4 y E5 y cada vez que cambia la clasificación del diccionario.
# MAGIC
# MAGIC La estrategia de gobierno se vuelve exigible con controles nativos de Unity Catalog. En Free Edition no hay
# MAGIC grupos de cuenta, así que los **roles se modelan como datos** (`yelp_gov.gov_rol_usuario`) y el notebook
# MAGIC **reconcilia** el catálogo contra esas tablas: lo que sobra se revoca, lo que falta se otorga. Re-ejecutarlo
# MAGIC sin cambios no ejecuta ninguna sentencia.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección 5 «Estrategia de gobierno y matriz RACI» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Un rol sin GRANT y una clasificación sin máscara son gobierno
# MAGIC > declarativo, no efectivo.
# MAGIC
# MAGIC | Sección | Qué hace | Spec |
# MAGIC |---|---|---|
# MAGIC | Inventario | Foto «antes» de los permisos | FR-006 |
# MAGIC | Roles | `gov_rol`, `gov_rol_usuario`, `gov_politica_mascara`, `fn_autorizado` | FR-001, FR-003 |
# MAGIC | Permisos | `gov_permiso_rol` y reconciliación de GRANT/REVOKE | US1 · FR-001, FR-002, FR-007 |
# MAGIC | RACI | Carga de `matriz_raci` | FR-008 |
# MAGIC | Máscaras | Funciones por tipo y reconciliación desde el tag `clasificacion` | US2, US3 · FR-003, FR-004 |
# MAGIC | Etiquetas | `capa`/`dominio` en schemas, `dominio`/`data_owner` en tablas | US3 · FR-005 |
# MAGIC | Validadores | Permisos, comportamiento, cobertura | SC-001 a SC-003 |
# MAGIC | Linaje | Evidencia de `gld_user_fraud_profile` | US4 · SC-004 |
# MAGIC | Decisiones y DoD | Limitaciones de Free Edition y decisiones provisionales | FR-009 |

# COMMAND ----------

# DBTITLE 1,Parámetros
import time
from pyspark.sql import Row, functions as F

INICIO = time.time()
CAT = "workspace"
GOV = f"{CAT}.yelp_gov"
SCHEMAS = ["yelp_bronze", "yelp_silver", "yelp_gold", "yelp_gov"]
RUTA_ROLES = "/Volumes/workspace/yelp_gov/artefactos/roles_usuarios.csv"   # plantilla: E6_controles_gobierno/artefactos/
SENSIBLES = ("confidencial", "restringido")
YO = spark.sql("SELECT current_user()").first()[0]

dbutils.widgets.dropdown("probar_reclasificacion", "no", ["no", "si"])

sentencias = []   # GRANT/REVOKE/MASK ejecutados en esta corrida (van a la bitácora)

def ejecutar(sql):
    print("→", sql)
    spark.sql(sql)
    sentencias.append(sql)

def cargar(tabla, filas):
    """Overwrite controlado: respeta el DDL (COMMENT, NOT NULL) en vez de recrear la tabla."""
    spark.createDataFrame(filas).select(*spark.table(tabla).columns).write.insertInto(tabla, overwrite=True)

print(f"Ejecuta: {YO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Inventario inicial de permisos

# COMMAND ----------

# DBTITLE 1,Foto «antes» de los permisos sobre el catálogo y los schemas yelp_*
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.gov_inventario_permisos (
  capturado_ts TIMESTAMP NOT NULL COMMENT 'Momento de la captura',
  momento      STRING    NOT NULL COMMENT 'antes | despues de la reconciliación de E6',
  grantee      STRING    NOT NULL COMMENT 'Usuario o grupo que recibe el privilegio',
  objeto       STRING    NOT NULL COMMENT 'Catálogo o schema sobre el que se otorga',
  privilegio   STRING    NOT NULL COMMENT 'Privilegio de Unity Catalog'
) COMMENT 'E6 · Evidencia antes/después de los permisos otorgados (FR-006)'
""")

def capturar_permisos(momento):
    permisos = spark.sql(f"""
      SELECT grantee, concat('{CAT}.', schema_name) AS objeto, replace(privilege_type, '_', ' ') AS privilegio
      FROM {CAT}.information_schema.schema_privileges
      WHERE schema_name LIKE 'yelp%' AND inherited_from = 'NONE'
      UNION ALL
      SELECT grantee, catalog_name, replace(privilege_type, '_', ' ')
      FROM {CAT}.information_schema.catalog_privileges
      WHERE catalog_name = '{CAT}' AND inherited_from = 'NONE'
    """)
    (permisos.select(F.current_timestamp().alias("capturado_ts"), F.lit(momento).alias("momento"), "*")
             .write.insertInto(f"{GOV}.gov_inventario_permisos"))
    return permisos

antes = capturar_permisos("antes")
display(antes.groupBy("grantee", "objeto").agg(F.concat_ws(", ", F.sort_array(F.collect_set("privilegio"))).alias("privilegios"))
             .orderBy("grantee", "objeto").limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Roles como datos

# COMMAND ----------

# DBTITLE 1,gov_rol — catálogo de roles del caso
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.gov_rol (
  rol         STRING NOT NULL COMMENT 'Código del rol (clave)',
  nombre_rol  STRING NOT NULL COMMENT 'Nombre de negocio del rol',
  descripcion STRING          COMMENT 'Responsabilidad según el Libro de Gobierno',
  tipo        STRING NOT NULL COMMENT 'gobierno | dueno_datos | consumo | tecnico'
) COMMENT 'E6 · Roles del caso YelpConnect; en Free Edition reemplazan a los grupos de cuenta'
""")

cargar(f"{GOV}.gov_rol", [
    Row(rol="cdo", nombre_rol="Chief Data Officer", tipo="gobierno",
        descripcion="Accountable del gobierno de datos y de gld_dim_fecha; aprueba políticas y clasificación"),
    Row(rol="steward", nombre_rol="Steward de gobierno", tipo="gobierno",
        descripcion="Mantiene reglas de calidad, glosario, diccionario y controles; ve datos sensibles para auditar"),
    Row(rol="producto", nombre_rol="Producto", tipo="dueno_datos",
        descripcion="Data Owner de Negocio, Usuario y Check-in y sus dimensiones"),
    Row(rol="trust_safety", nombre_rol="Trust & Safety", tipo="dueno_datos",
        descripcion="Data Owner de Reseña, Tip, reglas de fraude, equipos y productos de riesgo"),
    Row(rol="marketing", nombre_rol="Marketing", tipo="consumo",
        descripcion="Consume productos de datos gold"),
    Row(rol="operaciones_comerciales", nombre_rol="Operaciones Comerciales", tipo="consumo",
        descripcion="Consume productos de datos gold"),
    Row(rol="data_science", nombre_rol="Data Science", tipo="consumo",
        descripcion="Explora silver y gold para análisis y modelos"),
    Row(rol="custodio", nombre_rol="Custodio (pipeline)", tipo="tecnico",
        descripcion="Identidad que ejecuta el job y es dueña de las tablas; escribe todas las capas"),
])
print("gov_rol ✔")

# COMMAND ----------

# DBTITLE 1,gov_rol_usuario — quién actúa cada rol (desde el CSV del volumen, fuera de Git)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.gov_rol_usuario (
  usuario       STRING NOT NULL COMMENT 'Correo del usuario en el workspace',
  rol           STRING NOT NULL COMMENT 'Rol que actúa (gov_rol.rol); un usuario puede tener varios roles',
  integrante    STRING          COMMENT 'Integrante del grupo que actúa el rol',
  vigente_desde DATE   NOT NULL COMMENT 'Fecha de asignación'
) COMMENT 'E6 · Asignación usuario ↔ rol; simula los grupos que Free Edition no permite crear'
""")

roles_csv = (spark.read.option("header", True).csv(RUTA_ROLES)
               .select(F.lower(F.trim("usuario")).alias("usuario"), F.trim("rol").alias("rol"),
                       F.trim("integrante").alias("integrante"), F.to_date("vigente_desde").alias("vigente_desde")))
roles_csv.select(*spark.table(f"{GOV}.gov_rol_usuario").columns).write.insertInto(f"{GOV}.gov_rol_usuario", overwrite=True)

asignacion = spark.table(f"{GOV}.gov_rol_usuario")
rol_invalido = asignacion.join(spark.table(f"{GOV}.gov_rol"), "rol", "left_anti").count()
custodios = [r.usuario for r in asignacion.filter("rol = 'custodio'").collect()]
assert rol_invalido == 0, f"❌ {rol_invalido} filas con un rol que no existe en gov_rol"
assert len(custodios) == 1, f"❌ El rol custodio debe tener exactamente 1 usuario, tiene {custodios}"
assert custodios[0] == YO, f"❌ Este notebook lo ejecuta el custodio ({custodios[0]}), no {YO}"
CUSTODIO = custodios[0]
display(asignacion.orderBy("usuario", "rol"))

# COMMAND ----------

# DBTITLE 1,gov_politica_mascara — qué roles ven en claro cada clasificación
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.gov_politica_mascara (
  clasificacion STRING NOT NULL COMMENT 'confidencial | restringido (las demás no se enmascaran)',
  rol           STRING NOT NULL COMMENT 'Rol autorizado a ver en claro (gov_rol.rol)',
  justificacion STRING NOT NULL COMMENT 'Por qué el rol necesita el dato en claro'
) COMMENT 'E6 · Política de enmascaramiento: clasificación → roles que ven en claro'
""")

JUSTIFICACION = {
    "steward": "Audita calidad y controles sobre el dato real",
    "trust_safety": "Data Owner de reseñas, fraude y riesgo: investiga casos con el dato real",
    "custodio": "El pipeline lee silver para calcular gold; enmascarado, gold se degradaría en silencio",
}
cargar(f"{GOV}.gov_politica_mascara",
       [Row(clasificacion=c, rol=r, justificacion=j) for c in SENSIBLES for r, j in JUSTIFICACION.items()])
print("gov_politica_mascara ✔ (decisión provisional D4: revisar al cierre del proyecto)")

# COMMAND ----------

# DBTITLE 1,fn_autorizado — ¿el usuario actual ve en claro esta clasificación?
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_autorizado(clasificacion STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'E6 · TRUE si current_user() tiene un rol autorizado para la clasificación; desconocida o NULL → FALSE'
# MAGIC RETURN EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM workspace.yelp_gov.gov_rol_usuario u
# MAGIC   JOIN workspace.yelp_gov.gov_politica_mascara p ON p.rol = u.rol
# MAGIC   WHERE u.usuario = current_user()
# MAGIC     AND p.clasificacion = fn_autorizado.clasificacion   -- parámetro calificado: sin esto se compara la columna consigo misma
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Prueba de fn_autorizado
r = spark.sql(f"""SELECT {GOV}.fn_autorizado('confidencial') AS conf, {GOV}.fn_autorizado('restringido') AS restr,
                         {GOV}.fn_autorizado('desconocida') AS desconocida, {GOV}.fn_autorizado(NULL) AS nulo""").first()
assert r.conf and r.restr, "❌ El custodio debe estar autorizado en ambas clasificaciones (si no, gold se degrada)"
assert not r.desconocida and not r.nulo, "❌ fn_autorizado debe fallar cerrada ante clasificaciones desconocidas o NULL"
print("✔ fn_autorizado: custodio autorizado, falla cerrada ante clasificación desconocida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Permisos por rol con mínimo privilegio (US1)

# COMMAND ----------

# DBTITLE 1,gov_permiso_rol — matriz rol × schema × privilegio
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.gov_permiso_rol (
  rol           STRING NOT NULL COMMENT 'Rol (gov_rol.rol)',
  objeto        STRING NOT NULL COMMENT 'Schema sobre el que se otorga',
  privilegio    STRING NOT NULL COMMENT 'USE SCHEMA | SELECT | MODIFY | EXECUTE | ALL PRIVILEGES',
  justificacion STRING NOT NULL COMMENT 'Cita de la RACI o del principio de mínimo privilegio'
) COMMENT 'E6 · Permisos esperados por rol; fuente de los GRANT y del validador de permisos'
""")

LECTURA = ["USE SCHEMA", "SELECT"]
MATRIZ = {   # rol → {schema: privilegios}; contrato en specs/004-e6-controles-gobierno/contracts/matriz_permisos.md
    "custodio":                {s: ["ALL PRIVILEGES"] for s in SCHEMAS},
    "steward":                 {"yelp_silver": LECTURA, "yelp_gold": LECTURA, "yelp_gov": LECTURA + ["MODIFY"]},
    "cdo":                     {"yelp_gold": LECTURA, "yelp_gov": LECTURA},
    "trust_safety":            {"yelp_silver": LECTURA, "yelp_gold": LECTURA},
    "data_science":            {"yelp_gold": LECTURA},
    "producto":                {"yelp_gold": LECTURA},
    "marketing":               {"yelp_gold": LECTURA},
    "operaciones_comerciales": {"yelp_gold": LECTURA},
}
POR_QUE = {
    "yelp_bronze": "Bronze es solo del pipeline: ningún consumidor lee dato crudo (FR-002)",
    "yelp_silver": "Silver es zona restringida: tiene datos sensibles en claro, solo la leen roles autorizados a verlos",
    "yelp_gold":   "Gold es la capa de consumo de negocio",
    "yelp_gov":    "Gobierno: lo consultan CDO y Steward; el Steward mantiene reglas y catálogos",
}
cargar(f"{GOV}.gov_permiso_rol",
       [Row(rol=rol, objeto=f"{CAT}.{s}", privilegio=p,
            justificacion="El custodio escribe todas las capas" if rol == "custodio" else POR_QUE[s])
        for rol, schemas in MATRIZ.items() for s, privs in schemas.items() for p in privs])
print("gov_permiso_rol ✔")

# COMMAND ----------

# DBTITLE 1,Reconciliación de GRANT — revoca lo que sobra, otorga lo que falta
def permisos_esperados():
    filas = spark.sql(f"""
      SELECT DISTINCT u.usuario AS grantee, p.objeto, p.privilegio
      FROM {GOV}.gov_rol_usuario u JOIN {GOV}.gov_permiso_rol p ON p.rol = u.rol
    """).collect()
    esperado = {}
    for f in filas:
        esperado.setdefault((f.grantee, f.objeto), set()).add(f.privilegio)
    # Quien tiene ALL PRIVILEGES no necesita los privilegios sueltos (Mateo es steward y custodio)
    return {k: ({"ALL PRIVILEGES"} if "ALL PRIVILEGES" in v else v) for k, v in esperado.items()}

def permisos_otorgados():
    filas = spark.sql(f"""
      SELECT p.grantee, concat('{CAT}.', p.schema_name) AS objeto, replace(p.privilege_type, '_', ' ') AS privilegio
      FROM {CAT}.information_schema.schema_privileges p
      JOIN {CAT}.information_schema.schemata s ON s.schema_name = p.schema_name
      WHERE p.schema_name IN ({", ".join(f"'{s}'" for s in SCHEMAS)})
        AND p.inherited_from = 'NONE'
        AND p.grantee <> s.schema_owner          -- el owner del schema no se gestiona con GRANT
    """).collect()
    otorgado = {}
    for f in filas:
        otorgado.setdefault((f.grantee, f.objeto), set()).add(f.privilegio)
    return otorgado

def diferencias():
    esperado, otorgado = permisos_esperados(), permisos_otorgados()
    sobran = {(g, o, p) for (g, o), ps in otorgado.items() for p in ps - esperado.get((g, o), set())}
    faltan = {(g, o, p) for (g, o), ps in esperado.items() for p in ps - otorgado.get((g, o), set())}
    return sobran, faltan

sobran, faltan = diferencias()
for g, o, p in sorted(sobran):    # primero revocar (cambio de rol: lo anterior se quita antes de dar lo nuevo)
    ejecutar(f"REVOKE {p} ON SCHEMA {o} FROM `{g}`")
for g, o, p in sorted(faltan):
    ejecutar(f"GRANT {p} ON SCHEMA {o} TO `{g}`")
print(f"Reconciliación de permisos: {len(sobran)} REVOKE · {len(faltan)} GRANT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Matriz RACI consultable (FR-008)
# MAGIC `matriz_raci` ya existía (la crea `00_setup`) y E8 la consulta; E6 la llena. Formato ancho, una fila por entidad:
# MAGIC `data_owner` = **A**, `data_steward` = **R** de gobierno, `data_custodian` = **R** técnico, `consumidores` = **I**.

# COMMAND ----------

# DBTITLE 1,Carga de matriz_raci
S, G, V = f"{CAT}.yelp_silver", f"{CAT}.yelp_gold", GOV
ENTIDADES = [  # entidad, dominio (diccionario), Data Owner (constitución IV), tablas
    ("negocio",             "Negocio",               "producto",     [f"{S}.slv_business", f"{G}.gld_dim_negocio"]),
    ("usuario",             "Usuarios",              "producto",     [f"{S}.slv_user", f"{G}.gld_dim_usuario"]),
    ("resena",              "Contenido",             "trust_safety", [f"{S}.slv_review", f"{S}.slv_review_cuarentena", f"{G}.gld_fact_resena"]),
    ("tip",                 "Contenido",             "trust_safety", [f"{S}.slv_tip"]),
    ("checkin",             "Interacciones",         "producto",     [f"{S}.slv_checkin", f"{G}.gld_fact_checkin_diario"]),
    ("regla_fraude",        "Confianza y Seguridad", "trust_safety", [f"{S}.slv_fraud_rule"]),
    ("equipo_trust_safety", "Confianza y Seguridad", "trust_safety", [f"{S}.slv_trust_safety_area"]),
    ("producto_riesgo",     "Confianza y Seguridad", "trust_safety", [f"{G}.gld_user_fraud_profile", f"{G}.gld_business_anomaly_bursts", f"{G}.gld_trust_governance"]),
    ("fecha",               "Tiempo",                "cdo",          [f"{G}.gld_dim_fecha"]),
    ("gobierno_calidad",    "Gobierno de Datos",     "cdo",          [f"{V}.dq_reglas", f"{V}.dq_resultados", f"{V}.glosario_negocio",
                                                                      f"{V}.diccionario_datos", f"{V}.pipeline_bitacora", f"{V}.matriz_raci"]),
]
OWNER_DE_TABLA = {t: owner for _, _, owner, tablas in ENTIDADES for t in tablas}
NIVEL = {"publico": 0, "interno": 1, "confidencial": 2, "restringido": 3}

clase_por_tabla = {}
for r in spark.sql(f"""SELECT concat_ws('.', catalog_name, schema_name, table_name) AS tabla, tag_value
                       FROM {CAT}.information_schema.column_tags WHERE tag_name = 'clasificacion'""").collect():
    if NIVEL.get(r.tag_value, -1) > NIVEL.get(clase_por_tabla.get(r.tabla), -1):
        clase_por_tabla[r.tabla] = r.tag_value

lectores = {}   # schema → roles que lo leen (sin el custodio, que no es consumidor)
for rol, schemas in MATRIZ.items():
    for s, privs in schemas.items():
        if rol != "custodio" and "SELECT" in privs:
            lectores.setdefault(f"{CAT}.{s}", set()).add(rol)

filas_raci = []
for entidad, dominio, owner, tablas in ENTIDADES:
    clases = [clase_por_tabla[t] for t in tablas if t in clase_por_tabla]
    consumidores = set().union(*[lectores.get(t.rsplit(".", 1)[0], set()) for t in tablas])
    filas_raci.append(Row(entidad=entidad, dominio=dominio, data_owner=owner, data_steward="steward",
                          data_custodian="custodio", consumidores=", ".join(sorted(consumidores)),
                          clasificacion=max(clases, key=NIVEL.get) if clases else "interno"))
cargar(f"{GOV}.matriz_raci", filas_raci)

for col, comentario in {
    "dominio":        "Dominio o tema de datos del diccionario",
    "data_owner":     "A · Rol accountable de la entidad (gov_rol.rol)",
    "data_steward":   "R · Rol de gobierno que mantiene calidad y metadata",
    "data_custodian": "R · Rol técnico que opera el pipeline",
    "consumidores":   "I · Roles con lectura sobre las tablas de la entidad (gov_permiso_rol)",
    "clasificacion":  "Clasificación máxima de las columnas de la entidad: publico|interno|confidencial|restringido",
}.items():
    spark.sql(f"ALTER TABLE {GOV}.matriz_raci ALTER COLUMN {col} COMMENT '{comentario}'")
display(spark.table(f"{GOV}.matriz_raci").orderBy("entidad"))

# COMMAND ----------

# DBTITLE 1,Validador de permisos (SC-002) — lo otorgado coincide con la matriz
despues = capturar_permisos("despues")
sobran, faltan = diferencias()
assert not sobran, f"❌ Privilegios otorgados fuera de la matriz: {sorted(sobran)}"
assert not faltan, f"❌ Privilegios de la matriz sin otorgar: {sorted(faltan)}"

en_bronze = {g for (g, o) in permisos_otorgados() if o == f"{CAT}.yelp_bronze"} - {CUSTODIO}
assert not en_bronze, f"❌ Solo el custodio puede tener privilegios en bronze (FR-002): {en_bronze}"

raci = spark.table(f"{GOV}.matriz_raci")
assert raci.count() == len(ENTIDADES), f"❌ matriz_raci debe tener {len(ENTIDADES)} entidades"
assert raci.filter("data_owner IS NULL").count() == 0, "❌ Entidades sin Data Owner"

def resumen(df, momento):
    return df.groupBy("grantee").agg(F.count("*").alias(f"privilegios_{momento}"))
display(resumen(antes, "antes").join(resumen(despues, "despues"), "grantee", "full")
        .fillna(0).orderBy("grantee"))
print("✔ Permisos = matriz rol × schema · bronze solo para el custodio · RACI con 10 entidades y owner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Enmascaramiento derivado de la clasificación (US2, US3)
# MAGIC Una función por tipo de dato (la máscara debe devolver el tipo de la columna). Los números y arreglos se
# MAGIC enmascaran con `NULL`: un número sustituto (p. ej. 0) se confundiría con un dato real al agregar.
# MAGIC
# MAGIC Unity Catalog **no admite máscaras en tablas con restricciones CHECK** (`COLUMN_MASKS_FEATURE_NOT_SUPPORTED`).
# MAGIC Una columna sensible de una tabla con CHECK queda **protegida por acceso**: el notebook exige que todos los
# MAGIC roles que leen su schema estén autorizados a verla en claro, y falla si no.

# COMMAND ----------

# DBTITLE 1,Funciones de máscara
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_mascara_texto(valor STRING, clasificacion STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'E6 · Máscara de texto: en claro si fn_autorizado, si no ****'
# MAGIC RETURN CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor ELSE '****' END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_mascara_nombre(valor STRING, clasificacion STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'E6 · Máscara de nombre de persona: inicial + **** para que la demo muestre que la fila existe'
# MAGIC RETURN CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor ELSE concat(left(valor, 1), '****') END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_mascara_decimal(valor DOUBLE, clasificacion STRING)
# MAGIC RETURNS DOUBLE
# MAGIC COMMENT 'E6 · Máscara numérica decimal: en claro si fn_autorizado, si no NULL'
# MAGIC RETURN CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_mascara_entero(valor INT, clasificacion STRING)
# MAGIC RETURNS INT
# MAGIC COMMENT 'E6 · Máscara numérica entera: en claro si fn_autorizado, si no NULL'
# MAGIC RETURN CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.fn_mascara_arreglo(valor ARRAY<BIGINT>, clasificacion STRING)
# MAGIC RETURNS ARRAY<BIGINT>
# MAGIC COMMENT 'E6 · Máscara de arreglo de enteros: en claro si fn_autorizado, si no NULL'
# MAGIC RETURN CASE WHEN workspace.yelp_gov.fn_autorizado(clasificacion) THEN valor END;

# COMMAND ----------

# DBTITLE 1,Reconciliación de máscaras desde el tag clasificacion
FUNCION_POR_TIPO = {"string": "fn_mascara_texto", "double": "fn_mascara_decimal",
                    "int": "fn_mascara_entero", "array<bigint>": "fn_mascara_arreglo"}
EXCEPCIONES = {("yelp_silver", "slv_user", "nombre"): "fn_mascara_nombre"}

def columnas_sensibles():
    return spark.sql(f"""
      SELECT t.schema_name, t.table_name, t.column_name, t.tag_value AS clasificacion, c.full_data_type
      FROM {CAT}.information_schema.column_tags t
      JOIN {CAT}.information_schema.columns c
        ON c.table_schema = t.schema_name AND c.table_name = t.table_name AND c.column_name = t.column_name
      WHERE t.tag_name = 'clasificacion' AND t.tag_value IN {SENSIBLES}
        AND t.schema_name IN ('yelp_silver', 'yelp_gold')
    """).collect()

def mascaras_actuales():
    return {(r.table_schema, r.table_name, r.column_name): (r.mask_name, r.using_columns)
            for r in spark.sql(f"""SELECT table_schema, table_name, column_name, mask_name, using_columns
                                   FROM {CAT}.information_schema.column_masks
                                   WHERE table_schema IN ('yelp_silver', 'yelp_gold')""").collect()}

def tiene_check(schema, tabla):
    return any(r.key.startswith("delta.constraints.")
               for r in spark.sql(f"SHOW TBLPROPERTIES {CAT}.{schema}.{tabla}").collect())

def lectores_no_autorizados(schema, clasificacion):
    """Roles que leen el schema según gov_permiso_rol y no ven en claro la clasificación."""
    return {r.rol for r in spark.sql(f"""
      SELECT DISTINCT p.rol FROM {GOV}.gov_permiso_rol p
      WHERE p.objeto = '{CAT}.{schema}' AND p.privilegio IN ('SELECT', 'ALL PRIVILEGES')
        AND p.rol NOT IN (SELECT rol FROM {GOV}.gov_politica_mascara WHERE clasificacion = '{clasificacion}')
    """).collect()}

def reconciliar_mascaras():
    deseadas, por_acceso = {}, []
    for c in columnas_sensibles():
        clave = (c.schema_name, c.table_name, c.column_name)
        if tiene_check(c.schema_name, c.table_name):
            expuestos = lectores_no_autorizados(c.schema_name, c.clasificacion)
            if expuestos:
                raise ValueError(f"❌ {'.'.join(clave)} es {c.clasificacion}, su tabla tiene CHECK (no admite máscara) "
                                 f"y la leen roles no autorizados: {sorted(expuestos)}")
            por_acceso.append(clave)
            continue
        funcion = EXCEPCIONES.get(clave) or FUNCION_POR_TIPO.get(c.full_data_type)
        if funcion is None:   # una columna sensible nunca queda sin máscara en silencio
            raise ValueError(f"❌ {'.'.join(clave)} es {c.clasificacion} y su tipo {c.full_data_type} no tiene función de máscara")
        deseadas[clave] = (f"{GOV}.{funcion}", f"'{c.clasificacion}'")
    actuales = mascaras_actuales()
    for clave, (funcion, clase) in sorted(deseadas.items()):
        if actuales.get(clave) != (funcion, clase):
            ejecutar(f"ALTER TABLE {CAT}.{clave[0]}.{clave[1]} ALTER COLUMN {clave[2]} SET MASK {funcion} USING COLUMNS ({clase})")
    for clave in sorted(set(actuales) - set(deseadas)):   # perdió la etiqueta sensible
        ejecutar(f"ALTER TABLE {CAT}.{clave[0]}.{clave[1]} ALTER COLUMN {clave[2]} DROP MASK")
    return set(deseadas), set(por_acceso)

con_mascara, por_acceso = reconciliar_mascaras()
print(f"Columnas sensibles: {len(con_mascara)} con máscara · {len(por_acceso)} protegidas por acceso (tabla con CHECK)")
for clave in sorted(por_acceso):
    print("   🔒", ".".join(clave))

# COMMAND ----------

# DBTITLE 1,Validador de comportamiento (SC-001) — lo que ve el usuario actual
mis_roles = {r.rol for r in spark.table(f"{GOV}.gov_rol_usuario").filter(F.col("usuario") == YO).collect()}
politica = spark.table(f"{GOV}.gov_politica_mascara").collect()
for clase in SENSIBLES:
    esperado = any(p.rol in mis_roles for p in politica if p.clasificacion == clase)
    obtenido = spark.sql(f"SELECT {GOV}.fn_autorizado('{clase}')").first()[0]
    assert obtenido == esperado, f"❌ fn_autorizado('{clase}') = {obtenido}, se esperaba {esperado} para {mis_roles}"
    print(f"✔ {clase}: {'en claro' if obtenido else 'enmascarado'} para {YO} (roles {sorted(mis_roles)})")

display(spark.sql(f"""SELECT user_id, clasificacion_riesgo, score_riesgo_usuario, reglas_activadas
                      FROM {CAT}.yelp_gold.gld_user_fraud_profile WHERE clasificacion_riesgo = 'Alto' LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Etiquetas de schema y tabla (US3)

# COMMAND ----------

# DBTITLE 1,Etiquetas de schema: capa y dominio
CAPA = {"yelp_bronze": "bronze", "yelp_silver": "silver", "yelp_gold": "gold", "yelp_gov": "gobierno"}
for s, capa in CAPA.items():
    spark.sql(f"ALTER SCHEMA {CAT}.{s} SET TAGS ('capa' = '{capa}', 'dominio' = 'yelpconnect')")
print("Tags de schema ✔")

# COMMAND ----------

# DBTITLE 1,Etiquetas de tabla: dominio y data_owner desde el diccionario
OWNER_DICCIONARIO = {"Producto": "producto", "Trust & Safety": "trust_safety", "Gobierno de Datos": "cdo"}

def sin_tildes(texto):
    return texto.lower().translate(str.maketrans("áéíóúñ ", "aeioun_"))

por_tabla = spark.sql(f"""
  SELECT concat_ws('.', split(nombre_fisico, '[.]')[0], split(nombre_fisico, '[.]')[1], split(nombre_fisico, '[.]')[2]) AS tabla,
         collect_set(dominio) AS dominios, collect_set(responsable) AS responsables
  FROM {GOV}.diccionario_datos
  WHERE nombre_fisico LIKE 'workspace.yelp_%'
  GROUP BY 1
""").collect()

inconsistentes = []
for r in por_tabla:
    assert len(r.dominios) == 1 and len(r.responsables) == 1, f"❌ {r.tabla}: dominio u owner ambiguo en el diccionario"
    owner = next((v for k, v in OWNER_DICCIONARIO.items() if k in r.responsables[0]), None)
    assert owner, f"❌ {r.tabla}: responsable no reconocido «{r.responsables[0]}»"
    spark.sql(f"ALTER TABLE {r.tabla} SET TAGS ('dominio' = '{sin_tildes(r.dominios[0])}', 'data_owner' = '{owner}')")
    if OWNER_DE_TABLA.get(r.tabla) != owner:
        inconsistentes.append((r.tabla, owner, OWNER_DE_TABLA.get(r.tabla)))
assert not inconsistentes, f"❌ data_owner del diccionario ≠ matriz_raci (tabla, diccionario, RACI): {inconsistentes}"
print(f"Tags de tabla ✔ en {len(por_tabla)} tablas · data_owner coherente con matriz_raci y la constitución")

# COMMAND ----------

# DBTITLE 1,Validador de cobertura (SC-003) — toda columna sensible tiene máscara o está protegida por acceso
etiquetadas = {(c.schema_name, c.table_name, c.column_name) for c in columnas_sensibles()}
enmascaradas = set(mascaras_actuales())
protegidas = enmascaradas | por_acceso
assert etiquetadas, "❌ No hay columnas sensibles etiquetadas: ¿se ejecutó E4?"
assert not etiquetadas - protegidas, f"❌ Columnas sensibles sin control: {sorted(etiquetadas - protegidas)}"
assert not enmascaradas - etiquetadas, f"❌ Máscaras en columnas no sensibles: {sorted(enmascaradas - etiquetadas)}"
assert not {k for k in por_acceso if k[0] == "yelp_gold"}, "❌ Gold es capa de consumo: sus columnas sensibles deben tener máscara"

en_diccionario = spark.sql(f"""
  SELECT count(*) FROM {GOV}.diccionario_datos
  WHERE translate(lower(split(sensibilidad, ' ')[0]), 'áéíóú', 'aeiou') IN {SENSIBLES}
""").first()[0]
assert len(etiquetadas) == en_diccionario, \
    f"❌ El diccionario tiene {en_diccionario} columnas sensibles y el catálogo {len(etiquetadas)} etiquetadas: re-ejecutar E4"

tags_schema = spark.sql(f"""SELECT schema_name, collect_set(tag_name) AS tags FROM {CAT}.information_schema.schema_tags
                            WHERE schema_name LIKE 'yelp%' GROUP BY 1""").collect()
sin_tags = set(CAPA) - {r.schema_name for r in tags_schema if {"capa", "dominio"} <= set(r.tags)}
assert not sin_tags, f"❌ Schemas sin tags capa/dominio: {sin_tags}"
print(f"✔ Cobertura 100 %: {len(etiquetadas)} columnas sensibles ({en_diccionario} en el diccionario) = "
      f"{len(enmascaradas)} con máscara + {len(por_acceso)} protegidas por acceso")

# COMMAND ----------

# DBTITLE 1,Prueba de reclasificación (solo con probar_reclasificacion = si)
# Demuestra FR-004: reclasificar una columna cambia su control sin editar SQL. Se restaura al terminar.
if dbutils.widgets.get("probar_reclasificacion") == "si":
    tabla, columna = f"{CAT}.yelp_gold.gld_dim_negocio", "nombre"
    clave = ("yelp_gold", "gld_dim_negocio", columna)
    try:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} SET TAGS ('clasificacion' = 'confidencial')")
        reconciliar_mascaras()   # gld_dim_negocio no tiene CHECK: la columna recibe máscara
        assert clave in mascaras_actuales(), "❌ La columna reclasificada no recibió máscara"
        print(f"✔ {tabla}.{columna} → confidencial: máscara aplicada {mascaras_actuales()[clave]}")
    finally:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} SET TAGS ('clasificacion' = 'publico')")
        reconciliar_mascaras()
    assert clave not in mascaras_actuales(), "❌ La máscara no se retiró al restaurar la clasificación"
    print(f"✔ {tabla}.{columna} → publico: máscara retirada; clasificación original restaurada")
else:
    print("Prueba de reclasificación omitida (widget probar_reclasificacion = no)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Linaje de extremo a extremo (US4)
# MAGIC Producto elegido: `gld_user_fraud_profile`, que responde al dolor de Trust & Safety «reglas de fraude sin
# MAGIC linaje claro». Evidencia en Catalog Explorer → `yelp_gold.gld_user_fraud_profile` → *Lineage*:
# MAGIC
# MAGIC ![Linaje de gld_user_fraud_profile](img/linaje_gld_user_fraud_profile.png)
# MAGIC
# MAGIC El salto gold → dashboard se agrega cuando E8 publique el dashboard.

# COMMAND ----------

# DBTITLE 1,Linaje desde las tablas de sistema (si están habilitadas)
CADENA = [f"{G}.gld_user_fraud_profile", f"{S}.slv_user", f"{S}.slv_review", f"{S}.slv_fraud_rule",
          f"{CAT}.yelp_bronze.brz_user", f"{CAT}.yelp_bronze.brz_review", f"{CAT}.yelp_bronze.brz_fraud_rule"]
try:
    display(spark.sql(f"""
      SELECT coalesce(source_table_full_name, source_path) AS origen, target_table_full_name AS destino,
             entity_type, max(event_time) AS ultimo_evento
      FROM system.access.table_lineage
      WHERE target_table_full_name IN ({", ".join(f"'{t}'" for t in CADENA)})
      GROUP BY 1, 2, 3
      ORDER BY destino, origen
      LIMIT 50
    """))
except Exception as e:
    print(f"⚠️ system.access.table_lineage no disponible en este workspace: {str(e)[:150]}")
    print("   La evidencia de linaje es la captura de Catalog Explorer.")

# COMMAND ----------

# DBTITLE 1,Registro en la bitácora
spark.sql(f"""
  INSERT INTO {GOV}.pipeline_bitacora (ejecucion_ts, job_run_id, tarea, capa, tabla, estado, intentos, filas, duracion_seg)
  VALUES (current_timestamp(), 'manual', 'E6_controles', 'gobierno', 'gov_*, matriz_raci, máscaras, grants',
          'SUCCESS', 1, {len(sentencias)}, {round(time.time() - INICIO, 1)})
""")
print(f"✔ E6 completo · {len(sentencias)} sentencias GRANT/REVOKE/MASK en esta corrida (0 = ya estaba reconciliado)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **Roles como datos.** Free Edition no permite grupos de cuenta y los grupos locales del workspace no sirven
# MAGIC   en GRANT de Unity Catalog. Los roles viven en `gov_rol_usuario` y los permisos se otorgan por usuario. Los
# MAGIC   correos no se versionan: se cargan desde `roles_usuarios.csv` en el volumen de artefactos.
# MAGIC - **Máscara nativa con tabla de mapeo.** `SET MASK … USING COLUMNS ('<clasificacion>')` con funciones que
# MAGIC   consultan `gov_rol_usuario` × `gov_politica_mascara` para `current_user()`. Se verificó en el workspace que
# MAGIC   funciona y que la máscara persiste tras `INSERT OVERWRITE` (el pipeline escribe con `insertInto`).
# MAGIC - **La clasificación gobierna.** La reconciliación lee el tag `clasificacion` de E4: reclasificar en el
# MAGIC   diccionario y re-ejecutar E4 y E6 cambia el control sin editar SQL. Las políticas ABAC por tag no se
# MAGIC   asumen disponibles en Free Edition; esta reconciliación las emula.
# MAGIC - **Silver es zona restringida.** Unity Catalog no admite máscaras en tablas con CHECK, y 4 de las 6 tablas
# MAGIC   silver con columnas sensibles los tienen (`slv_fraud_rule`, `slv_review`, `slv_review_cuarentena`,
# MAGIC   `slv_user`). En vez de perder la calidad preventiva de E3, silver solo la leen roles autorizados a ver en
# MAGIC   claro (Steward, Trust & Safety, custodio); CDO y Data Science leen gold. Gold, la capa de consumo, se
# MAGIC   enmascara completa. Decisión del Steward, 2026-10-30.
# MAGIC - **El custodio está autorizado.** Las máscaras aplican a todos, también al owner. Si la cuenta del job no
# MAGIC   viera en claro, gold se calcularía con valores enmascarados sin fallar.
# MAGIC - **Alcance del diccionario.** Las tablas `gov_*` llevan COMMENT completo pero no entran al diccionario de E4,
# MAGIC   que cubre silver y gold.
# MAGIC - **Privilegios a nivel de catálogo.** Se dejan como están (`USE CATALOG` para todos y los privilegios de
# MAGIC   catálogo del custodio); la matriz gobierna los schemas `yelp_*`.
# MAGIC
# MAGIC **Limitaciones de Free Edition y decisiones provisionales** (revisar al cierre del proyecto, FR-009):
# MAGIC
# MAGIC | # | Hoy | Cambio previsto |
# MAGIC |---|---|---|
# MAGIC | D1 | Los integrantes son admins del workspace y el grupo de admins es dueño del catálogo: pueden leer todo aunque se les revoque el GRANT. Las máscaras sí les aplican | Quitar admin a quienes no despliegan para que el acceso denegado sea efectivo |
# MAGIC | D2 | El custodio es una cuenta personal (`mateomirandah`), que también es Steward | Cuenta del grupo como custodio (`run_as` en el bundle y propiedad de las tablas) |
# MAGIC | D3 | Asignación integrante ↔ rol incompleta | Completar `roles_usuarios.csv` con todo el grupo |
# MAGIC | D4 | Ven en claro: steward, trust_safety y custodio | Ratificar con los Data Owners |
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [x] Permisos otorgados = matriz rol × schema, bronze solo para el custodio (SC-002, validador).
# MAGIC - [x] 100 % de columnas `confidencial`/`restringido` con máscara o protegidas por acceso en silver, coherente con el diccionario (SC-003, validador).
# MAGIC - [x] Schemas con tags `capa`/`dominio`; tablas con `dominio`/`data_owner`.
# MAGIC - [x] `matriz_raci` cargada con 10 entidades y su Data Owner (FR-008).
# MAGIC - [x] Re-ejecutable: la segunda corrida no ejecuta sentencias (FR-007).
# MAGIC - [ ] 0 accesos en claro fuera del rol autorizado, verificado con los integrantes (SC-001; pendiente D3).
# MAGIC - [ ] Captura del linaje de extremo a extremo; el salto al dashboard llega con E8 (SC-004).
# MAGIC - [ ] Demo de permisos en vivo en menos de 10 minutos con `02_demo_roles` (SC-005; acceso denegado depende de D1).
# MAGIC - [ ] En `main` vía PR revisado por otro integrante.
