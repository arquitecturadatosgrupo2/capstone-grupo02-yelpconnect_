# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie)
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El entregable son dos artefactos **definidos como código** en esta carpeta:
# MAGIC 1. **AI/BI Dashboard** `dashboard_yelpconnect.lvdash.json`, desplegado por el bundle
# MAGIC    (`resources/capstone_yelp.dashboard.yml`) con 2 páginas:
# MAGIC    - *Negocio*: REQ-01 a REQ-05 (Trust & Safety, Producto, Marketing, Operaciones Comerciales).
# MAGIC    - *Gobierno*: REQ-06 a REQ-10 para la CDO: calidad (E5), documentación y ownership (E4, E6),
# MAGIC      alertas de las reglas de fraude y operación del pipeline (E7).
# MAGIC 2. **Genie Space** `genie_space.json`, creado o actualizado por `02_genie_space` (REQ-11).
# MAGIC
# MAGIC Este notebook **no repite las consultas**: lee las del JSON del dashboard, las ejecuta y las valida.
# MAGIC Así, lo que se valida es exactamente lo que ve el usuario.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección 7 «Requerimientos de información y KPIs» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Cada visual responde a un requerimiento declarado en el
# MAGIC > documento (REQ-nn); un KPI sin stakeholder que lo pidió no va al tablero.

# COMMAND ----------

# DBTITLE 1,Datasets del dashboard (leídos del JSON versionado)
import json, re

with open("dashboard_yelpconnect.lvdash.json", encoding="utf-8") as f:   # junto a este notebook
    dashboard = json.load(f)

datasets = {d["name"]: "".join(d["queryLines"]) for d in dashboard["datasets"]}
filas = {}
for nombre, sql in datasets.items():
    filas[nombre] = spark.sql(f"SELECT count(*) FROM ({sql})").first()[0]
    print(f"{nombre:<22} {filas[nombre]:>8,} filas")
assert all(n > 0 for n in filas.values()), f"❌ Datasets vacíos: {[k for k, v in filas.items() if v == 0]}"

# COMMAND ----------

# DBTITLE 1,Validador: solo gold y gobierno (FR-005 · SC-003)
tablas = {t for sql in datasets.values() for t in re.findall(r"workspace\.(\w+)\.\w+", sql)}
assert tablas <= {"yelp_gold", "yelp_gov"}, f"❌ El dashboard lee capas no publicadas: {tablas}"

with open("genie_space.json", encoding="utf-8") as f:
    genie = json.load(f)["serialized_space"]
fuera = [t["identifier"] for t in genie["data_sources"]["tables"]
         if not t["identifier"].startswith(("workspace.yelp_gold.", "workspace.yelp_gov."))]
assert not fuera, f"❌ Genie lee tablas fuera de gold/gobierno: {fuera}"
print(f"✔ Dashboard y Genie consumen solo {sorted(tablas)}")

# COMMAND ----------

# DBTITLE 1,Validador: cada visual traza a un requerimiento (FR-002 · SC-001 · SC-005)
visuales = [(p["displayName"], w["widget"]["spec"]["frame"])
            for p in dashboard["pages"] for w in p["layout"]
            if "spec" in w["widget"] and not w["widget"]["spec"]["widgetType"].startswith("filter")]
huerfanos = [f["title"] for _, f in visuales if not re.match(r"REQ-\d{2} · ", f.get("description", ""))]
assert not huerfanos, f"❌ Visuales sin requerimiento: {huerfanos}"
assert len(visuales) >= 6, "❌ Se piden ≥ 6 visuales"
reqs = sorted({re.match(r"(REQ-\d{2})", f["description"]).group(1) for _, f in visuales})
print(f"✔ {len(visuales)} visuales en {len(dashboard['pages'])} páginas, todos trazados · requerimientos cubiertos: {reqs}")

# COMMAND ----------

# DBTITLE 1,Validador: los KPIs cuadran con la tabla de origen
G, V = "workspace.yelp_gold", "workspace.yelp_gov"

def total(dataset, expr):
    return spark.sql(f"SELECT {expr} FROM ({datasets[dataset]})").first()[0]

chequeos = {
    "Ráfagas sospechosas (REQ-01)":
        (total("ds_rafagas", "count(*)"),
         spark.sql(f"SELECT count_if(es_rafaga_sospechosa) FROM {G}.gld_business_anomaly_bursts").first()[0]),
    "Usuarios por nivel de riesgo (REQ-02)":
        (total("ds_riesgo_nivel", "sum(usuarios)"), spark.table(f"{G}.gld_user_fraud_profile").count()),
    "Reseñas (REQ-03)":
        (total("ds_resenas", "sum(resenas)"), spark.table(f"{G}.gld_fact_resena").count()),
    "Check-ins válidos (REQ-05)":
        (total("ds_checkins", "sum(checkins)"),
         spark.sql(f"SELECT sum(n_checkins) FROM {G}.gld_fact_checkin_diario").first()[0]),
    "Reglas de calidad en la última ejecución (REQ-06)":
        (total("ds_calidad_ultima", "count(*)"),
         spark.sql(f"SELECT count(*) FROM {V}.dq_reglas WHERE regla_id <> 'DQ-900'").first()[0]),
    "Entidades de la RACI (REQ-08)":
        (total("ds_cobertura", "max(entidades)"), spark.table(f"{V}.matriz_raci").count()),
    "Reglas de fraude (REQ-09)":
        (total("ds_alertas", "count(*)"), spark.table(f"{G}.gld_trust_governance").count()),
}
for kpi, (dashboard_valor, origen) in chequeos.items():
    assert dashboard_valor == origen, f"❌ {kpi}: dashboard {dashboard_valor} ≠ origen {origen}"
    print(f"✔ {kpi}: {dashboard_valor:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de implementación
# MAGIC - **Dashboard y Genie como código.** El dashboard se despliega con `databricks bundle deploy`, igual que el job,
# MAGIC   y Genie se crea o actualiza con `02_genie_space` desde su JSON. Un cambio hecho en la UI se pierde en el
# MAGIC   siguiente despliegue: los cambios se hacen en el JSON y entran por Pull Request.
# MAGIC - **Sin credenciales embebidas** (`embed_credentials: false`). Cada espectador ve según sus permisos y las
# MAGIC   máscaras de E6: Trust & Safety ve en claro el nivel de riesgo y las reglas de fraude; un rol de consumo ve
# MAGIC   `****`. La página de Gobierno lee `yelp_gov`, que solo leen Steward, CDO y custodio.
# MAGIC - **Datasets pre-agregados** al grano de cada visual (mes × categoría, mes × día de la semana × categoría). Las
# MAGIC   medidas se guardan como sumas y los promedios se calculan en el widget, para que el filtro por categoría no
# MAGIC   promedie promedios.
# MAGIC - **Rankings por categoría**: las 15 categorías principales con más reseñas (hay 727).
# MAGIC - **Filtro de ciudad**: la muestra es de Philadelphia (14.575 negocios en PA y 2 en NJ); el filtro existe
# MAGIC   porque el modelo admite más ciudades.
# MAGIC - La regla **DQ-900** (prueba controlada del gate de E5) se excluye de la página de Gobierno.
# MAGIC - **Refresco**: el dashboard lee las tablas en vivo, así que muestra lo que el job publicó en su última corrida.
# MAGIC   Una programación de refresco solo calienta la caché. Si se agrega una, va a las 07:00 America/Lima, una hora
# MAGIC   después del job, y queda en pausa en `dev` como el schedule del job.
# MAGIC - **Corrección de la plantilla**: la consulta de cobertura usaba `descripcion_columna`, que no existe en el
# MAGIC   diccionario; la columna es `descripcion_negocio` (FR-011).
# MAGIC
# MAGIC ### Definition of Done (E8)
# MAGIC - [x] Dashboard con 2 páginas, 18 visuales, filtros de mes, ciudad y categoría y textos de contexto.
# MAGIC - [x] Cada visual nombra el requerimiento (REQ) y el stakeholder al que responde (validador).
# MAGIC - [x] Dashboard publicado consumiendo solo gold y gobierno, sin credenciales embebidas (validador).
# MAGIC - [x] Genie Space sobre gold y gobierno con sinónimos del glosario y las 5 preguntas de prueba respondidas (ver `02_genie_space`).
# MAGIC - [ ] Revisión visual del dashboard en la UI y capturas de cada página.
# MAGIC - [ ] Storytelling: una slide resumen por página con la decisión que habilita.
