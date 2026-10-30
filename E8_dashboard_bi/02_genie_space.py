# Databricks notebook source
# MAGIC %md
# MAGIC # E8.2 · Genie Space como código
# MAGIC **Cierra con:** M9 BI y Visualización · **Requerimiento:** REQ-11 (Mei Lin, Data Science)
# MAGIC
# MAGIC El espacio se define en `genie_space.json`, versionado junto a este notebook: tablas de gold y de
# MAGIC gobierno, instrucciones con los sinónimos del glosario, SQL de ejemplo y las 5 preguntas de prueba
# MAGIC como *benchmarks*. Este notebook crea el espacio o, si ya existe uno con el mismo título, lo
# MAGIC actualiza. Re-ejecutarlo no duplica espacios.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección 7 «Requerimientos de información y KPIs»
# MAGIC > del **Documento Formal de Arquitectura de Datos** (REQ-11 y preguntas de prueba G1–G5).

# COMMAND ----------

# DBTITLE 1,Crear o actualizar el espacio
import json
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "656cbc31df3fbcc2"
w = WorkspaceClient()

with open("genie_space.json", encoding="utf-8") as f:   # junto a este notebook
    definicion = json.load(f)

cuerpo = {"title": definicion["title"], "description": definicion["description"],
          "serialized_space": json.dumps(definicion["serialized_space"], ensure_ascii=False)}

existentes = [s for s in w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
              if s.get("title") == definicion["title"]]
if existentes:
    space_id = existentes[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=cuerpo)
    print(f"✔ Espacio actualizado: {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**cuerpo, "warehouse_id": WAREHOUSE_ID})["space_id"]
    print(f"✔ Espacio creado: {space_id}")

# COMMAND ----------

# DBTITLE 1,Validador: solo gold y gobierno, ≥ 5 sinónimos, 5 preguntas de prueba
espacio = json.loads(w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}",
                                     query={"include_serialized_space": "true"})["serialized_space"])
tablas = [t["identifier"] for t in espacio["data_sources"]["tables"]]
fuera = [t for t in tablas if not t.startswith(("workspace.yelp_gold.", "workspace.yelp_gov."))]
assert not fuera, f"❌ Tablas fuera de gold/gobierno (FR-005): {fuera}"

texto = "".join(l for i in espacio["instructions"]["text_instructions"] for l in i["content"])
sinonimos = texto.count("→ ") // 2
assert sinonimos >= 5, f"❌ Se esperaban ≥ 5 sinónimos del glosario en las instrucciones (FR-006), hay {sinonimos}"
assert len(espacio["benchmarks"]["questions"]) == 5, "❌ Deben existir las 5 preguntas de prueba G1–G5"
print(f"✔ {len(tablas)} tablas de gold/gobierno · {sinonimos} sinónimos del glosario · 5 preguntas de prueba")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pruebas de las 5 preguntas (SC-002) · 2026-10-30
# MAGIC Cada pregunta se planteó al espacio con la API de conversaciones de Genie. El resultado de la consulta que
# MAGIC generó Genie se comparó con el SQL de referencia del benchmark (mismas claves y mismos valores; se admiten
# MAGIC columnas extra y orden distinto entre empates).
# MAGIC
# MAGIC | # | Pregunta (sinónimo del glosario) | Interpretación de Genie | Resultado |
# MAGIC |---|---|---|---|
# MAGIC | G1 | ¿Qué negocios tuvieron más **picos de reseñas** sospechosos en 2021? | Ráfaga de Reseñas: Philadelphia Flower Show (3), Dalessandro's (2) y 11 negocios con 1 | ✔ 13 de 13 negocios, 0 diferencias |
# MAGIC | G2 | ¿Cuál fue el **conteo oficial de check-ins** por categoría en diciembre de 2021? | Check-in Válido: Restaurants 562, Food 276, Nightlife 116… | ✔ |
# MAGIC | G3 | ¿Cuál fue el **rating** promedio de las reseñas por mes en 2021? | Calificación: de 3,96 (enero) a 3,66 (agosto) | ✔ |
# MAGIC | G4 | ¿Qué **reglas antifraude** marcaron más casos y cuáles tienen una **alerta de ownership**? | Regla 1 (1.226 casos); alerta solo en la regla 9 (equipo inactivo) | ✔ |
# MAGIC | G5 | ¿Qué **reglas DQ** quedaron bajo su umbral en la última ejecución? | DQ-005, consistencia (excluye DQ-900) | ✔ |
# MAGIC
# MAGIC **5 de 5 correctas.** Preguntas de control fuera del benchmark, también correctas: «¿cuántos usuarios
# MAGIC sospechosos hay?» → 1.260 (Alto + Moderado, aplica la definición del glosario); «¿qué local tuvo más visitas
# MAGIC en 2019?» → Philadelphia International Airport, 3.874 check-ins; «¿qué porcentaje de las reseñas de 2021 tiene
# MAGIC calificación extrema?» → 69,65 %.
# MAGIC
# MAGIC ### Iteración documentada (FR-008)
# MAGIC | Paso | Qué pasó |
# MAGIC |---|---|
# MAGIC | Pregunta | «¿Qué negocios tuvieron ráfagas sospechosas de reseñas **el último mes**?» (ejemplo de la spec) |
# MAGIC | Falla | Genie tomó el último mes de la muestra (enero de 2022) y respondió que no hubo ráfagas. Enero de 2022 está incompleto: los datos llegan al día 19. Para el negocio, «el último mes» es el último mes completo. |
# MAGIC | Corrección | Se agregó a las instrucciones de `genie_space.json` una regla de fechas relativas: el último mes completo es diciembre de 2021, «este año» es 2021, nunca usar la fecha del sistema. Se actualizó el espacio con este notebook. |
# MAGIC | Resultado | Genie responde «en el último mes completo con datos (diciembre de 2021)…» y devuelve el negocio correcto (Cilantro New Mediterranean Cuisine, ráfaga del 30/12, 4 reseñas de 5 estrellas). |
