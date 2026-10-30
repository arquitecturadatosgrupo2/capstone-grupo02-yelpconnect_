# Databricks notebook source
# MAGIC %md
# MAGIC # E7.4 · Orquestación, monitoreo y FinOps
# MAGIC **Cierra con:** M10 DataOps / MLOps / FinOps · **Tarea del job:** `MONITOREO_bitacora`
# MAGIC
# MAGIC El pipeline deja de ser "notebooks que alguien corre a mano" y se vuelve un
# MAGIC **Lakeflow Job** con dependencias, reintentos y alertas. Es el entregable de
# MAGIC operativización que convierte el capstone en una plataforma, no en un demo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Modelo operativo y FinOps» del
# MAGIC > **Documento Formal de Arquitectura de Datos**.
# MAGIC
# MAGIC ---
# MAGIC ## 1. El Job (definido como código)
# MAGIC El job **no se crea a mano en la UI**: está versionado en `resources/capstone_yelp.job.yml` (Databricks Asset
# MAGIC Bundle) y se despliega con `databricks bundle deploy`. Así la configuración operativa pasa por Pull Request
# MAGIC igual que los notebooks.
# MAGIC ```
# MAGIC BRONZE_yelp ──────┬─► SILVER_business ─┬─► SILVER_review ─┬─► GOLD_user_fraud_profile ─────┐
# MAGIC                   ├─► SILVER_user ─────┘                  ├─► GOLD_business_anomaly_bursts ─┼─► GOLD_trust_governance ─┐
# MAGIC                   ├─► SILVER_checkin ──────────────────────┴─► GOLD_modelo_dimensional ─────┼──────────────────────────┼─► MONITOREO_bitacora
# MAGIC                   └─► SILVER_tip ──────────────────────────────────────────────────────────────────────────────────┘    (corre siempre)
# MAGIC BRONZE_simulados ─► SILVER_trust_safety_area ─► SILVER_fraud_rule ─► (gold de Trust & Safety)
# MAGIC ```
# MAGIC Configuración evaluada:
# MAGIC - [x] 15 tareas con dependencias (no un solo notebook gigante). `DQ_gate` (E5) corre entre silver y gold:
# MAGIC   si falla una regla crítica, gold no se publica.
# MAGIC - [x] Retries: 1 reintento por tarea, 60 s después de la falla, también si la falla es por timeout. El gate no
# MAGIC   se reintenta: con los mismos datos fallaría igual.
# MAGIC - [x] Timeout por tarea (10 a 30 min según el volumen) y de 2 h para la corrida completa: protege la cuota diaria.
# MAGIC - [x] Notificación on-failure por correo a quien despliega el bundle (`${workspace.current_user.userName}`).
# MAGIC - [x] Trigger programado: diario a las 06:00 (America/Lima). Justificación abajo.
# MAGIC
# MAGIC **Por qué schedule diario y no file-arrival:** el origen es una muestra estática que no recibe archivos de forma
# MAGIC continua, así que un trigger por llegada de archivos casi nunca dispararía y no aporta. Además, el Auto Loader
# MAGIC escribe su checkpoint dentro del mismo Volume (`raw/_checkpoints/`), por lo que un file-arrival sobre `raw/`
# MAGIC se dispararía con los propios archivos del pipeline. En el target `dev` del bundle el schedule queda en pausa
# MAGIC automáticamente, para que el desarrollo no consuma cuota sin que nadie lo pida.
# MAGIC
# MAGIC **Falla controlada del gate:** correr el job con el parámetro `forzar_falla_gate = true` agrega a E5 la regla de
# MAGIC prueba DQ-900, que no se puede cumplir. `DQ_gate` falla, las tareas gold quedan en `UPSTREAM_FAILED`, llega la
# MAGIC alerta y la bitácora registra la corrida.
# MAGIC
# MAGIC > ✏️ TODO — Insertar captura del grafo del job y de una corrida exitosa + la corrida de la falla controlada.

# COMMAND ----------

# DBTITLE 1,2. Monitoreo — bitácora de corridas del pipeline
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
# MAGIC   ejecucion_ts     TIMESTAMP COMMENT 'Inicio de la corrida del job',
# MAGIC   job_run_id       STRING    COMMENT 'Identificador de la corrida del job',
# MAGIC   tarea            STRING    COMMENT 'task_key de la tarea del job',
# MAGIC   capa             STRING    COMMENT 'Capa que escribe la tarea (bronze, silver, gold)',
# MAGIC   tabla            STRING    COMMENT 'Tabla o tablas que escribe la tarea',
# MAGIC   estado           STRING    COMMENT 'Resultado final de la tarea (SUCCESS, FAILED, SKIPPED...)',
# MAGIC   intentos         INT       COMMENT 'Cantidad de intentos, incluidos los reintentos automáticos',
# MAGIC   filas            BIGINT    COMMENT 'Filas de la tabla al terminar la corrida',
# MAGIC   duracion_seg     DOUBLE    COMMENT 'Duración del último intento de la tarea'
# MAGIC ) COMMENT 'Observabilidad del pipeline: volumetría, duración, reintentos y resultado por tarea y corrida';

# COMMAND ----------

# DBTITLE 1,Registro de la corrida actual
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row

dbutils.widgets.text("job_run_id", "")
job_run_id = dbutils.widgets.get("job_run_id").strip()

# Tablas que escribe cada tarea del job
TABLAS = {
    "BRONZE_yelp":                  ["yelp_bronze.brz_business", "yelp_bronze.brz_user", "yelp_bronze.brz_review",
                                     "yelp_bronze.brz_tip", "yelp_bronze.brz_checkin"],
    "BRONZE_simulados":             ["yelp_bronze.brz_fraud_rule", "yelp_bronze.brz_trust_safety_area"],
    "SILVER_business":              ["yelp_silver.slv_business"],
    "SILVER_user":                  ["yelp_silver.slv_user"],
    "SILVER_review":                ["yelp_silver.slv_review", "yelp_silver.slv_review_cuarentena"],
    "SILVER_tip":                   ["yelp_silver.slv_tip"],
    "SILVER_checkin":               ["yelp_silver.slv_checkin"],
    "SILVER_trust_safety_area":     ["yelp_silver.slv_trust_safety_area"],
    "SILVER_fraud_rule":            ["yelp_silver.slv_fraud_rule"],
    "DQ_gate":                      ["yelp_gov.dq_resultados"],
    "GOLD_user_fraud_profile":      ["yelp_gold.gld_user_fraud_profile"],
    "GOLD_business_anomaly_bursts": ["yelp_gold.gld_business_anomaly_bursts"],
    "GOLD_modelo_dimensional":      ["yelp_gold.gld_dim_fecha", "yelp_gold.gld_dim_negocio", "yelp_gold.gld_dim_usuario",
                                     "yelp_gold.gld_fact_resena", "yelp_gold.gld_fact_checkin_diario"],
    "GOLD_trust_governance":        ["yelp_gold.gld_trust_governance"],
}

if not job_run_id or not job_run_id.isdigit():
    dbutils.notebook.exit("Sin job_run_id: el registro solo se hace cuando el notebook corre dentro del job.")

run = WorkspaceClient().jobs.get_run(int(job_run_id))
inicio = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc)

# Un mismo task_key aparece una vez por intento: se queda el último y se cuentan los intentos
intentos, ultimo = {}, {}
for t in run.tasks or []:
    if t.task_key not in TABLAS:
        continue
    intentos[t.task_key] = intentos.get(t.task_key, 0) + 1
    if t.task_key not in ultimo or (t.attempt_number or 0) >= (ultimo[t.task_key].attempt_number or 0):
        ultimo[t.task_key] = t

def filas_de(tablas):
    total = 0
    for t in tablas:
        if not spark.catalog.tableExists(f"workspace.{t}"):
            return None
        total += spark.table(f"workspace.{t}").count()
    return total

registros = []
for tarea, t in sorted(ultimo.items()):
    estado = (t.state.result_state.value if t.state and t.state.result_state
              else t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "DESCONOCIDO")
    duracion = (t.end_time - t.start_time) / 1000 if t.start_time and t.end_time else None
    registros.append(Row(
        ejecucion_ts=inicio, job_run_id=job_run_id, tarea=tarea, capa=tarea.split("_")[0].lower(),
        tabla=", ".join(x.split(".")[1] for x in TABLAS[tarea]), estado=estado, intentos=intentos[tarea],
        filas=filas_de(TABLAS[tarea]), duracion_seg=duracion,
    ))

destino = "workspace.yelp_gov.pipeline_bitacora"
(spark.createDataFrame(registros, schema=spark.table(destino).schema)
    .write.mode("append").saveAsTable(destino))

fallidas = [r.tarea for r in registros if r.estado != "SUCCESS"]
reintentadas = [r.tarea for r in registros if r.intentos > 1]
print(f"✔ Corrida {job_run_id}: {len(registros)} tareas registradas · fallidas {fallidas or 'ninguna'} · "
      f"con reintento {reintentadas or 'ninguna'}")

# COMMAND ----------

# DBTITLE 1,Validación automática
corridas = spark.sql(f"SELECT count(DISTINCT job_run_id) c FROM {destino}").first().c
assert spark.sql(f"SELECT count(*) c FROM {destino} WHERE job_run_id = '{job_run_id}'").first().c == len(registros), \
    "❌ La corrida actual no quedó registrada completa en la bitácora"
print(f"✔ Bitácora con {corridas} corrida(s) registrada(s) (la plantilla pide ≥ 3)")

# COMMAND ----------

# DBTITLE 1,3. FinOps en Free Edition
# MAGIC %md
# MAGIC No hay factura, pero SÍ hay cuota — el mejor simulador de FinOps posible.
# MAGIC
# MAGIC **Decisiones de eficiencia tomadas en el proyecto**
# MAGIC | Decisión | Efecto |
# MAGIC |---|---|
# MAGIC | Muestra de una ciudad con topes (400 mil reseñas, 150 mil usuarios) | Raw de ~350 MB en lugar del dataset completo de varios GB; una corrida completa entra en la cuota diaria. Costo: 6,7 % de reseñas con autor fuera de la muestra (ver `slv_review.autor_en_muestra`). |
# MAGIC | Auto Loader con `trigger(availableNow)` para review | Procesa solo archivos nuevos y termina: sin cómputo encendido esperando eventos. Una re-ejecución sin archivos nuevos lee 0 filas. |
# MAGIC | Validaciones con agregados en lugar de `display()` | Los notebooks no muestran tablas grandes en pantalla: cada validación es un `agg` que devuelve una fila. |
# MAGIC | Timeouts por tarea y de corrida, schedule en pausa en `dev` | Una tarea colgada no consume la cuota del día; el desarrollo no dispara corridas programadas. |
# MAGIC | Escritura con `insertInto` sobre tablas existentes | Evita recrear tablas y reescribir metadata en cada corrida. |
# MAGIC
# MAGIC **Estimación de costo enterprise** (celda siguiente): costo mensual = DBU por corrida × precio por DBU ×
# MAGIC 30 corridas (schedule diario). Los DBU salen de `system.billing.usage` si la cuenta tiene acceso; en Free
# MAGIC Edition ese schema requiere que un admin de cuenta lo otorgue, así que la celda cae a un supuesto explícito:
# MAGIC tiempo de cómputo facturable = suma de duraciones de las tareas de la última corrida exitosa en la bitácora
# MAGIC (serverless factura por tarea, no por cluster encendido) × un rango de DBU por hora de una tarea serverless pequeña.
# MAGIC Precio de lista de Jobs Serverless en AWS us-east (octubre 2026): USD 0,35/DBU en Premium y 0,45/DBU en
# MAGIC Enterprise; verificar en la calculadora oficial antes de presupuestar.

# COMMAND ----------

# DBTITLE 1,4. Estimación de costo mensual en un workspace pagado
PRECIO_DBU = {"Premium": 0.35, "Enterprise": 0.45}   # USD por DBU, Jobs Serverless, AWS us-east, lista oct-2026
DBU_HORA_SUPUESTO = (2.0, 6.0)                        # rango supuesto de DBU/hora por tarea serverless pequeña
CORRIDAS_MES = 30

ultima = spark.sql(f"""
  SELECT job_run_id, count(*) AS tareas, sum(duracion_seg) AS seg_computo
  FROM {destino}
  WHERE job_run_id RLIKE '^[0-9]+$'
  GROUP BY job_run_id
  HAVING count_if(estado <> 'SUCCESS') = 0
  ORDER BY max(ejecucion_ts) DESC LIMIT 1
""").first()
horas = ultima.seg_computo / 3600

try:
    dbu_corrida = spark.sql(f"""SELECT sum(usage_quantity) FROM system.billing.usage
                                WHERE usage_metadata.job_run_id = '{ultima.job_run_id}'""").first()[0]
    fuente = "system.billing.usage (DBU reales)"
    rango = (dbu_corrida, dbu_corrida)
except Exception:
    fuente = f"supuesto {DBU_HORA_SUPUESTO[0]:g}–{DBU_HORA_SUPUESTO[1]:g} DBU/h (sin acceso a system.billing)"
    rango = (horas * DBU_HORA_SUPUESTO[0], horas * DBU_HORA_SUPUESTO[1])

print(f"Última corrida exitosa {ultima.job_run_id}: {ultima.tareas} tareas, {horas:.2f} h de cómputo facturable")
print(f"DBU por corrida: {rango[0]:.2f}–{rango[1]:.2f} · fuente: {fuente}")
for nivel, precio in PRECIO_DBU.items():
    print(f"  {nivel:<10} USD {rango[0] * precio * CORRIDAS_MES:,.2f} – {rango[1] * precio * CORRIDAS_MES:,.2f} al mes "
          f"({CORRIDAS_MES} corridas)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Lectura del resultado:** con la muestra de una ciudad, las corridas exitosas suman entre 0,1 y 0,25 horas
# MAGIC de cómputo, así que el pipeline diario cuesta unos pocos dólares al mes en un workspace pagado (con los
# MAGIC supuestos de DBU/hora; el valor real requiere `system.billing`). El costo escala casi linealmente con el volumen: el dataset completo
# MAGIC (varias veces la muestra) y más ciudades multiplican el cómputo de bronze y silver, que es la mayor parte de la
# MAGIC duración. Las palancas de la tabla de arriba (Auto Loader incremental, schedule diario y no continuo,
# MAGIC timeouts) son las que mantienen el costo proporcional a los datos nuevos y no al histórico.
# MAGIC
# MAGIC ### Definition of Done (E7.4)
# MAGIC - [x] Job multi-tarea operativo con retries, timeout y alertas (definido en el bundle). Faltan las capturas.
# MAGIC - [x] Evidencia del gate de calidad deteniendo una corrida: en la bitácora, corrida `710328375413952` con
# MAGIC   `DQ_gate` FAILED y las 4 tareas gold en `UPSTREAM_FAILED`. Falta la captura.
# MAGIC - [x] Bitácora poblada con ≥3 corridas.
# MAGIC - [x] Análisis FinOps con estimación de costo enterprise (con supuesto de DBU/hora hasta tener acceso a `system.billing`).
