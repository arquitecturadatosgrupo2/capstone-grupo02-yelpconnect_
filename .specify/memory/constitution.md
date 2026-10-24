<!--
Sync Impact Report
- Version change: (plantilla sin completar) → 1.0.0
- Principios definidos (primera adopción; el template traía 5 slots, se usan 7):
  I. El documento define, la plataforma demuestra
  II. Convenciones del Lakehouse
  III. Calidad como datos y gate obligatorio (NO NEGOCIABLE)
  IV. Gobierno por diseño
  V. Reproducibilidad e idempotencia
  VI. Frugalidad en Free Edition (FinOps)
  VII. Operación observable
- Secciones añadidas: "Restricciones de plataforma y datos", "Flujo de trabajo y revisión"
- Secciones eliminadas: ninguna
- Templates de .specify/templates/: no se modifican (leen la constitución en tiempo de ejecución);
  el "Constitution Check" de plan-template.md debe evaluarse contra los principios I–VII.
- TODOs diferidos: ninguno. RATIFICATION_DATE = fecha de primera adopción (2026-10-24).
-->

# Constitución de la Plataforma de Datos YelpConnect (Capstone UTEC · Grupo 02 BANSEG)

## Core Principles

### I. El documento define, la plataforma demuestra

- El Documento Formal de Arquitectura de Datos es la fuente del QUÉ y el POR QUÉ; este
  repositorio es la evidencia ejecutable de QUE SE CUMPLE. Ningún notebook rehace el diseño.
- Todo notebook de entregable (E3–E8, EX) DEBE abrir con un bloque **Trazabilidad** que nombre
  la sección del Documento Formal que implementa.
- La implementación NO DEBE contradecir el documento. Si la realidad obliga a cambiar el diseño,
  se actualiza la sección del documento en el mismo ciclo y se registra la decisión (qué cambió,
  por qué, quién la aprobó) en el markdown del notebook.

**Razón:** la evaluación del Capstone mide coherencia entre diseño y ejecución; un desfase entre
ambos soportes invalida la evidencia.

### II. Convenciones del Lakehouse

- Catálogo `workspace`; schemas `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov`. No se
  crean schemas fuera de esta lista sin enmienda.
- Prefijos obligatorios: `brz_` (bronze), `slv_` (silver), `gld_` (gold), `vw_` (vistas);
  reglas de calidad `DQ-nnn`, correlativas y nunca reutilizadas.
- Los datos crudos viven solo en `/Volumes/workspace/yelp_bronze/raw/`; los artefactos de
  gobierno en `/Volumes/workspace/yelp_gov/artefactos/`.
- El flujo es estrictamente bronze → silver → gold: gold NO DEBE leer bronze ni datos crudos.
- El DDL de silver y gold en `E3_modelo_fisico` es la fuente de verdad del esquema físico; los
  notebooks de pipeline escriben sobre esas tablas, no las redefinen.
- Comentarios, metadata y nombres de negocio en español, de forma consistente.

**Razón:** las convenciones se evalúan explícitamente y son lo que permite a cualquier integrante
(o al profesor) ubicar un objeto sin leer código.

### III. Calidad como datos y gate obligatorio (NO NEGOCIABLE)

- Las reglas de calidad viven como filas en `yelp_gov.dq_reglas` y sus resultados en
  `yelp_gov.dq_resultados`; NO se hardcodean validaciones de negocio dispersas en los pipelines.
- Cada regla DEBE declarar dimensión, criticidad, umbral, justificación del umbral, acción si
  falla y Data Owner.
- El `DQ_gate` corre entre silver y gold. Una regla de criticidad **alta** bajo su umbral DEBE
  fallar la tarea e impedir la publicación de gold; las de criticidad **media** se registran y
  reportan sin detener.
- Cambiar el umbral de una regla requiere aprobación del Data Owner correspondiente.
- Los parámetros de negocio de gold (p. ej. reglas de fraude en `slv_fraud_rule`) se gobiernan
  como catálogo parametrizado, no como constantes en código.

**Razón:** publicar un producto de datos de Trust & Safety con datos inválidos es peor que no
publicarlo; las reglas como datos son auditables y alimentan el scorecard de E8.

### IV. Gobierno por diseño

- Toda tabla y columna de silver y gold DEBE tener `COMMENT` y figurar en el diccionario de datos;
  todo término de negocio usado en gold DEBE existir en el glosario.
- Glosario y diccionario se completan en las plantillas institucionales de
  `E4_diccionario_glosario/plantillas/` y se cargan sin traducir columnas; el Excel anexado al
  Documento Formal y el cargado en `yelp_gov` son el mismo archivo.
- Toda entidad tiene un Data Owner asignado: Producto → Negocio, Usuario, Check-in y sus dims;
  Trust & Safety → Reseña, Tip, fraude, equipos y productos de riesgo; CDO → `gld_dim_fecha` y
  gobierno. Un cambio de ownership se refleja a la vez en E3, E5, glosario, diccionario y RACI.
- La clasificación del diccionario se aplica como tag `clasificacion` en Unity Catalog con
  valores `publico | interno | confidencial | restringido`, y esos tags DEBEN gobernar los
  permisos y el enmascaramiento de E6.
- Toda regla citada como `DQ-nnn` en un artefacto DEBE existir en `yelp_gov.dq_reglas`.

**Razón:** el gobierno que solo existe en un documento no se puede demostrar; aquí se exige que
cada definición tenga un efecto técnico verificable.

### V. Reproducibilidad e idempotencia

- Clonar el repo y ejecutar los notebooks en el orden documentado en el README DEBE producir el
  mismo resultado, sin pasos manuales no documentados.
- Todo notebook DEBE ser re-ejecutable sin duplicar datos ni fallar por objetos existentes
  (`CREATE ... IF NOT EXISTS`, `MERGE`/overwrite controlado, checkpoints de Auto Loader).
- Datos semilla o simulados se versionan en `datos/semillas/` y se declaran como simulados.
- Las decisiones de aceptar un riesgo de datos (p. ej. autores fuera de la muestra) se documentan
  con su impacto cuantificado en el notebook que las implementa.

**Razón:** la entrega se valida ejecutándola; lo que no es reproducible no cuenta como evidencia.

### VI. Frugalidad en Free Edition (FinOps)

- Se trabaja con la muestra de una ciudad (< 300 MB en crudo), nunca con el dataset completo.
- Prohibido `display()` o `collect()` de tablas completas; se muestran agregados o `LIMIT`.
- Solo cómputo serverless; los schedules del job permanecen pausados en desarrollo.
- Toda nueva carga o tarea del job DEBE estimar su impacto en la cuota diaria antes de mergearse.

**Razón:** agotar la cuota detiene a los cinco integrantes hasta el día siguiente.

### VII. Operación observable

- El pipeline se orquesta como Job definido en el bundle (`databricks.yml` / `resources/`), con
  reintentos, timeouts y alertas por correo en cada tarea.
- Cada corrida DEBE registrarse en `yelp_gov.pipeline_bitacora` (tarea, estado, filas, duración).
- Las tablas bronze conservan columnas de auditoría de ingesta (origen y timestamp de carga).
- Las alertas de gobierno de gold (p. ej. `gld_trust_governance`) se tratan como incidentes a
  resolver o aceptar explícitamente, no como ruido.

**Razón:** el módulo DataOps evalúa evidencia de operación real (corridas, fallas, alertas), no
solo código que podría correr.

## Restricciones de plataforma y datos

- Plataforma única: Databricks Free Edition (un workspace, un metastore, cuenta del grupo, no
  personal). Sin clusters clásicos, sin servicios externos de pago.
- Fuente: Yelp Open Dataset muestreado con `datos/preparar_muestra_yelp.py`, ejecutado en local;
  los archivos crudos no se versionan en Git.
- El despliegue del bundle se hace con un CLI de Databricks compatible con el estado del bundle;
  ninguna credencial, token o perfil se versiona en el repositorio.
- Datos personales de usuarios de Yelp (nombres, identificadores) se tratan según su
  clasificación en el diccionario; los de nivel `confidencial` o `restringido` NO DEBEN
  exponerse sin máscara en gold ni en el dashboard de E8.

## Flujo de trabajo y revisión

- `main` está protegida: todo cambio entra por Pull Request desde una rama
  `feature/E<k>-<tema>` y con revisión de al menos otro integrante.
- Un entregable está terminado (Definition of Done) solo si: corre de inicio a fin sin errores y
  sus validadores pasan; sus objetos existen en el schema correcto con comentarios y tags; su
  cabecera de Trazabilidad es correcta y no contradice el documento; sus decisiones y supuestos
  están en el markdown; y está en `main` vía PR revisado.
- Cada PR describe qué entregable toca, qué validadores ejecutó en el workspace y su resultado.
- Ramas experimentales que no cumplan esta constitución no se mergean a `main`.

## Governance

- Esta constitución prevalece sobre prácticas individuales y guía los "Constitution Check" de
  los planes de Spec Kit. Toda especificación, plan y PR DEBE verificar cumplimiento con los
  principios I–VII; una excepción se justifica por escrito en el plan (sección de complejidad) y
  se aprueba en la revisión del PR.
- Enmiendas: se proponen por PR que modifique este archivo, con la razón del cambio; requieren
  aprobación del Steward de gobierno y de al menos otro integrante. Si la enmienda altera
  ownership, clasificación o reglas de calidad, se actualiza en el mismo ciclo el Documento
  Formal (Libro de Gobierno / RACI).
- Versionado semántico: MAJOR al eliminar o redefinir un principio de forma incompatible; MINOR
  al añadir un principio o sección o ampliar materialmente una guía; PATCH para aclaraciones y
  redacción.
- Revisión de cumplimiento: antes de cada entrega parcial del Capstone se recorre el checklist
  de principios contra `main` y se registran las brechas como tareas.
- Guía operativa de ejecución: `README.md` del repositorio.

**Version**: 1.0.0 | **Ratified**: 2026-10-24 | **Last Amended**: 2026-10-24
