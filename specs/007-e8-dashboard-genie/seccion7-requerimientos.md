# Sección 7 · Requerimientos de información y KPIs — BORRADOR

**Estado**: aprobado por el Steward de gobierno (Mateo Miranda) el 2026-10-30; pendiente de validación del grupo. Fuente para la sección 7 del Documento
Formal y para trazar cada visual de E8.

**Base**: dolores y expectativas del mapa de stakeholders (Lienzo E1), caso de uso **Trust & Safety**
(motivadores MOT-01 cuentas válidas, MOT-02 reseñas válidas, MOT-03 automatización de patrones de
fraude), y los productos gold y de gobierno que ya existen en la plataforma. Solo se piden preguntas
que gold o gobierno pueden responder hoy: un requerimiento sin dato publicado se lista aparte como
brecha.

Datos de la muestra: Philadelphia, reseñas de 2005 a enero de 2022.

## 7.1 Preguntas de negocio por stakeholder

| ID | Pregunta de negocio | Stakeholder | Decisión que habilita | Frecuencia | Motivador | Página · fuente |
|---|---|---|---|---|---|---|
| REQ-01 | ¿Cuántas ráfagas sospechosas de reseñas hubo cada mes y en qué negocios se concentran? | Rafael Nakamura, Trust & Safety | Priorizar qué negocios se escalan al comité de integridad | Mensual | MOT-03 | Negocio · `gld_business_anomaly_bursts`, `gld_dim_negocio` |
| REQ-02 | ¿Cuántos usuarios hay en cada nivel de riesgo y qué reglas de fraude los marcan? | Rafael Nakamura, Trust & Safety | Decidir qué cuentas se revisan manualmente y cuáles se suspenden | Semanal | MOT-01 | Negocio · `gld_user_fraud_profile` (columnas confidenciales: en claro solo para Trust & Safety) |
| REQ-03 | ¿Cómo evolucionan el volumen de reseñas y la calificación promedio por mes y categoría? | Andrew Collins, Producto | Elegir qué categorías priorizar en mejoras de experiencia | Mensual | MOT-02 | Negocio · `gld_fact_resena`, `gld_dim_fecha`, `gld_dim_negocio` |
| REQ-04 | ¿Qué porcentaje de las reseñas de cada categoría tiene calificación extrema (1 o 5 estrellas) y cuántos votos útiles reciben? | Jessica Martinez, Marketing & Growth | Elegir qué categorías y reseñas usar como prueba social en campañas sin apoyarse en reseñas polarizadas | Mensual | MOT-02 | Negocio · `gld_fact_resena`, `gld_dim_negocio` |
| REQ-05 | ¿Cuántos check-ins válidos tiene cada categoría por día y cómo varían según el día de la semana? | Carla Romero, Operaciones Comerciales | Fijar precio y franjas de los anuncios de Yelp Ads con un conteo único de visitas | Semanal | MOT-01 (CE-01 del caso: una sola cifra de check-ins) | Negocio · `gld_fact_checkin_diario`, `gld_dim_fecha`, `gld_dim_negocio` |
| REQ-06 | ¿Qué reglas de calidad están en rojo en la última ejecución, de qué dimensión son y qué Data Owner responde? | Laura Hernández, CDO | Asignar la corrección al Data Owner y decidir si se acepta el riesgo | Diaria | — (gobierno) | Gobierno · `dq_resultados`, `dq_reglas` |
| REQ-07 | ¿Cómo evoluciona el cumplimiento de calidad por dimensión entre ejecuciones? | Laura Hernández, CDO | Ver si la calidad mejora o se degrada antes de cada comité | Por ejecución | — (gobierno) | Gobierno · `dq_resultados`, `dq_reglas` |
| REQ-08 | ¿Qué porcentaje de las columnas está documentado y qué porcentaje de las entidades tiene Data Owner? | Laura Hernández, CDO | Medir el avance del gobierno y cerrar huecos de documentación | Mensual | — (gobierno) | Gobierno · `diccionario_datos`, `matriz_raci` |
| REQ-09 | ¿Qué alertas de gobierno de las reglas de fraude están abiertas y cuántos casos marca cada regla? | Laura Hernández, CDO, y Rafael Nakamura, Trust & Safety | Reasignar reglas activas a equipos activos y revisar reglas que no marcan casos | Semanal | MOT-03 | Gobierno · `gld_trust_governance` (descripción y umbrales restringidos: en claro solo para Trust & Safety) |
| REQ-10 | ¿El pipeline corrió, cuánto duró y qué tareas fallaron o se reintentaron? | Laura Hernández, CDO | Saber si los indicadores del día son confiables antes de usarlos | Diaria | — (operación) | Gobierno · `pipeline_bitacora` |
| REQ-11 | ¿Puedo consultar en lenguaje natural los productos de datos usando el vocabulario del negocio? | Mei Lin, Data Science | Explorar los datos certificados sin pedir extracciones ad hoc | A demanda | — | Genie Space sobre gold y gobierno |

## 7.2 Definición de KPIs

| KPI | Fórmula en lenguaje de negocio | Términos del glosario | REQ |
|---|---|---|---|
| Ráfagas sospechosas del mes | Cantidad de días-negocio del mes marcados como ráfaga sospechosa de reseñas | Ráfaga de Reseñas, Negocio, Señal de Fraude | REQ-01 |
| Negocios con ráfaga | Negocios distintos con al menos una ráfaga sospechosa en el periodo | Ráfaga de Reseñas, Negocio | REQ-01 |
| Usuarios por nivel de riesgo | Usuarios en cada clasificación de riesgo (Alto, Moderado, Bajo) según su puntaje | Usuario Sospechoso, Puntaje de Riesgo | REQ-02 |
| Casos marcados por regla | Usuarios o negocios que activan cada regla de fraude activa | Regla de Fraude, Señal de Fraude | REQ-02, REQ-09 |
| Reseñas del mes | Reseñas publicadas en el mes | Reseña | REQ-03 |
| Calificación promedio | Promedio de estrellas de las reseñas del periodo | Calificación, Reseña | REQ-03 |
| % de calificaciones extremas | Reseñas con 1 o 5 estrellas sobre el total de reseñas del periodo | Calificación, Desviación de Calificación | REQ-04 |
| Votos útiles por reseña | Votos útiles recibidos sobre el número de reseñas | Voto Útil, Reseña | REQ-04 |
| Check-ins válidos diarios | Check-ins válidos por negocio y día, sumados por categoría | Check-in Válido, Negocio | REQ-05 |
| Cumplimiento de calidad | Porcentaje de registros que cumple cada regla de calidad; una regla está en rojo si queda bajo su umbral | Regla de Calidad de Datos, Data Owner | REQ-06, REQ-07 |
| Cobertura de documentación | Columnas de silver y gold con descripción de negocio sobre el total de columnas | Data Steward | REQ-08 |
| Cobertura de ownership | Entidades con Data Owner asignado sobre el total de entidades | Data Owner | REQ-08 |
| Alertas de gobierno abiertas | Reglas de fraude activas con una alerta de gobierno sin resolver | Alerta de Gobierno, Regla de Fraude | REQ-09 |
| Corridas exitosas | Corridas del pipeline sin tareas fallidas sobre el total de corridas | *(sin término: falta «Corrida del Pipeline» en el glosario)* | REQ-10 |

## Brechas detectadas (no se piden visuales para ellas)

- **Reseñas en cuarentena / duplicadas para Marketing**: el dato está en `slv_review_cuarentena`
  (silver, zona restringida por E6). Para mostrarlo en el dashboard haría falta un producto gold
  agregado (p. ej. reseñas en cuarentena por mes y motivo). Queda como mejora.
- **Reseña Verificada / Reseña Sospechosa a nivel de reseña**: el glosario las define, pero gold no
  marca reseñas individuales (el riesgo se calcula por usuario y por negocio-día).
- **Término «Corrida del Pipeline»** falta en el glosario para el KPI de REQ-10.

## Preguntas de prueba para Genie (REQ-11, 5 de 5)

| # | Pregunta (con sinónimo del glosario) | Término oficial | Fuente |
|---|---|---|---|
| G1 | ¿Qué negocios tuvieron más picos de reseñas sospechosos en 2021? | Ráfaga de Reseñas | `gld_business_anomaly_bursts` + `gld_dim_negocio` |
| G2 | ¿Cuál fue el conteo oficial de check-ins por categoría en diciembre de 2021? | Check-in Válido | `gld_fact_checkin_diario` + dims |
| G3 | ¿Cuál fue el rating promedio de las reseñas por mes en 2021? | Calificación | `gld_fact_resena` + `gld_dim_fecha` |
| G4 | ¿Qué reglas antifraude marcaron más casos y cuáles tienen una alerta de ownership? | Regla de Fraude, Alerta de Gobierno | `gld_trust_governance` |
| G5 | ¿Qué reglas DQ quedaron bajo su umbral en la última ejecución y de qué dimensión son? | Regla de Calidad de Datos | `dq_resultados` + `dq_reglas` |
