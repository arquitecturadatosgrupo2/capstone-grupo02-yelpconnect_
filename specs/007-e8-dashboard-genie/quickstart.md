# Quickstart · Validar E8

## Prerrequisitos

- Job con al menos una corrida exitosa (gold publicado) y E6 aplicado (máscaras, permisos).
- CLI de la extensión de VS Code, perfil `FREE`, warehouse `656cbc31df3fbcc2`.

## 1 · Consultas y validadores

Ejecutar `E8_dashboard_bi/01_consultas_base_dashboard`. Cada dataset del dashboard tiene su consulta y
un validador que la cruza con una consulta directa a la tabla de origen (US1, test independiente).

## 2 · Desplegar el dashboard

`databricks bundle deploy -t dev` despliega el job y el dashboard
(`resources/capstone_yelp.dashboard.yml`). Esperado: dashboard con 2 páginas y 17 visuales, publicado
sin credenciales embebidas ([contracts/visuales.md](contracts/visuales.md)).

## 3 · Crear o actualizar Genie y probarlo

Ejecutar `E8_dashboard_bi/02_genie_space` (crea o actualiza el espacio desde `genie_space.json`) y luego
las 5 preguntas G1–G5 con `genie ask`. Esperado: 5 de 5 coinciden con el SQL de referencia (SC-002); al
menos una iteración documentada (FR-008).

## 4 · Revisión con roles

- Con Steward o Trust & Safety: la página de Negocio muestra niveles de riesgo y reglas en claro.
- Con un rol de consumo: esas columnas se ven `****`/NULL y la página de Gobierno no carga (sin acceso a
  `yelp_gov`). Con los integrantes como admins (E6, D1) esta prueba solo se ve con un usuario no admin.
