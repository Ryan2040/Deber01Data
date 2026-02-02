Proyecto 01 — Data Mining

Ryan de la Torre 00326806

Backfill histórico desde QuickBooks Online hacia Postgres usando Mage

Descripción
En este proyecto se construyó un pipeline de backfill histórico que extrae información desde la API de QuickBooks Online (QBO) para las entidades:
- Customers
- Invoices
- Items

Los datos se almacenan en Postgres dentro del esquema raw, guardando el payload completo en JSONB y metadatos de ingesta.
La orquestación se realizó con Mage y el despliegue con Docker Compose.
Todos los secretos se gestionan mediante Mage Secrets.

Arquitectura
QuickBooks API → Mage Pipelines → Postgres (raw)
                         ↓
                    Triggers one-time

Servicios en Docker:
- Mage
- PostgreSQL
- PgAdmin

Levantar el proyecto
docker compose up -d

Gestión de secretos (Mage Secrets)
QBO:
- QBO_CLIENT_ID
- QBO_CLIENT_SECRET
- QBO_REFRESH_TOKEN
- QBO_REALM_ID
- QBO_ENV

Postgres:
- PG_HOST
- PG_PORT
- PG_DB
- PG_USER
- PG_PASSWORD

Pipelines implementados
- qb_customers_backfill
- qb_invoices_backfill
- qb_items_backfill

Cada pipeline recibe fecha_inicio y fecha_fin en UTC desde un trigger one-time, realiza chunking por día, usa OAuth 2.0, maneja paginación y rate limits, y realiza UPSERT en tablas raw.

Trigger one-time
Para cada pipeline se configuró un trigger tipo Once con variables:
- fecha_inicio (ISO UTC)
- fecha_fin (ISO UTC)

Esquema RAW en Postgres
Tablas creadas:
- raw.qb_customers
- raw.qb_invoices
- raw.qb_items

Cada tabla contiene:
id (PK), payload (JSONB), ingested_at_utc, extract_window_start_utc, extract_window_end_utc,
page_number, page_size, request_payload.

Idempotencia
Se ejecutó el mismo rango dos veces y no se generaron duplicados gracias al ON CONFLICT DO UPDATE.

Validaciones y volumetría
SELECT extract_window_start_utc, COUNT(*) FROM raw.qb_items GROUP BY 1 ORDER BY 1;
SELECT COUNT(*), COUNT(DISTINCT id) FROM raw.qb_items;

Runbook
Si un tramo falla, se reejecuta el pipeline desde el último extract_window_end_utc exitoso.

Troubleshooting
- Error OAuth: revisar Secrets
- Error de fechas: revisar variables del Trigger
- Error de conexión: revisar red Docker y PG_HOST
- Rate limit: el loader tiene reintentos con backoff

Checklist
Mage y Postgres en la misma red
Secretos en Mage Secrets
Pipelines parametrizados
Triggers one-time ejecutados
Tablas raw con metadatos
Idempotencia verificada

