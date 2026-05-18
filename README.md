# Prueba Técnica Lovelytics - Databricks

Solucion al DataChallenge de Databricks.

## Configuracion

Se utilizan Databricks Widgets (`utils/env_setup.py`) para parametrizar catalogo y esquemas, permitiendo cambiar entre entornos sin modificar codigo.

### Estructura
- `notebooks/`: Notebooks del flujo (01 a 04).
- `utils/`: Modulos compartidos.

### Ramas
Cada paso se desarrollo en su propia rama (`feat/01-bronze-ingestion`, `feat/02-silver-transformation`, etc.).

---

## Bronze (`01_bronze_ingestion.py`)

- Deteccion automatica de delimitador (`,` o `;`) leyendo la primera linea del archivo.
- Carga de 4 CSVs a tablas Delta con columnas de auditoria.
- Validacion de esquema y conteo por tabla.

## Silver (`02_silver_transformation.py`)

- Validacion de integridad referencial con `left_anti` joins.
- Creacion de `dim_vendedor` (cruce empleados + locales) y `dim_producto`.
- Creacion de `fact_ventas` con INNER JOIN y separacion de fecha en `dia`, `mes`, `ano`.

## Gold (`03_gold_aggregations.py`)

- Calculo de `monto_total = cantidad * precio_unitario`.
- Tabla `fact_ventas_final` particionada por `mes`.
- DELETE de la particion de diciembre y UPDATE con descuento del 10% en junio.

## Analytics (`04_analytics.py`)

- Top-10 sucursales, ranking de vendedores con top 3 productos, sucursales con monto < $4M.
- Delta Time Travel: historial de versiones y restauracion a version 0.