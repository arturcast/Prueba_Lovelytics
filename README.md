# Prueba Técnica Lovelytics - Databricks

Este repositorio contiene la solución al DataChallenge de Databricks.

## Arquitectura y Configuración

### 1. Parametrización y Entornos Dinámicos
El código utiliza el catálogo `workspace` y el esquema `bronze`. Para soportar múltiples entornos, se implementaron Databricks Widgets en el script `utils/env_setup.py`. Esto permite inyectar el catálogo y esquema de forma dinámica.

### 2. Estructura del Repositorio
- `notebooks/`: Contiene los notebooks principales del flujo.
- `utils/`: Módulos compartidos, como `env_setup.py`.

### 3. Estrategia de Ramas
Cada desarrollo se realiza en su propia rama (ej. `feat/01-bronze-ingestion`) antes de integrarse.

---

## Desarrollo: Capa Bronze (`01_bronze_ingestion.py`)

- **Deteccion Dinamica de Delimitadores:** Se usa la funcion `detect_delimiter()` para inferir el delimitador (`,` o `;`) leyendo la primera linea del archivo. Esto evita errores como `[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES]`.
- **Ingesta:** Iteracion de archivos mediante diccionario.
- **Metadatos:** Columnas de auditoria `bronze_ingestion_timestamp` y `source_file_path`.
- **Validaciones:** Impresion del esquema inferido y total de registros.

## Desarrollo: Capa Silver (`02_silver_transformation.py`)

- **Validacion de Datos:** Se identifican registros huerfanos usando joins `left_anti` para validar la integridad referencial antes del cruce final.
- **Dimensiones:** Creacion de `dim_vendedor` cruzando empleados y locales, y `dim_producto`.
- **Manejo de Ambigüedad:** En cruces complejos, se asignaron alias a los DataFrames (`f`, `p`, `v`) para evitar el error `[AMBIGUOUS_REFERENCE]` al operar sobre columnas compartidas.
- **Tabla de Hechos:** Creacion de `fact_ventas` aplicando un `INNER JOIN` con las dimensiones para descartar transacciones sin referencias validas, garantizando la integridad. Parseo del campo `timestamp` en `dia`, `mes`, `ano`.