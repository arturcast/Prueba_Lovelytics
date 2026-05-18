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

- **Detección Dinámica de Delimitadores:** Los archivos CSV suelen mezclar delimitadores (ej. `,` vs `;`). Si Databricks intenta leer un archivo separado por `;` utilizando `,` por defecto, arroja el error `[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES]`. Para evitar mapeos manuales por archivo (anti-patrón), se implementó la función `detect_delimiter()` que lee la primera línea del archivo desde el volumen de Unity Catalog e infiere estadísticamente el delimitador a utilizar. Esto permite procesar fuentes con formatos mixtos de forma resiliente y estandarizada.
- **Ingesta:** Se iteran los archivos sobre un diccionario base.
- **Metadatos:** Se agregan las columnas `bronze_ingestion_timestamp` y `source_file_path`.
- **Validaciones:** Se incluye una función de validación que imprime el esquema inferido, el total de registros y alertas de tablas vacías.