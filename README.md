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

- **Ingesta:** Se iteran los archivos sobre un diccionario mapeando origen y destino.
- **Metadatos:** Se agregan las columnas `bronze_ingestion_timestamp` y `source_file_path`.
- **Validaciones:** Se incluye una función de validación que imprime el esquema inferido, el total de registros y alertas de tablas vacías.