# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks notebook source
# MAGIC
# MAGIC # Bronze Zone
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Esquema y tablas
# MAGIC
# MAGIC Crear un esquema llamado **“bronze"** en el catálogo **“workspace”** y crear tablas con los datos de cada archivo definiendo la metadata. 
# MAGIC
# MAGIC Nombrar tablas como:
# MAGIC
# MAGIC * raw_empleados
# MAGIC * raw_locales
# MAGIC * raw_productos
# MAGIC * raw_fact
# MAGIC

# COMMAND ----------

# MAGIC %run ../utils/env_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Ejecucion de Ingesta

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# Crear esquema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

def detect_delimiter(file_path):
    """Lee la primera linea del archivo y determina si usa ',' o ';'."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            return ';' if first_line.count(';') > first_line.count(',') else ','
    except Exception as e:
        print(f"Error detectando delimitador: {e}. Usando ',' por defecto.")
        return ','

def ingest_csv_to_bronze(file_name, table_name):
    """Carga un CSV a una tabla Delta en Bronze con columnas de auditoria."""
    file_path = f"{VOLUME_PATH}/{file_name}"
    
    delimiter = detect_delimiter(file_path)
    print(f"Procesando {file_name} (Delimitador detectado: '{delimiter}')...")
    
    # Lectura CSV
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("delimiter", delimiter)
          .option("inferSchema", "true")
          .load(file_path))
    
    # Columnas de auditoria
    df_bronze = df.withColumn("bronze_ingestion_timestamp", current_timestamp()) \
                  .withColumn("source_file_path", col("_metadata.file_path"))
    
    # Escritura en Delta
    full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
    
    return df_bronze.count()

# Mapeo de archivos a tablas
archivos_tablas = {
    "empleados.csv": "raw_empleados",
    "locales.csv": "raw_locales",
    "producto.csv": "raw_productos",
    "fact.csv": "raw_fact"
}

# Ejecutar ingesta
for file_csv, table_delta in archivos_tablas.items():
    filas = ingest_csv_to_bronze(file_csv, table_delta)
    print(f"{table_delta} ingestada con {filas} registros.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación capa bronze
# MAGIC
# MAGIC Validar con una consulta que se hayan cargado correctamente todos los registros de cada uno de los archivos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Validaciones de Calidad

# COMMAND ----------

def validate_bronze_table(table_name):
    """Imprime esquema, conteo y muestra de la tabla."""
    full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    
    # Conteo
    count_df = spark.sql(f"SELECT COUNT(1) AS total_registros FROM {full_table_name}")
    total = count_df.collect()[0]["total_registros"]
    
    # Esquema
    schema_info = spark.table(full_table_name).schema.simpleString()
    
    print(f"Tabla: {full_table_name}")
    print("-" * 50)
    print(f"Total Registros: {total}")
    print(f"Esquema inferido: {schema_info}")
    
    if total == 0:
        print("ALERTA: Tabla vacia.")
    else:
        print("OK.")
        
    # Muestra
    display(spark.sql(f"SELECT * FROM {full_table_name} LIMIT 3"))
    print("\n")

# Validar todas las tablas
for table in archivos_tablas.values():
    validate_bronze_table(table)
