# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Zone

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validación de datos
# MAGIC
# MAGIC Validar la integridad referencial de los datos.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../utils/env_setup

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, to_date

# Cargar tablas Bronze
df_raw_empleados = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_empleados")
df_raw_locales = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_locales")
df_raw_productos = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_productos")
df_raw_fact = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_fact")

# Validacion de integridad referencial: Contar registros sin dimension (huerfanos)
orphans_empleados = df_raw_empleados.join(df_raw_locales, df_raw_empleados["sucursal"] == df_raw_locales["id_sucursal"], "left_anti")
print(f"Empleados con sucursal inexistente: {orphans_empleados.count()}")

orphans_ventas_prod = df_raw_fact.join(df_raw_productos, df_raw_fact["SKU"] == df_raw_productos["id_producto"], "left_anti")
print(f"Ventas con SKU inexistente: {orphans_ventas_prod.count()}")

orphans_ventas_vend = df_raw_fact.join(df_raw_empleados, df_raw_fact["vendedor"] == df_raw_empleados["id_empleado"], "left_anti")
print(f"Ventas con vendedor inexistente: {orphans_ventas_vend.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Esquema y tablas
# MAGIC
# MAGIC Crear un esquema llamado **"silver"** en el catálogo **"workspace"** y crear dos tablas delta llamadas:
# MAGIC
# MAGIC
# MAGIC   a) **dim_vendedor** - Que unifique la información de vendedor con la de locales. La tabla debe tener las columnas:
# MAGIC
# MAGIC
# MAGIC        i. Id_vendedor
# MAGIC
# MAGIC        ii. vendedor_nombre
# MAGIC
# MAGIC        iii. sucursal_nombre
# MAGIC
# MAGIC        iv. region
# MAGIC
# MAGIC   b) **dim_producto** - respetando la estructura la tabla equivalente (raw_productos).
# MAGIC

# COMMAND ----------

# Crear esquema Silver
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# a) dim_vendedor
df_dim_vendedor = df_raw_empleados.join(
    df_raw_locales,
    df_raw_empleados["sucursal"] == df_raw_locales["id_sucursal"],
    "inner"
).select(
    df_raw_empleados["id_empleado"].alias("Id_vendedor"),
    df_raw_empleados["nombre"].alias("vendedor_nombre"),
    df_raw_locales["nombre"].alias("sucursal_nombre"),
    df_raw_locales["tipo"].alias("region") 
)

df_dim_vendedor.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_vendedor")
print("dim_vendedor guardada en Silver.")

# b) dim_producto
df_dim_producto = df_raw_productos
df_dim_producto.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_producto")
print("dim_producto guardada en Silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Tabla de hechos
# MAGIC
# MAGIC Crear una tabla delta llamada **fact_ventas** asegurando: 
# MAGIC
# MAGIC a) Integridad referencial entre la Fact y las dimensiones.
# MAGIC
# MAGIC b) Separar la fecha en los campos: dia, mes, ano

# COMMAND ----------

# Asegurar integridad referencial con INNER JOIN
df_fact_ventas = df_raw_fact.join(
    df_dim_producto,
    df_raw_fact["SKU"] == df_dim_producto["id_producto"],
    "inner"
).join(
    df_dim_vendedor,
    df_raw_fact["vendedor"] == df_dim_vendedor["Id_vendedor"],
    "inner"
)

# Parseo de fecha
df_fact_ventas = df_fact_ventas.withColumn("fecha_dt", to_date(col("fecha"))) \
    .withColumn("dia", dayofmonth(col("fecha_dt"))) \
    .withColumn("mes", month(col("fecha_dt"))) \
    .withColumn("ano", year(col("fecha_dt"))) \
    .drop("fecha_dt")

# Seleccionar campos requeridos de la fact table original mas las fechas separadas
cols_fact = [c for c in df_raw_fact.columns] + ["dia", "mes", "ano"]
df_fact_ventas_final = df_fact_ventas.select(*cols_fact)

df_fact_ventas_final.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.fact_ventas")
print("fact_ventas guardada en Silver con integridad referencial.")