# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Gold Zone
# MAGIC  

# COMMAND ----------

# MAGIC %run ../utils/env_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fact ventas final
# MAGIC
# MAGIC Crear la tabla delta **fact_ventas_final**:
# MAGIC
# MAGIC a) Particionada por el campo *mes*.
# MAGIC
# MAGIC b) Incluir el campo *monto_total* resolviendo el cálculo entre la cantidad del producto vendido y su precio unitario.

# COMMAND ----------

from pyspark.sql.functions import col

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# Leer tablas de Silver
df_fact_ventas = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_ventas")
df_dim_producto = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_producto")

# Cruzar para obtener precio_unitario y calcular monto_total
# Usamos alias para evitar colisiones
df_fact_ventas_final = df_fact_ventas.alias("f").join(
    df_dim_producto.alias("p"),
    col("f.sku") == col("p.id_producto"),
    "inner"
).withColumn(
    "monto_total", col("f.cantidad") * col("p.precio_unitario")
)

# Seleccionar columnas necesarias. Se mantiene toda la fact mas el monto_total
# Usamos f.* para traer todas las de fact_ventas y el nuevo monto_total
df_fact_ventas_final = df_fact_ventas_final.select("f.*", "monto_total")

# Escribir la tabla particionada por 'mes'
df_fact_ventas_final.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("mes") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_ventas_final")

print("fact_ventas_final guardada en Gold y particionada por mes.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Corrección de datos
# MAGIC
# MAGIC a) Truncar la partición de diciembre.

# COMMAND ----------

# Ejecutar DML sobre Delta Lake
# La forma nativa y robusta de vaciar una particion en Delta es usar DELETE
spark.sql(f"DELETE FROM {CATALOG}.{GOLD_SCHEMA}.fact_ventas_final WHERE mes = 12")
print("Particion de diciembre truncada (mes = 12).")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC b) Actualizar la tabla descontando el 10% a la columna *monto_total* para el mes de junio.

# COMMAND ----------

# Ejecutar DML de actualizacion sobre Delta Lake
spark.sql(f"UPDATE {CATALOG}.{GOLD_SCHEMA}.fact_ventas_final SET monto_total = monto_total * 0.9 WHERE mes = 6")
print("Descuento del 10% aplicado a la particion de junio (mes = 6).")

# COMMAND ----------