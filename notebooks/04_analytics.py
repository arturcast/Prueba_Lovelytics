# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics y algo más

# COMMAND ----------

# MAGIC %run ../utils/env_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Consultas analíticas
# MAGIC Resolver las siguientes consultas analíticas

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Mejores sucursales
# MAGIC
# MAGIC Top-10 de sucursales según monto vendido ordenado de mayor a menor. Debe tener ambas columnas.

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window

df_fact = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_ventas_final")
df_vendedor = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_vendedor")
df_producto = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_producto")

# Top-10 sucursales por monto vendido
df_top_sucursales = df_fact.alias("f").join(
    df_vendedor.alias("v"),
    col("f.vendedor") == col("v.Id_vendedor"),
    "inner"
).groupBy("sucursal_nombre") \
 .agg(_sum("monto_total").alias("monto_vendido")) \
 .orderBy(col("monto_vendido").desc()) \
 .limit(10)

df_top_sucursales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Mejores vendedores con detalle de top 3 de productos
# MAGIC
# MAGIC - Listado ordenado de mayor a menor con los mejores vendedores por "monto_total".
# MAGIC
# MAGIC - Agregar los 3 productos más vendidos por cada uno con sus cantidades correspondientes.

# COMMAND ----------

# Ranking de vendedores
df_vendedores_rank = df_fact.alias("f").join(
    df_vendedor.alias("v"),
    col("f.vendedor") == col("v.Id_vendedor"),
    "inner"
).groupBy("Id_vendedor", "vendedor_nombre") \
 .agg(_sum("monto_total").alias("monto_total_vendedor")) \
 .orderBy(col("monto_total_vendedor").desc())

df_vendedores_rank.display()

# COMMAND ----------

# Top 3 productos por vendedor
df_detalle = df_fact.alias("f").join(
    df_producto.alias("p"),
    col("f.sku") == col("p.id_producto"),
    "inner"
).groupBy("vendedor", col("p.nombre").alias("producto")) \
 .agg(_sum("cantidad").alias("cantidad_total"))

w = Window.partitionBy("vendedor").orderBy(col("cantidad_total").desc())
df_top3_productos = df_detalle.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") <= 3) \
    .drop("rn") \
    .orderBy("vendedor", col("cantidad_total").desc())

df_top3_productos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Peores sucursales
# MAGIC
# MAGIC Peores sucursales: identificar las sucursales con monto vendido menor a $4.000.000.

# COMMAND ----------

df_peores_sucursales = df_fact.alias("f").join(
    df_vendedor.alias("v"),
    col("f.vendedor") == col("v.Id_vendedor"),
    "inner"
).groupBy("sucursal_nombre") \
 .agg(_sum("monto_total").alias("monto_vendido")) \
 .filter(col("monto_vendido") < 4000000) \
 .orderBy(col("monto_vendido").asc())

df_peores_sucursales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta time travel
# MAGIC
# MAGIC ### a. Consultar el historial de versiones de la tabla

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {CATALOG}.{GOLD_SCHEMA}.fact_ventas_final").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### b. Restaurar la tabla a la versión original (0)

# COMMAND ----------

spark.sql(f"RESTORE TABLE {CATALOG}.{GOLD_SCHEMA}.fact_ventas_final TO VERSION AS OF 0")
print("Tabla restaurada a la version 0.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contestá la pregunta
# MAGIC Una fecha de entrega de una tarea que tenés asignada se está acercando y, salvo un milagro, no vas a llegar: ¿Qué hacés?

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta:**
# MAGIC
# MAGIC Comunico el riesgo lo antes posible al lider y al equipo, explico las causas del retraso y propongo alternativas: entregar un alcance reducido en la fecha pactada o negociar una nueva fecha con el alcance completo. Lo peor que se puede hacer es quedarse callado hasta el ultimo dia.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cuál es el área de la siguiente figura:
# MAGIC ![](/Volumes/workspace/default/test_volume/figura.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta:**
# MAGIC
# MAGIC - El rectangulo central mide 6 x 3 = 18 u²
# MAGIC - Triangulo superior mide (5 x 1)/2 = 2.5 u²
# MAGIC - Triangulo inferior mide (6 x 2)/2 = 6 u²
# MAGIC - Tirangulo izquiero mide (2 x 3)/2 = 3 u²
# MAGIC
# MAGIC Area = 18 + 2.5 + 6 + 3 = **29.5 u²**
# MAGIC
# MAGIC Investigando se podría aplicar la fórmula del área de Gauss (Shoelace formula) en python.
# MAGIC
# MAGIC 1. Habría que identificar los vertices, la forma sencilla sería identificar los vertices y escribirlos directamente o por medio de una librería de procesamiento de imagenes encontrar la forma de identificarlos automaticamente. Es importante que los vertices deben ir en orden en sentido horario
# MAGIC 2. Los vertices son (2,2),(7,1),(7,2),(8,2),(8,5),(2,7),(2,5),(0,5)
