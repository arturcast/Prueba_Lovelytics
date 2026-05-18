# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Gold Zone
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fact ventas final
# MAGIC
# MAGIC Crear la tabla delta **fact_ventas_final**:
# MAGIC
# MAGIC a) Particionada por el campo *mes*.
# MAGIC
# MAGIC b) Incluir el campo *monto_total* resolviendo el cálculo entre la cantidad del producto vendido y su precio unitario.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##2. Corrección de datos
# MAGIC
# MAGIC a) Truncar la partición de diciembre.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC b) Actualizar la tabla descontando el 10% a la columna *monto_total* para el mes de junio.
# MAGIC