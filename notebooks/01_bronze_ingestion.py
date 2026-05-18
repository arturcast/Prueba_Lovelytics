# Databricks notebook source
# MAGIC %md
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

# MAGIC %md
# MAGIC
# MAGIC ## 2. Validación capa bronze
# MAGIC
# MAGIC Validar con una consulta que se hayan cargado correctamente todos los registros de cada uno de los archivos.