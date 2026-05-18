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

# MAGIC %md
# MAGIC
# MAGIC ## 3. Tabla de hechos
# MAGIC
# MAGIC Crear una tabla delta llamada **fact_ventas** asegurando: 
# MAGIC
# MAGIC a) Integridad referencial entre la Fact y las dimensiones.
# MAGIC
# MAGIC b) Separar la fecha en los campos: dia, mes, ano