# Databricks notebook source
# MAGIC %md
# MAGIC ### Environment Variables
# MAGIC Configuracion de variables de entorno mediante widgets.

# COMMAND ----------

# Widgets
dbutils.widgets.text("env_catalog", "workspace", "1. Catalog Name")
dbutils.widgets.text("env_schema", "bronze", "2. Bronze Schema")
dbutils.widgets.text("env_volume", "default/test_volume", "3. Source Volume (Schema/Volume)")

CATALOG = dbutils.widgets.get("env_catalog")
SCHEMA = dbutils.widgets.get("env_schema")
VOLUME = dbutils.widgets.get("env_volume")

VOLUME_PATH = f"/Volumes/{CATALOG}/{VOLUME}"

assert CATALOG != "", "Catalog vacio"
assert SCHEMA != "", "Schema vacio"

print("Entorno Configurado:")
print(f"- Catalogo: {CATALOG}")
print(f"- Esquema: {SCHEMA}")
print(f"- Volumen: {VOLUME_PATH}")
