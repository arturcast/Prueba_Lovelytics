# Databricks notebook source
# MAGIC %md
# MAGIC ### Environment Variables
# MAGIC Configuracion de variables de entorno mediante widgets.

# COMMAND ----------

# Widgets
dbutils.widgets.text("env_catalog", "workspace", "1. Catalog Name")
dbutils.widgets.text("env_bronze", "bronze", "2. Bronze Schema")
dbutils.widgets.text("env_silver", "silver", "3. Silver Schema")
dbutils.widgets.text("env_gold", "gold", "4. Gold Schema")
dbutils.widgets.text("env_volume", "default/test_volume", "5. Source Volume (Schema/Volume)")

CATALOG = dbutils.widgets.get("env_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("env_bronze")
SILVER_SCHEMA = dbutils.widgets.get("env_silver")
GOLD_SCHEMA = dbutils.widgets.get("env_gold")
VOLUME = dbutils.widgets.get("env_volume")

VOLUME_PATH = f"/Volumes/{CATALOG}/{VOLUME}"

assert CATALOG != "", "Catalog vacio"
assert BRONZE_SCHEMA != "", "Bronze Schema vacio"
assert SILVER_SCHEMA != "", "Silver Schema vacio"
assert GOLD_SCHEMA != "", "Gold Schema vacio"

print("Entorno Configurado:")
print(f"- Catalogo: {CATALOG}")
print(f"- Esquemas: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
print(f"- Volumen: {VOLUME_PATH}")
