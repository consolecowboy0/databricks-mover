# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.widgets.text("source_path", "", "Source (catalog.schema)")
dbutils.widgets.text("dest_path", "", "Destination (catalog.schema)")
dbutils.widgets.text("table_name", "", "Table Name (Optional)")
dbutils.widgets.dropdown("drop_source", "False", ["True", "False"], "Drop Source Tables?")

# COMMAND ----------

import sys
import os

# Ensure we can import the local package
sys.path.append(os.path.abspath('..'))

from databricks_mover.migrator import SchemaMigrator

# COMMAND ----------

source_path = dbutils.widgets.get("source_path")
dest_path = dbutils.widgets.get("dest_path")
table_name = dbutils.widgets.get("table_name")
drop_source_str = dbutils.widgets.get("drop_source")
drop_source = drop_source_str == "True"

if not source_path or not dest_path:
    print("Please provide both Source and Destination paths.")
else:
    migrator = SchemaMigrator(spark, source_path, dest_path)
    
    if table_name:
        print(f"Migrating single table from {source_path} to {dest_path}. Table: {table_name}. Drop Source: {drop_source}")
        migrator.migrate_table(table_name, drop_source=drop_source)
    else:
        print(f"Migrating all tables from {source_path} to {dest_path}. Drop Source: {drop_source}")
        migrator.migrate(drop_source=drop_source)
        
    print("Migration complete.")
