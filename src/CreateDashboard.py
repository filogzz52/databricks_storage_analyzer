# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------
dbutils.widgets.text("catalog_schema", "main.default")

# COMMAND ----------
catalog_schema = dbutils.widgets.get("catalog_schema")
# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import LakeviewAPI
from string import Template

w = WorkspaceClient()

# COMMAND ----------

try:
  with open("Data Storage Analysis.lvdash.json.tmpl", "r") as f:
    s = Template(f.read())    
    w.lakeview.create("StorageAnalyzer", serialized_dashboard=s.substitute(catalog_schema=catalog_schema))
    print("Dashboard was created")
except:
  print("Dashboard already exists")

# COMMAND ----------


