# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("config_path", "/Workspace/Shared/search_config.yml")
dbutils.widgets.text("catalog_schema", "main.default")
dbutils.widgets.text("num_requests", "10")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")
catalog_schema = dbutils.widgets.get("catalog_schema")
num_requests = int(dbutils.widgets.get("num_requests"))
output_table = f"{catalog_schema}.tables_size"
print("Output table:", output_table)

# COMMAND ----------

import yaml
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from pyspark.sql.functions import current_timestamp    


with open(config_path) as f:
    config = yaml.safe_load(f.read())

print("Config loaded")    
pprint(config)

# COMMAND ----------

def ret_table_set(table):
        set_table = set()
        set_table.add(table)
        return set_table
    
def unfold_catalogs(table):
    if table[0] == '*':
        return {(catalog["catalog"], table[1], table[2]) for catalog in spark.sql("show catalogs").collect()}
    else:        
        return ret_table_set(table)

def unfold_schemas(table):
    
    if table[1] == '*':
        try:
            print(f"show schemas in {table[0]}")
            return {(table[0], schema["databaseName"], table[2]) for schema in spark.sql(f"show schemas in {table[0]}").collect()}
        except:
            return ret_table_set(table)
    else:
        return ret_table_set(table)


def unfold_tables(table):
    if table[2] == '*':
        try:
            print(f"show tables in {table[0]}.{table[1]}")
            return {(table[0], table[1], table_name["tableName"]) for table_name in spark.sql(f"show tables in {table[0]}.{table[1]}").collect()}
        except:
            return ret_table_set(table)
    else:
        return ret_table_set(table)

def unfold(tables):
    unfolded_catalogs = set()
    unfolded_schemas = set()
    unfolded_tables = set()

    for table in tables:        
        unfolded_catalogs = unfolded_catalogs.union(unfold_catalogs(table))    
    # print("unfolded_catalogs", unfolded_catalogs)
    
    for table in unfolded_catalogs:
        unfolded_schemas = unfolded_schemas.union(unfold_schemas(table))
    # print("unfolded_schemas", unfolded_schemas)

    for table in unfolded_schemas:
        unfolded_tables = unfolded_tables.union(unfold_tables(table))        
    # print("unfolded_tables", unfolded_tables)        

    return unfolded_tables


def collect_tables(search, exclusions):
    search = [tuple(table.split(".")) for table in search]
    exclusions = [tuple(table.split(".")) for table in exclusions]
    
    unfolded_search = unfold(search)   
    unfolded_exclusions = unfold(exclusions)
    tables = unfolded_search.difference(unfolded_exclusions)
    print("Tables_final", tables)
    return tables
    
tables = collect_tables(config["search"], config["exclusions"])


# COMMAND ----------

print("Number of tables to search:", len(tables))

# COMMAND ----------

def get_table_size(table):
    table = ".".join(table)
    try:
        df = spark.sql(f"describe detail {table}")    
        return df.collect()[0], df.schema
    except:
        print("Error procesing this can be a view:", table) 
        return "Error", None

    

rows = []
with ThreadPoolExecutor(max_workers=num_requests) as executor:    
    rows = list(executor.map(get_table_size, tables))


rows = list(filter(lambda row: row[1] is not None, rows))

if len(rows) == 0:
    print("No information for the provided tables was found")
    dbutils.notebook.exit(0)

schema = rows[0][1]    
rows = [row[0] for row in rows]    

# COMMAND ----------

df_describe = spark.createDataFrame(rows, schema).withColumn("timestamp", current_timestamp())
df_describe.display()

# COMMAND ----------

def table_exists(table):
    table = table.split(".")
    query = f"SHOW TABLES IN {table[0]}.{table[1]} LIKE '{table[0]}'"
    return spark.sql(query).count() > 0
    

# COMMAND ----------

if table_exists(output_table):
    print("Appending")
    df_describe.write.mode("append").saveAsTable(output_table)
else:
    print("New Table")
    df_describe.write.mode("overwrite").saveAsTable(output_table)