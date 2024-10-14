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

def sql_filter(row):    
    return tuple(['' if col == '*' else col for col in row])

def search_tables(search_filter):
    search_query = f"""select table_catalog, table_schema, table_name 
            from system.information_schema.tables 
            where table_catalog ilike '%{search_filter[0]}%' 
            and table_schema ilike '%{search_filter[1]}%' 
            and table_name ilike '%{search_filter[2]}%'
            and table_type = 'MANAGED'"""

    print(search_query)            
    df = spark.sql(search_query)
    
    if df.count() == 0:
        return set()
    # TODO
    # This can all be done in DF and only brought to memory in the end
    ret = set([(row[0], row[1], row[2]) for row in df.collect()])
    
    return ret


def collect_tables(search, exclusions):
    search = [tuple(table.split(".")) for table in search]
    exclusions = [tuple(table.split(".")) for table in exclusions]
    
    search_filters = {sql_filter(row) for row in search}
    
    unfolded_search = set()
    for search_filter in search_filters:
        unfolded_search = unfolded_search.union(search_tables(search_filter))
    
    # print("Tables_unfolded", unfolded_search)

    exclusion_filters = {sql_filter(row) for row in exclusions}
    unfolded_exclusions = set()
    for exclusion_filter in exclusion_filters:
        unfolded_exclusions = unfolded_exclusions.union(search_tables(exclusion_filter))
    
    # print("Exclusions", unfolded_exclusions)
    
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
        print("Error procesing this can be a view or user does not have permission:", table) 
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