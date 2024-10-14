-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_schema", "main.default")

-- COMMAND ----------

create or replace view ${catalog_schema}.tables_size_latest as
select *, split_part(name, '.', 1) as catalog, 
            split_part(name, '.', 2) as schema, 
            split_part(name, '.', 3) as table_name, 
            sizeInBytes / 1073741824 as sizeInGb from
(select *, 
  rank() over (partition by `name` order by `timestamp` desc) as rank
  from ${catalog_schema}.tables_size) t
where t.rank=1

-- COMMAND ----------


