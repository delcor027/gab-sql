# Databricks notebook source
df = spark.read.format("json").load("/mnt/datalake/pokemon/pokemon/")
df.createGlobalTempView("pokemon")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tb_pokemons AS (
# MAGIC   SELECT ingestion_date,
# MAGIC         explode(results) as pokemons
# MAGIC
# MAGIC   FROM global_temp.pokemon
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC        pokemons.*
# MAGIC FROM tb_pokemons

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ingestion_date,
# MAGIC        pokemons.*
# MAGIC FROM global_temp.pokemon
# MAGIC LATERAL VIEW explode(results) AS pokemons
# MAGIC QUALIFY row_number() OVER (PARTITION BY name ORDER BY ingestion_date DESC) = 1
# MAGIC ORDER BY pokemons.name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.pokemon.pokemon
