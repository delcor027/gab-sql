# Databricks notebook source
import delta

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()


def table_exists(table, database):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName='{table}'")
                  .count())
    return count > 0

# COMMAND ----------

query = import_query("rfv.sql")

# COMMAND ----------

df_new = spark.sql(query, date='2017-12-02')

if not table_exists('rfv', 'sandbox.twitch'):
    print("Criando a tabelinda...")
    (df_new.coalesce(1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("dtRef")
    .saveAsTable("sandbox.twitch.rfv")
    )

else:

    df_delta = delta.DeltaTable.forName(spark, "sandbox.twitch.rfv")

    (df_delta.alias("d")
             .merge(df_new.alias("n"), "d.idVendedor = n.idVendedor AND d.dtRef = n.dtRef")
            # .whenMatchedDelete(condition = "d.CHANGE_TYPE = 'D'")    #conditions are optional
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dtref,
# MAGIC        count(*)
# MAGIC        
# MAGIC FROM sandbox.twitch.rfv
# MAGIC group by all

# COMMAND ----------

staging_data.alias("s")
.merge(delta_data.alias("d"),
"s.PassengerId = d.PassengerId")
.whenMatchedDelete(condition = "d.CHANGE_TYPE = 'D'")    #conditions are optional
.whenMatchedUpdateAll(condition = "d.CHANGE_TYPE ='A'")
.whenNotMatchedInsertAll(condition = "d.CHANGE_TYPE = 'I'")
.execute() 

# COMMAND ----------

teo, 31, I, 
teo, 32, U
teo, 32, D

# COMMAND ----------

tablepath = "s3://teomewhy-datalake-unity/053137af-f7a7-4229-990e-338451a9d0ca/tables/052c5752-6cb9-4a72-80ad-2aecd0509fba/dtRef=2017-12-02/"

[i.name for i in dbutils.fs.ls(tablepath)]

# COMMAND ----------

tablepath = "s3://teomewhy-datalake-unity/053137af-f7a7-4229-990e-338451a9d0ca/tables/052c5752-6cb9-4a72-80ad-2aecd0509fba/"

[i.name for i in dbutils.fs.ls(tablepath)]

# COMMAND ----------

df_delta.vacuum()
