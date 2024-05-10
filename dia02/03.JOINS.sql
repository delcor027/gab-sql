-- Databricks notebook source
SELECT date(t1.dtPedido) AS dataPedido,
       sum(COALESCE(t2.vlPreco,0)) AS vlReceita,
       count(distinct t1.idPedido) AS qtdePedido,
       count(distinct t1.idPedido, t2.idPedidoItem) AS qtdeProdutos

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

GROUP BY ALL
ORDER BY dataPedido

-- COMMAND ----------

COALESCE(tel1, tel2, cel1, cel2, fax)

-- COMMAND ----------

SELECT count(*)

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC data = {
-- MAGIC     "nome": ["teo", "nah", "jose"],
-- MAGIC     "tel1": [None, None, 1],
-- MAGIC     "tel2": [1, None, 2],
-- MAGIC     "cel1": [None, 2, None],
-- MAGIC     "cel2": [2, None, None],
-- MAGIC }
-- MAGIC
-- MAGIC df = spark.createDataFrame(pd.DataFrame(data))
-- MAGIC df.createOrReplaceTempView("my_data")

-- COMMAND ----------

SELECT *,
       coalesce(cel1, cel2, tel1, tel2, 'danasee')

FROM my_data

-- COMMAND ----------


