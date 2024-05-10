-- Databricks notebook source
SELECT 
        idPedido AS pedido,
        idCliente,
        dtPedido,
        datediff(now(), dtPedido) AS qtDiasPedido

FROM silver.olist.pedido

LIMIT 10

-- COMMAND ----------

SELECT 
      *

EXCEPT (dtAprovado, dtEnvio)

FROM silver.olist.pedido

LIMIT 10
