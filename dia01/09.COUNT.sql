-- Databricks notebook source
SELECT
  count(*),
  count(DISTINCT idPedido),
  count(DISTINCT idProduto),
  count(DISTINCT idPedido, idProduto),
  count(1),
  count(idPedido)
FROM
  silver.olist.item_pedido

-- COMMAND ----------

SELECT count(*),
       count(dtEntregue)
FROM silver.olist.pedido
