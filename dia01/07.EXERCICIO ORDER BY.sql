-- Databricks notebook source
-- Qual pedido com maior valor de frete?
-- E considerando o total pago? Max? Min?

SELECT idPedido, vlFrete
FROM silver.olist.item_pedido
ORDER BY vlFrete desc
LIMIT 1

-- COMMAND ----------

-- E o menor?
SELECT idPedido, vlFrete, vlPreco
FROM silver.olist.item_pedido
ORDER BY vlFrete, vlPreco
LIMIT 1

-- COMMAND ----------

SELECT idPedido,
       vlFrete,
       vlPreco, 
       vlFrete + vlPreco AS precoTotal
FROM silver.olist.item_pedido
ORDER BY vlFrete + vlPreco DESC
LIMIT 1

-- COMMAND ----------

SELECT idPedido,
       vlFrete,
       vlPreco, 
       vlFrete + vlPreco AS precoTotal
FROM silver.olist.item_pedido
ORDER BY vlFrete + vlPreco ASC
LIMIT 1
