-- Databricks notebook source
-- DBTITLE 1,Exercício 01
-- Selecione todos os clientes paulistanos

-- COMMAND ----------

-- DBTITLE 1,Foco WHERE - 04
-- Lista de pedidos que foram entregues com atraso.
SELECT idPedido
FROM silver.olist.pedido
WHERE date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

-- DBTITLE 1,Foco WHERE 07
-- Lista de pedidos com avaliação maior ou igual que 4

SELECT idPedido
FROM silver.olist.avaliacao_pedido
WHERE vlNota >= 4
