-- Databricks notebook source
SELECT 
    avg(vlPreco) AS avgPreco,
    avg(vlFrete) AS avgFrete,
    median(vlFrete) AS medianVlFrete,
    percentile(vlPreco, 0.25) AS pct25Preco,
    percentile(vlPreco, 0.75) AS pct75Preco,
    min(vlPreco)AS minPreco,
    max(vlPreco + vlFrete) AS maxTotal

FROM silver.olist.item_pedido

-- COMMAND ----------


