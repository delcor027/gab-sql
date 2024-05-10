-- Databricks notebook source
SELECT *,
       CASE
          WHEN date(dtEntregue) > date(dtEstimativaEntrega) THEN 'Atraso'
          WHEN date(dtEntregue) = date(dtEstimativaEntrega) THEN 'Em dia'
          WHEN date(dtEntregue) < date(dtEstimativaEntrega) THEN 'Adiantado'
          ELSE 'Sem Status'
       END AS DescStatusEntrega

FROM silver.olist.pedido

-- COMMAND ----------

SELECT idProduto,
       descCategoria,
       vlPesoGramas,
       CASE
          WHEN vlPesoGramas < 100 THEN 'Pena'
          WHEN vlPesoGramas < 500 THEN 'Leve'
          WHEN vlPesoGramas < 2500 THEN 'Medio'
          ELSE 'Pesado'
       END AS descPeso
FROM silver.olist.produto
