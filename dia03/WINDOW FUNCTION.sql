-- Databricks notebook source
WITH tb_hist AS (

  SELECT date(dtPedido) AS dtPedido,
        count(distinct idPedido) AS qtdePedido
  FROM silver.olist.pedido
  GROUP BY ALL
)

SELECT *,
       sum(qtdePedido) OVER (ORDER BY dtPedido) AS qtdeAcum
FROM tb_hist
ORDER BY dtPedido

-- COMMAND ----------

WITH tb_cat_uf AS (

  SELECT 
        t2.descCategoria,
        t4.descUF,
        count(*) AS qdeItens

  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  LEFT JOIN silver.olist.pedido AS t3
  ON t1.idPedido = t3.idPedido

  LEFT JOIN silver.olist.cliente AS t4
  ON t3.idCliente = t4.idCliente

  GROUP BY ALL
  ORDER BY qdeItens DESC

),

tb_rank AS (
  SELECT *
  FROM tb_cat_uf
  QUALIFY row_number() OVER (PARTITION BY descUF ORDER BY qdeItens DESC) = 1
  ORDER BY descUF, qdeItens DESC
)

SELECT *
FROM tb_rank

