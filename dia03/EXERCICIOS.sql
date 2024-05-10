-- Databricks notebook source
-- 1. Quais são os Top 5 vendedores campeões de vendas (quantidade) de cada UF?

WITH tb_vendedores_uf AS (

  SELECT t1.idVendedor,
         t2.descUF,
         count(*) AS qtdeItems

  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.vendedor AS t2
  ON t1.idVendedor = t2.idVendedor

  GROUP BY ALL
)

SELECT *
FROM tb_vendedores_uf
QUALIFY row_number() OVER (PARTITION BY descUF ORDER BY qtdeItems DESC) <= 5
ORDER BY descUF

-- COMMAND ----------

-- 5. Quantidade acumulada de itens vendidos por categoria ao longo do tempo.

WITH tb_cat_dia (
  SELECT date(t2.dtPedido) AS dataPedido,
        t3.descCategoria,
        count(*) AS qtdeItens

  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t1.idProduto = t3.idProduto

  GROUP BY ALL
)

SELECT *,
       SUM(qtdeItens) OVER (PARTITION BY descCategoria ORDER BY dataPedido) AS qtdeItensAcum

FROM tb_cat_dia
ORDER  BY descCategoria, dataPedido

-- COMMAND ----------

-- 5. Quantidade acumulada de itens vendidos por categoria ao longo do tempo.

WITH tb_cat_dia (
  SELECT date(t2.dtPedido) AS dataPedido,
        t3.descCategoria,
        count(*) AS qtdeItens

  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t1.idProduto = t3.idProduto

  GROUP BY ALL
),

tb_cat_7d AS (
    SELECT *
    FROM tb_cat_dia
    QUALIFY row_number() OVER (PARTITION BY descCategoria ORDER BY dataPedido DESC) <= 7
)

SELECT *,
       SUM(qtdeItens) OVER (PARTITION BY descCategoria ORDER BY dataPedido) AS qtdeItensAcum
FROM tb_cat_7d
-- QUALIFY  row_number() OVER (PARTITION BY descCategoria ORDER BY dataPedido DESC) = 1

-- COMMAND ----------

-- 7. **PLUS:** Selecione um dia de venda aleatório de cada vendedor

WITH tb_vendedor_data AS (

SELECT date(t2.dtPedido) AS dataPedido,
       t1.idVendedor

FROM silver.olist.item_pedido As t1
LEFT JOIN silver.olist.pedido AS t2
ON t1.idPedido = t2.idPedido

)

SELECT *
FROM tb_vendedor_data
QUALIFY row_number() OVER (PARTITION BY idVendedor ORDER BY RAND()) <= 5

-- COMMAND ----------

SELECT *
FROM silver.olist.vendedor
QUALIFY row_number() OVER (PARTITION BY descUF ORDER BY rand()) <= 5
