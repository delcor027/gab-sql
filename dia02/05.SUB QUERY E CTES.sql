-- Databricks notebook source
-- 1. Quais são os TOP 10 vendedores que mais venderam (em quantidade) no mês com maior número de vendas no Olist?

SELECT idVendedor,
       count(*)  AS qtde
FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE date(date_trunc('MONTH', t2.dtPedido)) IN ( 
    SELECT 
          date(date_trunc('MONTH', dtPedido)) AS dtMes
    FROM silver.olist.pedido
    GROUP BY ALL
    ORDER BY count(idPedido) DESC
    LIMIT 1
)

GROUP BY ALL
ORDER BY qtde DESC
LIMIT 10

-- COMMAND ----------

WITH tb_mes_top AS (
  
  SELECT 
        date(date_trunc('MONTH', dtPedido)) AS dtMes
  FROM silver.olist.pedido
  GROUP BY ALL
  ORDER BY count(idPedido) DESC
  LIMIT 1

),

tb_vendedores AS (

  SELECT idVendedor,
        count(*)  AS qtde
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  INNER JOIN tb_mes_top AS t3
  ON DATE(DATE_TRUNC('MONTH', t2.dtPedido)) = t3.dtMes

  GROUP BY ALL
  ORDER BY qtde DESC

)

SELECT *
FROM tb_vendedores
LIMIT 10

-- COMMAND ----------

SELECT * FROM tb_mes_top
