-- Databricks notebook source
-- DBTITLE 1,JOIN 04
-- Quando o frete é mais caro que a mercadoria,
-- os clientes preferem qual meio de pagamento?

SELECT descTipoPagamento,
       COUNT(distinct idPedido) AS qtdePedidos

FROM (
    SELECT t1.idPedido,
          t2.descTipoPagamento,
          sum(t1.vlFrete) AS totalFrete,
          sum(t1.vlPreco) AS totalPreco

    FROM silver.olist.item_pedido AS t1

    LEFT JOIN silver.olist.pagamento_pedido AS t2
    ON t1.idPedido = t2.idPedido

    GROUP BY ALL
    HAVING totalFrete > totalPreco
)

GROUP BY ALL
ORDER BY qtdePedidos DESC

-- COMMAND ----------

-- DBTITLE 1,JOIN
-- 6.Quantidade de vendas por categoria em cada estado (cliente)?

SELECT 
      t2.descCategoria,
      t4.descUF,
      count(*) AS qtdeVenda,
      count(DISTINCT t1.idPedido, t1.idPedidoItem, t3.idCliente) AS qtdeVendaDST

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

LEFT JOIN silver.olist.pedido AS t3
ON t1.idPedido = t3.idPedido

LEFT JOIN silver.olist.cliente AS t4
ON t3.idCliente = t4.idCliente

GROUP BY ALL
ORDER BY qtdeVenda DESC

-- COMMAND ----------

SELECT 
      t2.descCategoria,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'AC' THEN t1.idPedido END) AS AC,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'AL' THEN t1.idPedido END) AS AL,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'AM' THEN t1.idPedido END) AS AM,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'AP' THEN t1.idPedido END) AS AP,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'BA' THEN t1.idPedido END) AS BA,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'CE' THEN t1.idPedido END) AS CE,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'DF' THEN t1.idPedido END) AS DF,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'ES' THEN t1.idPedido END) AS ES,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'GO' THEN t1.idPedido END) AS GO,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'MA' THEN t1.idPedido END) AS MA,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'MG' THEN t1.idPedido END) AS MG,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'MS' THEN t1.idPedido END) AS MS,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'MT' THEN t1.idPedido END) AS MT,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'PA' THEN t1.idPedido END) AS PA,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'PB' THEN t1.idPedido END) AS PB,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'PE' THEN t1.idPedido END) AS PE,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'PI' THEN t1.idPedido END) AS PI,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'PR' THEN t1.idPedido END) AS PR,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'RJ' THEN t1.idPedido END) AS RJ,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'RN' THEN t1.idPedido END) AS RN,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'RO' THEN t1.idPedido END) AS RO,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'RR' THEN t1.idPedido END) AS RR,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'RS' THEN t1.idPedido END) AS RS,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'SC' THEN t1.idPedido END) AS SC,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'SE' THEN t1.idPedido END) AS SE,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'SP' THEN t1.idPedido END) AS SP,
      COUNT(DISTINCT CASE WHEN t4.descUF = 'TO' THEN t1.idPedido END) AS TO

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

LEFT JOIN silver.olist.pedido AS t3
ON t1.idPedido = t3.idPedido

LEFT JOIN silver.olist.cliente AS t4
ON t3.idCliente = t4.idCliente

GROUP BY ALL
ORDER BY descCategoria DESC

-- COMMAND ----------

select distinct DESCUF from silver.olist.cliente

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('''
-- MAGIC SELECT 
-- MAGIC       t2.descCategoria,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'AC' THEN t1.idPedido END) AS AC,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'AL' THEN t1.idPedido END) AS AL,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'AM' THEN t1.idPedido END) AS AM,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'AP' THEN t1.idPedido END) AS AP,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'BA' THEN t1.idPedido END) AS BA,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'CE' THEN t1.idPedido END) AS CE,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'DF' THEN t1.idPedido END) AS DF,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'ES' THEN t1.idPedido END) AS ES,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'GO' THEN t1.idPedido END) AS GO,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'MA' THEN t1.idPedido END) AS MA,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'MG' THEN t1.idPedido END) AS MG,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'MS' THEN t1.idPedido END) AS MS,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'MT' THEN t1.idPedido END) AS MT,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'PA' THEN t1.idPedido END) AS PA,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'PB' THEN t1.idPedido END) AS PB,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'PE' THEN t1.idPedido END) AS PE,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'PI' THEN t1.idPedido END) AS PI,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'PR' THEN t1.idPedido END) AS PR,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'RJ' THEN t1.idPedido END) AS RJ,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'RN' THEN t1.idPedido END) AS RN,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'RO' THEN t1.idPedido END) AS RO,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'RR' THEN t1.idPedido END) AS RR,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'RS' THEN t1.idPedido END) AS RS,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'SC' THEN t1.idPedido END) AS SC,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'SE' THEN t1.idPedido END) AS SE,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'SP' THEN t1.idPedido END) AS SP,
-- MAGIC       COUNT(DISTINCT CASE WHEN t4.descUF = 'TO' THEN t1.idPedido END) AS TO
-- MAGIC
-- MAGIC FROM silver.olist.item_pedido AS t1
-- MAGIC
-- MAGIC LEFT JOIN silver.olist.produto AS t2
-- MAGIC ON t1.idProduto = t2.idProduto
-- MAGIC
-- MAGIC LEFT JOIN silver.olist.pedido AS t3
-- MAGIC ON t1.idPedido = t3.idPedido
-- MAGIC
-- MAGIC LEFT JOIN silver.olist.cliente AS t4
-- MAGIC ON t3.idCliente = t4.idCliente
-- MAGIC
-- MAGIC GROUP BY ALL
-- MAGIC ORDER BY descCategoria DESC
-- MAGIC ''').toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.set_index('descCategoria').T.corr()

-- COMMAND ----------

-- DBTITLE 1,CTE 02
-- 2. Total de vendas histórica (independente da categoria) dos vendedores que venderam ao menos um produto da categoria BEBES na blackfriday de 2017-11-24.
-- 1. Com IN
-- 2. Com JOIN

WITH tb_vendedores_blackfriday AS (
  
  SELECT distinct t2.idVendedor
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido As t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE date(t1.dtPedido) = '2017-11-24'
  AND upper(t3.descCategoria) = 'BEBES'
)

SELECT  
       t1.idVendedor,
       count(DISTINCT t2.idPedido, t2.idPedidoItem) AS totalItemVendido

FROM tb_vendedores_blackfriday As t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idVendedor = t2.idVendedor

GROUP BY ALL
ORDER BY totalItemVendido

-- COMMAND ----------

-- DBTITLE 0,1.ID
WITH tb_vendedores_blackfriday AS (
  
  SELECT distinct t2.idVendedor
  
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido As t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE date(t1.dtPedido) = '2017-11-24'
  AND upper(t3.descCategoria) = 'BEBES'
)

SELECT  
       t1.idVendedor,
       count(DISTINCT t1.idPedido, t1.idPedidoItem) AS totalItemVendido

FROM silver.olist.item_pedido AS t1

WHERE t1.idVendedor IN (SELECT * FROM tb_vendedores_blackfriday)

GROUP BY ALL
ORDER BY totalItemVendido
