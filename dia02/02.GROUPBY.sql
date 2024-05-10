-- Databricks notebook source
SELECT descUF,
       count(*) AS qtdeVendedores
FROM silver.olist.vendedor
GROUP BY descUF
ORDER BY descUF ASC

-- COMMAND ----------

SELECT 
        idPedido,
        sum(vlPreco) AS totalPreco,
        sum(vlFrete) AS totalFrete,
        sum(vlFrete + vlPreco) AS totalPedido,
        count(*) AS qtdeProdutos,
        sum(vlPreco) / count(*) AS vlPrecoMedio,
        sum(vlFrete + vlPreco) / count(*) AS vlTotalMedio

FROM silver.olist.item_pedido
GROUP BY idPedido
ORDER BY totalFrete DESC

-- COMMAND ----------

SELECT idPedido,
       sum(vlFrete + vlPreco) AS vlTotal

FROM silver.olist.item_pedido
GROUP BY idPedido
HAVING vlTotal > 1000
ORDER BY vlTotal ASC
LIMIT 10

-- COMMAND ----------

SELECT date(dtPedido) AS dataPedido,
       count(idPedido) AS qtdePedidos

FROM silver.olist.pedido
GROUP BY dataPedido
ORDER BY dataPedido

-- COMMAND ----------

SELECT date(dtPedido) AS dataPedido,
       count(idPedido) AS qtdePedidos

FROM silver.olist.pedido
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

SELECT date(dtPedido) AS dataPedido,
       COUNT(*)

FROM silver.olist.pedido
GROUP BY dataPedido
ORDER BY dataPedido

-- COMMAND ----------

select descTipoPagamento,
       nrParcelas,
       sum(vlPagamento),
       count(idPedido)

from silver.olist.pagamento_pedido

GROUP BY ALL

-- COMMAND ----------

SELECT coalesce(NULL, 0) + 10
