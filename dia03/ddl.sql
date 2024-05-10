-- Databricks notebook source
CREATE TABLE sandbox.twitch.tb_teo_gab AS (

  select *
  from silver.olist.pedido
  limit 10

)

-- COMMAND ----------

SELECT * FROM sandbox.twitch.tb_teo_gab

-- COMMAND ----------

CREATE TABLE sandbox.twitch.teo_gab_cte AS (
    WITH tb_cat_dia AS(
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
)

-- COMMAND ----------

select * from sandbox.twitch.teo_gab_cte

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.twitch.teo_gab_empty (
  nome string,
  idade int,
  email string 
)

-- COMMAND ----------

SELECT * FROM sandbox.twitch.teo_gab_empty

-- COMMAND ----------

-- DBTITLE 1,DROP
DROP TABLE IF EXISTS sandbox.twitch.teo_gab_empty

-- COMMAND ----------

-- DBTITLE 1,TRUNCATE
TRUNCATE TABLE sandbox.twitch.teo_gab_cte

-- COMMAND ----------

SELECT * FROM sandbox.twitch.teo_gab_cte
