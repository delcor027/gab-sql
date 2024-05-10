-- Databricks notebook source
SELECT *

FROM silver.olist.cliente

WHERE descUF = 'ES'
AND codCep = 29830

-- COMMAND ----------

SELECT *

FROM silver.olist.cliente

WHERE descUF = 'SP'
OR descCidade = 'rio de janeiro'
