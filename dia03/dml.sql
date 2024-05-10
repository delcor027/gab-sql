-- Databricks notebook source
describe table sandbox.twitch.teo_gab_cte

-- COMMAND ----------

INSERT INTO sandbox.twitch.teo_gab_cte VALUES ('2024-01-01', 'BEBES', 10, 10)

-- COMMAND ----------

SELECT * FROM sandbox.twitch.teo_gab_cte

-- COMMAND ----------

INSERT INTO sandbox.twitch.teo_gab_cte
VALUES ('2024-01-01', 'perfumaria', 10, 10),
       ('2024-01-02', 'perfumaria', 10, 20)

-- COMMAND ----------

SELECT * FROM sandbox.twitch.teo_gab_cte

-- COMMAND ----------

INSERT INTO sandbox.twitch.teo_gab_cte
VALUES ('2024-01-01', '2024-01-01', 10, 10)

-- COMMAND ----------

INSERT INTO sandbox.twitch.teo_gab_cte
(descCategoria, dataPedido, qtdeItens, qtdeItensAcum)
VALUES ('pefumaria', '2024-01-10', 10, 10)

-- COMMAND ----------

SELECT * FROM sandbox.twitch.teo_gab_cte

-- COMMAND ----------

DELETE FROM sandbox.twitch.teo_gab_cte
WHERE descCategoria = '2024-01-01'

-- COMMAND ----------

UPDATE sandbox.twitch.teo_gab_cte
SET descCategoria = 'bebes'
WHERE desccategoria = 'BEBES';

-- COMMAND ----------

SELECT *
FROM sandbox.twitch.teo_gab_cte
WHERE upper(descCategoria) = 'BEBES'
