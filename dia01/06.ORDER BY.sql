-- Databricks notebook source
SELECT  idProduto,
        descCategoria,
        vlComprimentoCm * vlAlturaCm * vlLarguraCm AS cubagem

FROM silver.olist.produto

WHERE vlComprimentoCm IS NOT NULL
AND vlAlturaCm IS NOT NULL
AND vlLarguraCm IS NOT NULL

ORDER BY cubagem DESC, descCategoria ASC
