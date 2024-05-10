-- Databricks notebook source
SELECT count(*)
FROM silver.olist.vendedor
WHERE upper(descUF) = 'RJ'
