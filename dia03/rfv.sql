SELECT 
       {date} AS dtRef,
       t2.idVendedor,
       min(date_diff({date}, t1.dtPedido)) AS recencia,
       count(distinct date(t1.dtPedido)) AS frequencia,
       sum(t2.vlPreco) AS valor

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < {date} -- hoje
AND t1.dtPedido >= {date} - INTERVAL 60 DAY
AND t2.idVendedor is not null

GROUP BY ALL