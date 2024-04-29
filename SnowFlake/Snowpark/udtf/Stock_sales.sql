create or replace table 
demo_db.public.stock_sale 
(
Stock	 varchar,
Date	 varchar,
Party	 varchar,
Category	 varchar,
Txntype      varchar,
Quantity	 varchar,
Value_traded varchar,	
Avgtradeprice varchar
)

truncate table demo_db.public.stock_sale 

select * from demo_db.public.stock_sale
where stock = 'ITC Ltd'
order by stock


SELECT b.symbol,b.cost
    FROM  demo_db.public.stock_sale a ,TABLE(process_stock_price(a.Stock,cast(a.quantity as INTEGER),cast(replace(a.Avgtradeprice,',','') as INTEGER))) b
    --where a.stock = 'ITC Ltd'

    
select b.symbol,b.cost
  from demo_db.public.stock_sale a, table(process_stock_price_partition_aware(a.Stock,cast(a.quantity as INTEGER),cast(replace(a.Avgtradeprice,',','') as INTEGER)) 
  
over (partition by a.Stock order by a.stock asc)) b
where a.stock = 'ITC Ltd'