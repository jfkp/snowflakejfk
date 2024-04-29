/****************************************************************/

select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER limit 100

select c_name, c_address,c_acctbal,c_mktsegment 
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
where c_acctbal<5000;

select c_name, c_address,c_acctbal,c_mktsegment 
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
where c_acctbal<300;


CREATE OR REPLACE FUNCTION return_customer_by_amount(amt number)
    RETURNS TABLE (c_name VARCHAR, c_address VARCHAR,c_acctbal number,c_mktsegment VARCHAR)
    language SQL
    as
    $$
    select c_name, c_address,c_acctbal,c_mktsegment 
    from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
    where c_acctbal<amt
    $$
    ;

    SELECT *
    FROM  TABLE(return_customer_by_amount(5000))

    -- SELECT *
    -- FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
    -- a, TABLE(return_customer_by_amount(1))
    --ORDER BY city_name;



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

/***************************************************************************************/

select distinct stock,
sum(cast(replace(Avgtradeprice,',','') as INTEGER)*cast(quantity as INTEGER)) over (partition by stock order by stock asc) cost
from demo_db.public.stock_sale 
where stock = 'ITC Ltd'
--group by stock


select *
from table(information_schema.query_history())
order by start_time;


select *
from table(information_schema.query_history(dateadd('hours',-1,current_timestamp()),current_timestamp()))
order by start_time;