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