create or replace table demo_db.public.colors as
    select 2017 year, 'red' color, true favorite
    UNION ALL
    select 2017 year, 'orange' color, true favorite
    UNION ALL
    select 2017 year, 'green' color, false favorite
    UNION ALL
    select 2018 year, 'blue' color, true favorite
    UNION ALL
    select 2018 year, 'violet' color, true favorite
    UNION ALL
    select 2018 year, 'brown' color, false favorite;

select * from demo_db.public.colors