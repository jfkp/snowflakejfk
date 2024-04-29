select *
    from colors

select *
    from colors a,table(get_color(a.year,a.color,a.favorite,2017))