--Please change database if have you use a different!!
use default;


--From te two most commonly appearing regiosn, which is the latest datasource?

with stage as
(
    SELECT *,
        count() over (PARTITION BY region) as rnk1
            ,row_number() over (PARTITION BY region,datasource order BY `datetime` desc) as rnk2 
                FROM test_trips where dt_partition='052018'
    ORDER BY datasource desc
)
,
region_most_frequency 
as
(
    select  distinct region,rnk1 from stage order by rnk1 desc limit 2
)
,
latest_datasource
as
(
    select stage.region,datasource,`datetime`
                ,row_number() over (PARTITION BY stage.region order BY `datetime` desc) as rnk3
        FROM stage 
        inner join
        region_most_frequency
        on stage.region = region_most_frequency.region
        where rnk2 =1
)
select region, datasource, `datetime` from latest_datasource where rnk3=1;

--What Regions has the "cheap_mobile" datasource appeared in?
select count(*) as quantity_appeared,region  fROM test_trips where dt_partition='052018' and datasource = 'cheap_mobile' group by region
