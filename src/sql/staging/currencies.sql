drop table if exists stv2023121143__staging.currencies;
create table stv2023121143__staging.currencies
(
    date_update        timestamp,
    currency_code      integer,
    currency_code_with integer,
    currency_with_div  numeric(5, 3)
)
order by currency_code, currency_code_with
segmented by hash(date_update) all nodes
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2);
