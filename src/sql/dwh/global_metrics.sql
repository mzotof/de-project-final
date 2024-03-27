drop table if exists stv2023121143__dwh.global_metrics;
create table stv2023121143__dwh.global_metrics
(
    date_update date,
    currency_from integer,
    amount_total decimal(18, 2),
    cnt_transactions integer,
    avg_transactions_per_account decimal(18, 2),
    cnt_accounts_make_transactions integer
)
order by currency_from
segmented by hash(date_update, currency_from) all nodes
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2);
