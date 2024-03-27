drop table if exists stv2023121143__staging.transactions;
create table stv2023121143__staging.transactions
(
    operation_id        varchar(60),
    account_number_from integer,
    account_number_to   integer,
    currency_code       integer,
    country             varchar(30),
    status              varchar(30),
    transaction_type    varchar(30),
    amount              integer,
    transaction_dt      timestamp
)
order by operation_id
segmented by hash(transaction_dt, operation_id) all nodes
partition by transaction_dt::date
group by calendar_hierarchy_day(transaction_dt::date, 3, 2);
