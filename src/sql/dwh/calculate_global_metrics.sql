delete from stv2023121143__dwh.global_metrics
where date_update = date :date_update;

insert into stv2023121143__dwh.global_metrics (
    date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions
)
with finished_transactions as (
    select
        *,
        last_value(status) over (
            partition by operation_id
            order by transaction_dt
            rows between unbounded preceding and unbounded following
        ) = 'done' as finished_transaction
    from stv2023121143__staging.transactions
)
select
    date :date_update as date_update,
    t.currency_code as currency_from,
    cast(round(sum(t.amount * coalesce(c.currency_with_div, 1)), 2) as decimal(18, 2)) as amount_total,
    count(1) as cnt_transactions,
    cast(
        round(
            sum(t.amount * coalesce(c.currency_with_div, 1)) / count(distinct t.account_number_from), 2
        ) as decimal(18, 2)
    ) as avg_transactions_per_account,
    count(distinct t.account_number_from) as cnt_accounts_make_transactions
from finished_transactions t
left join stv2023121143__staging.currencies c
on t.currency_code = c.currency_code
    and c.currency_code_with = 420
    and c.date_update = date :date_update
where t.transaction_dt::date = date :date_update
    and t.finished_transaction is True
    and t.status = 'done'
group by t.currency_code;
