from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor


@dag(
    schedule='0 1 * * *',
    start_date=pendulum.datetime(2022, 10, 2),
    render_template_as_native_obj=True,
    catchup=False,
)
def staging2dwh():
    sensor = ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='source2staging',
        external_task_ids=['currencies', 'transactions'],
        execution_delta=timedelta(hours=1),
        poke_interval=60,
        timeout=3600,
        mode='reschedule'
    )
    global_metrics = SQLExecuteQueryOperator(
        task_id='global_metrics',
        conn_id='dwh',
        sql='./sql/dwh/calculate_global_metrics.sql',
        parameters={'date_update': '{{ yesterday_ds }}'},
    )
    sensor >> global_metrics


_ = staging2dwh()
