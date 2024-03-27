import logging
from typing import Iterable

import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from pandas import DataFrame
from sqlalchemy.engine import Engine

CHUNK_SIZE = 1000
BUFFER_SIZE = 65536
VERTICA_SCHEMA = 'stv2023121143__staging'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_data_source2staging(table_name: str, dfs: Iterable[DataFrame]):
    target_hook = VerticaHook('dwh')
    target_conn = target_hook.get_conn()

    for i, df in enumerate(dfs):
        df.to_csv('/tmp/chunk.csv', index=False)
        logger.info(f'Отправляем чанк №{i}, размер чанка = {len(df)}')

        columns = ', '.join(list(df.columns))
        copy_expr = f"COPY {VERTICA_SCHEMA}.{table_name} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"'"
        with target_conn.cursor() as cursor:
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cursor.copy(copy_expr, chunk, buffer_size=BUFFER_SIZE)

        logger.info(f'Чанк отправлен')

    target_conn.close()


def get_source_query_and_prepare_target(
    table_name: str,
    date_column: str,
    previous_date: str,
    recalculate: bool = False
):
    target_hook = VerticaHook('dwh')

    with target_hook.get_conn() as target_conn:
        if recalculate:
            logger.info(f'Очищаем таблицу в целевом ХД, так как {recalculate=}')
            with target_conn.cursor() as cursor:
                cursor.execute(f'truncate table {VERTICA_SCHEMA}.{table_name}')

            logger.info(f'Читаем источник полностью, так как {recalculate=}')
            query = table_name
        else:
            where_clause = f'where cast({date_column} as date) = cast(%s as date)'

            logger.info(f'Удаляем из таблицы данные с {date_column}={previous_date}, так как {recalculate=}')
            with target_conn.cursor() as cursor:
                cursor.execute(f'delete from {VERTICA_SCHEMA}.{table_name} {where_clause}', [previous_date])

            logger.info(f'Читаем источник с фильтром {date_column}={previous_date}, так как {recalculate=}')
            query = f'select * from {table_name} {where_clause}'

    return query


def load_source2staging(table_name: str, date_column: str, previous_date: str, recalculate: bool = False):
    source_hook = PostgresHook('source')
    source_engine: Engine = source_hook.get_sqlalchemy_engine()

    query = get_source_query_and_prepare_target(table_name, date_column, previous_date, recalculate)

    dfs = pd.read_sql(query, source_engine, params=(previous_date, ), chunksize=CHUNK_SIZE)

    load_data_source2staging(table_name, dfs)

    source_engine.dispose()


@dag(
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2022, 10, 2),
    params={'recalculate': False},
    render_template_as_native_obj=True,
    catchup=False,
)
def source2staging():
    table_name_x_date_column = {'currencies': 'date_update', 'transactions': 'transaction_dt'}
    for table_name, date_column in table_name_x_date_column.items():
        PythonOperator(
            task_id=table_name,
            python_callable=load_source2staging,
            op_args=[table_name, date_column, '{{ yesterday_ds }}', '{{ params["recalculate"] }}']
        )


_ = source2staging()
