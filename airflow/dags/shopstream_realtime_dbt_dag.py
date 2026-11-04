from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'shopstream',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'shopstream_realtime_dbt',
    default_args=default_args,
    description='Real-time: watermark-проверка -> dbt build -> обновление watermark',
    schedule_interval='* * * * *',
    catchup=False,
    tags=['shopstream', 'dbt', 'realtime'],
) as dag:

    # 1. Сенсор: есть ли данные новее последнего watermark?
    check_new_data = SqlSensor(
        task_id='check_for_new_events',
        conn_id='clickhouse_shop',
        sql="""
            SELECT count() > 0
            FROM shop.raw_events
            WHERE _ingested_at > COALESCE(
                (SELECT max(last_ingested_at) FROM shop.dbt_watermark WHERE id = 1),
                toDateTime('1970-01-01 00:00:00')
            )
        """,
        poke_interval=30,
        timeout=120,
    )

    # 2. dbt build (инкрементальный, target=docker для подключения к clickhouse)
    run_dbt_build = BashOperator(
        task_id='run_dbt_build',
        bash_command='cd /opt/airflow/dbt_shop && dbt build --profiles-dir /opt/airflow/dbt_shop --target docker',
    )

    # 3. Обновить watermark после успешного build
    update_watermark = SQLExecuteQueryOperator(
        task_id='update_watermark',
        conn_id='clickhouse_shop',
        sql="""
            INSERT INTO shop.dbt_watermark (id, last_ingested_at)
            SELECT 1, max(_ingested_at)
            FROM shop.raw_events
        """,
    )

    check_new_data >> run_dbt_build >> update_watermark
