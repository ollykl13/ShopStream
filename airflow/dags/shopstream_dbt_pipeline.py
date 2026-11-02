from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
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
    description='Real-time: проверка новых данных в ClickHouse -> запуск dbt',
    schedule_interval='* * * * *',  # каждую минуту
    catchup=False,
    tags=['shopstream', 'dbt', 'realtime'],
) as dag:

    # 1. Сенсор: проверяет, появились ли новые события за последние 2 минуты
    check_new_data = SqlSensor(
        task_id='check_for_new_events',
        conn_id='clickhouse_shop',  # ID подключения, которое мы создаем в UI
        sql="""
            SELECT count() FROM shop.raw_events
            WHERE _ingested_at >= now() - INTERVAL 2 MINUTE
        """,
        poke_interval=30,  # проверять каждые 30 секунд
        timeout=120,       # ждать до 2 минут
    )

    # 2. Оператор: запускает dbt build для пересборки всех витрин
    run_dbt_build = BashOperator(
        task_id='run_dbt_build',
        bash_command='cd /opt/airflow/dbt_shop && dbt build --profiles-dir /opt/airflow/dbt_shop',
    )

    # Определяем порядок выполнения
    check_new_data >> run_dbt_build