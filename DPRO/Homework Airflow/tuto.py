"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    # "depends_on_past": False,
    # ������ ���� ������ �����������
    "depends_on_past": True,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'end_date': datetime(2015, 6, 15),
}

# dag = DAG("tutorial", default_args=default_args, schedule_interval=timedelta(1))
# �� ������ ���� ��������
dag = DAG("tutorial", default_args=default_args, schedule_interval=timedelta(1), max_active_runs=2)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 2", retries=3, dag=dag)

templated_command = """
    {% for i in range(2) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
# t1 >> t2 >> t3