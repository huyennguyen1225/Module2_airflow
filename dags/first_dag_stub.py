import airflow
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
#from openpyxl.styles.builtins import output

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='first_dag_stub',
    default_args=default_args_dict,
    catchup=False,
)

task_one = BashOperator(
    task_id='get_bitcoin_price',
    dag=first_dag,
    bash_command='curl -s "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd" --output /opt/airflow/dags/{{ds_nodash}}_price.json',
)

task_two = BashOperator(
    task_id='extract_usd_price',
    dag=first_dag,
    bash_command='grep -oiE \'\{"usd":[0-9.]+\}\' /opt/airflow/dags/{{ ds_nodash }}_price.json > /opt/airflow/dags/{{ ds_nodash }}_usd.txt',
)

task_three = BashOperator(
    task_id='log_price',
    dag=first_dag,
    bash_command='cat /opt/airflow/dags/{{ds_nodash}}_usd.txt'
)

task_four = BashOperator(
    task_id='cleanup',
    dag=first_dag,
    bash_command='rm -f /opt/airflow/dags/{{ ds_nodash }}_price.json'
)

# TODO 1: (Tùy chọn) Chuyển các DummyOperator ở trên thành BashOperator để thực hiện công việc thực tế (tham khảo file giải nếu cần).
# Nhiệm vụ: Tải giá Bitcoin (API: https://api.coindesk.com/v1/bpi/currentprice.json), trích xuất chữ "USD", in ra log, và xóa file tạm.

# TODO 2: Khai báo DAG Dependencies (mối quan hệ phụ thuộc) giữa các task
# Ví dụ: task_one >> task_two
task_one >> task_two >> task_three >> task_four