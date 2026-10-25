import airflow
import datetime
import urllib.request as request
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy import DummyOperator
from pathlib import Path

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

second_dag = DAG(
    dag_id='second_dag_stub',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def get_random_user(epoch, output_folder):
    request.urlretrieve(url="https://randomuser.me/api/", filename=f"{output_folder}/{epoch}_user.json")

task_one = PythonOperator(task_id='get_random_user', dag=second_dag, python_callable=get_random_user,
                          op_kwargs={
                              "output_folder":"/opt/airflow/dags",
                              "epoch": "{{ execution_date.int_timestamp }}"
                          })
def check_gender(epoch, output_folder):
    with open(f"{output_folder}/{epoch}_user.json",'r') as f:
        data = json.load(f)
    gender = data['results'][0]['gender']
    if gender == 'female': return 'process_female'
    else: return 'process_male'

task_two = BranchPythonOperator(task_id='check_gender', dag=second_dag, python_callable=check_gender,
                                op_kwargs={'epoch': '{{ execution_date.int_timestamp }}', "output_folder":
                                           "/opt/airflow/dags"},
                          )
def create_sql_file(epoch, output_folder,gender):
    json_file = Path(output_folder) / f"{epoch}_user.json"
    sql_file = Path(output_folder) / f"{gender}_user.sql"

    with json_file.open('r',encoding ="utf-8") as f:
        data = json.load(f)

    user = data["results"][0]
    first_name = user['name']['first']
    last_name = user['name']['last']
    location = user['location']['country']
    email = user['email']
    dob = user['dob']['date']

    sql = f"""
            Insert into testing.random_users (first_name, last_name, location, email, dob, gender)
            values ('{first_name}', '{last_name}', '{location}', '{email}','{dob}','{gender}');

        """
    sql_file.write_text(sql,encoding='utf-8')

def process_female(epoch, output_folder):
    create_sql_file(epoch, output_folder, "female")


def process_male(epoch, output_folder):
    create_sql_file(epoch, output_folder, "male")

task_three_a = PythonOperator(task_id='process_female', dag=second_dag,python_callable=process_female,
                                op_kwargs={'epoch': '{{ execution_date.int_timestamp }}', "output_folder":
                                           "/opt/airflow/dags"},)
task_three_b = PythonOperator(task_id='process_male', dag=second_dag,python_callable=process_male,
                                op_kwargs={'epoch': '{{ execution_date.int_timestamp }}', "output_folder":
                                           "/opt/airflow/dags"},)
task_four_a = PostgresOperator(task_id='insert_female', dag=second_dag, postgres_conn_id="postgres_default",
                               sql="female_user.sql",)
task_four_b = PostgresOperator(task_id='insert_male', dag=second_dag, postgres_conn_id="postgres_default",
                               sql="male_user.sql",)

join_tasks = DummyOperator(
    task_id='join_tasks',
    dag=second_dag,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end',
    dag=second_dag,
    trigger_rule='none_failed'
)

# TODO 1: (Tùy chọn) Nghiên cứu và thay thế các DummyOperator ở trên bằng các PythonOperator, BranchPythonOperator, PostgresOperator tương ứng.
# Nhiệm vụ: Tải dữ liệu JSON từ https://randomuser.me/api/, đọc trường 'gender', rẽ nhánh tạo file SQL tương ứng và chèn vào database.

# TODO 2: Khai báo DAG Dependencies (mối quan hệ luồng đi của các task)
# Gợi ý luồng đi:
task_one >> task_two
task_two >> [task_three_a, task_three_b] #(Rẽ nhánh)
# Nhánh A:
task_three_a >> task_four_a >> join_tasks
# Nhánh B:
task_three_b >> task_four_b >> join_tasks
# Cuối cùng:
join_tasks >> end


