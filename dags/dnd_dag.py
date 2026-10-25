from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
from faker import Faker
import random
from airflow.utils.trigger_rule import TriggerRule

# TODO: Thêm các thư viện cần thiết như requests, faker, random

BASE_URL = "https://www.dnd5eapi.co/api/2014"

default_args = {
    'owner': 'student',
    'start_date': datetime(2026, 10, 1),
    'retries': 1,
}

def get_api_data(endpoint, params = None):
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url,
                                params = params,
                                timeout = 10)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"API request failed: url={url}, error={exc}")
        raise

def generate_base_chars(**kwargs):
    """PythonOperator 1: Khởi tạo 5 nhân vật cơ bản"""
    # TODO: Dùng faker tạo ra 5 nhân vật, mỗi nhân vật có name, attributes (random 6 chỉ số), level (1-3)
    # TODO: Đẩy list các nhân vật này lên XCom

    fake = Faker()
    characters = []
    for i in range(0,5):
        char = {
                "name": fake.name(),
                "attributes":{
                            "Strength": random.randint(6,18),
                            "Dexterity":random.randint(2,18),
                            "Constitution": random.randint(2,18),
                            "Intelligence": random.randint(2,18),
                            "Wisdom":random.randint(2,18),
                            "Charisma":random.randint(2,18)},
                "level": random.randint(1,3),

                }
        characters.append(char)
    kwargs['ti'].xcom_push(key='base_characters',
                           value=characters)


def fetch_races_and_classes(**kwargs):
    """PythonOperator 2: Gọi API lấy race và class cho từng nhân vật"""
    # TODO: Lấy list nhân vật từ XCom
    # TODO: Gọi API https://www.dnd5eapi.co/api/races và https://www.dnd5eapi.co/api/classes để lấy dữ liệu
    # TODO: Cập nhật thông tin race, class cho 5 nhân vật và đẩy lại lên XCom
    # TODO: Nếu gọi API thành công, return "SUCCESS", nếu lỗi, return "FAILED"
    ti = kwargs['ti']
    try:
        characters = ti.xcom_pull(
                                    task_ids = "generate_base_chars",
                                    key="base_characters",
                                    )
        races = get_api_data("races").get("results", [])
        classes = get_api_data("classes").get("results", [])
        if not characters or not races or not classes:
            raise ValueError("Missing characters, species or classes")
        for i in characters:
            i['race'] = random.choice(races)["name"]
            i['class'] = random.choice(classes)['name']
        ti.xcom_push(key='characters',
                     value=characters)
        return "SUCCESS"

    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"Failed to fetch race/class data: {exc}")
        return "FAILED"

def check_api_success(**kwargs):
    """BranchPythonOperator 1: Rẽ nhánh luồng chạy dựa trên kết quả gọi API"""
    # TODO: Lấy kết quả return (SUCCESS/FAILED) từ task fetch_races_and_classes qua XCom
    # TODO: Return tên task tiếp theo tương ứng (tiếp tục sinh dữ liệu hoặc đi vào nhánh báo lỗi)
    result = kwargs['ti'].xcom_pull(task_ids="fetch_races_and_classes",key="return_value",)
    if result == 'SUCCESS':
        return "fetch_spells_and_profs"

    return "log_error"

def fetch_spells_and_profs(**kwargs):
    """PythonOperator 3: Gọi API lấy spells và gán profficiencies, languages"""
    # TODO: Lấy nhân vật từ XCom
    # TODO: Xử lý logic chọn spells (số lượng = level + 3) cho các class phù hợp. Max spell level = 2.
    # TODO: Định dạng lại dữ liệu và lưu lên XCom để chuẩn bị insert
    ti = kwargs['ti']

    characters = ti.xcom_pull(
        task_ids="fetch_races_and_classes",
        key="characters",
    )
    spells = []
    for spell_level in (0, 1, 2):
        result = get_api_data(
            "spells",
            params={"level": spell_level},
        )
        spells.extend(result.get("results", []))

    if not characters or not spells:
        raise ValueError("No characters or spells returned by API")

    languages = [
        "Common", "Elvish", "Dwarvish",
        "Draconic", "Goblin", "Halfling",
    ]

    proficiencies = [
        "Athletics", "Arcana", "History",
        "Perception", "Stealth", "Survival",
    ]

    non_caster_classes = {
        "barbarian", "fighter", "monk", "rogue",
    }

    for character in characters:
        character['languages']=random.sample(languages, k=random.randint(1, 3))
        character["proficiency_choices"] = random.sample(
            proficiencies,
            k=random.randint(1, 3),
        )
        class_name = character['class'].lower()

        if class_name in non_caster_classes:
            character['spells'] = None
        else:
            max_count = min(character['level'] + 3, len(spells))
            count = random.randint(0, max_count)
            character["spells"] = [spell["name"] for spell in random.sample(spells, k=count)]

    ti.xcom_push(
        key="characters",
        value=characters,
    )

    return characters

def log_error(**kwargs):
    """PythonOperator 4: Ghi log lỗi"""
    print("API Failed. Cannot generate characters.")

with DAG('dnd_weekly_characters', default_args=default_args, 
         schedule_interval='@weekly', catchup=False) as dag:

    # TODO: Khai báo 2 DummyOperator (start và end). Lưu ý điều kiện trigger_rule cho end.
    
    # TODO: Khai báo 4 PythonOperator gắn với các hàm python_callable ở trên.
    
    # TODO: Khai báo 1 BranchPythonOperator.
    
    # TODO: Khai báo 1 PostgresOperator (postgres_default) để tạo bảng dnd_characters (nếu chưa có) và dùng Jinja để INSERT dữ liệu.

    # TODO: Nối các task tạo thành pipeline (DAG Dependencies).
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end",trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,)
    generate_chars_task = PythonOperator(task_id="generate_base_chars",
                                         python_callable=generate_base_chars,
                                         )
    fetch_api_task = PythonOperator(
        task_id="fetch_races_and_classes",
        python_callable=fetch_races_and_classes,
    )

    check_api_task = BranchPythonOperator(
        task_id="check_api_success",
        python_callable=check_api_success,
    )

    fetch_spells_task = PythonOperator(
        task_id="fetch_spells_and_profs",
        python_callable=fetch_spells_and_profs,
    )

    error_task = PythonOperator(
        task_id="log_error",
        python_callable=log_error,
    )

    create_insert_task = PostgresOperator(
        task_id="create_insert_characters",
        postgres_conn_id="postgres_default",
        sql="""
         CREATE SCHEMA IF NOT EXISTS testing;
         CREATE TABLE IF NOT EXISTS testing.dnd_characters (
             id SERIAL PRIMARY KEY,
             name TEXT,
             attributes JSONB,
             level INTEGER,
             race TEXT,
             class_name TEXT,
             languages JSONB,
             proficiency_choices JSONB,
             spells JSONB
         );

         {% set chars = ti.xcom_pull(
             task_ids='fetch_spells_and_profs',
             key='characters'
         ) %}

         {% for c in chars %}
         INSERT INTO testing.dnd_characters (
             name, attributes, level, race, class_name,
             languages, proficiency_choices, spells
         )
         VALUES (
             '{{ c.name | replace("'", "''") }}',
             '{{ c.attributes | tojson | replace("'", "''") }}'::jsonb,
             {{ c.level }},
             '{{ c.race | replace("'", "''") }}',
             '{{ c["class"] | replace("'", "''") }}',
             '{{ c.languages | tojson | replace("'", "''") }}'::jsonb,
             '{{ c.proficiency_choices | tojson | replace("'", "''") }}'::jsonb,
             {% if c.spells is none %}
             NULL
             {% else %}
             '{{ c.spells | tojson | replace("'", "''") }}'::jsonb
             {% endif %}
         );
         {% endfor %}
         """,
    )
    # Dependencies
start >> generate_chars_task >> fetch_api_task
fetch_api_task >> check_api_task

check_api_task >> fetch_spells_task
check_api_task >> error_task

fetch_spells_task >> create_insert_task

create_insert_task >> end
error_task >> end